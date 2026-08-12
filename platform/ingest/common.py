"""Shared ingestion utilities, extracted from two working fetchers.

Written only after `ingest.insurance.nfip_claims` and `ingest.fintech.cfpb` were both
complete and verified against live publishers, so everything here is a generalization
of code that already worked twice rather than a guess at what a fetcher might need.
See CLAUDE.md section 8.

What deliberately did NOT move here, because the two sources disagree about it and
inventing a common shape would mean inventing one neither publisher has:

  * discovery          a JSON dataset catalogue (OpenFEMA) vs scraping an HTML page (CFPB)
  * source counts      a `recordCount` field vs an Elasticsearch `hits.total.value`
  * refresh keys       a `lastDataSetRefresh` field vs a `last-modified` HTTP header
  * partition rules    "is this a US state code?" vs "is this a plausible complaint year?"
  * caveats            deprecation windows vs publication-window coverage limits

Those live with the source that owns them. A publisher surface that several sources do
share gets its own module rather than a home here -- `ingest.openfema` for the FEMA
dataset catalogue -- so that "shared by everything" and "shared by the FEMA sources"
stay visibly different things.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import duckdb
import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

REPO_ROOT = Path(__file__).resolve().parents[2]
BASELINE_DIR = REPO_ROOT / "platform" / "reconcile" / "baselines"
STATE_CODES_CSV = REPO_ROOT / "platform" / "reference" / "state_codes.csv"

# No publisher documents a hard rate limit; behave as if all of them do.
USER_AGENT = "bi-portfolio-pipeline/0.1 (chris.hall309@gmail.com)"
PAGE_SLEEP_SECONDS = 1.5
CHUNK_BYTES = 1024 * 1024

# DuckDB writes rows whose partition key is NULL into this directory and maps it back
# to NULL on read. Surfaced explicitly everywhere so it can never vanish quietly.
HIVE_NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__"


class DiscoveryError(RuntimeError):
    """The publisher's metadata did not look the way we require.

    Always carries the raw response. We stop rather than code around a surprise.
    """


class TransientHTTPError(RuntimeError):
    """A 429 or 5xx worth retrying."""


# ── small helpers ─────────────────────────────────────────────────────────────


def make_logger(track: str, source: str) -> Callable[[str], None]:
    def log(message: str) -> None:
        print(f"[{track}/{source}] {message}", flush=True)

    return log


def human_bytes(count: float) -> str:
    return f"{count / 1e9:.2f} GB" if count >= 1e9 else f"{count / 1e6:.1f} MB"


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: Path, payload: Any) -> None:
    """Deterministic committed-artifact JSON: sorted, LF, trailing newline.

    Windows text mode would otherwise emit CRLF and no final newline, so every run
    would rewrite the file and pre-commit would rewrite it back.
    """
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n"
    )


def read_secret(name: str) -> str | None:
    """Read an API key from the environment, falling back to a gitignored `.env` file.

    Ten lines instead of a dependency, and it makes `.env.example`'s instruction ("copy to
    `.env`") actually true. Only api.census.gov requires a key today; the value is never
    logged, never written to a manifest, and `.env` is gitignored.
    """
    if value := os.environ.get(name):
        return value.strip() or None

    env_file = REPO_ROOT / ".env"
    if not env_file.exists():
        return None
    for line in env_file.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        if key.strip() == name:
            return value.strip().strip("'\"") or None
    return None


def write_baseline(track: str, source: str, payload: Any, *, kind: str = "fields") -> Path:
    """Commit a schema baseline under one per-source name, and return where it went.

    Session 1 captures baselines only; diffing them for drift is L2's job. `kind` is
    'fields' for publishers that expose machine-readable field metadata and 'pointer'
    for the ones that only publish documentation -- CLAUDE.md rule 8.
    """
    BASELINE_DIR.mkdir(parents=True, exist_ok=True)
    path = BASELINE_DIR / f"{track}__{source}__{kind}.json"
    write_json(path, payload)
    return path


def sql_literal(path: Path | str) -> str:
    """Quote a path for inline SQL. DuckDB takes forward slashes on every platform."""
    return "'" + str(path).replace("\\", "/").replace("'", "''") + "'"


def paths_for(mode: str) -> tuple[Path, Path]:
    """Fixture runs are fully isolated so they can never clobber real raw data."""
    if mode == "fixture":
        return REPO_ROOT / "data" / "_fixture", REPO_ROOT / "platform" / "duckdb" / "fixture.duckdb"
    return REPO_ROOT / "data", REPO_ROOT / "platform" / "duckdb" / "bi_portfolio.duckdb"


def read_existing_manifest(raw_dir: Path) -> dict[str, Any] | None:
    manifest = raw_dir / "manifest.json"
    if not manifest.exists():
        return None
    try:
        return json.loads(manifest.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def tables_present(db_path: Path, *tables: str) -> bool:
    """True only when the database exists, opens, and holds every table named.

    A database that will not open counts as absent. That is the case this exists for: a
    deleted or corrupt warehouse is exactly when a run must not decide it has nothing to do.
    """
    if not db_path.exists():
        return False
    try:
        con = duckdb.connect(str(db_path), read_only=True)
    except duckdb.Error:
        return False
    try:
        for table in tables:
            schema, _, name = table.rpartition(".")
            found = con.execute(
                "SELECT count(*) FROM duckdb_tables() WHERE schema_name = ? AND table_name = ?",
                [schema or "main", name],
            ).fetchone()[0]
            if not found:
                return False
        return True
    finally:
        con.close()


def skip_as_current(
    *,
    force: bool,
    publisher_unchanged: bool,
    db_path: Path,
    tables: tuple[str, ...],
    log: Callable[[str], None],
) -> bool:
    """Is skipping this run safe? A manifest on its own cannot answer that.

    `publisher_unchanged` is the source's own answer to "has the publisher's copy moved?" --
    a refresh timestamp, a last-modified header, a per-year fingerprint; every publisher
    signals it differently and that half stays with the source. This adds the half that is
    identical everywhere: a skip is only safe if the tables the run would have produced are
    actually in the warehouse. Without it, a deleted database plus current manifests reports
    "SKIPPED (current)" for every source and leaves nothing behind.
    """
    if force or not publisher_unchanged:
        return False
    missing = [table for table in tables if not tables_present(db_path, table)]
    if not missing:
        return True
    log(
        f"manifest is current but {', '.join(missing)} is not in {db_path.name}; re-running. "
        "`just reload` rebuilds every table from raw parquet without the network."
    )
    return False


# ── network ───────────────────────────────────────────────────────────────────

retrying = retry(
    retry=retry_if_exception_type((TransientHTTPError, httpx.TimeoutException, httpx.NetworkError)),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    reraise=True,
)


def raise_for_transient(response: httpx.Response) -> None:
    if response.status_code == 429 or response.status_code >= 500:
        raise TransientHTTPError(f"HTTP {response.status_code} from {response.request.url}")
    response.raise_for_status()


def http_client(read_timeout: float = 300.0) -> httpx.Client:
    """An honest User-Agent on every request. BLS 403s the default one, and a
    publisher that wants to throttle us should be able to identify us first."""
    return httpx.Client(
        timeout=httpx.Timeout(30.0, read=read_timeout),
        headers={"User-Agent": USER_AGENT},
        follow_redirects=True,
    )


@retrying
def stream_download(
    client: httpx.Client,
    url: str,
    destination: Path,
    *,
    log: Callable[[str], None],
    progress_every: int = 50 * CHUNK_BYTES,
) -> tuple[int, str]:
    """Stream to a .part file, hashing as we go, and rename only on completion."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    part = destination.with_name(destination.name + ".part")
    digest = hashlib.sha256()
    total = 0
    next_progress = progress_every

    with client.stream("GET", url) as response:
        raise_for_transient(response)
        expected = int(response.headers.get("content-length") or 0)
        # A compressed transfer makes content-length describe the *encoded* body while we
        # count decoded bytes, so the two legitimately disagree -- CMS serves this file
        # gzipped at 24.9 MB for 57.9 MB of CSV. Comparing them would fail every time.
        # Truncation is still caught: gzip carries its own CRC and length trailer, and the
        # decoder raises when a stream ends early.
        encoded = (response.headers.get("content-encoding") or "").strip().lower()
        with part.open("wb") as handle:
            for chunk in response.iter_bytes(CHUNK_BYTES):
                handle.write(chunk)
                digest.update(chunk)
                total += len(chunk)
                if total >= next_progress:
                    share = f" of {human_bytes(expected)}" if expected and not encoded else ""
                    log(f"  downloaded {human_bytes(total)}{share}")
                    next_progress += progress_every

    if expected and not encoded and total != expected:
        part.unlink(missing_ok=True)
        raise TransientHTTPError(f"Truncated download: got {total} bytes, expected {expected}")

    part.replace(destination)
    return total, digest.hexdigest()


@retrying
def _odata_page(
    client: httpx.Client, url: str, params: dict[str, str], envelope_key: str
) -> list[dict[str, Any]]:
    response = client.get(url, params=params)
    raise_for_transient(response)
    return response.json().get(envelope_key, [])


def fetch_odata_records(
    client: httpx.Client,
    url: str,
    envelope_key: str,
    *,
    params: dict[str, str] | None = None,
    page_size: int = 1000,
    sleep: float = PAGE_SLEEP_SECONDS,
) -> list[dict[str, Any]]:
    """Page an OpenFEMA endpoint to exhaustion via $top/$skip.

    OData-shaped only: Socrata pages with $limit/$offset and needs an explicit
    $order to be deterministic, so it will bring its own loop when it lands rather
    than bending this one into a shape neither publisher actually uses.
    """
    records: list[dict[str, Any]] = []
    skip = 0
    while True:
        page = _odata_page(
            client,
            url,
            {**(params or {}), "$top": str(page_size), "$skip": str(skip)},
            envelope_key,
        )
        if not page:
            return records
        records.extend(page)
        skip += len(page)
        time.sleep(sleep)


# ── DuckDB conversion and load ────────────────────────────────────────────────


def raw_relation(raw_dir: Path) -> str:
    """Read the partitioned tree, restoring the partition key from the path.

    DuckDB's PARTITION_BY removes the key from the written files, so hive
    partitioning reconstructs it rather than colliding with a duplicate column.

    `hive_types_autocast=0` because reconstructing the key from a directory name is
    where DuckDB would otherwise apply type inference -- the one thing the all-varchar
    rule forbids in raw (CLAUDE.md rule 2). Nothing in the parquet files is wrong; a
    `year=2011` directory simply read back as BIGINT. That silently retyped five
    columns, cost two CASTs written to work around it, and made partition keys come
    back as `int` from a query but `str` from a re-read CSV.

    This is the only definition of it in the repo. `reconcile.l1_integrity` imports
    this function rather than restating it, so the harness can never read the same
    tree with different types than the loader wrote it.
    """
    pattern = raw_dir / "**" / "*.parquet"
    return f"read_parquet({sql_literal(pattern)}, hive_partitioning=true, hive_types_autocast=0)"


def repartition_to_raw(
    con: duckdb.DuckDBPyConnection,
    *,
    relation: str,
    raw_dir: Path,
    partition_column: str,
    projection: str = "*",
) -> tuple[int, int]:
    """Convert landing -> raw parquet, partitioned. Layout only, no content change.

    Truncate-and-reload: the new tree is built beside the old one and swapped in, so
    a re-run can never merge fresh partitions into stale ones.
    """
    rows_landing = con.execute(f"SELECT count(*) FROM {relation}").fetchone()[0]

    staging = raw_dir.parent / f".{raw_dir.name}.staging"
    previous = raw_dir.parent / f".{raw_dir.name}.previous"
    # DuckDB's COPY creates the partition tree but not its parents.
    raw_dir.parent.mkdir(parents=True, exist_ok=True)
    for scratch in (staging, previous):
        shutil.rmtree(scratch, ignore_errors=True)

    con.execute(
        f"COPY (SELECT {projection} FROM {relation}) TO {sql_literal(staging)} "
        f"(FORMAT parquet, PARTITION_BY ({partition_column}))"
    )

    if raw_dir.exists():
        raw_dir.rename(previous)
    staging.rename(raw_dir)

    # The manifest lives inside the tree we just replaced, so the swap would destroy it.
    # Carry it across: every caller writes a fresh manifest once its own checks pass, but
    # between here and there sit the table load and the source's validation -- and a failure
    # in that window would leave raw parquet with no record of which URL it came from, what
    # the publisher reported, or when. A stale manifest beside fresh parquet is a state
    # `just reload` names as a count disagreement, which is the honest signal that a run
    # did not finish. Silently having none is not.
    carried = previous / "manifest.json"
    if carried.exists():
        shutil.copy2(carried, raw_dir / "manifest.json")
    shutil.rmtree(previous, ignore_errors=True)

    rows_raw = con.execute(f"SELECT count(*) FROM {raw_relation(raw_dir)}").fetchone()[0]
    return rows_landing, rows_raw


def partition_counts(
    con: duckdb.DuckDBPyConnection, raw_dir: Path, partition_column: str
) -> dict[str, int]:
    rows = con.execute(
        f"SELECT COALESCE({partition_column}, '{HIVE_NULL_PARTITION}') AS partition_key, "
        f"count(*) AS n FROM {raw_relation(raw_dir)} GROUP BY 1 ORDER BY 1"
    ).fetchall()
    return {str(key): int(count) for key, count in rows}


def load_table(con: duckdb.DuckDBPyConnection, table: str, raw_dir: Path) -> int:
    schema = table.split(".")[0]
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {raw_relation(raw_dir)}")
    return con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


# ── reference data and partition keys ─────────────────────────────────────────


def load_state_codes() -> frozenset[str]:
    """The domain-neutral state reference, used to recognise partition keys."""
    with STATE_CODES_CSV.open(encoding="utf-8", newline="") as handle:
        return frozenset(row["state_code"] for row in csv.DictReader(handle))


def load_state_fips() -> dict[str, str]:
    """The same reference keyed by FIPS, for publishers that identify states numerically."""
    with STATE_CODES_CSV.open(encoding="utf-8", newline="") as handle:
        return {row["state_fips"]: row["state_code"] for row in csv.DictReader(handle)}


def nonstandard_partitions(
    partition_rows: dict[str, int], known_keys: frozenset[str]
) -> dict[str, int]:
    """Partition keys the reference set does not recognise, kept and counted.

    Publishers put real rows behind keys their own code lists do not contain, and those
    rows stay in raw. Surfacing them here is what stops them from disappearing into a
    failed join in session 2. Sources whose keys are not code-list members -- CFPB
    partitions on year -- bring their own plausibility rule instead.
    """
    return {key: rows for key, rows in sorted(partition_rows.items()) if key not in known_keys}


# ── manifest ──────────────────────────────────────────────────────────────────


def base_manifest(
    *,
    track: str,
    source: str,
    publisher: str,
    dataset_name: str | None,
    resolved_url: str,
    distribution_format: str,
    retrieved_at: str,
    landing_files: list[dict[str, Any]],
    source_reported_count: int | None,
    source_last_refresh: str | None,
    duckdb_table: str,
    rows_landing: int,
    rows_raw: int,
    rows_duckdb: int,
    partition_column: str,
    partition_rows: dict[str, int],
    nonstandard_partitions: dict[str, int],
) -> dict[str, Any]:
    """The fields every source records. Callers merge their own on top.

    This is what L1's count chain reads, plus enough provenance to defend a number
    months later: which URL, retrieved when, what the publisher claimed at the time.
    """
    return {
        "track": track,
        "source": source,
        "publisher": publisher,
        "dataset_name": dataset_name,
        "resolved_url": resolved_url,
        "distribution_format": distribution_format,
        "retrieved_at": retrieved_at,
        "landing_files": landing_files,
        "source_reported_count": source_reported_count,
        "source_last_refresh": source_last_refresh,
        "duckdb_table": duckdb_table,
        "rows_landing": rows_landing,
        "rows_raw": rows_raw,
        "rows_duckdb": rows_duckdb,
        "partition_column": partition_column,
        "partition_rows": partition_rows,
        "null_partition_rows": partition_rows.get(HIVE_NULL_PARTITION, 0),
        "nonstandard_partition_keys": sorted(nonstandard_partitions),
        "nonstandard_partition_rows": sum(nonstandard_partitions.values()),
    }
