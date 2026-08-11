"""Ingest the FEMA NFIP redacted claims bulk file into DuckDB's raw schema.

National scope, partitioned by state. Territories and null-state rows are kept and
counted, never dropped: FEMA's published national totals include them, and silently
excluding Puerto Rico is the classic way to miss those totals. Nothing here filters
to Michigan -- see CLAUDE.md section 4.

Run:  python -m ingest.insurance.nfip_claims [--mode live|fixture]
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import httpx
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

REPO_ROOT = Path(__file__).resolve().parents[3]

TRACK = "insurance"
SOURCE = "nfip_claims"
DATASET = "NfipClaims"
TABLE = "raw.ins_nfip_claims"
PARTITION_COLUMN = "state"

OPENFEMA_DATASETS = "https://www.fema.gov/api/open/v1/OpenFemaDataSets"
OPENFEMA_FIELDS = "https://www.fema.gov/api/open/v1/OpenFemaDataSetFields"

# No publisher documents a hard rate limit; behave as if all of them do.
USER_AGENT = "bi-portfolio-pipeline/0.1 (chris.hall309@gmail.com)"
PAGE_SLEEP_SECONDS = 1.5
CHUNK_BYTES = 1024 * 1024
PROGRESS_EVERY_BYTES = 50 * 1024 * 1024

# DuckDB writes rows whose partition key is NULL into this directory and maps it
# back to NULL on read. We surface it explicitly so it can never vanish quietly.
HIVE_NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__"


class DiscoveryError(RuntimeError):
    """The publisher's metadata did not look the way we require.

    Always carries the raw response. We stop rather than code around a surprise.
    """


class TransientHTTPError(RuntimeError):
    """A 429 or 5xx worth retrying."""


def log(message: str) -> None:
    print(f"[{TRACK}/{SOURCE}] {message}", flush=True)


# ── pure helpers (unit-tested without a network) ───────────────────────────────


def write_json(path: Path, payload: Any) -> None:
    """Write committed-artifact JSON deterministically: sorted, LF, trailing newline.

    Windows text mode would otherwise emit CRLF and no final newline, so every run
    would rewrite the file and pre-commit would rewrite it back.
    """
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n"
    )


def sql_literal(path: Path | str) -> str:
    """Quote a path for inline SQL. DuckDB takes forward slashes on every platform."""
    return "'" + str(path).replace("\\", "/").replace("'", "''") + "'"


def select_distribution(
    distributions: list[dict[str, Any]],
    preferred: tuple[str, ...] = ("parquet", "csv"),
) -> tuple[str, str]:
    """Pick the best bulk distribution the publisher offers, in preference order."""
    available: dict[str, str] = {}
    for distribution in distributions:
        fmt = (distribution.get("format") or "").strip().lower()
        url = distribution.get("accessURL")
        if fmt and url:
            available.setdefault(fmt, url)

    for fmt in preferred:
        if fmt in available:
            return fmt, available[fmt]

    raise DiscoveryError(
        f"No {' or '.join(preferred)} distribution for {DATASET}. "
        f"Publisher offered: {sorted(available) or 'nothing'}. "
        f"Raw distribution block: {json.dumps(distributions)}"
    )


def deprecation_notice(dataset: dict[str, Any], now: datetime | None = None) -> str | None:
    """Warn when the publisher has scheduled this dataset for removal.

    FEMA deprecated the v2 NFIP datasets with a two-month runway and froze the data
    months before that. A pipeline that does not read `depDate` finds out by breaking.
    """
    deprecation_date = dataset.get("depDate")
    if not deprecation_date:
        return None

    removal = datetime.fromisoformat(deprecation_date.replace("Z", "+00:00"))
    days_left = (removal - (now or datetime.now(UTC))).days
    return (
        f"DEPRECATED: {dataset.get('name')} v{dataset.get('version')} is removed on "
        f"{removal.date().isoformat()} ({days_left} days from now). "
        f"Replacement: {dataset.get('depNewURL') or 'none published'}. "
        f"Publisher note: {(dataset.get('depApiMessage') or '').strip()[:200]}"
    )


def load_state_codes() -> frozenset[str]:
    """The domain-neutral state reference, used here only to recognise partition keys."""
    path = REPO_ROOT / "platform" / "reference" / "state_codes.csv"
    with path.open(encoding="utf-8", newline="") as handle:
        return frozenset(row["state_code"] for row in csv.DictReader(handle))


def nonstandard_partitions(
    partition_rows: dict[str, int], known_codes: frozenset[str]
) -> dict[str, int]:
    """Partition keys the state reference does not recognise.

    NFIP uses the literal code 'UN' for claims whose state is unavailable -- roughly
    16k rows, mostly pre-1990 with `reportedCity` set to "Currently Unavailable". They
    are real claims and stay in raw. Surfacing them here is what stops them from
    disappearing into a failed join in session 2.
    """
    return {key: rows for key, rows in sorted(partition_rows.items()) if key not in known_codes}


def manifest_payload(
    *,
    dataset: dict[str, Any],
    distribution_format: str,
    resolved_url: str,
    landing_file: str,
    landing_bytes: int,
    landing_sha256: str,
    rows_landing: int,
    rows_raw: int,
    rows_duckdb: int,
    partition_rows: dict[str, int],
    known_partition_keys: frozenset[str],
    retrieved_at: str,
) -> dict[str, Any]:
    """Everything L1's count chain needs, plus the provenance to defend a number."""
    unknown = nonstandard_partitions(partition_rows, known_partition_keys)
    return {
        "track": TRACK,
        "source": SOURCE,
        "publisher": "OpenFEMA",
        "dataset_name": dataset.get("name"),
        "dataset_version": dataset.get("version"),
        "dataset_identifier": dataset.get("identifier"),
        "resolved_url": resolved_url,
        "distribution_format": distribution_format,
        "retrieved_at": retrieved_at,
        "landing_files": [{"name": landing_file, "bytes": landing_bytes, "sha256": landing_sha256}],
        "source_reported_count": dataset.get("recordCount"),
        "source_last_refresh": dataset.get("lastDataSetRefresh"),
        "source_metadata_refresh": dataset.get("lastRefresh"),
        "source_hash": dataset.get("hash"),
        "deprecation": (
            None
            if not dataset.get("depDate")
            else {"removal_date": dataset["depDate"], "replacement": dataset.get("depNewURL")}
        ),
        "duckdb_table": TABLE,
        "rows_landing": rows_landing,
        "rows_raw": rows_raw,
        "rows_duckdb": rows_duckdb,
        "partition_column": PARTITION_COLUMN,
        "partition_rows": partition_rows,
        "null_partition_rows": partition_rows.get(HIVE_NULL_PARTITION, 0),
        "nonstandard_partition_keys": sorted(unknown),
        "nonstandard_partition_rows": sum(unknown.values()),
    }


# ── network (retried, polite) ──────────────────────────────────────────────────

_retry = retry(
    retry=retry_if_exception_type((TransientHTTPError, httpx.TimeoutException, httpx.NetworkError)),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    stop=stop_after_attempt(5),
    reraise=True,
)


def _raise_for_transient(response: httpx.Response) -> None:
    if response.status_code == 429 or response.status_code >= 500:
        raise TransientHTTPError(f"HTTP {response.status_code} from {response.request.url}")
    response.raise_for_status()


@_retry
def discover_dataset(client: httpx.Client, name: str) -> dict[str, Any]:
    """Resolve dataset metadata from the publisher. Never hardcode a bulk URL."""
    response = client.get(OPENFEMA_DATASETS, params={"$filter": f"name eq '{name}'"})
    _raise_for_transient(response)
    records = response.json().get("OpenFemaDataSets", [])
    if len(records) != 1:
        raise DiscoveryError(
            f"Expected exactly one dataset named {name!r}, got {len(records)}. "
            f"Raw response: {response.text[:2000]}"
        )
    return records[0]


@_retry
def _field_page(client: httpx.Client, name: str, skip: int) -> list[dict[str, Any]]:
    response = client.get(
        OPENFEMA_FIELDS,
        params={
            "$filter": f"openFemaDataSet eq '{name}'",
            "$top": "1000",
            "$skip": str(skip),
        },
    )
    _raise_for_transient(response)
    return response.json().get("OpenFemaDataSetFields", [])


def fetch_field_baseline(client: httpx.Client, name: str) -> list[dict[str, Any]]:
    """Snapshot the publisher's own field metadata. Capture only -- drift diffing is L2."""
    fields: list[dict[str, Any]] = []
    skip = 0
    while True:
        page = _field_page(client, name, skip)
        if not page:
            break
        fields.extend(page)
        skip += len(page)
        time.sleep(PAGE_SLEEP_SECONDS)

    if not fields:
        raise DiscoveryError(f"No field metadata returned for {name!r}; refusing a blank baseline.")
    return sorted(fields, key=lambda field: field.get("name", ""))


@_retry
def stream_download(client: httpx.Client, url: str, destination: Path) -> tuple[int, str]:
    """Stream to a .part file, hashing as we go, and rename only on completion."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    part = destination.with_name(destination.name + ".part")
    digest = hashlib.sha256()
    total = 0
    next_progress = PROGRESS_EVERY_BYTES

    with client.stream("GET", url) as response:
        _raise_for_transient(response)
        expected = int(response.headers.get("content-length") or 0)
        with part.open("wb") as handle:
            for chunk in response.iter_bytes(CHUNK_BYTES):
                handle.write(chunk)
                digest.update(chunk)
                total += len(chunk)
                if total >= next_progress:
                    share = f" of {expected / 1e6:.0f} MB" if expected else ""
                    log(f"  downloaded {total / 1e6:.0f} MB{share}")
                    next_progress += PROGRESS_EVERY_BYTES

    if expected and total != expected:
        part.unlink(missing_ok=True)
        raise TransientHTTPError(f"Truncated download: got {total} bytes, expected {expected}")

    part.replace(destination)
    return total, digest.hexdigest()


# ── conversion and load ───────────────────────────────────────────────────────


def source_relation(landing: Path, distribution_format: str) -> str:
    """A DuckDB relation over the landed file, with no type inference for CSV."""
    if distribution_format == "parquet":
        return f"read_parquet({sql_literal(landing)})"
    # The all-varchar rule: government CSVs carry sentinels and suppression markers
    # that type inference destroys. Typing is session 2's job, column by column.
    return f"read_csv({sql_literal(landing)}, all_varchar=true, header=true)"


def repartition_to_raw(
    con: duckdb.DuckDBPyConnection,
    landing: Path,
    distribution_format: str,
    raw_dir: Path,
) -> tuple[int, int]:
    """Convert landing -> raw parquet partitioned by state. Layout only, no content change.

    Truncate-and-reload: the new tree is built beside the old one and swapped in, so a
    re-run can never merge fresh partitions into stale ones.
    """
    relation = source_relation(landing, distribution_format)
    rows_landing = con.execute(f"SELECT count(*) FROM {relation}").fetchone()[0]

    staging = raw_dir.parent / f".{raw_dir.name}.staging"
    previous = raw_dir.parent / f".{raw_dir.name}.previous"
    # DuckDB's COPY creates the partition tree but not its parents.
    raw_dir.parent.mkdir(parents=True, exist_ok=True)
    for scratch in (staging, previous):
        shutil.rmtree(scratch, ignore_errors=True)

    con.execute(
        f"COPY (SELECT * FROM {relation}) TO {sql_literal(staging)} "
        f"(FORMAT parquet, PARTITION_BY ({PARTITION_COLUMN}))"
    )

    if raw_dir.exists():
        raw_dir.rename(previous)
    staging.rename(raw_dir)
    shutil.rmtree(previous, ignore_errors=True)

    rows_raw = con.execute(f"SELECT count(*) FROM {raw_glob(raw_dir)}").fetchone()[0]
    return rows_landing, rows_raw


def raw_glob(raw_dir: Path) -> str:
    """Read the partitioned tree, restoring `state` from the path.

    DuckDB's PARTITION_BY removes the key from the written files, so hive partitioning
    reconstructs it rather than colliding with a duplicate column.
    """
    pattern = raw_dir / "**" / "*.parquet"
    return f"read_parquet({sql_literal(pattern)}, hive_partitioning=true)"


def partition_rows(con: duckdb.DuckDBPyConnection, raw_dir: Path) -> dict[str, int]:
    rows = con.execute(
        f"SELECT COALESCE({PARTITION_COLUMN}, '{HIVE_NULL_PARTITION}') AS partition_key, "
        f"count(*) AS n FROM {raw_glob(raw_dir)} GROUP BY 1 ORDER BY 1"
    ).fetchall()
    return {str(key): int(count) for key, count in rows}


def load_duckdb(con: duckdb.DuckDBPyConnection, raw_dir: Path) -> int:
    schema = TABLE.split(".")[0]
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"CREATE OR REPLACE TABLE {TABLE} AS SELECT * FROM {raw_glob(raw_dir)}")
    return con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]


# ── orchestration ─────────────────────────────────────────────────────────────


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


def run(mode: str, force: bool = False) -> int:
    data_root, db_path = paths_for(mode)
    landing_dir = data_root / "landing" / TRACK / SOURCE
    raw_dir = data_root / "raw" / TRACK / SOURCE
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if mode == "fixture":
        landing = REPO_ROOT / "tests" / "fixtures" / TRACK / f"{SOURCE}.parquet"
        if not landing.exists():
            log(f"FAIL: fixture missing at {landing}")
            return 1
        dataset = json.loads(
            (REPO_ROOT / "tests" / "fixtures" / TRACK / f"{SOURCE}.metadata.json").read_text(
                encoding="utf-8"
            )
        )
        distribution_format, resolved_url = "parquet", "fixture://" + landing.name
        landing_bytes = landing.stat().st_size
        landing_sha256 = hashlib.sha256(landing.read_bytes()).hexdigest()
        log("fixture mode: discovery and download SKIPPED (offline)")
    else:
        with httpx.Client(
            timeout=httpx.Timeout(30.0, read=300.0),
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        ) as client:
            log(f"discovering {DATASET} from {OPENFEMA_DATASETS}")
            dataset = discover_dataset(client, DATASET)

            notice = deprecation_notice(dataset)
            if notice:
                log(f"WARNING  {notice}")

            distribution_format, resolved_url = select_distribution(dataset["distribution"])
            log(f"resolved {distribution_format} distribution: {resolved_url}")
            log(
                f"source reports {dataset.get('recordCount'):,} rows, "
                f"last refreshed {dataset.get('lastDataSetRefresh')}"
            )

            existing = read_existing_manifest(raw_dir)
            if (
                not force
                and existing
                and existing.get("source_last_refresh") == dataset.get("lastDataSetRefresh")
            ):
                log("SKIPPED (current): manifest matches the publisher's refresh timestamp")
                return 0

            baseline_dir = REPO_ROOT / "platform" / "reconcile" / "baselines"
            baseline_dir.mkdir(parents=True, exist_ok=True)
            baseline = baseline_dir / f"{TRACK}__{SOURCE}__fields.json"
            fields = fetch_field_baseline(client, DATASET)
            write_json(baseline, fields)
            log(f"schema baseline: {len(fields)} fields -> {baseline.name}")

            landing = landing_dir / resolved_url.rsplit("/", 1)[-1]
            log(f"downloading -> {landing}")
            landing_bytes, landing_sha256 = stream_download(client, resolved_url, landing)
            log(f"landed {landing_bytes / 1e6:.1f} MB  sha256={landing_sha256[:16]}...")

    con = duckdb.connect(str(db_path))
    try:
        log(f"repartitioning by {PARTITION_COLUMN} -> {raw_dir}")
        rows_landing, rows_raw = repartition_to_raw(con, landing, distribution_format, raw_dir)
        counts = partition_rows(con, raw_dir)
        rows_duckdb = load_duckdb(con, raw_dir)
    finally:
        con.close()

    manifest = manifest_payload(
        dataset=dataset,
        distribution_format=distribution_format,
        resolved_url=resolved_url,
        landing_file=landing.name,
        landing_bytes=landing_bytes,
        landing_sha256=landing_sha256,
        rows_landing=rows_landing,
        rows_raw=rows_raw,
        rows_duckdb=rows_duckdb,
        partition_rows=counts,
        known_partition_keys=load_state_codes(),
        retrieved_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    )
    write_json(raw_dir / "manifest.json", manifest)

    log(f"partitions: {len(counts)}  (null-state rows: {counts.get(HIVE_NULL_PARTITION, 0):,})")
    if manifest["nonstandard_partition_keys"]:
        detail = ", ".join(
            f"{key}={counts[key]:,}" for key in manifest["nonstandard_partition_keys"]
        )
        log(f"NOTE  partition keys outside the state reference, kept and counted: {detail}")
    log(f"landing {rows_landing:,} -> raw {rows_raw:,} -> {TABLE} {rows_duckdb:,}")
    log(f"michigan rows: {counts.get('MI', 0):,}")
    log("done")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("live", "fixture"),
        default=os.environ.get("DATA_MODE", "live"),
        help="fixture mode never touches the network (default: $DATA_MODE or live)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-download even when the manifest matches the publisher's refresh timestamp",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return run(args.mode, force=args.force)
    except DiscoveryError as error:
        log(f"FAIL (discovery): {error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
