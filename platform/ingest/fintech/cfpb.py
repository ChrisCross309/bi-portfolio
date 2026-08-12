"""Ingest the CFPB Consumer Complaint Database bulk archive into DuckDB's raw schema.

National scope, partitioned by year of receipt. Nothing is filtered to Michigan --
see CLAUDE.md section 4.

Backfill comes from the bulk archive, never from the search API. That API is
Elasticsearch-backed with a deep-paging window limit and a reported bug where the
`frm` offset fails to advance results, so paging it would silently return the same
rows. We call it once, for a count, as an L1 control total only.

Run:  python -m ingest.fintech.cfpb [--mode live|fixture]
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import httpx
from ingest.common import (
    CHUNK_BYTES,
    REPO_ROOT,
    DiscoveryError,
    base_manifest,
    http_client,
    human_bytes,
    load_table,
    make_logger,
    partition_counts,
    paths_for,
    raise_for_transient,
    read_existing_manifest,
    repartition_to_raw,
    retrying,
    sha256_of,
    sql_literal,
    stream_download,
    write_baseline,
    write_json,
)

TRACK = "fintech"
SOURCE = "cfpb_complaints"
TABLE = "raw.fin_cfpb_complaints"
PARTITION_COLUMN = "year"

DATA_PAGE = "https://www.consumerfinance.gov/data-research/consumer-complaints/"
SEARCH_API = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
BULK_URL_PATTERN = re.compile(r"https://files\.consumerfinance\.gov/ccdb/[\w.\-]+\.csv\.zip")

PROGRESS_EVERY_BYTES = 200 * CHUNK_BYTES

# The database opened in 2011; anything outside this is a malformed or empty date.
FIRST_COMPLAINT_YEAR = 2011
YEAR_PATTERN = re.compile(r"^\d{4}$")

# A 9 GB+ CSV read single-threaded: let DuckDB spill rather than exhaust RAM.
DUCKDB_MEMORY_LIMIT = "4GB"

log = make_logger(TRACK, SOURCE)


# ── pure helpers (unit-tested without a network) ───────────────────────────────


def find_bulk_url(html: str) -> str:
    """Resolve the bulk archive link from CFPB's own data page. Never hardcode it."""
    matches = BULK_URL_PATTERN.findall(html)
    if not matches:
        raise DiscoveryError(
            "No ccdb *.csv.zip link found on the CFPB data page. The page layout may have "
            f"changed. Searched {len(html):,} bytes for {BULK_URL_PATTERN.pattern!r}."
        )
    unique = sorted(set(matches))
    if len(unique) > 1:
        raise DiscoveryError(f"Ambiguous bulk links on the CFPB data page: {unique}")
    return unique[0]


def find_date_column(columns: list[str]) -> str:
    """Locate the received-date column without assuming its exact spelling.

    The bulk CSV uses human-readable headers ("Date received") while the API uses
    snake_case, and we would rather discover it than hardcode a guess that breaks
    quietly when the publisher retitles a column.
    """
    for column in columns:
        if re.fullmatch(r"date[ _]?received", column.strip(), flags=re.IGNORECASE):
            return column
    raise DiscoveryError(
        f"No 'date received' column among {len(columns)} CSV headers: {columns[:25]}"
    )


def nonstandard_partitions(partition_rows: dict[str, int]) -> dict[str, int]:
    """Partition keys that are not plausible complaint years.

    Rows with a blank or malformed received date land here. They are real complaints
    and stay in raw; surfacing them stops them from vanishing into a failed date join.
    """
    current_year = datetime.now(UTC).year
    return {
        key: rows
        for key, rows in sorted(partition_rows.items())
        if not (
            YEAR_PATTERN.fullmatch(key) and FIRST_COMPLAINT_YEAR <= int(key) <= current_year + 1
        )
    }


def source_relation(csv_path: Path) -> str:
    """The all-varchar rule, plus a single-threaded read.

    Narratives contain newlines inside quoted fields. That is why row counts come from
    SQL rather than from counting lines -- and also why `parallel=false` is required:
    the parallel reader splits the file into byte ranges and cannot tell a record
    boundary from a newline inside a quoted narrative, so on this file it refuses
    outright with "The Parallel CSV Reader currently does not support a full read on
    this file." Slower, and the only correct option.
    """
    return f"read_csv({sql_literal(csv_path)}, all_varchar=true, header=true, parallel=false)"


def manifest_payload(
    *,
    resolved_url: str,
    http_metadata: dict[str, Any],
    landing_files: list[dict[str, Any]],
    source_reported_count: int | None,
    source_reported_michigan: int | None,
    date_column: str,
    rows_landing: int,
    rows_raw: int,
    rows_duckdb: int,
    partition_rows: dict[str, int],
    retrieved_at: str,
) -> dict[str, Any]:
    return {
        **base_manifest(
            track=TRACK,
            source=SOURCE,
            publisher="CFPB",
            dataset_name="Consumer Complaint Database",
            resolved_url=resolved_url,
            distribution_format="csv.zip",
            retrieved_at=retrieved_at,
            landing_files=landing_files,
            source_reported_count=source_reported_count,
            # `last-modified` on the archive is CFPB's refresh signal; it has no catalogue.
            source_last_refresh=http_metadata.get("last_modified"),
            duckdb_table=TABLE,
            rows_landing=rows_landing,
            rows_raw=rows_raw,
            rows_duckdb=rows_duckdb,
            partition_column=PARTITION_COLUMN,
            partition_rows=partition_rows,
            nonstandard_partitions=nonstandard_partitions(partition_rows),
        ),
        "discovered_from": DATA_PAGE,
        "source_etag": http_metadata.get("etag"),
        "source_content_length": http_metadata.get("content_length"),
        "source_reported_michigan": source_reported_michigan,
        "source_count_endpoint": SEARCH_API,
        "date_column": date_column,
        "coverage_caveats": [
            "Narratives exist only for the subset where the consumer consented to publication.",
            "Recent windows are incomplete: a complaint publishes after the company responds "
            "or 15 days elapse, so 'latest complete month' means a closed publication window.",
            "Complaints referred to other regulators (depositories under $10B) are absent "
            "entirely, so this is not a census of consumer complaints.",
            "Some ZIP codes are masked; they are text, per the all-varchar raw rule.",
        ],
    }


# ── network ───────────────────────────────────────────────────────────────────


@retrying
def discover_bulk_url(client: httpx.Client) -> str:
    response = client.get(DATA_PAGE)
    raise_for_transient(response)
    return find_bulk_url(response.text)


@retrying
def head_metadata(client: httpx.Client, url: str) -> dict[str, Any]:
    response = client.head(url)
    raise_for_transient(response)
    return {
        "content_length": int(response.headers.get("content-length") or 0),
        "last_modified": response.headers.get("last-modified"),
        "etag": response.headers.get("etag"),
    }


@retrying
def source_reported_count(client: httpx.Client, **filters: str) -> int:
    """Total complaints the search API reports, for L1's count chain only.

    `no_highlight=true` is required: without it the endpoint returns a bare 404 with
    `{"detail": "Not found."}` even though the query is otherwise valid.
    """
    params = {"size": "1", "no_aggs": "true", "no_highlight": "true", **filters}
    response = client.get(SEARCH_API, params=params)
    raise_for_transient(response)
    body = response.json()
    try:
        return int(body["hits"]["total"]["value"])
    except (KeyError, TypeError, ValueError) as error:
        raise DiscoveryError(
            f"Unexpected search-API count shape ({error}). Raw response: {response.text[:1000]}"
        ) from error


def extract_csv(archive: Path, destination_dir: Path) -> Path:
    """Extract the single CSV member. The archive itself stays as the landed artifact."""
    with zipfile.ZipFile(archive) as bundle:
        members = [name for name in bundle.namelist() if name.lower().endswith(".csv")]
        if len(members) != 1:
            raise DiscoveryError(f"Expected exactly one CSV in {archive.name}, found: {members}")
        member = members[0]
        expected = bundle.getinfo(member).file_size
        target = destination_dir / Path(member).name
        if target.exists() and target.stat().st_size == expected:
            log(f"reusing extracted {target.name} ({human_bytes(expected)})")
            return target
        log(f"extracting {member} ({human_bytes(expected)} uncompressed)")
        with bundle.open(member) as source, target.open("wb") as sink:
            shutil.copyfileobj(source, sink, length=CHUNK_BYTES)
    return target


# ── orchestration ─────────────────────────────────────────────────────────────


def run(mode: str, force: bool = False) -> int:
    data_root, db_path = paths_for(mode)
    landing_dir = data_root / "landing" / TRACK / SOURCE
    raw_dir = data_root / "raw" / TRACK / SOURCE
    db_path.parent.mkdir(parents=True, exist_ok=True)

    landing_files: list[dict[str, Any]] = []
    source_count: int | None = None
    source_michigan: int | None = None

    if mode == "fixture":
        csv_path = REPO_ROOT / "tests" / "fixtures" / TRACK / f"{SOURCE}.csv"
        if not csv_path.exists():
            log(f"FAIL: fixture missing at {csv_path}")
            return 1
        meta = json.loads(
            (REPO_ROOT / "tests" / "fixtures" / TRACK / f"{SOURCE}.metadata.json").read_text(
                encoding="utf-8"
            )
        )
        resolved_url = "fixture://" + csv_path.name
        http_metadata = {
            "content_length": csv_path.stat().st_size,
            "last_modified": meta.get("last_modified"),
            "etag": meta.get("etag"),
        }
        source_count = meta.get("source_reported_count")
        source_michigan = meta.get("source_reported_michigan")
        landing_files.append(
            {
                "name": csv_path.name,
                "bytes": csv_path.stat().st_size,
                "sha256": sha256_of(csv_path),
            }
        )
        log("fixture mode: discovery and download SKIPPED (offline)")
    else:
        with http_client(read_timeout=600.0) as client:
            log(f"discovering bulk archive from {DATA_PAGE}")
            resolved_url = discover_bulk_url(client)
            http_metadata = head_metadata(client, resolved_url)
            log(
                f"resolved {resolved_url} "
                f"({human_bytes(http_metadata['content_length'])}, "
                f"last-modified {http_metadata['last_modified']})"
            )

            existing = read_existing_manifest(raw_dir)
            if (
                not force
                and existing
                and existing.get("source_last_refresh") == http_metadata["last_modified"]
            ):
                log("SKIPPED (current): manifest matches the archive's last-modified header")
                return 0

            source_count = source_reported_count(client)
            source_michigan = source_reported_count(client, state="MI")
            log(f"search API reports {source_count:,} complaints ({source_michigan:,} Michigan)")

            write_baseline(
                TRACK,
                SOURCE,
                {
                    "note": "CFPB publishes no machine-readable field metadata; this is a "
                    "pointer file, per CLAUDE.md rule 8.",
                    "field_reference": "https://cfpb.github.io/api/ccdb/fields.html",
                    "api_reference": "https://cfpb.github.io/api/ccdb/api.html",
                    "release_notes": "https://cfpb.github.io/api/ccdb/release-notes.html",
                    "data_page": DATA_PAGE,
                    "retrieved_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                },
                kind="pointer",
            )

            archive = landing_dir / resolved_url.rsplit("/", 1)[-1]
            expected_bytes = http_metadata["content_length"]
            # A failure downstream of the download should not cost another 1.4 GB.
            if archive.exists() and archive.stat().st_size == expected_bytes:
                log(f"reusing landed archive {archive.name} ({human_bytes(expected_bytes)})")
                archive_bytes, archive_sha = expected_bytes, sha256_of(archive)
            else:
                log(f"downloading -> {archive}")
                archive_bytes, archive_sha = stream_download(
                    client, resolved_url, archive, log=log, progress_every=PROGRESS_EVERY_BYTES
                )
            log(f"landed {human_bytes(archive_bytes)}  sha256={archive_sha[:16]}...")
            landing_files.append(
                {"name": archive.name, "bytes": archive_bytes, "sha256": archive_sha}
            )

            csv_path = extract_csv(archive, landing_dir)
            landing_files.append(
                {"name": csv_path.name, "bytes": csv_path.stat().st_size, "sha256": None}
            )

    con = duckdb.connect(str(db_path))
    try:
        con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
        con.execute(f"SET temp_directory={sql_literal(data_root / '.duckdb_tmp')}")

        columns = [
            row[0]
            for row in con.execute(f"DESCRIBE SELECT * FROM {source_relation(csv_path)}").fetchall()
        ]
        date_column = find_date_column(columns)
        log(f"{len(columns)} columns; partitioning on {date_column!r}")

        log(f"converting to raw parquet -> {raw_dir}")
        rows_landing, rows_raw = repartition_to_raw(
            con,
            relation=source_relation(csv_path),
            raw_dir=raw_dir,
            partition_column=PARTITION_COLUMN,
            projection=f'*, substr("{date_column}", 1, 4) AS {PARTITION_COLUMN}',
        )
        counts = partition_counts(con, raw_dir, PARTITION_COLUMN)
        rows_duckdb = load_table(con, TABLE, raw_dir)
    finally:
        con.close()

    manifest = manifest_payload(
        resolved_url=resolved_url,
        http_metadata=http_metadata,
        landing_files=landing_files,
        source_reported_count=source_count,
        source_reported_michigan=source_michigan,
        date_column=date_column,
        rows_landing=rows_landing,
        rows_raw=rows_raw,
        rows_duckdb=rows_duckdb,
        partition_rows=counts,
        retrieved_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    )
    write_json(raw_dir / "manifest.json", manifest)

    span = f"{min(counts, default='-')} .. {max(counts, default='-')}"
    log(f"partitions: {len(counts)} years  ({span})")
    if manifest["nonstandard_partition_keys"]:
        detail = ", ".join(
            f"{key!r}={counts[key]:,}" for key in manifest["nonstandard_partition_keys"]
        )
        log(f"NOTE  partition keys that are not plausible years, kept and counted: {detail}")
    log(f"landing {rows_landing:,} -> raw {rows_raw:,} -> {TABLE} {rows_duckdb:,}")
    if source_count:
        log(f"source-reported {source_count:,}  (gap {source_count - rows_duckdb:+,})")
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
        help="re-download even when the manifest matches the archive's last-modified header",
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
