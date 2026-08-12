"""Rebuild every DuckDB raw table from local parquet. No network, ever.

Raw parquet is canonical -- that is the whole reason the landing/raw split exists (CLAUDE.md
section 3). A deleted or corrupt `bi_portfolio.duckdb` is therefore a five-minute local
rebuild, not a re-download of gigabytes from six publishers, and this is the recipe that
makes that true.

It imports no fetcher and opens no HTTP client, so there is no path by which running it can
reach a publisher. What it rebuilds is exactly what `ingest.registry` declares.

Every table is checked against the `rows_duckdb` its manifest recorded when the data was
fetched. That makes the rebuild self-verifying without rewriting a single manifest: a table
that comes back a different size than the run that created it means raw and the manifest
disagree, which is a failure, not a cosmetic difference. Sources whose raw tree is absent are
skipped and named -- an un-ingested source is not an error.

Run:  python -m ingest.reload [--track TRACK] [--mode live|fixture]
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb
from ingest.common import (
    load_table,
    make_logger,
    paths_for,
    read_existing_manifest,
    sql_literal,
)
from ingest.registry import TRACKS, RawSource, sources_for

PASS, FAIL, SKIP = "PASS", "FAIL", "SKIP"

# The largest table is 17M rows of CFPB narrative: let DuckDB spill rather than exhaust RAM.
DUCKDB_MEMORY_LIMIT = "4GB"

log = make_logger("platform", "reload")


@dataclass(frozen=True)
class ReloadResult:
    spec: RawSource
    status: str
    detail: str


def has_parquet(raw_dir: Path) -> bool:
    """Something to rebuild from. An empty directory is not a half-ingested source."""
    return raw_dir.is_dir() and any(raw_dir.glob("**/*.parquet"))


def reload_source(con: duckdb.DuckDBPyConnection, spec: RawSource, data_root: Path) -> ReloadResult:
    raw_dir = data_root / "raw" / spec.track / spec.source
    if not has_parquet(raw_dir):
        return ReloadResult(spec, SKIP, f"no parquet under {raw_dir}; not ingested yet")

    rows = load_table(con, spec.table, raw_dir)

    manifest = read_existing_manifest(raw_dir)
    recorded = None if manifest is None else manifest.get("rows_duckdb")
    if recorded is None:
        return ReloadResult(spec, PASS, f"{rows:,} rows (no manifest count to check against)")
    if rows != recorded:
        return ReloadResult(
            spec,
            FAIL,
            f"{rows:,} rows from parquet but the manifest recorded {recorded:,} "
            f"(gap {rows - recorded:+,})",
        )
    return ReloadResult(spec, PASS, f"{rows:,} rows, matching the manifest")


def run(mode: str, track: str) -> int:
    data_root, db_path = paths_for(mode)
    selected = sources_for(track)
    if not selected:
        log(f"no sources registered for track {track!r}")
        return 0

    db_path.parent.mkdir(parents=True, exist_ok=True)
    log(f"rebuilding {len(selected)} table(s) from {data_root / 'raw'} -- no network")

    results: list[ReloadResult] = []
    con = duckdb.connect(str(db_path))
    try:
        con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
        con.execute(f"SET temp_directory={sql_literal(data_root / '.duckdb_tmp')}")
        for spec in selected:
            result = reload_source(con, spec, data_root)
            results.append(result)
            log(f"  {result.status:<4} {result.spec.table:<32} {result.detail}")
    finally:
        con.close()

    failed = [result for result in results if result.status == FAIL]
    skipped = [result for result in results if result.status == SKIP]
    log(
        f"{len(results) - len(failed) - len(skipped)} rebuilt, "
        f"{len(skipped)} skipped, {len(failed)} failed"
    )
    return 1 if failed else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--track", choices=[*TRACKS, "all"], default="all")
    parser.add_argument(
        "--mode",
        choices=("live", "fixture"),
        default=os.environ.get("DATA_MODE", "live"),
        help="which data root and database to rebuild (default: $DATA_MODE or live)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return run(args.mode, args.track)


if __name__ == "__main__":
    sys.exit(main())
