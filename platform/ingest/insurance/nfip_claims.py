"""Ingest the FEMA NFIP redacted claims bulk file into DuckDB's raw schema.

National scope, partitioned by state. Territories and null-state rows are kept and
counted, never dropped: FEMA's published national totals include them, and silently
excluding Puerto Rico is the classic way to miss those totals. Nothing here filters
to Michigan -- see CLAUDE.md section 4.

Run:  python -m ingest.insurance.nfip_claims [--mode live|fixture]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
from ingest.common import (
    HIVE_NULL_PARTITION,
    REPO_ROOT,
    DiscoveryError,
    base_manifest,
    http_client,
    load_state_codes,
    load_table,
    make_logger,
    nonstandard_partitions,
    partition_counts,
    paths_for,
    read_existing_manifest,
    repartition_to_raw,
    sha256_of,
    sql_literal,
    stream_download,
    write_json,
)
from ingest.openfema import (
    OPENFEMA_DATASETS,
    deprecation_notice,
    discover_dataset,
    select_distribution,
    snapshot_field_baseline,
)

TRACK = "insurance"
SOURCE = "nfip_claims"
DATASET = "NfipClaims"
TABLE = "raw.ins_nfip_claims"
PARTITION_COLUMN = "state"

log = make_logger(TRACK, SOURCE)


# ── pure helpers (unit-tested without a network) ───────────────────────────────


def source_relation(landing: Path, distribution_format: str) -> str:
    """A DuckDB relation over the landed file, with no type inference for CSV."""
    if distribution_format == "parquet":
        return f"read_parquet({sql_literal(landing)})"
    # The all-varchar rule: government CSVs carry sentinels and suppression markers
    # that type inference destroys. Typing is session 2's job, column by column.
    return f"read_csv({sql_literal(landing)}, all_varchar=true, header=true)"


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
    return {
        **base_manifest(
            track=TRACK,
            source=SOURCE,
            publisher="OpenFEMA",
            dataset_name=dataset.get("name"),
            resolved_url=resolved_url,
            distribution_format=distribution_format,
            retrieved_at=retrieved_at,
            landing_files=[
                {"name": landing_file, "bytes": landing_bytes, "sha256": landing_sha256}
            ],
            source_reported_count=dataset.get("recordCount"),
            source_last_refresh=dataset.get("lastDataSetRefresh"),
            duckdb_table=TABLE,
            rows_landing=rows_landing,
            rows_raw=rows_raw,
            rows_duckdb=rows_duckdb,
            partition_column=PARTITION_COLUMN,
            partition_rows=partition_rows,
            # NFIP uses the literal code 'UN' for claims whose state is unavailable --
            # roughly 16k rows, mostly pre-1990 with `reportedCity` set to "Currently
            # Unavailable". Real claims, kept in raw, and named in the manifest so they
            # cannot vanish into a failed state join in session 2.
            nonstandard_partitions=nonstandard_partitions(partition_rows, known_partition_keys),
        ),
        "dataset_version": dataset.get("version"),
        "dataset_identifier": dataset.get("identifier"),
        # Two timestamps: L1 uses the pair to tell a refresh lag from a real defect.
        "source_metadata_refresh": dataset.get("lastRefresh"),
        "source_hash": dataset.get("hash"),
        "deprecation": (
            None
            if not dataset.get("depDate")
            else {"removal_date": dataset["depDate"], "replacement": dataset.get("depNewURL")}
        ),
    }


# ── orchestration ─────────────────────────────────────────────────────────────


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
        landing_sha256 = sha256_of(landing)
        log("fixture mode: discovery and download SKIPPED (offline)")
    else:
        with http_client() as client:
            log(f"discovering {DATASET} from {OPENFEMA_DATASETS}")
            dataset = discover_dataset(client, DATASET)

            notice = deprecation_notice(dataset)
            if notice:
                log(f"WARNING  {notice}")

            distribution_format, resolved_url = select_distribution(dataset)
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

            snapshot_field_baseline(
                client, track=TRACK, source=SOURCE, dataset_name=DATASET, log=log
            )

            landing = landing_dir / resolved_url.rsplit("/", 1)[-1]
            log(f"downloading -> {landing}")
            landing_bytes, landing_sha256 = stream_download(client, resolved_url, landing, log=log)
            log(f"landed {landing_bytes / 1e6:.1f} MB  sha256={landing_sha256[:16]}...")

    con = duckdb.connect(str(db_path))
    try:
        log(f"repartitioning by {PARTITION_COLUMN} -> {raw_dir}")
        rows_landing, rows_raw = repartition_to_raw(
            con,
            relation=source_relation(landing, distribution_format),
            raw_dir=raw_dir,
            partition_column=PARTITION_COLUMN,
        )
        counts = partition_counts(con, raw_dir, PARTITION_COLUMN)
        rows_duckdb = load_table(con, TABLE, raw_dir)
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
