"""Ingest FEMA NFIP policies for Michigan into DuckDB's raw schema.

This is the one source in the repo whose RAW scope is Michigan rather than national,
and the reason is size, not analysis: the national file is 74.3M policy-term records
(500MB-10GB) against roughly 384k for Michigan. National policy and premium
benchmarks for INS-E4 come from FEMA's published state-level statistics as
reconciliation control totals, not from raw. That exception is recorded here and in
the project README; it does not license filtering any other national source.

Two things about this endpoint that the retrieval strategy is built around:

  * `$skip` paging collapses. The server re-scans and discards N rows per request, so
    cost grows with depth: 12.7s at skip=0, 232.3s at skip=80,000, and the connection
    dropped outright at skip=110,000 -- which is only 29% of Michigan. We page by
    keyset instead (`id gt <last>` with `$orderby=id`), which is an indexed range scan
    at every depth and finished all 384k rows with no page over 29s.
  * `$count` with a broad `$filter` is refused. A selective filter counts fine and the
    unfiltered count is instant, but a filter selecting many rows times out server-side
    after 60s and returns 503. So this source has NO source-reported total to reconcile
    against, and the paging invariants below are the completeness evidence instead.

Run:  python -m ingest.insurance.nfip_policies [--mode live|fixture]
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import httpx
import pyarrow.parquet as pq
from ingest.common import (
    PAGE_SLEEP_SECONDS,
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
    skip_as_current,
    sql_literal,
    write_json,
)
from ingest.openfema import deprecation_notice, discover_dataset, snapshot_field_baseline

TRACK = "insurance"
SOURCE = "nfip_policies"
DATASET = "NfipPolicies"
TABLE = "raw.ins_nfip_policies"
PARTITION_COLUMN = "propertyState"
SCOPE_STATE = "MI"

POLICIES_API = "https://www.fema.gov/api/open/v3/NfipPolicies"
PAGE_SIZE = 10000
LANDING_GLOB = "page-*.parquet"

# Field names that could plausibly carry the property's state. Claims calls it `state`;
# policies calls it `propertyState`. Resolved from the publisher's field metadata at
# runtime rather than hardcoded, because they already disagree once.
STATE_FIELD_CANDIDATES = ("propertyState", "state")

log = make_logger(TRACK, SOURCE)


# ── pure helpers (unit-tested without a network) ───────────────────────────────


def resolve_state_field(field_names: list[str]) -> str:
    """Pick the property-state field from the publisher's own field metadata.

    NfipClaims exposes `state`; NfipPolicies exposes `propertyState`. Guessing either
    one produces a filter that silently matches nothing, so we look it up and fail
    loudly when neither candidate is present.
    """
    for candidate in STATE_FIELD_CANDIDATES:
        if candidate in field_names:
            return candidate
    near = sorted(name for name in field_names if "state" in name.lower())
    raise DiscoveryError(
        f"No property-state field on {DATASET}. Tried {list(STATE_FIELD_CANDIDATES)}; "
        f"fields containing 'state' were {near}"
    )


def verify_paging(pages: list[int], ids: list[int], page_size: int = PAGE_SIZE) -> dict[str, Any]:
    """Completeness evidence for a source that refuses to tell us its own total.

    With no `$count` to reconcile against, these invariants are what stands between a
    complete pull and a silently truncated one. An empty-page check alone would have
    accepted the 110,000-row `$skip` run as finished.
    """
    ascending = all(a < b for a, b in zip(ids, ids[1:], strict=False))
    full_except_last = all(size == page_size for size in pages[:-1]) if pages else False
    return {
        "pages": len(pages),
        "rows": len(ids),
        "ids_unique": len(set(ids)) == len(ids),
        "ids_strictly_ascending": ascending,
        "pages_full_except_last": full_except_last,
        "final_page_rows": pages[-1] if pages else 0,
        "id_min": min(ids) if ids else None,
        "id_max": max(ids) if ids else None,
    }


def paging_failures(verification: dict[str, Any]) -> list[str]:
    problems = []
    if not verification["ids_unique"]:
        problems.append("duplicate ids across pages: $skip or ordering re-served rows")
    if not verification["ids_strictly_ascending"]:
        problems.append("ids not strictly ascending: page boundaries are not ordered")
    if not verification["pages_full_except_last"]:
        problems.append("a non-final page came back short: the server truncated a page")
    if verification["final_page_rows"] == PAGE_SIZE:
        problems.append("final page was exactly full: the pull may have stopped early")
    return problems


def manifest_payload(
    *,
    dataset: dict[str, Any],
    state_field: str,
    landing_files: list[dict[str, Any]],
    verification: dict[str, Any],
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
            publisher="OpenFEMA",
            dataset_name=dataset.get("name"),
            resolved_url=POLICIES_API,
            distribution_format="parquet",
            retrieved_at=retrieved_at,
            landing_files=landing_files,
            # The publisher refuses a filtered count on this dataset; see below.
            source_reported_count=None,
            source_last_refresh=dataset.get("lastDataSetRefresh"),
            duckdb_table=TABLE,
            rows_landing=rows_landing,
            rows_raw=rows_raw,
            rows_duckdb=rows_duckdb,
            partition_column=PARTITION_COLUMN,
            partition_rows=partition_rows,
            # Raw is deliberately MI-only here, so any other partition is a scope leak.
            nonstandard_partitions={
                key: rows for key, rows in partition_rows.items() if key != SCOPE_STATE
            },
        ),
        "dataset_version": dataset.get("version"),
        "dataset_identifier": dataset.get("identifier"),
        "source_metadata_refresh": dataset.get("lastRefresh"),
        "source_hash": dataset.get("hash"),
        "source_reported_national": dataset.get("recordCount"),
        "source_count_unavailable_reason": (
            "$count with a broad $filter times out server-side after 60s and returns "
            "503 on this dataset; a selective filter and the unfiltered count both "
            "succeed. Completeness rests on the paging invariants instead."
        ),
        "raw_scope": {
            "state": SCOPE_STATE,
            "state_field": state_field,
            "reason": (
                "National policies are 74.3M rows; the Michigan slice is ~384k. National "
                "benchmarks for INS-E4 come from FEMA published state-level statistics, "
                "not from raw."
            ),
        },
        "pagination": {
            "strategy": "keyset",
            "order_by": "id",
            "page_size": PAGE_SIZE,
            "why_not_skip": (
                "$skip degrades with depth on this endpoint (12.7s at 0, 232.3s at "
                "80,000) and the connection dropped at skip=110,000 -- 29% of Michigan."
            ),
        },
        "landing_glob": LANDING_GLOB,
        "verification": verification,
    }


# ── network ───────────────────────────────────────────────────────────────────


@retrying
def _policy_page(client: httpx.Client, state_field: str, after_id: int) -> bytes:
    response = client.get(
        POLICIES_API,
        params={
            "$top": str(PAGE_SIZE),
            "$filter": f"{state_field} eq '{SCOPE_STATE}' and id gt {after_id}",
            "$orderby": "id",
            "$format": "parquet",
        },
    )
    raise_for_transient(response)
    return response.content


def download_pages(
    client: httpx.Client, state_field: str, landing_dir: Path
) -> tuple[list[dict[str, Any]], list[int], list[int]]:
    """Keyset-page the Michigan slice, landing each page untouched as its own parquet."""
    landing_dir.mkdir(parents=True, exist_ok=True)
    for stale in landing_dir.glob(LANDING_GLOB):
        stale.unlink()

    landing_files: list[dict[str, Any]] = []
    page_sizes: list[int] = []
    ids: list[int] = []
    after_id = 0

    while True:
        started = time.monotonic()
        payload = _policy_page(client, state_field, after_id)
        elapsed = time.monotonic() - started

        page_ids = pq.read_table(io.BytesIO(payload)).column("id").to_pylist()
        if not page_ids:
            log(f"  page {len(page_sizes) + 1:>2}: empty terminator in {elapsed:.1f}s")
            break

        target = landing_dir / f"page-{len(page_sizes) + 1:05d}.parquet"
        target.write_bytes(payload)
        landing_files.append(
            {"name": target.name, "bytes": len(payload), "sha256": sha256_of(target)}
        )
        page_sizes.append(len(page_ids))
        ids.extend(page_ids)
        log(
            f"  page {len(page_sizes):>2}: {len(page_ids):>5} rows in {elapsed:5.1f}s "
            f"({human_bytes(len(payload))}, after_id={after_id})"
        )

        after_id = page_ids[-1]
        if len(page_ids) < PAGE_SIZE:
            break
        time.sleep(PAGE_SLEEP_SECONDS)

    return landing_files, page_sizes, ids


# ── orchestration ─────────────────────────────────────────────────────────────


def run(mode: str, force: bool = False) -> int:
    data_root, db_path = paths_for(mode)
    landing_dir = data_root / "landing" / TRACK / SOURCE
    raw_dir = data_root / "raw" / TRACK / SOURCE
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if mode == "fixture":
        fixture = REPO_ROOT / "tests" / "fixtures" / TRACK / f"{SOURCE}.parquet"
        if not fixture.exists():
            log(f"FAIL: fixture missing at {fixture}")
            return 1
        dataset = json.loads(
            (REPO_ROOT / "tests" / "fixtures" / TRACK / f"{SOURCE}.metadata.json").read_text(
                encoding="utf-8"
            )
        )
        state_field = PARTITION_COLUMN
        ids = pq.read_table(fixture).column("id").to_pylist()
        page_sizes = [len(ids)]
        landing_files = [
            {"name": fixture.name, "bytes": fixture.stat().st_size, "sha256": sha256_of(fixture)}
        ]
        landing_pattern = fixture
        log("fixture mode: discovery and paging SKIPPED (offline)")
    else:
        with http_client() as client:
            log(f"discovering {DATASET}")
            dataset = discover_dataset(client, DATASET)
            notice = deprecation_notice(dataset)
            if notice:
                log(f"WARNING  {notice}")
            log(
                f"source reports {dataset.get('recordCount'):,} national rows, "
                f"last refreshed {dataset.get('lastDataSetRefresh')}"
            )

            existing = read_existing_manifest(raw_dir)
            if skip_as_current(
                force=force,
                publisher_unchanged=bool(existing)
                and existing.get("source_last_refresh") == dataset.get("lastDataSetRefresh"),
                db_path=db_path,
                tables=(TABLE,),
                log=log,
            ):
                log("SKIPPED (current): manifest matches the publisher's refresh timestamp")
                return 0

            fields = snapshot_field_baseline(
                client, track=TRACK, source=SOURCE, dataset_name=DATASET, log=log
            )
            state_field = resolve_state_field([field["name"] for field in fields])
            log(f"state field is {state_field!r}")

            log(f"keyset paging {SCOPE_STATE} at {PAGE_SIZE:,} rows/page")
            landing_files, page_sizes, ids = download_pages(client, state_field, landing_dir)
            landing_pattern = landing_dir / LANDING_GLOB

    verification = verify_paging(page_sizes, ids)
    problems = paging_failures(verification)
    if problems:
        for problem in problems:
            log(f"FAIL  {problem}")
        return 1
    log(
        f"paging verified: {verification['rows']:,} rows over {verification['pages']} pages, "
        f"ids unique and strictly ascending, final page {verification['final_page_rows']:,}"
    )

    con = duckdb.connect(str(db_path))
    try:
        log(f"repartitioning by {PARTITION_COLUMN} -> {raw_dir}")
        rows_landing, rows_raw = repartition_to_raw(
            con,
            relation=f"read_parquet({sql_literal(landing_pattern)})",
            raw_dir=raw_dir,
            partition_column=PARTITION_COLUMN,
        )
        counts = partition_counts(con, raw_dir, PARTITION_COLUMN)
        rows_duckdb = load_table(con, TABLE, raw_dir)
    finally:
        con.close()

    manifest = manifest_payload(
        dataset=dataset,
        state_field=state_field,
        landing_files=landing_files,
        verification=verification,
        rows_landing=rows_landing,
        rows_raw=rows_raw,
        rows_duckdb=rows_duckdb,
        partition_rows=counts,
        retrieved_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    )
    write_json(raw_dir / "manifest.json", manifest)

    if manifest["nonstandard_partition_keys"]:
        leaked = manifest["nonstandard_partition_keys"]
        log(f"FAIL  non-Michigan rows in an MI-scoped pull: {leaked}")
        return 1

    log(f"landing {rows_landing:,} -> raw {rows_raw:,} -> {TABLE} {rows_duckdb:,}")
    log(f"michigan policy records: {counts.get(SCOPE_STATE, 0):,}")
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
        help="re-page even when the manifest matches the publisher's refresh timestamp",
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
