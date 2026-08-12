"""Ingest FEMA disaster declarations into DuckDB's raw schema.

National scope, partitioned by state. This is the event dimension INS-E3 stands on:
without it there is no way to split Michigan's paid losses into event-driven versus
attritional, and no way to build the 30/60/90-day post-declaration windows.

The two Michigan anchor events are asserted to exist on every run, and their disaster
numbers are discovered from the data rather than hardcoded -- a hardcoded DR number
is a silent lie the day FEMA reissues or supersedes a declaration. If an anchor
disappears the run fails, because the event drill-downs would quietly return nothing.

Run:  python -m ingest.insurance.fema_declarations [--mode live|fixture]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import duckdb
import httpx
from ingest.common import (
    HIVE_NULL_PARTITION,
    REPO_ROOT,
    DiscoveryError,
    base_manifest,
    http_client,
    human_bytes,
    load_state_codes,
    load_table,
    make_logger,
    nonstandard_partitions,
    partition_counts,
    paths_for,
    raise_for_transient,
    read_existing_manifest,
    repartition_to_raw,
    retrying,
    sha256_of,
    sql_literal,
    stream_download,
    write_json,
)
from ingest.openfema import (
    deprecation_notice,
    discover_dataset,
    select_distribution,
    snapshot_field_baseline,
)

TRACK = "insurance"
SOURCE = "fema_declarations"
DATASET = "DisasterDeclarationsSummaries"
TABLE = "raw.ins_fema_declarations"
PARTITION_COLUMN = "state"

DECLARATIONS_API = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"

log = make_logger(TRACK, SOURCE)


@dataclass(frozen=True)
class AnchorEvent:
    """A Michigan event the insurance narrative depends on being present.

    Described by what it was and when, not by its disaster number, so the check keeps
    working across reissues and tells us the number rather than trusting one.
    """

    key: str
    state: str
    incident_from: str
    incident_to: str
    keywords: tuple[str, ...]
    note: str


ANCHOR_EVENTS: tuple[AnchorEvent, ...] = (
    AnchorEvent(
        key="midland_dam_failures_2020",
        state="MI",
        incident_from="2020-05-01",
        incident_to="2020-06-30",
        keywords=("dam", "flood"),
        note="Edenville and Sanford dam failures, Midland County",
    ),
    AnchorEvent(
        key="southeast_mi_flooding_2021",
        state="MI",
        incident_from="2021-06-01",
        incident_to="2021-07-31",
        keywords=("flood", "storm"),
        note="Detroit metropolitan urban flooding",
    ),
)


# ── anchor assertions ─────────────────────────────────────────────────────────


def find_anchor(con: duckdb.DuckDBPyConnection, anchor: AnchorEvent) -> list[dict[str, Any]]:
    """Locate an anchor by description. Casting here is for the check only, not raw."""
    keyword_sql = " OR ".join(
        f"lower(incidentType) LIKE '%{word}%' OR lower(declarationTitle) LIKE '%{word}%'"
        for word in anchor.keywords
    )
    rows = con.execute(
        f"""
        SELECT DISTINCT disasterNumber, declarationType, incidentType, declarationTitle,
               CAST(incidentBeginDate AS DATE) AS incident_begin,
               CAST(declarationDate AS DATE) AS declared
        FROM {TABLE}
        WHERE state = ?
          AND CAST(incidentBeginDate AS DATE) BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)
          AND ({keyword_sql})
        ORDER BY disasterNumber
        """,
        [anchor.state, anchor.incident_from, anchor.incident_to],
    ).fetchall()
    return [
        {
            "disaster_number": int(row[0]),
            "declaration_type": row[1],
            "incident_type": row[2],
            "declaration_title": row[3],
            "incident_begin": row[4].isoformat(),
            "declaration_date": row[5].isoformat() if row[5] else None,
        }
        for row in rows
    ]


def anchor_failures(found: dict[str, list[dict[str, Any]]]) -> list[str]:
    return [
        f"anchor event {key!r} not found; INS-E3 event windows would return nothing"
        for key, matches in found.items()
        if not matches
    ]


# ── orchestration ─────────────────────────────────────────────────────────────


@retrying
def source_reported_count(client: httpx.Client) -> int:
    """Unfiltered $count. Small table, so this one does not time out the way policies does."""
    response = client.get(DECLARATIONS_API, params={"$count": "true", "$top": "1", "$select": "id"})
    raise_for_transient(response)
    try:
        return int(response.json()["metadata"]["count"])
    except (KeyError, TypeError, ValueError) as error:
        raise DiscoveryError(
            f"Unexpected $count shape ({error}). Raw response: {response.text[:800]}"
        ) from error


def run(mode: str, force: bool = False) -> int:
    data_root, db_path = paths_for(mode)
    landing_dir = data_root / "landing" / TRACK / SOURCE
    raw_dir = data_root / "raw" / TRACK / SOURCE
    db_path.parent.mkdir(parents=True, exist_ok=True)
    source_count: int | None = None

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
        source_count = dataset.get("recordCount")
        landing_bytes, landing_sha256 = landing.stat().st_size, sha256_of(landing)
        log("fixture mode: discovery and download SKIPPED (offline)")
    else:
        with http_client() as client:
            log(f"discovering {DATASET}")
            dataset = discover_dataset(client, DATASET)
            notice = deprecation_notice(dataset)
            if notice:
                log(f"WARNING  {notice}")

            distribution_format, resolved_url = select_distribution(dataset)
            source_count = source_reported_count(client)
            refreshed = dataset.get("lastDataSetRefresh")
            log(f"resolved {distribution_format}: {resolved_url}")
            log(f"API reports {source_count:,} rows, refreshed {refreshed}")

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
            log(f"landed {human_bytes(landing_bytes)}  sha256={landing_sha256[:16]}...")

    con = duckdb.connect(str(db_path))
    try:
        log(f"repartitioning by {PARTITION_COLUMN} -> {raw_dir}")
        rows_landing, rows_raw = repartition_to_raw(
            con,
            relation=f"read_parquet({sql_literal(landing)})",
            raw_dir=raw_dir,
            partition_column=PARTITION_COLUMN,
        )
        counts = partition_counts(con, raw_dir, PARTITION_COLUMN)
        rows_duckdb = load_table(con, TABLE, raw_dir)
        anchors = {anchor.key: find_anchor(con, anchor) for anchor in ANCHOR_EVENTS}
    finally:
        con.close()

    problems = anchor_failures(anchors)
    for problem in problems:
        log(f"FAIL  {problem}")
    for anchor in ANCHOR_EVENTS:
        for match in anchors[anchor.key]:
            log(
                f"anchor {anchor.key}: DR-{match['disaster_number']} "
                f"({match['declaration_type']}, {match['incident_type']}, "
                f"begin {match['incident_begin']})"
            )
    if problems:
        return 1

    manifest = {
        **base_manifest(
            track=TRACK,
            source=SOURCE,
            publisher="OpenFEMA",
            dataset_name=dataset.get("name"),
            resolved_url=resolved_url,
            distribution_format=distribution_format,
            retrieved_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            landing_files=[
                {"name": landing.name, "bytes": landing_bytes, "sha256": landing_sha256}
            ],
            source_reported_count=source_count,
            source_last_refresh=dataset.get("lastDataSetRefresh"),
            duckdb_table=TABLE,
            rows_landing=rows_landing,
            rows_raw=rows_raw,
            rows_duckdb=rows_duckdb,
            partition_column=PARTITION_COLUMN,
            partition_rows=counts,
            nonstandard_partitions=nonstandard_partitions(counts, load_state_codes()),
        ),
        "dataset_version": dataset.get("version"),
        "dataset_identifier": dataset.get("identifier"),
        "source_metadata_refresh": dataset.get("lastRefresh"),
        "source_hash": dataset.get("hash"),
        "anchor_events": {
            anchor.key: {
                "note": anchor.note,
                "described_by": {
                    "state": anchor.state,
                    "incident_between": [anchor.incident_from, anchor.incident_to],
                    "keywords": list(anchor.keywords),
                },
                "matched": anchors[anchor.key],
            }
            for anchor in ANCHOR_EVENTS
        },
        "grain_note": (
            "One row per disaster number x designated area, so a single event spans many "
            "rows. Counting declarations means counting DISTINCT disasterNumber."
        ),
    }
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
    parser.add_argument("--force", action="store_true", help="re-download even when current")
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
