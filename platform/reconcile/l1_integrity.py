"""L1 integrity: prove raw is a faithful copy of what the publisher published.

Deterministic and hand-written -- every check is something a person can restate in a
sentence and defend in an interview. Output is grouped by project track.

The chain we assert, per source:

    source-reported total  ->  landing rows  ->  raw parquet rows  ->  DuckDB table rows

A gap between the publisher's own count and the bulk file it serves is a *timing*
observation, not a defect: publishers compute counts and generate bulk files at
different moments. A gap anywhere downstream of landing is ours, and fails the run.

Run:  python -m reconcile.l1_integrity [--track TRACK] [--mode live|fixture]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import httpx

REPO_ROOT = Path(__file__).resolve().parents[2]

PASS, FAIL, WARN, SKIP = "PASS", "FAIL", "WARN", "SKIP"

HIVE_NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__"
USER_AGENT = "bi-portfolio-pipeline/0.1 (chris.hall309@gmail.com)"
NFIP_API = "https://www.fema.gov/api/open/v3/NfipClaims"

FIRST_COMPLAINT_YEAR = 2011


@dataclass(frozen=True)
class Result:
    track: str
    source: str
    check: str
    status: str
    detail: str


@dataclass
class Profile:
    """One pass over a relation: totals, per-partition counts, and control sums."""

    total: int = 0
    by_partition: dict[str, int] = field(default_factory=dict)
    sums: dict[str, Decimal] = field(default_factory=dict)


@dataclass(frozen=True)
class SourceSpec:
    track: str
    source: str
    table: str
    partition_column: str
    landing_extension: str
    landing_reader: str  # "parquet" or "csv"
    control_sum_columns: tuple[str, ...]
    landing_partition_expr: Callable[[dict[str, Any]], str]
    partition_check: Callable[..., list[Result]]


# ── generic helpers ───────────────────────────────────────────────────────────


def sql_literal(path: Path | str) -> str:
    return "'" + str(path).replace("\\", "/").replace("'", "''") + "'"


def load_state_codes() -> frozenset[str]:
    path = REPO_ROOT / "platform" / "reference" / "state_codes.csv"
    with path.open(encoding="utf-8", newline="") as handle:
        return frozenset(row["state_code"] for row in csv.DictReader(handle))


def paths_for(mode: str) -> tuple[Path, Path]:
    if mode == "fixture":
        return REPO_ROOT / "data" / "_fixture", REPO_ROOT / "platform" / "duckdb" / "fixture.duckdb"
    return REPO_ROOT / "data", REPO_ROOT / "platform" / "duckdb" / "bi_portfolio.duckdb"


def landing_relation(path: Path, reader: str) -> str:
    if reader == "parquet":
        return f"read_parquet({sql_literal(path)})"
    # parallel=false: newlines inside quoted narrative fields defeat range-splitting.
    return f"read_csv({sql_literal(path)}, all_varchar=true, header=true, parallel=false)"


def raw_relation(raw_dir: Path) -> str:
    pattern = raw_dir / "**" / "*.parquet"
    return f"read_parquet({sql_literal(pattern)}, hive_partitioning=true)"


def profile(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    partition_expr: str,
    sum_columns: tuple[str, ...],
) -> Profile:
    """Single scan producing every control total we compare. Scans are expensive; do one."""
    projections = [
        f"COALESCE(CAST({partition_expr} AS VARCHAR), '{HIVE_NULL_PARTITION}') AS k",
        "count(*) AS n",
    ]
    projections += [f'sum("{column}") AS s{i}' for i, column in enumerate(sum_columns)]
    rows = con.execute(f"SELECT {', '.join(projections)} FROM {relation} GROUP BY 1").fetchall()

    result = Profile()
    result.sums = {column: Decimal(0) for column in sum_columns}
    for row in rows:
        key, count = str(row[0]), int(row[1])
        result.by_partition[key] = result.by_partition.get(key, 0) + count
        result.total += count
        for index, column in enumerate(sum_columns):
            value = row[2 + index]
            if value is not None:
                result.sums[column] += Decimal(str(value))
    return result


# ── the checks ────────────────────────────────────────────────────────────────


def classify_source_gap(manifest: dict[str, Any], rows_landing: int) -> tuple[str, str]:
    """Compare the publisher's own count to what its bulk file actually contained.

    Per the working rule: matching refresh timestamps with mismatched counts is a bug;
    differing timestamps make it a timing note. When a publisher exposes only one
    timestamp we cannot prove they match, so we warn rather than fail -- and say so.
    """
    reported = manifest.get("source_reported_count")
    if reported is None:
        return SKIP, "publisher exposes no count for this source"

    gap = reported - rows_landing
    if gap == 0:
        return PASS, f"source-reported {reported:,} = landing {rows_landing:,}"

    data_refresh = manifest.get("source_last_refresh")
    metadata_refresh = manifest.get("source_metadata_refresh")
    if metadata_refresh is None:
        return (
            WARN,
            f"source-reported {reported:,} vs landing {rows_landing:,} (gap {gap:+,}); "
            f"only one refresh timestamp published ({data_refresh}), so bulk-vs-API lag "
            "cannot be ruled out",
        )
    if data_refresh == metadata_refresh:
        return (
            FAIL,
            f"source-reported {reported:,} vs landing {rows_landing:,} (gap {gap:+,}) with "
            f"identical refresh timestamps ({data_refresh}) -- not explainable as timing",
        )
    return (
        WARN,
        f"source-reported {reported:,} vs landing {rows_landing:,} (gap {gap:+,}); refresh "
        f"lag: data {data_refresh}, metadata {metadata_refresh}",
    )


def check_count_chain(
    spec: SourceSpec, manifest: dict[str, Any], landing: Profile, raw: Profile, table_rows: int
) -> list[Result]:
    def result(check: str, status: str, detail: str) -> Result:
        return Result(spec.track, spec.source, check, status, detail)

    status, detail = classify_source_gap(manifest, landing.total)
    results = [result("source -> landing", status, detail)]

    if landing.total == raw.total == table_rows:
        results.append(
            result(
                "landing -> raw -> table",
                PASS,
                f"{landing.total:,} rows unchanged through both conversions",
            )
        )
    else:
        results.append(
            result(
                "landing -> raw -> table",
                FAIL,
                f"landing {landing.total:,} / raw {raw.total:,} / table {table_rows:,} disagree",
            )
        )
    return results


def check_lossless(spec: SourceSpec, landing: Profile, raw: Profile) -> list[Result]:
    """Control totals across the landing -> raw conversion.

    Monetary columns are summed where a sum is meaningful. Where it is not -- CMS and
    CDC publish aggregates over mixed averaging grains, so summing them produces a
    number with no referent -- we compare per-partition counts instead. CFPB has no
    monetary column at all, so it takes the count path too.
    """
    results: list[Result] = []

    if spec.control_sum_columns:
        mismatched = {
            column: (landing.sums[column], raw.sums[column])
            for column in spec.control_sum_columns
            if landing.sums[column] != raw.sums[column]
        }
        if mismatched:
            detail = "; ".join(f"{c}: landing {a} vs raw {b}" for c, (a, b) in mismatched.items())
            results.append(Result(spec.track, spec.source, "control sums", FAIL, detail))
        else:
            total = sum(landing.sums.values())
            results.append(
                Result(
                    spec.track,
                    spec.source,
                    "control sums",
                    PASS,
                    f"{len(spec.control_sum_columns)} monetary columns identical "
                    f"(${total:,.2f} combined)",
                )
            )
    else:
        results.append(
            Result(
                spec.track,
                spec.source,
                "control sums",
                SKIP,
                "no column where a sum is meaningful; per-partition counts used instead",
            )
        )

    if landing.by_partition == raw.by_partition:
        results.append(
            Result(
                spec.track,
                spec.source,
                "per-partition counts",
                PASS,
                f"{len(raw.by_partition)} partitions match row for row",
            )
        )
    else:
        only_landing = set(landing.by_partition) - set(raw.by_partition)
        only_raw = set(raw.by_partition) - set(landing.by_partition)
        differing = {
            key
            for key in set(landing.by_partition) & set(raw.by_partition)
            if landing.by_partition[key] != raw.by_partition[key]
        }
        results.append(
            Result(
                spec.track,
                spec.source,
                "per-partition counts",
                FAIL,
                f"only in landing: {sorted(only_landing)}; only in raw: {sorted(only_raw)}; "
                f"differing counts: {sorted(differing)}",
            )
        )
    return results


def check_state_partitions(
    spec: SourceSpec, raw: Profile, table_rows: int, mode: str = "live"
) -> list[Result]:
    """NFIP: every state and DC present, territories present, unknown-state kept.

    Geographic completeness asks "did ingestion drop a state the publisher publishes?"
    That is only answerable against the full dataset. A committed fixture is a
    stratified sample by construction, so the check is skipped there rather than
    satisfied by padding the sample until it passes -- which would prove nothing about
    production. The logic itself is covered by unit tests.
    """
    known = load_state_codes()
    present = set(raw.by_partition)
    territories = {"AS", "GU", "MP", "PR", "VI"}
    states_and_dc = known - territories
    results: list[Result] = []

    if mode == "fixture":
        for check in ("states + DC present", "territories present"):
            results.append(
                Result(
                    spec.track,
                    spec.source,
                    check,
                    SKIP,
                    "fixture is a stratified sample, not a census; coverage is a live check",
                )
            )
    else:
        missing_states = sorted(states_and_dc - present)
        results.append(
            Result(
                spec.track,
                spec.source,
                "states + DC present",
                FAIL if missing_states else PASS,
                f"missing {missing_states}"
                if missing_states
                else f"all {len(states_and_dc)} present",
            )
        )

        missing_territories = sorted(territories - present)
        results.append(
            Result(
                spec.track,
                spec.source,
                "territories present",
                WARN if missing_territories else PASS,
                f"missing {missing_territories}; national totals will not tie to FEMA"
                if missing_territories
                else f"all {len(territories)} present "
                f"({sum(raw.by_partition[t] for t in territories):,} rows)",
            )
        )

    unknown = {k: v for k, v in raw.by_partition.items() if k not in known}
    results.append(
        Result(
            spec.track,
            spec.source,
            "unknown-state rows kept",
            PASS,
            ", ".join(f"{k}={v:,}" for k, v in sorted(unknown.items())) or "none present",
        )
    )
    results.append(_partition_sum_result(spec, raw, table_rows))
    return results


def check_year_partitions(
    spec: SourceSpec, raw: Profile, table_rows: int, mode: str = "live"
) -> list[Result]:
    """CFPB: an unbroken run of years from the database's first year to the newest."""
    years = sorted(key for key in raw.by_partition if key.isdigit())
    results: list[Result] = []

    if not years:
        results.append(Result(spec.track, spec.source, "year coverage", FAIL, "no year partitions"))
        return results

    expected = {str(year) for year in range(int(years[0]), int(years[-1]) + 1)}
    gaps = sorted(expected - set(years))
    results.append(
        Result(
            spec.track,
            spec.source,
            "year coverage",
            FAIL if gaps else PASS,
            f"missing years {gaps}"
            if gaps
            else f"{years[0]}..{years[-1]} unbroken, {len(years)} partitions",
        )
    )

    if int(years[0]) != FIRST_COMPLAINT_YEAR:
        results.append(
            Result(
                spec.track,
                spec.source,
                "earliest year",
                WARN,
                f"earliest partition {years[0]}, expected {FIRST_COMPLAINT_YEAR}",
            )
        )

    malformed = {k: v for k, v in raw.by_partition.items() if not k.isdigit()}
    results.append(
        Result(
            spec.track,
            spec.source,
            "malformed dates kept",
            PASS,
            ", ".join(f"{k!r}={v:,}" for k, v in sorted(malformed.items())) or "none present",
        )
    )
    results.append(_partition_sum_result(spec, raw, table_rows))
    return results


def _partition_sum_result(spec: SourceSpec, raw: Profile, table_rows: int) -> Result:
    """Nothing fell between partitions on the way into the table."""
    total = sum(raw.by_partition.values())
    return Result(
        spec.track,
        spec.source,
        "partitions sum to table",
        PASS if total == table_rows else FAIL,
        f"{total:,} = {table_rows:,}"
        if total == table_rows
        else f"partitions sum to {total:,} but table holds {table_rows:,}",
    )


def check_nfip_api_spot_counts(
    spec: SourceSpec, raw: Profile, mode: str, states: tuple[str, ...] = ("MI", "LA", "PR")
) -> list[Result]:
    """Ask FEMA directly what it holds for a few states and compare to our partitions."""
    if mode == "fixture":
        return [
            Result(
                spec.track,
                spec.source,
                "API spot counts",
                SKIP,
                "SKIPPED (offline) -- fixture mode never calls a publisher",
            )
        ]

    results: list[Result] = []
    try:
        with httpx.Client(
            timeout=120, headers={"User-Agent": USER_AGENT}, follow_redirects=True
        ) as client:
            for state in states:
                response = client.get(
                    NFIP_API,
                    params={
                        "$count": "true",
                        "$top": "1",
                        "$select": "id",
                        "$filter": f"state eq '{state}'",
                    },
                )
                response.raise_for_status()
                reported = int(response.json()["metadata"]["count"])
                local = raw.by_partition.get(state, 0)
                gap = reported - local
                results.append(
                    Result(
                        spec.track,
                        spec.source,
                        f"API spot count {state}",
                        PASS if gap == 0 else WARN,
                        f"API {reported:,} vs local {local:,}"
                        + ("" if gap == 0 else f" (gap {gap:+,}; bulk file trails the API)"),
                    )
                )
    except Exception as error:  # noqa: BLE001 - a publisher outage must not fail the harness
        results.append(
            Result(spec.track, spec.source, "API spot counts", WARN, f"unreachable: {error}")
        )
    return results


# ── source registry ───────────────────────────────────────────────────────────

SOURCES: tuple[SourceSpec, ...] = (
    SourceSpec(
        track="insurance",
        source="nfip_claims",
        table="raw.ins_nfip_claims",
        partition_column="state",
        landing_extension=".parquet",
        landing_reader="parquet",
        control_sum_columns=(
            "amountPaidOnBuildingClaim",
            "amountPaidOnContentsClaim",
            "amountPaidOnIncreasedCostOfComplianceClaim",
            "netBuildingPaymentAmount",
            "netContentsPaymentAmount",
            "netIccPaymentAmount",
        ),
        landing_partition_expr=lambda manifest: '"state"',
        partition_check=check_state_partitions,
    ),
    SourceSpec(
        track="fintech",
        source="cfpb_complaints",
        table="raw.fin_cfpb_complaints",
        partition_column="year",
        landing_extension=".csv",
        landing_reader="csv",
        control_sum_columns=(),
        landing_partition_expr=lambda manifest: f'substr("{manifest["date_column"]}", 1, 4)',
        partition_check=check_year_partitions,
    ),
)


# ── orchestration ─────────────────────────────────────────────────────────────


def resolve_landing_path(spec: SourceSpec, manifest: dict[str, Any], mode: str, root: Path) -> Path:
    if mode == "fixture":
        return (
            REPO_ROOT / "tests" / "fixtures" / spec.track / f"{spec.source}{spec.landing_extension}"
        )
    names = [
        entry["name"]
        for entry in manifest.get("landing_files", [])
        if entry["name"].lower().endswith(spec.landing_extension)
    ]
    if not names:
        raise FileNotFoundError(
            f"no {spec.landing_extension} landing file recorded in the manifest"
        )
    return root / "landing" / spec.track / spec.source / names[0]


def check_source(spec: SourceSpec, mode: str) -> list[Result]:
    data_root, db_path = paths_for(mode)
    raw_dir = data_root / "raw" / spec.track / spec.source
    manifest_path = raw_dir / "manifest.json"

    if not manifest_path.exists():
        return [Result(spec.track, spec.source, "ingested", SKIP, "no manifest; not ingested yet")]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        table_rows = con.execute(f"SELECT count(*) FROM {spec.table}").fetchone()[0]
        raw = profile(
            con, raw_relation(raw_dir), f'"{spec.partition_column}"', spec.control_sum_columns
        )

        results: list[Result] = []
        landing_path = resolve_landing_path(spec, manifest, mode, data_root)
        if not landing_path.exists():
            results.append(
                Result(
                    spec.track,
                    spec.source,
                    "landing available",
                    SKIP,
                    f"{landing_path.name} reclaimed by `just clean-landing`; "
                    "lossless conversion cannot be re-proved until the next ingest",
                )
            )
            landing = raw  # count-chain checks below degrade to raw-vs-table only
        else:
            started = time.monotonic()
            landing = profile(
                con,
                landing_relation(landing_path, spec.landing_reader),
                spec.landing_partition_expr(manifest),
                spec.control_sum_columns,
            )
            elapsed = time.monotonic() - started
            results.append(
                Result(
                    spec.track,
                    spec.source,
                    "landing available",
                    PASS,
                    f"re-read {landing_path.name} in {elapsed:,.1f}s "
                    f"({landing_path.stat().st_size / 1e9:.2f} GB)",
                )
            )
            results += check_lossless(spec, landing, raw)

        results += check_count_chain(spec, manifest, landing, raw, table_rows)
        results += spec.partition_check(spec, raw, table_rows, mode)
        if spec.source == "nfip_claims":
            results += check_nfip_api_spot_counts(spec, raw, mode)
        return results
    finally:
        con.close()


def report(results: list[Result]) -> int:
    icons = {PASS: "PASS", FAIL: "FAIL", WARN: "WARN", SKIP: "SKIP"}
    tracks: dict[str, list[Result]] = {}
    for result in results:
        tracks.setdefault(result.track, []).append(result)

    for track, track_results in tracks.items():
        print(f"\n{'=' * 78}\n  {track.upper()}\n{'=' * 78}")
        sources: dict[str, list[Result]] = {}
        for result in track_results:
            sources.setdefault(result.source, []).append(result)
        for source, source_results in sources.items():
            print(f"\n  {source}")
            for result in source_results:
                print(f"    {icons[result.status]:<5} {result.check:<26} {result.detail}")

    counts = {
        status: sum(1 for r in results if r.status == status) for status in (PASS, WARN, SKIP, FAIL)
    }
    print(
        f"\n{'-' * 78}\n  {counts[PASS]} passed, {counts[WARN]} warned, "
        f"{counts[SKIP]} skipped, {counts[FAIL]} failed\n{'-' * 78}"
    )
    return 1 if counts[FAIL] else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        # The four tracks from CLAUDE.md section 1, not just the ones registered above.
        # Sources join SOURCES as their PRs land, and `just health` / `just shared` call this
        # with their own track name meanwhile -- main() answers that with "no sources
        # registered" rather than an argparse error on a track the repo genuinely has.
        "--track",
        choices=["insurance", "fintech", "health", "shared", "all"],
        default="all",
    )
    parser.add_argument(
        "--mode", choices=("live", "fixture"), default=os.environ.get("DATA_MODE", "live")
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    selected = [s for s in SOURCES if args.track in ("all", s.track)]
    if not selected:
        print(f"no sources registered for track {args.track!r}")
        return 0

    print(f"L1 integrity  ({args.mode} mode, {len(selected)} source(s))")
    results: list[Result] = []
    for spec in selected:
        results += check_source(spec, args.mode)
    return report(results)


if __name__ == "__main__":
    sys.exit(main())
