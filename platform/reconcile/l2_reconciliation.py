"""L2 reconciliation: has the schema moved, and does the semantic layer lose anything?

L1 proves raw is a faithful copy of what the publisher served. L2 asks the two questions L1
deliberately does not, and CLAUDE.md rule 8 defers here by name.

    schema drift            the committed baseline's field roster against the columns
                            actually in raw -- a publisher adding, dropping or renaming a
                            field shows up as a failure instead of as a silent NULL column
                            three models downstream

    staging conservation    raw row counts against staging row counts, all thirteen
                            sources. Nothing covered this gap: L1 stops at raw, and the
                            dbt singular tests start at staging, so a stray WHERE in a
                            typed view could drop rows and no check in the repo would
                            notice

Both run offline, in either mode, which is what lets CI carry them. In fixture mode the
comparison is between two committed things -- the baseline and the fixture that raw is built
from -- so a fixture regenerated after a publisher changed shape, without its baseline being
refreshed, fails here rather than being discovered by a confused reader later.

Drift is not uniformly detectable, and this refuses to pretend otherwise. Nine sources give a
full field roster, two give the variables we request, one gives only a column count, and two
give nothing machine-readable at all. The last pair reports SKIP with the reason, never PASS
-- the same rule L1 follows, for the same reason.

Two of the nine are BLS, which publishes no field metadata either: its roster is read from the
download itself, the way `ingest.fintech.hmda` records one. A roster observed in the file is
weaker evidence than a publisher's own schema endpoint -- it cannot tell an intended rename
from a mistake -- but it is the difference between detecting a change and not.

Run:  python -m reconcile.l2_reconciliation [--track TRACK] [--mode live|fixture]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
from ingest.common import BASELINE_DIR, paths_for
from ingest.registry import SOURCES as RAW_SOURCES
from ingest.registry import RawSource
from reconcile.results import FAIL, PASS, SKIP, WARN, Result

# Socrata publishes two synthetic geography columns in a dataset's view metadata that never
# appear in the data export. CDC's baseline therefore carries 33 fields where raw carries 31,
# and the difference is the publisher's UI rather than a schema change.
SOCRATA_COMPUTED_PREFIX = ":@computed_region_"


def openfema_names(payload: Any) -> set[str]:
    """OpenFEMA's field metadata: one object per field, `name` is the column."""
    return {field["name"] for field in payload}


def socrata_names(payload: Any) -> set[str]:
    """Socrata's column metadata. `fieldName` is the data column; `name` is its display
    title, which matches nothing in raw -- all 33 of them, if it were used by mistake."""
    return {
        field["fieldName"]
        for field in payload
        if not field["fieldName"].startswith(SOCRATA_COMPUTED_PREFIX)
    }


def key_names(key: str) -> Callable[[Any], set[str]]:
    """A pointer baseline that records the roster under one key."""

    def extract(payload: Any) -> set[str]:
        return set(payload[key])

    return extract


@dataclass(frozen=True)
class DriftSpec:
    """How one source's drift can be checked, and where it cannot be.

    `comparison` is the honest part. `exact` means the baseline is the whole roster and any
    difference either way is drift. `subset` means the baseline records only the variables we
    ask a publisher for -- ACS returns those plus the geography keys it always adds -- so the
    check is that every requested variable is still served. `count` is all HMDA affords.
    `none` is a source with no machine-readable roster, and it reports SKIP.
    """

    source: str
    baseline: str | None
    extract: Callable[[Any], Any] | None
    comparison: str
    reason: str = ""


DRIFT: tuple[DriftSpec, ...] = (
    DriftSpec("nfip_claims", "insurance__nfip_claims__fields", openfema_names, "exact"),
    DriftSpec("nfip_policies", "insurance__nfip_policies__fields", openfema_names, "exact"),
    DriftSpec("fema_declarations", "insurance__fema_declarations__fields", openfema_names, "exact"),
    DriftSpec(
        "cfpb_complaints",
        "fintech__cfpb_complaints__pointer",
        None,
        "none",
        "CFPB publishes no machine-readable field metadata; the baseline is a documentation "
        "pointer, so a rename can only be caught by reading the release notes it links",
    ),
    DriftSpec(
        "hmda_lar",
        "fintech__hmda_lar__pointer",
        lambda payload: payload["columns_observed"],
        "count",
        "",
    ),
    DriftSpec(
        "hmda_institutions",
        "fintech__hmda_institutions__pointer",
        key_names("fields_observed"),
        "exact",
    ),
    DriftSpec("cdc_healthy_aging", "health__cdc_healthy_aging__fields", socrata_names, "exact"),
    DriftSpec(
        "cms_geographic_variation",
        "health__cms_geographic_variation__pointer",
        None,
        "none",
        "CMS publishes the Geographic Variation data dictionary as a PDF beside the file "
        "rather than as machine-readable field metadata; the baseline is a pointer",
    ),
    DriftSpec(
        "acs5_detailed",
        "shared__acs5_detailed__pointer",
        key_names("variables_requested"),
        "subset",
    ),
    DriftSpec(
        "acs5_subject", "shared__acs5_subject__pointer", key_names("variables_requested"), "subset"
    ),
    DriftSpec("cpi_u", "shared__cpi_u__pointer", key_names("columns_observed"), "exact"),
    DriftSpec(
        "cpi_u_series", "shared__cpi_u_series__pointer", key_names("columns_observed"), "exact"
    ),
    DriftSpec(
        "zip_county_crosswalk",
        "shared__zip_county_crosswalk__pointer",
        key_names("row_fields"),
        "exact",
    ),
)

DRIFT_BY_SOURCE = {spec.source: spec for spec in DRIFT}


def raw_columns(con: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    schema, _, name = table.partition(".")
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ?",
        [schema, name],
    ).fetchall()
    return {column for (column,) in rows}


def staging_relation(spec: RawSource) -> str:
    """`raw.ins_nfip_claims` -> `stg.stg_ins__nfip_claims`.

    Derived rather than listed, because the naming rule is CLAUDE.md's own: raw tables are
    track-prefixed, and staging mirrors the prefix with a double underscore. A source whose
    staging model is named anything else would fail here, which is the intent.
    """
    prefix, _, name = spec.table.removeprefix("raw.").partition("_")
    return f"stg.stg_{prefix}__{name}"


def load_baseline(spec: DriftSpec) -> Any | None:
    if spec.baseline is None:
        return None
    path: Path = BASELINE_DIR / f"{spec.baseline}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def check_schema_drift(con: duckdb.DuckDBPyConnection, source: RawSource) -> list[Result]:
    """The job CLAUDE.md rule 8 defers to L2 by name."""
    spec = DRIFT_BY_SOURCE.get(source.source)
    if spec is None:
        return [
            Result(
                source.track,
                source.source,
                "schema drift",
                FAIL,
                "no drift specification -- a source was registered without deciding how its "
                "schema can be checked",
            )
        ]

    columns = raw_columns(con, source.table)
    if not columns:
        return [
            Result(source.track, source.source, "schema drift", SKIP, f"{source.table} not loaded")
        ]

    if spec.comparison == "none":
        return [Result(source.track, source.source, "schema drift", SKIP, spec.reason)]

    payload = load_baseline(spec)
    if payload is None:
        return [
            Result(
                source.track,
                source.source,
                "schema drift",
                FAIL,
                f"baseline {spec.baseline}.json is missing; rule 8 requires it committed",
            )
        ]

    assert spec.extract is not None  # every non-'none' comparison declares one
    expected = spec.extract(payload)

    if spec.comparison == "count":
        if expected == len(columns):
            return [
                Result(
                    source.track,
                    source.source,
                    "schema drift",
                    PASS,
                    f"{len(columns)} columns, matching the baseline's recorded count "
                    "(a count is all this publisher affords)",
                )
            ]
        return [
            Result(
                source.track,
                source.source,
                "schema drift",
                FAIL,
                f"baseline recorded {expected} columns, raw now has {len(columns)}",
            )
        ]

    missing = sorted(expected - columns)
    if spec.comparison == "subset":
        if not missing:
            return [
                Result(
                    source.track,
                    source.source,
                    "schema drift",
                    PASS,
                    f"all {len(expected)} requested variable(s) still served; "
                    f"raw adds {len(columns) - len(expected)} geography and vintage keys",
                )
            ]
        return [
            Result(
                source.track,
                source.source,
                "schema drift",
                FAIL,
                f"publisher no longer serves: {', '.join(missing)}",
            )
        ]

    added = sorted(columns - expected)
    if not missing and not added:
        return [
            Result(
                source.track,
                source.source,
                "schema drift",
                PASS,
                f"{len(columns)} columns, identical to the committed baseline",
            )
        ]

    detail = []
    if missing:
        detail.append(f"dropped or renamed: {', '.join(missing)}")
    if added:
        detail.append(f"new since the baseline: {', '.join(added)}")
    # A dropped column can break a model silently; a new one cannot, and the repo's rule is
    # to keep publishers' additions rather than to fail on them.
    status = FAIL if missing else WARN
    return [Result(source.track, source.source, "schema drift", status, "; ".join(detail))]


def check_staging_conservation(con: duckdb.DuckDBPyConnection, source: RawSource) -> list[Result]:
    """Every raw row must reach staging.

    Staging is a rename-and-cast over raw: it types columns, adds derived flags and renames,
    and it never filters. That is a real invariant and nothing asserted it before -- L1 stops
    at raw and the dbt tests start at staging, so the one join between them was unwatched.
    """
    relation = staging_relation(source)
    schema, _, name = relation.partition(".")
    exists = con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [schema, name],
    ).fetchone()[0]
    if not exists:
        return [
            Result(
                source.track,
                source.source,
                "staging conserves rows",
                SKIP,
                f"{relation} does not exist; run `just dbt build` first",
            )
        ]

    raw_rows = con.execute(f"SELECT count(*) FROM {source.table}").fetchone()[0]
    staged_rows = con.execute(f"SELECT count(*) FROM {relation}").fetchone()[0]
    if raw_rows == staged_rows:
        return [
            Result(
                source.track,
                source.source,
                "staging conserves rows",
                PASS,
                f"{raw_rows:,} = {staged_rows:,}",
            )
        ]
    # Spelled out rather than signed. A dropped row rendered as `(+1)` -- raw minus staging --
    # reads as though staging gained one, which is the opposite of what happened.
    lost = raw_rows - staged_rows
    movement = f"{abs(lost):,} row(s) {'dropped' if lost > 0 else 'added'}"
    return [
        Result(
            source.track,
            source.source,
            "staging conserves rows",
            FAIL,
            f"raw {raw_rows:,} but {relation} has {staged_rows:,} -- {movement}; "
            "staging types and renames, it never filters",
        )
    ]


def check_source(con: duckdb.DuckDBPyConnection, source: RawSource) -> list[Result]:
    return check_schema_drift(con, source) + check_staging_conservation(con, source)


def report(results: list[Result]) -> int:
    """Same shape as L1's, so two harnesses read the same way in a terminal."""
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
                print(f"    {icons[result.status]:<5} {result.check:<24} {result.detail}")

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
        "--track", choices=["insurance", "fintech", "health", "shared", "all"], default="all"
    )
    parser.add_argument(
        "--mode", choices=("live", "fixture"), default=os.environ.get("DATA_MODE", "live")
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    selected = [s for s in RAW_SOURCES if args.track in ("all", s.track)]
    if not selected:
        print(f"no sources registered for track {args.track!r}")
        return 0

    _, db_path = paths_for(args.mode)
    if not db_path.exists():
        print(f"no warehouse at {db_path}; run `just reload` or `just ci` first")
        return 1

    print(f"L2 reconciliation  ({args.mode} mode, {len(selected)} source(s))")
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        results: list[Result] = []
        for source in selected:
            results += check_source(con, source)
        return report(results)
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
