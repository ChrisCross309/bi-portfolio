"""L2's own checks, exercised against synthetic warehouses.

The point of testing an integrity harness is that a check which cannot fail is worse than no
check at all: it reports PASS forever and nobody looks again. So every check here is shown
failing on data that should fail it, not only passing on data that should pass.

These build their own in-memory DuckDB rather than reading the real warehouse, so they run in
`just check` with no `data/` and no `platform/duckdb/` present -- the same property that lets
`tests/test_transform_layout.py` catch a domain leak before `dbt build` ever runs.
"""

from __future__ import annotations

import json

import duckdb
import pytest
from ingest.registry import SOURCES as RAW_SOURCES
from ingest.registry import RawSource
from reconcile.l2_reconciliation import (
    DRIFT,
    DRIFT_BY_SOURCE,
    check_schema_drift,
    check_staging_conservation,
    key_names,
    load_baseline,
    openfema_names,
    socrata_names,
    staging_relation,
)
from reconcile.results import FAIL, PASS, SKIP, WARN

CLAIMS = next(s for s in RAW_SOURCES if s.source == "nfip_claims")
CPI_SERIES = next(s for s in RAW_SOURCES if s.source == "cpi_u_series")


def warehouse(raw_columns: list[str], staged_rows: int | None, raw_rows: int = 3):
    """A warehouse carrying one raw table, and optionally its staging view."""
    con = duckdb.connect()
    con.execute("CREATE SCHEMA raw")
    columns = ", ".join(f'"{c}" VARCHAR' for c in raw_columns)
    con.execute(f"CREATE TABLE {CLAIMS.table} ({columns})")
    values = ", ".join(
        ["('x')" if len(raw_columns) == 1 else "(" + ", ".join(["'x'"] * len(raw_columns)) + ")"]
        * raw_rows
    )
    con.execute(f"INSERT INTO {CLAIMS.table} VALUES {values}")
    if staged_rows is not None:
        con.execute("CREATE SCHEMA stg")
        con.execute(
            f"CREATE TABLE {staging_relation(CLAIMS)} AS "
            f"SELECT * FROM {CLAIMS.table} LIMIT {staged_rows}"
        )
    return con


def statuses(results):
    return [r.status for r in results]


# ── the registry is complete ──────────────────────────────────────────────────


def test_every_registered_source_has_a_drift_specification() -> None:
    """A source added without deciding how its schema can be checked is the failure mode
    this prevents: it would otherwise be silently unwatched."""
    missing = [s.source for s in RAW_SOURCES if s.source not in DRIFT_BY_SOURCE]
    assert missing == []


def test_no_drift_specification_names_a_source_that_does_not_exist() -> None:
    registered = {s.source for s in RAW_SOURCES}
    assert [spec.source for spec in DRIFT if spec.source not in registered] == []


def test_every_committed_baseline_still_parses_and_yields_a_roster() -> None:
    """The baselines are committed JSON, so a malformed one is a real possibility -- and a
    roster extractor that silently returns nothing would turn drift detection into a no-op."""
    for spec in DRIFT:
        if spec.comparison == "none":
            continue
        payload = load_baseline(spec)
        assert payload is not None, f"{spec.source}: baseline missing"
        assert spec.extract is not None
        roster = spec.extract(payload)
        assert roster, f"{spec.source}: extractor produced an empty roster"


def test_a_source_with_no_machine_readable_roster_states_why() -> None:
    """SKIP is only honest when it carries its reason. A blank one is a silent pass."""
    for spec in DRIFT:
        if spec.comparison == "none":
            assert spec.reason.strip(), f"{spec.source}: skipped without saying why"


# ── roster extraction ─────────────────────────────────────────────────────────


def test_openfema_fields_are_read_by_name() -> None:
    assert openfema_names([{"name": "a"}, {"name": "b"}]) == {"a", "b"}


def test_socrata_computed_regions_are_excluded() -> None:
    """Socrata publishes two synthetic geography columns in view metadata that never appear
    in the export. CDC's baseline carries 33 fields where raw carries 31 for this reason."""
    payload = [
        {"fieldName": "yearstart"},
        {"fieldName": ":@computed_region_hjsp_umg2"},
        {"fieldName": ":@computed_region_skr5_azej"},
    ]
    assert socrata_names(payload) == {"yearstart"}


def test_socrata_display_names_would_have_matched_nothing() -> None:
    """`name` is the human title and `fieldName` is the column. Using the wrong one is a
    plausible mistake that would have produced a 33-column mismatch on every run."""
    payload = [{"fieldName": "yearstart", "name": "YearStart"}]
    assert socrata_names(payload) == {"yearstart"}


def test_key_names_reads_the_roster_a_pointer_baseline_records() -> None:
    assert key_names("row_fields")({"row_fields": ["zip", "geoid"]}) == {"zip", "geoid"}


# ── staging relation naming ───────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        ("raw.ins_nfip_claims", "stg.stg_ins__nfip_claims"),
        ("raw.hlt_cms_geographic_variation", "stg.stg_hlt__cms_geographic_variation"),
        ("raw.ref_cpi_u_series", "stg.stg_ref__cpi_u_series"),
        ("raw.fin_hmda_lar", "stg.stg_fin__hmda_lar"),
    ],
)
def test_staging_relation_is_derived_from_the_track_prefix(table: str, expected: str) -> None:
    assert staging_relation(RawSource("t", "s", table, "p")) == expected


# ── schema drift ──────────────────────────────────────────────────────────────


def test_an_unchanged_roster_passes(monkeypatch) -> None:
    roster = openfema_names(load_baseline(DRIFT_BY_SOURCE["nfip_claims"]))
    con = warehouse(sorted(roster), staged_rows=None)
    assert statuses(check_schema_drift(con, CLAIMS)) == [PASS]


def test_a_dropped_column_fails() -> None:
    """The one that matters: a model reading a column the publisher removed produces NULLs,
    not an error, and the failure surfaces as a wrong number much later."""
    roster = sorted(openfema_names(load_baseline(DRIFT_BY_SOURCE["nfip_claims"])))
    con = warehouse(roster[1:], staged_rows=None)
    results = check_schema_drift(con, CLAIMS)
    assert statuses(results) == [FAIL]
    assert "dropped or renamed" in results[0].detail
    assert roster[0] in results[0].detail


def test_a_new_column_warns_rather_than_failing() -> None:
    """Publishers add fields, and this repo keeps what they add. Worth reporting, not worth
    failing a run over -- nothing downstream can break because a column appeared."""
    roster = sorted(openfema_names(load_baseline(DRIFT_BY_SOURCE["nfip_claims"])))
    con = warehouse([*roster, "newFieldTheyAdded"], staged_rows=None)
    results = check_schema_drift(con, CLAIMS)
    assert statuses(results) == [WARN]
    assert "newFieldTheyAdded" in results[0].detail


def test_a_rename_is_reported_as_both_halves() -> None:
    """A rename is a drop and an addition at once; reporting only one half sends the reader
    looking in the wrong place."""
    roster = sorted(openfema_names(load_baseline(DRIFT_BY_SOURCE["nfip_claims"])))
    con = warehouse([*roster[1:], "renamedColumn"], staged_rows=None)
    results = check_schema_drift(con, CLAIMS)
    assert statuses(results) == [FAIL]
    assert roster[0] in results[0].detail
    assert "renamedColumn" in results[0].detail


def test_a_source_with_no_roster_skips_and_never_passes() -> None:
    con = warehouse(["series_id", "area_code"], staged_rows=None)
    con.execute(f"CREATE TABLE {CPI_SERIES.table} AS SELECT * FROM {CLAIMS.table}")
    results = check_schema_drift(con, CPI_SERIES)
    assert statuses(results) == [SKIP]
    assert "no baseline is committed" in results[0].detail


def test_an_unloaded_table_skips_rather_than_failing() -> None:
    """Not ingested is not the same as drifted, and L1 already reports the former."""
    con = duckdb.connect()
    con.execute("CREATE SCHEMA raw")
    assert statuses(check_schema_drift(con, CLAIMS)) == [SKIP]


# ── staging conservation ──────────────────────────────────────────────────────


def test_staging_that_conserves_every_row_passes() -> None:
    con = warehouse(["a"], staged_rows=3, raw_rows=3)
    assert statuses(check_staging_conservation(con, CLAIMS)) == [PASS]


def test_staging_that_drops_a_row_fails() -> None:
    """A stray WHERE in a typed view. L1 stops at raw and the dbt tests start at staging, so
    before this check nothing in the repo looked at the join between them."""
    con = warehouse(["a"], staged_rows=2, raw_rows=3)
    results = check_staging_conservation(con, CLAIMS)
    assert statuses(results) == [FAIL]
    # Spelled out, not signed: "(+1)" for a dropped row reads as though staging gained one.
    assert "1 row(s) dropped" in results[0].detail


def test_absent_staging_skips_with_the_command_that_fixes_it() -> None:
    con = warehouse(["a"], staged_rows=None)
    results = check_staging_conservation(con, CLAIMS)
    assert statuses(results) == [SKIP]
    assert "dbt build" in results[0].detail


# ── the baselines on disk ─────────────────────────────────────────────────────


def test_the_cdc_baseline_still_carries_its_two_computed_regions() -> None:
    """If Socrata ever stops emitting them, the exclusion above becomes dead code that hides
    a real difference. This fails the day that assumption expires."""
    payload = load_baseline(DRIFT_BY_SOURCE["cdc_healthy_aging"])
    computed = [f for f in payload if f["fieldName"].startswith(":@computed_region_")]
    assert len(computed) == 2
    assert len(payload) - len(computed) == len(socrata_names(payload))


def test_baseline_files_are_valid_json() -> None:
    for spec in DRIFT:
        if spec.baseline is None:
            continue
        from ingest.common import BASELINE_DIR

        path = BASELINE_DIR / f"{spec.baseline}.json"
        assert path.exists(), spec.baseline
        json.loads(path.read_text(encoding="utf-8"))
