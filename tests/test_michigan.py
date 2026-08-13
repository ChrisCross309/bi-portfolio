"""Unit tests for the Michigan geography gate.

All offline: each test builds a small table in an in-memory DuckDB shaped like the source it
stands for, so the gate's SQL runs for real without needing the warehouse.
"""

import duckdb
import pytest
from ingest.health import cms
from reconcile.michigan import (
    GATES,
    MICHIGAN_COUNTY_COUNT,
    UNUSABLE_WARN_PCT,
    check_michigan_geography,
    gate_for,
    michigan_counties,
    usable_rates,
)
from reconcile.results import PASS, SKIP, WARN


@pytest.fixture
def con():
    connection = duckdb.connect()
    yield connection
    connection.close()


def _status(results, check: str) -> str:
    return next(r.status for r in results if r.check == check)


def _detail(results, check: str) -> str:
    return next(r.detail for r in results if r.check == check)


# ── the trap the gate exists for ──────────────────────────────────────────────


def test_hmda_na_literal_counts_as_missing(con) -> None:
    """The measurement that motivated this: a NULL-only predicate reports 0.00% missing on
    HMDA, because the publisher writes the literal string 'NA' instead of a null."""
    con.execute("CREATE SCHEMA raw")
    con.execute("""
        CREATE TABLE raw.fin_hmda_lar AS
        SELECT * FROM (VALUES
            ('MI', '26163'), ('MI', '26161'), ('MI', 'NA'), ('MI', 'NA'), ('MI', '39035')
        ) AS t(state_code, county_code)
    """)
    gate = gate_for("hmda_lar")

    naive = con.execute(
        "SELECT count(*) FROM raw.fin_hmda_lar WHERE county_code IS NULL"
    ).fetchone()[0]
    assert naive == 0, "a null check sees nothing wrong, which is the whole problem"

    _, mi_unusable, _, _ = usable_rates(con, gate)
    assert mi_unusable == 3  # two 'NA' plus one out-of-state FIPS on an MI filing


def test_nfip_uses_null_where_cfpb_uses_null_but_zips_use_empty_strings(con) -> None:
    """The convention is per column, not per publisher, so both forms must count."""
    con.execute("CREATE SCHEMA raw")
    con.execute("""
        CREATE TABLE raw.ins_nfip_claims AS
        SELECT * FROM (VALUES
            ('MI', '26163000100'), ('MI', NULL), ('MI', ''), ('LA', NULL)
        ) AS t(state, censusGeoid)
    """)
    mi_rows, mi_unusable, nat_rows, nat_unusable = usable_rates(con, gate_for("nfip_claims"))
    assert (mi_rows, mi_unusable) == (3, 2)  # the NULL and the empty string both count
    assert (nat_rows, nat_unusable) == (4, 3)


# ── a missing county is not always a defect ───────────────────────────────────


def _nfip_claims_table(con, geoids: list[str]) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    values = ", ".join(f"('MI', '{g}')" for g in geoids)
    con.execute(
        f"CREATE OR REPLACE TABLE raw.ins_nfip_claims AS "
        f"SELECT * FROM (VALUES {values}) AS t(state, censusGeoid)"
    )


def test_a_source_that_is_not_a_denominator_reports_coverage_and_passes(con) -> None:
    """NFIP covers 82 of 83 counties live. Flood claims are not filed everywhere, so that
    is a fact about flood insurance rather than a load failure."""
    _nfip_claims_table(con, [f"26{n:03d}000100" for n in range(1, 40)])
    results = check_michigan_geography(con, "insurance", gate_for("nfip_claims"))
    assert _status(results, "MI county roster") == PASS
    assert f"39 of {MICHIGAN_COUNTY_COUNT}" in _detail(results, "MI county roster")


def test_a_denominator_short_of_the_roster_warns(con) -> None:
    """HMDA, CMS and ACS are denominators: a missing county silently shrinks every
    per-capita rate built on them."""
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("""
        CREATE OR REPLACE TABLE raw.ref_acs5_detailed AS
        SELECT * FROM (VALUES ('county', '26', '163'), ('county', '26', '161'))
        AS t(geo_level, state, county)
    """)
    results = check_michigan_geography(con, "shared", gate_for("acs5_detailed"))
    assert _status(results, "MI county roster") == WARN
    assert "every rate built on it would be wrong" in _detail(results, "MI county roster")


def test_a_complete_denominator_passes(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    values = ", ".join(f"('county', '26', '{n:03d}')" for n in range(1, MICHIGAN_COUNTY_COUNT + 1))
    con.execute(
        f"CREATE OR REPLACE TABLE raw.ref_acs5_detailed AS "
        f"SELECT * FROM (VALUES {values}) AS t(geo_level, state, county)"
    )
    results = check_michigan_geography(con, "shared", gate_for("acs5_detailed"))
    assert _status(results, "MI county roster") == PASS
    assert f"all {MICHIGAN_COUNTY_COUNT} counties" in _detail(results, "MI county roster")


# ── sources with no county at all ─────────────────────────────────────────────


def test_cfpb_has_no_county_column_and_says_so(con) -> None:
    """CFPB publishes ZIP and state only. FIN-E1's county grain needs a crosswalk in
    session 2, and the gate is where that requirement surfaces."""
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("""
        CREATE OR REPLACE TABLE raw.fin_cfpb_complaints AS
        SELECT * FROM (VALUES ('MI', '48104'), ('MI', NULL), ('OH', '43004'))
        AS t("State", "ZIP code")
    """)
    results = check_michigan_geography(con, "fintech", gate_for("cfpb_complaints"))
    assert _status(results, "MI county roster") == PASS
    assert "no county column" in _detail(results, "MI county roster")
    assert "crosswalk" in _detail(results, "MI county roster")


# ── the CMS unassigned-county bucket ──────────────────────────────────────────


def test_the_cms_unknown_bucket_is_excluded_from_the_roster(con) -> None:
    """CMS files '26000' / MI-UNKNOWN at County level, so a naive count returns 84."""
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    codes = [f"26{n:03d}" for n in range(1, MICHIGAN_COUNTY_COUNT + 1)] + ["26000"]
    values = ", ".join(f"('County', '{c}')" for c in codes)
    con.execute(
        f"CREATE OR REPLACE TABLE raw.hlt_cms_geographic_variation AS "
        f"SELECT * FROM (VALUES {values}) AS t(BENE_GEO_LVL, BENE_GEO_CD)"
    )
    gate = gate_for("cms_geographic_variation")

    naive = con.execute(
        "SELECT count(DISTINCT BENE_GEO_CD) FROM raw.hlt_cms_geographic_variation "
        "WHERE BENE_GEO_LVL = 'County' AND BENE_GEO_CD LIKE '26%'"
    ).fetchone()[0]
    assert naive == MICHIGAN_COUNTY_COUNT + 1, "the bucket inflates a naive distinct count"

    assert len(michigan_counties(con, gate)) == MICHIGAN_COUNTY_COUNT
    results = check_michigan_geography(con, "health", gate)
    assert _status(results, "MI county roster") == PASS


def test_the_gate_and_the_cms_fetcher_agree_on_what_unassigned_means() -> None:
    """One definition, referenced twice. If the fetcher's suffix changed and the gate's
    predicate did not, the two would silently disagree about the roster."""
    gate = gate_for("cms_geographic_variation")
    assert cms.UNKNOWN_COUNTY_SUFFIX in gate.unusable
    assert cms.split_michigan_counties(["26163", "26000"]) == (["26163"], ["26000"])


# ── thresholds and registration ───────────────────────────────────────────────


def test_a_high_unusable_rate_warns(con) -> None:
    _nfip_claims_table(con, ["26163000100"] + [""] * 9)  # 90% unusable
    results = check_michigan_geography(con, "insurance", gate_for("nfip_claims"))
    assert _status(results, "MI geography usable") == WARN
    assert UNUSABLE_WARN_PCT < 90


def test_mi_scoped_sources_say_there_is_no_national_rate(con) -> None:
    """Comparing MI against itself would print a meaningless 'national' figure."""
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("""
        CREATE OR REPLACE TABLE raw.fin_hmda_lar AS
        SELECT * FROM (VALUES ('MI', '26163'), ('MI', 'NA')) AS t(state_code, county_code)
    """)
    results = check_michigan_geography(con, "fintech", gate_for("hmda_lar"))
    assert "no national rate to compare" in _detail(results, "MI geography usable")


def test_only_sources_with_michigan_geography_are_gated() -> None:
    """Four of the twelve carry none: the HMDA filer list is national and has no
    geography, CDC is state-grain, and the two CPI-U tables are not geographic at all."""
    gated = {gate.source for gate in GATES}
    assert gated == {
        "nfip_claims",
        "nfip_policies",
        "fema_declarations",
        "cfpb_complaints",
        "hmda_lar",
        "cms_geographic_variation",
        "acs5_detailed",
        "acs5_subject",
    }
    for source in ("hmda_institutions", "cdc_healthy_aging", "cpi_u", "cpi_u_series"):
        assert gate_for(source) is None


def test_the_roster_is_a_live_check_only(con) -> None:
    """The HMDA fixture carries 68 of 83 counties and is correct: 998 stratified rows
    cannot hold the roster. Padding it until this passed would prove nothing about
    production, which is the rule the state roster and year coverage already follow."""
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("""
        CREATE OR REPLACE TABLE raw.fin_hmda_lar AS
        SELECT * FROM (VALUES ('MI', '26163'), ('MI', '26161')) AS t(state_code, county_code)
    """)
    gate = gate_for("hmda_lar")
    assert (
        _status(check_michigan_geography(con, "fintech", gate, "fixture"), "MI county roster")
        == SKIP
    )
    # The same two counties fail live, so the skip narrowed the check without disarming it.
    assert _status(check_michigan_geography(con, "fintech", gate), "MI county roster") == WARN


def test_a_sample_rate_is_never_judged_against_the_threshold(con) -> None:
    """A stratified fixture over-represents edge cases by design, so its unusable rate is
    inflated. The number is printed; the threshold is not applied to it."""
    _nfip_claims_table(con, ["26163000100"] + [""] * 9)  # 90% unusable
    results = check_michigan_geography(con, "insurance", gate_for("nfip_claims"), "fixture")
    assert _status(results, "MI geography usable") == PASS
    assert "sample rate" in _detail(results, "MI geography usable")
