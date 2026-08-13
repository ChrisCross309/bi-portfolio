"""Unit tests for the L1 integrity harness, plus an end-to-end fixture-mode run.

All offline. The integration test is what CI's `just ci` exercises.
"""

from pathlib import Path

import pytest
from ingest.fintech import cfpb
from ingest.insurance import nfip_claims
from ingest.registry import SOURCES as RAW_SOURCES
from reconcile.l1_integrity import (
    FAIL,
    PASS,
    REPO_ROOT,
    SKIP,
    SOURCES,
    WARN,
    Profile,
    Result,
    check_expected_keys,
    check_peer_and_rollup_partitions,
    check_state_partitions,
    check_year_partitions,
    classify_source_gap,
    landing_files,
    load_state_codes,
    report,
    resolve_landing_path,
)

NFIP = next(s for s in SOURCES if s.source == "nfip_claims")
CFPB = next(s for s in SOURCES if s.source == "cfpb_complaints")
POLICIES = next(s for s in SOURCES if s.source == "nfip_policies")
HMDA_LAR = next(s for s in SOURCES if s.source == "hmda_lar")
CDC = next(s for s in SOURCES if s.source == "cdc_healthy_aging")
ACS_DETAILED = next(s for s in SOURCES if s.source == "acs5_detailed")
CPI_U = next(s for s in SOURCES if s.source == "cpi_u")
CPI_SERIES = next(s for s in SOURCES if s.source == "cpi_u_series")


def _manifest(reported: int, data_refresh: str | None, metadata_refresh: str | None) -> dict:
    return {
        "source_reported_count": reported,
        "source_last_refresh": data_refresh,
        "source_metadata_refresh": metadata_refresh,
    }


# ── the source-gap rule ───────────────────────────────────────────────────────


def test_exact_match_passes() -> None:
    status, _ = classify_source_gap(_manifest(100, "A", "A"), 100)
    assert status == PASS


def test_gap_with_differing_refresh_timestamps_is_a_timing_note() -> None:
    """The real NFIP case: catalogue said 2,724,656, the bulk file held 2,724,014."""
    status, detail = classify_source_gap(
        _manifest(2_724_656, "2026-08-04T09:21:02.984Z", "2026-08-04T22:40:36.263Z"), 2_724_014
    )
    assert status == WARN
    assert "gap +642" in detail
    assert "refresh lag" in detail


def test_gap_with_identical_refresh_timestamps_is_a_bug() -> None:
    """Same timestamps means timing cannot explain it, so the run must fail."""
    status, detail = classify_source_gap(_manifest(100, "SAME", "SAME"), 90)
    assert status == FAIL
    assert "not explainable as timing" in detail


def test_single_timestamp_publisher_warns_rather_than_failing() -> None:
    """CFPB exposes only last-modified, so bulk-vs-API lag cannot be ruled out."""
    status, detail = classify_source_gap(_manifest(100, "Tue, 11 Aug 2026 09:30:31 GMT", None), 90)
    assert status == WARN
    assert "only one refresh timestamp" in detail


def test_source_without_a_published_count_is_skipped_not_guessed() -> None:
    status, _ = classify_source_gap(_manifest(None, "A", "B"), 90)
    assert status == SKIP


# ── partition completeness ────────────────────────────────────────────────────


def _all_states_profile(extra: dict[str, int] | None = None) -> Profile:
    by_partition = dict.fromkeys(load_state_codes(), 10)
    by_partition.update(extra or {})
    return Profile(total=sum(by_partition.values()), by_partition=by_partition)


def _statuses(results: list[Result], check: str) -> str:
    return next(r.status for r in results if r.check == check)


def test_complete_state_coverage_passes() -> None:
    profile = _all_states_profile()
    results = check_state_partitions(NFIP, profile, profile.total)
    assert _statuses(results, "states + DC present") == PASS
    assert _statuses(results, "territories present") == PASS


def test_missing_state_fails() -> None:
    profile = _all_states_profile()
    del profile.by_partition["MI"]
    profile.total = sum(profile.by_partition.values())
    results = check_state_partitions(NFIP, profile, profile.total)
    assert _statuses(results, "states + DC present") == FAIL


def test_missing_territory_warns_because_national_totals_stop_tying() -> None:
    profile = _all_states_profile()
    del profile.by_partition["PR"]
    profile.total = sum(profile.by_partition.values())
    results = check_state_partitions(NFIP, profile, profile.total)
    assert _statuses(results, "territories present") == WARN


def test_unknown_state_partition_is_reported_not_treated_as_an_error() -> None:
    profile = _all_states_profile({"UN": 16_441})
    results = check_state_partitions(NFIP, profile, profile.total)
    unknown = next(r for r in results if r.check == "unknown-state rows kept")
    assert unknown.status == PASS
    assert "UN=16,441" in unknown.detail


def test_fixture_mode_skips_census_checks_rather_than_padding_the_sample() -> None:
    """A stratified sample cannot answer "did we drop a state?", so it must not pretend to."""
    profile = Profile(total=30, by_partition={"MI": 10, "LA": 10, "PR": 10})
    results = check_state_partitions(NFIP, profile, profile.total, mode="fixture")
    assert _statuses(results, "states + DC present") == SKIP
    assert _statuses(results, "territories present") == SKIP
    # Structural checks still run on the sample.
    assert _statuses(results, "partitions sum to table") == PASS


def test_partitions_that_do_not_sum_to_the_table_fail() -> None:
    profile = _all_states_profile()
    results = check_state_partitions(NFIP, profile, profile.total + 1)
    assert _statuses(results, "partitions sum to table") == FAIL


def test_unbroken_year_run_passes() -> None:
    profile = Profile(by_partition={str(y): 5 for y in range(2011, 2027)})
    profile.total = sum(profile.by_partition.values())
    results = check_year_partitions(CFPB, profile, profile.total)
    assert _statuses(results, "year coverage") == PASS


def test_missing_year_fails() -> None:
    years = {str(y): 5 for y in range(2011, 2027)}
    del years["2019"]
    profile = Profile(total=sum(years.values()), by_partition=years)
    results = check_year_partitions(CFPB, profile, profile.total)
    assert _statuses(results, "year coverage") == FAIL


def test_malformed_date_partitions_are_reported_not_dropped() -> None:
    years = {str(y): 5 for y in range(2011, 2027)}
    years[""] = 3
    profile = Profile(total=sum(years.values()), by_partition=years)
    results = check_year_partitions(CFPB, profile, profile.total)
    malformed = next(r for r in results if r.check == "malformed dates kept")
    assert "''=3" in malformed.detail


# ── the registry is the single source of truth ────────────────────────────────


def test_every_spec_is_backed_by_a_registry_entry() -> None:
    """L1 must never invent a table or a partition column the loaders do not produce."""
    registered = {(s.track, s.source, s.table, s.partition_column) for s in RAW_SOURCES}
    for spec in SOURCES:
        assert (spec.track, spec.source, spec.table, spec.partition_column) in registered


def test_every_raw_source_is_registered_in_l1() -> None:
    """All twelve. A source that lands but is never verified is the gap this closes."""
    assert {s.source for s in SOURCES} == {s.source for s in RAW_SOURCES}
    assert len(SOURCES) == 12


# ── landing resolution ────────────────────────────────────────────────────────


def test_multi_file_sources_resolve_to_a_glob_not_one_file() -> None:
    """The defect this replaced: the old resolver returned the manifest's first name, so
    39 keyset pages and 8 HMDA years were each verified one file deep."""
    policies = resolve_landing_path(POLICIES, "live", Path("data"))
    assert policies.name == "page-*.parquet"
    assert policies.parent == Path("data/landing/insurance/nfip_policies")

    lar = resolve_landing_path(HMDA_LAR, "live", Path("data"))
    assert lar.name == "hmda_lar_MI_*.csv"


def test_cfpb_landing_glob_excludes_the_archive_beside_it() -> None:
    """`complaints.csv.zip` sits next to `complaints.csv`; only the CSV is readable."""
    pattern = resolve_landing_path(CFPB, "live", Path("data"))
    assert pattern.name == "*.csv"
    assert not Path("complaints.csv.zip").match(pattern.name)


def test_every_fixture_glob_matches_a_committed_file() -> None:
    """The guard that `just ci` can resolve landing for every registered source."""
    for spec in SOURCES:
        pattern = resolve_landing_path(spec, "fixture", REPO_ROOT / "data")
        assert landing_files(pattern), f"{spec.source}: nothing matches {pattern}"


def test_landing_files_is_empty_when_the_directory_is_gone(tmp_path: Path) -> None:
    """Reclaimed landing must read as absent, not raise."""
    assert landing_files(tmp_path / "nope" / "*.parquet") == []


def test_sources_sharing_a_landing_directory_resolve_into_it() -> None:
    """census lands both ACS sources under shared/acs5, bls both CPI sources under
    shared/cpi_u, so the directory is not the source name for four of the twelve."""
    detailed = resolve_landing_path(ACS_DETAILED, "live", Path("data"))
    assert detailed.parent == Path("data/landing/shared/acs5")
    assert detailed.name == "acs5-detailed-*.json"

    series = resolve_landing_path(CPI_SERIES, "live", Path("data"))
    assert series.parent == Path("data/landing/shared/cpi_u")


def test_bls_files_resolve_by_name_because_they_have_no_usable_extension() -> None:
    """`cu.data.1.AllItems` and `cu.series` are why matching landing by file extension --
    what the resolver used to do -- could never have worked for this publisher."""
    assert resolve_landing_path(CPI_U, "live", Path("data")).name == "cu.data.1.AllItems"
    assert resolve_landing_path(CPI_SERIES, "live", Path("data")).name == "cu.series"


def test_the_nested_acs_fixtures_resolve_into_their_subdirectory() -> None:
    """Every other source's fixtures sit directly under the track directory."""
    pattern = resolve_landing_path(ACS_DETAILED, "fixture", REPO_ROOT / "data")
    assert pattern.parent.name == "acs5"
    assert len(landing_files(pattern)) == 6  # 2 vintages x 3 geographies


# ── peers and rollups ─────────────────────────────────────────────────────────


def test_rollup_rows_are_reported_as_peers_of_the_states() -> None:
    """The trap this exists for: MI sits beside MDW, which contains it, beside US, which
    contains both. Summing `locationabbr` counts every state up to three times."""
    by_partition = dict.fromkeys(load_state_codes() - {"AS", "MP"}, 100)
    by_partition.update({"MDW": 6_099, "NRE": 6_102, "SOU": 6_030, "WEST": 6_126, "US": 6_132})
    profile = Profile(total=sum(by_partition.values()), by_partition=by_partition)

    results = check_peer_and_rollup_partitions(CDC, profile, profile.total)
    assert _statuses(results, "states + DC present") == PASS
    rollups = next(r for r in results if r.check == "rollups kept, not summed")
    assert rollups.status == PASS
    for key in ("MDW", "NRE", "SOU", "WEST", "US"):
        assert key in rollups.detail
    assert "double counts" in rollups.detail


def test_losing_the_rollups_fails_because_hlt_e1_needs_them() -> None:
    """HLT-E1 compares Michigan against exactly those rows, so dropping them is a defect."""
    by_partition = dict.fromkeys(load_state_codes() - {"AS", "MP"}, 100)
    profile = Profile(total=sum(by_partition.values()), by_partition=by_partition)
    results = check_peer_and_rollup_partitions(CDC, profile, profile.total)
    assert _statuses(results, "rollups kept, not summed") == FAIL


def test_a_missing_state_fails_for_cdc_too() -> None:
    by_partition = dict.fromkeys(load_state_codes() - {"AS", "MP", "MI"}, 100)
    by_partition["US"] = 6_132
    profile = Profile(total=sum(by_partition.values()), by_partition=by_partition)
    results = check_peer_and_rollup_partitions(CDC, profile, profile.total)
    assert _statuses(results, "states + DC present") == FAIL


# ── coverage questions are never answered by a sample ─────────────────────────


def test_year_coverage_is_skipped_against_a_fixture() -> None:
    """The ACS fixture carries vintages 2011 and 2024 on purpose -- 2011 is the only one
    that enumerates the island areas. Demanding an unbroken run of it would mean padding
    the sample to fifteen vintages to prove nothing about production, which is the same
    reason the state roster is skipped."""
    profile = Profile(total=20, by_partition={"2011": 10, "2024": 10})
    results = check_year_partitions(ACS_DETAILED, profile, profile.total, mode="fixture")
    assert _statuses(results, "year coverage") == SKIP
    assert _statuses(results, "earliest year") == SKIP
    # Structural checks still run on the sample.
    assert _statuses(results, "partitions sum to table") == PASS


def test_the_same_gap_fails_live() -> None:
    """The skip above must not have disarmed the check it was narrowing."""
    profile = Profile(total=20, by_partition={"2011": 10, "2024": 10})
    results = check_year_partitions(ACS_DETAILED, profile, profile.total)
    assert _statuses(results, "year coverage") == FAIL


# ── a small fixed domain ──────────────────────────────────────────────────────


def _keys(by_partition: dict[str, int]) -> Profile:
    return Profile(total=sum(by_partition.values()), by_partition=by_partition)


def test_an_exact_domain_passes() -> None:
    profile = _keys({"MI": 384_067})
    results = check_expected_keys(POLICIES, profile, profile.total, expected=frozenset({"MI"}))
    assert _statuses(results, "expected keys present") == PASS
    assert _statuses(results, "no unexpected keys") == PASS


def test_a_missing_key_fails() -> None:
    profile = _keys({"National": 33, "State": 1_815})
    results = check_expected_keys(
        POLICIES, profile, profile.total, expected=frozenset({"National", "State", "County"})
    )
    assert _statuses(results, "expected keys present") == FAIL


def test_a_key_outside_the_domain_fails_as_a_scope_leak() -> None:
    """NFIP policies raw is Michigan-only by decision, so another state is the leak
    CLAUDE.md section 4 calls the most likely scope bug in the repo."""
    profile = _keys({"MI": 384_067, "OH": 12})
    results = check_expected_keys(POLICIES, profile, profile.total, expected=frozenset({"MI"}))
    assert _statuses(results, "no unexpected keys") == FAIL
    unexpected = next(r for r in results if r.check == "no unexpected keys")
    assert "OH=12" in unexpected.detail


# ── the parameterized first year ──────────────────────────────────────────────


def test_each_source_carries_its_own_first_year() -> None:
    assert CFPB.first_expected_year == 2011
    assert HMDA_LAR.first_expected_year == 2018


def test_a_later_first_year_than_expected_warns() -> None:
    """A hole in the middle is ours; a moved starting year is the publisher's."""
    years = {str(y): 5 for y in range(2015, 2027)}
    profile = _keys(years)
    results = check_year_partitions(CFPB, profile, profile.total)
    assert _statuses(results, "year coverage") == PASS
    assert _statuses(results, "earliest year") == WARN


def test_hmda_starting_at_2018_does_not_warn() -> None:
    """The same run of years is correct for HMDA and wrong for CFPB."""
    years = {str(y): 5 for y in range(2018, 2026)}
    profile = _keys(years)
    results = check_year_partitions(HMDA_LAR, profile, profile.total)
    assert _statuses(results, "year coverage") == PASS
    assert not any(r.check == "earliest year" for r in results)


# ── exit code ─────────────────────────────────────────────────────────────────


def test_warnings_do_not_fail_the_run_but_failures_do() -> None:
    warned = [Result("t", "s", "c", WARN, ""), Result("t", "s", "c", PASS, "")]
    assert report(warned) == 0
    assert report([*warned, Result("t", "s", "c", FAIL, "")]) == 1


# ── end to end, offline ───────────────────────────────────────────────────────


@pytest.mark.filterwarnings("ignore")
def test_fixture_mode_load_and_l1_pass_offline() -> None:
    """Exactly what `just ci` runs: load committed fixtures, then verify them.

    Every registered source is loaded here, not just some. Leaving one out would let it
    pass on fixture data another test happened to leave behind, and report SKIP -- "no
    manifest; not ingested yet" -- on a clean checkout.
    """
    from ingest.fintech import hmda
    from ingest.health import cdc as cdc_module
    from ingest.health import cms
    from ingest.insurance import fema_declarations, nfip_policies
    from ingest.shared import bls, census
    from reconcile import l1_integrity

    for module in (
        nfip_claims,
        nfip_policies,
        fema_declarations,
        cfpb,
        hmda,
        cdc_module,
        cms,
        bls,
        census,
    ):
        assert module.main(["--mode", "fixture"]) == 0, module.__name__
    assert l1_integrity.main(["--mode", "fixture"]) == 0


@pytest.mark.filterwarnings("ignore")
def test_no_registered_source_reports_itself_unchecked(capsys: pytest.CaptureFixture) -> None:
    """A SKIP is a real answer for a reclaimed landing file, but never for a whole source.

    This is the guard against a spec whose fixture glob or manifest path is wrong: it would
    still exit 0, quietly verifying nothing.
    """
    from reconcile import l1_integrity

    assert l1_integrity.main(["--mode", "fixture"]) == 0
    printed = capsys.readouterr().out
    assert "not ingested yet" not in printed
    for spec in SOURCES:
        assert spec.source in printed
