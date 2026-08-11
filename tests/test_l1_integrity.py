"""Unit tests for the L1 integrity harness, plus an end-to-end fixture-mode run.

All offline. The integration test is what CI's `just ci` exercises.
"""

import pytest
from ingest.fintech import cfpb
from ingest.insurance import nfip_claims
from reconcile.l1_integrity import (
    FAIL,
    PASS,
    SKIP,
    SOURCES,
    WARN,
    Profile,
    Result,
    check_state_partitions,
    check_year_partitions,
    classify_source_gap,
    load_state_codes,
    report,
)

NFIP = next(s for s in SOURCES if s.source == "nfip_claims")
CFPB = next(s for s in SOURCES if s.source == "cfpb_complaints")


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


# ── exit code ─────────────────────────────────────────────────────────────────


def test_warnings_do_not_fail_the_run_but_failures_do() -> None:
    warned = [Result("t", "s", "c", WARN, ""), Result("t", "s", "c", PASS, "")]
    assert report(warned) == 0
    assert report([*warned, Result("t", "s", "c", FAIL, "")]) == 1


# ── end to end, offline ───────────────────────────────────────────────────────


@pytest.mark.filterwarnings("ignore")
def test_fixture_mode_load_and_l1_pass_offline() -> None:
    """Exactly what `just ci` runs: load committed fixtures, then verify them."""
    from ingest.insurance import nfip_policies
    from reconcile import l1_integrity

    assert nfip_claims.main(["--mode", "fixture"]) == 0
    assert nfip_policies.main(["--mode", "fixture"]) == 0
    assert cfpb.main(["--mode", "fixture"]) == 0
    assert l1_integrity.main(["--mode", "fixture"]) == 0
