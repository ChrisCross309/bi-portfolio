"""Unit tests for the HMDA fetcher.

Two of these lock behaviour that a plausible implementation would have got wrong, and
both were found by probing the live API rather than by reading its documentation: every
year's extract resolves to the same filename, and the year validator's own error message
names a range it does not honour.

Everything is offline except the `slow` tests, which CI never runs.
"""

import httpx
import pytest
from ingest.common import USER_AGENT as UA
from ingest.common import DiscoveryError
from ingest.fintech.hmda import (
    ALL_ACTIONS_TAKEN,
    CSV_ENDPOINT,
    FILERS_ENDPOINT,
    FIRST_MODERN_YEAR,
    SCOPE_STATE,
    YearExtract,
    aggregation_total,
    dataset_edition,
    discover_years,
    filers_landing_name,
    filers_total,
    institutions_manifest_payload,
    institutions_relation,
    lar_landing_name,
    lar_manifest_payload,
    lar_relation,
    newest_last_modified,
    partition_failures,
    refresh_fingerprint,
    scope_failures,
    year_is_unpublished,
)

YEARS = (2018, 2019, 2020)

# Two real resolved URLs, differing only in the year and the edition segment. The hash is
# a function of the filter, not of the year -- which is the whole point of the first test.
URL_2023 = (
    "https://files.ffiec.cfpb.gov/data-browser/datasets/2023/filtered-queries/"
    "one-year/24d725610cddfb66fa9b44fc8afd9576.csv"
)
URL_2018 = (
    "https://files.ffiec.cfpb.gov/data-browser/datasets/2018/filtered-queries/"
    "three-year/24d725610cddfb66fa9b44fc8afd9576.csv"
)


def _response(status: int, body: str) -> httpx.Response:
    return httpx.Response(status, text=body, request=httpx.Request("GET", FILERS_ENDPOINT))


def _extract(year: int, records: int, by_filers: int, last_modified: str | None = None):
    return YearExtract(
        year=year,
        resolved_url=URL_2023,
        edition="one-year",
        landing_bytes=130_462_833,
        last_modified=last_modified,
        etag='"2f65ea86cab20ff2c63b9a387571b7b5-16"',
        records_reported=records,
        filers_reported=by_filers,
        institutions_reported=1014,
    )


# ── the two traps ─────────────────────────────────────────────────────────────


def test_every_year_lands_under_its_own_name() -> None:
    """The API returns the same filename for every year -- the hash is of the filter, and
    content-disposition says `state_MI.csv` throughout. URL-derived naming, which is what
    the other fetchers here do, would write eight years into one file."""
    assert URL_2018.rsplit("/", 1)[-1] == URL_2023.rsplit("/", 1)[-1]
    names = {lar_landing_name(year) for year in range(2018, 2026)}
    assert len(names) == 8
    assert lar_landing_name(2023) == "hmda_lar_MI_2023.csv"


def test_the_stale_range_in_the_error_message_is_not_believed() -> None:
    """The validator says 2018-2023 while serving 2025, so only the shape is used."""
    unpublished = _response(
        400, '"must provide years in the range of 2018-2023, you have provided (2026)"'
    )
    assert year_is_unpublished(unpublished) is True

    # A different 400 is a real error, not the end of the available years.
    other_400 = _response(
        400,
        '{"errorType":"provide-atleast-one-filter-criteria","message":"Provide at least 1 '
        'filter criteria to perform aggregations"}',
    )
    assert year_is_unpublished(other_400) is False
    assert year_is_unpublished(_response(500, "gateway blew up")) is False
    assert year_is_unpublished(_response(200, '{"institutions":[]}')) is False


# ── control totals ────────────────────────────────────────────────────────────


def test_aggregation_total_sums_the_requested_buckets() -> None:
    payload = {
        "aggregations": [
            {"count": 186138, "actions_taken": "1"},
            {"count": 11591, "actions_taken": "2"},
            {"count": 63519, "actions_taken": "3"},
        ]
    }
    assert aggregation_total(payload, ALL_ACTIONS_TAKEN) == 261_248


def test_a_bucket_we_never_requested_stops_the_run() -> None:
    """A ninth action_taken code would make every control total silently short."""
    payload = {"aggregations": [{"count": 5, "actions_taken": "9"}]}
    with pytest.raises(DiscoveryError) as caught:
        aggregation_total(payload, ALL_ACTIONS_TAKEN)
    assert "action_taken domain has grown" in str(caught.value)


def test_an_empty_aggregation_is_not_a_total_of_zero() -> None:
    with pytest.raises(DiscoveryError):
        aggregation_total({"aggregations": []}, ALL_ACTIONS_TAKEN)


def test_filers_total_reports_rows_and_institutions() -> None:
    payload = {
        "institutions": [
            {"lei": "A", "name": "One", "count": 31, "period": 2023},
            {"lei": "B", "name": "Two", "count": 110461, "period": 2023},
        ]
    }
    assert filers_total(payload) == (110_492, 2)


def test_an_empty_filer_list_is_refused_rather_than_landed_blank() -> None:
    with pytest.raises(DiscoveryError):
        filers_total({"institutions": []})


# ── refresh signals ───────────────────────────────────────────────────────────


def test_the_fingerprint_is_per_year_so_a_new_year_cannot_hide() -> None:
    """A newly published year appears as a new key, not as a newer timestamp."""
    before = refresh_fingerprint((_extract(2023, 1, 1, "Wed, 15 Oct 2025 06:17:19 GMT"),))
    after = refresh_fingerprint(
        (
            _extract(2023, 1, 1, "Wed, 15 Oct 2025 06:17:19 GMT"),
            _extract(2024, 1, 1, "Tue, 30 Jun 2026 03:59:51 GMT"),
        )
    )
    assert before != after
    assert before == {"2023": "Wed, 15 Oct 2025 06:17:19 GMT"}


def test_newest_last_modified_orders_http_dates_not_strings() -> None:
    """'Wed, 15 Oct 2025' sorts above 'Tue, 30 Jun 2026' alphabetically."""
    extracts = (
        _extract(2023, 1, 1, "Wed, 15 Oct 2025 06:17:19 GMT"),
        _extract(2024, 1, 1, "Tue, 30 Jun 2026 03:59:51 GMT"),
    )
    assert newest_last_modified(extracts) == "Tue, 30 Jun 2026 03:59:51 GMT"


def test_no_timestamps_at_all_reads_as_none() -> None:
    assert newest_last_modified((_extract(2023, 1, 1, None),)) is None


# ── scope and partition proofs ────────────────────────────────────────────────


def test_all_requested_years_present_is_the_passing_case() -> None:
    assert partition_failures({"2018": 457_602, "2019": 518_271, "2020": 721_796}, YEARS) == []


def test_a_year_that_failed_to_land_is_named() -> None:
    problems = partition_failures({"2018": 457_602, "2020": 721_796}, YEARS)
    assert len(problems) == 1
    assert "['2019']" in problems[0]


def test_an_extract_holding_the_wrong_year_is_caught() -> None:
    """The extracts are cache files with the year only in the path; this is the check
    that a file matches the request it was made for."""
    problems = partition_failures({"2018": 457_602, "2019": 518_271, "2021": 738_346}, YEARS)
    assert len(problems) == 2
    assert any("never requested ['2021']" in problem for problem in problems)


def test_michigan_only_raw_rejects_a_scope_leak() -> None:
    assert scope_failures(["MI"]) == []
    assert scope_failures([]) == []
    problems = scope_failures(["MI", "OH"])
    assert len(problems) == 1
    assert "OH" in problems[0]


# ── relations ─────────────────────────────────────────────────────────────────


def test_both_relations_refuse_type_inference() -> None:
    """ "Exempt" in numeric fields, 8888 ages, and JSON integers all have to survive raw."""
    assert "all_varchar=true" in lar_relation("/tmp/hmda_lar_MI_2023.csv")
    relation = institutions_relation("/tmp/hmda_filers_national_2023.json")
    assert relation.count("VARCHAR") == 4
    assert "read_json" in relation


# ── manifests ─────────────────────────────────────────────────────────────────


def _lar_manifest():
    return lar_manifest_payload(
        extracts=(
            _extract(2023, 344_149, 344_149, "Wed, 15 Oct 2025 06:17:19 GMT"),
            _extract(2024, 373_290, 370_602, "Tue, 30 Jun 2026 03:59:51 GMT"),
        ),
        landing_files=[{"name": "hmda_lar_MI_2023.csv", "bytes": 1, "sha256": "abc"}],
        rows_landing=717_439,
        rows_raw=717_439,
        rows_duckdb=717_439,
        partition_rows={"2023": 344_149, "2024": 373_290},
        retrieved_at="2026-08-11T21:00:00Z",
    )


def test_the_lar_manifest_carries_both_control_totals_per_year() -> None:
    manifest = _lar_manifest()
    assert manifest["source_reported_count"] == 717_439
    assert [year["control_total_gap"] for year in manifest["years"]] == [0, 2688]
    assert manifest["years"][1]["source_reported_by_filers"] == 370_602
    # The landed rows matched the record-level total in all eight years, so the note has
    # to say which endpoint is the one that undercounts rather than leaving it open.
    note = manifest["control_totals"]["disagreement_note"]
    assert "it is the filer sum that undercounts" in note
    assert "Cause not established" in note


def test_the_lar_manifest_records_the_michigan_scope_exception() -> None:
    manifest = _lar_manifest()
    assert manifest["raw_scope"]["state"] == SCOPE_STATE
    assert "tens of millions" in manifest["raw_scope"]["reason"]
    assert manifest["source_last_refresh"] == "Tue, 30 Jun 2026 03:59:51 GMT"


def test_the_lar_manifest_states_the_fair_lending_limit() -> None:
    """HMDA has no credit score and no DTI, so findings are descriptive. CLAUDE.md rule 8."""
    caveats = " ".join(_lar_manifest()["coverage_caveats"])
    assert "credit score" in caveats
    assert "Exempt" in caveats


def test_the_institution_manifest_admits_it_cannot_prove_currency() -> None:
    manifest = institutions_manifest_payload(
        extracts=(_extract(2023, 344_149, 344_149),),
        landing_files=[{"name": "hmda_filers_national_2023.json", "bytes": 1, "sha256": "abc"}],
        rows_landing=5129,
        rows_raw=5129,
        rows_duckdb=5129,
        partition_rows={"2023": 5129},
        retrieved_at="2026-08-11T21:00:00Z",
    )
    assert manifest["source_last_refresh"] is None
    assert "no last-modified header" in manifest["source_refresh_unavailable_reason"]
    assert manifest["raw_scope"]["state"] is None  # national on purpose
    assert "DISTINCT lei" in manifest["grain_note"]


def test_filers_landing_names_are_year_specific_too() -> None:
    assert filers_landing_name(2023) == "hmda_filers_national_2023.json"


# ── live endpoint behaviour (excluded from CI) ─────────────────────────────────


@pytest.mark.slow
def test_live_aggregations_still_refuses_a_geography_only_query() -> None:
    """The mandatory-filter surprise. If this ever starts working, the control total can
    be simplified -- until then, enumerating actions_taken is not optional."""
    with httpx.Client(timeout=60, headers={"User-Agent": UA}, follow_redirects=True) as client:
        response = client.get(
            "https://ffiec.cfpb.gov/v2/data-browser-api/view/aggregations",
            params={"years": "2023", "states": SCOPE_STATE},
        )
    assert response.status_code == 400
    assert response.json()["errorType"] == "provide-atleast-one-filter-criteria"


@pytest.mark.slow
def test_live_year_discovery_finds_an_unbroken_range_from_2018() -> None:
    with httpx.Client(timeout=60, headers={"User-Agent": UA}, follow_redirects=True) as client:
        years = discover_years(client)
    assert years[0] == FIRST_MODERN_YEAR
    assert list(years) == list(range(years[0], years[-1] + 1))
    assert years[-1] >= 2024


@pytest.mark.slow
def test_live_extracts_answer_a_ranged_get_because_head_is_405() -> None:
    with httpx.Client(timeout=60, headers={"User-Agent": UA}, follow_redirects=True) as client:
        assert (
            client.head(CSV_ENDPOINT, params={"years": "2023", "states": "MI"}).status_code == 405
        )
        ranged = client.get(
            CSV_ENDPOINT,
            params={"years": "2023", "states": SCOPE_STATE},
            headers={"Range": "bytes=0-0"},
        )
    assert ranged.status_code == 206
    assert dataset_edition(str(ranged.url)) in {"one-year", "three-year", "snapshot"}
