"""Unit tests for the Census ACS fetcher.

Three things carry the weight here. The API rejects unkeyed requests with an HTTP *200* and
an HTML page, so the usual status check sees nothing wrong. The payload is an array of
arrays read by position, so a reordered header would mislabel every column silently. And the
geography roster moves between vintages, so a check written as an exact row count fails on
real data for the wrong reason.

Everything is offline. The `slow` tests need a key and are excluded from CI, which never
touches the network.
"""

import json
import os
from pathlib import Path

import duckdb
import httpx
import pytest
from ingest.common import USER_AGENT, DiscoveryError, load_state_fips
from ingest.shared.census import (
    CATALOG,
    CORE_STATE_COUNT,
    DATASETS,
    GEOGRAPHIES,
    KEY_NAME,
    MICHIGAN_COUNTY_COUNT,
    REDACTED,
    catalog_entries,
    dataset_relation,
    expected_header,
    fetch_catalog,
    geography_relation,
    header_failures,
    landed_payload,
    landing_name,
    manifest_payload,
    michigan_failures,
    redact,
    row_count_failures,
    state_failures,
    vintage_failures,
)

FIXTURE_DIR = Path("tests/fixtures/shared/acs5")
# Deliberately not shaped like a real key. A Census key is 40 lowercase hex, which the
# `no-long-hex-strings` pre-commit hook rejects on sight -- and a stand-in that trips the
# secret scanner would teach the wrong lesson about what belongs in a tracked file.
# `redact` is a string replace, so the shape was never what these tests exercise.
KEY = "NOT-A-REAL-CENSUS-KEY"


# ── the key, and never leaking it ─────────────────────────────────────────────


def test_the_key_is_scrubbed_from_anything_we_record() -> None:
    """The API takes the key only as a query parameter, so every URL we might log or store
    contains it. Nothing recorded may."""
    url = f"https://api.census.gov/data/2024/acs/acs5?get=NAME&for=us:*&key={KEY}"
    scrubbed = redact(url, KEY)
    assert KEY not in scrubbed
    assert REDACTED in scrubbed


def test_redaction_is_a_no_op_without_a_key() -> None:
    assert redact("nothing to hide", None) == "nothing to hide"


def test_the_manifest_never_carries_the_key() -> None:
    manifest = _manifest()
    serialised = json.dumps(manifest)
    assert KEY not in serialised
    assert "key=" not in serialised
    assert manifest["resolved_url"] == "https://api.census.gov/data/<vintage>/acs/acs5"
    assert manifest["api_key"]["required"] is True
    assert KEY_NAME in manifest["api_key"]["read_from"]


# ── the positional-read guard ─────────────────────────────────────────────────


def test_expected_header_puts_the_api_keys_after_our_variables() -> None:
    variables = DATASETS["detailed"]["variables"]
    assert expected_header(variables, "county") == ["NAME", *variables, "state", "county"]
    assert expected_header(variables, "us") == ["NAME", *variables, "us"]


def test_a_matching_header_passes() -> None:
    variables = DATASETS["subject"]["variables"]
    assert header_failures(["NAME", *variables, "state"], variables, "state") == []


def test_a_reordered_header_is_caught_before_anything_is_read_by_position() -> None:
    variables = DATASETS["detailed"]["variables"]
    swapped = ["NAME", variables[1], variables[0], variables[2], variables[3], "state", "county"]
    problems = header_failures(swapped, variables, "county")
    assert len(problems) == 1
    assert "silently mislabel" in problems[0]


def test_every_committed_fixture_header_matches_what_we_request() -> None:
    for dataset, spec in DATASETS.items():
        for path in sorted(FIXTURE_DIR.glob(f"acs5-{dataset}-*.json")):
            geography = path.stem.rsplit("-", 1)[-1]
            header, rows = landed_payload(path)
            assert header_failures(header, spec["variables"], geography) == [], path.name
            assert rows, f"{path.name} landed no data rows"


def test_a_payload_that_is_not_an_array_of_arrays_stops_the_run(tmp_path: Path) -> None:
    """This is the shape an HTML error page would arrive in if it ever parsed as JSON."""
    broken = tmp_path / "acs5-detailed-2024-us.json"
    broken.write_text('{"error": "Missing Key"}', encoding="utf-8")
    with pytest.raises(DiscoveryError) as caught:
        landed_payload(broken)
    assert "not an array of arrays" in str(caught.value)


# ── roster checks that have to tolerate a moving roster ───────────────────────


def test_row_count_floors_pass_on_real_shapes() -> None:
    assert row_count_failures("us", 1) == []
    assert row_count_failures("state", 52) == []
    assert row_count_failures("state", 56) == []  # the 2011 subject vintage really is 56
    assert row_count_failures("county", 3_222) == []


def test_a_truncated_geography_is_caught() -> None:
    problems = row_count_failures("county", 500)
    assert len(problems) == 1
    assert "expected at least 3,000" in problems[0]


def test_row_count_floors_are_skipped_against_a_sample() -> None:
    """A committed fixture is a sample by construction; padding it until a census check
    passed would prove nothing about production."""
    assert row_count_failures("county", 816, "fixture") == []


def test_state_rows_are_validated_against_the_shared_reference() -> None:
    known = load_state_fips()
    core = [code for code in known if int(code) <= 56]
    assert len(core) == CORE_STATE_COUNT
    assert state_failures([*core, "72"], known) == []
    # The island areas are legitimate extras, not errors.
    assert state_failures([*core, "72", "60", "66", "69", "78"], known) == []


def test_an_unknown_state_code_is_caught() -> None:
    known = load_state_fips()
    core = [code for code in known if int(code) <= 56]
    problems = state_failures([*core, "99"], known)
    assert any("does not recognise" in problem for problem in problems)


def test_a_missing_state_is_caught_and_named() -> None:
    known = load_state_fips()
    core = [code for code in known if int(code) <= 56 and code != "26"]
    problems = state_failures(core, known)
    assert any("26 (MI)" in problem for problem in problems)


def test_michigan_roster_must_be_complete_in_every_vintage() -> None:
    assert michigan_failures({"2024": MICHIGAN_COUNTY_COUNT}) == []
    problems = michigan_failures({"2023": MICHIGAN_COUNTY_COUNT, "2024": 82})
    assert len(problems) == 1
    assert "2024" in problems[0]
    assert "every per-capita denominator would be wrong" in problems[0]


def test_the_fixture_carries_michigans_whole_roster_in_both_vintages() -> None:
    con = duckdb.connect()
    for dataset, spec in DATASETS.items():
        relation = dataset_relation(FIXTURE_DIR, dataset, spec["variables"])
        counts = dict(
            con.execute(
                f"SELECT vintage, count(DISTINCT county) FROM {relation} "
                "WHERE geo_level = 'county' AND state = '26' GROUP BY 1"
            ).fetchall()
        )
        assert michigan_failures(counts) == [], dataset
        assert len(counts) == 2
    con.close()


# ── vintages ──────────────────────────────────────────────────────────────────


def test_an_unbroken_vintage_range_passes() -> None:
    assert vintage_failures(tuple(range(2010, 2025))) == []


def test_a_hole_in_the_vintage_range_is_caught() -> None:
    problems = vintage_failures((2010, 2011, 2013))
    assert "[2012]" in problems[0]


def test_no_shared_vintage_at_all_is_caught() -> None:
    assert vintage_failures(()) != []


def test_catalog_entries_are_keyed_by_vintage() -> None:
    catalog = [
        {"c_dataset": ["acs", "acs5"], "c_vintage": 2024, "modified": "2025-09-02"},
        {"c_dataset": ["acs", "acs5"], "c_vintage": 2023, "modified": "2024-09-01"},
        {"c_dataset": ["acs", "acs5", "subject"], "c_vintage": 2024, "modified": "2025-09-03"},
        {"c_dataset": ["dec", "pl"], "c_vintage": 2020, "modified": "2021-01-01"},
    ]
    detailed = catalog_entries(catalog, "detailed")
    assert sorted(detailed) == [2023, 2024]
    assert catalog_entries(catalog, "subject")[2024]["modified"] == "2025-09-03"


def test_a_catalogue_with_no_matching_dataset_stops_the_run() -> None:
    with pytest.raises(DiscoveryError):
        catalog_entries([{"c_dataset": ["dec", "pl"], "c_vintage": 2020}], "detailed")


# ── conversion ────────────────────────────────────────────────────────────────


def test_landing_names_carry_the_vintage_and_geography() -> None:
    """The response contains neither, so the filename is where that provenance lives."""
    assert landing_name("detailed", 2024, "county") == "acs5-detailed-2024-county.json"
    names = {landing_name(d, v, g) for d in DATASETS for v in (2010, 2024) for g in GEOGRAPHIES}
    assert len(names) == 2 * 2 * 3


def test_the_relation_reads_by_position_and_drops_the_header_row() -> None:
    relation = geography_relation(
        FIXTURE_DIR / "acs5-detailed-*-county.json",
        DATASETS["detailed"]["variables"],
        "county",
    )
    assert 'row[1] AS "NAME"' in relation
    assert "WHERE row[1] <> 'NAME'" in relation
    assert "format='array'" in relation


def test_the_conversion_adds_only_vintage_and_geo_level() -> None:
    con = duckdb.connect()
    relation = dataset_relation(FIXTURE_DIR, "detailed", DATASETS["detailed"]["variables"])
    columns = [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()]
    con.close()
    assert columns == [
        "NAME",
        "B01003_001E",
        "B01003_001M",
        "B25001_001E",
        "B25001_001M",
        "state",
        "county",
        "us",
        "vintage",
        "geo_level",
    ]


def test_the_three_geographies_are_distinguishable_after_the_union() -> None:
    """us, state and county rows share one table and pad the keys they do not have."""
    con = duckdb.connect()
    relation = dataset_relation(FIXTURE_DIR, "detailed", DATASETS["detailed"]["variables"])
    rows = dict(con.execute(f"SELECT geo_level, count(*) FROM {relation} GROUP BY 1").fetchall())
    nulls = con.execute(
        f"SELECT count(*) FROM {relation} WHERE geo_level = 'state' AND county IS NOT NULL"
    ).fetchone()[0]
    us_rows = con.execute(
        f"SELECT count(*) FROM {relation} WHERE geo_level = 'us' AND us IS NOT NULL"
    ).fetchone()[0]
    con.close()
    assert set(rows) == {"us", "state", "county"}
    assert nulls == 0
    assert us_rows == rows["us"]


def test_the_acs_negative_sentinel_survives_as_text() -> None:
    """-555555555 is an annotation meaning "controlled estimate, no margin applies", not a
    measurement. Type inference would turn it into a number and someone would average it."""
    con = duckdb.connect()
    relation = dataset_relation(FIXTURE_DIR, "detailed", DATASETS["detailed"]["variables"])
    sentinels = con.execute(
        f"SELECT count(*) FROM {relation} WHERE B01003_001M LIKE '-5555%'"
    ).fetchone()[0]
    con.close()
    assert sentinels > 0


def test_the_island_area_placeholders_are_kept_with_null_estimates() -> None:
    """The 2011 subject vintage lists four island areas and publishes nothing for them."""
    con = duckdb.connect()
    relation = dataset_relation(FIXTURE_DIR, "subject", DATASETS["subject"]["variables"])
    blank = con.execute(
        f"SELECT count(*) FROM {relation} WHERE S0101_C01_030E IS NULL AND vintage = '2011'"
    ).fetchone()[0]
    con.close()
    assert blank > 0


# ── manifest ──────────────────────────────────────────────────────────────────


def _manifest():
    return manifest_payload(
        dataset="detailed",
        catalog_entries={
            2010: {"modified": "2020-01-01", "c_variablesLink": "https://example.gov/v.json"},
            2024: {"modified": "2025-09-02", "c_variablesLink": "https://example.gov/v.json"},
        },
        vintages=(2010, 2024),
        landing_files=[{"name": "acs5-detailed-2024-us.json", "bytes": 120, "sha256": "abc"}],
        rows_landing=49_107,
        rows_raw=49_107,
        rows_duckdb=49_107,
        partition_rows={"2010": 3_273, "2024": 3_275},
        rows_by_geography={"us": 15, "state": 780, "county": 48_312},
        michigan_counties={"2010": 83, "2024": 83},
        valueless_rows={},
        retrieved_at="2026-08-12T00:00:00Z",
    )


def test_the_manifest_warns_that_vintages_overlap() -> None:
    vintages = _manifest()["vintages"]
    assert "share four years of sample" in vintages["overlap_warning"]
    assert "do not plot consecutive vintages as a trend" in vintages["overlap_warning"]
    assert "subject tables begin at 2010" in vintages["why_this_range"]


def test_the_manifest_explains_the_columns_we_added() -> None:
    geography = _manifest()["geography"]
    assert "`vintage` and `geo_level` are ours" in geography["added_columns"]
    assert "counts the country three times" in geography["levels_are_peers"]
    assert geography["michigan_counties_by_vintage"] == {"2010": 83, "2024": 83}
    assert "planning regions" in geography["county_roster_warning"]


def test_the_manifest_says_why_there_is_no_count_to_reconcile() -> None:
    manifest = _manifest()
    assert manifest["source_reported_count"] is None
    assert "reports no row count" in manifest["source_count_unavailable_reason"]
    assert manifest["source_last_refresh"] == "2025-09-02"


def test_the_manifest_states_these_are_estimates_not_counts() -> None:
    caveats = " ".join(_manifest()["coverage_caveats"])
    assert "margins of error, not counts" in caveats
    assert "-555555555" in caveats
    assert "1-year product excludes areas under 65,000" in caveats


# ── live endpoint behaviour (needs a key; excluded from CI) ────────────────────


needs_key = pytest.mark.skipif(
    not os.environ.get(KEY_NAME), reason=f"{KEY_NAME} is not set in the environment"
)


@pytest.mark.slow
def test_live_catalogue_still_serves_both_datasets() -> None:
    """The catalogue needs no key; only the data endpoints do."""
    with httpx.Client(
        timeout=120, headers={"User-Agent": USER_AGENT}, follow_redirects=True
    ) as client:
        catalog = fetch_catalog(client)
    detailed = catalog_entries(catalog, "detailed")
    subject = catalog_entries(catalog, "subject")
    shared = tuple(sorted(set(detailed) & set(subject)))
    assert vintage_failures(shared) == []
    assert shared[0] == 2010, "the subject tables used to start at 2010"


@pytest.mark.slow
def test_live_unkeyed_request_still_fails_as_http_200_html() -> None:
    """The trap that makes an explicit JSON check necessary. If Census ever starts returning
    a proper 4xx, this fails and the check can be simplified."""
    with httpx.Client(
        timeout=60, headers={"User-Agent": USER_AGENT}, follow_redirects=True
    ) as client:
        response = client.get(
            "https://api.census.gov/data/2024/acs/acs5",
            params={"get": "NAME,B01003_001E", "for": "us:*"},
        )
    assert response.status_code == 200
    assert "json" not in (response.headers.get("content-type") or "").lower()
    assert "Missing Key" in response.text


@pytest.mark.slow
@needs_key
def test_live_michigan_still_has_its_83_counties() -> None:
    with httpx.Client(
        timeout=120, headers={"User-Agent": USER_AGENT}, follow_redirects=True
    ) as client:
        response = client.get(
            "https://api.census.gov/data/2024/acs/acs5",
            params={
                "get": "NAME,B01003_001E",
                "for": "county:*",
                "in": "state:26",
                "key": os.environ[KEY_NAME],
            },
        )
    assert "json" in (response.headers.get("content-type") or "").lower()
    assert len(response.json()) - 1 == MICHIGAN_COUNTY_COUNT


@pytest.mark.slow
def test_live_catalogue_url_is_the_one_the_manifest_records() -> None:
    assert CATALOG == "https://api.census.gov/data.json"
