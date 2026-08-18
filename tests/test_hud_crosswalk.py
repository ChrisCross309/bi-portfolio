"""The HUD ZIP-to-county crosswalk fetcher.

Every check here runs offline against the committed fixture or against hand-built payloads.
The one test that calls huduser.gov is marked `slow` and excluded from CI.

The bias of this file is toward the four things that would silently corrupt FIN-E1 rather
than fail loudly: an error envelope read as data, a hardcoded vintage, a ratio reformatted
on the way in, and a ZIP with no residential addresses vanishing from an allocation.
"""

import json

import pytest
from ingest.common import DiscoveryError, load_state_codes
from ingest.shared import hud_crosswalk as hud

FIXTURE = hud.REPO_ROOT / "tests" / "fixtures" / "shared" / "zip_county_crosswalk.json"
METADATA = hud.REPO_ROOT / "tests" / "fixtures" / "shared" / "zip_county_crosswalk.metadata.json"


def fixture_rows() -> list[dict]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))["data"]["results"]


def payload(**overrides) -> dict:
    body = {
        "data": {
            "year": 2026,
            "quarter": 1,
            "input": "MI",
            "crosswalk_type": "zip-county",
            "results": [{"zip": "48001", "geoid": "26147", "state": "MI"}],
        }
    }
    body["data"].update(overrides)
    return body


# ── the error envelope ────────────────────────────────────────────────────────


def test_a_list_response_is_named_as_an_error_not_dereferenced() -> None:
    """HUD returns `[{"error": ...}]`, so `.get("data")` would raise AttributeError.

    The failure mode this prevents is a stack trace three frames away from the cause.
    """
    body = [{"error": "No data found using the value MI for type 2"}]
    with pytest.raises(DiscoveryError) as caught:
        hud.require_payload(body, context="query=MI")
    assert "error envelope" in str(caught.value)
    assert "No data found" in str(caught.value), "the raw response must be shown, not summarised"


def test_a_renumbered_crosswalk_stops_the_run() -> None:
    """Type 2 is ZIP->county today. If HUD renumbers, we must not silently load ZIP->tract."""
    with pytest.raises(DiscoveryError, match="crosswalk numbering"):
        hud.require_payload(payload(crosswalk_type="zip-tract"), context="query=MI")


@pytest.mark.parametrize("body", [[], {}, {"data": []}, {"data": {"results": {}}}, "text"])
def test_every_shape_that_is_not_a_result_set_is_refused(body) -> None:
    with pytest.raises(DiscoveryError):
        hud.require_payload(body, context="probe")


def test_a_well_formed_response_is_unwrapped() -> None:
    data = hud.require_payload(payload(), context="query=MI")
    assert data["results"][0]["geoid"] == "26147"


# ── the vintage ───────────────────────────────────────────────────────────────


def test_vintage_comes_from_the_publishers_own_answer() -> None:
    assert hud.vintage_of(payload()["data"], context="query=MI") == "2026Q1"


def test_a_response_without_a_vintage_stops_the_run() -> None:
    """CLAUDE.md rule 3. Without year and quarter there is nothing to record and nothing
    to compare a later run against, so guessing a quarter is worse than stopping."""
    data = payload()["data"]
    del data["quarter"]
    with pytest.raises(DiscoveryError, match="did not report its own vintage"):
        hud.vintage_of(data, context="query=MI")


def test_no_quarter_is_hardcoded_anywhere_in_the_module() -> None:
    """A literal quarter in the source would mean a run pinned to a stale crosswalk."""
    source = (hud.REPO_ROOT / "platform" / "ingest" / "shared" / "hud_crosswalk.py").read_text(
        encoding="utf-8"
    )
    body = source.split('"""', 2)[-1]  # the module docstring may cite 2021Q1 as history
    for literal in ("year=", "quarter="):
        assert f'"{literal}' not in body and f"'{literal}" not in body


# ── the completeness checks ───────────────────────────────────────────────────


def test_a_missing_state_is_a_failure_not_a_gap() -> None:
    known = load_state_codes()
    assert hud.state_coverage_failures(set(known), known) == []
    problems = hud.state_coverage_failures(set(known) - {"MI", "OH"}, known)
    assert len(problems) == 1 and "MI" in problems[0] and "OH" in problems[0]


def test_the_spot_check_compares_the_publisher_against_itself() -> None:
    assert hud.spot_check_failures(1601, 1601) == []
    problems = hud.spot_check_failures(1500, 1601)
    assert "-101" in problems[0] and "not complete" in problems[0]


def test_a_short_michigan_roster_fails_because_this_source_is_a_denominator() -> None:
    assert hud.michigan_failures(83) == []
    assert "82 of 83" in hud.michigan_failures(82)[0]


def test_a_missing_token_explains_how_to_get_one(monkeypatch) -> None:
    monkeypatch.setenv("HUD_API_TOKEN", "")
    monkeypatch.setattr(hud, "read_secret", lambda name: None)
    with pytest.raises(hud.MissingToken) as caught:
        hud.bearer_token()
    assert "USPS Crosswalk" in str(caught.value) and ".env" in str(caught.value)


# ── the reader ────────────────────────────────────────────────────────────────


def test_the_reader_keeps_every_column_as_text(tmp_path) -> None:
    """CLAUDE.md rule 2. A DOUBLE here would reformat HUD's own numbers."""
    import duckdb

    con = duckdb.connect()
    types = {
        d[0]: str(d[1])
        for d in con.execute(f"SELECT * FROM {hud.crosswalk_relation(FIXTURE)} LIMIT 0").description
    }
    assert set(types) == set(hud.ROW_FIELDS)
    assert set(types.values()) == {"VARCHAR"}


def test_the_reader_preserves_the_publishers_own_number_formatting() -> None:
    """The point of landing text: `1` must not become `1.0`, and a long decimal must
    survive to the last digit, or L1's lossless comparison is checking a rounding.

    Note the fixture's ratios are JSON *strings* while HUD's are JSON *numbers*: every
    fixture in this repo is re-serialized from all-varchar raw rather than sliced out of
    the original download, which `reconcile.make_fixtures` documents. `->>` extracts the
    text either way, which is exactly why the reader is written with it.
    """
    import duckdb

    con = duckdb.connect()
    read = {
        (row[0], row[1]): row[2]
        for row in con.execute(
            f"SELECT zip, geoid, res_ratio FROM {hud.crosswalk_relation(FIXTURE)}"
        ).fetchall()
    }
    for row in fixture_rows():
        text = read[(row["zip"], row["geoid"])]
        # Character for character, not merely numerically equal: a whole-number ratio
        # stays `1` and never becomes `1.0`, and no digit is lost off a long decimal.
        assert text == str(row["res_ratio"])


# ── the fixture itself ────────────────────────────────────────────────────────


def test_the_fixture_carries_every_state_the_reference_file_names() -> None:
    """Otherwise fixture mode would fail a check the live data passes."""
    assert load_state_codes() - {row["state"] for row in fixture_rows()} == set()


def test_the_fixture_carries_all_83_michigan_counties() -> None:
    michigan = {row["geoid"] for row in fixture_rows() if row["state"] == "MI"}
    assert len(michigan) == hud.MICHIGAN_COUNTY_COUNT


def test_the_fixture_keeps_the_edge_cases_the_allocation_rule_depends_on() -> None:
    rows = fixture_rows()
    by_zip: dict[str, list[dict]] = {}
    for row in rows:
        by_zip.setdefault(row["zip"], []).append(row)

    split = [zip_code for zip_code, group in by_zip.items() if len(group) > 1]
    assert split, "a ZIP crossing a county line is the whole reason this source exists"

    no_residential = [
        zip_code
        for zip_code, group in by_zip.items()
        if sum(float(row["res_ratio"]) for row in group) == 0
    ]
    assert no_residential, (
        "a ZIP with no residential addresses must survive sampling: an allocation weighted "
        "by res_ratio alone silently drops every complaint behind one"
    )
    assert all(
        sum(float(row["tot_ratio"]) for row in by_zip[zip_code]) > 0 for zip_code in no_residential
    ), "tot_ratio is the documented fallback, so it must be non-zero where res_ratio is not"

    assert [row for row in rows if len(row["geoid"]) != 5], "territory geoids are not 5 characters"
    assert [row for row in rows if row["state"] in {"FM", "MH", "PW"}]


def test_the_fixture_metadata_matches_the_fixture() -> None:
    """Offline, this file plays the publisher for the spot check. If it drifts from the
    sample, fixture mode fails against a number nobody published."""
    metadata = json.loads(METADATA.read_text(encoding="utf-8"))
    michigan = sum(1 for row in fixture_rows() if row["state"] == "MI")
    assert metadata["spot_check_rows"] == michigan
    assert metadata["vintage"], "the fixture records the vintage it was built from"


def test_the_fixture_is_wrapped_in_the_publishers_envelope() -> None:
    """A bare array would exercise a reader the live path never uses."""
    body = json.loads(FIXTURE.read_text(encoding="utf-8"))
    data = hud.require_payload(body, context="fixture")
    assert data["crosswalk_type"] == hud.EXPECTED_CROSSWALK_NAME
    assert hud.vintage_of(data, context="fixture")


# ── live endpoint ─────────────────────────────────────────────────────────────


@pytest.mark.slow
def test_the_live_api_still_answers_the_shape_this_module_expects() -> None:
    """Excluded from CI, which never calls an external API."""
    from ingest.common import http_client

    with http_client() as client:
        client.headers["Authorization"] = f"Bearer {hud.bearer_token()}"
        data, _, _ = hud.fetch_crosswalk(client, hud.SPOT_CHECK_STATE)
    assert hud.vintage_of(data, context="live")
    assert set(data["results"][0]) == set(hud.ROW_FIELDS)
    assert {row["geoid"] for row in data["results"]}.__len__() == hud.MICHIGAN_COUNTY_COUNT
