"""Unit tests for the BLS CPI-U fetcher.

The padding is the thing. BLS ships `series_id` right-padded to a fixed width in both
files, so a literal equality filter silently returns nothing — and the deflator is the one
series every constant-dollar measure in the repo depends on finding.

Everything is offline except the `slow` tests, which CI never runs. The fixture-backed
tests read the committed samples with DuckDB and touch no network.
"""

import duckdb
import httpx
import pytest
from ingest.common import USER_AGENT, DiscoveryError
from ingest.shared.bls import (
    ANNUAL_PERIOD,
    API_SERIES,
    CU_DIRECTORY,
    DEFLATOR_SERIES_ID,
    OBSERVATIONS_FILE,
    SERIES_FILE,
    SERIES_ID_WIDTH,
    annual_period_failures,
    crosscheck_failures,
    definition_failures,
    fetch_index,
    observations_manifest_payload,
    padding_failures,
    parse_index,
    require_files,
    series_manifest_payload,
    tsv_relation,
)

OBSERVATIONS_FIXTURE = "tests/fixtures/shared/cpi_u.tsv"
SERIES_FIXTURE = "tests/fixtures/shared/cpi_u_series.tsv"

# A trimmed-down copy of what download.bls.gov actually serves: an IIS directory listing.
INDEX_HTML = (
    "<html><head><title>download.bls.gov - /pub/time.series/cu/</title></head><body>"
    "<H1>download.bls.gov - /pub/time.series/cu/</H1><hr>\r\n\r\n<pre>"
    '<A HREF="/pub/time.series/">[To Parent Directory]</A><br><br>'
    ' 7/22/2026 12:08 PM         2185 <A HREF="/pub/time.series/cu/cu.area">cu.area</A><br>'
    ' 7/22/2026 12:08 PM      2683417 <A HREF="/pub/time.series/cu/cu.data.1.AllItems">'
    "cu.data.1.AllItems</A><br>"
    ' 7/22/2026 12:08 PM      1339447 <A HREF="/pub/time.series/cu/cu.series">cu.series</A><br>'
    "</pre><hr></body></html>"
)


def _relation(path: str) -> str:
    return tsv_relation(path)


# ── discovery ─────────────────────────────────────────────────────────────────


def test_the_file_index_is_read_rather_than_the_names_hardcoded() -> None:
    names = parse_index(INDEX_HTML)
    assert names == ["cu.area", "cu.data.1.AllItems", "cu.series"]


def test_an_unrecognisable_index_fails_loudly() -> None:
    """If BLS changes its listing format we stop, rather than guess a filename."""
    with pytest.raises(DiscoveryError) as caught:
        parse_index("<html><body>nothing here</body></html>")
    assert "listing format may have changed" in str(caught.value)


def test_a_withdrawn_file_stops_the_run() -> None:
    with pytest.raises(DiscoveryError) as caught:
        require_files(["cu.area", "cu.series"], (OBSERVATIONS_FILE, SERIES_FILE))
    assert OBSERVATIONS_FILE in str(caught.value)


def test_both_required_files_present_is_silent() -> None:
    assert (
        require_files([OBSERVATIONS_FILE, SERIES_FILE, "cu.area"], (OBSERVATIONS_FILE, SERIES_FILE))
        is None
    )


# ── the padding trap ──────────────────────────────────────────────────────────


def test_a_uniform_pad_width_passes_and_a_mixed_one_does_not() -> None:
    assert padding_failures([SERIES_ID_WIDTH]) == []
    problems = padding_failures([11, SERIES_ID_WIDTH])
    assert len(problems) == 1
    assert "Joins between the two files" in problems[0]


def test_the_deflator_is_unreachable_without_trimming() -> None:
    """The trap, demonstrated on committed data: the literal filter finds nothing."""
    con = duckdb.connect()
    relation = _relation(OBSERVATIONS_FIXTURE)
    literal, trimmed = con.execute(
        f"""SELECT count(*) FILTER (WHERE series_id = '{DEFLATOR_SERIES_ID}'),
                   count(*) FILTER (WHERE trim(series_id) = '{DEFLATOR_SERIES_ID}')
            FROM {relation}"""
    ).fetchone()
    con.close()
    assert literal == 0
    assert trimmed > 0


def test_the_fixture_preserves_the_exact_pad_width() -> None:
    con = duckdb.connect()
    widths = [
        int(row[0])
        for row in con.execute(
            f"SELECT DISTINCT length(series_id) FROM {_relation(OBSERVATIONS_FIXTURE)}"
        ).fetchall()
    ]
    con.close()
    assert widths == [SERIES_ID_WIDTH]
    assert padding_failures(widths) == []


def test_values_keep_their_padding_too() -> None:
    """`value` arrives space-padded; raw is not the place to clean that up."""
    con = duckdb.connect()
    padded = con.execute(
        f"SELECT count(*) FROM {_relation(OBSERVATIONS_FIXTURE)} WHERE value <> trim(value)"
    ).fetchone()[0]
    con.close()
    assert padded > 0


# ── period and definition coverage ────────────────────────────────────────────


def test_the_annual_average_period_is_required() -> None:
    assert annual_period_failures(["M01", "M12", ANNUAL_PERIOD]) == []
    problems = annual_period_failures(["M01", "M12"])
    assert len(problems) == 1
    assert "thirteenth month" in problems[0]


def test_the_fixture_carries_the_annual_average() -> None:
    con = duckdb.connect()
    periods = [
        str(row[0])
        for row in con.execute(
            f"SELECT DISTINCT period FROM {_relation(OBSERVATIONS_FIXTURE)}"
        ).fetchall()
    ]
    con.close()
    assert annual_period_failures(periods) == []


def test_the_period_column_mixes_months_with_averages() -> None:
    """M13 is the annual average and S01-S03 the semiannual ones, in the same column as the
    twelve real months. Anything that averages across `period` counts them twice."""
    con = duckdb.connect()
    periods = {
        str(row[0])
        for row in con.execute(
            f"SELECT DISTINCT period FROM {_relation(OBSERVATIONS_FIXTURE)}"
        ).fetchall()
    }
    con.close()
    months = {f"M{month:02d}" for month in range(1, 13)}
    assert months < periods
    assert {ANNUAL_PERIOD, "S01", "S02", "S03"} <= periods


def test_an_observed_series_with_no_definition_is_a_failure() -> None:
    assert definition_failures([]) == []
    problems = definition_failures(["CUUR9999SA0"])
    assert "disagree about what exists" in problems[0]


def test_every_observed_series_in_the_fixture_is_defined() -> None:
    """The reverse does not hold: the definitions file covers series we did not land."""
    con = duckdb.connect()
    undefined = con.execute(
        f"""SELECT count(DISTINCT trim(o.series_id))
            FROM {_relation(OBSERVATIONS_FIXTURE)} o
            LEFT JOIN {_relation(SERIES_FIXTURE)} s
              ON trim(o.series_id) = trim(s.series_id)
            WHERE s.series_id IS NULL"""
    ).fetchone()[0]
    definitions = con.execute(f"SELECT count(*) FROM {_relation(SERIES_FIXTURE)}").fetchone()[0]
    observed = con.execute(
        f"SELECT count(DISTINCT trim(series_id)) FROM {_relation(OBSERVATIONS_FIXTURE)}"
    ).fetchone()[0]
    con.close()
    assert undefined == 0
    assert definitions > observed


def test_the_fixture_exercises_both_seasonal_partitions() -> None:
    """Seasonally adjusted is the wrong series for deflation, so both must be present to
    prove the distinction survives into raw."""
    con = duckdb.connect()
    seasonal = dict(
        con.execute(
            f"SELECT trim(seasonal), count(*) FROM {_relation(SERIES_FIXTURE)} GROUP BY 1"
        ).fetchall()
    )
    con.close()
    assert {"S", "U"} <= set(seasonal)


# ── the API cross-check ───────────────────────────────────────────────────────


REPORTED = {"series_id": DEFLATOR_SERIES_ID, "year": "2026", "period": "M06", "value": "333.952"}


def test_agreement_between_the_file_and_the_api_passes() -> None:
    assert crosscheck_failures({"value": "333.952"}, REPORTED) == []


def test_a_disagreement_is_a_failure_naming_both_numbers() -> None:
    problems = crosscheck_failures({"value": "331.000"}, REPORTED)
    assert len(problems) == 1
    assert "331.000" in problems[0]
    assert "333.952" in problems[0]


def test_a_missing_observation_is_a_failure() -> None:
    problems = crosscheck_failures(None, REPORTED)
    assert "absent from the landed file" in problems[0]


def test_an_unreachable_api_is_not_a_failure() -> None:
    """The flat file is the source of record; the API is a courtesy."""
    assert crosscheck_failures(None, None) == []
    assert crosscheck_failures({"value": "333.952"}, None) == []


# ── manifests ─────────────────────────────────────────────────────────────────


def _observations_manifest():
    return observations_manifest_payload(
        landing_files=[{"name": OBSERVATIONS_FILE, "bytes": 2_683_417, "sha256": "c939b099"}],
        resolved_url=CU_DIRECTORY + OBSERVATIONS_FILE,
        last_modified="Wed, 22 Jul 2026 16:08:27 GMT",
        rows_landing=63_888,
        rows_raw=63_888,
        rows_duckdb=63_888,
        partition_rows={"1913": 12, "2026": 6},
        periods=["M01", "M13", "S01"],
        series_observed=201,
        crosscheck={"agrees": True, "reported": REPORTED, "landed_value": "333.952"},
        retrieved_at="2026-08-11T23:30:00Z",
    )


def test_the_manifest_says_why_there_is_no_row_count_to_reconcile() -> None:
    manifest = _observations_manifest()
    assert manifest["source_reported_count"] is None
    assert "no manifest and no row count" in manifest["source_count_unavailable_reason"]
    assert manifest["source_last_refresh"] == "Wed, 22 Jul 2026 16:08:27 GMT"


def test_the_manifest_names_the_deflator_and_why_it_is_unadjusted() -> None:
    deflator = _observations_manifest()["deflator"]
    assert deflator["series_id"] == DEFLATOR_SERIES_ID
    assert "not seasonally adjusted" in deflator["why"]
    assert "wrong series for deflation" in deflator["why"]
    assert deflator["api_crosscheck"]["agrees"] is True


def test_the_manifest_warns_about_the_padding_and_the_thirteenth_month() -> None:
    gotchas = " ".join(_observations_manifest()["gotchas"])
    assert "returns zero rows; trim() first" in gotchas
    assert ANNUAL_PERIOD in gotchas
    assert "double counts" in gotchas


def test_the_series_manifest_explains_the_two_files_differ_in_coverage() -> None:
    manifest = series_manifest_payload(
        landing_files=[{"name": SERIES_FILE, "bytes": 1_339_447, "sha256": "6ad9eaa7"}],
        resolved_url=CU_DIRECTORY + SERIES_FILE,
        last_modified="Wed, 22 Jul 2026 16:08:28 GMT",
        rows_landing=8_104,
        rows_raw=8_104,
        rows_duckdb=8_104,
        partition_rows={"U": 7_778, "S": 326},
        series_observed=201,
        retrieved_at="2026-08-11T23:30:00Z",
    )
    assert "Definitions without observations are expected" in manifest["relationship_note"]
    assert "seasonally adjusted" in manifest["grain_note"]
    assert "cu.area" in manifest["not_landed"]


def test_tsv_relation_disables_type_inference_and_quoting() -> None:
    relation = tsv_relation("/tmp/cu.series")
    assert "all_varchar=true" in relation
    assert "quote=''" in relation
    assert "delim='\\t'" in relation


# ── live endpoint behaviour (excluded from CI) ─────────────────────────────────


@pytest.mark.slow
def test_live_index_still_lists_both_files_we_depend_on() -> None:
    with httpx.Client(
        timeout=60, headers={"User-Agent": USER_AGENT}, follow_redirects=True
    ) as client:
        names = parse_index(fetch_index(client))
    assert require_files(names, (OBSERVATIONS_FILE, SERIES_FILE)) is None
    assert len(names) > 20


@pytest.mark.slow
def test_live_bls_serves_us_with_our_own_user_agent() -> None:
    """BLS 403s a default python-httpx User-Agent; an honest one is required."""
    with httpx.Client(timeout=60, follow_redirects=True) as anonymous:
        refused = anonymous.get(CU_DIRECTORY + SERIES_FILE)
    with httpx.Client(
        timeout=60, headers={"User-Agent": USER_AGENT}, follow_redirects=True
    ) as identified:
        allowed = identified.get(CU_DIRECTORY + SERIES_FILE, headers={"Range": "bytes=0-100"})
    assert allowed.status_code in (200, 206)
    assert refused.status_code == 403, (
        "BLS now serves anonymous callers; the User-Agent note can be relaxed"
    )


@pytest.mark.slow
def test_live_api_agrees_with_the_series_we_deflate_with() -> None:
    with httpx.Client(
        timeout=60, headers={"User-Agent": USER_AGENT}, follow_redirects=True
    ) as client:
        body = client.get(API_SERIES).json()
    assert body["status"] == "REQUEST_SUCCEEDED"
    latest = body["Results"]["series"][0]["data"][0]
    assert latest["period"].startswith("M")
    assert float(latest["value"]) > 100  # the 1982-84=100 base is long behind us
