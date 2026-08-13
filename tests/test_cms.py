"""Unit tests for the CMS geographic-variation fetcher.

The catalogue holds three datasets with "Geographic Variation" in the title, two of which
would land the wrong population or the wrong geography in the health track. That, and the
Michigan county roster, are what these lock.

Everything is offline except the `slow` tests, which CI never runs.
"""

import duckdb
import httpx
import pytest
from ingest.common import USER_AGENT, DiscoveryError
from ingest.health.cms import (
    DATASET_TITLE,
    GEO_LEVELS,
    MICHIGAN_COUNTY_COUNT,
    PARTITION_COLUMN,
    fetch_catalog,
    geo_level_failures,
    landing_name,
    manifest_payload,
    michigan_failures,
    select_csv_distribution,
    select_dataset,
    source_relation,
    split_michigan_counties,
    year_coverage_failures,
)

FIXTURE = "tests/fixtures/health/cms_geographic_variation.csv"

CSV_URL = (
    "https://data.cms.gov/sites/default/files/2026-04/cc600d1e/"
    "2014-2024%20Original%20Medicare%20Geographic%20Variation%20Public%20Use%20File.csv"
)
WANTED = {
    "title": DATASET_TITLE,
    "identifier": "https://data.cms.gov/data-api/v1/dataset/6219697b/data-viewer",
    "modified": "2026-05-15",
    "temporal": "2024-01-01/2024-12-31",
    "accrualPeriodicity": "R/P1Y",
    "distribution": [
        {"format": "API", "accessURL": "https://data.cms.gov/data-api/v1/dataset/6219697b/data"},
        {"format": "CSV", "downloadURL": CSV_URL},
        {"format": "API", "accessURL": "https://data.cms.gov/data-api/v1/dataset/1e426446/data"},
    ],
}
# Real neighbours in the same catalogue: a different population, and a different geography.
ADVANTAGE = {"title": "Medicare Advantage Geographic Variation - National & State"}
HRR = {"title": "Medicare Geographic Variation - by Hospital Referral Region"}


def _fixture_relation() -> str:
    return source_relation(FIXTURE)


# ── discovery ─────────────────────────────────────────────────────────────────


def test_the_right_geographic_variation_dataset_is_chosen() -> None:
    assert select_dataset([ADVANTAGE, WANTED, HRR])["modified"] == "2026-05-15"


def test_a_missing_title_names_the_neighbours_it_rejected() -> None:
    """Medicare Advantage is a different population; HRR is a different geography. If the
    exact title ever moves, the error has to show what was on offer."""
    with pytest.raises(DiscoveryError) as caught:
        select_dataset([ADVANTAGE, HRR])
    assert "Hospital Referral Region" in str(caught.value)
    assert "got 0" in str(caught.value)


def test_a_duplicated_title_stops_the_run() -> None:
    with pytest.raises(DiscoveryError):
        select_dataset([WANTED, WANTED])


def test_the_csv_distribution_wins_over_the_api_ones() -> None:
    """Two of the three distributions are paged API endpoints that would each need their
    own completeness proof."""
    assert select_csv_distribution(WANTED) == ("csv", CSV_URL)


def test_no_csv_distribution_fails_loudly_with_the_raw_block() -> None:
    api_only = {"title": DATASET_TITLE, "distribution": WANTED["distribution"][:1]}
    with pytest.raises(DiscoveryError) as caught:
        select_csv_distribution(api_only)
    assert "data-api" in str(caught.value)


def test_landing_name_unescapes_the_publishers_spaces() -> None:
    assert landing_name(CSV_URL).startswith("2014-2024 Original Medicare")
    assert "%20" not in landing_name(CSV_URL)


# ── coverage checks ───────────────────────────────────────────────────────────


def test_unbroken_year_coverage_passes() -> None:
    assert year_coverage_failures([str(y) for y in range(2014, 2025)]) == []


def test_a_missing_year_is_caught_rather_than_read_as_a_trend() -> None:
    years = [str(y) for y in range(2014, 2025) if y != 2020]
    problems = year_coverage_failures(years)
    assert len(problems) == 1
    assert "['2020']" in problems[0]


def test_no_usable_years_is_a_failure() -> None:
    assert year_coverage_failures(["", "n/a"]) != []


def test_all_three_grains_must_be_present() -> None:
    assert geo_level_failures(list(GEO_LEVELS)) == []
    problems = geo_level_failures(["County", "State"])
    assert "National" in problems[0]
    assert "benchmark" in problems[0]


# ── the Michigan county roster ────────────────────────────────────────────────


def test_the_unassigned_county_is_separated_from_the_real_roster() -> None:
    codes = [f"26{n:03d}" for n in range(1, 166, 2)] + ["26000"]
    real, unknown = split_michigan_counties(codes)
    assert unknown == ["26000"]
    assert len(real) == MICHIGAN_COUNTY_COUNT
    assert michigan_failures(codes) == []


def test_a_missing_county_fails_the_run() -> None:
    """A short roster silently shrinks every county denominator in the track."""
    codes = [f"26{n:03d}" for n in range(1, 164, 2)] + ["26000"]
    problems = michigan_failures(codes)
    assert len(problems) == 1
    assert "expected 83" in problems[0]


def test_other_states_do_not_count_toward_michigan() -> None:
    real, unknown = split_michigan_counties(["39035", "18097", "26001"])
    assert real == ["26001"]
    assert unknown == []


# ── the committed fixture ─────────────────────────────────────────────────────


def test_the_fixture_carries_the_whole_michigan_county_roster() -> None:
    con = duckdb.connect()
    codes = [
        row[0]
        for row in con.execute(
            f"SELECT DISTINCT BENE_GEO_CD FROM {_fixture_relation()} "
            f"WHERE {PARTITION_COLUMN} = 'County' AND BENE_GEO_CD LIKE '26%'"
        ).fetchall()
    ]
    con.close()
    assert michigan_failures(codes) == []
    real, unknown = split_michigan_counties(codes)
    assert len(real) == MICHIGAN_COUNTY_COUNT
    assert unknown == ["26000"]


def test_the_fixture_keeps_the_three_grains_and_the_age_levels() -> None:
    con = duckdb.connect()
    levels = dict(
        con.execute(
            f"SELECT {PARTITION_COLUMN}, count(*) FROM {_fixture_relation()} GROUP BY 1"
        ).fetchall()
    )
    ages = dict(
        con.execute(
            f"SELECT BENE_AGE_LVL, count(*) FROM {_fixture_relation()} GROUP BY 1"
        ).fetchall()
    )
    con.close()
    assert geo_level_failures(list(levels)) == []
    assert {"All", "<65", ">=65"} <= set(ages)


def test_the_fixture_preserves_the_suppression_marker_verbatim() -> None:
    """`*` is information. Type inference would have turned it into a null."""
    con = duckdb.connect()
    suppressed, columns = con.execute(
        f"""SELECT count(*) FILTER (WHERE TOT_MDCR_STDZD_PYMT_PC = '*'),
                   (SELECT count(*) FROM (DESCRIBE SELECT * FROM {_fixture_relation()}))
            FROM {_fixture_relation()}"""
    ).fetchone()
    con.close()
    assert suppressed > 0
    assert columns == 246


def test_csv_relation_disables_type_inference() -> None:
    assert "all_varchar=true" in source_relation("/tmp/gv.csv")


# ── manifest ──────────────────────────────────────────────────────────────────


def _manifest():
    return manifest_payload(
        dataset=WANTED,
        resolved_url=CSV_URL,
        landing_file="2014-2024 Original Medicare Geographic Variation Public Use File.csv",
        landing_bytes=57_865_948,
        landing_sha256="10c8304012da34da",
        http_metadata={"content_length": None, "content_encoding": "gzip"},
        rows_landing=36_994,
        rows_raw=36_994,
        rows_duckdb=36_994,
        partition_rows={"County": 35_146, "State": 1_815, "National": 33},
        rows_by_age_level={"All": 35_762, "<65": 616, ">=65": 616},
        years=[str(y) for y in range(2014, 2025)],
        michigan_counties=[f"26{n:03d}" for n in range(1, 166, 2)],
        michigan_unknown=["26000"],
        columns=246,
        retrieved_at="2026-08-11T23:00:00Z",
    )


def test_the_manifest_says_why_there_is_no_count_to_reconcile() -> None:
    manifest = _manifest()
    assert manifest["source_reported_count"] is None
    assert "no row count" in manifest["source_count_unavailable_reason"]
    assert "Michigan county roster" in manifest["source_count_unavailable_reason"]


def test_the_manifest_records_both_double_counting_axes() -> None:
    grain = _manifest()["grain"]
    assert grain["levels"]["County"] == 35_146
    assert "counts every beneficiary three times" in grain["note"]
    assert grain["age_levels"]["<65"] == 616
    assert "double count their own 'All'" in grain["age_level_note"]


def test_the_manifest_records_the_michigan_roster_and_the_unknown_bucket() -> None:
    michigan = _manifest()["michigan"]
    assert michigan["counties_present"] == MICHIGAN_COUNTY_COUNT
    assert michigan["unknown_county_codes"] == ["26000"]
    assert "Real people, kept in raw" in michigan["unknown_county_note"]


def test_the_manifest_warns_that_this_is_ffs_only_and_has_no_condition_detail() -> None:
    caveats = " ".join(_manifest()["coverage_caveats"])
    assert "Medicare Advantage" in caveats
    assert "no dementia or" in caveats
    assert "Standardized payment columns" in caveats
    assert "suppressed small cell" in caveats


def test_the_manifest_flags_the_misleading_temporal_window() -> None:
    """DCAT says 2024 only; the file holds 2014 onward."""
    dcat = _manifest()["dcat"]
    assert dcat["temporal"] == "2024-01-01/2024-12-31"
    assert "Trust the data, not the metadata window" in dcat["temporal_note"]
    assert [d["format"] for d in dcat["other_distributions"]] == ["API", "API"]


# ── live endpoint behaviour (excluded from CI) ─────────────────────────────────


@pytest.mark.slow
def test_live_catalogue_still_offers_this_dataset_with_a_csv() -> None:
    with httpx.Client(
        timeout=120, headers={"User-Agent": USER_AGENT}, follow_redirects=True
    ) as client:
        datasets = fetch_catalog(client)
        dataset = select_dataset(datasets)
        distribution_format, url = select_csv_distribution(dataset)
    assert distribution_format == "csv"
    assert url.endswith(".csv")
    assert dataset.get("accrualPeriodicity") == "R/P1Y", "annual grain is a modelling assumption"


@pytest.mark.slow
def test_live_chronic_conditions_is_still_absent_from_the_catalogue() -> None:
    """Watch a retired dataset for republication.

    CMS retired its Medicare chronic-conditions statistics effective 2026-06-15 and points
    users at data.cms.gov, which carries no replacement. That file was the only public
    source of dementia prevalence and dementia-attributable spending for Medicare
    beneficiaries at county grain, and losing it cost HLT-E2 and HLT-E3 their original
    wording -- both were re-scoped onto Geographic Variation, which has no condition
    dimension at all.

    So this is not a search that failed and might succeed on a retry. It is a tripwire on a
    publisher decision: the day CMS brings the dataset back, this fails, and the re-scope
    recorded in the health README is worth revisiting.
    """
    with httpx.Client(
        timeout=120, headers={"User-Agent": USER_AGENT}, follow_redirects=True
    ) as client:
        titles = [(d.get("title") or "").lower() for d in fetch_catalog(client)]
    assert not [t for t in titles if "chronic condition" in t], (
        "CMS has republished a chronic-conditions dataset; HLT-E2 and HLT-E3 can go back to "
        "asking about dementia directly"
    )
