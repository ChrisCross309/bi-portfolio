"""Unit tests for the CDC healthy-aging fetcher.

The two that matter most are the ones a plausible implementation gets wrong: the catalogue
search returns three things that are not the dataset, and the publisher's `rowid` column is
not a key. Both are locked here.

Everything is offline except the `slow` tests, which CI never runs. The fixture-backed
tests read the committed sample with DuckDB and touch no network.
"""

import duckdb
import httpx
import pytest
from ingest.common import USER_AGENT, DiscoveryError, load_state_codes, nonstandard_partitions
from ingest.health.cdc import (
    CATALOG_API,
    DATASET_NAME,
    DOMAIN,
    GRAIN_COLUMNS,
    PAGE_SIZE,
    RESOURCE_API,
    SOURCE,
    discover_dataset,
    epoch_to_iso,
    expected_pages,
    grain_failures,
    grain_key_sql,
    manifest_payload,
    pull_failures,
    select_dataset,
    source_relation,
    verify_pull,
)

FIXTURE = "tests/fixtures/health/cdc_healthy_aging.csv"

# The catalogue really returns all four of these for this search.
DATASET = {
    "resource": {"id": "hfr9-rurv", "name": DATASET_NAME, "type": "dataset"},
    "metadata": {"domain": DOMAIN},
}
HHS_HREF = {
    "resource": {"id": "dtq5-yrhg", "name": DATASET_NAME, "type": "href"},
    "metadata": {"domain": "datahub.hhs.gov"},
}
FILTER_VIEW = {
    "resource": {
        "id": "jhd5-u276",
        "name": "Alzheimer's Disease and Healthy Aging Indicators: Cognitive Decline",
        "type": "filter",
    },
    "metadata": {"domain": DOMAIN},
}
UNRELATED = {
    "resource": {"id": "hc5t-p62z", "name": "Asian American Quality of Life", "type": "dataset"},
    "metadata": {"domain": "datahub.austintexas.gov"},
}


def _fixture_relation() -> str:
    return source_relation(FIXTURE)


# ── discovery ─────────────────────────────────────────────────────────────────


def test_the_dataset_is_picked_out_of_its_own_near_misses() -> None:
    """A filter view carries a subset of rows and an href is not data at all."""
    assert select_dataset([HHS_HREF, FILTER_VIEW, DATASET, UNRELATED])["id"] == "hfr9-rurv"


def test_a_same_named_dataset_on_another_domain_is_not_accepted() -> None:
    with pytest.raises(DiscoveryError) as caught:
        select_dataset([HHS_HREF, FILTER_VIEW, UNRELATED])
    assert "datahub.hhs.gov" in str(caught.value)
    assert "got 0" in str(caught.value)


def test_two_matching_datasets_stop_the_run_rather_than_picking_one() -> None:
    with pytest.raises(DiscoveryError) as caught:
        select_dataset([DATASET, DATASET])
    assert "got 2" in str(caught.value)


def test_socrata_epoch_timestamps_become_iso() -> None:
    assert epoch_to_iso(1739553542) == "2025-02-14T17:19:02Z"
    assert epoch_to_iso(None) is None


# ── paging ────────────────────────────────────────────────────────────────────


def test_page_count_comes_from_the_publishers_own_total() -> None:
    assert expected_pages(284_142) == 6
    assert expected_pages(100_000) == 2
    assert expected_pages(1) == 1


def test_a_reported_total_of_zero_is_refused() -> None:
    """An over-the-end offset returns 200 with a header, so a zero total means the count
    endpoint changed shape -- not that the dataset is empty."""
    with pytest.raises(DiscoveryError):
        expected_pages(0)


def _verification(**overrides):
    defaults = {
        "page_rows": [PAGE_SIZE] * 5 + [34_142],
        "reported_total": 284_142,
        "refresh_before": "2025-02-14T17:19:02Z",
        "refresh_after": "2025-02-14T17:19:02Z",
    }
    return verify_pull(**{**defaults, **overrides})


def test_a_complete_pull_reports_no_problems() -> None:
    verification = _verification()
    assert verification["rows"] == 284_142
    assert verification["pages"] == 6
    assert pull_failures(verification) == []


def test_a_short_pull_is_caught_against_the_reported_total() -> None:
    problems = pull_failures(_verification(page_rows=[PAGE_SIZE] * 5))
    assert any("short or doubled" in problem for problem in problems)


def test_a_truncated_middle_page_is_caught() -> None:
    problems = pull_failures(_verification(page_rows=[PAGE_SIZE, 40_000, 194_142]))
    assert any("truncated a page" in problem for problem in problems)


def test_a_republication_mid_pull_invalidates_offset_paging() -> None:
    """Offset paging's one real weakness, so it is asserted rather than hoped for."""
    problems = pull_failures(_verification(refresh_after="2026-08-11T00:00:00Z"))
    assert len(problems) == 1
    assert "republished mid-pull" in problems[0]


# ── grain ─────────────────────────────────────────────────────────────────────


def test_grain_key_is_a_struct_so_no_separator_can_collide() -> None:
    """Values in this dataset contain '~' -- rowid is full of it -- so a concatenated key
    would be ambiguous."""
    sql = grain_key_sql()
    for column in GRAIN_COLUMNS:
        assert column in sql
    assert "~" not in sql
    assert "concat" not in sql.lower()


def test_grain_failure_names_the_columns_it_checked() -> None:
    assert grain_failures(284_142, 284_142) == []
    problems = grain_failures(284_142, 36_046)
    assert "248,096 rows share a grain key" in problems[0]
    assert "stratificationid2" in problems[0]


def test_the_fixture_preserves_the_grain_invariant_the_loader_asserts() -> None:
    con = duckdb.connect()
    relation = _fixture_relation()
    rows = con.execute(f"SELECT count(*) FROM {relation}").fetchone()[0]
    keys = con.execute(f"SELECT count(DISTINCT {grain_key_sql()}) FROM {relation}").fetchone()[0]
    con.close()
    assert rows == keys
    assert grain_failures(rows, keys) == []


def test_the_fixture_shows_rowid_is_not_unique() -> None:
    """The trap, demonstrated on committed data rather than only described in a comment."""
    con = duckdb.connect()
    relation = _fixture_relation()
    rows, distinct_rowids = con.execute(
        f"SELECT count(*), count(DISTINCT rowid) FROM {relation}"
    ).fetchone()
    con.close()
    assert distinct_rowids < rows


# ── the mixed second stratification, and the rollups ──────────────────────────


def test_the_fixture_keeps_all_three_shapes_of_the_second_stratification() -> None:
    con = duckdb.connect()
    categories = dict(
        con.execute(
            f"SELECT COALESCE(stratificationcategory2, '(null)'), count(*) "
            f"FROM {_fixture_relation()} GROUP BY 1"
        ).fetchall()
    )
    con.close()
    assert {"Race/Ethnicity", "Sex", "(null)"} <= set(categories)
    # The category is NULL exactly where the value is the ungrouped total.
    con = duckdb.connect()
    mismatched = con.execute(
        f"SELECT count(*) FROM {_fixture_relation()} "
        "WHERE (stratificationcategory2 IS NULL) <> (stratificationid2 = 'OVERALL')"
    ).fetchone()[0]
    con.close()
    assert mismatched == 0


def test_rollup_locations_are_flagged_by_the_state_reference() -> None:
    """US and the four census regions sit in the same column as states; summing across
    locationabbr double counts. The state reference is what surfaces them."""
    con = duckdb.connect()
    counts = dict(
        con.execute(
            f"SELECT locationabbr, count(*) FROM {_fixture_relation()} GROUP BY 1"
        ).fetchall()
    )
    con.close()
    rollups = nonstandard_partitions(counts, load_state_codes())
    assert set(rollups) == {"US", "MDW", "NRE", "SOU", "WEST"}
    assert "MI" in counts


def test_suppressed_cells_survive_as_missing_values_not_as_zero() -> None:
    con = duckdb.connect()
    relation = _fixture_relation()
    empty_strings, suppressed, symbols = con.execute(
        f"""SELECT count(*) FILTER (WHERE data_value = ''),
                   count(*) FILTER (WHERE data_value IS NULL),
                   count(DISTINCT data_value_footnote_symbol)
            FROM {relation}"""
    ).fetchone()
    con.close()
    assert empty_strings == 0
    assert suppressed > 0
    assert symbols >= 5  # ****, &, ~, #, ** all present in the sample


def test_csv_relation_disables_type_inference() -> None:
    assert "all_varchar=true" in source_relation("/tmp/page-00001.csv")


# ── manifest ──────────────────────────────────────────────────────────────────


def _manifest():
    return manifest_payload(
        dataset={"id": "hfr9-rurv"},
        view={
            "name": DATASET_NAME,
            "attribution": "CDC Division of Population Health",
            "category": "Healthy Aging",
            "rowsUpdatedAt": 1739553542,
            "viewLastModified": 1739553795,
            "columns": [{"fieldName": f"c{i}"} for i in range(33)],
        },
        landing_files=[{"name": "page-00001.csv", "bytes": 1, "sha256": "abc"}],
        verification=_verification(),
        distinct_grain_keys=284_142,
        rows_landing=284_142,
        rows_raw=284_142,
        rows_duckdb=284_142,
        partition_rows={"MI": 5113, "OH": 4700, "US": 6132, "MDW": 6099, "WEST": 6126},
        retrieved_at="2026-08-11T22:00:00Z",
    )


def test_the_manifest_names_the_rollups_it_kept() -> None:
    manifest = _manifest()
    assert manifest["rollup_rows"]["keys"] == ["MDW", "US", "WEST"]
    assert manifest["rollup_rows"]["rows"] == 18_357
    assert "double counts" in manifest["rollup_rows"]["note"]
    assert manifest["nonstandard_partition_keys"] == ["MDW", "US", "WEST"]


def test_the_manifest_warns_off_the_rowid_column() -> None:
    grain = _manifest()["grain"]
    assert grain["columns"] == list(GRAIN_COLUMNS)
    assert "is not unique" in grain["rowid_is_not_a_key"]
    assert "do not join on it" in grain["rowid_is_not_a_key"]
    assert "cell, not a person" in grain["note"]
    assert (
        "average a race breakdown against a sex breakdown"
        in (grain["second_stratification_is_mixed"])
    )


def test_the_manifest_records_that_this_source_has_no_county_grain() -> None:
    """CLAUDE.md wants MI at county grain; this dataset cannot supply it, so it says so."""
    caveats = " ".join(_manifest()["coverage_caveats"])
    assert "no county grain" in caveats
    assert "CMS sources" in caveats
    assert "Suppression is information, not a null" in caveats


def test_the_manifest_carries_the_count_chain_and_the_refresh_signal() -> None:
    manifest = _manifest()
    assert manifest["source_reported_count"] == 284_142
    assert manifest["source_last_refresh"] == "2025-02-14T17:19:02Z"
    assert manifest["dataset_identifier"] == "hfr9-rurv"
    assert manifest["columns_in_view_metadata"] == 33
    assert manifest["verification"]["refresh_stable_during_pull"] is True


# ── live endpoint behaviour (excluded from CI) ─────────────────────────────────


@pytest.mark.slow
def test_live_catalogue_still_resolves_exactly_one_dataset() -> None:
    with httpx.Client(
        timeout=60, headers={"User-Agent": USER_AGENT}, follow_redirects=True
    ) as client:
        dataset = discover_dataset(client)
    assert dataset["id"] == "hfr9-rurv", "the four-by-four moved; raw paths follow the id"


@pytest.mark.slow
def test_live_over_the_end_offset_returns_a_header_and_no_rows() -> None:
    """Why the paging loop is driven by the reported count instead of an empty-page check."""
    with httpx.Client(
        timeout=60, headers={"User-Agent": USER_AGENT}, follow_redirects=True
    ) as client:
        response = client.get(
            f"{RESOURCE_API}/hfr9-rurv.csv",
            params={"$order": ":id", "$limit": "1000", "$offset": "999999"},
        )
    assert response.status_code == 200
    assert len(response.text.splitlines()) == 1  # a header, and nothing else


@pytest.mark.slow
def test_live_catalogue_search_still_returns_the_near_misses_we_filter_out() -> None:
    """If the publisher ever stops shipping the same-named href and the filter views, the
    filtering in select_dataset is over-engineering and can be simplified."""
    with httpx.Client(
        timeout=60, headers={"User-Agent": USER_AGENT}, follow_redirects=True
    ) as client:
        results = client.get(CATALOG_API, params={"q": DATASET_NAME, "limit": "20"}).json()[
            "results"
        ]
    types = {result["resource"]["type"] for result in results}
    assert {"dataset", "href", "filter"} <= types
    assert SOURCE == "cdc_healthy_aging"
