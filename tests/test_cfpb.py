"""Unit tests for the CFPB complaints fetcher.

Everything here is offline except the one `slow` test, which CI never runs.
"""

from pathlib import Path

import duckdb
import httpx
import pytest
from ingest.fintech.cfpb import (
    SEARCH_API,
    DiscoveryError,
    discover_bulk_url,
    find_bulk_url,
    find_date_column,
    manifest_payload,
    nonstandard_partitions,
    source_relation,
    source_reported_count,
)

BULK = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"


def test_bulk_link_is_discovered_from_page_markup() -> None:
    html = f'<a href="/foo">x</a><a href="{BULK}">Download</a>'
    assert find_bulk_url(html) == BULK


def test_repeated_links_are_the_same_link() -> None:
    assert find_bulk_url(f'<a href="{BULK}">a</a><a href="{BULK}">b</a>') == BULK


def test_missing_link_fails_loudly_rather_than_falling_back_to_a_guess() -> None:
    with pytest.raises(DiscoveryError) as caught:
        find_bulk_url("<html>the page was redesigned</html>")
    assert "CFPB data page" in str(caught.value)


def test_ambiguous_links_fail_rather_than_picking_one() -> None:
    html = (
        f'<a href="{BULK}">a</a><a href="https://files.consumerfinance.gov/ccdb/old.csv.zip">b</a>'
    )
    with pytest.raises(DiscoveryError):
        find_bulk_url(html)


@pytest.mark.parametrize(
    "header", ["Date received", "date_received", "DATE RECEIVED", "Date_Received"]
)
def test_date_column_is_discovered_not_assumed(header: str) -> None:
    """Bulk CSV headers are human-readable; the API uses snake_case. Discover, don't guess."""
    assert find_date_column(["Product", header, "State"]) == header


def test_missing_date_column_fails_loudly() -> None:
    with pytest.raises(DiscoveryError):
        find_date_column(["Product", "State", "Company"])


def test_plausible_years_are_standard_partitions() -> None:
    assert nonstandard_partitions({"2011": 5, "2020": 7, "2024": 9}) == {}


def test_blank_and_impossible_dates_are_surfaced_not_dropped() -> None:
    flagged = nonstandard_partitions({"2020": 7, "": 3, "1899": 1, "9999": 2, "n/a": 4})
    assert flagged == {"": 3, "1899": 1, "9999": 2, "n/a": 4}


def test_csv_relation_disables_type_inference() -> None:
    assert "all_varchar=true" in source_relation(Path("/tmp/complaints.csv"))


def test_quoted_newlines_in_narratives_are_rows_not_lines(tmp_path: Path) -> None:
    """Narratives contain embedded newlines. This is why row counts come from SQL.

    The file below is 5 physical lines but 2 records; counting lines would overstate
    the row count by 150%.
    """
    csv_path = tmp_path / "narratives.csv"
    csv_path.write_text(
        "Date received,Consumer complaint narrative\n"
        '2024-01-05,"I called them.\nThey hung up.\n\nThen nothing."\n'
        '2024-02-06,"Single line."\n',
        encoding="utf-8",
        newline="",
    )
    assert len(csv_path.read_text(encoding="utf-8").splitlines()) == 6

    con = duckdb.connect()
    rows = con.execute(f"SELECT count(*) FROM {source_relation(csv_path)}").fetchone()[0]
    narrative = con.execute(
        f'SELECT "Consumer complaint narrative" FROM {source_relation(csv_path)} ORDER BY 1 LIMIT 1'
    ).fetchone()[0]
    con.close()

    assert rows == 2
    assert "\n" in narrative


def _payload(partitions: dict[str, int]):
    return manifest_payload(
        resolved_url=BULK,
        http_metadata={
            "content_length": 1412520286,
            "last_modified": "Tue, 11 Aug 2026 09:30:31 GMT",
            "etag": '"abc-169"',
        },
        landing_files=[{"name": "complaints.csv.zip", "bytes": 10, "sha256": "deadbeef"}],
        source_reported_count=17021062,
        source_reported_michigan=366735,
        date_column="Date received",
        rows_landing=100,
        rows_raw=100,
        rows_duckdb=100,
        partition_rows=partitions,
        retrieved_at="2026-08-11T21:00:00Z",
    )


def test_manifest_carries_the_count_chain_and_the_refresh_key() -> None:
    manifest = _payload({"2024": 60, "2025": 40})
    assert manifest["source_reported_count"] == 17021062
    assert manifest["source_reported_michigan"] == 366735
    assert manifest["source_last_refresh"] == "Tue, 11 Aug 2026 09:30:31 GMT"
    assert (manifest["rows_landing"], manifest["rows_raw"], manifest["rows_duckdb"]) == (
        100,
        100,
        100,
    )


def test_manifest_records_the_coverage_caveats_that_gate_reconciliation() -> None:
    """FIN-E1 and FIN-E3 are wrong if these are forgotten, so raw carries them."""
    caveats = " ".join(_payload({"2024": 100})["coverage_caveats"]).lower()
    assert "consent" in caveats
    assert "15 days" in caveats
    assert "$10b" in caveats


@pytest.mark.slow
def test_live_discovery_and_count_endpoint() -> None:
    """Hits CFPB. Excluded from CI, which never touches the network."""
    with httpx.Client(timeout=120, follow_redirects=True) as client:
        url = discover_bulk_url(client)
        total = source_reported_count(client)
        michigan = source_reported_count(client, state="MI")

        # no_highlight=true is load-bearing: without it the endpoint 404s.
        bare = client.get(SEARCH_API, params={"size": "1", "format": "json"})

    assert url.endswith(".csv.zip")
    assert total > 5_000_000
    assert 0 < michigan < total
    assert bare.status_code == 404
