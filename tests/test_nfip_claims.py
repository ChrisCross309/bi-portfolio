"""Unit tests for the NFIP claims fetcher.

Everything here is offline except the one `slow` test, which CI never runs.
"""

from datetime import UTC, datetime

import httpx
import pytest
from ingest.common import USER_AGENT
from ingest.insurance.nfip_claims import (
    DATASET,
    HIVE_NULL_PARTITION,
    DiscoveryError,
    deprecation_notice,
    discover_dataset,
    load_state_codes,
    manifest_payload,
    nonstandard_partitions,
    select_distribution,
    source_relation,
    sql_literal,
)

PARQUET = {"format": "parquet", "accessURL": "https://example.gov/NfipClaimsV3.parquet"}
CSV = {"format": "csv", "accessURL": "https://example.gov/NfipClaimsV3.csv"}


def test_parquet_is_preferred_over_csv() -> None:
    assert select_distribution([CSV, PARQUET]) == ("parquet", PARQUET["accessURL"])


def test_csv_is_the_fallback() -> None:
    assert select_distribution([CSV]) == ("csv", CSV["accessURL"])


def test_format_matching_ignores_case_and_padding() -> None:
    assert select_distribution([{"format": " Parquet ", "accessURL": "u"}]) == ("parquet", "u")


def test_missing_distribution_fails_loudly_with_the_raw_block() -> None:
    """We stop and show the response rather than guessing a URL."""
    with pytest.raises(DiscoveryError) as caught:
        select_distribution([{"format": "gdb", "accessURL": "https://example.gov/x.gdb.zip"}])
    assert "gdb" in str(caught.value)


def test_entries_without_a_url_are_not_selectable() -> None:
    with pytest.raises(DiscoveryError):
        select_distribution([{"format": "parquet", "accessURL": None}])


def test_live_dataset_has_no_deprecation_notice() -> None:
    assert deprecation_notice({"name": "NfipClaims", "version": 3}) is None


def test_deprecated_dataset_reports_days_remaining() -> None:
    """The trap that cost us a plan revision: v2 was frozen and scheduled for deletion."""
    notice = deprecation_notice(
        {
            "name": "FimaNfipClaims",
            "version": 2,
            "depDate": "2026-10-15T00:00:00.000Z",
            "depNewURL": "https://www.fema.gov/openfema-data-page/nfip-redacted-claims-v3",
            "depApiMessage": "Data is frozen as of 06/01/2026.",
        },
        now=datetime(2026, 8, 11, tzinfo=UTC),
    )
    assert notice is not None
    assert "DEPRECATED" in notice
    assert "2026-10-15" in notice
    assert "65 days" in notice
    assert "nfip-redacted-claims-v3" in notice


def test_csv_relation_disables_type_inference() -> None:
    """The all-varchar rule. Parquet carries its own schema and is left alone."""
    assert "all_varchar=true" in source_relation("/tmp/x.csv", "csv")
    assert "all_varchar" not in source_relation("/tmp/x.parquet", "parquet")


def test_sql_literal_normalizes_separators_and_escapes_quotes() -> None:
    assert sql_literal(r"F:\data\raw\x.parquet") == "'F:/data/raw/x.parquet'"
    assert sql_literal("it's.parquet") == "'it''s.parquet'"


def test_state_reference_recognises_real_states_and_flags_the_nfip_unknown_code() -> None:
    """NFIP files carry state='UN' for claims whose state is unavailable -- ~16k rows."""
    known = load_state_codes()
    assert {"MI", "PR", "DC"} <= known
    assert "UN" not in known
    assert nonstandard_partitions({"MI": 14938, "PR": 25386, "UN": 16441}, known) == {"UN": 16441}


def _payload(partitions: dict[str, int]):
    return manifest_payload(
        known_partition_keys=load_state_codes(),
        dataset={
            "name": "NfipClaims",
            "version": 3,
            "identifier": "openfema-99",
            "recordCount": 10,
            "lastDataSetRefresh": "2026-08-04T09:21:02.984Z",
            "hash": "abc",
        },
        distribution_format="parquet",
        resolved_url=PARQUET["accessURL"],
        landing_file="NfipClaimsV3.parquet",
        landing_bytes=123,
        landing_sha256="deadbeef",
        rows_landing=10,
        rows_raw=10,
        rows_duckdb=10,
        partition_rows=partitions,
        retrieved_at="2026-08-11T20:00:00Z",
    )


def test_manifest_carries_the_count_chain_l1_will_check() -> None:
    manifest = _payload({"MI": 4, "PR": 1})
    assert manifest["source_reported_count"] == 10
    assert (manifest["rows_landing"], manifest["rows_raw"], manifest["rows_duckdb"]) == (10, 10, 10)
    assert manifest["landing_files"][0]["sha256"] == "deadbeef"
    assert manifest["deprecation"] is None


def test_manifest_surfaces_null_state_rows_rather_than_hiding_them() -> None:
    manifest = _payload({"MI": 4, HIVE_NULL_PARTITION: 3})
    assert manifest["null_partition_rows"] == 3


@pytest.mark.slow
def test_live_discovery_still_offers_a_parquet_bulk_file() -> None:
    """Hits OpenFEMA. Excluded from CI, which never touches the network."""
    with httpx.Client(
        timeout=60, headers={"User-Agent": USER_AGENT}, follow_redirects=True
    ) as client:
        dataset = discover_dataset(client, DATASET)

    fmt, url = select_distribution(dataset["distribution"])
    assert fmt == "parquet"
    assert url.startswith("https://")
    assert deprecation_notice(dataset) is None, "NfipClaims v3 has been deprecated too"
