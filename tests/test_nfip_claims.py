"""Unit tests for the NFIP claims fetcher.

The FEMA catalogue tests that used to live here moved to `test_openfema.py` with the
code. What is left is what claims itself owns: the all-varchar rule for its landing
file, and the manifest it hands L1.
"""

from ingest.common import load_state_codes
from ingest.insurance.nfip_claims import (
    HIVE_NULL_PARTITION,
    manifest_payload,
    source_relation,
    sql_literal,
)

PARQUET_URL = "https://example.gov/NfipClaimsV3.parquet"


def test_csv_relation_disables_type_inference() -> None:
    """The all-varchar rule. Parquet carries its own schema and is left alone."""
    assert "all_varchar=true" in source_relation("/tmp/x.csv", "csv")
    assert "all_varchar" not in source_relation("/tmp/x.parquet", "parquet")


def test_sql_literal_normalizes_separators_and_escapes_quotes() -> None:
    assert sql_literal(r"F:\data\raw\x.parquet") == "'F:/data/raw/x.parquet'"
    assert sql_literal("it's.parquet") == "'it''s.parquet'"


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
        resolved_url=PARQUET_URL,
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


def test_manifest_names_the_nfip_unknown_state_code_it_kept() -> None:
    """NFIP files carry state='UN' for claims whose state is unavailable -- ~16k rows.
    They stay in raw, and the manifest says so, so they cannot vanish into a bad join."""
    manifest = _payload({"MI": 14_938, "PR": 25_386, "UN": 16_441})
    assert manifest["nonstandard_partition_keys"] == ["UN"]
    assert manifest["nonstandard_partition_rows"] == 16_441
