"""Unit tests for the shared ingestion utilities extracted in chore/shared-ingest.

These lock the behaviour both fetchers already depended on before the extraction.
"""

import json
from pathlib import Path

import pytest
from ingest.common import (
    HIVE_NULL_PARTITION,
    REPO_ROOT,
    base_manifest,
    human_bytes,
    paths_for,
    read_existing_manifest,
    sha256_of,
    sql_literal,
    write_json,
)


def test_sql_literal_normalizes_separators_and_escapes_quotes() -> None:
    assert sql_literal(r"F:\data\raw\x.parquet") == "'F:/data/raw/x.parquet'"
    assert sql_literal("it's.parquet") == "'it''s.parquet'"


@pytest.mark.parametrize(
    ("count", "expected"),
    [(163_700_000, "163.7 MB"), (1_412_520_286, "1.41 GB"), (0, "0.0 MB")],
)
def test_human_bytes_switches_unit_at_a_gigabyte(count: int, expected: str) -> None:
    assert human_bytes(count) == expected


def test_fixture_paths_are_isolated_from_live_data() -> None:
    """A fixture run must never be able to overwrite real raw data or the real database."""
    live_root, live_db = paths_for("live")
    fixture_root, fixture_db = paths_for("fixture")
    assert live_root != fixture_root
    assert live_db != fixture_db
    assert fixture_root.is_relative_to(live_root)
    assert live_root == REPO_ROOT / "data"


def test_write_json_is_byte_stable_across_runs(tmp_path: Path) -> None:
    """Committed artifacts must not churn: LF endings, trailing newline, sorted keys."""
    target = tmp_path / "out.json"
    payload = {"b": 2, "a": [1, {"z": 0, "y": 1}]}

    write_json(target, payload)
    first = target.read_bytes()
    write_json(target, json.loads(first.decode("utf-8")))

    assert target.read_bytes() == first
    assert b"\r\n" not in first
    assert first.endswith(b"\n")
    assert first.index(b'"a"') < first.index(b'"b"')


def test_sha256_of_matches_hashlib(tmp_path: Path) -> None:
    import hashlib

    target = tmp_path / "blob.bin"
    target.write_bytes(b"x" * (3 * 1024 * 1024 + 7))  # spans several read chunks
    assert sha256_of(target) == hashlib.sha256(target.read_bytes()).hexdigest()


def test_missing_or_corrupt_manifest_reads_as_none(tmp_path: Path) -> None:
    assert read_existing_manifest(tmp_path) is None
    (tmp_path / "manifest.json").write_text("{ not json", encoding="utf-8")
    assert read_existing_manifest(tmp_path) is None


def _manifest(partition_rows: dict[str, int], nonstandard: dict[str, int]):
    return base_manifest(
        track="insurance",
        source="nfip_claims",
        publisher="OpenFEMA",
        dataset_name="NfipClaims",
        resolved_url="https://example.gov/x.parquet",
        distribution_format="parquet",
        retrieved_at="2026-08-11T21:00:00Z",
        landing_files=[{"name": "x.parquet", "bytes": 1, "sha256": "abc"}],
        source_reported_count=100,
        source_last_refresh="2026-08-04T09:21:02.984Z",
        duckdb_table="raw.ins_nfip_claims",
        rows_landing=99,
        rows_raw=99,
        rows_duckdb=99,
        partition_column="state",
        partition_rows=partition_rows,
        nonstandard_partitions=nonstandard,
    )


def test_base_manifest_carries_the_count_chain_l1_reads() -> None:
    manifest = _manifest({"MI": 50, "PR": 49}, {})
    assert manifest["source_reported_count"] == 100
    assert (manifest["rows_landing"], manifest["rows_raw"], manifest["rows_duckdb"]) == (99, 99, 99)
    assert manifest["duckdb_table"] == "raw.ins_nfip_claims"


def test_base_manifest_derives_null_and_nonstandard_partition_totals() -> None:
    manifest = _manifest({"MI": 50, HIVE_NULL_PARTITION: 4, "UN": 7}, {"UN": 7})
    assert manifest["null_partition_rows"] == 4
    assert manifest["nonstandard_partition_keys"] == ["UN"]
    assert manifest["nonstandard_partition_rows"] == 7


def test_base_manifest_reports_zero_when_nothing_is_unusual() -> None:
    manifest = _manifest({"MI": 50}, {})
    assert manifest["null_partition_rows"] == 0
    assert manifest["nonstandard_partition_keys"] == []
    assert manifest["nonstandard_partition_rows"] == 0
