"""Unit tests for the shared ingestion utilities extracted in chore/shared-ingest.

These lock the behaviour both fetchers already depended on before the extraction.
"""

import gzip
import hashlib
import json
from pathlib import Path

import duckdb
import httpx
import pytest
from ingest import common
from ingest.common import (
    HIVE_NULL_PARTITION,
    REPO_ROOT,
    TransientHTTPError,
    base_manifest,
    human_bytes,
    load_state_codes,
    nonstandard_partitions,
    paths_for,
    raw_relation,
    read_existing_manifest,
    repartition_to_raw,
    sha256_of,
    skip_as_current,
    sql_literal,
    stream_download,
    tables_present,
    write_baseline,
    write_json,
)
from tenacity import stop_after_attempt


def _silent(message: str) -> None:
    """stream_download logs progress; these tests do not care about it."""


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


def _mock_client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_a_compressed_body_is_not_mistaken_for_a_truncated_one(tmp_path: Path) -> None:
    """CMS serves its geographic-variation CSV gzipped, with content-length describing the
    compressed body while we count decoded bytes. Comparing the two failed every attempt."""
    payload = b"col_a,col_b\n" + b"26001,11949.75\n" * 5_000
    compressed = gzip.compress(payload)
    assert len(compressed) < len(payload)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-encoding": "gzip", "content-length": str(len(compressed))},
            content=compressed,
        )

    target = tmp_path / "gv.csv"
    with _mock_client(handler) as client:
        total, digest = stream_download(client, "https://example.gov/gv.csv", target, log=_silent)

    assert total == len(payload)
    assert target.read_bytes() == payload
    assert digest == hashlib.sha256(payload).hexdigest()


def test_a_truncated_uncompressed_body_is_still_caught(tmp_path: Path) -> None:
    """The guard above must not have disarmed the check it was narrowing."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"content-length": "999999"}, content=b"short")

    # One attempt only: the retry policy would otherwise back off for ~30s on the way to
    # the same failure.
    single_attempt = stream_download.retry_with(stop=stop_after_attempt(1))
    with _mock_client(handler) as client, pytest.raises(TransientHTTPError) as caught:
        single_attempt(client, "https://example.gov/x.csv", tmp_path / "x.csv", log=_silent)

    assert "Truncated download" in str(caught.value)
    assert not (tmp_path / "x.csv").exists()
    assert not (tmp_path / "x.csv.part").exists()


def test_sha256_of_matches_hashlib(tmp_path: Path) -> None:
    import hashlib

    target = tmp_path / "blob.bin"
    target.write_bytes(b"x" * (3 * 1024 * 1024 + 7))  # spans several read chunks
    assert sha256_of(target) == hashlib.sha256(target.read_bytes()).hexdigest()


def test_missing_or_corrupt_manifest_reads_as_none(tmp_path: Path) -> None:
    assert read_existing_manifest(tmp_path) is None
    (tmp_path / "manifest.json").write_text("{ not json", encoding="utf-8")
    assert read_existing_manifest(tmp_path) is None


@pytest.mark.parametrize(
    ("key", "column"),
    [
        ("2011", "year"),  # cfpb_complaints, and ref_cpi_u
        ("2018", "activity_year"),  # hmda_lar
        ("2024", "vintage"),  # ref_acs5_detailed and ref_acs5_subject
        ("2023", "period"),  # hmda_institutions -- missed by the original defect report
        ("MI", "state"),  # the keys that were never at risk, still VARCHAR
    ],
)
def test_partition_keys_read_back_as_varchar_whatever_they_look_like(
    tmp_path: Path, key: str, column: str
) -> None:
    """The all-varchar rule holds on read, not just on write (CLAUDE.md rule 2).

    A numeric-looking directory name used to come back BIGINT. Nothing in the parquet was
    wrong -- DuckDB re-typed the key while reconstructing it from the path.
    """
    raw_dir = tmp_path / "raw" / "shared" / "sample"
    raw_dir.parent.mkdir(parents=True)
    con = duckdb.connect()
    try:
        con.execute(
            f"COPY (SELECT '{key}' AS {column}, 'x' AS payload) TO '{raw_dir.as_posix()}' "
            f"(FORMAT parquet, PARTITION_BY ({column}))"
        )
        types = {
            row[0]: row[1]
            for row in con.execute(
                f"DESCRIBE SELECT * FROM {raw_relation(raw_dir)} LIMIT 0"
            ).fetchall()
        }
        assert types[column] == "VARCHAR"
        # And the value survives as written, not as a number that lost its shape.
        assert con.execute(f"SELECT {column} FROM {raw_relation(raw_dir)}").fetchone()[0] == key
    finally:
        con.close()


def test_repartition_keeps_the_manifest_the_swap_would_have_destroyed(tmp_path: Path) -> None:
    """`manifest.json` lives inside the tree truncate-and-reload replaces.

    Every fetcher writes a fresh manifest after its own checks pass, but a failure between
    the swap and that write used to leave raw parquet with no provenance at all -- found by
    interrupting a live HMDA run during `load_table`.
    """
    raw_dir = tmp_path / "raw" / "insurance" / "nfip_claims"
    raw_dir.mkdir(parents=True)
    (raw_dir / "manifest.json").write_text('{"rows_duckdb": 3}', encoding="utf-8")

    con = duckdb.connect()
    try:
        for expected in (3, 5):
            rows_landing, rows_raw = repartition_to_raw(
                con,
                relation=f"(SELECT 'MI' AS state, i AS n FROM range({expected}) t(i))",
                raw_dir=raw_dir,
                partition_column="state",
            )
            assert (rows_landing, rows_raw) == (expected, expected)
            assert read_existing_manifest(raw_dir) == {"rows_duckdb": 3}
    finally:
        con.close()


# ── the currency guard ────────────────────────────────────────────────────────


def _database_with(tmp_path: Path, *tables: str) -> Path:
    db_path = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        for table in tables:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {table.split('.')[0]}")
            con.execute(f"CREATE TABLE {table} AS SELECT 1 AS x")
    finally:
        con.close()
    return db_path


def test_tables_present_is_false_for_a_database_that_does_not_exist(tmp_path: Path) -> None:
    assert tables_present(tmp_path / "nothing.duckdb", "raw.ins_nfip_claims") is False


def test_tables_present_is_false_when_any_named_table_is_missing(tmp_path: Path) -> None:
    db_path = _database_with(tmp_path, "raw.ref_cpi_u")
    assert tables_present(db_path, "raw.ref_cpi_u") is True
    # BLS gates two tables from one manifest; one missing must sink the pair.
    assert tables_present(db_path, "raw.ref_cpi_u", "raw.ref_cpi_u_series") is False


def test_tables_present_is_false_for_a_file_that_is_not_a_database(tmp_path: Path) -> None:
    """A corrupt warehouse is exactly the case a re-run must not skip."""
    corrupt = tmp_path / "corrupt.duckdb"
    corrupt.write_bytes(b"not a duckdb file at all")
    assert tables_present(corrupt, "raw.ins_nfip_claims") is False


def test_skip_as_current_requires_both_a_current_manifest_and_the_table(tmp_path: Path) -> None:
    db_path = _database_with(tmp_path, "raw.ins_nfip_claims")
    lines: list[str] = []
    call = {
        "db_path": db_path,
        "tables": ("raw.ins_nfip_claims",),
        "log": lines.append,
    }

    assert skip_as_current(force=False, publisher_unchanged=True, **call) is True
    assert lines == []
    assert skip_as_current(force=False, publisher_unchanged=False, **call) is False
    assert skip_as_current(force=True, publisher_unchanged=True, **call) is False
    # Nothing above should have needed to explain itself.
    assert lines == []


def test_skip_as_current_explains_itself_when_the_table_is_gone(tmp_path: Path) -> None:
    """The defect this exists for: current manifests plus a deleted database used to
    report SKIPPED for every source and leave an empty warehouse behind."""
    lines: list[str] = []
    assert (
        skip_as_current(
            force=False,
            publisher_unchanged=True,
            db_path=tmp_path / "deleted.duckdb",
            tables=("raw.ins_nfip_claims",),
            log=lines.append,
        )
        is False
    )
    assert len(lines) == 1
    assert "raw.ins_nfip_claims" in lines[0]
    assert "just reload" in lines[0]


def test_state_reference_holds_states_dc_and_territories_but_no_invented_codes() -> None:
    known = load_state_codes()
    assert {"MI", "DC", "PR", "GU"} <= known
    assert len(known) == 56  # 50 states + DC + 5 territories
    assert "UN" not in known


def test_nonstandard_partitions_flags_only_keys_outside_the_reference() -> None:
    """The NFIP case that motivated it: state='UN' is a real code FEMA uses and no
    state list contains, so it must be reported rather than silently dropped."""
    known = load_state_codes()
    assert nonstandard_partitions({"MI": 14_938, "PR": 25_386}, known) == {}
    assert nonstandard_partitions({"MI": 14_938, "UN": 16_441}, known) == {"UN": 16_441}


def test_nonstandard_partitions_returns_keys_in_sorted_order() -> None:
    """The manifest is a committed artifact; a set's iteration order would churn it."""
    flagged = nonstandard_partitions({"ZZ": 1, "UN": 2, "XX": 3}, frozenset())
    assert list(flagged) == ["UN", "XX", "ZZ"]


def test_write_baseline_names_files_per_source_and_kind(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Field metadata and documentation pointers share one naming scheme and one writer."""
    monkeypatch.setattr(common, "BASELINE_DIR", tmp_path / "baselines")

    fields = write_baseline("insurance", "nfip_claims", [{"name": "state"}])
    pointer = write_baseline("fintech", "cfpb_complaints", {"field_reference": "x"}, kind="pointer")

    assert fields.name == "insurance__nfip_claims__fields.json"
    assert pointer.name == "fintech__cfpb_complaints__pointer.json"
    # Proves the monkeypatch took: without it this test would rewrite committed baselines.
    assert fields.is_relative_to(tmp_path)
    assert json.loads(fields.read_text(encoding="utf-8")) == [{"name": "state"}]
    assert fields.read_bytes().endswith(b"\n")


def test_a_baseline_is_left_alone_when_only_the_timestamp_would_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Six baselines carry a `retrieved_at`, so writing unconditionally left them modified
    after every ingest -- and made a real schema change indistinguishable from a no-op run.
    """
    monkeypatch.setattr(common, "BASELINE_DIR", tmp_path / "baselines")
    first = write_baseline(
        "shared", "cpi_u", {"files_landed": ["cu.series"], "retrieved_at": "2026-08-11T00:00:00Z"}
    )
    original = first.read_bytes()

    again = write_baseline(
        "shared", "cpi_u", {"files_landed": ["cu.series"], "retrieved_at": "2026-08-13T09:99:99Z"}
    )

    assert again == first
    # Byte-identical, so the recorded timestamp is when the schema last *moved*.
    assert first.read_bytes() == original
    assert "2026-08-11" in first.read_text(encoding="utf-8")


def test_a_baseline_is_rewritten_when_the_publisher_changed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The half of the bargain that matters: real drift must still land on disk."""
    monkeypatch.setattr(common, "BASELINE_DIR", tmp_path / "baselines")
    write_baseline("shared", "cpi_u", {"files_landed": ["cu.series"], "retrieved_at": "A"})
    path = write_baseline(
        "shared", "cpi_u", {"files_landed": ["cu.series", "cu.item"], "retrieved_at": "B"}
    )

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["files_landed"] == ["cu.series", "cu.item"]
    assert payload["retrieved_at"] == "B"


def test_a_field_list_baseline_compares_by_content_too(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """OpenFEMA and CDC snapshot a bare list with no timestamp in it at all."""
    monkeypatch.setattr(common, "BASELINE_DIR", tmp_path / "baselines")
    fields = [{"name": "state"}, {"name": "censusGeoid"}]
    path = write_baseline("insurance", "nfip_claims", fields)
    unchanged = path.read_bytes()

    assert write_baseline("insurance", "nfip_claims", list(fields)).read_bytes() == unchanged
    write_baseline("insurance", "nfip_claims", [*fields, {"name": "newColumn"}])
    assert json.loads(path.read_text(encoding="utf-8"))[-1] == {"name": "newColumn"}


def test_a_corrupt_baseline_is_replaced_rather_than_preserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(common, "BASELINE_DIR", tmp_path / "baselines")
    (tmp_path / "baselines").mkdir(parents=True)
    (tmp_path / "baselines" / "shared__cpi_u__pointer.json").write_text("{ not json", "utf-8")

    path = write_baseline("shared", "cpi_u", {"files_landed": ["cu.series"]}, kind="pointer")
    assert json.loads(path.read_text(encoding="utf-8"))["files_landed"] == ["cu.series"]


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
