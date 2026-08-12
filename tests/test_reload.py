"""Unit tests for the offline rebuild, plus an end-to-end fixture-mode run.

All offline by construction: `ingest.reload` imports no fetcher and opens no HTTP client,
which `test_reload_cannot_reach_a_publisher` asserts rather than trusts.
"""

import json
from pathlib import Path

import duckdb
import pytest
from ingest import reload
from ingest.registry import RawSource
from ingest.reload import FAIL, PASS, SKIP, has_parquet, reload_source

SPEC = RawSource("insurance", "nfip_claims", "raw.ins_nfip_claims", "state")


def _raw_tree(root: Path, rows: int, recorded: int | None) -> Path:
    """A partitioned raw tree with `rows` rows, and a manifest claiming `recorded`."""
    raw_dir = root / "raw" / SPEC.track / SPEC.source
    # COPY ... PARTITION_BY builds the partition tree but not its parents.
    raw_dir.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(
            f"COPY (SELECT 'MI' AS state, i AS n FROM range({rows}) t(i)) "
            f"TO '{raw_dir.as_posix()}' (FORMAT parquet, PARTITION_BY (state))"
        )
    finally:
        con.close()
    if recorded is not None:
        (raw_dir / "manifest.json").write_text(json.dumps({"rows_duckdb": recorded}), "utf-8")
    return raw_dir


@pytest.fixture
def con():
    connection = duckdb.connect()
    yield connection
    connection.close()


def test_has_parquet_is_false_for_missing_and_empty_directories(tmp_path: Path) -> None:
    assert has_parquet(tmp_path / "absent") is False
    (tmp_path / "empty").mkdir()
    assert has_parquet(tmp_path / "empty") is False


def test_a_source_that_was_never_ingested_is_skipped_not_failed(tmp_path: Path, con) -> None:
    result = reload_source(con, SPEC, tmp_path)
    assert result.status == SKIP
    assert "not ingested yet" in result.detail


def test_a_rebuild_matching_the_manifest_passes(tmp_path: Path, con) -> None:
    _raw_tree(tmp_path, rows=250, recorded=250)
    result = reload_source(con, SPEC, tmp_path)
    assert result.status == PASS
    assert con.execute(f"SELECT count(*) FROM {SPEC.table}").fetchone()[0] == 250


def test_a_rebuild_disagreeing_with_the_manifest_fails(tmp_path: Path, con) -> None:
    """Raw and the manifest describing different data is a defect, not a rounding note."""
    _raw_tree(tmp_path, rows=250, recorded=300)
    result = reload_source(con, SPEC, tmp_path)
    assert result.status == FAIL
    assert "gap -50" in result.detail


def test_a_rebuild_without_a_manifest_still_loads_and_says_so(tmp_path: Path, con) -> None:
    _raw_tree(tmp_path, rows=7, recorded=None)
    result = reload_source(con, SPEC, tmp_path)
    assert result.status == PASS
    assert "no manifest count" in result.detail


def test_reload_replaces_rather_than_appends(tmp_path: Path, con) -> None:
    """Re-running must be idempotent: `just reload` twice is one table, not two."""
    _raw_tree(tmp_path, rows=40, recorded=40)
    assert reload_source(con, SPEC, tmp_path).status == PASS
    assert reload_source(con, SPEC, tmp_path).status == PASS
    assert con.execute(f"SELECT count(*) FROM {SPEC.table}").fetchone()[0] == 40


def test_an_unregistered_track_is_a_no_op_rather_than_an_error() -> None:
    assert reload.run(mode="fixture", track="nonexistent") == 0


def test_reload_cannot_reach_a_publisher() -> None:
    """The offline guarantee, asserted rather than described: no httpx, no fetcher."""
    source = Path(reload.__file__).read_text(encoding="utf-8")
    assert "httpx" not in source
    assert "import" in source and "ingest.insurance" not in source
    assert "ingest.fintech" not in source
    assert "ingest.health" not in source
    assert "ingest.shared" not in source


@pytest.mark.filterwarnings("ignore")
def test_fixture_tables_rebuild_from_parquet_with_no_network() -> None:
    """The defect, at fixture scale: load, then rebuild every table from raw parquet alone."""
    from ingest.fintech import cfpb
    from ingest.insurance import nfip_claims

    assert nfip_claims.main(["--mode", "fixture"]) == 0
    assert cfpb.main(["--mode", "fixture"]) == 0

    _, db_path = reload.paths_for("fixture")
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute("DROP TABLE raw.ins_nfip_claims")
        connection.execute("DROP TABLE raw.fin_cfpb_complaints")
    finally:
        connection.close()

    assert reload.main(["--mode", "fixture"]) == 0

    connection = duckdb.connect(str(db_path), read_only=True)
    try:
        for table in ("raw.ins_nfip_claims", "raw.fin_cfpb_complaints"):
            assert connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0] > 0
    finally:
        connection.close()
