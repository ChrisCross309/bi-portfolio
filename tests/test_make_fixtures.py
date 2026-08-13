"""Unit tests for the fixture generator.

The generator's whole value is that it is reproducible and that its strata protect real
edge cases, so that is what these check. They do not regenerate fixtures -- that needs the
live warehouse and is what `just fixture` is for.
"""

import duckdb
import pytest
from ingest.registry import SOURCES as RAW_SOURCES
from reconcile.make_fixtures import (
    COUNT_KEYS,
    FIXTURE_ROOT,
    SPECS,
    FixtureSpec,
    Stratum,
    metadata_path,
    sample_sql,
    stratum_sql,
)


@pytest.fixture
def con():
    connection = duckdb.connect()
    connection.execute("CREATE SCHEMA raw")
    connection.execute("""
        CREATE TABLE raw.t AS SELECT * FROM (VALUES
            ('MI', 'a'), ('MI', 'b'), ('OH', 'c'), ('OH', 'd'), ('UN', 'e')
        ) AS v(state, tag)
    """)
    yield connection
    connection.close()


def _spec(*strata: Stratum) -> FixtureSpec:
    return FixtureSpec(
        source="t",
        track="insurance",
        table="raw.t",
        path="x.parquet",
        writer="parquet",
        strata=strata,
    )


# ── determinism ───────────────────────────────────────────────────────────────


def test_the_same_rows_come_back_every_time(con) -> None:
    """No RNG and no seed: ordering is md5 of the row, so the sample is a function of the
    data. `USING SAMPLE REPEATABLE` would tie the fixture to a DuckDB version."""
    sql = stratum_sql(_spec(), Stratum("all", "1 = 1", 3))
    assert con.execute(sql).fetchall() == con.execute(sql).fetchall()


def test_the_sample_is_ordered_so_the_bytes_are_stable(con) -> None:
    """UNION fixes which rows are chosen but not the order they come back in, and an
    unordered write makes the file differ run to run even when its contents do not."""
    sql = sample_sql(_spec(Stratum("mi", "state = 'MI'", 5), Stratum("all", "1 = 1", 5)))
    assert "ORDER BY" in sql
    assert con.execute(sql).fetchall() == con.execute(sql).fetchall()


def test_overlapping_strata_do_not_duplicate_rows(con) -> None:
    """A Michigan row with a null geoid belongs to two strata; the fixture wants one copy."""
    rows = con.execute(
        sample_sql(_spec(Stratum("mi", "state = 'MI'", 5), Stratum("all", "1 = 1", 5)))
    ).fetchall()
    assert len(rows) == len(set(rows)) == 5


def test_a_stratum_takes_its_slice_not_the_whole_table(con) -> None:
    assert len(con.execute(stratum_sql(_spec(), Stratum("mi", "state = 'MI'", 1))).fetchall()) == 1


# ── the specs themselves ──────────────────────────────────────────────────────


def test_every_table_backed_spec_names_a_registered_table() -> None:
    known = {spec.table for spec in RAW_SOURCES}
    for spec in SPECS:
        assert spec.table in known, spec.source


def test_every_spec_writes_where_a_fixture_already_lives() -> None:
    """The generator rebuilds the committed set; a new path would mean a fixture no loader
    is looking for."""
    for spec in SPECS:
        assert (FIXTURE_ROOT / spec.path).exists(), spec.path


def test_every_count_key_exists_in_the_metadata_it_updates() -> None:
    """In fixture mode the metadata file is the publisher, so a key that does not exist
    would silently fail to update and leave the sample disagreeing with itself."""
    import json

    for spec in SPECS:
        if spec.source not in COUNT_KEYS:
            continue
        payload = json.loads(metadata_path(spec).read_text(encoding="utf-8"))
        for key in COUNT_KEYS[spec.source]:
            assert key in payload, f"{spec.source}: {key}"


def test_the_strata_cover_the_sentinels_each_source_is_known_for() -> None:
    """Each of these is an edge case a uniform sample would lose, and each has already
    cost real debugging somewhere in this repo."""
    by_source = {spec.source: " ".join(s.where for s in spec.strata) for spec in SPECS}
    assert "'UN'" in by_source["nfip_claims"]  # FEMA's unavailable-state code
    assert "Exempt" in by_source["hmda_lar"]  # the partial exemption literal
    assert "8888" in by_source["hmda_lar"]  # age codes rather than nulls
    assert "chr(10)" in by_source["cfpb_complaints"]  # newlines inside quoted narratives
    assert "'*'" in by_source["cms_geographic_variation"]  # suppression marker
    assert "26" in by_source["cms_geographic_variation"]  # the Michigan county roster
    assert "reportedZipCode = ''" in by_source["nfip_policies"]  # empty string, not null


def test_michigan_appears_in_every_fixture_that_has_a_geography() -> None:
    """CLAUDE.md section 6: fixtures are stratified to include Michigan rows."""
    for source in ("nfip_claims", "fema_declarations", "cfpb_complaints", "cdc_healthy_aging"):
        spec = next(s for s in SPECS if s.source == source)
        wheres = " ".join(s.where for s in spec.strata)
        assert "MI" in wheres, source
