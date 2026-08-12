"""Every partition column in `raw` is VARCHAR, in all twelve tables.

The all-varchar rule (CLAUDE.md rule 2) is about what raw stores, and partition keys are
the one place it used to leak: DuckDB reconstructs the key from a directory name on read
and used to type-infer it, so `year=2011` arrived as BIGINT no matter what the file held.
This asserts the rule end to end -- load every committed fixture, rebuild from parquet, then
read the resulting schema -- rather than asserting the flag that currently implements it.

Offline: fixture mode never touches a publisher.
"""

import duckdb
import pytest
from ingest import reload
from ingest.registry import SOURCES


@pytest.fixture(scope="module")
def fixture_warehouse():
    """Exactly what `just ci` builds: every fixture loaded, then reloaded from parquet."""
    from ingest.fintech import cfpb, hmda
    from ingest.health import cdc, cms
    from ingest.insurance import fema_declarations, nfip_claims, nfip_policies
    from ingest.shared import bls, census

    for module in (
        nfip_claims,
        nfip_policies,
        fema_declarations,
        cfpb,
        hmda,
        cdc,
        cms,
        bls,
        census,
    ):
        assert module.main(["--mode", "fixture"]) == 0, module.__name__
    assert reload.main(["--mode", "fixture"]) == 0

    _, db_path = reload.paths_for("fixture")
    con = duckdb.connect(str(db_path), read_only=True)
    yield con
    con.close()


@pytest.mark.filterwarnings("ignore")
def test_every_partition_column_is_varchar(fixture_warehouse) -> None:
    numeric = {"BIGINT", "INTEGER", "DOUBLE", "HUGEINT", "DECIMAL"}
    observed = {}
    for spec in SOURCES:
        schema, _, table = spec.table.rpartition(".")
        row = fixture_warehouse.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? AND column_name = ?",
            [schema, table, spec.partition_column],
        ).fetchone()
        assert row is not None, f"{spec.table}.{spec.partition_column} is missing"
        observed[f"{spec.table}.{spec.partition_column}"] = row[0]

    retyped = {name: kind for name, kind in observed.items() if kind.split("(")[0] in numeric}
    assert retyped == {}, f"partition keys re-typed on read: {retyped}"
    assert len(observed) == 12


@pytest.mark.filterwarnings("ignore")
def test_the_five_columns_the_autocast_had_retyped(fixture_warehouse) -> None:
    """Named individually because four of them were on the defect list and one was not.

    `fin_hmda_institutions.period` was missed by the original report and found by querying
    the warehouse rather than by re-reading it.
    """
    for table, column in (
        ("raw.fin_cfpb_complaints", "year"),
        ("raw.fin_hmda_lar", "activity_year"),
        ("raw.fin_hmda_institutions", "period"),
        ("raw.ref_cpi_u", "year"),
        ("raw.ref_acs5_detailed", "vintage"),
        ("raw.ref_acs5_subject", "vintage"),
    ):
        kind = fixture_warehouse.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_schema = 'raw' AND table_name = ? AND column_name = ?",
            [table.split(".")[1], column],
        ).fetchone()[0]
        assert kind == "VARCHAR", f"{table}.{column} is {kind}"


@pytest.mark.filterwarnings("ignore")
def test_padding_and_leading_zeros_survive_into_the_table(fixture_warehouse) -> None:
    """What the rule is actually protecting: values a numeric type would have destroyed."""
    # BLS pads series_id to 17 characters, and `year` is its partition key.
    years = fixture_warehouse.execute(
        "SELECT DISTINCT year FROM raw.ref_cpi_u ORDER BY 1 LIMIT 1"
    ).fetchone()[0]
    assert isinstance(years, str)
    # ACS vintages are partition keys too, and geography codes keep their leading zeros.
    counties = fixture_warehouse.execute(
        "SELECT county FROM raw.ref_acs5_detailed WHERE geo_level = 'county' "
        "AND county LIKE '0%' LIMIT 1"
    ).fetchone()
    assert counties is not None and counties[0].startswith("0")
