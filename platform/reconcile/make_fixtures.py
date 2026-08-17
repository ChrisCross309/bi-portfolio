"""Rebuild every committed fixture from local raw, deterministically.

The committed fixtures were originally built by scripts that were never committed, so a
publisher adding a column left no reproducible way to refresh them. This is that way, and
the strata now live in code where they can be read and argued with.

**Deterministic without a random seed.** Rows are ordered by `md5` of the whole row cast to
text and the first N taken. That needs no key column, survives a change of query plan, and
gives the same sample on any machine and any DuckDB version -- which `USING SAMPLE
REPEATABLE` does not promise. Verified stable across repeated runs and altered plans.

**Built from raw, not landing.** Raw parquet is canonical and survives `just clean-landing`,
so a generator that reads landing would stop working the moment disk is reclaimed -- and
four sources have already reclaimed theirs. The cost is that a fixture is a re-serialization
of raw rather than a byte-slice of the original download; values are identical because raw
is all-varchar, but column order follows raw, which appends the partition key. Every loader
reads by header name, so nothing depends on the order.

The exception is ACS, whose landed shape is an array of arrays with a header row that the
loader reads *by position*. That shape is reconstructed here explicitly rather than sampled,
because getting it wrong would silently mislabel every column.

**Strata, not a plain sample.** CLAUDE.md section 6 asks for Michigan rows, edge cases and
sentinel values in every fixture. A uniform sample of 2.7M NFIP claims would contain almost
no Michigan and no `state = 'UN'`; a uniform sample of HMDA would miss the literal `Exempt`.
Each stratum below says what it is protecting.

Run:  python -m reconcile.make_fixtures [--source NAME] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
from ingest.common import (
    REPO_ROOT,
    make_logger,
    paths_for,
    read_existing_manifest,
    sql_literal,
    write_json,
)
from ingest.shared import census

FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures"

# A fixture must stay far below pre-commit's 2 MB ceiling; this is the alarm, not the limit.
SIZE_WARN_BYTES = 1_500_000

# 2011 lists the island areas with null estimates; 2024 is the newest. Two vintages is
# enough to exercise the overlap caveat without carrying fifteen.
FIXTURE_VINTAGES = ("2011", "2024")
# Non-Michigan counties per file: enough to prove the geography works, few enough to stay small.
ACS_OTHER_ROWS = 40
# Filers kept per year in the institution reference, and the count its metadata declares.
HMDA_FILERS_PER_YEAR = 40

# The CPI-U series the two BLS fixtures share. Named once because the observations fixture
# and the definitions fixture must agree: the loader refuses a sample whose observed series
# have no definition.
CPI_DEFLATOR_SERIES = "('CUUR0000SA0', 'CUSR0000SA0')"
# Semiannual averages live only on the `CUUS` series, never on the deflator pair.
CPI_SEMIANNUAL_SERIES = "('CUUS0000AA0', 'CUUS0000SA0')"

log = make_logger("platform", "fixtures")


@dataclass(frozen=True)
class Stratum:
    """One slice a fixture must contain, and why losing it would matter."""

    name: str
    where: str
    limit: int


@dataclass(frozen=True)
class FixtureSpec:
    source: str
    track: str
    table: str
    path: str  # relative to tests/fixtures
    writer: str  # parquet | csv | tsv
    strata: tuple[Stratum, ...]
    exclude: tuple[str, ...] = ()  # columns raw added that landing never had
    order_by: str | None = None  # where the loader depends on row order


SPECS: tuple[FixtureSpec, ...] = (
    FixtureSpec(
        source="nfip_claims",
        track="insurance",
        table="raw.ins_nfip_claims",
        path="insurance/nfip_claims.parquet",
        writer="parquet",
        strata=(
            Stratum("michigan", "state = 'MI'", 300),
            # FEMA's own code for a claim whose state is unavailable. A fixture without it
            # cannot exercise the unknown-state partition L1 reports.
            Stratum("unknown_state", "state = 'UN'", 60),
            Stratum("territories", "state IN ('PR', 'VI', 'GU', 'AS', 'MP')", 120),
            Stratum("null_geoid", "censusGeoid IS NULL", 60),
            Stratum("zero_paid", "COALESCE(amountPaidOnBuildingClaim, 0) = 0", 60),
            Stratum("national_spread", "1 = 1", 400),
        ),
    ),
    FixtureSpec(
        source="nfip_policies",
        track="insurance",
        table="raw.ins_nfip_policies",
        path="insurance/nfip_policies.parquet",
        writer="parquet",
        # Ascending id, so fixture mode exercises the same keyset-paging invariant the live
        # pull asserts.
        order_by="id",
        strata=(
            Stratum("michigan_spread", "1 = 1", 500),
            Stratum("null_geoid", "censusGeoid IS NULL", 60),
            # Seventeen rows nationally; NFIP writes an empty string here, never NULL.
            Stratum("empty_zip", "reportedZipCode = ''", 20),
        ),
    ),
    FixtureSpec(
        source="fema_declarations",
        track="insurance",
        table="raw.ins_fema_declarations",
        path="insurance/fema_declarations.parquet",
        writer="parquet",
        strata=(
            Stratum("michigan", "state = 'MI'", 250),
            # Kept whole so fixture mode runs the same anchor assertions as the live pull.
            Stratum("anchors", "disasterNumber IN ('3525', '4547', '4607')", 200),
            # A statewide declaration names no county: the gate's '000' case.
            Stratum("statewide", "fipsCountyCode = '000'", 60),
            Stratum("national_spread", "1 = 1", 350),
        ),
    ),
    FixtureSpec(
        source="cfpb_complaints",
        track="fintech",
        table="raw.fin_cfpb_complaints",
        path="fintech/cfpb_complaints.csv",
        writer="csv",
        # `year` is ours, derived from the received date; the archive never had it.
        exclude=("year",),
        strata=(
            Stratum("michigan", "\"State\" = 'MI'", 220),
            # 9% of this file has newlines inside quoted narratives -- the reason the loader
            # must read it single-threaded. A fixture without them cannot prove that.
            Stratum(
                "narrative_newlines",
                "\"Consumer complaint narrative\" LIKE '%' || chr(10) || '%'",
                60,
            ),
            Stratum("null_state", '"State" IS NULL', 40),
            Stratum("null_zip", '"ZIP code" IS NULL', 30),
            Stratum("national_spread", "1 = 1", 380),
        ),
    ),
    FixtureSpec(
        source="hmda_lar",
        track="fintech",
        table="raw.fin_hmda_lar",
        path="fintech/hmda_lar.csv",
        writer="csv",
        strata=(
            Stratum("spread", "1 = 1", 500),
            # The partial exemption arrives as a literal string in a numeric-looking column
            # -- the single strongest argument for the all-varchar rule.
            Stratum("exempt", "loan_term = 'Exempt'", 80),
            Stratum("na_tract", "census_tract = 'NA'", 80),
            # Applicant age uses codes rather than nulls.
            Stratum("age_code", "applicant_age IN ('8888', '9999')", 80),
            # '0', not 'NA', is how this file says "no MSA".
            Stratum("no_msa", "\"derived_msa-md\" = '0'", 80),
            Stratum("multifamily", "total_units NOT IN ('1', '2', '3', '4')", 80),
            Stratum("no_mi_county", "county_code = 'NA' OR county_code NOT LIKE '26%'", 80),
        ),
    ),
    FixtureSpec(
        source="cdc_healthy_aging",
        track="health",
        table="raw.hlt_cdc_healthy_aging",
        path="health/cdc_healthy_aging.csv",
        writer="csv",
        strata=(
            Stratum("michigan", "locationabbr = 'MI'", 200),
            # The four census regions and the national row sit as peers of the states.
            Stratum("rollups", "locationabbr IN ('MDW', 'NRE', 'SOU', 'WEST', 'US')", 120),
            Stratum("territories", "locationabbr IN ('GU', 'PR', 'VI')", 60),
            Stratum("cognitive_decline", "lower(topic) LIKE '%cognitive decline%'", 120),
            Stratum("caregiving", "lower(class) LIKE '%caregiv%'", 120),
            # Suppression markers are information, not nulls, and must survive raw.
            Stratum("suppressed", "data_value_footnote_symbol IS NOT NULL", 120),
            Stratum("race_stratified", "stratificationcategory2 = 'Race/Ethnicity'", 80),
            Stratum("sex_stratified", "stratificationcategory2 = 'Sex'", 80),
            Stratum("overall_null_category2", "stratificationcategory2 IS NULL", 80),
        ),
    ),
    FixtureSpec(
        source="cms_geographic_variation",
        track="health",
        table="raw.hlt_cms_geographic_variation",
        path="health/cms_geographic_variation.csv",
        writer="csv",
        strata=(
            # Every Michigan county in the earliest and latest year, so the county-roster
            # check has a full roster to find rather than a sample of one.
            Stratum(
                "mi_counties_earliest",
                "BENE_GEO_LVL = 'County' AND BENE_GEO_CD LIKE '26%' AND YEAR = '2014'",
                90,
            ),
            Stratum(
                "mi_counties_latest",
                "BENE_GEO_LVL = 'County' AND BENE_GEO_CD LIKE '26%' AND YEAR = '2024'",
                90,
            ),
            Stratum("mi_state_all_years", "BENE_GEO_LVL = 'State' AND BENE_GEO_DESC = 'MI'", 40),
            Stratum("national_all_years", "BENE_GEO_LVL = 'National'", 40),
            Stratum("peer_states_latest", "BENE_GEO_LVL = 'State' AND YEAR = '2024'", 60),
            Stratum("peer_counties", "BENE_GEO_LVL = 'County'", 60),
            # CMS writes a literal '*' where a count is too small to publish -- never a
            # null. That is information the all-varchar rule exists to preserve, so the
            # fixture has to carry some.
            Stratum("suppressed_cells", "TOT_MDCR_STDZD_PYMT_PC = '*'", 40),
        ),
    ),
    FixtureSpec(
        source="cpi_u",
        track="shared",
        table="raw.ref_cpi_u",
        path="shared/cpi_u.tsv",
        writer="tsv",
        strata=(
            # The deflator this repo actually uses, and its seasonally adjusted twin so the
            # wrong-series trap is present to be got wrong.
            Stratum("deflator_and_twin", f"trim(series_id) IN {CPI_DEFLATOR_SERIES}", 3000),
            # Neither deflator series carries a semiannual period, so a fixture built from
            # them alone loses S01-S03 entirely -- and with them the fact that `period`
            # mixes twelve months with three kinds of average. Caught by test_bls.
            Stratum("semiannual_periods", f"trim(series_id) IN {CPI_SEMIANNUAL_SERIES}", 400),
        ),
    ),
    FixtureSpec(
        source="cpi_u_series",
        track="shared",
        table="raw.ref_cpi_u_series",
        path="shared/cpi_u_series.tsv",
        writer="tsv",
        strata=(
            Stratum("both_seasonal_flags", "1 = 1", 40),
            # Every series the observations fixture holds must have a definition here, or
            # the loader's own `definition_failures` check fails on our own sample. The two
            # fixtures are coupled, so they name the same series list rather than guessing.
            Stratum(
                "definitions_for_observed_series",
                f"trim(series_id) IN {CPI_DEFLATOR_SERIES} "
                f"OR trim(series_id) IN {CPI_SEMIANNUAL_SERIES}",
                10,
            ),
        ),
    ),
    FixtureSpec(
        source="zip_county_crosswalk",
        track="shared",
        table="raw.ref_zip_county_crosswalk",
        path="shared/zip_county_crosswalk.json",
        # HUD answers a query rather than serving a file, so the sample is written back
        # inside its response envelope. See `write_envelope_fixture`.
        writer="json_envelope",
        strata=(
            # All of Michigan, deliberately, and it is only ~1,600 tiny rows. The fetcher
            # asserts all 83 counties are present on every run including fixture runs,
            # because this source is a county denominator -- so a sampled Michigan would
            # make the fixture fail a check the live data passes.
            Stratum("michigan_complete", "state = 'MI'", 2_000),
            # `state_coverage_failures` requires every code in state_codes.csv, so the
            # sample needs at least one row behind each. Three per state, deterministically.
            Stratum(
                "every_state",
                "(zip, geoid) IN (SELECT zip, geoid FROM (SELECT zip, geoid, row_number() "
                "OVER (PARTITION BY state ORDER BY zip, geoid) AS rn "
                "FROM raw.ref_zip_county_crosswalk) WHERE rn <= 3)",
                300,
            ),
            # The reason this source exists: a ZIP that lands in more than one county.
            Stratum(
                "zips_crossing_county_lines",
                "zip IN (SELECT zip FROM raw.ref_zip_county_crosswalk GROUP BY zip "
                "HAVING count(*) > 1)",
                200,
            ),
            # PO-box and business-only ZIPs, where res_ratio sums to zero and an allocation
            # rule that uses it alone drops the row. Losing this stratum would hide the bug.
            Stratum(
                "no_residential_addresses",
                "zip IN (SELECT zip FROM raw.ref_zip_county_crosswalk GROUP BY zip "
                "HAVING sum(CAST(res_ratio AS DOUBLE)) = 0)",
                100,
            ),
            # Territory codes with no county component, and the three Freely Associated
            # States that no US code list contains.
            Stratum("short_geoid", "length(geoid) <> 5", 20),
            Stratum("freely_associated_states", "state IN ('FM', 'MH', 'PW')", 20),
        ),
    ),
)


# ── sampling ──────────────────────────────────────────────────────────────────


def stratum_sql(spec: FixtureSpec, stratum: Stratum) -> str:
    """Deterministic top-N of one stratum: no RNG, no key column, no plan dependence."""
    return (
        f"SELECT * FROM (SELECT * FROM {spec.table} WHERE {stratum.where}) s "
        f"ORDER BY md5(CAST(s AS VARCHAR)) LIMIT {stratum.limit}"
    )


def sample_sql(spec: FixtureSpec) -> str:
    """Every stratum, unioned. UNION rather than UNION ALL: strata overlap by design --
    a Michigan claim with a null geoid belongs to both -- and a fixture should carry one
    copy of it."""
    parts = " UNION ".join(f"({stratum_sql(spec, stratum)})" for stratum in spec.strata)
    projection = f"* EXCLUDE ({', '.join(spec.exclude)})" if spec.exclude else "*"
    # UNION fixes which rows are chosen but says nothing about the order they come back in,
    # and an unordered write makes the file's bytes vary run to run even when its contents
    # do not. Ordering the output is what makes `just fixture` idempotent, which is the
    # property that lets a reviewer re-run it and see an empty diff.
    ordering = spec.order_by or "md5(CAST(u AS VARCHAR))"
    return f"SELECT {projection} FROM ({parts}) u ORDER BY {ordering}"


# Which key in each metadata file carries the count L1 reconciles the fixture against.
# Sources absent from this map publish no count, and L1 reports SKIP for them.
COUNT_KEYS: dict[str, tuple[str, ...]] = {
    "nfip_claims": ("recordCount",),
    "fema_declarations": ("recordCount",),
    "cfpb_complaints": ("source_reported_count",),
    "cdc_healthy_aging": ("reported_total",),
}


COPY_OPTIONS = {
    "parquet": "FORMAT parquet",
    "csv": "FORMAT csv, HEADER",
    # quote='' keeps BLS's fixed-width padding byte for byte; these files are not CSV.
    "tsv": "FORMAT csv, HEADER, DELIMITER '\t', QUOTE ''",
}


def write_envelope_fixture(con: duckdb.DuckDBPyConnection, spec: FixtureSpec, target: Path) -> int:
    """Write a sample back inside the publisher's own JSON envelope.

    HUD does not serve a file; it answers a query with `{"data": {..., "results": [...]}}`,
    and the fetcher reads that shape. A bare array of the sampled rows would be a fixture of
    a response HUD never sends, so fixture mode would exercise a reader the live path does
    not use -- which is the one thing a fixture must not do. The envelope's `year` and
    `quarter` are copied from the manifest of the raw the sample came from, so the fixture
    reports the vintage it was actually built from rather than a made-up one.
    """
    manifest = read_existing_manifest(paths_for("live")[0] / "raw" / spec.track / spec.source) or {}
    vintage = str(manifest.get("vintage", ""))
    year, _, quarter = vintage.partition("Q")
    result = con.execute(sample_sql(spec))
    columns = [description[0] for description in result.description]
    rows = result.fetchall()
    target.parent.mkdir(parents=True, exist_ok=True)
    write_json(
        target,
        {
            "data": {
                "year": int(year) if year.isdigit() else None,
                "quarter": int(quarter) if quarter.isdigit() else None,
                "input": "All",
                "crosswalk_type": "zip-county",
                "results": [dict(zip(columns, row, strict=True)) for row in rows],
            }
        },
    )
    return len(rows)


def write_fixture(con: duckdb.DuckDBPyConnection, spec: FixtureSpec, target: Path) -> int:
    target.parent.mkdir(parents=True, exist_ok=True)
    if spec.writer == "json_envelope":
        return write_envelope_fixture(con, spec, target)
    con.execute(f"COPY ({sample_sql(spec)}) TO {sql_literal(target)} ({COPY_OPTIONS[spec.writer]})")
    return int(con.execute(f"SELECT count(*) FROM ({sample_sql(spec)})").fetchone()[0])


def metadata_path(spec: FixtureSpec) -> Path:
    return FIXTURE_ROOT / spec.track / f"{spec.source}.metadata.json"


def update_metadata(spec: FixtureSpec, rows: int, counts: dict[str, int]) -> Path | None:
    """Rewrite the counts a regenerated fixture has just invalidated.

    In fixture mode the metadata file *is* the publisher: L1's count chain compares its
    reported total against the rows that landed. Regenerating a sample without updating it
    makes every source disagree with itself -- which is exactly how this was caught.

    Only the count keys and the recipe comment are touched. Everything else in these files
    is publisher shape -- column lists, distribution stanzas, identifiers -- and is left as
    it was written.
    """
    path = metadata_path(spec)
    if not path.exists() or spec.source not in COUNT_KEYS:
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    for key in COUNT_KEYS[spec.source]:
        payload[key] = rows
    payload["_comment"] = (
        f"Stratified sample of {spec.table}, rebuilt by `just fixture` from local raw. "
        f"Deterministic md5 ordering, no RNG. Strata: {sorted(counts)}. "
        "Not a live publisher response."
    )
    write_json(path, payload)
    return path


def update_hud_metadata(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """The crosswalk fixture's metadata is what fixture mode checks itself against.

    Two numbers the fetcher asserts on every run: the vintage it reports, and the Michigan
    row count its spot check compares the national pull against. Live, that second number
    comes from asking HUD for Michigan on its own. Offline there is no publisher to ask, so
    the metadata file plays that part -- and it has to be recomputed from the sample, or the
    fixture disagrees with itself the moment the sample changes.
    """
    path = FIXTURE_ROOT / "shared" / "zip_county_crosswalk.metadata.json"
    spec = next(spec for spec in SPECS if spec.source == "zip_county_crosswalk")
    manifest = read_existing_manifest(paths_for("live")[0] / "raw" / spec.track / spec.source) or {}
    michigan = int(
        con.execute(f"SELECT count(*) FROM ({sample_sql(spec)}) WHERE state = 'MI'").fetchone()[0]
    )
    payload = {
        "_comment": (
            "Stratified sample of raw.ref_zip_county_crosswalk, rebuilt by `just fixture` "
            "from local raw and wrapped in HUD's own response envelope. Not a live "
            "publisher response."
        ),
        "vintage": manifest.get("vintage"),
        "spot_check_rows": michigan,
        "spot_check_note": (
            "Michigan rows in this sample. Live, the fetcher gets this by asking HUD for "
            "Michigan alone; offline this file is the publisher."
        ),
    }
    write_json(path, payload)
    return {"vintage": payload["vintage"], "spot_check_rows": michigan}


def stratum_counts(con: duckdb.DuckDBPyConnection, spec: FixtureSpec) -> dict[str, int]:
    """What each stratum actually contributed. A zero is a stratum that has stopped
    protecting anything -- a publisher changed a sentinel, and the fixture would quietly
    lose the edge case it was built for."""
    return {
        stratum.name: int(
            con.execute(f"SELECT count(*) FROM ({stratum_sql(spec, stratum)})").fetchone()[0]
        )
        for stratum in spec.strata
    }


def update_cfpb_michigan(con: duckdb.DuckDBPyConnection) -> int:
    """CFPB's metadata carries a second figure: the Michigan slice the search API reports."""
    path = FIXTURE_ROOT / "fintech" / "cfpb_complaints.metadata.json"
    michigan = int(
        con.execute(
            "SELECT count(*) FROM read_csv(?, all_varchar=true, header=true, parallel=false) "
            "WHERE \"State\" = 'MI'",
            [str(FIXTURE_ROOT / "fintech" / "cfpb_complaints.csv")],
        ).fetchone()[0]
    )
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["source_reported_michigan"] = michigan
    write_json(path, payload)
    return michigan


def update_hmda_metadata(con: duckdb.DuckDBPyConnection, per_year: int) -> dict[str, int]:
    """HMDA's metadata is per year, and both its control totals must match the sample.

    The live source publishes two independent totals that disagree for three of the eight
    years. A fixture cannot reproduce that disagreement honestly -- it is a fact about the
    publisher, not about the sample -- so both are set to what the sample actually holds,
    and the disagreement stays recorded in the live manifest where it belongs.
    """
    lar = FIXTURE_ROOT / "fintech" / "hmda_lar.csv"
    by_year = {
        str(year): int(rows)
        for year, rows in con.execute(
            "SELECT activity_year, count(*) FROM read_csv(?, all_varchar=true, header=true) "
            "GROUP BY 1 ORDER BY 1",
            [str(lar)],
        ).fetchall()
    }
    path = FIXTURE_ROOT / "fintech" / "hmda.metadata.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["years"] = [
        {
            "year": int(year),
            "last_modified": "Thu, 01 Jan 1970 00:00:00 GMT",
            "source_reported_records": rows,
            "source_reported_by_filers": rows,
            "institutions": per_year,
        }
        for year, rows in sorted(by_year.items())
    ]
    payload["_comment"] = (
        "Per-year control totals for fixture mode, matching the committed sample rather "
        "than the live publisher. Rebuilt by `just fixture`, deterministic md5 ordering, "
        "no RNG. The live source's two totals disagree for three years; that is a fact "
        "about the publisher and is recorded in the live manifest, not here."
    )
    write_json(path, payload)
    return by_year


# ── the two sources whose landed shape is not a table ─────────────────────────


def write_hmda_institutions(
    con: duckdb.DuckDBPyConnection, per_year: int = HMDA_FILERS_PER_YEAR
) -> tuple[int, Path]:
    """The filer list as the API returns it: one `institutions` envelope, not a table."""
    rows = con.execute(
        f"""
        SELECT lei, name, count, period FROM (
          SELECT *, row_number() OVER (
            PARTITION BY period ORDER BY md5(CAST(f AS VARCHAR))
          ) AS rank FROM raw.fin_hmda_institutions f
        ) WHERE rank <= {per_year} ORDER BY period, lei
        """
    ).fetchall()
    payload = {
        "_comment": (
            f"Stratified sample of the HMDA filer list, {per_year} LEIs per year, "
            "deterministic md5 ordering, no RNG. Rebuilt by `just fixture`."
        ),
        "institutions": [
            {"lei": lei, "name": name, "count": count, "period": period}
            for lei, name, count, period in rows
        ],
    }
    target = FIXTURE_ROOT / "fintech" / "hmda_institutions.json"
    write_json(target, payload)
    return len(rows), target


def write_acs_files(con: duckdb.DuckDBPyConnection) -> tuple[int, list[Path]]:
    """Rebuild the array-of-arrays the ACS API returns, header row and all.

    The loader reads these *by position*, so the header is not decoration -- it is what
    `header_failures` checks before any of it is trusted as data. It is rebuilt from the
    same `DATASETS` definition the fetcher requests with, so the two cannot disagree.

    Vintages 2011 and 2024: 2011 is the only vintage-dataset pair that enumerates the four
    island areas, every estimate null, and those rows are the reason `valueless_rows` exists.
    All Michigan counties and all state rows are kept because the loader asserts both
    rosters -- a sample would fail its own check.
    """
    written: list[Path] = []
    total = 0
    target_dir = FIXTURE_ROOT / "shared" / "acs5"
    for stale in target_dir.glob("acs5-*.json"):
        stale.unlink()

    for dataset, spec in census.DATASETS.items():
        variables = list(spec["variables"])
        for vintage in FIXTURE_VINTAGES:
            for geography, keys in (
                ("us", ["us"]),
                ("state", ["state"]),
                ("county", ["state", "county"]),
            ):
                columns = ["NAME", *variables, *keys]
                # Michigan whole; everything else sampled, deterministically.
                where = "1 = 1" if geography != "county" else "state = '26'"
                rows = con.execute(
                    f"""SELECT {", ".join(f'"{c}"' for c in columns)}
                        FROM {spec["table"]}
                        WHERE geo_level = '{geography}' AND vintage = '{vintage}' AND ({where})
                        ORDER BY {", ".join(f'"{k}"' for k in keys)}"""
                ).fetchall()
                extra = con.execute(
                    f"""SELECT {", ".join(f'"{c}"' for c in columns)} FROM (
                          SELECT * FROM {spec["table"]} WHERE geo_level = '{geography}'
                          AND vintage = '{vintage}' AND NOT ({where})
                        ) s ORDER BY md5(CAST(s AS VARCHAR)) LIMIT {ACS_OTHER_ROWS}"""
                ).fetchall()
                payload = [columns] + [list(row) for row in (*rows, *extra)]
                target = target_dir / census.landing_name(dataset, vintage, geography)
                write_json(target, payload)
                written.append(target)
                total += len(payload) - 1
    return total, written


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", help="rebuild one fixture instead of all of them")
    parser.add_argument(
        "--dry-run", action="store_true", help="report what each stratum would contribute"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    data_root, db_path = paths_for("live")
    if not db_path.exists():
        log(f"FAIL: {db_path} does not exist; run an ingestion recipe or `just reload` first")
        return 1

    # The two sources whose landed shape is not a table have their own writers, so the
    # names `--source` accepts are the table-backed specs plus those two.
    known = {spec.source for spec in SPECS} | {"hmda_institutions", "acs5"}
    if args.source is not None and args.source not in known:
        log(f"no fixture registered for {args.source!r}; known: {sorted(known)}")
        return 1
    selected = [s for s in SPECS if args.source in (None, s.source)]

    problems: list[str] = []
    con = duckdb.connect(str(db_path), read_only=True)
    con.execute("SET memory_limit='4GB'")
    try:
        for spec in selected:
            counts = stratum_counts(con, spec)
            empty = [name for name, count in counts.items() if count == 0]
            if empty:
                problems.append(
                    f"{spec.source}: strata matched nothing and are protecting nothing: "
                    f"{empty}. A publisher has changed a sentinel or a code."
                )
            if args.dry_run:
                log(f"{spec.source}: {counts}")
                continue

            target = FIXTURE_ROOT / spec.path
            rows = write_fixture(con, spec, target)
            size = target.stat().st_size
            flag = "  <-- large" if size > SIZE_WARN_BYTES else ""
            log(f"{spec.path:<42} {rows:>6,} rows  {size / 1e3:>8,.1f} KB{flag}")
            log(f"    strata: {counts}")
            if update_metadata(spec, rows, counts):
                log(f"    metadata counts updated to {rows:,}")
            if spec.source == "cfpb_complaints":
                log(f"    michigan slice recorded: {update_cfpb_michigan(con):,}")
            if spec.source == "hmda_lar":
                log(f"    per-year totals: {update_hmda_metadata(con, HMDA_FILERS_PER_YEAR)}")
            if spec.source == "zip_county_crosswalk":
                log(f"    vintage and spot check recorded: {update_hud_metadata(con)}")

        if args.source in (None, "hmda_institutions"):
            rows, target = write_hmda_institutions(con)
            log(
                f"{'fintech/hmda_institutions.json':<42} {rows:>6,} rows  "
                f"{target.stat().st_size / 1e3:>8,.1f} KB"
            )
        if args.source in (None, "acs5"):
            rows, written = write_acs_files(con)
            size = sum(path.stat().st_size for path in written)
            log(
                f"{'shared/acs5/*.json':<42} {rows:>6,} rows  {size / 1e3:>8,.1f} KB "
                f"across {len(written)} files"
            )
    finally:
        con.close()

    for problem in problems:
        log(f"FAIL  {problem}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
