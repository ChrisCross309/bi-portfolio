"""The Michigan geography gate: can each source actually answer a county-grain question?

CLAUDE.md section 4 makes Michigan the analytical focus at **county grain** in all three
tracks. That is a claim about the data, not a filter, and it is only true if the Michigan
rows carry a usable county. This is where that gets measured instead of assumed.

Two things make it harder than a null check.

**Every publisher spells "no county" differently.** A predicate written for one source
reports a comforting 0.0% on the next:

    nfip_claims / nfip_policies   censusGeoid is NULL          (never the empty string)
    cfpb_complaints               "ZIP code" is NULL           (never the empty string)
    hmda_lar                      county_code is the literal 'NA', or an out-of-state FIPS
    fema_declarations             fipsCountyCode is '000', meaning a statewide declaration
    cms_geographic_variation      BENE_GEO_CD is '26000', filed at County level as MI-UNKNOWN

Note especially that NFIP uses NULL for `censusGeoid` while using the empty string for
`reportedZipCode` -- the convention is per column, not per publisher, so each gate names its
own predicate rather than inheriting one.

**A missing county is not always a defect.** Flood claims are not filed in every county and
flood policies are not sold in every county, so NFIP covering 82 and 77 of Michigan's 83 is
a fact about flood insurance. The 83-county roster is only *required* where the source is a
denominator -- HMDA, CMS and ACS -- because there a missing county silently shrinks every
per-capita rate built on it. Sources that are not denominators report their coverage and
pass.

The gate warns rather than fails: a publisher's geography quality is an input to session 2's
modelling decisions, not a reason to reject a faithful copy of what was published.
"""

from __future__ import annotations

from dataclasses import dataclass

import duckdb
from ingest.health import cms
from reconcile.results import PASS, SKIP, WARN, Result

# Michigan's 83 counties, and the state FIPS every county code here begins with.
MICHIGAN_STATE_FIPS = "26"
MICHIGAN_COUNTY_COUNT = 83

# Above this share of Michigan rows with no usable county, say so loudly. Ten percent is the
# threshold the root README has always quoted; nothing here is near it today.
UNUSABLE_WARN_PCT = 10.0


@dataclass(frozen=True)
class MichiganGate:
    """One source's answer to "can this support a Michigan county-grain measure?"."""

    source: str
    table: str
    michigan_filter: str
    geography_column: str
    # SQL predicate: this row has no county we could use, whatever the publisher calls it.
    unusable: str
    # SQL yielding the state+county FIPS, or None where the source carries no county at all.
    county_expr: str | None
    # 83 where the source is a denominator; None where coverage is reported, not required.
    expected_counties: int | None
    # Why the roster is not required, or what stands in for a county.
    note: str
    # MI-scoped raw has nothing national to compare against, and says so.
    national: bool = True


GATES: tuple[MichiganGate, ...] = (
    MichiganGate(
        source="nfip_claims",
        table="raw.ins_nfip_claims",
        michigan_filter="state = 'MI'",
        geography_column="censusGeoid",
        unusable="censusGeoid IS NULL OR trim(censusGeoid) = ''",
        county_expr="substr(censusGeoid, 1, 5)",
        expected_counties=None,
        note="flood claims are not filed in every county, so the roster is reported not required",
    ),
    MichiganGate(
        source="nfip_policies",
        table="raw.ins_nfip_policies",
        michigan_filter="propertyState = 'MI'",
        geography_column="censusGeoid",
        unusable="censusGeoid IS NULL OR trim(censusGeoid) = ''",
        county_expr="substr(censusGeoid, 1, 5)",
        expected_counties=None,
        note="raw is MI-scoped by decision, and policies are not sold in every county",
        national=False,
    ),
    MichiganGate(
        source="fema_declarations",
        table="raw.ins_fema_declarations",
        michigan_filter="state = 'MI'",
        geography_column="fipsCountyCode",
        # '000' is a statewide declaration: a real row that names no county.
        unusable="fipsCountyCode IS NULL OR trim(fipsCountyCode) = '' OR fipsCountyCode = '000'",
        county_expr="fipsStateCode || fipsCountyCode",
        expected_counties=None,
        note="a declaration covers the counties it names, so the roster is coverage not a census",
    ),
    MichiganGate(
        source="cfpb_complaints",
        table="raw.fin_cfpb_complaints",
        michigan_filter="State = 'MI'",
        geography_column='"ZIP code"',
        unusable='"ZIP code" IS NULL OR trim("ZIP code") = \'\'',
        # There is no county column at all -- this is the finding, not an omission.
        county_expr=None,
        expected_counties=None,
        note="CFPB publishes ZIP and state only; county grain needs a ZIP-to-county crosswalk",
    ),
    MichiganGate(
        source="hmda_lar",
        table="raw.fin_hmda_lar",
        michigan_filter="state_code = 'MI'",
        geography_column="county_code",
        # 'NA' is the publisher's own literal, and an out-of-state FIPS on an MI filing is
        # just as unusable for a Michigan county measure.
        unusable=(
            "county_code IS NULL OR trim(county_code) = '' OR county_code = 'NA' "
            f"OR county_code NOT LIKE '{MICHIGAN_STATE_FIPS}%'"
        ),
        county_expr="county_code",
        expected_counties=MICHIGAN_COUNTY_COUNT,
        note="the denominator for every county-grain lending rate",
        national=False,
    ),
    MichiganGate(
        source="cms_geographic_variation",
        table="raw.hlt_cms_geographic_variation",
        michigan_filter=(f"BENE_GEO_LVL = 'County' AND BENE_GEO_CD LIKE '{MICHIGAN_STATE_FIPS}%'"),
        geography_column="BENE_GEO_CD",
        # `UNKNOWN_COUNTY_SUFFIX` comes from the fetcher, which already owns what an
        # unassigned county is and asserts the same 83-county roster on every run. One
        # definition, referenced twice, rather than two rules that could drift apart.
        unusable=f"BENE_GEO_CD LIKE '%{cms.UNKNOWN_COUNTY_SUFFIX}'",
        county_expr="BENE_GEO_CD",
        expected_counties=MICHIGAN_COUNTY_COUNT,
        note="the denominator for HLT-E2 and HLT-E3 at county grain",
    ),
    *(
        MichiganGate(
            source=source,
            table=table,
            michigan_filter=f"geo_level = 'county' AND state = '{MICHIGAN_STATE_FIPS}'",
            geography_column="county",
            unusable="county IS NULL OR trim(county) = ''",
            county_expr="state || county",
            expected_counties=MICHIGAN_COUNTY_COUNT,
            note="the population and housing denominator every per-capita rate rests on",
        )
        for source, table in (
            ("acs5_detailed", "raw.ref_acs5_detailed"),
            ("acs5_subject", "raw.ref_acs5_subject"),
        )
    ),
    MichiganGate(
        source="zip_county_crosswalk",
        table="raw.ref_zip_county_crosswalk",
        michigan_filter="state = 'MI'",
        geography_column="geoid",
        # A county FIPS is five digits. HUD serves a few two-character territory codes with
        # no county component; none of them is Michigan's, but the predicate says what it
        # means rather than assuming.
        unusable="geoid IS NULL OR trim(geoid) = '' OR length(geoid) <> 5",
        county_expr="geoid",
        expected_counties=MICHIGAN_COUNTY_COUNT,
        note=(
            "the bridge that gives CFPB complaints a county at all -- a missing county here "
            "would be missing from every county-grain complaint rate"
        ),
    ),
)


def gate_for(source: str) -> MichiganGate | None:
    return next((gate for gate in GATES if gate.source == source), None)


def usable_rates(con: duckdb.DuckDBPyConnection, gate: MichiganGate) -> tuple[int, int, int, int]:
    """Michigan and national row counts, with the unusable share of each. One scan."""
    national_scope = "1 = 1" if gate.national else gate.michigan_filter
    row = con.execute(
        f"""
        SELECT
          count(*) FILTER (WHERE {gate.michigan_filter}),
          count(*) FILTER (WHERE ({gate.michigan_filter}) AND ({gate.unusable})),
          count(*) FILTER (WHERE {national_scope}),
          count(*) FILTER (WHERE ({national_scope}) AND ({gate.unusable}))
        FROM {gate.table}
        """
    ).fetchone()
    return tuple(int(value or 0) for value in row)


def michigan_counties(con: duckdb.DuckDBPyConnection, gate: MichiganGate) -> list[str]:
    """The distinct Michigan county codes the source carries, as the publisher wrote them."""
    if gate.county_expr is None:
        return []
    rows = con.execute(
        f"SELECT DISTINCT {gate.county_expr} FROM {gate.table} "
        f"WHERE ({gate.michigan_filter}) AND NOT ({gate.unusable}) ORDER BY 1"
    ).fetchall()
    return [str(row[0]) for row in rows if row[0] is not None]


def check_michigan_geography(
    con: duckdb.DuckDBPyConnection, track: str, gate: MichiganGate, mode: str = "live"
) -> list[Result]:
    """Report how much of Michigan this source can actually place on a map."""
    results: list[Result] = []
    mi_rows, mi_unusable, nat_rows, nat_unusable = usable_rates(con, gate)

    if not mi_rows:
        return [
            Result(
                track,
                gate.source,
                "MI geography usable",
                WARN,
                f"no rows match {gate.michigan_filter}",
            )
        ]

    mi_pct = 100 * mi_unusable / mi_rows
    benchmark = (
        f", national {100 * nat_unusable / nat_rows:.2f}%"
        if gate.national and nat_rows
        else " (raw is MI-scoped, so there is no national rate to compare)"
    )
    # A fixture is stratified to over-represent edge cases, so its unusable rate is higher
    # than production's by construction -- HMDA's sample reads 4% against 1.1% live. The
    # number is still worth printing, because it proves the predicate ran; it is the
    # threshold that would be meaningless, so it is not applied.
    results.append(
        Result(
            track,
            gate.source,
            "MI geography usable",
            PASS if mode == "fixture" or mi_pct <= UNUSABLE_WARN_PCT else WARN,
            f"{mi_unusable:,} of {mi_rows:,} MI rows have no usable county "
            f"via {gate.geography_column} ({mi_pct:.2f}%{benchmark})"
            + (" -- sample rate, not a production estimate" if mode == "fixture" else ""),
        )
    )

    if mode == "fixture" and gate.expected_counties is not None:
        # "Did we drop a county?" is a coverage question about the whole dataset, and the
        # same rule the state roster and year coverage follow applies here: a stratified
        # sample cannot answer it. The HMDA sample carries 68 of 83 and is correct.
        # Whether a fixture was built with the roster it needs belongs to the generator.
        results.append(
            Result(
                track,
                gate.source,
                "MI county roster",
                SKIP,
                "fixture is a stratified sample, not a census; coverage is a live check",
            )
        )
        return results

    codes = michigan_counties(con, gate)
    if gate.county_expr is None:
        results.append(
            Result(track, gate.source, "MI county roster", PASS, f"no county column -- {gate.note}")
        )
        return results

    present = len(codes)
    if gate.expected_counties is None:
        detail = f"{present} of {MICHIGAN_COUNTY_COUNT} counties present -- {gate.note}"
        status = PASS
    elif present == gate.expected_counties:
        detail = f"all {present} counties present -- {gate.note}"
        status = PASS
    else:
        detail = (
            f"{present} of {gate.expected_counties} counties present -- {gate.note}, "
            "so every rate built on it would be wrong"
        )
        status = WARN
    results.append(Result(track, gate.source, "MI county roster", status, detail))
    return results
