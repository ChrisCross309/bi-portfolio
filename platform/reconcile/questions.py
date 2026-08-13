"""Question coverage: is each executive question actually answerable from raw?

CLAUDE.md section 2 sets the finish line for session 1: *a raw pipeline is not done because
data landed, it is done when the data the questions need has landed.* This is what proves
it, for all fifteen questions, by their stable IDs.

These are **smoke checks, not answers.** Each question names the ingredients a person would
reach for -- a column, a geography, a benchmark, a deflator -- and the check asserts they are
present and non-empty. Computing INS-E1 is session 2's job; proving nothing is missing before
session 2 starts is this one's.

Two questions need something no table holds, and say so rather than pretending:

  * **INS-E5** asks whether our totals tie to FEMA's published figures. The publisher's own
    total lives in the manifest, not in a table, and the tie itself is what L1's count chain
    already checks. Here the ingredient is that the figure was recorded at all.
  * **FIN-E4** compares Michigan mortgage volume against national. HMDA raw is MI-scoped by
    decision (CLAUDE.md section 4), so the national side comes from the publisher's
    aggregation endpoint, recorded per year in the manifest's `control_totals`.

An ingredient with a minimum above one is a *coverage* claim -- "there are at least five
years to trend" -- and coverage cannot be asserted against a stratified fixture. In fixture
mode those minimums relax to presence, exactly as the state roster, year coverage and county
roster already do.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import duckdb
from reconcile.results import FAIL, PASS, SKIP, Result


@dataclass(frozen=True)
class Ingredient:
    """One thing a question needs, and how to prove it is there.

    Exactly one of `sql` or `manifest` is set. `minimum` above 1 is a coverage claim and is
    relaxed to 1 against a fixture.
    """

    name: str
    sql: str | None = None
    manifest: tuple[str, str, str] | None = None  # (track, source, manifest key)
    minimum: int = 1


@dataclass(frozen=True)
class Question:
    id: str
    track: str
    asks: str
    ingredients: tuple[Ingredient, ...]


# Shared fragments, written once so a change of mind lands in one place.
MI_CLAIMS = "FROM raw.ins_nfip_claims WHERE state = 'MI'"
MI_COMPLAINTS = "FROM raw.fin_cfpb_complaints WHERE State = 'MI'"
MI_CMS_COUNTY = (
    "FROM raw.hlt_cms_geographic_variation WHERE BENE_GEO_LVL = 'County' AND BENE_GEO_CD LIKE '26%'"
)
CPI_DEFLATOR = (
    "SELECT count(*) FROM raw.ref_cpi_u WHERE trim(series_id) = 'CUUR0000SA0' "
    "AND trim(period) = 'M13'"
)


QUESTIONS: tuple[Question, ...] = (
    # ── insurance ─────────────────────────────────────────────────────────────
    Question(
        "INS-E1",
        "insurance",
        "MI flood cost in the latest complete loss year, vs prior year and trailing 5",
        (
            Ingredient(
                "MI claims with a loss date",
                f"SELECT count(*) {MI_CLAIMS} AND dateOfLoss IS NOT NULL",
            ),
            Ingredient("paid amounts", f"SELECT count(amountPaidOnBuildingClaim) {MI_CLAIMS}"),
            # Latest, prior, and five trailing years is six distinct years at minimum.
            Ingredient(
                "enough loss years to trend",
                f"SELECT count(DISTINCT year(dateOfLoss)) {MI_CLAIMS}",
                minimum=6,
            ),
        ),
    ),
    Question(
        "INS-E2",
        "insurance",
        "MI severity per claim in constant dollars, vs the national median",
        (
            Ingredient(
                "paid amounts nationally",
                "SELECT count(amountPaidOnBuildingClaim) FROM raw.ins_nfip_claims",
            ),
            Ingredient(
                "states to take a median across",
                "SELECT count(DISTINCT state) FROM raw.ins_nfip_claims",
                minimum=50,
            ),
            Ingredient("CPI-U annual deflator", CPI_DEFLATOR),
        ),
    ),
    Question(
        "INS-E3",
        "insurance",
        "share of MI paid losses that is event-driven vs attritional",
        (
            Ingredient(
                "MI claims with a loss date",
                f"SELECT count(*) {MI_CLAIMS} AND dateOfLoss IS NOT NULL",
            ),
            Ingredient(
                "MI declarations with an incident window",
                "SELECT count(*) FROM raw.ins_fema_declarations WHERE state = 'MI' "
                "AND incidentBeginDate IS NOT NULL",
            ),
        ),
    ),
    Question(
        "INS-E4",
        "insurance",
        "MI policies in force, and take-up per 1,000 housing units vs national",
        (
            Ingredient(
                "MI policies with a policy count",
                "SELECT count(policyCount) FROM raw.ins_nfip_policies",
            ),
            Ingredient(
                "ACS housing units for MI counties",
                "SELECT count(B25001_001E) FROM raw.ref_acs5_detailed "
                "WHERE geo_level = 'county' AND state = '26'",
            ),
            Ingredient(
                "ACS housing units nationally",
                "SELECT count(B25001_001E) FROM raw.ref_acs5_detailed WHERE geo_level = 'us'",
            ),
        ),
    ),
    Question(
        "INS-E5",
        "insurance",
        "do our totals tie to FEMA's published figures",
        (
            Ingredient("claims landed", "SELECT count(*) FROM raw.ins_nfip_claims"),
            Ingredient("declarations landed", "SELECT count(*) FROM raw.ins_fema_declarations"),
            # The tie itself is the count chain's job; this is that FEMA's own figure exists
            # to tie against.
            Ingredient(
                "FEMA's own claim total recorded",
                manifest=("insurance", "nfip_claims", "source_reported_count"),
            ),
        ),
    ),
    # ── fintech ───────────────────────────────────────────────────────────────
    Question(
        "FIN-E1",
        "fintech",
        "MI complaint volume in the latest complete window, per capita vs national",
        (
            Ingredient(
                "MI complaints with a received date",
                f'SELECT count("Date received") {MI_COMPLAINTS}',
            ),
            Ingredient(
                "enough months to trend",
                f'SELECT count(DISTINCT substr("Date received", 1, 7)) {MI_COMPLAINTS}',
                minimum=12,
            ),
            Ingredient(
                "ACS population for MI counties",
                "SELECT count(B01003_001E) FROM raw.ref_acs5_detailed "
                "WHERE geo_level = 'county' AND state = '26'",
            ),
            Ingredient(
                "ACS population nationally",
                "SELECT count(B01003_001E) FROM raw.ref_acs5_detailed WHERE geo_level = 'us'",
            ),
        ),
    ),
    Question(
        "FIN-E2",
        "fintech",
        "MI's largest and fastest-growing complaint product",
        (
            Ingredient(
                "MI products", f'SELECT count(DISTINCT "Product") {MI_COMPLAINTS}', minimum=2
            ),
            Ingredient(
                "years to measure growth across",
                f'SELECT count(DISTINCT "year") {MI_COMPLAINTS}',
                minimum=2,
            ),
        ),
    ),
    Question(
        "FIN-E3",
        "fintech",
        "timeliness and relief for MI consumers, vs a year ago and vs national",
        (
            Ingredient("timely-response flag", f'SELECT count("Timely response?") {MI_COMPLAINTS}'),
            Ingredient(
                "company response outcomes",
                f'SELECT count(DISTINCT "Company response to consumer") {MI_COMPLAINTS}',
                minimum=2,
            ),
            Ingredient(
                "national comparison rows",
                'SELECT count("Timely response?") FROM raw.fin_cfpb_complaints',
            ),
        ),
    ),
    Question(
        "FIN-E4",
        "fintech",
        "MI origination volume and denial rate, vs prior year and vs national",
        (
            Ingredient(
                "originated and denied actions",
                "SELECT count(DISTINCT action_taken) FROM raw.fin_hmda_lar",
                minimum=2,
            ),
            Ingredient("loan amounts", "SELECT count(loan_amount) FROM raw.fin_hmda_lar"),
            Ingredient(
                "years to compare",
                "SELECT count(DISTINCT activity_year) FROM raw.fin_hmda_lar",
                minimum=2,
            ),
            # Raw is MI-scoped by decision, so national is the publisher's own aggregate.
            Ingredient(
                "national control totals recorded",
                manifest=("fintech", "hmda_lar", "control_totals"),
            ),
        ),
    ),
    Question(
        "FIN-E5",
        "fintech",
        "MI lenders' complaints per $1B originated, against peers",
        (
            Ingredient(
                "lender identifiers with loan amounts",
                "SELECT count(*) FROM raw.fin_hmda_lar WHERE lei IS NOT NULL "
                "AND loan_amount IS NOT NULL",
            ),
            Ingredient(
                "the institution name reference",
                "SELECT count(*) FROM raw.fin_hmda_institutions WHERE lei IS NOT NULL "
                "AND name IS NOT NULL",
            ),
            # CFPB names companies, HMDA identifies them by LEI, and nothing published
            # joins the two -- entity resolution is session 2's problem. Both sides being
            # present is what raw can promise.
            Ingredient("complaint company names", f'SELECT count("Company") {MI_COMPLAINTS}'),
        ),
    ),
    # ── health ────────────────────────────────────────────────────────────────
    Question(
        "HLT-E1",
        "health",
        "MI subjective cognitive decline among adults 45+, CI-aware, vs national",
        (
            Ingredient(
                "MI cognitive-decline estimates with confidence limits",
                "SELECT count(*) FROM raw.hlt_cdc_healthy_aging WHERE locationabbr = 'MI' "
                "AND lower(topic) LIKE '%cognitive decline%' AND low_confidence_limit IS NOT NULL",
            ),
            Ingredient(
                "the national benchmark row",
                "SELECT count(*) FROM raw.hlt_cdc_healthy_aging WHERE locationabbr = 'US' "
                "AND lower(topic) LIKE '%cognitive decline%'",
            ),
        ),
    ),
    Question(
        "HLT-E2",
        "health",
        "MI skilled-nursing and home-health use vs national, five-year direction",
        (
            Ingredient(
                "MI county SNF and home-health per beneficiary",
                f"SELECT count(*) {MI_CMS_COUNTY} AND SNF_MDCR_STDZD_PYMT_PC IS NOT NULL "
                "AND HH_MDCR_STDZD_PYMT_PC IS NOT NULL",
            ),
            Ingredient(
                "the national benchmark",
                "SELECT count(*) FROM raw.hlt_cms_geographic_variation "
                "WHERE BENE_GEO_LVL = 'National'",
            ),
            Ingredient(
                "five years to trend",
                f"SELECT count(DISTINCT YEAR) {MI_CMS_COUNTY}",
                minimum=5,
            ),
        ),
    ),
    Question(
        "HLT-E3",
        "health",
        "MI Medicare cost per 65+ beneficiary in constant dollars, vs national",
        (
            Ingredient(
                "standardized cost per beneficiary for 65+",
                "SELECT count(TOT_MDCR_STDZD_PYMT_PC) FROM raw.hlt_cms_geographic_variation "
                "WHERE BENE_AGE_LVL = '>=65'",
            ),
            Ingredient(
                "MI and national both present at 65+",
                "SELECT count(DISTINCT BENE_GEO_LVL) FROM raw.hlt_cms_geographic_variation "
                "WHERE BENE_AGE_LVL = '>=65' "
                "AND (BENE_GEO_DESC = 'MI' OR BENE_GEO_LVL = 'National')",
                minimum=2,
            ),
            Ingredient("CPI-U annual deflator", CPI_DEFLATOR),
        ),
    ),
    Question(
        "HLT-E4",
        "health",
        "whether MI caregiver-burden indicators are improving or deteriorating",
        (
            Ingredient(
                "MI caregiving indicators",
                "SELECT count(*) FROM raw.hlt_cdc_healthy_aging WHERE locationabbr = 'MI' "
                "AND lower(class) LIKE '%caregiv%'",
            ),
            Ingredient(
                "cycles to compare",
                "SELECT count(DISTINCT yearstart) FROM raw.hlt_cdc_healthy_aging "
                "WHERE locationabbr = 'MI' AND lower(class) LIKE '%caregiv%'",
                minimum=2,
            ),
        ),
    ),
    Question(
        "HLT-E5",
        "health",
        "whether MI's 65+ need is growing faster than its screening and care indicators",
        (
            Ingredient(
                "ACS 65+ population for MI counties",
                "SELECT count(S0101_C01_030E) FROM raw.ref_acs5_subject "
                "WHERE geo_level = 'county' AND state = '26'",
            ),
            Ingredient(
                "vintages to project across",
                "SELECT count(DISTINCT vintage) FROM raw.ref_acs5_subject "
                "WHERE geo_level = 'county' AND state = '26'",
                minimum=5,
            ),
            Ingredient(
                "MI screening and care indicators",
                "SELECT count(*) FROM raw.hlt_cdc_healthy_aging WHERE locationabbr = 'MI'",
            ),
        ),
    ),
)


def questions_for(track: str) -> tuple[Question, ...]:
    if track == "all":
        return QUESTIONS
    return tuple(question for question in QUESTIONS if question.track == track)


def manifest_value(data_root: Path, track: str, source: str, key: str):
    path = data_root / "raw" / track / source / "manifest.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8")).get(key)


class NotIngested(Exception):
    """A table the question needs does not exist yet, which is not the same as empty."""


def measure(
    con: duckdb.DuckDBPyConnection, ingredient: Ingredient, data_root: Path
) -> tuple[bool, str]:
    """Is this ingredient present? Returns (ok, what we found).

    A missing *table* raises rather than returning zero, because the two mean different
    things: an un-ingested source is a "come back later", while an empty one is a defect.
    A missing *column* is neither -- it means the publisher's schema moved under us -- so it
    surfaces as a failure carrying the database's own message.
    """
    if ingredient.manifest is not None:
        value = manifest_value(data_root, *ingredient.manifest)
        return (value is not None), ("recorded" if value is not None else "absent")
    try:
        found = int(con.execute(ingredient.sql).fetchone()[0] or 0)
    except duckdb.CatalogException as error:
        raise NotIngested(str(error).splitlines()[0]) from error
    except duckdb.Error as error:
        return False, f"query failed: {str(error).splitlines()[0]}"
    return found >= ingredient.minimum, f"{found:,}"


def check_question(
    con: duckdb.DuckDBPyConnection, question: Question, data_root: Path, mode: str = "live"
) -> Result:
    """One line per question: everything it needs is there, or exactly what is not."""
    missing: list[str] = []
    for ingredient in question.ingredients:
        # A minimum above one is a coverage claim, and a stratified fixture cannot answer
        # one -- the same rule the roster and year-coverage checks follow.
        effective = (
            Ingredient(ingredient.name, ingredient.sql, ingredient.manifest, 1)
            if mode == "fixture"
            else ingredient
        )
        try:
            ok, found = measure(con, effective, data_root)
        except NotIngested as absent:
            # Questions reach across tracks -- INS-E4 needs NFIP policies and ACS housing
            # units -- so `just insurance` alone can legitimately run before `just shared`.
            # That is a "not yet", not a defect, and must not fail the run.
            return Result(
                question.track,
                question.id,
                "answerable from raw",
                SKIP,
                f"{question.asks} -- {absent}",
            )
        if not ok:
            missing.append(f"{ingredient.name} ({found}, needs {ingredient.minimum:,})")

    if missing:
        return Result(
            question.track,
            question.id,
            "answerable from raw",
            FAIL,
            f"{question.asks} -- missing: {'; '.join(missing)}",
        )
    return Result(
        question.track,
        question.id,
        "answerable from raw",
        PASS,
        f"{len(question.ingredients)} ingredients present: {question.asks}",
    )


def check_question_coverage(
    con: duckdb.DuckDBPyConnection, track: str, data_root: Path, mode: str = "live"
) -> list[Result]:
    return [check_question(con, question, data_root, mode) for question in questions_for(track)]
