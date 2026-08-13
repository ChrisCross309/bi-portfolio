"""Unit tests for the question-coverage checks.

The point of these checks is to fail when an ingredient goes missing, so most of what is
worth testing is the failure path -- a check that cannot fail proves nothing.
"""

import re
from pathlib import Path

import duckdb
import pytest
from ingest.common import REPO_ROOT
from reconcile.questions import (
    QUESTIONS,
    Ingredient,
    Question,
    check_question,
    questions_for,
)
from reconcile.results import FAIL, PASS

TRACK_README = {
    "insurance": "projects/01-insurance-nfip/README.md",
    "fintech": "projects/02-fintech-lending/README.md",
    "health": "projects/03-health-dementia/README.md",
}


@pytest.fixture
def con():
    connection = duckdb.connect()
    yield connection
    connection.close()


# ── the set itself ────────────────────────────────────────────────────────────


def test_all_fifteen_questions_are_registered() -> None:
    assert len(QUESTIONS) == 15
    for track in ("insurance", "fintech", "health"):
        assert len(questions_for(track)) == 5, track
    assert questions_for("shared") == ()


def test_the_ids_match_the_project_readmes() -> None:
    """CLAUDE.md section 2: the IDs travel with the work and are never renumbered.

    If a README renames or renumbers a question and this file does not, the coverage check
    would be proving something about a question nobody asks any more.
    """
    for track, path in TRACK_README.items():
        text = (REPO_ROOT / path).read_text(encoding="utf-8")
        in_readme = set(re.findall(r"\*\*((?:INS|FIN|HLT)-E\d)\*\*", text))
        registered = {question.id for question in questions_for(track)}
        assert registered == in_readme, f"{track}: {registered ^ in_readme}"


def test_every_question_names_at_least_two_ingredients() -> None:
    """Every executive question needs a level and something to compare it against, so a
    single-ingredient question is a sign the benchmark half was forgotten."""
    for question in QUESTIONS:
        assert len(question.ingredients) >= 2, question.id


def test_every_ingredient_is_sql_or_a_manifest_key_but_not_both() -> None:
    for question in QUESTIONS:
        for ingredient in question.ingredients:
            assert (ingredient.sql is None) != (ingredient.manifest is None), (
                f"{question.id}: {ingredient.name}"
            )


# ── the failure path ──────────────────────────────────────────────────────────


def _question(*ingredients: Ingredient) -> Question:
    return Question("TST-E1", "insurance", "a test question", ingredients)


def test_a_missing_ingredient_fails_and_names_it(con, tmp_path: Path) -> None:
    con.execute("CREATE TABLE t AS SELECT 1 AS x WHERE false")  # deliberately empty
    result = check_question(
        con, _question(Ingredient("some rows", "SELECT count(*) FROM t")), tmp_path
    )
    assert result.status == FAIL
    assert "some rows" in result.detail


def test_a_present_ingredient_passes(con, tmp_path: Path) -> None:
    con.execute("CREATE TABLE t AS SELECT 1 AS x")
    result = check_question(
        con, _question(Ingredient("some rows", "SELECT count(*) FROM t")), tmp_path
    )
    assert result.status == PASS


def test_a_coverage_minimum_fails_when_short(con, tmp_path: Path) -> None:
    """ "Enough years to trend" is the shape of half these ingredients."""
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (2023), (2024)) AS v(y)")
    ingredient = Ingredient("six years", "SELECT count(DISTINCT y) FROM t", minimum=6)
    result = check_question(con, _question(ingredient), tmp_path)
    assert result.status == FAIL
    assert "needs 6" in result.detail and "(2," in result.detail


def test_a_coverage_minimum_relaxes_against_a_fixture(con, tmp_path: Path) -> None:
    """A stratified sample cannot carry six years, and padding it would prove nothing --
    the same rule the state roster, year coverage and county roster follow."""
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (2023), (2024)) AS v(y)")
    ingredient = Ingredient("six years", "SELECT count(DISTINCT y) FROM t", minimum=6)
    assert check_question(con, _question(ingredient), tmp_path, "fixture").status == PASS
    # But presence is still required, so an empty sample still fails.
    con.execute("CREATE OR REPLACE TABLE t AS SELECT 1 AS y WHERE false")
    assert check_question(con, _question(ingredient), tmp_path, "fixture").status == FAIL


# ── manifest-backed ingredients ───────────────────────────────────────────────


def test_a_manifest_ingredient_reads_the_manifest(con, tmp_path: Path) -> None:
    """INS-E5 and FIN-E4 need figures no table holds: FEMA's own published total, and
    HMDA's national control totals for a raw layer that is MI-scoped by decision."""
    ingredient = Ingredient("a recorded total", manifest=("insurance", "nfip_claims", "total"))
    assert check_question(con, _question(ingredient), tmp_path).status == FAIL

    raw_dir = tmp_path / "raw" / "insurance" / "nfip_claims"
    raw_dir.mkdir(parents=True)
    (raw_dir / "manifest.json").write_text('{"total": 2724656}', encoding="utf-8")
    assert check_question(con, _question(ingredient), tmp_path).status == PASS


def test_the_manifest_ingredients_point_at_registered_sources() -> None:
    """A typo in a source name would read as 'absent' forever rather than as a mistake."""
    from ingest.registry import SOURCES

    known = {(spec.track, spec.source) for spec in SOURCES}
    for question in QUESTIONS:
        for ingredient in question.ingredients:
            if ingredient.manifest is not None:
                track, source, _ = ingredient.manifest
                assert (track, source) in known, f"{question.id}: {track}/{source}"


# ── running before everything is ingested ─────────────────────────────────────


def test_a_missing_table_is_not_yet_rather_than_a_defect(con, tmp_path: Path) -> None:
    """Questions reach across tracks: INS-E4 needs NFIP policies and ACS housing units, so
    `just insurance` can legitimately run before `just shared` has ever been run. That must
    report "not ingested" and not fail the run, and must not crash the harness -- which is
    what it did before this, with a raw CatalogException."""
    from reconcile.questions import check_question_coverage
    from reconcile.results import SKIP

    results = check_question_coverage(con, "insurance", tmp_path)
    assert len(results) == 5
    assert {r.status for r in results} == {SKIP}
    assert "does not exist" in results[0].detail


def test_a_vanished_column_fails_rather_than_skipping(con, tmp_path: Path) -> None:
    """A table that exists but has lost a column means the publisher's schema moved, which
    is a finding -- not a "come back later"."""
    con.execute("CREATE SCHEMA raw")
    con.execute("CREATE TABLE raw.t AS SELECT 1 AS present")
    ingredient = Ingredient("a column that is gone", "SELECT count(gone) FROM raw.t")
    result = check_question(con, _question(ingredient), tmp_path)
    assert result.status == FAIL
    assert "query failed" in result.detail
