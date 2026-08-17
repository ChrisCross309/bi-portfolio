"""dbt's source declarations must not drift from the registry that produced them.

`transform/models/staging/_sources.yml` is generated and committed. Committing it buys a
readable diff when a source is added; this is what stops the committed copy from becoming
a stale second list of what `raw` holds. Same shape as `tests/test_registry.py`, which
keeps the registry itself honest against the fetchers -- the file is regenerated in
memory and compared byte for byte.
"""

from pathlib import Path

from ingest.dbt_sources import SOURCES_YML, render
from ingest.registry import SOURCES

TRANSFORM = Path(__file__).resolve().parents[1] / "transform"


def test_committed_sources_yml_matches_the_generator() -> None:
    """The whole point. Run `just dbt-sources` when this fails."""
    assert SOURCES_YML.read_text(encoding="utf-8") == render()


def test_every_registered_source_appears_exactly_once() -> None:
    rendered = render()
    for spec in SOURCES:
        _, _, identifier = spec.table.partition(".")
        assert rendered.count(f"identifier: {identifier}\n") == 1, spec.table
        assert rendered.count(f"      - name: {spec.source}\n") == 1, spec.source


def test_sources_are_grouped_one_dbt_source_per_track() -> None:
    """`source('raw_fintech', 'hmda_lar')` should name the domain at the call site."""
    rendered = render()
    tracks = {spec.track for spec in SOURCES}
    for track in tracks:
        assert f"  - name: raw_{track}\n" in rendered
    assert rendered.count("    schema: raw\n") == len(tracks)


def test_no_source_declares_a_database() -> None:
    """A hardcoded database would pin every model to the live warehouse.

    The `ci` target reads `fixture.duckdb`, whose database is `fixture`, and CI has no
    live warehouse at all -- `data/` and `platform/duckdb/` are gitignored. Letting the
    database resolve from the target is what makes one set of models build in both.
    """
    assert "database:" not in render()


def test_generated_file_is_lf_with_a_trailing_newline() -> None:
    """Windows text mode would rewrite this on every run and pre-commit would rewrite back."""
    raw = SOURCES_YML.read_bytes()
    assert b"\r\n" not in raw
    assert raw.endswith(b"\n")


def test_dbt_project_disables_anonymous_usage_stats() -> None:
    """CLAUDE.md section 6: CI never calls an external API. Telemetry is an API call."""
    project = (TRANSFORM / "dbt_project.yml").read_text(encoding="utf-8")
    assert "send_anonymous_usage_stats: false" in project


def test_dbt_lives_beside_platform_and_stays_unimportable() -> None:
    """`platform/` is on sys.path (CLAUDE.md section 5); a dbt project must not be."""
    assert (TRANSFORM / "dbt_project.yml").exists()
    assert not (TRANSFORM / "__init__.py").exists()
    assert not list(TRANSFORM.glob("*.py"))
