"""Track separation, expressed for the semantic layer.

`tests/test_scaffold.py` already holds the three project READMEs to CLAUDE.md section 1 --
no insurance data in the fintech or health docs, and so on -- and it caught a real leak in
session 1. These are the same rule applied to `transform/`, where it is easier to break and
harder to see: a `ref()` two models deep does not look like a domain leak until someone
traces it.

**Conformed models are deliberately exempt from the cross-track rule, and that is the point
of them.** `dim_geography_county` reads NFIP, CMS, HMDA and HUD, because its whole job is to
be the one place three domains resolve geography the same way. What it takes is a key column
and never a measure. The rule that does apply to conformed is the opposite direction, and it
is tested below: a conformed model must sit *beneath* the tracks, depending only on sources
and seeds, never on a track's staging or mart models.

These read the SQL rather than dbt's manifest so they run in `just check` without a built
warehouse, which is what lets CI catch a leak before `dbt build` ever runs.
"""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MODELS = REPO_ROOT / "transform" / "models"

TRACKS = ("insurance", "fintech", "health")

# `source('raw_fintech', 'hmda_lar')` and `ref('stg_fin__hmda_lar')`, however they are spaced.
SOURCE_CALL = re.compile(r"source\(\s*['\"](\w+)['\"]\s*,\s*['\"](\w+)['\"]\s*\)")
REF_CALL = re.compile(r"ref\(\s*['\"](\w+)['\"]\s*\)")

# Model-name prefixes that mark a model as belonging to one track.
TRACK_PREFIX = {"insurance": "ins", "fintech": "fin", "health": "hlt"}


def sql_models(*parts: str) -> list[Path]:
    directory = MODELS.joinpath(*parts)
    return sorted(directory.rglob("*.sql")) if directory.is_dir() else []


def referenced_sources(sql: str) -> set[str]:
    """The dbt source names a model reads, e.g. `raw_health`."""
    return {match.group(1) for match in SOURCE_CALL.finditer(sql)}


def referenced_models(sql: str) -> set[str]:
    return {match.group(1) for match in REF_CALL.finditer(sql)}


def foreign_track(name: str, own_track: str) -> str | None:
    """Which other track a source or model name belongs to, if any."""
    for track, prefix in TRACK_PREFIX.items():
        if track == own_track:
            continue
        if name == f"raw_{track}" or name.startswith((f"stg_{prefix}__", f"fct_{prefix}_")):
            return track
    return None


def test_no_mart_reads_another_tracks_data() -> None:
    """CLAUDE.md section 1. A mart that reaches into another domain is wrong even when
    it is convenient, and this is the check that says so before a reviewer has to."""
    for track in TRACKS:
        for model in sql_models("marts", track):
            sql = model.read_text(encoding="utf-8")
            for name in referenced_sources(sql) | referenced_models(sql):
                leaked = foreign_track(name, track)
                assert leaked is None, (
                    f"{model.relative_to(REPO_ROOT)} is a {track} mart but reads "
                    f"{name!r}, which belongs to {leaked}"
                )


def test_no_staging_model_reads_another_tracks_source() -> None:
    """Staging is a typed view of one source; reaching sideways is never right there."""
    for track in TRACKS:
        for model in sql_models("staging", track):
            sql = model.read_text(encoding="utf-8")
            for name in referenced_sources(sql):
                leaked = foreign_track(name, track)
                assert leaked is None, (
                    f"{model.relative_to(REPO_ROOT)} is a {track} staging model but reads "
                    f"{name!r}, which belongs to {leaked}"
                )


def test_conformed_models_depend_only_on_sources_and_seeds() -> None:
    """A conformed dimension sits beneath the tracks, so nothing track-shaped may be above it.

    Reading every track's *sources* is what a conformed dimension is for. Depending on a
    track's staging or mart model would invert that -- the shared dimension would then be
    downstream of one domain's modelling choices, and a change in that track could silently
    move geography or dates for the other two.
    """
    allowed_prefixes = ("dim_", "seed_", "state_codes")
    for model in sql_models("conformed"):
        sql = model.read_text(encoding="utf-8")
        for name in referenced_models(sql):
            assert name.startswith(allowed_prefixes), (
                f"{model.relative_to(REPO_ROOT)} refs {name!r}; conformed models may only "
                "ref other conformed dimensions and seeds"
            )


def test_every_conformed_model_is_documented() -> None:
    """A dimension three tracks share is the last place an undocumented column belongs."""
    schema = (MODELS / "conformed" / "_conformed.yml").read_text(encoding="utf-8")
    for model in sql_models("conformed"):
        assert f"- name: {model.stem}\n" in schema, f"{model.stem} is missing from _conformed.yml"


def test_conformed_dimensions_enforce_their_contracts() -> None:
    """Contracts are session 2's promise that a mart's join keys cannot silently change type.

    A FIPS that becomes an integer still looks fine and joins nothing -- Alaska is `02`.
    """
    schema = (MODELS / "conformed" / "_conformed.yml").read_text(encoding="utf-8")
    assert schema.count("enforced: true") == len(sql_models("conformed"))


def test_mart_documentation_carries_only_its_own_tracks_question_ids() -> None:
    """The fifteen IDs travel with the work (CLAUDE.md section 2), and they travel per track.

    Vacuous until the marts land in PRs 7-9, which is when a stray `HLT-E2` in the insurance
    mart docs becomes possible -- and it would be exactly the leak this repo is arranged to
    prevent.
    """
    prefixes = {"insurance": "INS", "fintech": "FIN", "health": "HLT"}
    for track in prefixes:
        directory = MODELS / "marts" / track
        for doc in sorted(directory.glob("*.yml")) if directory.is_dir() else []:
            text = doc.read_text(encoding="utf-8")
            for other_track, other in prefixes.items():
                if other_track == track:
                    continue
                found = re.findall(rf"\b{other}-E[1-5]\b", text)
                assert not found, (
                    f"{doc.relative_to(REPO_ROOT)} documents a {track} mart but cites "
                    f"{sorted(set(found))}, which belong to {other_track}"
                )
