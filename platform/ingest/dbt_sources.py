"""Write dbt's source declarations from the raw registry.

Twelve sources hand-listed in a YAML file is twelve chances to drift from what `raw`
actually holds. `ingest.registry` already owns that list for `ingest.reload` and
`reconcile.l1_integrity`; this makes dbt a third consumer of the same list rather than a
second copy of it.

The output is **committed**, not generated at parse time. A generated-then-committed file
gives a readable diff when a source is added and needs no dbt hook to stay honest --
`tests/test_dbt_sources.py` regenerates it and compares, exactly the way
`tests/test_registry.py` compares the registry against the fetchers' own constants. Run
`just dbt-sources` after touching the registry and the diff is the review.

The YAML is written as text rather than through a serializer for two reasons: the header
comment explaining where the file comes from survives, and the byte output is trivially
deterministic, which is what the drift test rests on.

Run:  python -m ingest.dbt_sources [--check]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ingest.common import REPO_ROOT
from ingest.registry import SOURCES, TRACKS, RawSource

SOURCES_YML = REPO_ROOT / "transform" / "models" / "staging" / "_sources.yml"

# One dbt source per track, so `source('raw_fintech', 'hmda_lar')` names the domain at the
# call site. CLAUDE.md rule 9 puts every table in one `raw` schema with a track prefix for
# the same reason: a query's domain should be visible without looking anything up.
SOURCE_PREFIX = "raw_"
RAW_SCHEMA = "raw"

TRACK_BLURB = {
    "insurance": "FEMA National Flood Insurance Program and disaster declarations.",
    "fintech": "CFPB consumer complaints and HMDA mortgage lending.",
    "health": "CDC healthy-aging indicators and CMS Medicare geographic variation.",
    "shared": "Domain-neutral reference series every track may use.",
}

HEADER = """\
# GENERATED FILE -- do not edit by hand.
#
# Written by `python -m ingest.dbt_sources` (recipe: `just dbt-sources`) from
# `ingest.registry.SOURCES`, the one list of what the `raw` schema holds.
# `tests/test_dbt_sources.py` regenerates this and fails if the committed copy differs,
# so a source added to the registry cannot silently go missing from dbt.
#
# `database` is deliberately unset: it resolves to the target's own database, which is
# `bi_portfolio` on the live warehouse and `fixture` in CI. Both carry the same `raw`
# schema and the same table names, so every model below resolves in either target and
# `just ci` can build the whole project offline against the committed samples.
#
# Nothing here is typed. Raw is all-varchar by rule (CLAUDE.md rule 2) except where a
# publisher shipped parquet with its schema embedded; typing happens in staging, column
# by column, deliberately.

version: 2

sources:
"""


def table_block(spec: RawSource) -> str:
    """One dbt table entry: the registry's four fields, in dbt's vocabulary."""
    _, _, identifier = spec.table.partition(".")
    return (
        f"      - name: {spec.source}\n"
        f"        identifier: {identifier}\n"
        f"        description: >\n"
        f"          Raw `{spec.table}`, partitioned on `{spec.partition_column}`.\n"
        f"          Untyped -- see the staging model that reads it.\n"
    )


def render() -> str:
    """The full file, tracks in registry order, sources in registry order within each."""
    blocks = [HEADER]
    for track in TRACKS:
        specs = [spec for spec in SOURCES if spec.track == track]
        if not specs:
            continue
        blocks.append(
            f"  - name: {SOURCE_PREFIX}{track}\n"
            f"    schema: {RAW_SCHEMA}\n"
            f"    description: {TRACK_BLURB[track]}\n"
            f"    tables:\n"
        )
        blocks.extend(table_block(spec) for spec in specs)
    return "".join(blocks)


def write(path: Path = SOURCES_YML) -> Path:
    """LF and a trailing newline, so Windows runs and pre-commit agree on the bytes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render(), encoding="utf-8", newline="\n")
    return path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit non-zero if the committed file is not what this would write",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    expected = render()

    if args.check:
        actual = SOURCES_YML.read_text(encoding="utf-8") if SOURCES_YML.exists() else ""
        if actual == expected:
            print(f"{SOURCES_YML.relative_to(REPO_ROOT)} is current ({len(SOURCES)} sources)")
            return 0
        print(
            f"{SOURCES_YML.relative_to(REPO_ROOT)} is stale. Run `just dbt-sources`.",
            file=sys.stderr,
        )
        return 1

    path = write()
    print(f"wrote {path.relative_to(REPO_ROOT)} ({len(SOURCES)} sources)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
