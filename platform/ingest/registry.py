"""What raw holds: one entry per DuckDB raw table, and where its parquet lives.

Extracted after all twelve fetchers were written and merged, not designed ahead of them --
CLAUDE.md section 8. Two things now need the same list and neither owns it:

  * `ingest.reload` rebuilds every table from parquet with no network at all
  * `reconcile.l1_integrity` verifies every table against its landing and its publisher

Each fetcher still declares its own `TABLE` and `PARTITION_COLUMN` at the top of its module,
because that is where a reader looks for them. This file does not replace those constants and
must not drift from them: `tests/test_registry.py` asserts every entry matches the fetcher that
produces it, and that no fetcher is missing.

Deliberately excluded: landing paths, readers, control-sum columns and check functions. Those
belong to whoever consumes them -- L1 keeps them in its own `SourceSpec` -- because a registry
that tries to describe everything about a source stops being readable.
"""

from __future__ import annotations

from dataclasses import dataclass

TRACKS = ("insurance", "fintech", "health", "shared")


@dataclass(frozen=True)
class RawSource:
    """One raw table: which track owns it, and which parquet tree it is built from.

    `source` is the directory name under both `data/raw/<track>/` and
    `data/landing/<track>/`, which is why it is a single field rather than a path -- the
    root differs between live and fixture mode and belongs to the caller.
    """

    track: str
    source: str
    table: str
    partition_column: str


# Ordered by track, then by the order each source landed in session 1.
SOURCES: tuple[RawSource, ...] = (
    RawSource("insurance", "nfip_claims", "raw.ins_nfip_claims", "state"),
    RawSource("insurance", "nfip_policies", "raw.ins_nfip_policies", "propertyState"),
    RawSource("insurance", "fema_declarations", "raw.ins_fema_declarations", "state"),
    RawSource("fintech", "cfpb_complaints", "raw.fin_cfpb_complaints", "year"),
    RawSource("fintech", "hmda_lar", "raw.fin_hmda_lar", "activity_year"),
    RawSource("fintech", "hmda_institutions", "raw.fin_hmda_institutions", "period"),
    RawSource("health", "cdc_healthy_aging", "raw.hlt_cdc_healthy_aging", "locationabbr"),
    RawSource(
        "health",
        "cms_geographic_variation",
        "raw.hlt_cms_geographic_variation",
        "BENE_GEO_LVL",
    ),
    RawSource("shared", "acs5_detailed", "raw.ref_acs5_detailed", "vintage"),
    RawSource("shared", "acs5_subject", "raw.ref_acs5_subject", "vintage"),
    RawSource("shared", "cpi_u", "raw.ref_cpi_u", "year"),
    RawSource("shared", "cpi_u_series", "raw.ref_cpi_u_series", "seasonal"),
    RawSource("shared", "zip_county_crosswalk", "raw.ref_zip_county_crosswalk", "state"),
)


def sources_for(track: str) -> tuple[RawSource, ...]:
    """The sources one track owns, or all of them for 'all'.

    A track with nothing registered returns empty rather than raising: `just health` and
    `just shared` both call into this before every one of their sources has landed.
    """
    if track == "all":
        return SOURCES
    return tuple(spec for spec in SOURCES if spec.track == track)
