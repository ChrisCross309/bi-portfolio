"""The registry must not drift from the fetchers that produce the tables it lists.

`ingest.registry` restates twelve table names that already live as module constants in nine
fetchers. That is a deliberate duplication -- a reader looks for `TABLE` at the top of the
module that writes it -- so this file is what makes it safe: the two are compared entry for
entry, in both directions, and a new source that never reaches the registry fails here.
"""

from ingest.fintech import cfpb, hmda
from ingest.health import cdc, cms
from ingest.insurance import fema_declarations, nfip_claims, nfip_policies
from ingest.registry import SOURCES, TRACKS, sources_for
from ingest.shared import bls, census

# Assembled from the fetchers' own constants, never from the registry.
FROM_FETCHERS = {
    (nfip_claims.TRACK, nfip_claims.SOURCE, nfip_claims.TABLE, nfip_claims.PARTITION_COLUMN),
    (
        nfip_policies.TRACK,
        nfip_policies.SOURCE,
        nfip_policies.TABLE,
        nfip_policies.PARTITION_COLUMN,
    ),
    (
        fema_declarations.TRACK,
        fema_declarations.SOURCE,
        fema_declarations.TABLE,
        fema_declarations.PARTITION_COLUMN,
    ),
    (cfpb.TRACK, cfpb.SOURCE, cfpb.TABLE, cfpb.PARTITION_COLUMN),
    (hmda.TRACK, hmda.LAR_SOURCE, hmda.LAR_TABLE, hmda.LAR_PARTITION_COLUMN),
    (
        hmda.TRACK,
        hmda.INSTITUTIONS_SOURCE,
        hmda.INSTITUTIONS_TABLE,
        hmda.INSTITUTIONS_PARTITION_COLUMN,
    ),
    (cdc.TRACK, cdc.SOURCE, cdc.TABLE, cdc.PARTITION_COLUMN),
    (cms.TRACK, cms.SOURCE, cms.TABLE, cms.PARTITION_COLUMN),
    (bls.TRACK, bls.OBSERVATIONS_SOURCE, bls.OBSERVATIONS_TABLE, bls.OBSERVATIONS_PARTITION_COLUMN),
    (bls.TRACK, bls.SERIES_SOURCE, bls.SERIES_TABLE, bls.SERIES_PARTITION_COLUMN),
    *(
        (census.TRACK, spec["source"], spec["table"], census.PARTITION_COLUMN)
        for spec in census.DATASETS.values()
    ),
}


def test_registry_matches_the_fetchers_exactly() -> None:
    """Both directions: no invented entry, and no source that forgot to register."""
    registered = {(s.track, s.source, s.table, s.partition_column) for s in SOURCES}
    assert registered == FROM_FETCHERS


def test_registry_holds_twelve_sources_with_unique_names_and_tables() -> None:
    assert len(SOURCES) == 12
    assert len({(s.track, s.source) for s in SOURCES}) == 12
    assert len({s.table for s in SOURCES}) == 12


def test_every_table_is_track_prefixed_in_the_raw_schema() -> None:
    """CLAUDE.md rule 9: one `raw` schema, prefixed so a query's domain is visible."""
    prefixes = {"insurance": "ins_", "fintech": "fin_", "health": "hlt_", "shared": "ref_"}
    for spec in SOURCES:
        assert spec.track in TRACKS
        assert spec.table.startswith(f"raw.{prefixes[spec.track]}"), spec.table


def test_sources_for_filters_by_track_and_answers_all() -> None:
    assert sources_for("all") == SOURCES
    assert {s.track for s in sources_for("health")} == {"health"}
    assert len(sources_for("shared")) == 4
    assert sources_for("nonexistent") == ()
