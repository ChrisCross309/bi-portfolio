"""Unit tests for the FEMA declarations fetcher.

The anchor-event assertions matter most: INS-E3 splits Michigan losses into
event-driven versus attritional, and a missing anchor makes that split silently
return nothing rather than fail.
"""

import duckdb
import pytest
from ingest.insurance.fema_declarations import (
    ANCHOR_EVENTS,
    TABLE,
    AnchorEvent,
    anchor_failures,
    find_anchor,
)

MIDLAND = next(a for a in ANCHOR_EVENTS if a.key == "midland_dam_failures_2020")
SOUTHEAST = next(a for a in ANCHOR_EVENTS if a.key == "southeast_mi_flooding_2021")


def _declarations(rows: list[tuple]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("CREATE SCHEMA raw")
    con.execute(
        f"""CREATE TABLE {TABLE} (
            disasterNumber BIGINT, state VARCHAR, declarationType VARCHAR,
            incidentType VARCHAR, declarationTitle VARCHAR,
            incidentBeginDate TIMESTAMP, declarationDate TIMESTAMP, designatedArea VARCHAR
        )"""
    )
    con.executemany(f"INSERT INTO {TABLE} VALUES (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    return con


MIDLAND_ROWS = [
    (
        4547,
        "MI",
        "DR",
        "Dam/Levee Break",
        "SEVERE STORMS AND FLOODING",
        "2020-05-16",
        "2020-07-09",
        "Midland (County)",
    ),
    (
        4547,
        "MI",
        "DR",
        "Dam/Levee Break",
        "SEVERE STORMS AND FLOODING",
        "2020-05-16",
        "2020-07-09",
        "Gladwin (County)",
    ),
    (
        3525,
        "MI",
        "EM",
        "Dam/Levee Break",
        "SEVERE STORMS AND FLOODING",
        "2020-05-16",
        "2020-05-21",
        "Midland (County)",
    ),
]
SOUTHEAST_ROWS = [
    (
        4607,
        "MI",
        "DR",
        "Severe Storm",
        "SEVERE STORMS, FLOODING, AND TORNADOES",
        "2021-06-25",
        "2021-07-15",
        "Wayne (County)",
    ),
]
NOISE_ROWS = [
    (4494, "MI", "DR", "Biological", "COVID-19 PANDEMIC", "2020-01-20", "2020-03-27", "Statewide"),
    (
        4611,
        "OH",
        "DR",
        "Severe Storm",
        "SEVERE STORMS AND FLOODING",
        "2021-06-25",
        "2021-07-20",
        "Cuyahoga (County)",
    ),
]


def test_both_anchors_are_found_in_representative_data() -> None:
    con = _declarations(MIDLAND_ROWS + SOUTHEAST_ROWS + NOISE_ROWS)
    found = {a.key: find_anchor(con, a) for a in ANCHOR_EVENTS}
    con.close()
    assert anchor_failures(found) == []
    assert {m["disaster_number"] for m in found[MIDLAND.key]} == {3525, 4547}
    assert {m["disaster_number"] for m in found[SOUTHEAST.key]} == {4607}


def test_anchor_numbers_are_discovered_not_hardcoded() -> None:
    """A reissued declaration must be reported, not missed. Same event, new number."""
    renumbered = [(9901, *row[1:]) for row in MIDLAND_ROWS]
    con = _declarations(renumbered)
    found = find_anchor(con, MIDLAND)
    con.close()
    assert {m["disaster_number"] for m in found} == {9901}


def test_a_missing_anchor_fails_the_run() -> None:
    con = _declarations(NOISE_ROWS)
    found = {a.key: find_anchor(con, a) for a in ANCHOR_EVENTS}
    con.close()
    problems = anchor_failures(found)
    assert len(problems) == 2
    assert "INS-E3" in problems[0]


def test_the_covid_declaration_is_not_mistaken_for_a_flood_event() -> None:
    """MI's largest 2020 declaration by area count is biological, not flood."""
    con = _declarations(NOISE_ROWS)
    assert find_anchor(con, MIDLAND) == []
    con.close()


def test_an_out_of_state_flood_in_the_same_window_is_excluded() -> None:
    con = _declarations(NOISE_ROWS)
    assert find_anchor(con, SOUTHEAST) == []
    con.close()


def test_one_event_spans_many_designated_areas() -> None:
    """The grain is disasterNumber x designated area; counting rows overstates events."""
    con = _declarations(MIDLAND_ROWS)
    matches = find_anchor(con, MIDLAND)
    rows = con.execute(f"SELECT count(*) FROM {TABLE}").fetchone()[0]
    con.close()
    assert rows == 3
    assert len(matches) == 2  # distinct declarations, not distinct areas


@pytest.mark.parametrize("anchor", ANCHOR_EVENTS, ids=lambda a: a.key)
def test_every_anchor_is_michigan_and_described_not_numbered(anchor: AnchorEvent) -> None:
    assert anchor.state == "MI"
    assert anchor.keywords
    assert anchor.incident_from < anchor.incident_to
