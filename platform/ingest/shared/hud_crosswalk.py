"""Ingest HUD's USPS ZIP-to-county crosswalk into DuckDB's raw schema.

Domain-neutral reference, and the thirteenth source. It lands under `shared` for the same
reason CPI-U and the ACS denominators do: nothing about a ZIP-to-county mapping belongs to
one domain, and more than one track may eventually need it.

**Why this source exists at all.** CFPB publishes a complaint's state and ZIP and nothing
else -- there is no county column in that file, which the Michigan geography gate reports
outright. FIN-E1 asks for Michigan complaint volume at county grain. ZIPs are postal
delivery routes, not areas, and **34.6% of Michigan's ZIPs cross a county line**, so turning
one into the other is a weighted *allocation* with a stated rule, never a lookup. HUD
publishes the weights: the share of a ZIP's residential, business and other addresses that
fall in each county, rebuilt quarterly from USPS delivery data. That is the closest thing to
ground truth that exists for this, and it is why the alternative -- the Census ZCTA-to-county
relationship file -- was not used: ZCTAs approximate ZIPs from census blocks, its only
overlap measures are land and water area, and it has not been rebuilt since 2020.

The allocation rule itself is session 2's modelling job, not raw's. Raw's job is to land the
weights faithfully, which per CLAUDE.md rule 2 means every ratio stays text.

Five things this API will do to anyone who has not read it:

  * **`query=All` returns the whole country in one call** -- 54,562 rows, 7.8 MB, about two
    seconds. A per-state loop would stitch a national picture out of 56 responses taken
    minutes apart; one call is a single self-consistent snapshot. A per-state call is still
    made, but as the *verification*, the same way the NFIP claims loader asks FEMA's API for
    per-state counts to check its bulk file.
  * **The error envelope is a JSON list, not an object.** Success is `{"data": {...}}`;
    failure is `[{"error": "No data found using the value MI for type 2"}]` under an HTTP
    4xx. Code that reaches for `.get("data")` dies with an AttributeError on the list, so
    the shape is checked explicitly and the raw body is shown when it is wrong.
  * **The response reports its own vintage** in `data.year` and `data.quarter`. That is the
    discovery mechanism required by CLAUDE.md rule 3 -- no quarter is ever hardcoded, and
    the vintage that served a run is recorded in the manifest. History reaches back to
    2021Q1; earlier quarters answer 404.
  * **`res_ratio` sums to exactly zero for 3,571 ZIPs**, nine percent of them: PO-box-only
    and business-only ZIPs with no residential addresses at all. They are real ZIPs with
    real complaints behind them -- 67 Michigan CFPB complaints sit in one -- and a rule that
    weights by residential share alone sends every one of them nowhere. `tot_ratio` covers
    all of them. The counts are recorded in the manifest so the model that allocates cannot
    discover this by losing rows.
  * **Three state codes are not US states or territories.** `FM`, `MH` and `PW` -- Micronesia,
    the Marshall Islands and Palau -- are sovereign nations with US postal service, and eight
    rows carry a two-character `geoid` with no county component. All of it is real, kept, and
    counted as nonstandard rather than dropped.

Requires `HUD_API_TOKEN` in `.env`; register at huduser.gov and select the USPS Crosswalk
dataset. An unauthenticated call answers a clean `401 {"error": "Unauthenticated"}`, so a
missing token fails loudly rather than looking like a parse error the way Census's does.

Note that huduser.gov's *portal* pages answer non-browser clients with HTTP 202 and an empty
body. Discovery therefore comes from the API's own response and never from scraping the
publication page, which is what the CFPB fetcher does for its publisher.

Run:  python -m ingest.shared.hud_crosswalk [--mode live|fixture] [--force]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import httpx
from ingest.common import (
    REPO_ROOT,
    DiscoveryError,
    base_manifest,
    http_client,
    human_bytes,
    load_state_codes,
    load_table,
    make_logger,
    nonstandard_partitions,
    partition_counts,
    paths_for,
    raise_for_transient,
    read_existing_manifest,
    read_secret,
    repartition_to_raw,
    retrying,
    skip_as_current,
    sql_literal,
    write_baseline,
    write_json,
)

TRACK = "shared"
SOURCE = "zip_county_crosswalk"
TABLE = "raw.ref_zip_county_crosswalk"
PARTITION_COLUMN = "state"

API_BASE = "https://www.huduser.gov/hudapi/public/usps"
# HUD numbers its six crosswalks; type 2 is ZIP -> county. The other five map ZIP to tract
# or CBSA, or run the mapping the other way, and none of them answers FIN-E1.
CROSSWALK_TYPE = "2"
EXPECTED_CROSSWALK_NAME = "zip-county"
NATIONAL_QUERY = "All"
# Michigan is the verification slice because it is the one this repo has to be right about.
SPOT_CHECK_STATE = "MI"

LANDING_FILE = "zip-county-all.json"
LANDING_GLOB = LANDING_FILE

# The row fields HUD returns, in its own order. Declared rather than auto-detected: the
# reader below forces every one of them to VARCHAR, and a publisher that adds or renames a
# field should show up as a change here rather than as a silently different table.
ROW_FIELDS = ("zip", "geoid", "city", "state", "res_ratio", "bus_ratio", "oth_ratio", "tot_ratio")
# The weight a complaint-allocation model should use, and its fallback. Recorded in the
# manifest rather than left to the model to assert about itself.
PRIMARY_WEIGHT = "res_ratio"
FALLBACK_WEIGHT = "tot_ratio"

DOCUMENTATION_URL = "https://www.huduser.gov/portal/dataset/uspszip-api.html"
TERMS_URL = "https://www.huduser.gov/portal/dataset/api-terms-of-service.html"

MICHIGAN_STATE_FIPS = "26"
MICHIGAN_COUNTY_COUNT = 83

log = make_logger(TRACK, SOURCE)


class MissingToken(DiscoveryError):
    """No HUD_API_TOKEN. Its own type so the message can carry the signup instructions."""


# ── pure helpers (unit-tested without a network) ───────────────────────────────


def require_payload(body: Any, *, context: str) -> dict[str, Any]:
    """Unwrap `{"data": {...}}`, or stop and show what came back instead.

    HUD returns errors as a JSON *list* -- `[{"error": "..."}]` -- so the usual
    `body.get("data")` raises AttributeError somewhere unhelpful. CLAUDE.md section 8: stop
    and show the raw response rather than coding around a publisher surprise.
    """
    if isinstance(body, list):
        raise DiscoveryError(
            f"{context}: HUD returned an error envelope rather than data: {json.dumps(body)[:300]}"
        )
    if not isinstance(body, dict) or not isinstance(body.get("data"), dict):
        raise DiscoveryError(
            f"{context}: expected an object with a 'data' object; got "
            f"{type(body).__name__} {json.dumps(body)[:300]}"
        )
    data = body["data"]
    if data.get("crosswalk_type") != EXPECTED_CROSSWALK_NAME:
        raise DiscoveryError(
            f"{context}: asked for type={CROSSWALK_TYPE} and expected "
            f"{EXPECTED_CROSSWALK_NAME!r}, but HUD reports "
            f"{data.get('crosswalk_type')!r}. The crosswalk numbering may have changed."
        )
    if not isinstance(data.get("results"), list):
        raise DiscoveryError(f"{context}: 'data.results' is not a list: {json.dumps(data)[:300]}")
    return data


def vintage_of(data: dict[str, Any], *, context: str) -> str:
    """`2026Q1`, taken from the publisher's own answer. Never a hardcoded quarter."""
    year, quarter = data.get("year"), data.get("quarter")
    if year is None or quarter is None:
        raise DiscoveryError(
            f"{context}: HUD did not report its own vintage. Without `year` and `quarter` "
            f"there is no way to record which crosswalk served this run. Got: "
            f"{json.dumps({k: v for k, v in data.items() if k != 'results'})[:300]}"
        )
    return f"{year}Q{quarter}"


def crosswalk_relation(landing: Path) -> str:
    """Read the landed response, all-varchar, preserving HUD's own number formatting.

    Two things this idiom is chosen for. `->>` pulls each field out as text exactly as the
    publisher wrote it, so `0.9285286313446889` stays that and a ratio of `1` does not
    become `1.0` -- which is what the all-varchar rule is protecting and what L1's lossless
    check compares. And it is fast: declaring the envelope as a nested STRUCT and unnesting
    it instead never returned on this 7.8 MB file, while this reads all 54,562 rows in
    under a second.
    """
    projection = ", ".join(f"j->>'$.{field}' AS {field}" for field in ROW_FIELDS)
    return (
        f"(SELECT {projection} FROM (SELECT unnest(CAST(data->'$.results' AS JSON[])) AS j "
        f"FROM read_json({sql_literal(landing)}, columns={{'data': 'JSON'}}, "
        f"maximum_object_size=200000000)))"
    )


def state_coverage_failures(present: set[str], known: frozenset[str]) -> list[str]:
    """Every state and territory the reference file names must be in a national pull."""
    if missing := sorted(known - present):
        return [
            f"{len(missing)} of the {len(known)} codes in state_codes.csv are absent from a "
            f"query={NATIONAL_QUERY} response: {missing}. That is not a national crosswalk."
        ]
    return []


def spot_check_failures(national_rows: int, spot_rows: int) -> list[str]:
    """HUD publishes no row count, so the publisher checks itself.

    Asking for one state on its own and comparing that count to the same state's slice of
    the national pull is the only reconciliation available here -- the same shape as the
    NFIP per-state API spot counts, and the reason an empty-page check alone is never
    enough (CLAUDE.md rule 6).
    """
    if national_rows == spot_rows:
        return []
    return [
        f"{SPOT_CHECK_STATE} has {national_rows:,} rows in the national pull but HUD returns "
        f"{spot_rows:,} when asked for {SPOT_CHECK_STATE} alone (gap "
        f"{national_rows - spot_rows:+,}). The national response is not complete."
    ]


def michigan_failures(counties: int) -> list[str]:
    """This source is a denominator, so a missing county silently shrinks every rate."""
    if counties == MICHIGAN_COUNTY_COUNT:
        return []
    return [
        f"{counties} of {MICHIGAN_COUNTY_COUNT} Michigan counties appear in the crosswalk. "
        "Every county-grain complaint rate built on it would be wrong."
    ]


def manifest_payload(
    *,
    landing_files: list[dict[str, Any]],
    resolved_url: str,
    retrieved_at: str,
    vintage: str,
    rows_landing: int,
    rows_raw: int,
    rows_duckdb: int,
    partition_rows: dict[str, int],
    nonstandard: dict[str, int],
    coverage: dict[str, Any],
    weights: dict[str, Any],
    spot_check: dict[str, Any],
) -> dict[str, Any]:
    return {
        **base_manifest(
            track=TRACK,
            source=SOURCE,
            publisher="HUD Office of Policy Development and Research",
            dataset_name="HUD-USPS ZIP Code Crosswalk (ZIP to county)",
            resolved_url=resolved_url,
            distribution_format="json",
            retrieved_at=retrieved_at,
            landing_files=landing_files,
            # HUD publishes no row count for a query response; the spot check below is the
            # reconciliation instead.
            source_reported_count=None,
            source_last_refresh=vintage,
            duckdb_table=TABLE,
            rows_landing=rows_landing,
            rows_raw=rows_raw,
            rows_duckdb=rows_duckdb,
            partition_column=PARTITION_COLUMN,
            partition_rows=partition_rows,
            nonstandard_partitions=nonstandard,
        ),
        "discovered_from": API_BASE,
        "documentation_url": DOCUMENTATION_URL,
        "terms_url": TERMS_URL,
        "vintage": vintage,
        "vintage_note": (
            "HUD reports its own year and quarter in every response, and that is what is "
            "recorded here. No quarter is hardcoded anywhere in the fetcher. The crosswalk "
            "is rebuilt quarterly and moves slightly each time, so a figure allocated with "
            "one vintage is not identical to the same figure allocated with the next."
        ),
        "source_count_unavailable_reason": (
            "The API answers a query rather than serving a file, and reports no row count. "
            f"Completeness rests on the {SPOT_CHECK_STATE} spot check recorded below and on "
            "every state code in the reference file being present."
        ),
        "coverage": coverage,
        "weights": weights,
        "spot_check": spot_check,
        "api_key": {
            "note": (
                "Requires a bearer token in HUD_API_TOKEN. The token is account-scoped, "
                "never written to this manifest or to a log line, and .env is gitignored."
            ),
            "signup": "https://www.huduser.gov/hudapi/public/login",
            "dataset_to_select": "USPS Crosswalk",
        },
        "coverage_caveats": [
            "A ZIP is a postal delivery route, not an area, and ZIPs cross county lines. Any "
            "ZIP-to-county figure is an allocation under a stated rule, never a lookup.",
            f"`{PRIMARY_WEIGHT}` is zero for ZIPs with no residential addresses -- PO-box and "
            f"business-only ZIPs. `{FALLBACK_WEIGHT}` covers every one of them, and a rule "
            "that weights by residential share alone silently drops their rows.",
            "FM, MH and PW are Micronesia, the Marshall Islands and Palau: sovereign nations "
            "with US postal service, not US territories. They are kept and counted as "
            "nonstandard partition keys.",
            "A few geoids are two characters rather than five -- territory codes with no "
            "county component. Kept as published.",
            "HUD serves vintages back to 2021Q1 only; earlier quarters answer HTTP 404.",
        ],
    }


# ── network ───────────────────────────────────────────────────────────────────


def bearer_token() -> str:
    token = read_secret("HUD_API_TOKEN")
    if not token:
        raise MissingToken(
            "HUD_API_TOKEN is not set. The ZIP-to-county crosswalk needs a free huduser.gov "
            "bearer token: register at https://www.huduser.gov/hudapi/public/login, select "
            "the USPS Crosswalk dataset, click Create New Token, and put it in `.env` as "
            "HUD_API_TOKEN=... (.env is gitignored; see .env.example)."
        )
    return token


@retrying
def fetch_crosswalk(client: httpx.Client, query: str) -> tuple[dict[str, Any], bytes, str]:
    """One crosswalk query. Returns the unwrapped payload, the verbatim body, and the URL."""
    response = client.get(API_BASE, params={"type": CROSSWALK_TYPE, "query": query})
    raise_for_transient(response)
    data = require_payload(response.json(), context=f"query={query}")
    return data, response.content, str(response.request.url)


# ── orchestration ─────────────────────────────────────────────────────────────


def summarise(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """Everything the manifest reports about the landed table, in one pass each."""
    row = con.execute(
        f"""
        SELECT count(*),
               count(DISTINCT zip),
               count(DISTINCT geoid),
               count(*) FILTER (WHERE length(geoid) <> 5),
               count(DISTINCT geoid) FILTER (WHERE geoid LIKE '{MICHIGAN_STATE_FIPS}%'),
               count(*) FILTER (WHERE state = '{SPOT_CHECK_STATE}')
        FROM {TABLE}
        """
    ).fetchone()
    zero_primary = con.execute(
        f"SELECT count(*) FROM (SELECT zip FROM {TABLE} GROUP BY zip "
        f"HAVING sum(CAST({PRIMARY_WEIGHT} AS DOUBLE)) = 0)"
    ).fetchone()[0]
    zero_both = con.execute(
        f"SELECT count(*) FROM (SELECT zip FROM {TABLE} GROUP BY zip "
        f"HAVING sum(CAST({PRIMARY_WEIGHT} AS DOUBLE)) = 0 "
        f"AND sum(CAST({FALLBACK_WEIGHT} AS DOUBLE)) = 0)"
    ).fetchone()[0]
    return {
        "rows": int(row[0]),
        "zips": int(row[1]),
        "counties": int(row[2]),
        "short_geoids": int(row[3]),
        "michigan_counties": int(row[4]),
        "michigan_rows": int(row[5]),
        "zips_without_residential_addresses": int(zero_primary),
        "zips_without_any_weight": int(zero_both),
    }


def run(mode: str, force: bool = False) -> int:
    data_root, db_path = paths_for(mode)
    landing_dir = data_root / "landing" / TRACK / SOURCE
    raw_dir = data_root / "raw" / TRACK / SOURCE
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if mode == "fixture":
        fixtures = REPO_ROOT / "tests" / "fixtures" / TRACK
        landing = fixtures / f"{SOURCE}.json"
        metadata_path = fixtures / f"{SOURCE}.metadata.json"
        if not landing.exists() or not metadata_path.exists():
            log(f"FAIL: fixture missing at {landing} or {metadata_path}")
            return 1
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        vintage = metadata["vintage"]
        resolved_url = "fixture://" + landing.name
        landing_bytes = landing.stat().st_size
        spot_rows = metadata.get("spot_check_rows")
        log("fixture mode: HUD API call SKIPPED (offline)")
    else:
        with http_client() as client:
            client.headers["Authorization"] = f"Bearer {bearer_token()}"

            # The spot check doubles as the refresh probe: one small call names the vintage
            # for a few hundred KB, so an unchanged quarter costs that instead of 7.8 MB.
            log(f"asking HUD for {SPOT_CHECK_STATE} -- vintage probe and completeness check")
            spot_data, _, _ = fetch_crosswalk(client, SPOT_CHECK_STATE)
            vintage = vintage_of(spot_data, context=f"query={SPOT_CHECK_STATE}")
            spot_rows = len(spot_data["results"])
            log(f"  HUD reports vintage {vintage}; {spot_rows:,} {SPOT_CHECK_STATE} rows")

            existing = read_existing_manifest(raw_dir)
            if skip_as_current(
                force=force,
                publisher_unchanged=bool(existing) and existing.get("vintage") == vintage,
                db_path=db_path,
                tables=(TABLE,),
                log=log,
            ):
                log(f"SKIPPED (current): raw already holds the {vintage} crosswalk")
                return 0

            log(f"downloading the national crosswalk (query={NATIONAL_QUERY})")
            national, body, resolved_url = fetch_crosswalk(client, NATIONAL_QUERY)
            national_vintage = vintage_of(national, context=f"query={NATIONAL_QUERY}")
            if national_vintage != vintage:
                # Two calls seconds apart landing either side of a quarterly republication.
                # Rare, but it would mix vintages in one file, so stop rather than guess.
                raise DiscoveryError(
                    f"HUD served {vintage} for {SPOT_CHECK_STATE} but {national_vintage} for "
                    f"query={NATIONAL_QUERY}. Re-run: the crosswalk was republished mid-fetch."
                )

            landing_dir.mkdir(parents=True, exist_ok=True)
            landing = landing_dir / LANDING_FILE
            part = landing.with_name(landing.name + ".part")
            part.write_bytes(body)
            part.replace(landing)
            landing_bytes = len(body)
            log(f"  landed {human_bytes(landing_bytes)}, {len(national['results']):,} rows")

            write_baseline(
                TRACK,
                SOURCE,
                {
                    "note": (
                        "HUD documents this API on a portal page rather than as "
                        "machine-readable field metadata, and that page answers non-browser "
                        "clients with HTTP 202 and an empty body. This is a pointer file, "
                        "per CLAUDE.md rule 8, plus the field names the API actually returned."
                    ),
                    "endpoint": API_BASE,
                    "crosswalk_type": CROSSWALK_TYPE,
                    "crosswalk_name": EXPECTED_CROSSWALK_NAME,
                    "documentation_url": DOCUMENTATION_URL,
                    "terms_url": TERMS_URL,
                    "row_fields": list(ROW_FIELDS),
                    "response_fields": sorted(k for k in national if k != "results"),
                    "retrieved_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                },
                kind="pointer",
            )

    retrieved_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    con = duckdb.connect(str(db_path))
    try:
        log(f"converting -> {raw_dir}")
        rows_landing, rows_raw = repartition_to_raw(
            con,
            relation=crosswalk_relation(landing),
            raw_dir=raw_dir,
            partition_column=PARTITION_COLUMN,
        )
        partition_rows = partition_counts(con, raw_dir, PARTITION_COLUMN)
        rows_duckdb = load_table(con, TABLE, raw_dir)
        stats = summarise(con)
    finally:
        con.close()

    known = load_state_codes()
    nonstandard = nonstandard_partitions(partition_rows, known)
    problems = state_coverage_failures(set(partition_rows), known) + michigan_failures(
        stats["michigan_counties"]
    )
    if spot_rows is not None:
        problems += spot_check_failures(stats["michigan_rows"], spot_rows)
    for problem in problems:
        log(f"FAIL  {problem}")
    if problems:
        return 1

    write_json(
        raw_dir / "manifest.json",
        manifest_payload(
            landing_files=[{"name": Path(landing).name, "bytes": landing_bytes}],
            resolved_url=resolved_url,
            retrieved_at=retrieved_at,
            vintage=vintage,
            rows_landing=rows_landing,
            rows_raw=rows_raw,
            rows_duckdb=rows_duckdb,
            partition_rows=partition_rows,
            nonstandard=nonstandard,
            coverage={
                "states": len(partition_rows),
                "zips": stats["zips"],
                "counties": stats["counties"],
                "michigan_counties": stats["michigan_counties"],
                "michigan_rows": stats["michigan_rows"],
                "geoids_shorter_than_five": stats["short_geoids"],
            },
            weights={
                "primary": PRIMARY_WEIGHT,
                "fallback": FALLBACK_WEIGHT,
                "why": (
                    "A consumer complaint is filed by a household, so the residential "
                    "address share is the right weight for allocating one across the "
                    "counties a ZIP touches. Business and 'other' shares are landed too "
                    "because a different question could want them."
                ),
                "fallback_why": (
                    f"{stats['zips_without_residential_addresses']:,} ZIPs have no "
                    f"residential addresses at all, so {PRIMARY_WEIGHT} sums to zero across "
                    f"their counties and allocating by it alone would drop every row behind "
                    f"them. {FALLBACK_WEIGHT} is every address type and covers them."
                ),
                "zips_without_residential_addresses": stats["zips_without_residential_addresses"],
                "zips_without_any_weight": stats["zips_without_any_weight"],
                "landed_ratios": ["res_ratio", "bus_ratio", "oth_ratio", "tot_ratio"],
                "typed_in": "session 2 staging; every ratio is text in raw per CLAUDE.md rule 2",
            },
            spot_check={
                "state": SPOT_CHECK_STATE,
                "rows_in_national_pull": stats["michigan_rows"],
                "rows_when_asked_alone": spot_rows,
                "agrees": spot_rows is None or stats["michigan_rows"] == spot_rows,
                "why": (
                    "HUD reports no row count, so the publisher is asked the same question "
                    "twice and the answers are compared -- the shape the NFIP claims loader "
                    "uses against FEMA's API."
                ),
            },
        ),
    )

    log(f"landing {rows_landing:,} -> raw {rows_raw:,} -> {TABLE} {rows_duckdb:,}")
    log(
        f"vintage {vintage}: {stats['zips']:,} ZIPs across {stats['counties']:,} counties in "
        f"{len(partition_rows)} states"
    )
    log(
        f"Michigan: {stats['michigan_rows']:,} rows, all {stats['michigan_counties']} counties "
        f"present -- usable as a county denominator"
    )
    log(
        f"{stats['zips_without_residential_addresses']:,} ZIPs have no residential addresses; "
        f"{FALLBACK_WEIGHT} covers all but {stats['zips_without_any_weight']}"
    )
    if nonstandard:
        log(
            "nonstandard state keys kept: "
            + ", ".join(f"{k}={v:,}" for k, v in sorted(nonstandard.items()))
        )
    log("done")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("live", "fixture"),
        default=os.environ.get("DATA_MODE", "live"),
        help="fixture mode never touches the network (default: $DATA_MODE or live)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="re-download even when HUD reports the vintage already in raw",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return run(args.mode, force=args.force)
    except DiscoveryError as error:
        log(f"FAIL (discovery): {error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
