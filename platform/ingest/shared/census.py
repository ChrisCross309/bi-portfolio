"""Ingest Census ACS 5-year denominators into DuckDB's raw schema.

Domain-neutral reference, and the one every per-capita measure in the repo rests on:
FIN-E1 divides Michigan complaints by population, INS-E4 divides policies by housing units,
HLT-E5 asks whether need is growing faster than the 65-and-over population. Three variables
serve those three questions and nothing else is pulled.

National scope. Every US county lands, not just Michigan -- the whole point of a denominator
table is that the Michigan numbers have a benchmark, and CLAUDE.md section 4 forbids
filtering a national source in raw.

Two datasets land, because the publisher splits them and joining them here would be a
content change raw is not allowed to make:

  * `acs/acs5`         -> `raw.ref_acs5_detailed`  total population and housing units
  * `acs/acs5/subject` -> `raw.ref_acs5_subject`   the 65-and-over total

The 65+ figure is taken as the publisher's own `S0101_C01_030E` rather than assembled from
the twelve `B01001` age components. Summing components is aggregation, and raw does not
aggregate.

Five things about this API that the code is shaped around:

  * It requires a key now, and says so with HTTP 200. An unkeyed request returns an HTML
    page titled "Missing Key" with a 200 status, so nothing in the transport layer notices
    and `response.json()` fails with a decode error three frames away. The response is
    checked for JSON explicitly. The key is read from the environment or a gitignored
    `.env`, and is redacted from every log line and every manifest field -- the API accepts
    it only as a query parameter, so the URL is never recorded as sent.
  * The response is an array of arrays, not records. Row one is the header. The conversion
    projects by position, which is only safe because every landed file's header is checked
    against the columns we asked for before any of it is read as data.
  * Vintages overlap by four years. The 2020 5-year estimate covers 2016-2020 and the 2021
    covers 2017-2021, sharing four years of sample, so consecutive vintages are *not* a time
    series and the Census Bureau says not to compare them. Only every fifth vintage is
    independent. All 15 land, and the manifest says this in the caveats.
  * Vintage coverage is the intersection of two datasets, discovered rather than assumed.
    The detailed tables go back to 2009 but the subject tables start at 2010, so 2010-2024
    is what both can answer for.
  * The geography roster is not stable, in two different ways. Connecticut replaced its
    eight counties with nine planning regions in the 2022 vintage, so county FIPS sets
    differ across vintages and a county-level series spanning 2021-2022 breaks for that
    state. And the 2011 subject tables -- alone among all thirty vintage-dataset pairs --
    enumerate the four island areas at state and county level with every estimate null: 36
    rows that are geographies without data. Both are kept, counted and named. Michigan's 83
    counties are asserted present in every vintage, which is the roster this repo depends on.

Neither dataset publishes a row count, so completeness rests on structural checks instead:
a floor on rows per geography rather than an exact count (the roster moves), state FIPS
validated against the shared state reference with the 50 states and DC required, the
Michigan county roster, and an unbroken vintage range.

Run:  python -m ingest.shared.census [--mode live|fixture]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import httpx
from ingest.common import (
    PAGE_SLEEP_SECONDS,
    REPO_ROOT,
    DiscoveryError,
    base_manifest,
    http_client,
    load_state_fips,
    load_table,
    make_logger,
    partition_counts,
    paths_for,
    raise_for_transient,
    read_existing_manifest,
    read_secret,
    repartition_to_raw,
    retrying,
    sha256_of,
    skip_as_current,
    sql_literal,
    write_baseline,
    write_json,
)

TRACK = "shared"
API_ROOT = "https://api.census.gov/data"
CATALOG = f"{API_ROOT}.json"
KEY_NAME = "CENSUS_API_KEY"
REDACTED = "<redacted>"

# The two datasets, their variables, and where each lands. NAME comes first in every
# request, so the positional projection below counts from it.
DATASETS: dict[str, dict[str, Any]] = {
    "detailed": {
        "path": "acs/acs5",
        "c_dataset": ["acs", "acs5"],
        "source": "acs5_detailed",
        "table": "raw.ref_acs5_detailed",
        "variables": ("B01003_001E", "B01003_001M", "B25001_001E", "B25001_001M"),
        "purpose": "total population (B01003) and total housing units (B25001), with margins",
    },
    "subject": {
        "path": "acs/acs5/subject",
        "c_dataset": ["acs", "acs5", "subject"],
        "source": "acs5_subject",
        "table": "raw.ref_acs5_subject",
        "variables": ("S0101_C01_030E", "S0101_C01_030M"),
        "purpose": "population aged 65 and over (S0101), the publisher's own total",
    },
}

# Geography levels, the query that asks for each, and how many rows each should return.
# The API appends its own key columns to the response, which is why the expected header
# differs per level.
GEOGRAPHIES: dict[str, dict[str, Any]] = {
    "us": {"query": {"for": "us:*"}, "keys": ("us",), "minimum_rows": 1},
    "state": {"query": {"for": "state:*"}, "keys": ("state",), "minimum_rows": 51},
    "county": {"query": {"for": "county:*"}, "keys": ("state", "county"), "minimum_rows": 3_000},
}

# The states and DC that must appear in every vintage. The five territories are a different
# matter: Puerto Rico is in every vintage, and the four island areas appear in exactly one
# (the 2011 subject tables) with null estimates -- see `territory_note` in the manifest.
CORE_STATE_COUNT = 51

PARTITION_COLUMN = "vintage"
MICHIGAN_STATE_FIPS = "26"
MICHIGAN_COUNTY_COUNT = 83

# ACS annotates unavailable or controlled values with large negative integers rather than
# nulls -- -555555555 means the estimate is controlled so no margin of error applies.
SENTINEL_PREFIX = "-5555"

FIELD_REFERENCE = "https://api.census.gov/data/2024/acs/acs5/variables.html"
KEY_SIGNUP = "https://api.census.gov/data/key_signup.html"

log = make_logger(TRACK, "census")


# ── pure helpers (unit-tested without a network) ───────────────────────────────


def redact(text: str, key: str | None) -> str:
    """Never let the key reach a log line, a manifest or an exception message."""
    if not key:
        return text
    return text.replace(key, REDACTED)


def expected_header(variables: tuple[str, ...], geography: str) -> list[str]:
    """What the API should echo back: NAME, the variables we asked for, then its own keys."""
    return ["NAME", *variables, *GEOGRAPHIES[geography]["keys"]]


def header_failures(observed: list[str], variables: tuple[str, ...], geography: str) -> list[str]:
    """The conversion reads by position, so a reordered header would mislabel every column.

    This is the check that makes positional extraction safe rather than optimistic.
    """
    expected = expected_header(variables, geography)
    if observed == expected:
        return []
    return [
        f"{geography}: header {observed} does not match the requested {expected}. "
        "Columns are read by position, so this would silently mislabel the data."
    ]


def row_count_failures(geography: str, rows: int, mode: str = "live") -> list[str]:
    """A geography that comes back short is a truncated pull, not a small country.

    A floor rather than an exact count, because the roster genuinely moves: Connecticut's
    counties became planning regions in 2022, and one vintage lists the island areas.

    Skipped against a fixture. "Did every county arrive?" is only answerable about the whole
    dataset, and a committed sample is a sample by construction -- padding one until it
    passed would prove nothing about production. The Michigan roster check still runs,
    because the fixture carries all 83 counties on purpose.
    """
    if mode == "fixture":
        return []
    minimum = GEOGRAPHIES[geography]["minimum_rows"]
    if rows < minimum:
        return [f"{geography}: {rows:,} rows, expected at least {minimum:,}"]
    return []


def state_failures(fips_codes: list[str], known_fips: dict[str, str]) -> list[str]:
    """Validate state rows against the shared state reference rather than against a count.

    Two different things would otherwise hide behind one number: a truncated pull, and a
    vintage that legitimately carries more state-equivalents than its neighbours.
    """
    unknown = sorted(code for code in set(fips_codes) if code not in known_fips)
    problems = []
    if unknown:
        problems.append(
            f"state rows carry FIPS codes the state reference does not recognise: {unknown}"
        )
    core = {code for code in fips_codes if code in known_fips and int(code) <= 56}
    if len(core) < CORE_STATE_COUNT:
        missing = sorted(
            f"{code} ({known_fips[code]})"
            for code in known_fips
            if int(code) <= 56 and code not in core
        )
        problems.append(f"only {len(core)} of the 50 states and DC are present; missing {missing}")
    return problems


def vintage_failures(vintages: tuple[int, ...]) -> list[str]:
    """The usable range is where both datasets overlap, and it must be unbroken."""
    if not vintages:
        return ["no vintage is served by both the detailed and subject datasets"]
    expected = set(range(min(vintages), max(vintages) + 1))
    if gaps := sorted(expected - set(vintages)):
        return [f"vintages {gaps} are missing from the overlap of the two datasets"]
    return []


def michigan_failures(counts: dict[str, int]) -> list[str]:
    """Michigan's 83 counties are the roster this repo's county grain depends on."""
    problems = []
    for vintage, count in sorted(counts.items()):
        if count != MICHIGAN_COUNTY_COUNT:
            problems.append(
                f"vintage {vintage}: {count} Michigan counties, expected "
                f"{MICHIGAN_COUNTY_COUNT} -- every per-capita denominator would be wrong"
            )
    return problems


def landing_name(dataset: str, vintage: int, geography: str) -> str:
    """Vintage and geography live in the filename because the response contains neither."""
    return f"acs5-{dataset}-{vintage}-{geography}.json"


def geography_relation(
    landing_glob: Path, variables: tuple[str, ...], geography: str, dataset: str
) -> str:
    """Project one geography's landed files into the shared column layout.

    Positional, because the payload is an array of arrays; safe, because every file's header
    was verified first. Row one of each file is that header and is dropped here.

    `vintage` and `geo_level` are added from the filename, and they are the only columns not
    published by the API. Without them a union of forty-five files would be indistinguishable
    rubble -- the response says nothing about which vintage or geography produced it.
    """
    offset = len(variables) + 1  # NAME occupies position 1
    keys = GEOGRAPHIES[geography]["keys"]
    key_expressions = {
        "state": "NULL",
        "county": "NULL",
        "us": "NULL",
    }
    for index, key in enumerate(keys, start=1):
        key_expressions[key] = f"row[{offset + index}]"

    projections = ['row[1] AS "NAME"']
    projections += [f'row[{index + 2}] AS "{name}"' for index, name in enumerate(variables)]
    projections += [f'{key_expressions[key]} AS "{key}"' for key in ("state", "county", "us")]
    projections += [
        "regexp_extract(filename, '-(\\d{4})-', 1) AS vintage",
        f"'{geography}' AS geo_level",
    ]

    return (
        f"SELECT {', '.join(projections)} FROM read_json({sql_literal(landing_glob)}, "
        "format='array', columns={'row': 'VARCHAR[]'}, filename=true) "
        "WHERE row[1] <> 'NAME'"
    )


def dataset_relation(landing_dir: Path, dataset: str, variables: tuple[str, ...]) -> str:
    """One relation per dataset: the three geographies, unioned into a single layout.

    The geographies genuinely differ in shape -- the API returns `us`, or `state`, or
    `state` and `county` -- so the union pads with NULLs. A NULL `county` on a state row is
    a true statement about that row, not a missing value.
    """
    parts = [
        geography_relation(
            landing_dir / f"acs5-{dataset}-*-{geography}.json", variables, geography, dataset
        )
        for geography in GEOGRAPHIES
    ]
    return "(" + " UNION ALL ".join(parts) + ")"


def manifest_payload(
    *,
    dataset: str,
    catalog_entries: dict[int, dict[str, Any]],
    vintages: tuple[int, ...],
    landing_files: list[dict[str, Any]],
    rows_landing: int,
    rows_raw: int,
    rows_duckdb: int,
    partition_rows: dict[str, int],
    rows_by_geography: dict[str, int],
    michigan_counties: dict[str, int],
    valueless_rows: dict[str, int],
    retrieved_at: str,
) -> dict[str, Any]:
    spec = DATASETS[dataset]
    return {
        **base_manifest(
            track=TRACK,
            source=spec["source"],
            publisher="US Census Bureau",
            dataset_name=f"ACS 5-Year Estimates ({spec['path']})",
            # Recorded without the key, which the API only accepts as a query parameter.
            resolved_url=f"{API_ROOT}/<vintage>/{spec['path']}",
            distribution_format="json",
            retrieved_at=retrieved_at,
            landing_files=landing_files,
            # The API reports no row count for a query; structural checks stand in.
            source_reported_count=None,
            source_last_refresh=catalog_entries[max(vintages)].get("modified"),
            duckdb_table=spec["table"],
            rows_landing=rows_landing,
            rows_raw=rows_raw,
            rows_duckdb=rows_duckdb,
            partition_column=PARTITION_COLUMN,
            partition_rows=partition_rows,
            # Every partition key is a vintage we requested, or the run already failed.
            nonstandard_partitions={},
        ),
        "discovered_from": CATALOG,
        "variables": {
            "requested": list(spec["variables"]),
            "purpose": spec["purpose"],
        },
        "source_count_unavailable_reason": (
            "The ACS API answers a query, not a file, and reports no row count. "
            "Completeness rests on the expected row count per geography, the Michigan "
            "county roster, and an unbroken vintage range."
        ),
        "api_key": {
            "required": True,
            "read_from": f"${KEY_NAME} or a gitignored .env",
            "signup": KEY_SIGNUP,
            "note": (
                "api.census.gov rejects unkeyed requests with an HTML page titled "
                "'Missing Key' and an HTTP 200 status, so the response is checked for JSON "
                "rather than trusted. The key is redacted from every recorded URL."
            ),
        },
        "vintages": {
            "landed": list(vintages),
            "overlap_warning": (
                "Consecutive ACS 5-year vintages share four years of sample -- 2020 covers "
                "2016-2020 and 2021 covers 2017-2021 -- so they are not a time series and "
                "the Census Bureau advises against comparing them. Only every fifth "
                "vintage is independent. Use one vintage as a denominator for its own "
                "window; do not plot consecutive vintages as a trend."
            ),
            "why_this_range": (
                "The intersection of the two datasets' published vintages: the detailed "
                "tables reach back to 2009 but the subject tables begin at 2010."
            ),
            "catalog_modified": {
                str(vintage): catalog_entries[vintage].get("modified") for vintage in vintages
            },
        },
        "geography": {
            "rows": rows_by_geography,
            "levels_are_peers": (
                "us, state and county rows sit in one table, distinguished by `geo_level`. "
                "Summing across it counts the country three times."
            ),
            "added_columns": (
                "`vintage` and `geo_level` are ours, taken from the request and recorded in "
                "the landing filename. The API response contains neither, and without them "
                "the landed files are indistinguishable from one another."
            ),
            "michigan_counties_by_vintage": michigan_counties,
            "rows_without_estimates": {
                "by_vintage_and_level": valueless_rows,
                "total": sum(valueless_rows.values()),
                "note": (
                    "Geographies the API enumerates without publishing any estimate. The "
                    "2011 subject vintage is the only one that lists the four island areas "
                    "-- American Samoa, Guam, the Northern Marianas and the US Virgin "
                    "Islands -- and every value for them is null. The rows stay in raw; a "
                    "denominator that joins to them divides by nothing."
                ),
            },
            "county_roster_warning": (
                "County FIPS sets are not stable across vintages: Connecticut replaced its "
                "eight counties with nine planning regions in the 2022 vintage. Michigan is "
                "unaffected and its 83 counties are asserted present in every vintage."
            ),
        },
        "coverage_caveats": [
            "These are survey estimates with margins of error, not counts. Every estimate "
            "column has an `M` twin and per-capita rates built from them inherit that "
            "uncertainty.",
            "ACS annotates rather than nulls: a margin of error of -555555555 means the "
            "estimate is controlled so no margin applies. Large negative values in these "
            "columns are annotations, not measurements, and raw keeps them verbatim.",
            "5-year estimates are the only ACS product covering every county; the 1-year "
            "product excludes areas under 65,000 population, which is most of Michigan.",
            "A 5-year estimate describes its whole window, not its final year. Labelling a "
            "2024 vintage as '2024 population' overstates what it measures.",
        ],
    }


# ── network ───────────────────────────────────────────────────────────────────


def require_key() -> str:
    """Fail with instructions rather than with a JSON decode error 200 lines later."""
    key = read_secret(KEY_NAME)
    if not key:
        raise DiscoveryError(
            f"api.census.gov now requires an API key and rejects unkeyed requests with an "
            f"HTML page and an HTTP 200 status. Set {KEY_NAME} in the environment or in a "
            f"gitignored .env file. Free signup: {KEY_SIGNUP}"
        )
    return key


@retrying
def fetch_catalog(client: httpx.Client) -> list[dict[str, Any]]:
    """The API's own dataset catalogue, which needs no key."""
    response = client.get(CATALOG)
    raise_for_transient(response)
    datasets = response.json().get("dataset")
    if not datasets:
        raise DiscoveryError(f"No `dataset` array in {CATALOG}. Raw: {response.text[:600]}")
    return datasets


def catalog_entries(datasets: list[dict[str, Any]], dataset: str) -> dict[int, dict[str, Any]]:
    """Vintage -> catalogue entry for one ACS dataset, as the publisher lists them."""
    spec = DATASETS[dataset]
    entries = {
        int(entry["c_vintage"]): entry
        for entry in datasets
        if entry.get("c_dataset") == spec["c_dataset"] and entry.get("c_vintage")
    }
    if not entries:
        raise DiscoveryError(
            f"The catalogue lists no vintages for {spec['c_dataset']}; refusing to guess one."
        )
    return entries


@retrying
def fetch_geography(
    client: httpx.Client, dataset: str, vintage: int, geography: str, key: str
) -> bytes:
    """One query. Checked for JSON, because a failure here arrives as HTTP 200 HTML."""
    spec = DATASETS[dataset]
    url = f"{API_ROOT}/{vintage}/{spec['path']}"
    response = client.get(
        url,
        params={
            "get": ",".join(("NAME", *spec["variables"])),
            **GEOGRAPHIES[geography]["query"],
            "key": key,
        },
    )
    raise_for_transient(response)
    content_type = response.headers.get("content-type") or ""
    if "json" not in content_type.lower():
        raise DiscoveryError(
            f"{url} ({geography}, vintage {vintage}) answered {content_type!r} with HTTP "
            f"{response.status_code} instead of JSON. First 300 bytes: "
            f"{redact(response.text[:300], key)}"
        )
    return response.content


# ── orchestration ─────────────────────────────────────────────────────────────


def landed_payload(path: Path) -> tuple[list[str], list[list[Any]]]:
    """Split an ACS payload into its header row and its data rows.

    Read once, in Python, before DuckDB reads any of it by position -- the header is the
    only thing standing between a positional projection and mislabelled columns.
    """
    payload = json.loads(path.read_text(encoding="utf-8"))
    # `isinstance(payload, list)` first: an error object would make payload[0] a KeyError
    # rather than the loud, explained failure this is here to raise.
    if not isinstance(payload, list) or not payload or not isinstance(payload[0], list):
        raise DiscoveryError(f"{path.name} is not an array of arrays: {str(payload)[:200]}")
    return [str(column) for column in payload[0]], payload[1:]


def run(mode: str, force: bool = False) -> int:
    data_root, db_path = paths_for(mode)
    landing_dir = data_root / "landing" / TRACK / "acs5"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    key: str | None = None

    if mode == "fixture":
        landing_dir = REPO_ROOT / "tests" / "fixtures" / TRACK / "acs5"
        if not landing_dir.exists():
            log(f"FAIL: fixture directory missing at {landing_dir}")
            return 1
        metadata = json.loads(
            (REPO_ROOT / "tests" / "fixtures" / TRACK / "acs5.metadata.json").read_text(
                encoding="utf-8"
            )
        )
        vintages = tuple(int(vintage) for vintage in metadata["vintages"])
        entries = {vintage: {"modified": metadata.get("modified")} for vintage in vintages}
        catalog = {dataset: entries for dataset in DATASETS}
        log("fixture mode: discovery and download SKIPPED (offline)")
    else:
        key = require_key()
        with http_client() as client:
            log(f"discovering ACS vintages from {CATALOG}")
            datasets = fetch_catalog(client)
            catalog = {name: catalog_entries(datasets, name) for name in DATASETS}
            vintages = tuple(sorted(set.intersection(*(set(e) for e in catalog.values()))))
            if problems := vintage_failures(vintages):
                for problem in problems:
                    log(f"FAIL  {problem}")
                return 1
            log(
                f"both datasets serve {vintages[0]}..{vintages[-1]} "
                f"({len(vintages)} vintages); the detailed tables alone reach "
                f"{min(catalog['detailed'])}"
            )

            fingerprint = {
                name: {str(v): catalog[name][v].get("modified") for v in vintages}
                for name in DATASETS
            }
            existing = read_existing_manifest(
                data_root / "raw" / TRACK / DATASETS["detailed"]["source"]
            )
            # The detailed manifest gates both datasets, so both tables have to be there.
            if skip_as_current(
                force=force,
                publisher_unchanged=bool(existing)
                and existing.get("refresh_fingerprint") == fingerprint,
                db_path=db_path,
                tables=tuple(spec["table"] for spec in DATASETS.values()),
                log=log,
            ):
                log("SKIPPED (current): the catalogue's modified dates are unchanged")
                return 0

            for name, spec in DATASETS.items():
                write_baseline(
                    TRACK,
                    spec["source"],
                    {
                        "note": "Census publishes variable metadata per vintage; this is a "
                        "pointer file naming what we request, per CLAUDE.md rule 8.",
                        "variables_requested": list(spec["variables"]),
                        "purpose": spec["purpose"],
                        "variables_reference": FIELD_REFERENCE,
                        "variables_link": catalog[name][max(vintages)].get("c_variablesLink"),
                        "catalog": CATALOG,
                        "vintages": list(vintages),
                        "retrieved_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                    },
                    kind="pointer",
                )

            landing_dir.mkdir(parents=True, exist_ok=True)
            for stale in landing_dir.glob("acs5-*.json"):
                stale.unlink()

            total_requests = len(DATASETS) * len(vintages) * len(GEOGRAPHIES)
            log(f"querying {total_requests} vintage x geography combinations")
            for name in DATASETS:
                for vintage in vintages:
                    for geography in GEOGRAPHIES:
                        payload = fetch_geography(client, name, vintage, geography, key)
                        target = landing_dir / landing_name(name, vintage, geography)
                        target.write_bytes(payload)
                        time.sleep(PAGE_SLEEP_SECONDS)
                    log(f"  {name} {vintage}: {len(GEOGRAPHIES)} geographies landed")

    # Positional extraction is only safe if every file's header is what we asked for.
    known_fips = load_state_fips()
    problems: list[str] = []
    landing_files: dict[str, list[dict[str, Any]]] = {name: [] for name in DATASETS}
    rows_by_geography: dict[str, dict[str, int]] = {name: {} for name in DATASETS}
    valueless: dict[str, dict[str, int]] = {name: {} for name in DATASETS}
    for name, spec in DATASETS.items():
        for vintage in vintages:
            for geography in GEOGRAPHIES:
                path = landing_dir / landing_name(name, vintage, geography)
                if not path.exists():
                    problems.append(f"{path.name} did not land")
                    continue
                header, rows = landed_payload(path)
                problems += header_failures(header, spec["variables"], geography)
                problems += row_count_failures(geography, len(rows), mode)
                if geography == "state":
                    problems += state_failures([str(row[-1]) for row in rows], known_fips)

                rows_by_geography[name][geography] = rows_by_geography[name].get(
                    geography, 0
                ) + len(rows)
                # Geographies the API lists without publishing any estimate for them. Real
                # rows, kept in raw, and counted here so a denominator cannot join to a
                # placeholder and quietly divide by nothing.
                blank = sum(
                    1
                    for row in rows
                    if all(value is None for value in row[1 : len(spec["variables"]) + 1])
                )
                if blank:
                    valueless[name][f"{vintage}-{geography}"] = blank
                landing_files[name].append(
                    {
                        "name": path.name,
                        "bytes": path.stat().st_size,
                        "sha256": sha256_of(path),
                    }
                )
    for problem in problems:
        log(f"FAIL  {problem}")
    if problems:
        return 1
    log(f"headers verified on {sum(len(f) for f in landing_files.values())} landed files")
    if mode == "fixture":
        log("per-geography row-count floors SKIPPED (offline) -- the fixture is a sample")
    for name, blanks in valueless.items():
        if blanks:
            log(
                f"NOTE  {name}: {sum(blanks.values())} rows are geographies listed with no "
                f"estimates at all, in {sorted(blanks)}"
            )

    retrieved_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    con = duckdb.connect(str(db_path))
    results: dict[str, dict[str, Any]] = {}
    try:
        for name, spec in DATASETS.items():
            raw_dir = data_root / "raw" / TRACK / spec["source"]
            log(f"converting {name} -> {raw_dir}")
            rows_landing, rows_raw = repartition_to_raw(
                con,
                relation=dataset_relation(landing_dir, name, spec["variables"]),
                raw_dir=raw_dir,
                partition_column=PARTITION_COLUMN,
            )
            counts = partition_counts(con, raw_dir, PARTITION_COLUMN)
            rows_duckdb = load_table(con, spec["table"], raw_dir)
            michigan = {
                str(row[0]): int(row[1])
                for row in con.execute(
                    f"SELECT {PARTITION_COLUMN}, count(DISTINCT county) "
                    f"FROM {spec['table']} WHERE geo_level = 'county' "
                    f"AND state = '{MICHIGAN_STATE_FIPS}' GROUP BY 1 ORDER BY 1"
                ).fetchall()
            }
            results[name] = {
                "raw_dir": raw_dir,
                "rows_landing": rows_landing,
                "rows_raw": rows_raw,
                "rows_duckdb": rows_duckdb,
                "counts": counts,
                "michigan": michigan,
            }
    finally:
        con.close()

    for result in results.values():
        problems += michigan_failures(result["michigan"])
    for problem in problems:
        log(f"FAIL  {problem}")
    if problems:
        return 1

    for name, spec in DATASETS.items():
        result = results[name]
        manifest = {
            **manifest_payload(
                dataset=name,
                catalog_entries=catalog[name],
                vintages=vintages,
                landing_files=landing_files[name],
                rows_landing=result["rows_landing"],
                rows_raw=result["rows_raw"],
                rows_duckdb=result["rows_duckdb"],
                partition_rows=result["counts"],
                rows_by_geography=rows_by_geography[name],
                michigan_counties=result["michigan"],
                valueless_rows=valueless[name],
                retrieved_at=retrieved_at,
            ),
            "refresh_fingerprint": {
                other: {str(v): catalog[other][v].get("modified") for v in vintages}
                for other in DATASETS
            },
        }
        write_json(result["raw_dir"] / "manifest.json", manifest)
        log(
            f"{spec['table']}: landing {result['rows_landing']:,} -> raw "
            f"{result['rows_raw']:,} -> table {result['rows_duckdb']:,}"
        )
        log(f"  geographies: {rows_by_geography[name]}")
        log(
            f"  michigan counties per vintage: "
            f"{sorted(set(result['michigan'].values()))} across {len(result['michigan'])} vintages"
        )

    log(f"vintages {vintages[0]}..{vintages[-1]} -- consecutive ones overlap, see the manifest")
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
        help="re-query even when the catalogue's modified dates are unchanged",
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
