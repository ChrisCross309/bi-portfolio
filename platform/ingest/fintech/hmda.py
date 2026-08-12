"""Ingest HMDA for the fintech track: the Michigan LAR by year, plus the filer reference.

Two sources land from one discovery pass, because the endpoint that tells us which years
exist is the same endpoint that publishes the institution list:

  * `hmda_lar`          one filtered extract per year, Michigan, ~4M rows over 2018-2025
  * `hmda_institutions` the nationwide filer list per year: LEI -> name, and a count

Raw is Michigan-scoped for the LAR, and the reason is size rather than analysis -- a year
of national LAR runs to tens of millions of rows against roughly 400k for Michigan. Per
CLAUDE.md section 4 the national benchmark comes from the publisher's own aggregate
endpoint as a reconciliation control total, not from raw. `hmda_institutions` stays
national precisely so the institution universe has no Michigan filter on it.

Five things about this API that the retrieval strategy is built around, every one of them
learned by asking it rather than by reading its documentation:

  * The aggregations endpoint refuses to aggregate. `years=2023&states=MI` alone returns
    HTTP 400 `provide-atleast-one-filter-criteria`: it wants a non-geographic dimension
    before it will count anything. Enumerating the whole `actions_taken` domain (1-8) is
    what turns it into a per-year control total, and the response echoes back the buckets
    it answered with -- so a domain that grows a ninth code surfaces as a bucket we never
    requested instead of as a quiet undercount.
  * No endpoint publishes the available years. `/view/config`, `/view/years` and
    `/view/filers/years` are all 404, so the range is discovered by probing upward from
    2018 -- the first year of the current schema -- until the API refuses. Its refusal
    names a stale range ("must provide years in the range of 2018-2023") while serving
    2024 and 2025 happily, which is why the probe trusts the status code, not the prose.
  * HEAD is 405. Resolving a year to its file URL, size and `last-modified` therefore
    uses a one-byte ranged GET, which the CDN answers 206 with a `content-range` total.
  * Every year resolves to the same filename. The generated extract is named for a hash
    of the *filter* (`state_MI`), the year appears only in the path, and
    `content-disposition` says `state_MI.csv` for all eight years. Naming landing files
    after the URL -- what every other fetcher in this repo does -- would have written
    eight years into one file and left a raw layer that looked complete. Landing names
    come from the year we asked for, and `partition_failures` below is what proves the
    file we got is the file we asked for.
  * The two control totals do not always agree. Summing the aggregation buckets and
    summing the per-institution filer counts are independent statements about the same
    slice. They match exactly for 2019-2023 and differ for 2018 (+89), 2024 (+2,688) and
    2025 (+2,553). Both are recorded per year. The record-level aggregation is the total
    landed rows are compared against; the filer sum is a second observation, not a target.

Run:  python -m ingest.fintech.hmda [--mode live|fixture]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import duckdb
import httpx
from ingest.common import (
    PAGE_SLEEP_SECONDS,
    REPO_ROOT,
    DiscoveryError,
    base_manifest,
    http_client,
    human_bytes,
    load_table,
    make_logger,
    partition_counts,
    paths_for,
    raise_for_transient,
    read_existing_manifest,
    repartition_to_raw,
    retrying,
    sha256_of,
    skip_as_current,
    sql_literal,
    stream_download,
    write_baseline,
    write_json,
)

TRACK = "fintech"
LAR_SOURCE = "hmda_lar"
INSTITUTIONS_SOURCE = "hmda_institutions"
LAR_TABLE = "raw.fin_hmda_lar"
INSTITUTIONS_TABLE = "raw.fin_hmda_institutions"
LAR_PARTITION_COLUMN = "activity_year"
INSTITUTIONS_PARTITION_COLUMN = "period"
SCOPE_STATE = "MI"

BROWSER_API = "https://ffiec.cfpb.gov/v2/data-browser-api/view"
CSV_ENDPOINT = f"{BROWSER_API}/csv"
AGGREGATIONS_ENDPOINT = f"{BROWSER_API}/aggregations"
FILERS_ENDPOINT = f"{BROWSER_API}/filers"

# The modern HMDA schema begins with the 2018 filing year; 2017 and earlier use the
# pre-rule layout, and the API refuses them outright rather than converting.
FIRST_MODERN_YEAR = 2018

# The complete action_taken domain: 1 originated, 2 approved not accepted, 3 denied,
# 4 withdrawn, 5 file closed incomplete, 6 purchased, 7 preapproval denied,
# 8 preapproval approved but not accepted. Sent explicitly so the response has to name
# the buckets it answered with.
ALL_ACTIONS_TAKEN = ("1", "2", "3", "4", "5", "6", "7", "8")

LAR_GLOB = f"hmda_lar_{SCOPE_STATE}_*.csv"
FILERS_GLOB = "hmda_filers_national_*.json"

FIELD_REFERENCE = (
    "https://ffiec.cfpb.gov/documentation/publications/loan-level-datasets/lar-data-fields"
)
API_REFERENCE = "https://cfpb.github.io/hmda-platform/#data-browser-api"

# 1.5 GB of CSV across eight files: let DuckDB spill rather than exhaust RAM.
DUCKDB_MEMORY_LIMIT = "4GB"

log = make_logger(TRACK, "hmda")


@dataclass(frozen=True)
class YearExtract:
    """One year of the Michigan LAR, as the publisher describes it before we download."""

    year: int
    resolved_url: str
    edition: str | None
    landing_bytes: int
    last_modified: str | None
    etag: str | None
    records_reported: int
    filers_reported: int
    institutions_reported: int

    @property
    def control_total_gap(self) -> int:
        return self.records_reported - self.filers_reported


# ── pure helpers (unit-tested without a network) ───────────────────────────────


def lar_landing_name(year: int) -> str:
    """Landing files are named for the year requested, never for the URL returned.

    Every year's extract resolves to the same hash filename and the same
    `content-disposition` name, so URL-derived naming collapses eight years into one.
    """
    return f"hmda_lar_{SCOPE_STATE}_{year}.csv"


def filers_landing_name(year: int) -> str:
    return f"hmda_filers_national_{year}.json"


def dataset_edition(resolved_url: str) -> str | None:
    """Which generated edition the publisher served this year's filter from.

    Recorded, never relied on: 2018-2022 arrived from `three-year`, 2023-2024 from
    `one-year`, 2025 from `snapshot`. If a year moves between editions -- which changes
    nothing about the rows but everything about which publication they came from -- the
    manifest is where that shows up.
    """
    parts = [part for part in urlparse(resolved_url).path.split("/") if part]
    return parts[-2] if len(parts) >= 2 else None


def year_is_unpublished(response: httpx.Response) -> bool:
    """Distinguish "that year is not published" from any other 400 the API might raise.

    The year validator rejects out-of-range years with a message naming a range that is
    already wrong -- it claims 2018-2023 while serving 2025 -- so the range in the prose
    is ignored and only its shape is used. Every other 4xx is a real error and must not
    be read as the end of the available years.
    """
    if response.status_code != 400:
        return False
    return "provide years in the range" in response.text.lower()


def aggregation_total(payload: dict[str, Any], requested: tuple[str, ...]) -> int:
    """Sum the buckets, refusing to report a total the publisher did not fully cover.

    A bucket we did not request means the `action_taken` domain has grown, and every
    control total computed this way would be short by however many rows sit in the new
    code. That is a stop, not a warning.
    """
    buckets = payload.get("aggregations")
    if not buckets:
        raise DiscoveryError(
            f"No aggregation buckets in the response; a control total cannot be formed. "
            f"Raw response: {json.dumps(payload)[:800]}"
        )
    unexpected = {str(bucket.get("actions_taken")) for bucket in buckets} - set(requested)
    if unexpected:
        raise DiscoveryError(
            f"Aggregation answered with action codes we did not request: {sorted(unexpected)}. "
            f"The action_taken domain has grown beyond {list(requested)} and every control "
            f"total built from it would undercount. Raw response: {json.dumps(payload)[:800]}"
        )
    return sum(int(bucket["count"]) for bucket in buckets)


def filers_total(payload: dict[str, Any]) -> tuple[int, int]:
    """Rows and institutions the filer list reports: the second, independent count."""
    institutions = payload.get("institutions")
    if not institutions:
        raise DiscoveryError(
            f"No institutions in the filer response; refusing a blank reference table. "
            f"Raw response: {json.dumps(payload)[:800]}"
        )
    return sum(int(filer["count"]) for filer in institutions), len(institutions)


def refresh_fingerprint(extracts: tuple[YearExtract, ...]) -> dict[str, str | None]:
    """Per-year `last-modified`, which is what "is this source current?" means here.

    A single timestamp cannot answer it: a new filing year appears as a new key rather
    than as a newer stamp, and comparing only the newest would skip the download that
    would have landed it.
    """
    return {str(extract.year): extract.last_modified for extract in extracts}


def newest_last_modified(extracts: tuple[YearExtract, ...]) -> str | None:
    """One timestamp for L1's manifest chain; the fingerprint above is what we compare.

    Sorted as a date rather than as a string: these are RFC 7231 HTTP dates, where
    "Wed, 15 Oct 2025" sorts above "Tue, 30 Jun 2026" alphabetically.
    """
    stamps = [extract.last_modified for extract in extracts if extract.last_modified]
    if not stamps:
        return None
    return max(stamps, key=parsedate_to_datetime)


def lar_relation(landing_pattern: Path) -> str:
    """The all-varchar rule, which HMDA needs more than any other source in the repo.

    Numeric-looking columns carry the literal string "Exempt" where a small filer claimed
    the partial exemption, "NA" where the field does not apply, ages arrive as the codes
    8888 and 9999, and `debt_to_income_ratio` mixes bare numbers with ranges like "20%-<30%".
    Type inference silences all of it. Typing is session 2's job, column by column.
    """
    return f"read_csv({sql_literal(landing_pattern)}, all_varchar=true, header=true)"


def institutions_relation(landing_pattern: Path) -> str:
    """The filer list, read as JSON with every column pinned to VARCHAR.

    The same rule as the CSVs, enforced differently: left to itself DuckDB would infer
    `count` and `period` as integers off the JSON, which is type inference in raw by
    another route. The struct is declared so the conversion stays layout-only.
    """
    struct = 'STRUCT(lei VARCHAR, "name" VARCHAR, "count" VARCHAR, period VARCHAR)[]'
    return (
        '(SELECT filer.lei, filer."name", filer."count", filer.period FROM '
        f"(SELECT unnest(institutions) AS filer FROM read_json({sql_literal(landing_pattern)}, "
        f"columns={{institutions: '{struct}'}})))"
    )


def partition_failures(partition_rows: dict[str, int], years: tuple[int, ...]) -> list[str]:
    """Prove the files we landed are the files we asked for: one year each, all present.

    The extracts are cache files named for the filter with the year only in the path, so
    "did this file hold the year we requested?" is worth asking of every one of them. A
    mismatch here means either a year failed to land or a file held the wrong year --
    both of which would otherwise pass a row-count check.
    """
    expected = {str(year) for year in years}
    present = set(partition_rows)
    problems = []
    if missing := sorted(expected - present):
        problems.append(f"no rows landed for requested years {missing}")
    if unexpected := sorted(present - expected):
        problems.append(
            f"rows landed under years we never requested {unexpected}: an extract "
            "contained a different year than its path claimed"
        )
    return problems


def scope_failures(states: list[str]) -> list[str]:
    """Raw is deliberately MI-only here, so any other state is a scope leak."""
    leaked = sorted(state for state in states if state != SCOPE_STATE)
    if not leaked:
        return []
    return [f"non-Michigan rows in an MI-scoped pull: {leaked}"]


def lar_manifest_payload(
    *,
    extracts: tuple[YearExtract, ...],
    landing_files: list[dict[str, Any]],
    rows_landing: int,
    rows_raw: int,
    rows_duckdb: int,
    partition_rows: dict[str, int],
    retrieved_at: str,
) -> dict[str, Any]:
    reported = sum(extract.records_reported for extract in extracts)
    return {
        **base_manifest(
            track=TRACK,
            source=LAR_SOURCE,
            publisher="CFPB / FFIEC",
            dataset_name="HMDA Loan/Application Register (Michigan)",
            resolved_url=CSV_ENDPOINT,
            distribution_format="csv",
            retrieved_at=retrieved_at,
            landing_files=landing_files,
            source_reported_count=reported,
            source_last_refresh=newest_last_modified(extracts),
            duckdb_table=LAR_TABLE,
            rows_landing=rows_landing,
            rows_raw=rows_raw,
            rows_duckdb=rows_duckdb,
            partition_column=LAR_PARTITION_COLUMN,
            partition_rows=partition_rows,
            # Every partition key is a year we asked for, or the run already failed.
            nonstandard_partitions={},
        ),
        "discovered_from": AGGREGATIONS_ENDPOINT,
        "raw_scope": {
            "state": SCOPE_STATE,
            "reason": (
                "A year of national LAR is tens of millions of rows against roughly 400k "
                "for Michigan. National benchmarks come from the publisher's aggregation "
                "endpoint as control totals, not from raw."
            ),
        },
        "years": [
            {
                "year": extract.year,
                "resolved_url": extract.resolved_url,
                "dataset_edition": extract.edition,
                "bytes": extract.landing_bytes,
                "last_modified": extract.last_modified,
                "etag": extract.etag,
                "source_reported_records": extract.records_reported,
                "source_reported_by_filers": extract.filers_reported,
                "control_total_gap": extract.control_total_gap,
                "institutions": extract.institutions_reported,
                "rows_raw": partition_rows.get(str(extract.year), 0),
            }
            for extract in extracts
        ],
        "refresh_fingerprint": refresh_fingerprint(extracts),
        "control_totals": {
            "records_endpoint": AGGREGATIONS_ENDPOINT,
            "records_method": (
                "sum of per-action_taken counts over the full domain "
                f"{list(ALL_ACTIONS_TAKEN)}; the endpoint refuses to aggregate without a "
                "non-geographic filter"
            ),
            "filers_endpoint": FILERS_ENDPOINT,
            "filers_method": "sum of per-institution counts for the same year and state",
            "disagreement_note": (
                "The two totals agree exactly for 2019-2023 and disagree for 2018 (+89), "
                "2024 (+2,688) and 2025 (+2,553). The landed extracts matched the "
                "record-level total exactly in all eight years, so it is the filer sum "
                "that undercounts, not the aggregation -- most likely rows whose LEI is "
                "absent from the published filer list for that year. Cause not "
                "established at the publisher; both totals are recorded per year, and "
                "the record-level one is what landed rows are compared against."
            ),
        },
        "coverage_caveats": [
            "HMDA carries no credit score and no debt-to-income underwriting detail, so "
            "fair-lending findings from it are descriptive, never conclusions.",
            "Small filers may claim a partial exemption, which arrives as the literal "
            "string 'Exempt' in otherwise numeric fields.",
            "Applicant age uses the codes 8888 and 9999 rather than nulls.",
            "Census tract and county are as-filed; a tract of 'NA' is a real filing.",
            "County is as-filed and not always a Michigan county: all 83 MI counties are "
            "present, and alongside them sit 44,167 rows with county_code 'NA' and 254 "
            "rows carrying out-of-state county FIPS on Michigan-state filings -- 1.1% of "
            "the LAR has no usable MI county. County-grain measures need a denominator "
            "that says so rather than one that quietly drops them.",
            "Institutions below the reporting thresholds never file, so this is not a "
            "census of Michigan mortgage activity.",
        ],
    }


def institutions_manifest_payload(
    *,
    extracts: tuple[YearExtract, ...],
    landing_files: list[dict[str, Any]],
    rows_landing: int,
    rows_raw: int,
    rows_duckdb: int,
    partition_rows: dict[str, int],
    retrieved_at: str,
) -> dict[str, Any]:
    return {
        **base_manifest(
            track=TRACK,
            source=INSTITUTIONS_SOURCE,
            publisher="CFPB / FFIEC",
            dataset_name="HMDA filers by year (national)",
            resolved_url=FILERS_ENDPOINT,
            distribution_format="json",
            retrieved_at=retrieved_at,
            landing_files=landing_files,
            # One row per filer per year: the reference table's own count, not the LAR's.
            source_reported_count=rows_landing,
            # The filer endpoint publishes no refresh signal of any kind, so currency
            # cannot be proved and this source re-fetches every run. It is 4 MB.
            source_last_refresh=None,
            duckdb_table=INSTITUTIONS_TABLE,
            rows_landing=rows_landing,
            rows_raw=rows_raw,
            rows_duckdb=rows_duckdb,
            partition_column=INSTITUTIONS_PARTITION_COLUMN,
            partition_rows=partition_rows,
            nonstandard_partitions={},
        ),
        "raw_scope": {
            "state": None,
            "reason": (
                "National on purpose: the institution universe is the benchmark Michigan "
                "filer counts are read against, and it is small enough to keep whole."
            ),
        },
        "source_refresh_unavailable_reason": (
            "The filers endpoint is a query API with no last-modified header, ETag or "
            "published refresh timestamp, so a re-run cannot prove it is current."
        ),
        "years": [
            {
                "year": extract.year,
                "institutions_reported": extract.institutions_reported,
                "rows_raw": partition_rows.get(str(extract.year), 0),
            }
            for extract in extracts
        ],
        "grain_note": (
            "One row per LEI per filing year, so an institution that filed in six years "
            "appears six times. Counting institutions means counting DISTINCT lei within "
            "a year."
        ),
    }


# ── network ───────────────────────────────────────────────────────────────────


@retrying
def _get(client: httpx.Client, url: str, params: dict[str, str]) -> httpx.Response:
    response = client.get(url, params=params)
    raise_for_transient(response)
    return response


@retrying
def resolve_extract_file(client: httpx.Client, year: int) -> httpx.Response:
    """Resolve a year's extract to its file URL, size and `last-modified`.

    A one-byte ranged GET because HEAD on this endpoint is 405. The CDN answers 206 with
    the full size in `content-range`, and the redirect target is the URL we then download
    -- discovered at runtime, never constructed here.
    """
    response = client.get(
        CSV_ENDPOINT,
        params={"years": str(year), "states": SCOPE_STATE},
        headers={"Range": "bytes=0-0"},
    )
    raise_for_transient(response)
    return response


def fetch_year_counts(client: httpx.Client, year: int) -> tuple[int, int, int]:
    """The two independent control totals for one year, plus the institution count."""
    aggregations = _get(
        client,
        AGGREGATIONS_ENDPOINT,
        {
            "years": str(year),
            "states": SCOPE_STATE,
            "actions_taken": ",".join(ALL_ACTIONS_TAKEN),
        },
    ).json()
    records = aggregation_total(aggregations, ALL_ACTIONS_TAKEN)
    time.sleep(PAGE_SLEEP_SECONDS)

    filers = _get(client, FILERS_ENDPOINT, {"years": str(year), "states": SCOPE_STATE}).json()
    by_filers, institutions = filers_total(filers)
    return records, by_filers, institutions


def discover_years(client: httpx.Client) -> tuple[int, ...]:
    """Probe upward from the first modern filing year until the API stops serving one.

    No endpoint publishes the range, and the validator's own message about it is stale, so
    availability is whatever the API actually answers. A probe that fails on the first
    year is a hard stop: it means the endpoint moved, not that HMDA published nothing.
    """
    years: list[int] = []
    for year in range(FIRST_MODERN_YEAR, datetime.now(UTC).year + 1):
        response = client.get(FILERS_ENDPOINT, params={"years": str(year), "states": SCOPE_STATE})
        if year_is_unpublished(response):
            log(f"  {year}: not published yet (the API declined the year)")
            break
        raise_for_transient(response)
        institutions = len(response.json().get("institutions", []))
        if not institutions:
            log(f"  {year}: served but empty; treating the published range as ending at {year - 1}")
            break
        log(f"  {year}: available, {institutions:,} Michigan filers")
        years.append(year)
        time.sleep(PAGE_SLEEP_SECONDS)

    if not years:
        raise DiscoveryError(
            f"No HMDA years available from {FIRST_MODERN_YEAR} onward. The data-browser API "
            f"has moved or changed shape; refusing to continue against {FILERS_ENDPOINT}."
        )
    return tuple(years)


def resolve_extracts(client: httpx.Client, years: tuple[int, ...]) -> tuple[YearExtract, ...]:
    extracts: list[YearExtract] = []
    for year in years:
        probe = resolve_extract_file(client, year)
        size = int((probe.headers.get("content-range") or "0/0").rsplit("/", 1)[-1])
        if not size:
            raise DiscoveryError(
                f"{year}: no content-range on the ranged GET, so the extract size is "
                f"unknown. Headers: {dict(probe.headers)}"
            )
        time.sleep(PAGE_SLEEP_SECONDS)

        records, by_filers, institutions = fetch_year_counts(client, year)
        extracts.append(
            YearExtract(
                year=year,
                resolved_url=str(probe.url),
                edition=dataset_edition(str(probe.url)),
                landing_bytes=size,
                last_modified=probe.headers.get("last-modified"),
                etag=probe.headers.get("etag"),
                records_reported=records,
                filers_reported=by_filers,
                institutions_reported=institutions,
            )
        )
        extract = extracts[-1]
        gap = (
            f"  (filer sum differs by {extract.control_total_gap:+,})"
            if extract.control_total_gap
            else ""
        )
        log(
            f"  {year}: {human_bytes(size)} from {extract.edition}, "
            f"{records:,} records reported{gap}"
        )
        time.sleep(PAGE_SLEEP_SECONDS)
    return tuple(extracts)


def download_lar(
    client: httpx.Client, extracts: tuple[YearExtract, ...], landing_dir: Path
) -> list[dict[str, Any]]:
    """Download each year to a name of our own choosing. See `lar_landing_name`."""
    landing_dir.mkdir(parents=True, exist_ok=True)
    for stale in landing_dir.glob(LAR_GLOB):
        stale.unlink()

    landing_files: list[dict[str, Any]] = []
    for extract in extracts:
        target = landing_dir / lar_landing_name(extract.year)
        log(f"  {extract.year}: downloading {human_bytes(extract.landing_bytes)} -> {target.name}")
        landed_bytes, digest = stream_download(client, extract.resolved_url, target, log=log)
        landing_files.append({"name": target.name, "bytes": landed_bytes, "sha256": digest})
        time.sleep(PAGE_SLEEP_SECONDS)
    return landing_files


def download_filers(
    client: httpx.Client, years: tuple[int, ...], landing_dir: Path
) -> list[dict[str, Any]]:
    """Land the nationwide filer list per year, exactly as the API returned it."""
    landing_dir.mkdir(parents=True, exist_ok=True)
    for stale in landing_dir.glob(FILERS_GLOB):
        stale.unlink()

    landing_files: list[dict[str, Any]] = []
    for year in years:
        response = _get(client, FILERS_ENDPOINT, {"years": str(year)})
        payload = response.json()
        _, institutions = filers_total(payload)
        target = landing_dir / filers_landing_name(year)
        target.write_bytes(response.content)
        landing_files.append(
            {"name": target.name, "bytes": len(response.content), "sha256": sha256_of(target)}
        )
        log(f"  {year}: {institutions:,} filers nationwide -> {target.name}")
        time.sleep(PAGE_SLEEP_SECONDS)
    return landing_files


# ── conversion ────────────────────────────────────────────────────────────────


def convert(
    con: duckdb.DuckDBPyConnection,
    *,
    relation: str,
    raw_dir: Path,
    table: str,
    partition_column: str,
) -> tuple[int, int, int, dict[str, int]]:
    rows_landing, rows_raw = repartition_to_raw(
        con, relation=relation, raw_dir=raw_dir, partition_column=partition_column
    )
    counts = partition_counts(con, raw_dir, partition_column)
    rows_duckdb = load_table(con, table, raw_dir)
    return rows_landing, rows_raw, rows_duckdb, counts


def landed_states(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    rows = con.execute(f"SELECT DISTINCT state_code FROM {table} ORDER BY 1").fetchall()
    return [str(row[0]) for row in rows]


# ── orchestration ─────────────────────────────────────────────────────────────


def _fixture_extracts() -> tuple[YearExtract, ...]:
    """Fixture mode reads the years, sizes and control totals from the committed sample."""
    metadata = json.loads(
        (REPO_ROOT / "tests" / "fixtures" / TRACK / "hmda.metadata.json").read_text(
            encoding="utf-8"
        )
    )
    return tuple(
        YearExtract(
            year=int(year["year"]),
            resolved_url="fixture://" + lar_landing_name(int(year["year"])),
            edition="fixture",
            landing_bytes=0,
            last_modified=year.get("last_modified"),
            etag=None,
            records_reported=int(year["source_reported_records"]),
            filers_reported=int(year["source_reported_by_filers"]),
            institutions_reported=int(year["institutions"]),
        )
        for year in metadata["years"]
    )


def run(mode: str, force: bool = False) -> int:
    data_root, db_path = paths_for(mode)
    lar_raw_dir = data_root / "raw" / TRACK / LAR_SOURCE
    institutions_raw_dir = data_root / "raw" / TRACK / INSTITUTIONS_SOURCE
    lar_landing_dir = data_root / "landing" / TRACK / LAR_SOURCE
    institutions_landing_dir = data_root / "landing" / TRACK / INSTITUTIONS_SOURCE
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if mode == "fixture":
        fixtures = REPO_ROOT / "tests" / "fixtures" / TRACK
        lar_pattern = fixtures / "hmda_lar.csv"
        filers_pattern = fixtures / "hmda_institutions.json"
        for required in (lar_pattern, filers_pattern):
            if not required.exists():
                log(f"FAIL: fixture missing at {required}")
                return 1
        extracts = _fixture_extracts()
        years = tuple(extract.year for extract in extracts)
        lar_landing_files = [
            {
                "name": lar_pattern.name,
                "bytes": lar_pattern.stat().st_size,
                "sha256": sha256_of(lar_pattern),
            }
        ]
        filers_landing_files = [
            {
                "name": filers_pattern.name,
                "bytes": filers_pattern.stat().st_size,
                "sha256": sha256_of(filers_pattern),
            }
        ]
        lar_current = False
        log("fixture mode: discovery and download SKIPPED (offline)")
        log(f"years from the fixture: {years[0]}..{years[-1]}")
    else:
        with http_client(read_timeout=600.0) as client:
            log(f"discovering published years from {FILERS_ENDPOINT}")
            years = discover_years(client)
            log(f"years available: {years[0]}..{years[-1]} ({len(years)} years)")

            log("resolving each year's extract and control totals")
            extracts = resolve_extracts(client, years)

            existing = read_existing_manifest(lar_raw_dir)
            # Guarded on the table as well as the fingerprint: the skip branch below reads
            # `SELECT count(*) FROM raw.fin_hmda_lar`, so skipping into a missing table
            # crashed rather than no-opped.
            lar_current = skip_as_current(
                force=force,
                publisher_unchanged=bool(existing)
                and existing.get("refresh_fingerprint") == refresh_fingerprint(extracts),
                db_path=db_path,
                tables=(LAR_TABLE,),
                log=log,
            )

            write_baseline(
                TRACK,
                LAR_SOURCE,
                {
                    "note": "HMDA publishes no machine-readable field metadata; this is a "
                    "pointer file, per CLAUDE.md rule 8.",
                    "field_reference": FIELD_REFERENCE,
                    "api_reference": API_REFERENCE,
                    "filing_instructions": "https://ffiec.cfpb.gov/documentation/publications/"
                    "loan-level-datasets/lar-data-fields",
                    "columns_observed": 99,
                    "years_observed": list(years),
                    "retrieved_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                },
                kind="pointer",
            )
            write_baseline(
                TRACK,
                INSTITUTIONS_SOURCE,
                {
                    "note": "The filers endpoint publishes no field metadata and no schema; "
                    "this is a pointer file, per CLAUDE.md rule 8.",
                    "api_reference": API_REFERENCE,
                    "fields_observed": ["lei", "name", "count", "period"],
                    "years_observed": list(years),
                    "retrieved_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                },
                kind="pointer",
            )

            # The filer list has no refresh signal at all, so its currency can never be
            # proved and it always re-fetches. Four megabytes; the LAR is what we skip.
            log(f"landing the national filer list for {len(years)} years")
            filers_landing_files = download_filers(client, years, institutions_landing_dir)

            if lar_current:
                log("SKIPPED (current): every year's extract matches the recorded last-modified")
                lar_landing_files = existing.get("landing_files", []) if existing else []
            else:
                total = sum(extract.landing_bytes for extract in extracts)
                log(f"landing {len(years)} Michigan extracts, {human_bytes(total)} total")
                lar_landing_files = download_lar(client, extracts, lar_landing_dir)

        lar_pattern = lar_landing_dir / LAR_GLOB
        filers_pattern = institutions_landing_dir / FILERS_GLOB

    retrieved_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    con = duckdb.connect(str(db_path))
    try:
        con.execute(f"SET memory_limit='{DUCKDB_MEMORY_LIMIT}'")
        con.execute(f"SET temp_directory={sql_literal(data_root / '.duckdb_tmp')}")

        log(f"converting the filer list -> {institutions_raw_dir}")
        inst_landing, inst_raw, inst_table, inst_counts = convert(
            con,
            relation=institutions_relation(filers_pattern),
            raw_dir=institutions_raw_dir,
            table=INSTITUTIONS_TABLE,
            partition_column=INSTITUTIONS_PARTITION_COLUMN,
        )
        log(f"landing {inst_landing:,} -> raw {inst_raw:,} -> {INSTITUTIONS_TABLE} {inst_table:,}")

        if lar_current:
            log("LAR conversion skipped: raw already matches the published extracts")
            lar_counts = partition_counts(con, lar_raw_dir, LAR_PARTITION_COLUMN)
            lar_landing_rows = lar_raw_rows = sum(lar_counts.values())
            lar_table_rows = con.execute(f"SELECT count(*) FROM {LAR_TABLE}").fetchone()[0]
        else:
            log(f"converting {len(years)} Michigan extracts -> {lar_raw_dir}")
            lar_landing_rows, lar_raw_rows, lar_table_rows, lar_counts = convert(
                con,
                relation=lar_relation(lar_pattern),
                raw_dir=lar_raw_dir,
                table=LAR_TABLE,
                partition_column=LAR_PARTITION_COLUMN,
            )
        states = landed_states(con, LAR_TABLE)
    finally:
        con.close()

    problems = partition_failures(lar_counts, years) + scope_failures(states)
    for problem in problems:
        log(f"FAIL  {problem}")
    if problems:
        return 1

    write_json(
        institutions_raw_dir / "manifest.json",
        institutions_manifest_payload(
            extracts=extracts,
            landing_files=filers_landing_files,
            rows_landing=inst_landing,
            rows_raw=inst_raw,
            rows_duckdb=inst_table,
            partition_rows=inst_counts,
            retrieved_at=retrieved_at,
        ),
    )
    write_json(
        lar_raw_dir / "manifest.json",
        lar_manifest_payload(
            extracts=extracts,
            landing_files=lar_landing_files,
            rows_landing=lar_landing_rows,
            rows_raw=lar_raw_rows,
            rows_duckdb=lar_table_rows,
            partition_rows=lar_counts,
            retrieved_at=retrieved_at,
        ),
    )

    log(f"landing {lar_landing_rows:,} -> raw {lar_raw_rows:,} -> {LAR_TABLE} {lar_table_rows:,}")
    log(f"michigan LAR by year: {', '.join(f'{y}={lar_counts[y]:,}' for y in sorted(lar_counts))}")
    for extract in extracts:
        landed = lar_counts.get(str(extract.year), 0)
        gap = landed - extract.records_reported
        if gap:
            log(
                f"NOTE  {extract.year}: landed {landed:,} vs {extract.records_reported:,} "
                f"reported by the aggregation endpoint (gap {gap:+,}); filer sum was "
                f"{extract.filers_reported:,}"
            )
    log(f"states present in raw: {states}")
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
        help="re-download every year even when each one matches the recorded last-modified",
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
