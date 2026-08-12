"""Ingest the BLS CPI-U time series into DuckDB's raw schema as the shared deflator.

Domain-neutral reference. Two of the three projects state monetary comparisons in constant
dollars -- INS-E2 compares Michigan severity per claim, HLT-E3 compares cost per
beneficiary -- and neither can do it without a published price index. Nothing here belongs
to a single track, which is why it lands under `shared` and builds with `just shared`.

Two files land, from BLS's own `cu` (CPI for All Urban Consumers) flat-file database:

  * `cu.data.1.AllItems` -> `raw.ref_cpi_u`         63,888 observations, 1913-2026
  * `cu.series`          -> `raw.ref_cpi_u_series`   8,104 series definitions

Four things this source will do to anyone who has not read it:

  * `series_id` is padded to 17 characters, in both files. `WHERE series_id =
    'CUUR0000SA0'` matches nothing at all -- the stored value is `'CUUR0000SA0      '`.
    The padding is what BLS publishes and raw keeps it verbatim per the all-varchar rule,
    so every filter and join needs `trim()` until session 2 types these columns. `value`
    is space-padded the same way. The run asserts the width is uniform, so this is a known
    constant rather than a surprise waiting in one file.
  * Seasonally adjusted and unadjusted series sit side by side, distinguished only inside
    the series id: `CUSR0000SA0` is adjusted, `CUUR0000SA0` is not. Deflating historical
    dollars uses the *unadjusted* annual average; seasonal adjustment exists for
    month-over-month movement and is the wrong series for constant-dollar work. The series
    table is partitioned on `seasonal` so that choice is visible in the path.
  * The annual average is a thirteenth month. BLS publishes it as `period = 'M13'`, and
    semiannual averages as S01-S03, in the same column as the twelve real months. Summing
    or averaging across `period` therefore double counts. M13 is what annual deflation
    uses; its presence is asserted on every run.
  * The two files do not cover the same series. `cu.series` defines all 8,104 CPI series
    while the All-items data file carries observations for 201 of them; the rest live in
    the other 19 `cu.data.*` files, which are not landed because no question in the set
    needs them. Every observed series is checked to exist in the definitions -- the
    reverse does not hold and is not a defect.

BLS publishes no row count, so reconciliation comes from the publisher's other mouth: the
unkeyed Public API v2 is asked for the latest value of `CUUR0000SA0` and compared to what
landed. The API is used only for that cross-check -- it returns three years to unregistered
callers, which is why the flat file is the source of record.

Run:  python -m ingest.shared.bls [--mode live|fixture]
"""

from __future__ import annotations

import argparse
import json
import os
import re
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
    load_table,
    make_logger,
    partition_counts,
    paths_for,
    raise_for_transient,
    read_existing_manifest,
    repartition_to_raw,
    retrying,
    sql_literal,
    stream_download,
    write_baseline,
    write_json,
)

TRACK = "shared"
OBSERVATIONS_SOURCE = "cpi_u"
SERIES_SOURCE = "cpi_u_series"
OBSERVATIONS_TABLE = "raw.ref_cpi_u"
SERIES_TABLE = "raw.ref_cpi_u_series"
OBSERVATIONS_PARTITION_COLUMN = "year"
SERIES_PARTITION_COLUMN = "seasonal"

CU_DIRECTORY = "https://download.bls.gov/pub/time.series/cu/"
OBSERVATIONS_FILE = "cu.data.1.AllItems"
SERIES_FILE = "cu.series"
INDEX_PATTERN = re.compile(r'HREF="/pub/time\.series/cu/(cu\.[\w.]+)"', re.IGNORECASE)

# The deflator series every constant-dollar measure in this repo should use, and the one
# the API cross-check is run against: All items, US city average, not seasonally adjusted.
DEFLATOR_SERIES_ID = "CUUR0000SA0"
API_SERIES = f"https://api.bls.gov/publicAPI/v2/timeseries/data/{DEFLATOR_SERIES_ID}"

# BLS pads every identifier to a fixed width. Asserted rather than trimmed away.
SERIES_ID_WIDTH = 17
# The annual average, which BLS files as a thirteenth month.
ANNUAL_PERIOD = "M13"

FIELD_REFERENCE = "https://download.bls.gov/pub/time.series/cu/cu.txt"
API_REFERENCE = "https://www.bls.gov/developers/api_signature_v2.htm"

log = make_logger(TRACK, "bls")


# ── pure helpers (unit-tested without a network) ───────────────────────────────


def parse_index(html: str) -> list[str]:
    """The files BLS's own directory listing offers. Never hardcode a filename.

    `download.bls.gov` publishes an IIS-style index rather than a catalogue, so this is the
    publisher's own statement of what exists -- the same approach the CFPB fetcher takes to
    that publisher's data page.
    """
    names = sorted(set(INDEX_PATTERN.findall(html)))
    if not names:
        raise DiscoveryError(
            f"No cu.* files found in the {CU_DIRECTORY} index. The listing format may have "
            f"changed. Searched {len(html):,} bytes for {INDEX_PATTERN.pattern!r}."
        )
    return names


def require_files(available: list[str], wanted: tuple[str, ...]) -> None:
    """Stop if the publisher stopped shipping a file we depend on."""
    if missing := sorted(set(wanted) - set(available)):
        raise DiscoveryError(
            f"{CU_DIRECTORY} no longer offers {missing}. It listed {len(available)} files: "
            f"{available}"
        )


def tsv_relation(landing: Path) -> str:
    """Tab-separated, all-varchar, and with quote processing switched off.

    These files are not CSV: a stray double quote inside a series title would otherwise be
    read as a quoted field and swallow the delimiters after it. Disabling quoting is what
    keeps the conversion byte-faithful, and the all-varchar rule keeps the padding BLS
    ships inside `series_id` and `value`.
    """
    return f"read_csv({sql_literal(landing)}, all_varchar=true, header=true, delim='\\t', quote='')"


def padding_failures(widths: list[int]) -> list[str]:
    """A uniform pad width is a documented quirk; a mixed one is a silent join bug."""
    if widths == [SERIES_ID_WIDTH]:
        return []
    return [
        f"series_id widths are {sorted(widths)}, expected a uniform {SERIES_ID_WIDTH}. "
        "Joins between the two files rely on the padding being identical in both."
    ]


def annual_period_failures(periods: list[str]) -> list[str]:
    """Annual deflation needs BLS's annual average, which is period M13."""
    if ANNUAL_PERIOD in periods:
        return []
    return [
        f"no {ANNUAL_PERIOD} rows: BLS files the annual average as a thirteenth month, and "
        "constant-dollar comparisons have nothing to deflate with without it"
    ]


def definition_failures(undefined: list[str]) -> list[str]:
    """Every observed series must have a definition; the reverse is expected and fine."""
    if not undefined:
        return []
    return [
        f"{len(undefined)} observed series have no row in {SERIES_FILE}: "
        f"{sorted(undefined)[:5]}. The two files disagree about what exists."
    ]


def crosscheck_failures(
    landed: dict[str, Any] | None, reported: dict[str, Any] | None
) -> list[str]:
    """Compare the flat file against the publisher's other mouth, the public API.

    A missing API answer is not a failure -- the file is the source of record and the API
    is a courtesy -- but a *disagreement* is, because it means one of them is not what we
    think it is.
    """
    if reported is None:
        return []
    if landed is None:
        return [
            f"the API reports {DEFLATOR_SERIES_ID} at {reported['year']} "
            f"{reported['period']} = {reported['value']}, and that observation is absent "
            "from the landed file"
        ]
    if landed["value"] != reported["value"]:
        return [
            f"{DEFLATOR_SERIES_ID} {reported['year']} {reported['period']}: landed "
            f"{landed['value']!r} but the API reports {reported['value']!r}"
        ]
    return []


def observations_manifest_payload(
    *,
    landing_files: list[dict[str, Any]],
    resolved_url: str,
    last_modified: str | None,
    rows_landing: int,
    rows_raw: int,
    rows_duckdb: int,
    partition_rows: dict[str, int],
    periods: list[str],
    series_observed: int,
    crosscheck: dict[str, Any],
    retrieved_at: str,
) -> dict[str, Any]:
    years = sorted(key for key in partition_rows if key.isdigit())
    return {
        **base_manifest(
            track=TRACK,
            source=OBSERVATIONS_SOURCE,
            publisher="BLS",
            dataset_name="CPI for All Urban Consumers (CPI-U), all items",
            resolved_url=resolved_url,
            distribution_format="tsv",
            retrieved_at=retrieved_at,
            landing_files=landing_files,
            # BLS publishes no row count for its flat files; the API cross-check below is
            # the reconciliation instead.
            source_reported_count=None,
            source_last_refresh=last_modified,
            duckdb_table=OBSERVATIONS_TABLE,
            rows_landing=rows_landing,
            rows_raw=rows_raw,
            rows_duckdb=rows_duckdb,
            partition_column=OBSERVATIONS_PARTITION_COLUMN,
            partition_rows=partition_rows,
            # Every partition key is a four-digit year, or the run already failed.
            nonstandard_partitions={},
        ),
        "discovered_from": CU_DIRECTORY,
        "source_count_unavailable_reason": (
            "BLS ships flat files with no manifest and no row count. Reconciliation is a "
            f"value-level cross-check of {DEFLATOR_SERIES_ID} against the Public API v2."
        ),
        "coverage": {
            "years": [years[0], years[-1]] if years else [],
            "year_partitions": len(years),
            "periods": sorted(periods),
            "series_observed": series_observed,
            "annual_average_period": ANNUAL_PERIOD,
        },
        "deflator": {
            "series_id": DEFLATOR_SERIES_ID,
            "why": (
                "All items, US city average, not seasonally adjusted. Constant-dollar "
                "comparisons use the unadjusted annual average (period M13); the "
                "seasonally adjusted twin CUSR0000SA0 exists for month-over-month "
                "movement and is the wrong series for deflation."
            ),
            "api_crosscheck": crosscheck,
        },
        "gotchas": [
            f"`series_id` is padded to {SERIES_ID_WIDTH} characters. A literal filter on "
            f"'{DEFLATOR_SERIES_ID}' returns zero rows; trim() first.",
            "`value` is space-padded too, and stays that way in raw.",
            f"`period` mixes twelve months with the annual average {ANNUAL_PERIOD} and the "
            "semiannual averages S01-S03. Averaging across period double counts.",
            "Series begin and end in different years; an absent year for a series is a "
            "real gap in publication, not a load failure.",
        ],
    }


def series_manifest_payload(
    *,
    landing_files: list[dict[str, Any]],
    resolved_url: str,
    last_modified: str | None,
    rows_landing: int,
    rows_raw: int,
    rows_duckdb: int,
    partition_rows: dict[str, int],
    series_observed: int,
    retrieved_at: str,
) -> dict[str, Any]:
    return {
        **base_manifest(
            track=TRACK,
            source=SERIES_SOURCE,
            publisher="BLS",
            dataset_name="CPI-U series definitions",
            resolved_url=resolved_url,
            distribution_format="tsv",
            retrieved_at=retrieved_at,
            landing_files=landing_files,
            source_reported_count=None,
            source_last_refresh=last_modified,
            duckdb_table=SERIES_TABLE,
            rows_landing=rows_landing,
            rows_raw=rows_raw,
            rows_duckdb=rows_duckdb,
            partition_column=SERIES_PARTITION_COLUMN,
            partition_rows=partition_rows,
            nonstandard_partitions={},
        ),
        "discovered_from": CU_DIRECTORY,
        "source_count_unavailable_reason": (
            "BLS ships flat files with no manifest and no row count."
        ),
        "grain_note": (
            "One row per CPI series definition. Partitioned on `seasonal` -- 'U' "
            "unadjusted, 'S' seasonally adjusted -- because that is the choice a "
            "constant-dollar measure has to get right, and it belongs in the path rather "
            "than in a column somebody filters from memory."
        ),
        "relationship_note": (
            f"This file defines every CPI series. Observations were landed only for the "
            f"{series_observed} series in {OBSERVATIONS_FILE}; the remaining definitions "
            "describe series in the other cu.data.* files, which are not landed because no "
            "question in the set needs them. Definitions without observations are expected."
        ),
        "not_landed": (
            "cu.area, cu.item, cu.period, cu.periodicity, cu.seasonal and cu.footnote are "
            "code lookups BLS also publishes. `series_title` already carries the "
            "human-readable label, so they are left until something needs them."
        ),
    }


# ── network ───────────────────────────────────────────────────────────────────


@retrying
def fetch_index(client: httpx.Client) -> str:
    response = client.get(CU_DIRECTORY)
    raise_for_transient(response)
    return response.text


@retrying
def resolve_file(client: httpx.Client, name: str) -> dict[str, Any]:
    """A one-byte ranged GET for the refresh signal, before committing to the download."""
    url = CU_DIRECTORY + name
    response = client.get(url, headers={"Range": "bytes=0-0"})
    raise_for_transient(response)
    return {
        "name": name,
        "url": url,
        "last_modified": response.headers.get("last-modified"),
        # These files arrive gzipped, so any content-length describes the compressed body.
        "content_encoding": response.headers.get("content-encoding"),
    }


def fetch_api_latest(client: httpx.Client) -> dict[str, Any] | None:
    """The publisher's second statement about the same number.

    Unregistered callers get three years and a low daily quota, so this is a cross-check
    and never a source. A failure here is logged and does not fail the run -- the flat file
    is the source of record.
    """
    try:
        response = client.get(API_SERIES)
        raise_for_transient(response)
        body = response.json()
        if body.get("status") != "REQUEST_SUCCEEDED":
            log(f"WARNING  API cross-check unavailable: status {body.get('status')}")
            return None
        latest = body["Results"]["series"][0]["data"][0]
        return {
            "series_id": DEFLATOR_SERIES_ID,
            "year": latest["year"],
            "period": latest["period"],
            "value": latest["value"],
        }
    except (httpx.HTTPError, KeyError, IndexError, ValueError, json.JSONDecodeError) as error:
        log(f"WARNING  API cross-check unavailable: {type(error).__name__}: {error}")
        return None


# ── orchestration ─────────────────────────────────────────────────────────────


def landed_value(
    con: duckdb.DuckDBPyConnection, reported: dict[str, Any] | None
) -> dict[str, Any] | None:
    """Find the observation the API named. Trimmed here because of the padding, in the
    check only -- raw keeps what BLS shipped.

    `year` is cast because it is the partition column, and reading a hive-partitioned tree
    type-infers the key: it comes back BIGINT even though the file stored a padded string.
    That autocast is a platform-wide wrinkle, not something this source can fix locally.
    """
    if reported is None:
        return None
    row = con.execute(
        f"SELECT trim(value) FROM {OBSERVATIONS_TABLE} "
        "WHERE trim(series_id) = ? AND trim(CAST(year AS VARCHAR)) = ? AND trim(period) = ?",
        [reported["series_id"], reported["year"], reported["period"]],
    ).fetchone()
    return None if row is None else {"value": row[0]}


def run(mode: str, force: bool = False) -> int:
    data_root, db_path = paths_for(mode)
    landing_dir = data_root / "landing" / TRACK / "cpi_u"
    observations_raw = data_root / "raw" / TRACK / OBSERVATIONS_SOURCE
    series_raw = data_root / "raw" / TRACK / SERIES_SOURCE
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if mode == "fixture":
        fixtures = REPO_ROOT / "tests" / "fixtures" / TRACK
        observations_landing = fixtures / f"{OBSERVATIONS_SOURCE}.tsv"
        series_landing = fixtures / f"{SERIES_SOURCE}.tsv"
        for required in (observations_landing, series_landing):
            if not required.exists():
                log(f"FAIL: fixture missing at {required}")
                return 1
        metadata = json.loads((fixtures / "cpi_u.metadata.json").read_text(encoding="utf-8"))
        resolved = {
            OBSERVATIONS_FILE: {
                "name": observations_landing.name,
                "url": "fixture://" + observations_landing.name,
                "last_modified": metadata.get("last_modified"),
                "content_encoding": None,
            },
            SERIES_FILE: {
                "name": series_landing.name,
                "url": "fixture://" + series_landing.name,
                "last_modified": metadata.get("last_modified"),
                "content_encoding": None,
            },
        }
        landed = {
            OBSERVATIONS_FILE: observations_landing,
            SERIES_FILE: series_landing,
        }
        reported = metadata.get("api_latest")
        log("fixture mode: discovery, download and API cross-check SKIPPED (offline)")
    else:
        with http_client() as client:
            log(f"reading the file index at {CU_DIRECTORY}")
            available = parse_index(fetch_index(client))
            require_files(available, (OBSERVATIONS_FILE, SERIES_FILE))
            log(
                f"index lists {len(available)} cu.* files; taking "
                f"{OBSERVATIONS_FILE} and {SERIES_FILE}"
            )

            resolved = {
                name: resolve_file(client, name) for name in (OBSERVATIONS_FILE, SERIES_FILE)
            }
            fingerprint = {name: meta["last_modified"] for name, meta in resolved.items()}
            existing = read_existing_manifest(observations_raw)
            if not force and existing and existing.get("refresh_fingerprint") == fingerprint:
                log("SKIPPED (current): both files match the recorded last-modified")
                return 0

            write_baseline(
                TRACK,
                OBSERVATIONS_SOURCE,
                {
                    "note": "BLS documents its flat-file layout in cu.txt rather than as "
                    "machine-readable field metadata; this is a pointer file, per "
                    "CLAUDE.md rule 8.",
                    "field_reference": FIELD_REFERENCE,
                    "api_reference": API_REFERENCE,
                    "directory_index": CU_DIRECTORY,
                    "files_listed": available,
                    "files_landed": [OBSERVATIONS_FILE, SERIES_FILE],
                    "retrieved_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                },
                kind="pointer",
            )

            landing_dir.mkdir(parents=True, exist_ok=True)
            landed = {}
            for name, meta in resolved.items():
                target = landing_dir / name
                log(f"downloading {name}")
                size, digest = stream_download(client, meta["url"], target, log=log)
                meta["bytes"], meta["sha256"] = size, digest
                landed[name] = target
                log(f"  landed {human_bytes(size)}  sha256={digest[:16]}...")

            reported = fetch_api_latest(client)
            if reported:
                log(
                    f"API reports {DEFLATOR_SERIES_ID} {reported['year']} "
                    f"{reported['period']} = {reported['value']}"
                )

    retrieved_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    con = duckdb.connect(str(db_path))
    try:
        observations_relation = tsv_relation(landed[OBSERVATIONS_FILE])
        series_relation = tsv_relation(landed[SERIES_FILE])

        log(f"converting observations -> {observations_raw}")
        obs_landing, obs_raw = repartition_to_raw(
            con,
            relation=observations_relation,
            raw_dir=observations_raw,
            partition_column=OBSERVATIONS_PARTITION_COLUMN,
        )
        obs_counts = partition_counts(con, observations_raw, OBSERVATIONS_PARTITION_COLUMN)
        obs_table = load_table(con, OBSERVATIONS_TABLE, observations_raw)

        log(f"converting series definitions -> {series_raw}")
        ser_landing, ser_raw = repartition_to_raw(
            con,
            relation=series_relation,
            raw_dir=series_raw,
            partition_column=SERIES_PARTITION_COLUMN,
        )
        ser_counts = partition_counts(con, series_raw, SERIES_PARTITION_COLUMN)
        ser_table = load_table(con, SERIES_TABLE, series_raw)

        widths = [
            int(row[0])
            for row in con.execute(
                f"SELECT DISTINCT length(series_id) FROM {OBSERVATIONS_TABLE}"
            ).fetchall()
        ]
        periods = [
            str(row[0])
            for row in con.execute(f"SELECT DISTINCT period FROM {OBSERVATIONS_TABLE}").fetchall()
        ]
        series_observed = con.execute(
            f"SELECT count(DISTINCT trim(series_id)) FROM {OBSERVATIONS_TABLE}"
        ).fetchone()[0]
        undefined = [
            str(row[0])
            for row in con.execute(
                f"SELECT DISTINCT trim(o.series_id) FROM {OBSERVATIONS_TABLE} o "
                f"LEFT JOIN {SERIES_TABLE} s ON trim(o.series_id) = trim(s.series_id) "
                "WHERE s.series_id IS NULL"
            ).fetchall()
        ]
        crosscheck_landed = landed_value(con, reported)
    finally:
        con.close()

    problems = (
        padding_failures(widths)
        + annual_period_failures(periods)
        + definition_failures(undefined)
        + crosscheck_failures(crosscheck_landed, reported)
    )
    for problem in problems:
        log(f"FAIL  {problem}")
    if problems:
        return 1

    crosscheck = {
        "endpoint": API_SERIES,
        "reported": reported,
        "landed_value": None if crosscheck_landed is None else crosscheck_landed["value"],
        "agrees": reported is not None and crosscheck_landed is not None,
        "note": (
            "Unregistered API callers get three years of history, so the flat file is the "
            "source of record and this is a value-level agreement check only."
        ),
    }

    write_json(
        observations_raw / "manifest.json",
        {
            **observations_manifest_payload(
                landing_files=[
                    {
                        "name": resolved[OBSERVATIONS_FILE]["name"],
                        "bytes": resolved[OBSERVATIONS_FILE].get("bytes"),
                        "sha256": resolved[OBSERVATIONS_FILE].get("sha256"),
                    }
                ],
                resolved_url=resolved[OBSERVATIONS_FILE]["url"],
                last_modified=resolved[OBSERVATIONS_FILE]["last_modified"],
                rows_landing=obs_landing,
                rows_raw=obs_raw,
                rows_duckdb=obs_table,
                partition_rows=obs_counts,
                periods=periods,
                series_observed=series_observed,
                crosscheck=crosscheck,
                retrieved_at=retrieved_at,
            ),
            "refresh_fingerprint": {name: meta["last_modified"] for name, meta in resolved.items()},
        },
    )
    write_json(
        series_raw / "manifest.json",
        series_manifest_payload(
            landing_files=[
                {
                    "name": resolved[SERIES_FILE]["name"],
                    "bytes": resolved[SERIES_FILE].get("bytes"),
                    "sha256": resolved[SERIES_FILE].get("sha256"),
                }
            ],
            resolved_url=resolved[SERIES_FILE]["url"],
            last_modified=resolved[SERIES_FILE]["last_modified"],
            rows_landing=ser_landing,
            rows_raw=ser_raw,
            rows_duckdb=ser_table,
            partition_rows=ser_counts,
            series_observed=series_observed,
            retrieved_at=retrieved_at,
        ),
    )

    years = sorted(key for key in obs_counts if key.isdigit())
    log(
        f"observations: landing {obs_landing:,} -> raw {obs_raw:,} -> "
        f"{OBSERVATIONS_TABLE} {obs_table:,}"
    )
    log(f"series:       landing {ser_landing:,} -> raw {ser_raw:,} -> {SERIES_TABLE} {ser_table:,}")
    log(f"years {years[0]}..{years[-1]} across {len(years)} partitions")
    log(f"periods: {', '.join(sorted(periods))}")
    log(f"seasonal partitions: {', '.join(f'{k}={v:,}' for k, v in sorted(ser_counts.items()))}")
    log(f"{series_observed} series observed, all defined in {SERIES_FILE}")
    log(f"series_id padded to {widths[0]} characters -- trim() before any filter or join")
    if crosscheck["agrees"]:
        log(
            f"cross-check: {DEFLATOR_SERIES_ID} {reported['year']} {reported['period']} = "
            f"{crosscheck['landed_value']} in raw, matching the API"
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
        help="re-download even when both files match the recorded last-modified",
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
