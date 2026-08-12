"""Ingest the CMS Medicare Geographic Variation PUF into DuckDB's raw schema.

36,994 rows x 246 columns covering 2014-2024, discovered from `data.cms.gov/data.json`
and partitioned by geographic level. This is where the health track gets its Michigan
county grain: all 83 counties, every year, with per-capita standardized spending -- which
is what HLT-E3 compares against the state and national rows in the same file.

Not filtered to Michigan. The national and state rows are the benchmark, and they ship in
the same file as the counties -- which is also the main hazard here:

  * Three grains share one table, so the partition column is the grain. `BENE_GEO_LVL`
    holds County (35,146 rows), State (1,815) and National (33) as peers. Summing across
    it counts every beneficiary three times. Partitioning raw on that column puts the grain
    in the directory path, where it cannot be overlooked, rather than leaving it as a
    column somebody has to remember to filter.
  * Age level is a second such axis. `BENE_AGE_LVL` is 'All' for every county row, but
    state and national rows also appear as '<65' and '>=65' -- 1,232 extra rows that
    double count their own 'All'. Recorded in the manifest per level.
  * Michigan has an 84th county. `26000` / 'MI-UNKNOWN' carries beneficiaries whose county
    could not be assigned, one row per year. Those are real people and the row stays in
    raw; the run asserts the 83 real counties are present and names the unknown bucket
    separately, so a county-level denominator can decide about it deliberately.
  * `*` means suppressed, not zero and not null. CMS masks small cells, 639 of them in the
    per-capita standardized payment column alone. The all-varchar rule keeps the marker
    verbatim -- CLAUDE.md rule 2 -- and typing it is session 2's job.

The publisher exposes no row count for this file, so there is no source-to-landing count
to reconcile. What stands in for it: unbroken year coverage, all three geographic levels
present, and the full Michigan county roster.

One transport note, because it broke the shared downloader: CMS serves this CSV gzipped,
with `content-length` describing the compressed 24.9 MB rather than the 57.9 MB of CSV
that arrives. See the encoding guard in `ingest.common.stream_download`.

Run:  python -m ingest.health.cms [--mode live|fixture]
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

TRACK = "health"
SOURCE = "cms_geographic_variation"
TABLE = "raw.hlt_cms_geographic_variation"
PARTITION_COLUMN = "BENE_GEO_LVL"

DCAT_CATALOG = "https://data.cms.gov/data.json"
DATASET_TITLE = "Medicare Geographic Variation - by National, State & County"

# The grains the publisher ships together, and the one Michigan analysis needs.
GEO_LEVELS = ("National", "State", "County")
MICHIGAN_STATE_FIPS = "26"
# Michigan has 83 counties. A missing one would silently shrink every county denominator,
# so the roster is asserted rather than assumed.
MICHIGAN_COUNTY_COUNT = 83
UNKNOWN_COUNTY_SUFFIX = "000"

FIELD_REFERENCE = (
    "https://data.cms.gov/summary-statistics-on-use-and-payments/"
    "medicare-geographic-comparisons/medicare-geographic-variation-by-national-state-county"
)

log = make_logger(TRACK, SOURCE)


# ── pure helpers (unit-tested without a network) ───────────────────────────────


def select_dataset(datasets: list[dict[str, Any]], title: str = DATASET_TITLE) -> dict[str, Any]:
    """Resolve the dataset from CMS's DCAT catalogue by exact title.

    The catalogue carries 159 datasets and several near neighbours -- Medicare *Advantage*
    Geographic Variation is a different population, and the Hospital Referral Region file a
    different geography. Requiring an exact title, and exactly one match, is what keeps a
    plausible-looking substitute from landing in the health track.
    """
    matches = [dataset for dataset in datasets if (dataset.get("title") or "").strip() == title]
    if len(matches) != 1:
        near = sorted(
            (dataset.get("title") or "")
            for dataset in datasets
            if "geographic variation" in (dataset.get("title") or "").lower()
        )
        raise DiscoveryError(
            f"Expected exactly one catalogue entry titled {title!r}, got {len(matches)}. "
            f"Geographic-variation titles offered: {near}"
        )
    return matches[0]


def select_csv_distribution(dataset: dict[str, Any]) -> tuple[str, str]:
    """Take the bulk CSV, never the API distribution.

    CMS lists the same dataset three times: two `API` distributions pointing at paged
    data-viewer endpoints and one `CSV` holding every year in a single file. We want the
    file. Fail loudly rather than fall back to a paged API that would need its own
    completeness proof.
    """
    available: dict[str, str] = {}
    for distribution in dataset.get("distribution") or []:
        fmt = (distribution.get("format") or distribution.get("mediaType") or "").strip().lower()
        url = distribution.get("downloadURL") or distribution.get("accessURL")
        if fmt and url:
            available.setdefault(fmt, url)

    if "csv" not in available:
        raise DiscoveryError(
            f"No CSV distribution for {dataset.get('title')!r}. Offered: {sorted(available)}. "
            f"Raw distribution block: {json.dumps(dataset.get('distribution'))[:1500]}"
        )
    return "csv", available["csv"]


def landing_name(url: str) -> str:
    """CMS embeds the year range in the filename, which is worth keeping as landed."""
    return url.rsplit("/", 1)[-1].replace("%20", " ")


def source_relation(landing: Path) -> str:
    """The all-varchar rule. `*` means suppressed and has to survive as `*`."""
    return f"read_csv({sql_literal(landing)}, all_varchar=true, header=true)"


def year_coverage_failures(years: list[str]) -> list[str]:
    """An annual file with a hole in it would show as a trend, not as an error."""
    numeric = sorted(year for year in years if year.isdigit())
    if not numeric:
        return ["no usable YEAR values in the file"]
    expected = {str(year) for year in range(int(numeric[0]), int(numeric[-1]) + 1)}
    if gaps := sorted(expected - set(numeric)):
        return [f"missing years {gaps} between {numeric[0]} and {numeric[-1]}"]
    return []


def geo_level_failures(levels: list[str]) -> list[str]:
    """All three grains must arrive; a missing National row removes the benchmark."""
    if missing := sorted(set(GEO_LEVELS) - set(levels)):
        return [f"missing geographic levels {missing}: the benchmark rows did not land"]
    return []


def split_michigan_counties(codes: list[str]) -> tuple[list[str], list[str]]:
    """Separate the real county roster from the unassigned-county bucket."""
    michigan = sorted(code for code in codes if code.startswith(MICHIGAN_STATE_FIPS))
    unknown = [code for code in michigan if code.endswith(UNKNOWN_COUNTY_SUFFIX)]
    return [code for code in michigan if code not in unknown], unknown


def michigan_failures(codes: list[str]) -> list[str]:
    """The Michigan county lens is the point of this source; prove the roster is whole."""
    real, _ = split_michigan_counties(codes)
    if len(real) != MICHIGAN_COUNTY_COUNT:
        return [
            f"{len(real)} Michigan county codes present, expected {MICHIGAN_COUNTY_COUNT}: "
            f"every county-level denominator in this track would be wrong"
        ]
    return []


def manifest_payload(
    *,
    dataset: dict[str, Any],
    resolved_url: str,
    landing_file: str,
    landing_bytes: int,
    landing_sha256: str,
    http_metadata: dict[str, Any],
    rows_landing: int,
    rows_raw: int,
    rows_duckdb: int,
    partition_rows: dict[str, int],
    rows_by_age_level: dict[str, int],
    years: list[str],
    michigan_counties: list[str],
    michigan_unknown: list[str],
    columns: int,
    retrieved_at: str,
) -> dict[str, Any]:
    return {
        **base_manifest(
            track=TRACK,
            source=SOURCE,
            publisher="CMS",
            dataset_name=dataset.get("title"),
            resolved_url=resolved_url,
            distribution_format="csv",
            retrieved_at=retrieved_at,
            landing_files=[
                {"name": landing_file, "bytes": landing_bytes, "sha256": landing_sha256}
            ],
            # CMS publishes no row count for this file; see below.
            source_reported_count=None,
            source_last_refresh=dataset.get("modified"),
            duckdb_table=TABLE,
            rows_landing=rows_landing,
            rows_raw=rows_raw,
            rows_duckdb=rows_duckdb,
            partition_column=PARTITION_COLUMN,
            partition_rows=partition_rows,
            # Every partition key is one of the publisher's three documented grains, or
            # the run already failed.
            nonstandard_partitions={},
        ),
        "dataset_identifier": dataset.get("identifier"),
        "discovered_from": DCAT_CATALOG,
        "source_count_unavailable_reason": (
            "CMS's DCAT entry carries no row count and the bulk file has no manifest, so "
            "there is no publisher total to reconcile against. Year coverage, geographic "
            "level coverage and the Michigan county roster stand in for it."
        ),
        "dcat": {
            "modified": dataset.get("modified"),
            "temporal": dataset.get("temporal"),
            "accrual_periodicity": dataset.get("accrualPeriodicity"),
            "temporal_note": (
                "DCAT `temporal` names only the newest reporting year while the file itself "
                "holds every year back to 2014. Trust the data, not the metadata window."
            ),
            "other_distributions": [
                {"format": d.get("format"), "url": d.get("downloadURL") or d.get("accessURL")}
                for d in (dataset.get("distribution") or [])
                if (d.get("format") or "").strip().lower() != "csv"
            ],
        },
        "http_metadata": http_metadata,
        "columns": columns,
        "grain": {
            "levels": partition_rows,
            "note": (
                "One row per geography x year x age level. A row is a geographic aggregate, "
                "not a beneficiary, and the three levels are peers in one file: summing "
                "across BENE_GEO_LVL counts every beneficiary three times."
            ),
            "age_levels": rows_by_age_level,
            "age_level_note": (
                "County rows are 'All' only; state and national rows also appear as '<65' "
                "and '>=65', which double count their own 'All'. Filter on one age level."
            ),
        },
        "years": sorted(years),
        "michigan": {
            "state_fips": MICHIGAN_STATE_FIPS,
            "counties_present": len(michigan_counties),
            "counties_expected": MICHIGAN_COUNTY_COUNT,
            "unknown_county_codes": michigan_unknown,
            "unknown_county_note": (
                "26000 / 'MI-UNKNOWN' holds beneficiaries whose county could not be "
                "assigned -- one row per year. Real people, kept in raw. A county-level "
                "denominator either includes them explicitly or states that it does not."
            ),
        },
        "coverage_caveats": [
            "A row is a geographic aggregate, never a beneficiary. Nothing here sums to a "
            "patient count.",
            "`*` marks a suppressed small cell. It is not zero and not null, and it stays "
            "as the literal '*' in raw.",
            "Original Medicare only: beneficiaries in Medicare Advantage are counted in "
            "BENES_MA_CNT but their utilization and spending are not in this file, so "
            "per-capita spend is per *FFS* beneficiary. MA participation varies by county, "
            "which makes raw county comparisons misleading without it.",
            "Standardized payment columns remove geographic wage adjustment and are the "
            "right ones for comparing places; unstandardized ones are not.",
            "This file carries no condition-level detail. There is no dementia or "
            "chronic-condition prevalence here -- only PQI ambulatory-care-sensitive "
            "admission rates for diabetes, COPD, hypertension, CHF, pneumonia, UTI, "
            "asthma and amputation.",
            "Annual grain, one file per publication covering every year. Never present it "
            "at a finer cadence than annual.",
        ],
    }


# ── network ───────────────────────────────────────────────────────────────────


@retrying
def fetch_catalog(client: httpx.Client) -> list[dict[str, Any]]:
    """CMS's DCAT catalogue: 3 MB of JSON describing every dataset it publishes."""
    response = client.get(DCAT_CATALOG)
    raise_for_transient(response)
    datasets = response.json().get("dataset")
    if not datasets:
        raise DiscoveryError(
            f"No `dataset` array in {DCAT_CATALOG}. Raw response: {response.text[:1000]}"
        )
    return datasets


@retrying
def head_metadata(client: httpx.Client, url: str) -> dict[str, Any]:
    """The file's own transport metadata, including the compression that fooled us once."""
    response = client.head(url)
    raise_for_transient(response)
    declared_length = response.headers.get("content-length")
    return {
        # This CDN answers HEAD with the encoding but no length, so the size is genuinely
        # unknown until the body arrives. Recorded as null rather than as a zero.
        "content_length": int(declared_length) if declared_length else None,
        "content_encoding": response.headers.get("content-encoding"),
        "last_modified": response.headers.get("last-modified"),
        "etag": response.headers.get("etag"),
        "content_length_note": (
            "This CDN's HEAD length is not usable: it answered 20 bytes for a 57.9 MB "
            "file. Recorded as observed, never used as a size. On the streaming GET, "
            "content-length describes the compressed body while we count decoded bytes, "
            "so landing_files[].bytes is the only real size here."
        ),
    }


# ── orchestration ─────────────────────────────────────────────────────────────


def profile(con: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """One pass over the loaded table for every coverage check the manifest records."""
    return {
        "years": [row[0] for row in con.execute(f"SELECT DISTINCT YEAR FROM {TABLE}").fetchall()],
        "levels": [
            row[0]
            for row in con.execute(f"SELECT DISTINCT {PARTITION_COLUMN} FROM {TABLE}").fetchall()
        ],
        "age_levels": {
            str(row[0]): int(row[1])
            for row in con.execute(
                f"SELECT BENE_AGE_LVL, count(*) FROM {TABLE} GROUP BY 1 ORDER BY 1"
            ).fetchall()
        },
        "michigan_codes": [
            row[0]
            for row in con.execute(
                f"SELECT DISTINCT BENE_GEO_CD FROM {TABLE} "
                f"WHERE {PARTITION_COLUMN} = 'County' "
                f"AND BENE_GEO_CD LIKE '{MICHIGAN_STATE_FIPS}%'"
            ).fetchall()
        ],
    }


def run(mode: str, force: bool = False) -> int:
    data_root, db_path = paths_for(mode)
    landing_dir = data_root / "landing" / TRACK / SOURCE
    raw_dir = data_root / "raw" / TRACK / SOURCE
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if mode == "fixture":
        fixtures = REPO_ROOT / "tests" / "fixtures" / TRACK
        landing = fixtures / f"{SOURCE}.csv"
        if not landing.exists():
            log(f"FAIL: fixture missing at {landing}")
            return 1
        dataset = json.loads((fixtures / f"{SOURCE}.metadata.json").read_text(encoding="utf-8"))
        resolved_url = "fixture://" + landing.name
        http_metadata = {"content_length": landing.stat().st_size, "content_encoding": None}
        landing_bytes, landing_sha256 = landing.stat().st_size, sha256_of(landing)
        log("fixture mode: discovery and download SKIPPED (offline)")
    else:
        with http_client(read_timeout=600.0) as client:
            log(f"discovering {DATASET_TITLE!r} from {DCAT_CATALOG}")
            datasets = fetch_catalog(client)
            dataset = select_dataset(datasets)
            _, resolved_url = select_csv_distribution(dataset)
            log(f"catalogue holds {len(datasets)} datasets; resolved CSV: {resolved_url}")
            log(
                f"publisher reports modified {dataset.get('modified')}, "
                f"temporal {dataset.get('temporal')}, "
                f"periodicity {dataset.get('accrualPeriodicity')}"
            )

            existing = read_existing_manifest(raw_dir)
            if skip_as_current(
                force=force,
                publisher_unchanged=bool(existing)
                and existing.get("source_last_refresh") == dataset.get("modified"),
                db_path=db_path,
                tables=(TABLE,),
                log=log,
            ):
                log("SKIPPED (current): manifest matches the catalogue's modified date")
                return 0

            http_metadata = head_metadata(client, resolved_url)
            if http_metadata["content_encoding"]:
                log(
                    f"served {http_metadata['content_encoding']}-encoded, and this CDN's "
                    f"HEAD reports content-length {http_metadata['content_length']} for it: "
                    "the landed byte count is the only real size"
                )

            write_baseline(
                TRACK,
                SOURCE,
                {
                    "note": "CMS publishes the GV data dictionary as a PDF alongside the "
                    "file rather than as machine-readable field metadata; this is a "
                    "pointer file, per CLAUDE.md rule 8.",
                    "field_reference": FIELD_REFERENCE,
                    "dcat_catalog": DCAT_CATALOG,
                    "dataset_title": dataset.get("title"),
                    "dataset_identifier": dataset.get("identifier"),
                    "dcat_modified": dataset.get("modified"),
                    "retrieved_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                },
                kind="pointer",
            )

            landing = landing_dir / landing_name(resolved_url)
            log(f"downloading -> {landing.name}")
            landing_bytes, landing_sha256 = stream_download(client, resolved_url, landing, log=log)
            log(f"landed {human_bytes(landing_bytes)}  sha256={landing_sha256[:16]}...")

    con = duckdb.connect(str(db_path))
    try:
        relation = source_relation(landing)
        columns = len(con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall())
        log(f"{columns} columns; partitioning on {PARTITION_COLUMN} so the grain is the path")

        rows_landing, rows_raw = repartition_to_raw(
            con, relation=relation, raw_dir=raw_dir, partition_column=PARTITION_COLUMN
        )
        counts = partition_counts(con, raw_dir, PARTITION_COLUMN)
        rows_duckdb = load_table(con, TABLE, raw_dir)
        observed = profile(con)
    finally:
        con.close()

    problems = (
        year_coverage_failures(observed["years"])
        + geo_level_failures(observed["levels"])
        + michigan_failures(observed["michigan_codes"])
    )
    for problem in problems:
        log(f"FAIL  {problem}")
    if problems:
        return 1

    michigan_counties, michigan_unknown = split_michigan_counties(observed["michigan_codes"])
    manifest = manifest_payload(
        dataset=dataset,
        resolved_url=resolved_url,
        landing_file=landing.name,
        landing_bytes=landing_bytes,
        landing_sha256=landing_sha256,
        http_metadata=http_metadata,
        rows_landing=rows_landing,
        rows_raw=rows_raw,
        rows_duckdb=rows_duckdb,
        partition_rows=counts,
        rows_by_age_level=observed["age_levels"],
        years=observed["years"],
        michigan_counties=michigan_counties,
        michigan_unknown=michigan_unknown,
        columns=columns,
        retrieved_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    )
    write_json(raw_dir / "manifest.json", manifest)

    years = sorted(year for year in observed["years"] if year.isdigit())
    log(f"landing {rows_landing:,} -> raw {rows_raw:,} -> {TABLE} {rows_duckdb:,}")
    log(f"years {years[0]}..{years[-1]} unbroken ({len(years)} annual cycles)")
    log(f"grains kept separate: {', '.join(f'{k}={v:,}' for k, v in sorted(counts.items()))}")
    log(f"age levels: {', '.join(f'{k}={v:,}' for k, v in observed['age_levels'].items())}")
    log(
        f"michigan counties: {len(michigan_counties)} of {MICHIGAN_COUNTY_COUNT} "
        f"plus unassigned {michigan_unknown}"
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
        help="re-download even when the manifest matches the catalogue's modified date",
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
