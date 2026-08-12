"""Ingest the CDC Alzheimer's Disease and Healthy Aging Data into DuckDB's raw schema.

284,142 rows of BRFSS-derived indicator cells, 2015-2022, partitioned by location. Full
dataset, no Michigan filter -- the MI lens is applied in marts, and the national and
regional rows are the benchmark HLT-E1 needs. See CLAUDE.md section 4.

A row is a *cell* -- indicator x location x year x age group x a second stratification --
not a person. Nothing here sums to a patient count. See the health README's first warning.

That second stratification is not one dimension. `stratification2` holds race/ethnicity
values (178,431 rows), sex values (68,838) and the literal `OVERALL` (36,873) in the same
column, and `stratificationcategory2` -- the column that says which of the three you are
looking at -- is NULL for exactly the OVERALL rows. Filtering on `stratification2` without
reading its category mixes a race breakdown with a sex breakdown and an ungrouped total.

Five things about this dataset that the retrieval strategy and the manifest are built
around, all established by probing the API:

  * `rowid` is not a row id. The publisher ships a column named `rowid` whose values look
    like keys (`BRFSS~2017~2017~9001~Q14~TSC06~AGE~RACE`) and are not: 284,142 rows carry
    only 36,046 distinct values, up to 15 rows each. It encodes the stratification
    *categories* and omits the stratification *values*, so the age group and the
    race/sex breakdown are exactly what it cannot distinguish. A session-2 uniqueness test
    on `rowid` fails and a join on it fans out 15 to 1. The real grain is `GRAIN_COLUMNS`
    below, verified unique across the whole dataset on every run.
  * States, regions and the nation share one column. `locationabbr` holds 51 states and
    DC, three territories, four census regions (MDW, NRE, SOU, WEST) and a US row, all
    peers. Summing across it double counts every state up to three times. The rollups are
    kept -- HLT-E1 compares Michigan against them -- and named in the manifest so nothing
    downstream can mistake them for states.
  * Offset paging is the only deterministic option, so the pull is bracketed instead.
    Socrata pages with `$limit`/`$offset` and needs an explicit `$order` to be stable at
    all; `$order=:id` is stable across repeat requests. What offset paging cannot rule out
    is the dataset being republished halfway through, so `rowsUpdatedAt` is read before
    and after and compared, alongside the row count and grain-key uniqueness. An
    over-the-end offset returns HTTP 200 with a header and no rows, which is exactly why
    the loop is driven by the publisher's own count rather than by an empty-page check.
  * A third of the cells have no value. 91,334 rows carry an empty `data_value` with a
    footnote symbol explaining why -- `****` sample size too small to age-standardize,
    `~` no data available, `#` fewer than 50 states reporting, `&` regional estimate may
    not represent every state in the region, `**` not comparable to earlier years. Those
    symbols are information, not nulls, and the all-varchar rule keeps them verbatim.

Run:  python -m ingest.health.cdc [--mode live|fixture]
"""

from __future__ import annotations

import argparse
import json
import math
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
    human_bytes,
    load_state_codes,
    load_table,
    make_logger,
    nonstandard_partitions,
    partition_counts,
    paths_for,
    raise_for_transient,
    read_existing_manifest,
    repartition_to_raw,
    retrying,
    sha256_of,
    skip_as_current,
    sql_literal,
    write_baseline,
    write_json,
)

TRACK = "health"
SOURCE = "cdc_healthy_aging"
TABLE = "raw.hlt_cdc_healthy_aging"
PARTITION_COLUMN = "locationabbr"

DOMAIN = "data.cdc.gov"
DATASET_NAME = "Alzheimer's Disease and Healthy Aging Data"
CATALOG_API = f"https://{DOMAIN}/api/catalog/v1"
VIEWS_API = f"https://{DOMAIN}/api/views"
RESOURCE_API = f"https://{DOMAIN}/resource"

# Socrata serves 50k rows per CSV page comfortably (~24 MB); six pages covers the dataset.
PAGE_SIZE = 50_000
LANDING_GLOB = "page-*.csv"

# The columns that actually identify a cell, as opposed to the `rowid` column that claims
# to. Verified unique across all 284,142 rows: the same count of rows and of keys.
GRAIN_COLUMNS = (
    "datasource",
    "yearstart",
    "yearend",
    "locationabbr",
    "questionid",
    "datavaluetypeid",
    "stratificationid1",
    "stratificationid2",
)

FIELD_REFERENCE = "https://www.cdc.gov/aging/agingdata/index.html"
API_REFERENCE = "https://dev.socrata.com/foundry/data.cdc.gov/hfr9-rurv"

log = make_logger(TRACK, SOURCE)


# ── pure helpers (unit-tested without a network) ───────────────────────────────


def select_dataset(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Resolve the dataset from the publisher's catalogue. Never hardcode the four-by-four.

    The search returns near-misses that would each break something different: derived
    `filter` views that carry a subset of rows, and an `href` entry on another HHS domain
    with the identical name that is a link rather than a dataset. Requiring an exact name
    on this domain with type `dataset`, and exactly one of them, is what makes the id
    trustworthy enough to build a URL from.
    """
    matches = [
        result["resource"]
        for result in results
        if (result.get("resource") or {}).get("name") == DATASET_NAME
        and (result.get("resource") or {}).get("type") == "dataset"
        and (result.get("metadata") or {}).get("domain") == DOMAIN
    ]
    if len(matches) != 1:
        offered = [
            f"{(r.get('resource') or {}).get('id')}"
            f" [{(r.get('resource') or {}).get('type')}"
            f" @ {(r.get('metadata') or {}).get('domain')}]"
            f" {(r.get('resource') or {}).get('name')!r}"
            for r in results
        ]
        raise DiscoveryError(
            f"Expected exactly one dataset named {DATASET_NAME!r} of type 'dataset' on "
            f"{DOMAIN}, got {len(matches)}. Catalogue offered: {offered}"
        )
    return matches[0]


def epoch_to_iso(epoch: int | float | None) -> str | None:
    """Socrata publishes its refresh timestamps as Unix epoch seconds."""
    if epoch is None:
        return None
    return datetime.fromtimestamp(int(epoch), UTC).isoformat().replace("+00:00", "Z")


def expected_pages(total: int, page_size: int = PAGE_SIZE) -> int:
    """The loop is driven by the publisher's count, not by waiting for an empty page.

    An offset past the end returns HTTP 200 with a header row and no data, so an
    empty-page check is both available and insufficient -- CLAUDE.md rule 6.
    """
    if total <= 0:
        raise DiscoveryError(f"Publisher reported {total} rows; refusing to page nothing.")
    return math.ceil(total / page_size)


def verify_pull(
    *,
    page_rows: list[int],
    reported_total: int,
    refresh_before: str | None,
    refresh_after: str | None,
    page_size: int = PAGE_SIZE,
) -> dict[str, Any]:
    """Everything we can assert about an offset-paged pull without re-reading the source."""
    return {
        "pages": len(page_rows),
        "rows": sum(page_rows),
        "reported_total": reported_total,
        "rows_match_reported": sum(page_rows) == reported_total,
        "pages_full_except_last": all(rows == page_size for rows in page_rows[:-1])
        if page_rows
        else False,
        "final_page_rows": page_rows[-1] if page_rows else 0,
        "refresh_before": refresh_before,
        "refresh_after": refresh_after,
        "refresh_stable_during_pull": refresh_before == refresh_after,
    }


def pull_failures(verification: dict[str, Any]) -> list[str]:
    problems = []
    if not verification["rows_match_reported"]:
        problems.append(
            f"landed {verification['rows']:,} rows against a reported "
            f"{verification['reported_total']:,}: the pull is short or doubled"
        )
    if not verification["pages_full_except_last"]:
        problems.append("a non-final page came back short: the server truncated a page")
    if not verification["refresh_stable_during_pull"]:
        problems.append(
            f"the dataset was republished mid-pull (rowsUpdatedAt moved "
            f"{verification['refresh_before']} -> {verification['refresh_after']}); "
            "offset paging cannot be trusted across a republication"
        )
    return problems


def grain_failures(rows: int, distinct_keys: int) -> list[str]:
    """The publisher's `rowid` is not unique; this is the key that is."""
    if rows == distinct_keys:
        return []
    return [
        f"{rows - distinct_keys:,} rows share a grain key "
        f"({' + '.join(GRAIN_COLUMNS)}): pages overlapped, or the grain has changed"
    ]


def source_relation(landing_pattern: Path) -> str:
    """The all-varchar rule. Suppression footnotes and blank estimates must survive raw.

    `parallel=true` is left at its default deliberately: no footnote or geolocation value
    in this dataset contains a newline, so the range-splitting reader is safe here -- and
    the row count DuckDB reports is cross-checked against the publisher's own count, which
    is what would catch it if that ever changed.
    """
    return f"read_csv({sql_literal(landing_pattern)}, all_varchar=true, header=true)"


def grain_key_sql() -> str:
    """A struct rather than a concatenation, so no separator can collide with a value."""
    return "(" + ", ".join(GRAIN_COLUMNS) + ")"


def manifest_payload(
    *,
    dataset: dict[str, Any],
    view: dict[str, Any],
    landing_files: list[dict[str, Any]],
    verification: dict[str, Any],
    distinct_grain_keys: int,
    rows_landing: int,
    rows_raw: int,
    rows_duckdb: int,
    partition_rows: dict[str, int],
    retrieved_at: str,
) -> dict[str, Any]:
    rollups = nonstandard_partitions(partition_rows, load_state_codes())
    return {
        **base_manifest(
            track=TRACK,
            source=SOURCE,
            publisher="CDC Division of Population Health",
            dataset_name=view.get("name") or DATASET_NAME,
            resolved_url=f"{RESOURCE_API}/{dataset['id']}.csv",
            distribution_format="csv",
            retrieved_at=retrieved_at,
            landing_files=landing_files,
            source_reported_count=verification["reported_total"],
            source_last_refresh=epoch_to_iso(view.get("rowsUpdatedAt")),
            duckdb_table=TABLE,
            rows_landing=rows_landing,
            rows_raw=rows_raw,
            rows_duckdb=rows_duckdb,
            partition_column=PARTITION_COLUMN,
            partition_rows=partition_rows,
            # Not anomalies: the national and census-region rollups the publisher ships
            # alongside states. Named here so nothing downstream sums across them.
            nonstandard_partitions=rollups,
        ),
        "dataset_identifier": dataset["id"],
        "discovered_from": CATALOG_API,
        "socrata_rows_updated_at": view.get("rowsUpdatedAt"),
        "socrata_view_last_modified": epoch_to_iso(view.get("viewLastModified")),
        "attribution": view.get("attribution"),
        "category": view.get("category"),
        "columns_in_view_metadata": len(view.get("columns") or []),
        "columns_in_data": None,  # filled by the caller once DuckDB has described the read
        "grain": {
            "columns": list(GRAIN_COLUMNS),
            "distinct_keys": distinct_grain_keys,
            "note": (
                "One row per indicator x location x year x age group x second "
                "stratification -- a cell, not a person. Nothing in this file sums to a "
                "patient count."
            ),
            "second_stratification_is_mixed": (
                "`stratification2` holds race/ethnicity (178,431 rows), sex (68,838) and "
                "the literal 'OVERALL' (36,873) in one column, and "
                "`stratificationcategory2` is NULL for exactly the OVERALL rows. Any "
                "model that groups on stratification2 must read the category with it, or "
                "it will average a race breakdown against a sex breakdown."
            ),
            "rowid_is_not_a_key": (
                "The publisher's `rowid` column is not unique: 284,142 rows carry 36,046 "
                "distinct values, up to 15 rows each, because it encodes the "
                "stratification categories and not their values. Do not test it for "
                "uniqueness and do not join on it."
            ),
        },
        "rollup_rows": {
            "keys": sorted(rollups),
            "rows": sum(rollups.values()),
            "note": (
                "`locationabbr` mixes states, territories, four census regions and a "
                "national US row as peers. Summing across it double counts every state. "
                "The rollups are retained because HLT-E1 benchmarks Michigan against them."
            ),
        },
        "pagination": {
            "strategy": "offset",
            "order_by": ":id",
            "page_size": PAGE_SIZE,
            "driven_by": (
                "the publisher's own count, not an empty-page check: an over-the-end "
                "offset returns HTTP 200 with a header row and no data"
            ),
        },
        "verification": verification,
        "landing_glob": LANDING_GLOB,
        "coverage_caveats": [
            "State-level and above only. This dataset has no county grain, so Michigan "
            "county analysis in this track comes from the CMS sources, not from here.",
            "A third of the cells (91,334 rows) have an empty data_value with a footnote "
            "symbol giving the reason. Suppression is information, not a null.",
            "Footnote symbols observed: '****' sample size too small to age-standardize, "
            "'~' no data available, '#' fewer than 50 states reporting, '&' regional "
            "estimate may not represent every state in the region, '**' not comparable to "
            "estimates from earlier years.",
            "BRFSS is a self-reported telephone survey with a survey-cycle grain. Annual "
            "and cycle grain only -- never interpolate between cycles.",
            "Confidence limits are published per cell; HLT-E1 comparisons must use them "
            "rather than ranking point estimates.",
        ],
    }


# ── network ───────────────────────────────────────────────────────────────────


@retrying
def _get(client: httpx.Client, url: str, params: dict[str, str] | None = None) -> httpx.Response:
    response = client.get(url, params=params)
    raise_for_transient(response)
    return response


def discover_dataset(client: httpx.Client) -> dict[str, Any]:
    """Ask the catalogue which four-by-four holds this dataset."""
    response = _get(client, CATALOG_API, {"q": DATASET_NAME, "limit": "20"})
    body = response.json()
    results = body.get("results")
    if not results:
        raise DiscoveryError(
            f"Catalogue returned no results for {DATASET_NAME!r}. "
            f"Raw response: {response.text[:1000]}"
        )
    return select_dataset(results)


def fetch_view(client: httpx.Client, dataset_id: str) -> dict[str, Any]:
    """The views API: column metadata for the baseline, and the refresh timestamp."""
    view = _get(client, f"{VIEWS_API}/{dataset_id}.json").json()
    if not view.get("columns"):
        raise DiscoveryError(
            f"No column metadata for {dataset_id}; refusing a blank schema baseline."
        )
    return view


def fetch_row_count(client: httpx.Client, dataset_id: str) -> int:
    """The publisher's own row count, which drives the paging loop."""
    response = _get(client, f"{RESOURCE_API}/{dataset_id}.json", {"$select": "count(1)"})
    payload = response.json()
    try:
        return int(payload[0]["count_1"])
    except (IndexError, KeyError, TypeError, ValueError) as error:
        raise DiscoveryError(
            f"Unexpected count shape ({error}). Raw response: {response.text[:800]}"
        ) from error


def download_pages(
    client: httpx.Client, dataset_id: str, total: int, landing_dir: Path
) -> tuple[list[dict[str, Any]], list[int]]:
    """Page the resource endpoint in a deterministic order, landing each page untouched.

    Per-page row counts come from counting lines, which is safe on this dataset because no
    value contains a newline -- and DuckDB's own count of the landed files is compared
    against the publisher's total afterwards, so the assumption is checked rather than
    trusted.
    """
    landing_dir.mkdir(parents=True, exist_ok=True)
    for stale in landing_dir.glob(LANDING_GLOB):
        stale.unlink()

    landing_files: list[dict[str, Any]] = []
    page_rows: list[int] = []

    for page in range(expected_pages(total)):
        offset = page * PAGE_SIZE
        started = time.monotonic()
        response = _get(
            client,
            f"{RESOURCE_API}/{dataset_id}.csv",
            {"$order": ":id", "$limit": str(PAGE_SIZE), "$offset": str(offset)},
        )
        elapsed = time.monotonic() - started

        target = landing_dir / f"page-{page + 1:05d}.csv"
        target.write_bytes(response.content)
        rows = max(len(response.text.splitlines()) - 1, 0)
        landing_files.append(
            {"name": target.name, "bytes": len(response.content), "sha256": sha256_of(target)}
        )
        page_rows.append(rows)
        log(
            f"  page {page + 1}/{expected_pages(total)}: {rows:,} rows in {elapsed:.1f}s "
            f"({human_bytes(len(response.content))}, offset={offset:,})"
        )
        time.sleep(PAGE_SLEEP_SECONDS)

    return landing_files, page_rows


# ── orchestration ─────────────────────────────────────────────────────────────


def run(mode: str, force: bool = False) -> int:
    data_root, db_path = paths_for(mode)
    landing_dir = data_root / "landing" / TRACK / SOURCE
    raw_dir = data_root / "raw" / TRACK / SOURCE
    db_path.parent.mkdir(parents=True, exist_ok=True)

    if mode == "fixture":
        fixtures = REPO_ROOT / "tests" / "fixtures" / TRACK
        landing_pattern = fixtures / f"{SOURCE}.csv"
        if not landing_pattern.exists():
            log(f"FAIL: fixture missing at {landing_pattern}")
            return 1
        view = json.loads((fixtures / f"{SOURCE}.metadata.json").read_text(encoding="utf-8"))
        dataset = {"id": view["id"]}
        reported_total = int(view["reported_total"])
        landing_files = [
            {
                "name": landing_pattern.name,
                "bytes": landing_pattern.stat().st_size,
                "sha256": sha256_of(landing_pattern),
            }
        ]
        # One fixture file stands in for the paged pull; the invariants below still run.
        page_rows = [reported_total]
        refresh_before = refresh_after = epoch_to_iso(view.get("rowsUpdatedAt"))
        page_size = reported_total
        log("fixture mode: discovery and paging SKIPPED (offline)")
    else:
        with http_client() as client:
            log(f"discovering {DATASET_NAME!r} from {CATALOG_API}")
            dataset = discover_dataset(client)
            log(f"resolved dataset id {dataset['id']} (updated {dataset.get('updatedAt')})")

            view = fetch_view(client, dataset["id"])
            refresh_before = epoch_to_iso(view.get("rowsUpdatedAt"))
            reported_total = fetch_row_count(client, dataset["id"])
            log(
                f"source reports {reported_total:,} rows, "
                f"{len(view['columns'])} columns, rows updated {refresh_before}"
            )

            existing = read_existing_manifest(raw_dir)
            if skip_as_current(
                force=force,
                publisher_unchanged=bool(existing)
                and existing.get("source_last_refresh") == refresh_before,
                db_path=db_path,
                tables=(TABLE,),
                log=log,
            ):
                log("SKIPPED (current): manifest matches the publisher's rowsUpdatedAt")
                return 0

            baseline = write_baseline(TRACK, SOURCE, view["columns"])
            log(f"schema baseline: {len(view['columns'])} columns -> {baseline.name}")

            log(f"paging {RESOURCE_API}/{dataset['id']}.csv at {PAGE_SIZE:,} rows/page")
            landing_files, page_rows = download_pages(
                client, dataset["id"], reported_total, landing_dir
            )

            # Offset paging's one real risk: a republication between page 1 and page 6.
            refresh_after = epoch_to_iso(fetch_view(client, dataset["id"]).get("rowsUpdatedAt"))

        landing_pattern = landing_dir / LANDING_GLOB
        page_size = PAGE_SIZE

    verification = verify_pull(
        page_rows=page_rows,
        reported_total=reported_total,
        refresh_before=refresh_before,
        refresh_after=refresh_after,
        page_size=page_size,
    )
    problems = pull_failures(verification)
    if problems:
        for problem in problems:
            log(f"FAIL  {problem}")
        return 1
    log(
        f"paging verified: {verification['rows']:,} rows over {verification['pages']} page(s) "
        f"= the reported total, refresh timestamp unchanged during the pull"
    )

    con = duckdb.connect(str(db_path))
    try:
        relation = source_relation(landing_pattern)
        columns_in_data = len(con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall())

        log(f"converting to raw parquet, partitioned by {PARTITION_COLUMN} -> {raw_dir}")
        rows_landing, rows_raw = repartition_to_raw(
            con, relation=relation, raw_dir=raw_dir, partition_column=PARTITION_COLUMN
        )
        counts = partition_counts(con, raw_dir, PARTITION_COLUMN)
        rows_duckdb = load_table(con, TABLE, raw_dir)
        distinct_grain_keys = con.execute(
            f"SELECT count(DISTINCT {grain_key_sql()}) FROM {TABLE}"
        ).fetchone()[0]
    finally:
        con.close()

    problems = grain_failures(rows_duckdb, distinct_grain_keys)
    for problem in problems:
        log(f"FAIL  {problem}")
    if problems:
        return 1

    manifest = manifest_payload(
        dataset=dataset,
        view=view,
        landing_files=landing_files,
        verification=verification,
        distinct_grain_keys=distinct_grain_keys,
        rows_landing=rows_landing,
        rows_raw=rows_raw,
        rows_duckdb=rows_duckdb,
        partition_rows=counts,
        retrieved_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    )
    manifest["columns_in_data"] = columns_in_data
    write_json(raw_dir / "manifest.json", manifest)

    log(f"landing {rows_landing:,} -> raw {rows_raw:,} -> {TABLE} {rows_duckdb:,}")
    log(f"grain verified: {distinct_grain_keys:,} distinct keys = {rows_duckdb:,} rows")
    log(
        f"{columns_in_data} columns in the data, "
        f"{manifest['columns_in_view_metadata']} in the view metadata"
    )
    log(f"partitions: {len(counts)}  (michigan rows: {counts.get('MI', 0):,})")
    rollups = manifest["rollup_rows"]
    log(
        f"rollup partitions kept and named: {rollups['keys']} "
        f"({rollups['rows']:,} rows -- never sum across locationabbr)"
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
        help="re-page even when the manifest matches the publisher's rowsUpdatedAt",
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
