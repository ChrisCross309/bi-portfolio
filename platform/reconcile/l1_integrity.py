"""L1 integrity: prove raw is a faithful copy of what the publisher published.

Deterministic and hand-written -- every check is something a person can restate in a
sentence and defend in an interview. Output is grouped by project track.

The chain we assert, per source:

    source-reported total  ->  landing rows  ->  raw parquet rows  ->  DuckDB table rows

A gap between the publisher's own count and the bulk file it serves is a *timing*
observation, not a defect: publishers compute counts and generate bulk files at
different moments. A gap anywhere downstream of landing is ours, and fails the run.

Run:  python -m reconcile.l1_integrity [--track TRACK] [--mode live|fixture]
"""

from __future__ import annotations

import argparse
import functools
import json
import os
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import httpx
from ingest.common import (
    HIVE_NULL_PARTITION,
    REPO_ROOT,
    load_state_codes,
    paths_for,
    raw_relation,
    sql_literal,
)
from ingest.fintech import cfpb, hmda
from ingest.health import cdc, cms
from ingest.insurance import nfip_claims, nfip_policies
from ingest.registry import SOURCES as RAW_SOURCES
from ingest.registry import RawSource
from ingest.shared import bls, census
from reconcile.michigan import check_michigan_geography, gate_for
from reconcile.questions import check_question_coverage, questions_for
from reconcile.results import FAIL, PASS, SKIP, WARN, Result

USER_AGENT = "bi-portfolio-pipeline/0.1 (chris.hall309@gmail.com)"
NFIP_API = "https://www.fema.gov/api/open/v3/NfipClaims"


@dataclass
class Profile:
    """One pass over a relation: totals, per-partition counts, and control sums."""

    total: int = 0
    by_partition: dict[str, int] = field(default_factory=dict)
    sums: dict[str, Decimal] = field(default_factory=dict)


@dataclass(frozen=True)
class SourceSpec:
    """One registered source: what raw says it is, plus how to find and read its landing.

    `raw` carries track, source, table and partition column. It is not restated here --
    `ingest.registry` owns those four for the whole repo, and `tests/test_registry.py`
    proves they match the fetchers. What this adds is the landing side, which the registry
    deliberately excludes because only L1 consumes it.

    The globs are separate from `landing_relation` on purpose: the globs answer "which
    files, and are they still on disk?", the relation answers "what SQL reads them?".
    Neither can drift into disagreeing with the other because they are never compared.
    """

    raw: RawSource
    landing_relation: Callable[[Path], str]
    landing_partition_expr: Callable[[dict[str, Any]], str]
    partition_check: Callable[..., list[Result]]
    landing_glob: str
    fixture_glob: str
    control_sum_columns: tuple[str, ...] = ()
    # Two publishers land more than one source into a single directory, so the directory
    # is not always the source name: census -> shared/acs5, bls -> shared/cpi_u.
    landing_subdir: str = ""
    # Only census nests its fixtures; every other source's sit directly under the track.
    fixture_subdir: str = ""
    # The earliest partition key a year-partitioned source should carry. CFPB's database
    # opened in 2011, HMDA's modern schema in 2018, ACS's overlap in 2010, CPI-U in 1913.
    first_expected_year: int | None = None

    @property
    def track(self) -> str:
        return self.raw.track

    @property
    def source(self) -> str:
        return self.raw.source

    @property
    def table(self) -> str:
        return self.raw.table

    @property
    def partition_column(self) -> str:
        return self.raw.partition_column


# ── generic helpers ───────────────────────────────────────────────────────────
#
# `sql_literal`, `load_state_codes`, `paths_for` and `raw_relation` were restated here
# when the harness was written, before `ingest.common` existed. They are imported now
# instead. `raw_relation` is why: it decides how the partition key is typed on read, and
# two copies of that decision meant the harness could profile a tree the loader had
# written under different rules. Both live under `platform/`, so the import costs nothing.


def parquet_relation(landing: Path) -> str:
    """The plain read the two OpenFEMA paged sources use. Types come from the file."""
    return f"read_parquet({sql_literal(landing)})"


def profile(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    partition_expr: str,
    sum_columns: tuple[str, ...],
) -> Profile:
    """Single scan producing every control total we compare. Scans are expensive; do one."""
    projections = [
        f"COALESCE(CAST({partition_expr} AS VARCHAR), '{HIVE_NULL_PARTITION}') AS k",
        "count(*) AS n",
    ]
    projections += [f'sum("{column}") AS s{i}' for i, column in enumerate(sum_columns)]
    rows = con.execute(f"SELECT {', '.join(projections)} FROM {relation} GROUP BY 1").fetchall()

    result = Profile()
    result.sums = {column: Decimal(0) for column in sum_columns}
    for row in rows:
        key, count = str(row[0]), int(row[1])
        result.by_partition[key] = result.by_partition.get(key, 0) + count
        result.total += count
        for index, column in enumerate(sum_columns):
            value = row[2 + index]
            if value is not None:
                result.sums[column] += Decimal(str(value))
    return result


# ── the checks ────────────────────────────────────────────────────────────────


def classify_source_gap(manifest: dict[str, Any], rows_landing: int) -> tuple[str, str]:
    """Compare the publisher's own count to what its bulk file actually contained.

    Per the working rule: matching refresh timestamps with mismatched counts is a bug;
    differing timestamps make it a timing note. When a publisher exposes only one
    timestamp we cannot prove they match, so we warn rather than fail -- and say so.
    """
    reported = manifest.get("source_reported_count")
    if reported is None:
        return SKIP, "publisher exposes no count for this source"

    gap = reported - rows_landing
    if gap == 0:
        return PASS, f"source-reported {reported:,} = landing {rows_landing:,}"

    data_refresh = manifest.get("source_last_refresh")
    metadata_refresh = manifest.get("source_metadata_refresh")
    if metadata_refresh is None:
        return (
            WARN,
            f"source-reported {reported:,} vs landing {rows_landing:,} (gap {gap:+,}); "
            f"only one refresh timestamp published ({data_refresh}), so bulk-vs-API lag "
            "cannot be ruled out",
        )
    if data_refresh == metadata_refresh:
        return (
            FAIL,
            f"source-reported {reported:,} vs landing {rows_landing:,} (gap {gap:+,}) with "
            f"identical refresh timestamps ({data_refresh}) -- not explainable as timing",
        )
    return (
        WARN,
        f"source-reported {reported:,} vs landing {rows_landing:,} (gap {gap:+,}); refresh "
        f"lag: data {data_refresh}, metadata {metadata_refresh}",
    )


def check_count_chain(
    spec: SourceSpec, manifest: dict[str, Any], landing: Profile, raw: Profile, table_rows: int
) -> list[Result]:
    def result(check: str, status: str, detail: str) -> Result:
        return Result(spec.track, spec.source, check, status, detail)

    status, detail = classify_source_gap(manifest, landing.total)
    results = [result("source -> landing", status, detail)]

    if landing.total == raw.total == table_rows:
        results.append(
            result(
                "landing -> raw -> table",
                PASS,
                f"{landing.total:,} rows unchanged through both conversions",
            )
        )
    else:
        results.append(
            result(
                "landing -> raw -> table",
                FAIL,
                f"landing {landing.total:,} / raw {raw.total:,} / table {table_rows:,} disagree",
            )
        )
    return results


def check_lossless(spec: SourceSpec, landing: Profile, raw: Profile) -> list[Result]:
    """Control totals across the landing -> raw conversion.

    Monetary columns are summed where a sum is meaningful. Where it is not -- CMS and
    CDC publish aggregates over mixed averaging grains, so summing them produces a
    number with no referent -- we compare per-partition counts instead. CFPB has no
    monetary column at all, so it takes the count path too.
    """
    results: list[Result] = []

    if spec.control_sum_columns:
        mismatched = {
            column: (landing.sums[column], raw.sums[column])
            for column in spec.control_sum_columns
            if landing.sums[column] != raw.sums[column]
        }
        if mismatched:
            detail = "; ".join(f"{c}: landing {a} vs raw {b}" for c, (a, b) in mismatched.items())
            results.append(Result(spec.track, spec.source, "control sums", FAIL, detail))
        else:
            total = sum(landing.sums.values())
            results.append(
                Result(
                    spec.track,
                    spec.source,
                    "control sums",
                    PASS,
                    f"{len(spec.control_sum_columns)} monetary columns identical "
                    f"(${total:,.2f} combined)",
                )
            )
    else:
        results.append(
            Result(
                spec.track,
                spec.source,
                "control sums",
                SKIP,
                "no column where a sum is meaningful; per-partition counts used instead",
            )
        )

    if landing.by_partition == raw.by_partition:
        results.append(
            Result(
                spec.track,
                spec.source,
                "per-partition counts",
                PASS,
                f"{len(raw.by_partition)} partitions match row for row",
            )
        )
    else:
        only_landing = set(landing.by_partition) - set(raw.by_partition)
        only_raw = set(raw.by_partition) - set(landing.by_partition)
        differing = {
            key
            for key in set(landing.by_partition) & set(raw.by_partition)
            if landing.by_partition[key] != raw.by_partition[key]
        }
        results.append(
            Result(
                spec.track,
                spec.source,
                "per-partition counts",
                FAIL,
                f"only in landing: {sorted(only_landing)}; only in raw: {sorted(only_raw)}; "
                f"differing counts: {sorted(differing)}",
            )
        )
    return results


def check_state_partitions(
    spec: SourceSpec, raw: Profile, table_rows: int, mode: str = "live"
) -> list[Result]:
    """NFIP: every state and DC present, territories present, unknown-state kept.

    Geographic completeness asks "did ingestion drop a state the publisher publishes?"
    That is only answerable against the full dataset. A committed fixture is a
    stratified sample by construction, so the check is skipped there rather than
    satisfied by padding the sample until it passes -- which would prove nothing about
    production. The logic itself is covered by unit tests.
    """
    known = load_state_codes()
    present = set(raw.by_partition)
    territories = {"AS", "GU", "MP", "PR", "VI"}
    states_and_dc = known - territories
    results: list[Result] = []

    if mode == "fixture":
        for check in ("states + DC present", "territories present"):
            results.append(
                Result(
                    spec.track,
                    spec.source,
                    check,
                    SKIP,
                    "fixture is a stratified sample, not a census; coverage is a live check",
                )
            )
    else:
        missing_states = sorted(states_and_dc - present)
        results.append(
            Result(
                spec.track,
                spec.source,
                "states + DC present",
                FAIL if missing_states else PASS,
                f"missing {missing_states}"
                if missing_states
                else f"all {len(states_and_dc)} present",
            )
        )

        missing_territories = sorted(territories - present)
        results.append(
            Result(
                spec.track,
                spec.source,
                "territories present",
                WARN if missing_territories else PASS,
                f"missing {missing_territories}; national totals will not tie to FEMA"
                if missing_territories
                else f"all {len(territories)} present "
                f"({sum(raw.by_partition[t] for t in territories):,} rows)",
            )
        )

    unknown = {k: v for k, v in raw.by_partition.items() if k not in known}
    results.append(
        Result(
            spec.track,
            spec.source,
            "unknown-state rows kept",
            PASS,
            ", ".join(f"{k}={v:,}" for k, v in sorted(unknown.items())) or "none present",
        )
    )
    results.append(_partition_sum_result(spec, raw, table_rows))
    return results


def check_year_partitions(
    spec: SourceSpec, raw: Profile, table_rows: int, mode: str = "live"
) -> list[Result]:
    """An unbroken run of years from the source's first published year to its newest.

    The first year is the source's, not a constant: CFPB's database opened in 2011, HMDA's
    modern schema in 2018, the ACS vintage overlap in 2010, and CPI-U reaches back to 1913.
    A hole in the middle is a load failure; a different starting year is a publisher
    changing what it offers, which is worth a warning rather than a stop.
    """
    years = sorted(key for key in raw.by_partition if key.isdigit())
    results: list[Result] = []

    if not years:
        results.append(Result(spec.track, spec.source, "year coverage", FAIL, "no year partitions"))
        return results

    if mode == "fixture":
        # The same rule the state roster follows: "is a year missing?" is a question about
        # the whole dataset, and a committed fixture is a stratified sample by
        # construction. The ACS fixture is the proof -- it carries vintages 2011 and 2024
        # deliberately, because 2011 is the only vintage that enumerates the island areas.
        # Padding it to fifteen vintages so this check could pass would make the fixture
        # bigger and prove nothing about production.
        for check in ("year coverage", "earliest year"):
            results.append(
                Result(
                    spec.track,
                    spec.source,
                    check,
                    SKIP,
                    "fixture is a stratified sample, not a census; coverage is a live check",
                )
            )
    else:
        expected = {str(year) for year in range(int(years[0]), int(years[-1]) + 1)}
        gaps = sorted(expected - set(years))
        results.append(
            Result(
                spec.track,
                spec.source,
                "year coverage",
                FAIL if gaps else PASS,
                f"missing years {gaps}"
                if gaps
                else f"{years[0]}..{years[-1]} unbroken, {len(years)} partitions",
            )
        )

        if spec.first_expected_year is not None and int(years[0]) != spec.first_expected_year:
            results.append(
                Result(
                    spec.track,
                    spec.source,
                    "earliest year",
                    WARN,
                    f"earliest partition {years[0]}, expected {spec.first_expected_year}",
                )
            )

    malformed = {k: v for k, v in raw.by_partition.items() if not k.isdigit()}
    results.append(
        Result(
            spec.track,
            spec.source,
            "malformed dates kept",
            PASS,
            ", ".join(f"{k!r}={v:,}" for k, v in sorted(malformed.items())) or "none present",
        )
    )
    results.append(_partition_sum_result(spec, raw, table_rows))
    return results


def check_expected_keys(
    spec: SourceSpec, raw: Profile, table_rows: int, mode: str = "live", expected: frozenset = None
) -> list[Result]:
    """A partition column whose whole domain is small, fixed and known in advance.

    Three sources qualify, for three different reasons. NFIP policies is MI-only by a
    scope decision, so any other state is the leak CLAUDE.md section 4 warns about, not a
    coverage gap. CMS ships National/State/County grains stacked in one file. The CPI-U
    series file partitions on seasonal adjustment, which is two values and always will be.

    A missing key and an unexpected key are different failures and are reported that way:
    the first means something did not land, the second means the publisher's domain grew
    and every downstream filter written against it is now incomplete.
    """
    present = set(raw.by_partition)
    results: list[Result] = []

    missing = sorted(expected - present)
    results.append(
        Result(
            spec.track,
            spec.source,
            "expected keys present",
            FAIL if missing else PASS,
            f"missing {missing}"
            if missing
            else f"all {len(expected)} present: "
            + ", ".join(f"{key}={raw.by_partition[key]:,}" for key in sorted(expected)),
        )
    )

    unexpected = sorted(present - expected)
    results.append(
        Result(
            spec.track,
            spec.source,
            "no unexpected keys",
            FAIL if unexpected else PASS,
            f"keys outside the known domain {sorted(expected)}: "
            + ", ".join(f"{key}={raw.by_partition[key]:,}" for key in unexpected)
            if unexpected
            else f"domain is exactly {sorted(expected)}",
        )
    )
    results.append(_partition_sum_result(spec, raw, table_rows))
    return results


def check_peer_and_rollup_partitions(
    spec: SourceSpec, raw: Profile, table_rows: int, mode: str = "live"
) -> list[Result]:
    """CDC: states, territories, census regions and the nation, all stored as peers.

    `locationabbr` mixes four different grains in one column. Michigan sits beside `MDW`,
    which contains it, which sits beside `US`, which contains both -- so summing across
    this column counts every state up to three times. The rollups are kept deliberately,
    because HLT-E1 compares Michigan against exactly them, and naming them here is what
    stops session 2 from discovering the double count the hard way.

    The state roster is a coverage question and is skipped against a fixture, for the same
    reason NFIP's is: a stratified sample cannot answer "did we drop a state?", and padding
    one until it could would prove nothing about production.
    """
    known = load_state_codes()
    present = set(raw.by_partition)
    rollups = present - known
    results: list[Result] = []

    if mode == "fixture":
        results.append(
            Result(
                spec.track,
                spec.source,
                "states + DC present",
                SKIP,
                "fixture is a stratified sample, not a census; coverage is a live check",
            )
        )
    else:
        # CDC publishes three territories, not five: no American Samoa, no Northern
        # Marianas. So the expectation is the states and DC, and territories are counted
        # rather than required.
        states_and_dc = known - {"AS", "GU", "MP", "PR", "VI"}
        missing = sorted(states_and_dc - present)
        results.append(
            Result(
                spec.track,
                spec.source,
                "states + DC present",
                FAIL if missing else PASS,
                f"missing {missing}" if missing else f"all {len(states_and_dc)} present",
            )
        )
        territories = sorted((known - states_and_dc) & present)
        results.append(
            Result(
                spec.track,
                spec.source,
                "territories present",
                PASS,
                ", ".join(f"{key}={raw.by_partition[key]:,}" for key in territories) or "none",
            )
        )

    results.append(
        Result(
            spec.track,
            spec.source,
            "rollups kept, not summed",
            FAIL if not rollups else PASS,
            "no rollup rows found; HLT-E1 has nothing to compare Michigan against"
            if not rollups
            else ", ".join(f"{key}={raw.by_partition[key]:,}" for key in sorted(rollups))
            + " -- peers of the states in this column, so summing it double counts",
        )
    )
    results.append(_partition_sum_result(spec, raw, table_rows))
    return results


def _partition_sum_result(spec: SourceSpec, raw: Profile, table_rows: int) -> Result:
    """Nothing fell between partitions on the way into the table."""
    total = sum(raw.by_partition.values())
    return Result(
        spec.track,
        spec.source,
        "partitions sum to table",
        PASS if total == table_rows else FAIL,
        f"{total:,} = {table_rows:,}"
        if total == table_rows
        else f"partitions sum to {total:,} but table holds {table_rows:,}",
    )


def check_nfip_api_spot_counts(
    spec: SourceSpec, raw: Profile, mode: str, states: tuple[str, ...] = ("MI", "LA", "PR")
) -> list[Result]:
    """Ask FEMA directly what it holds for a few states and compare to our partitions."""
    if mode == "fixture":
        return [
            Result(
                spec.track,
                spec.source,
                "API spot counts",
                SKIP,
                "SKIPPED (offline) -- fixture mode never calls a publisher",
            )
        ]

    results: list[Result] = []
    try:
        with httpx.Client(
            timeout=120, headers={"User-Agent": USER_AGENT}, follow_redirects=True
        ) as client:
            for state in states:
                response = client.get(
                    NFIP_API,
                    params={
                        "$count": "true",
                        "$top": "1",
                        "$select": "id",
                        "$filter": f"state eq '{state}'",
                    },
                )
                response.raise_for_status()
                reported = int(response.json()["metadata"]["count"])
                local = raw.by_partition.get(state, 0)
                gap = reported - local
                results.append(
                    Result(
                        spec.track,
                        spec.source,
                        f"API spot count {state}",
                        PASS if gap == 0 else WARN,
                        f"API {reported:,} vs local {local:,}"
                        + ("" if gap == 0 else f" (gap {gap:+,}; bulk file trails the API)"),
                    )
                )
    except Exception as error:  # noqa: BLE001 - a publisher outage must not fail the harness
        results.append(
            Result(spec.track, spec.source, "API spot counts", WARN, f"unreachable: {error}")
        )
    return results


# ── source registry ───────────────────────────────────────────────────────────


def raw_source(source: str) -> RawSource:
    """Look the four registry-owned fields up rather than restating them here."""
    return next(spec for spec in RAW_SOURCES if spec.source == source)


def _acs_relation(dataset: str) -> Callable[[Path], str]:
    """The one source whose reader wants a directory rather than a glob.

    `census.dataset_relation` unions three geographies, each with its own glob, because the
    API returns a different shape for us, state and county. It builds those globs itself,
    so it is handed the directory the spec's glob lives in.
    """

    def build(landing_glob: Path) -> str:
        return census.dataset_relation(
            landing_glob.parent, dataset, census.DATASETS[dataset]["variables"]
        )

    return build


SOURCES: tuple[SourceSpec, ...] = (
    SourceSpec(
        raw=raw_source("nfip_claims"),
        # The fetcher's own reader, so what is compared is the conversion and not the
        # parse. Reimplementing it here would mean reimplementing its quirks too.
        landing_relation=lambda path: nfip_claims.source_relation(path, "parquet"),
        landing_partition_expr=lambda manifest: '"state"',
        partition_check=check_state_partitions,
        landing_glob="*.parquet",
        fixture_glob="nfip_claims.parquet",
        control_sum_columns=(
            "amountPaidOnBuildingClaim",
            "amountPaidOnContentsClaim",
            "amountPaidOnIncreasedCostOfComplianceClaim",
            "netBuildingPaymentAmount",
            "netContentsPaymentAmount",
            "netIccPaymentAmount",
        ),
    ),
    SourceSpec(
        raw=raw_source("nfip_policies"),
        landing_relation=parquet_relation,
        landing_partition_expr=lambda manifest: '"propertyState"',
        partition_check=functools.partial(check_expected_keys, expected=frozenset({"MI"})),
        # 39 keyset pages, so the glob is doing real work here.
        landing_glob=nfip_policies.LANDING_GLOB,
        fixture_glob="nfip_policies.parquet",
        # Monetary and integer. The DOUBLE latitude and elevation columns are neither, and
        # a float sum would be a control total nobody could defend.
        control_sum_columns=(
            "policyCost",
            "totalInsurancePremiumOfThePolicy",
            "totalBuildingInsuranceCoverage",
            "totalContentsInsuranceCoverage",
        ),
    ),
    SourceSpec(
        raw=raw_source("fema_declarations"),
        landing_relation=parquet_relation,
        landing_partition_expr=lambda manifest: '"state"',
        partition_check=check_state_partitions,
        landing_glob="*.parquet",
        fixture_glob="fema_declarations.parquet",
        # No numeric column at all: declarations are dates, codes and names. Per-partition
        # counts carry the lossless proof instead.
        control_sum_columns=(),
    ),
    SourceSpec(
        raw=raw_source("cfpb_complaints"),
        landing_relation=cfpb.source_relation,
        landing_partition_expr=lambda manifest: f'substr("{manifest["date_column"]}", 1, 4)',
        partition_check=check_year_partitions,
        # `*.csv` and not `*` because the archive it was extracted from sits beside it.
        landing_glob="*.csv",
        fixture_glob="cfpb_complaints.csv",
        first_expected_year=2011,
    ),
    SourceSpec(
        raw=raw_source("hmda_lar"),
        landing_relation=hmda.lar_relation,
        landing_partition_expr=lambda manifest: '"activity_year"',
        partition_check=check_year_partitions,
        landing_glob=hmda.LAR_GLOB,
        fixture_glob="hmda_lar.csv",
        # Every numeric-looking column can hold the literal "Exempt", so nothing here is
        # summable without the typing that is session 2's job.
        control_sum_columns=(),
        first_expected_year=hmda.FIRST_MODERN_YEAR,
    ),
    SourceSpec(
        raw=raw_source("hmda_institutions"),
        landing_relation=hmda.institutions_relation,
        landing_partition_expr=lambda manifest: '"period"',
        partition_check=check_year_partitions,
        landing_glob=hmda.FILERS_GLOB,
        fixture_glob="hmda_institutions.json",
        first_expected_year=hmda.FIRST_MODERN_YEAR,
    ),
    SourceSpec(
        raw=raw_source("cdc_healthy_aging"),
        landing_relation=cdc.source_relation,
        landing_partition_expr=lambda manifest: '"locationabbr"',
        partition_check=check_peer_and_rollup_partitions,
        landing_glob=cdc.LANDING_GLOB,
        fixture_glob="cdc_healthy_aging.csv",
        # Aggregate cells over mixed averaging grains: a sum of them has no referent.
        control_sum_columns=(),
    ),
    SourceSpec(
        raw=raw_source("cms_geographic_variation"),
        landing_relation=cms.source_relation,
        landing_partition_expr=lambda manifest: '"BENE_GEO_LVL"',
        partition_check=functools.partial(check_expected_keys, expected=frozenset(cms.GEO_LEVELS)),
        # One file, whose name carries the year range and several spaces.
        landing_glob="*.csv",
        fixture_glob="cms_geographic_variation.csv",
        control_sum_columns=(),
    ),
    # Both ACS datasets land under one directory, and their fixtures nest in one too --
    # the two cases `landing_subdir` and `fixture_subdir` exist for.
    *(
        SourceSpec(
            raw=raw_source(spec["source"]),
            landing_relation=_acs_relation(dataset),
            landing_partition_expr=lambda manifest: '"vintage"',
            partition_check=check_year_partitions,
            landing_glob=f"acs5-{dataset}-*.json",
            fixture_glob=f"acs5-{dataset}-*.json",
            landing_subdir="acs5",
            fixture_subdir="acs5",
            # Estimates carry annotations like -555555555, which are not measurements.
            control_sum_columns=(),
            first_expected_year=2010,
        )
        for dataset, spec in census.DATASETS.items()
    ),
    SourceSpec(
        raw=raw_source("cpi_u"),
        landing_relation=bls.tsv_relation,
        landing_partition_expr=lambda manifest: '"year"',
        partition_check=check_year_partitions,
        # Not a glob and not an extension: BLS names this file `cu.data.1.AllItems`, which
        # is why matching landing by suffix could never have worked here.
        landing_glob=bls.OBSERVATIONS_FILE,
        fixture_glob="cpi_u.tsv",
        landing_subdir="cpi_u",
        # `value` is a space-padded index level, not an amount; summing it is meaningless.
        control_sum_columns=(),
        first_expected_year=1913,
    ),
    SourceSpec(
        raw=raw_source("cpi_u_series"),
        landing_relation=bls.tsv_relation,
        landing_partition_expr=lambda manifest: '"seasonal"',
        partition_check=functools.partial(check_expected_keys, expected=frozenset({"S", "U"})),
        landing_glob=bls.SERIES_FILE,
        fixture_glob="cpi_u_series.tsv",
        landing_subdir="cpi_u",
        control_sum_columns=(),
    ),
)


# ── orchestration ─────────────────────────────────────────────────────────────


def resolve_landing_path(spec: SourceSpec, mode: str, root: Path) -> Path:
    """Where this source's landing lives, as a glob DuckDB can read directly.

    A glob and not a filename, because half the sources land many files: 39 keyset pages
    for NFIP policies, 45 per ACS dataset, 8 per HMDA source, 6 for CDC. The previous
    version returned the manifest's first recorded name, which for those sources silently
    verified one file out of many.

    Two more things the manifest cannot be asked for. The landing directory is not always
    the source name -- census lands both ACS sources under `shared/acs5` and bls lands both
    CPI sources under `shared/cpi_u` -- so specs that share a directory name it. And
    matching by file extension does not work at all for BLS, whose files are
    `cu.data.1.AllItems` and `cu.series`.
    """
    if mode == "fixture":
        base = REPO_ROOT / "tests" / "fixtures" / spec.track
        return (base / spec.fixture_subdir if spec.fixture_subdir else base) / spec.fixture_glob
    return root / "landing" / spec.track / (spec.landing_subdir or spec.source) / spec.landing_glob


def landing_files(pattern: Path) -> list[Path]:
    """The files a landing glob actually matches, so "is it still there?" has an answer."""
    return sorted(pattern.parent.glob(pattern.name)) if pattern.parent.is_dir() else []


def check_source(spec: SourceSpec, mode: str) -> list[Result]:
    data_root, db_path = paths_for(mode)
    raw_dir = data_root / "raw" / spec.track / spec.source
    manifest_path = raw_dir / "manifest.json"

    if not manifest_path.exists():
        return [Result(spec.track, spec.source, "ingested", SKIP, "no manifest; not ingested yet")]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        table_rows = con.execute(f"SELECT count(*) FROM {spec.table}").fetchone()[0]
        raw = profile(
            con, raw_relation(raw_dir), f'"{spec.partition_column}"', spec.control_sum_columns
        )

        results: list[Result] = []
        landing_path = resolve_landing_path(spec, mode, data_root)
        landed = landing_files(landing_path)
        if not landed:
            results.append(
                Result(
                    spec.track,
                    spec.source,
                    "landing available",
                    SKIP,
                    f"no file matches {landing_path.name} -- reclaimed by "
                    "`just clean-landing`; lossless conversion cannot be re-proved "
                    "until the next ingest",
                )
            )
            landing = raw  # count-chain checks below degrade to raw-vs-table only
        else:
            started = time.monotonic()
            landing = profile(
                con,
                spec.landing_relation(landing_path),
                spec.landing_partition_expr(manifest),
                spec.control_sum_columns,
            )
            elapsed = time.monotonic() - started
            size = sum(path.stat().st_size for path in landed)
            results.append(
                Result(
                    spec.track,
                    spec.source,
                    "landing available",
                    PASS,
                    f"re-read {len(landed)} file(s) matching {landing_path.name} in "
                    f"{elapsed:,.1f}s ({size / 1e6:,.1f} MB)",
                )
            )
            results += check_lossless(spec, landing, raw)

        results += check_count_chain(spec, manifest, landing, raw, table_rows)
        results += spec.partition_check(spec, raw, table_rows, mode)
        # The Michigan gate reads the loaded table rather than a profile: it asks about
        # column contents, not partition keys, and only eight of the twelve sources carry
        # a Michigan geography at all.
        if gate := gate_for(spec.source):
            results += check_michigan_geography(con, spec.track, gate, mode)
        if spec.source == "nfip_claims":
            results += check_nfip_api_spot_counts(spec, raw, mode)
        return results
    finally:
        con.close()


def check_questions(track: str, mode: str) -> list[Result]:
    """Session 1's finish line: every executive question answerable from raw.

    Per track rather than per source, because a question reaches across sources -- INS-E4
    needs NFIP policies and ACS housing units, HLT-E3 needs CMS and the CPI-U deflator.
    """
    data_root, db_path = paths_for(mode)
    if not questions_for(track):
        return []
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        return check_question_coverage(con, track, data_root, mode)
    finally:
        con.close()


def report(results: list[Result]) -> int:
    icons = {PASS: "PASS", FAIL: "FAIL", WARN: "WARN", SKIP: "SKIP"}
    tracks: dict[str, list[Result]] = {}
    for result in results:
        tracks.setdefault(result.track, []).append(result)

    for track, track_results in tracks.items():
        print(f"\n{'=' * 78}\n  {track.upper()}\n{'=' * 78}")
        sources: dict[str, list[Result]] = {}
        for result in track_results:
            sources.setdefault(result.source, []).append(result)
        for source, source_results in sources.items():
            print(f"\n  {source}")
            for result in source_results:
                print(f"    {icons[result.status]:<5} {result.check:<26} {result.detail}")

    counts = {
        status: sum(1 for r in results if r.status == status) for status in (PASS, WARN, SKIP, FAIL)
    }
    print(
        f"\n{'-' * 78}\n  {counts[PASS]} passed, {counts[WARN]} warned, "
        f"{counts[SKIP]} skipped, {counts[FAIL]} failed\n{'-' * 78}"
    )
    return 1 if counts[FAIL] else 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        # The four tracks from CLAUDE.md section 1, not just the ones registered above.
        # Sources join SOURCES as their PRs land, and `just health` / `just shared` call this
        # with their own track name meanwhile -- main() answers that with "no sources
        # registered" rather than an argparse error on a track the repo genuinely has.
        "--track",
        choices=["insurance", "fintech", "health", "shared", "all"],
        default="all",
    )
    parser.add_argument(
        "--mode", choices=("live", "fixture"), default=os.environ.get("DATA_MODE", "live")
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    selected = [s for s in SOURCES if args.track in ("all", s.track)]
    if not selected:
        print(f"no sources registered for track {args.track!r}")
        return 0

    print(f"L1 integrity  ({args.mode} mode, {len(selected)} source(s))")
    results: list[Result] = []
    for spec in selected:
        results += check_source(spec, args.mode)
    results += check_questions(args.track, args.mode)
    return report(results)


if __name__ == "__main__":
    sys.exit(main())
