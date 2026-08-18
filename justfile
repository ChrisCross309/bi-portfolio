# bi-portfolio task runner.
#
# Recipes run under `sh` on Linux/macOS (and in CI) and PowerShell on Windows,
# so recipe bodies stay to commands that are identical in both. The two recipes
# that remove files carry per-OS variants.
set windows-shell := ["powershell.exe", "-NoLogo", "-NoProfile", "-Command"]

# `platform/` goes on the import path, not the repo root, so that `import platform`
# still resolves to the standard library. See CLAUDE.md section 5.
export PYTHONPATH := "platform"

# dbt reads its profile from the repo rather than `~/.dbt/`, so a fresh clone can build.
# Nothing in it is a secret. The two path variables are absolute so dbt resolves the
# warehouse the same way whatever directory it is invoked from; `transform/profiles.yml`
# falls back to repo-root-relative defaults for a bare `dbt` call outside `just`.
export DBT_PROFILES_DIR := "transform"
export DBT_WAREHOUSE_DIR := justfile_directory() / "platform" / "duckdb"
export DBT_TEMP_DIR := justfile_directory() / "data" / ".duckdb_tmp"

_default:
    @just --list --unsorted

# ── ingestion ──────────────────────────────────────────────
# Each module lands in its own PR; a recipe goes live when its PR merges.

# track 1 · insurance: NFIP claims bulk + MI policies + declarations -> raw -> load -> L1
insurance:
    uv run python -m ingest.insurance.nfip_claims
    uv run python -m ingest.insurance.nfip_policies
    uv run python -m ingest.insurance.fema_declarations
    uv run python -m reconcile.l1_integrity --track insurance

# track 2 · fintech: CFPB complaints bulk + HMDA MI years + institutions -> raw -> load -> L1
fintech:
    uv run python -m ingest.fintech.cfpb
    uv run python -m ingest.fintech.hmda
    uv run python -m reconcile.l1_integrity --track fintech

# track 3 · health: CDC healthy aging + CMS datasets -> raw -> load -> L1
health:
    uv run python -m ingest.health.cdc
    uv run python -m ingest.health.cms
    uv run python -m reconcile.l1_integrity --track health

# shared reference: ACS denominators + CPI-U deflator -> raw -> load -> L1
shared:
    uv run python -m ingest.shared.census
    uv run python -m ingest.shared.bls
    uv run python -m ingest.shared.hud_crosswalk
    uv run python -m reconcile.l1_integrity --track shared

# all four tracks, in any order
ingest-all: insurance fintech health shared

# rebuild every DuckDB raw table from local parquet — no network, no publisher
reload track="all":
    uv run python -m ingest.reload --track {{track}}

# ── semantic layer ─────────────────────────────────────────

# any dbt command against the live warehouse: `just dbt build`, `just dbt test`
dbt *ARGS:
    uv run dbt {{ARGS}} --project-dir transform

# regenerate dbt's source declarations from ingest.registry — run after adding a source
dbt-sources:
    uv run python -m ingest.dbt_sources

# ── verification ───────────────────────────────────────────

# ruff + pytest; live-endpoint tests are marked `slow` and excluded
check:
    uv run ruff check .
    uv run ruff format --check .
    uv run pytest

# what CI runs: load committed fixtures, then L1 logic with network checks skipped
ci:
    uv run python -m ingest.insurance.nfip_claims --mode fixture
    uv run python -m ingest.insurance.nfip_policies --mode fixture
    uv run python -m ingest.insurance.fema_declarations --mode fixture
    uv run python -m ingest.fintech.cfpb --mode fixture
    uv run python -m ingest.fintech.hmda --mode fixture
    uv run python -m ingest.health.cdc --mode fixture
    uv run python -m ingest.health.cms --mode fixture
    uv run python -m ingest.shared.bls --mode fixture
    uv run python -m ingest.shared.census --mode fixture
    uv run python -m ingest.shared.hud_crosswalk --mode fixture
    uv run python -m ingest.reload --mode fixture
    uv run python -m reconcile.l1_integrity --mode fixture
    uv run dbt build --project-dir transform --target ci

# regenerate committed fixtures from local raw — deliberate, never automatic
fixture source="":
    uv run python -m reconcile.make_fixtures {{ if source == "" { "" } else { "--source " + source } }}

# what each stratum would contribute, without writing anything
fixture-plan:
    uv run python -m reconcile.make_fixtures --dry-run

# ── housekeeping ───────────────────────────────────────────

# reclaim disk after L1 has passed; raw parquet is the canonical copy
[unix]
clean-landing:
    rm -rf data/landing/insurance/* data/landing/fintech/* data/landing/health/* data/landing/shared/*

[windows]
clean-landing:
    try { Remove-Item -Recurse -Force data/landing/*/* -ErrorAction Stop } catch {}

# remove all data and the DuckDB file — next run re-downloads everything
[unix]
clean:
    rm -rf data/landing/* data/raw/* data/marts/* platform/duckdb/*.duckdb

[windows]
clean:
    try { Remove-Item -Recurse -Force data/landing/*, data/raw/*, data/marts/*, platform/duckdb/*.duckdb -ErrorAction Stop } catch {}
