# CLAUDE.md — working rules for this repo

One repo, one shared platform, **three separate projects**. Read this before touching anything.

---

## 1. The three-track separation rule

**Shared platform, separate domains — code, data, and docs are organized by track, and domains never
mix.**

| Track | Slug | Domain | Datasets that may appear |
|---|---|---|---|
| Project 1 | `insurance` | Insurance / Insurtech | FEMA NFIP claims, NFIP policies, FEMA disaster declarations |
| Project 2 | `fintech` | Fintech / Consumer Lending | CFPB consumer complaints, HMDA LAR, HMDA institution reference |
| Project 3 | `health` | Health / Alzheimer's & Dementia | CDC Alzheimer's & Healthy Aging, CMS Geographic Variation |
| Shared | `shared` | Domain-neutral reference | Census ACS denominators, BLS CPI-U, state codes |

No insurance data in the fintech or health tracks. No fintech data in the insurance or health tracks.
No health data in the insurance or fintech tracks. What the tracks share is *platform* — DuckDB, the
ingestion utilities, the integrity harness, and the domain-neutral reference series that every
analytics project needs the way every warehouse needs a date table.

The interview story is "one platform, three regulated domains." The separation **is** the point.
**A change that blurs domains is wrong even when it is convenient.**

Thematic bridges between tracks (e.g. the older-American consumer tag in FIN pointing at HLT's
subject matter) are narrative only. They never involve shared data.

---

## 2. The question-set principle

Every metric answers three questions:

- **Level** — what is it now?
- **Benchmark** — compared to what? (prior period, trailing average, national, peer)
- **Direction** — better or worse, and is the change real?

Each project has five executive questions with stable IDs: `INS-E1…E5`, `FIN-E1…E5`, `HLT-E1…E5`.
**The IDs travel with the work** — they appear in each project README, in mart and model docs in
session 2, and in Power BI measure descriptions in session 3. Do not renumber them.

A raw pipeline is not done because data landed. It is done when the data the questions need has
landed — which is what the per-track question-coverage smoke checks in `l1_integrity.py` exist to
prove.

---

## 3. Ingestion rules (every source, every track)

1. **Landing → raw split.** As-downloaded files land untouched in `data/landing/<track>/<source>/`.
   Canonical raw is parquet under `data/raw/<track>/<source>/`, converted with **zero content
   change** — format and layout only. L1 proves the conversion lossless. `just clean-landing`
   reclaims disk once L1 passes.
2. **The all-varchar CSV rule.** Government CSVs are read into raw with `all_varchar=true`. **No type
   inference in raw, ever.** HMDA numerics contain the literal string `"Exempt"`; CMS aggregates use
   `*` for suppression; ACS uses negative sentinel annotations; HMDA age fields use codes like `8888`.
   Typing is session 2's job — deliberate, column by column. Parquet sources arrive with schemas
   embedded; keep them as-is.
3. **Discover links, never hardcode.** Resolve every bulk URL at runtime from the publisher's own
   metadata (OpenFEMA `OpenFemaDataSets` → `distribution`; `data.cms.gov/data.json`; the Socrata
   views API; the CFPB data page). Log the URL used. **Fail loudly if discovery fails** — never fall
   back to a guessed URL.
4. **Manifest per source, every run**, written to `data/raw/<track>/<source>/manifest.json`:
   resolved URL, `retrieved_at` (UTC, ISO 8601), SHA-256 of each landing file, source-reported
   count and refresh timestamp where available, rows landed.
5. **Polite network behavior.** Stream downloads to `.part` files and rename on completion. Retry
   with `tenacity`: exponential backoff on 429/5xx/timeouts, ≤5 attempts. Sleep 1–2s between
   paginated calls. No publisher documents a hard rate limit; behave as if all of them do.
6. **Paginated fetches** use a deterministic order where the API supports one, and verify the total
   against a source-reported count when one exists. **Never rely on an empty-page check alone.**
7. **Idempotent re-runs.** Partition and entity directories truncate-and-reload atomically. A
   manifest matching the source's current refresh timestamp means skip the download.
8. **Schema baselines.** Wherever a publisher exposes field metadata via API, snapshot it to
   `platform/reconcile/baselines/` and commit it (OpenFEMA fields; Socrata columns; CMS descriptors).
   For HMDA and CFPB, commit a pointer file with the documentation URL and retrieval date. Session 1
   captures baselines only — **no drift diffing this session**; that is L2's job.
9. **DuckDB naming.** One `raw` schema; tables prefixed by track — `ins_*`, `fin_*`, `hlt_*`, `ref_*`
   — so any query's domain is visible at a glance and session 2 can define dbt sources per project
   cleanly.

---

## 4. Michigan lens, national benchmark

Michigan is the analytical focus at **county grain** in all three projects. National data is retained
wherever it is cheap, so MI metrics have a benchmark.

> **Never filter a national source to Michigan in raw.**
> No `WHERE state = 'MI'` outside marts. This is the single most likely scope bug in the repo.

Where raw itself is MI-scoped for size reasons — NFIP policies (60M+ national rows), HMDA LAR (tens
of millions per year) — the reasoning is recorded in that project's README, and the national
benchmark comes from the publisher's own published aggregates as reconciliation control totals, not
from raw.

---

## 5. Repo layout and Python imports

```
data/{landing,raw}/{insurance,fintech,health,shared}/   gitignored
platform/ingest/<track>/<source>.py                     fetchers
platform/reconcile/l1_integrity.py                      the integrity harness
platform/reconcile/baselines/                           committed schema snapshots
projects/0N-<track>/README.md                           questions verbatim + scope/caveats
tests/fixtures/                                         committed per-source samples
```

**`platform/` must never contain an `__init__.py` at its root.** `platform` is a Python standard
library module name, and a regular package there would shadow it for every dependency that imports
it. The directory stays a namespace-package candidate, which loses to the stdlib module — safe.

Import across the codebase by the *sub*-package name, which is collision-free:

```python
from ingest.common import stream_download, write_manifest
from reconcile.l1_integrity import check_count_chain
```

This works because `platform/` is on the path, not the repo root: `pythonpath = ["platform"]` in
`[tool.pytest.ini_options]`, and `export PYTHONPATH := "platform"` in the justfile. Run entry points
as modules — `python -m ingest.insurance.nfip_claims` — never by file path.

---

## 6. CI — and the two things it must never do

`.github/workflows/ci.yml` runs on `pull_request` and on push to `main`.

> **CI never downloads bulk files. CI never calls an external API.**

Fixture mode is first-class:

- Every source gets a committed sample in `tests/fixtures/` — well under 2 MB, deterministic seed,
  stratified to include Michigan rows, edge cases, and null/sentinel values.
- A `DATA_MODE=fixture` switch is honored by every loader and by `l1_integrity.py`.
- Network-dependent checks report **`SKIPPED (offline)`** — never silently passed.
- Real-endpoint tests are marked `@pytest.mark.slow` and are excluded from CI.

This offline/full split is what lets a scheduled-refresh cron drop in later without rework.

---

## 7. Git and GitHub workflow

- **Trunk-based, PR-only.** No direct pushes to `main`. `main` is protected and requires the CI check.
- Branches: `feat/`, `fix/`, `docs/`, `chore/` + slug. **One component per branch, one component per
  PR.** Squash-merge.
- Conventional commits, imperative mood: `feat(ins): add NFIP claims bulk fetcher`.
- PR body follows `.github/pull_request_template.md`: what/why in one sentence · which project track ·
  integrity checks added or updated · `just check` passes locally · I can explain every line.
- **Never commit data or secrets.** `data/`, `*.duckdb`, `.env`, `.venv/` are gitignored;
  `check-added-large-files` (~2 MB) is the pre-commit backstop. Commit `uv.lock`.

---

## 8. Engineering conventions

- **No premature abstraction.** Shared utilities are *extracted from* working concrete
  implementations, never designed up front. `platform/ingest/common.py` exists because two fetchers
  in two different tracks proved which parts are actually shared — not before.
- **Never commit code the author cannot explain line by line.** This is a portfolio; every line is
  interview surface area.
- **Stop, don't code around it.** If a publisher's API or metadata responds unexpectedly — changed
  shape, missing distribution, missing count, a mandatory-filter surprise on HMDA — stop and show the
  raw response. Never paper over it silently.
- **Ask before adding any dependency** not already in `pyproject.toml`.
- Honest grain. Annual and survey-cycle sources stay annual and survey-cycle. Never build structure
  that invites fake daily or quarterly reporting on annual data.
- Aggregate files are aggregates. A row in a CMS or CDC file is a *cell* (profile or geography ×
  year), not a person. Nothing in those files sums to patient counts.
- Public aggregate data only. **No PHI, no re-identification attempts, no individual-level
  inference.**
- Fair-lending findings are **descriptive, never conclusions** — HMDA lacks credit score and DTI.

---

## 9. Session roadmap

| Session | Scope |
|---|---|
| **1 (current)** | Every raw pipeline, all three tracks: fetch → land → parquet → DuckDB `raw` → L1 integrity → schema baselines. **No dbt, no staging models, no marts, no Power BI.** |
| 2 | Semantic layer: dbt staging/intermediate/marts per project, seeds, contracts, tests, conformed dimensions, deeper reconciliation. |
| 3 | Power BI: one `.pbip` semantic model and report per project — executive summary drilling to detail. |

Toolchain: `uv`, `just`, `ruff`, `pre-commit`, `gh`. Runtime deps: `duckdb`, `httpx`, `pyarrow`,
`tenacity`, `openpyxl` (CMS Excel case only). **No dbt this session.**
