# bi-portfolio

**One repo. One shared platform. Three separate projects.**

A BI portfolio built the way a regulated-industry data team would build it: public data ingested
reproducibly, reconciled against the publisher's own totals, modelled in a semantic layer, and
delivered as executive reports that answer a fixed set of questions.

```
Python ingestion  →  DuckDB (raw)  →  dbt  →  parquet marts  →  Power BI (.pbip)
```

---

## The three projects

Each project is a distinct domain with its own datasets, its own questions, its own README, its own
build command, and its own deliverables. **Nothing from one project's domain appears in another's.**

| # | Project | Domain | Sources | Build |
|---|---|---|---|---|
| 1 | [Insurance / Insurtech](projects/01-insurance-nfip/README.md) | Flood insurance analytics | FEMA NFIP claims + policies, FEMA disaster declarations | `just insurance` |
| 2 | [Fintech / Consumer Lending](projects/02-fintech-lending/README.md) | Complaints + mortgage lending | CFPB Consumer Complaints, HMDA LAR + institutions | `just fintech` |
| 3 | [Health / Alzheimer's & Dementia](projects/03-health-dementia/README.md) | Cognitive-health & Medicare | CDC Alzheimer's & Healthy Aging, CMS Chronic Conditions + Geographic Variation | `just health` |

What they share is **platform, not data**: DuckDB, the ingestion utilities, the integrity harness,
and a small set of domain-neutral reference series — CPI-U, Census population and housing
denominators, state codes — that every analytics project needs the way every warehouse needs a date
table. Those live under `data/raw/shared/` and build with `just shared`.

The story is *one platform, three regulated domains*. The separation between domains is the point.

---

## The question principle

Every metric answers three questions:

- **Level** — what is it now?
- **Benchmark** — compared to what? (prior period, trailing average, national, peer)
- **Direction** — better or worse, and is the change real?

Each project has five executive questions with stable IDs (`INS-E1`, `FIN-D3`, `HLT-E2`, …) listed
verbatim in its README. Those IDs travel with the work: into mart documentation in session 2, into
Power BI measure descriptions in session 3. Every executive report is one summary page showing its
five headline questions as KPIs — with vs-prior and vs-benchmark deltas and a reconciliation
certificate — drilling through Michigan → county → detail.

**Analytical scope: Michigan lens, national benchmark.** Michigan is the focus at county grain in all
three projects; national data is retained wherever it is cheap so MI metrics have something to be
compared against. National sources are never filtered to Michigan in raw.

---

## Roadmap

| Session | Scope | Status |
|---|---|---|
| **1** | Every raw pipeline, all three tracks: discover → download → land → parquet → DuckDB `raw` → L1 integrity → schema baselines | **in progress** |
| 2 | Semantic layer: dbt staging/intermediate/marts per project, seeds, contracts, tests, conformed dimensions, deeper reconciliation | planned |
| 3 | Power BI: one `.pbip` semantic model and report per project, executive summary drilling to detail | planned |

Session 1 lands no dbt models, no marts, and no Power BI artifacts. Its finish line is that every
executive question is *answerable from raw* — proven by the per-track question-coverage checks in
the L1 harness, not asserted.

---

## Integrity

`platform/reconcile/l1_integrity.py` runs after every ingestion and exits non-zero on any failure.
Output is grouped by project track. It checks:

- **Count chain** — source-reported total → landing rows → raw parquet rows → DuckDB table rows,
  with differences itemized.
- **Lossless conversion** — landing vs. raw control totals. Monetary sums where summing is
  meaningful; counts and spot-cell equality where it is not (CMS and CDC aggregates must never be
  summed as controls — mixed averaging grains make the sum meaningless).
- **Partition completeness** — NFIP states including territories and the null-state partition;
  every expected year present for CFPB and HMDA.
- **Michigan geography-quality gate** — MI vs. national null rates for county and tract codes;
  warns above 10%.
- **Question-coverage smoke checks** — per track, that the data each executive question needs is
  actually present.

---

## Getting started

Requires [uv](https://docs.astral.sh/uv/), [just](https://just.systems/), and (optionally)
[gh](https://cli.github.com/).

```bash
uv sync
just check
```

`data/` is gitignored and created on demand by the fetchers — a fresh clone has no data directories
until you run an ingestion recipe. Expect roughly 5–10 GB once all four recipes have run; check free
disk space before the CFPB bulk download.

| Recipe | What it does |
|---|---|
| `just insurance` | Track 1 → raw → load → L1 |
| `just fintech` | Track 2 → raw → load → L1 |
| `just health` | Track 3 → raw → load → L1 |
| `just shared` | Reference denominators and deflator → raw → load → L1 |
| `just ingest-all` | All four, in any order |
| `just check` | ruff + pytest (live-endpoint tests excluded) |
| `just ci` | Fixture mode: load committed samples, run L1 logic offline |
| `just clean-landing` | Reclaim disk once L1 has passed |

**Cost: $0.** All sources are free public APIs or bulk files. One key is required —
`api.census.gov` stopped serving unkeyed data requests, and signals it with an HTML page and an
HTTP 200 status rather than an auth error. Register free at
[api.census.gov/data/key_signup.html](https://api.census.gov/data/key_signup.html) and put it in
`.env` (gitignored); `.env.example` documents it, along with two other keys that remain optional.

Working rules — track separation, the all-varchar raw rule, link discovery, manifests, the Git
workflow — live in [CLAUDE.md](CLAUDE.md).
