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
| 3 | [Health / Alzheimer's & Dementia](projects/03-health-dementia/README.md) | Cognitive-health & Medicare | CDC Alzheimer's & Healthy Aging, CMS Medicare Geographic Variation | `just health` |

CMS Medicare Chronic Conditions was planned for track 3 and is **not ingested**: the publisher
retired it effective 2026-06-15, and it is absent from CMS's own catalogue and from every other
discoverable route. It cost two questions, not one — HLT-E2 and HLT-E3 both needed a condition
dimension. Both were re-scoped onto the Medicare Geographic Variation data that *is* ingested,
keeping their IDs and their structure and losing only condition specificity, which means **neither
now measures dementia**. The evidence table and the blunt version of that caveat are in the
[health README](projects/03-health-dementia/README.md).

What they share is **platform, not data**: DuckDB, the ingestion utilities, the integrity harness,
and a small set of domain-neutral reference series — CPI-U, Census population and housing
denominators, a ZIP-to-county crosswalk, state codes — that every analytics project needs the way
every warehouse needs a date table. Those live under `data/raw/shared/` and build with
`just shared`.

The story is *one platform, three regulated domains*. The separation between domains is the point.

### The shared series, and the traps in them

Each shared source exists to serve questions in more than one track, and each carries a caveat that
would quietly corrupt a measure if it were missed.

**A ZIP is not a county, and 34.6% of Michigan's cross a line.** The HUD USPS crosswalk is the
bridge from a ZIP to a county, and it publishes weights rather than a mapping — the share of a ZIP's
residential, business and other addresses in each county, rebuilt quarterly. Any ZIP-to-county
figure is therefore an allocation under a stated rule. The trap is `res_ratio`: it sums to **zero**
for 3,571 ZIPs, the PO-box and business-only ones, so a rule that weights by residential share alone
silently drops every row behind them. `tot_ratio` covers all of them. HUD reports its own vintage in
every response and it is recorded per run, because the same figure allocated with two vintages is
not the same number.

**ACS 5-year estimates are not a time series.** Consecutive vintages share four years of sample —
the 2020 vintage covers 2016–2020, the 2021 covers 2017–2021 — so plotting them as a trend shows
mostly the same survey responses moving against themselves. Only every fifth vintage is independent,
and the Census Bureau advises against comparing overlapping ones. All 15 vintages (2010–2024) are
landed because each is the right denominator *for its own window*; using one as a denominator is
correct, drawing a line through them is not. Two further notes: a 5-year estimate describes its whole
window rather than its final year, so labelling the 2024 vintage as "2024 population" overstates it;
and ACS annotates rather than nulls, so a margin of error of `-555555555` means "controlled estimate,
no margin applies" and is not a measurement.

**CPI-U comes in two flavours and only one is right here.** `CUUR0000SA0` is not seasonally adjusted;
`CUSR0000SA0` is. Constant-dollar comparisons use the **unadjusted annual average** — seasonal
adjustment exists for month-over-month movement and is the wrong series for deflating a yearly
figure. That annual average is published as a thirteenth month, `period = 'M13'`, sitting in the same
column as the twelve real months alongside semiannual averages `S01`–`S03`, so averaging across
`period` double counts. And BLS pads `series_id` to 17 characters: a filter on `'CUUR0000SA0'`
matches nothing at all until you `trim()` it.

---

## The question principle

Every metric answers three questions:

- **Level** — what is it now?
- **Benchmark** — compared to what? (prior period, trailing average, national, peer)
- **Direction** — better or worse, and is the change real?

Each project has five executive questions with stable IDs (`INS-E1`, `FIN-E3`, `HLT-E2`, …) listed
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
| 1 | Every raw pipeline, all three tracks: discover → download → land → parquet → DuckDB `raw` → L1 integrity → schema baselines | **complete** — 12 sources, 24.8M rows |
| **2** | Semantic layer: dbt staging/intermediate/marts per project, seeds, contracts, tests, conformed dimensions, deeper reconciliation | **in progress** |
| 3 | Power BI: one `.pbip` semantic model and report per project, executive summary drilling to detail | planned |

Session 1 landed no dbt models, no marts, and no Power BI artifacts. Its finish line was that every
executive question is *answerable from raw* — proven by the per-track question-coverage checks in
the L1 harness, not asserted. Session 2 answers them: the dbt project lives in
[`transform/`](transform/), types every column raw deliberately left as text, and builds one mart
per track. It reads the live warehouse locally and the committed fixtures in CI, which is why the
whole thing still builds on a fresh clone with no network.

---

## Integrity

`platform/reconcile/l1_integrity.py` runs after every ingestion and exits non-zero on any failure.
Output is grouped by project track. `platform/reconcile/l2_reconciliation.py` (`just l2`) is the
second layer: it asks whether the publisher's schema moved under us, and whether the semantic layer
loses anything between raw and staging. Both run offline, so CI carries both.

**All 13 raw sources are registered.** The thirteenth is the HUD ZIP-to-county crosswalk,
added in session 2 because FIN-E1 needs a county grain CFPB does not publish; it is held to the same
standard as the first twelve.

- **Count chain** — source-reported total → landing rows → raw parquet rows → DuckDB table rows,
  with differences itemized. A gap between a publisher's own count and the bulk file it serves is
  a timing observation; a gap anywhere downstream of landing is ours and fails the run.
- **Lossless conversion** — landing vs. raw control totals. Monetary sums where summing is
  meaningful; per-partition counts where it is not (CMS and CDC aggregates must never be summed as
  controls — mixed averaging grains make the sum meaningless).
- **Partition completeness** — every state, territory and the null-state partition for NFIP; an
  unbroken run of years for CFPB, HMDA, ACS and CPI-U; the fixed domains CMS and NFIP policies
  are allowed to have; and CDC's rollup rows named rather than summed.
- **Michigan geography gate** — the share of Michigan rows carrying no usable county, against the
  national share, for the eight sources that have a geography. Every publisher spells "no county"
  differently, so each source declares its own predicate.
- **Question coverage** — all fifteen executive questions, checked for the columns, geographies,
  benchmarks and deflators they need. This is session 1's finish line.
- **Publisher spot checks** — NFIP per-state counts asked of the FEMA API directly.

**A `SKIP` is never a silent pass.** Fixture mode skips anything that would call a publisher; a
stratified sample skips coverage questions it cannot answer; and a source whose landing has been
reclaimed skips the lossless re-proof until its next download.

Two more recipes back it up. `just reload` rebuilds every `raw` table from local parquet with no
network, so a deleted or corrupt warehouse never means re-downloading from a publisher. `just
fixture` rebuilds the committed test samples from local raw, deterministically — run it twice and
the files are byte-identical.

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

Raw parquet under `data/raw/` is the canonical copy, which is what the landing/raw split is for: if
the DuckDB file is deleted or corrupted, `just reload` rebuilds every table from it without touching
a publisher.

| Recipe | What it does |
|---|---|
| `just insurance` | Track 1 → raw → load → L1 |
| `just fintech` | Track 2 → raw → load → L1 |
| `just health` | Track 3 → raw → load → L1 |
| `just shared` | Reference denominators, deflator and ZIP-to-county crosswalk → raw → load → L1 |
| `just ingest-all` | All four, in any order |
| `just reload` | Rebuild every `raw` table from local parquet — no network |
| `just dbt build` | The semantic layer against the live warehouse (any dbt command works) |
| `just dbt-sources` | Regenerate dbt's source declarations from `ingest.registry` |
| `just fixture` | Rebuild the committed test fixtures from local raw, deterministically |
| `just check` | ruff + pytest (live-endpoint tests excluded) |
| `just ci` | Fixture mode: load committed samples, run L1, then build the semantic layer — all offline |
| `just clean-landing` | Reclaim disk once L1 has passed |

**Cost: $0.** All sources are free public APIs or bulk files. One key is required —
`api.census.gov` stopped serving unkeyed data requests, and signals it with an HTML page and an
HTTP 200 status rather than an auth error. Register free at
[api.census.gov/data/key_signup.html](https://api.census.gov/data/key_signup.html) and put it in
`.env` (gitignored); `.env.example` documents it, along with two other keys that remain optional.

Working rules — track separation, the all-varchar raw rule, link discovery, manifests, the Git
workflow — live in [CLAUDE.md](CLAUDE.md).
