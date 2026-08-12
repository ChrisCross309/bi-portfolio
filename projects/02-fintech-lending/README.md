# Project 2 — Fintech / Consumer Lending

**Consumer financial complaints and mortgage lending.**

No insurance data appears anywhere in this track. Build with `just fintech`.

## Datasets

| Source | Scope | Path | Access |
|---|---|---|---|
| CFPB Consumer Complaint Database | National | `data/raw/fintech/cfpb_complaints/` | Bulk archive, partitioned by `year(date_received)` |
| HMDA LAR | Michigan, 2018–2025 (4.0M rows) | `data/raw/fintech/hmda_lar/activity_year=YYYY/` | Data Browser API, one filtered extract per year |
| HMDA filers by year | National (39,381 rows) | `data/raw/fintech/hmda_institutions/period=YYYY/` | Data Browser API, `filers` endpoint |

The filer list is the institution reference: LEI → name, plus each filer's own record count for
the year. The transmittal sheet and panel file carry more institution detail but are published only
inside the annual snapshot bundles, whose URLs cannot be resolved from any machine-readable index —
S3 listing is denied and the publication page is client-rendered — so taking them would mean
hardcoding a guessed URL. See CLAUDE.md rule 3.

The datasets are fully separate entities in raw. They join only in session 2, at institution
level, for FIN-E5.

## Executive questions

- **FIN-E1** — How many complaints did MI consumers file in the latest complete month/quarter, and is
  per-capita volume above or below national and trending which way?
- **FIN-E2** — Which product is MI's largest and fastest-growing complaint driver?
- **FIN-E3** — Are companies serving MI responding timely and granting relief at better or worse
  rates than a year ago and than nationally?
- **FIN-E4** — How did MI mortgage origination volume and denial rate move vs. prior year and vs.
  national?
- **FIN-E5** — Which large MI lenders over- or under-perform peers on complaints per $1B originated?

## Drill bank

Timely-response % · relief % · median and p90 days-to-response · top companies by MI volume · the
full daily→weekly→monthly→quarterly→YTD ladder (CFPB updates daily — the narrative engine's home
domain) · application-cohort vintage curves (structurally identical to loss development) ·
product→sub-product→issue · company · channel · county/ZIP · consumer tags (is the older-American
share rising?) · HMDA county, purpose, loan type, income band, denial-reason mix · per-capita rate
vs. national · peer-median benchmarks.

## Fair-lending framing

Denial-rate gaps surfaced in this project are **descriptive gaps warranting review, never
conclusions.** HMDA carries no credit score and no debt-to-income ratio, so it cannot support a
finding of discrimination. Every visual and every measure description states this.

---

> The CFPB bulk-vs-search-API decision, complaint publication-window and coverage caveats, the HMDA
> scope rationale, and the served-vintage trap are written up in the `docs/readmes` PR at the end of
> session 1.
