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

## The bulk archive, not the search API

CFPB publishes the same complaints two ways, and only one of them can be trusted for a backfill.
The search API is Elasticsearch-backed with a deep-paging window limit, and a reported bug where the
`frm` offset fails to advance results — page through it and you can silently receive the same rows
again, ending up with a plausible-looking count made of duplicates. The bulk archive has no such
failure mode.

So the archive is the source of record, and the search API is called exactly **once per run, for a
count**, which becomes L1's control total. Two smaller notes from probing it: the count endpoint
requires `no_highlight=true` or it returns a bare 404 on an otherwise valid query, and the archive's
`last-modified` header is CFPB's only refresh signal — there is no catalogue entry for it.

## What the complaint data is not

Four caveats that change how every FIN measure must be worded:

- **Recent windows are incomplete.** A complaint publishes only after the company responds or 15
  days elapse. "Latest complete month" therefore means a *closed publication window*, not the most
  recent calendar month, and a chart that plots the current month will always show a fake decline.
- **It is not a census of complaints.** Complaints referred to other regulators — depositories under
  $10B — are absent entirely. FIN-E1's per-capita rate measures complaints *to the CFPB*, not
  consumer dissatisfaction.
- **Narratives are a consented subset.** They exist only where the consumer agreed to publication,
  so any text analysis describes that subset and not all complainants.
- **Some ZIP codes are masked**, and they stay as text per the all-varchar rule.

**There is no county column at all.** CFPB publishes state and ZIP only, so FIN-E1's county grain
comes from the HUD USPS ZIP-to-county crosswalk, landed as a domain-neutral `shared` reference
alongside the population denominators and the deflator. The Michigan geography gate reports the gap
outright rather than leaving it to be discovered during modelling.

### The allocation rule, stated up front

**381 of Michigan's 1,102 ZIP codes — 34.6% — cross a county line.** A ZIP is a postal delivery
route, not an area, so turning one into a county is a weighted allocation, never a lookup. HUD
publishes the weights: the share of each ZIP's residential, business and other addresses falling in
each county, rebuilt quarterly from USPS delivery data. The rule this track uses, and states on
every county-grain complaint measure:

1. A complaint with a full 5-digit ZIP is allocated across counties by **`res_ratio`**, the
   residential address share — a complaint is filed by a household, so that is the right weight.
2. **3,571 ZIPs nationally have no residential addresses at all** — PO-box and business-only ZIPs,
   where `res_ratio` sums to zero. Those fall back to **`tot_ratio`**, all address types.
   **67 Michigan complaints sit in such a ZIP**, and a rule using `res_ratio` alone would silently
   drop every one of them.
3. A complaint whose ZIP is masked to three digits is allocated by the weighted aggregate of every
   ZIP sharing that prefix. One with no usable ZIP lands in an explicit **unallocated** bucket.

Counts are therefore **fractional and never rounded up**, and the allocated county counts re-sum
exactly to the state total. Michigan ZIPs split 337,178 full · 27,820 masked to three digits ·
1,728 fully masked · 8 null · 1 malformed, so roughly 92% allocate directly.

The crosswalk moves each quarter, so a figure allocated with one vintage is not identical to the
same figure allocated with the next; the vintage that served each run is recorded in its manifest.

## Why HMDA raw is Michigan only

A year of national LAR runs to tens of millions of rows; Michigan is roughly 400k. This is the
documented size exception CLAUDE.md section 4 allows, with the reasoning recorded in the manifest
under `raw_scope`, and the national benchmark comes from the publisher's own aggregation endpoint
rather than from raw. Those per-year control totals are recorded in the manifest, which is where
FIN-E4's national comparison reads from.

**The two control totals disagree, and the manifest records both.** Summing the aggregation buckets
and summing the per-institution filer counts are independent statements about the same slice. They
match exactly for 2019–2023 and differ for 2018 (+89), 2024 (+2,688) and 2025 (+2,553). Landed rows
matched the aggregation exactly in all eight years, so it is the filer sum that undercounts — most
likely rows whose LEI is absent from the published filer list. Cause not established at the
publisher; the record-level total is what landed rows are compared against.

## The served-vintage trap

Every year's Michigan extract resolves to the **same filename**. The generated file is named for a
hash of the *filter* (`state_MI`), the year appears only in the URL path, and
`content-disposition` says `state_MI.csv` for all eight years. Naming landing files after the URL —
what every other fetcher here does — would have written eight years into one file and left a raw
layer that looked complete. Landing names come from the year requested, and a partition check proves
each file held the year it was asked for.

Related: which *edition* served a year moves over time. 2018–2022 came from `three-year`, 2023–2024
from `one-year`, 2025 from `snapshot`. That changes nothing about the rows but everything about which
publication they came from, so the edition is recorded per year in the manifest.

## Sentinels that a numeric type would destroy

HMDA is the strongest argument in this repo for the all-varchar raw rule. `loan_term` and its
neighbours contain the literal string **`Exempt`** where a small filer claimed the partial exemption.
Applicant age arrives as the codes **`8888`** and **`9999`** rather than nulls.
`debt_to_income_ratio` mixes bare numbers with ranges like `20%-<30%`. County is as-filed: 44,167
Michigan rows carry `county_code = 'NA'` and 254 carry an out-of-state FIPS on a Michigan filing —
1.1% with no usable Michigan county. Typing any of these on the way in would silently destroy
information; that is session 2's job, column by column, deliberately.

## Joining the two datasets

FIN-E5 needs complaints per $1B originated, which means matching a CFPB **company name** to an HMDA
**LEI**. Nothing published joins them. That is an entity-resolution problem for session 2 with a
stated match rate and an explicit unmatched bucket — raw's job is only to keep both sides intact,
which the question-coverage check verifies.
