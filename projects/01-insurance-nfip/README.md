# Project 1 — Insurance / Insurtech

**Flood insurance analytics on FEMA National Flood Insurance Program data.**

This is the only project in the repo that touches insurance data. No fintech or health data appears
here. Build with `just insurance`.

## Datasets

| Source | Scope | Path | Access |
|---|---|---|---|
| `NfipClaims` **v3** | National, 2,724,014 rows | `data/raw/insurance/nfip_claims/state=XX/` | Bulk parquet, partitioned by state |
| `NfipPolicies` **v3** | Michigan, 384,067 rows | `data/raw/insurance/nfip_policies/propertyState=MI/` | Paginated OpenFEMA API, keyset paging |
| `DisasterDeclarationsSummaries` v2 | National, 70,184 rows | `data/raw/insurance/fema_declarations/state=XX/` | Paginated OpenFEMA API — the event dimension |

**Why v3.** OpenFEMA froze the v2 `FimaNfipClaims` / `FimaNfipPolicies` datasets on 2026-06-01 and
deletes them on 2026-10-15. Building on a frozen dataset would quietly break INS-E5 the moment FEMA
computes its published figures from v3. `DisasterDeclarationsSummaries` has no v3 and is unaffected.
Two field changes matter downstream: v3 drops `censusTract`, so the Michigan geography gate targets
`censusGeoid`, and it renames `crsClassificationCode` → `crsClassCode`.

## Executive questions

These five drive the executive page in session 3. The IDs travel into mart documentation and
measure descriptions — do not renumber them.

- **INS-E1** — How much did flooding cost Michigan in the latest complete loss year — total paid,
  claim count — and is that above or below the prior year and the trailing 5-year average?
- **INS-E2** — Is MI severity (average paid per claim, constant dollars) rising faster or slower than
  the national median?
- **INS-E3** — What share of MI paid losses is event-driven (tied to federally declared disasters)
  vs. attritional?
- **INS-E4** — Are MI policies in force growing or shrinking, and how does take-up per 1,000 housing
  units compare to national?
- **INS-E5** — Do our totals tie to FEMA's published figures?

## Drill bank

Building/contents/ICC mix · $0-paid share · top counties by paid and per-capita paid · loss-year
ladder (YoY, trailing-5) · monthly seasonality (MI spring-melt/convective profile vs. national
hurricane profile) · development maturity (cumulative paid at 12/24/36 months, national triangles
with MI overlay) · event windows (paid within 30/60/90 days of declaration) · flood zone (is the
non-SFHA share growing?) · occupancy · pre/post-FIRM · CRS class · named events (Midland 2020 vs.
southeast-MI 2021) · frequency per 1,000 policies in force vs. national · constant-dollar severity
trend · top-5-county loss concentration.

---

## Scope decisions

**Claims are national; policies are Michigan only.** Every national source in this repo stays
national in raw (CLAUDE.md section 4) — but the policies file is 74.3M rows against roughly 384k for
Michigan, and the questions need a Michigan policy count, not a national one. So policies are the
documented exception: raw is filtered to `propertyState = 'MI'`, the reason is in the manifest under
`raw_scope`, and INS-E4's national benchmark comes from FEMA's own published state-level statistics
rather than from raw. L1 enforces the decision in both directions — a non-Michigan row in that table
is a scope leak and fails the run.

**Why keyset paging, not `$skip`.** The policies endpoint degrades badly with offset depth: 12.7s at
`$skip=0`, 232.3s at 80,000, and the connection dropped entirely at 110,000 — 29% of the way through
Michigan. Paging on ascending `id` instead is both faster and resumable, and the manifest records the
paging invariants (`ids_unique`, `ids_strictly_ascending`, `final_page_rows`) because this endpoint
also refuses to give a `$count` for a broad filter: it times out server-side after 60s and returns
503. With no publisher total to reconcile against, those invariants are the completeness proof.

## Territories and the unknown-state code

FEMA's published national totals include the territories, so dropping them would mean our numbers
quietly stop matching FEMA's — the exact failure INS-E5 exists to catch. All five are present, and
L1 warns rather than passes if any disappears.

The claims file also uses the literal code **`UN`** for claims whose state is unavailable — 16,441
rows, mostly pre-1990 with `reportedCity` set to "Currently Unavailable". These are real claims. They
stay in raw, they are named in the manifest, and they are counted in the L1 output, because a state
join that silently drops them would understate national totals by exactly that much.

## Michigan floods on a different calendar

This is the single most useful thing the claims data says about Michigan, and it shapes every
seasonality visual in the track. Share of claims by loss month:

| | Feb | Mar | Apr | May | Aug | Sep | Oct |
|---|---|---|---|---|---|---|---|
| **Michigan** | 9.6% | **16.7%** | **16.0%** | **14.8%** | 8.2% | 7.8% | 5.2% |
| **National** | 2.9% | 5.1% | 7.4% | 7.0% | **21.1%** | **21.7%** | **14.4%** |

Michigan's flood year peaks in **March through May** — snowmelt and spring convective storms. The
national profile peaks in **August through October**, which is hurricane season. A month-over-month
comparison of Michigan against the national average is therefore comparing two different physical
processes, and any "MI vs national" seasonality chart has to say so or it will read as Michigan
being anomalous when it is simply inland.

## County geography quality

Claims and policies both carry `censusGeoid`, whose first five characters are the state and county
FIPS. Coverage is good but not complete, and the gaps are not errors:

| | Michigan rows | no usable county | MI counties present |
|---|---|---|---|
| Claims | 14,938 | 372 (2.49%) | 82 of 83 |
| Policies | 384,067 | 1,315 (0.34%) | 77 of 83 |

Nationally 5.03% of claims have no usable county, so Michigan is *better* covered than average. The
missing counties are a fact about flood insurance rather than a load failure — claims are not filed
and policies are not sold in every county — which is why L1 reports the roster for these two sources
instead of requiring all 83. Note the null convention: `censusGeoid` is NULL when absent, while
`reportedZipCode` uses the **empty string**. Same publisher, same file, two different conventions;
a county-grain measure needs a denominator that states which rows it excluded.

## The small-cell display rule

County-level flood counts get small quickly — a rural Michigan county may have a handful of claims in
a year. Rates built on tiny denominators swing wildly and invite over-reading. Any county-grain
measure in this track shows the underlying claim count next to the rate, and suppresses or greys a
rate built on fewer than 10 claims rather than publishing a number that looks precise. Nothing here
is a personal record — NFIP claims are already redacted by FEMA — but a rate of "100%" drawn from one
claim is misleading regardless of privacy.
