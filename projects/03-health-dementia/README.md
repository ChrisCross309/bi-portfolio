# Project 3 — Health / Alzheimer's & Dementia

**Cognitive-health indicators, and the Medicare care burden that surrounds them.**

No insurance or fintech data appears anywhere in this track. Build with `just health`.

## Datasets

| Source | Scope | Path | Access |
|---|---|---|---|
| CDC Alzheimer's Disease & Healthy Aging (`hfr9-rurv`) | Full dataset, 284,142 cells, 2015–2022 | `data/raw/health/cdc_healthy_aging/locationabbr=XX/` | Socrata resource API, paged by `:id`; dataset id resolved from the catalogue |
| CMS Medicare Geographic Variation | National / state / county, 36,994 rows × 246 cols, 2014–2024 | `data/raw/health/cms_geographic_variation/BENE_GEO_LVL=…/` | Discovered from `data.cms.gov/data.json`, bulk CSV |
| CMS Medicare Chronic Conditions — prevalence / spending | **retired by the publisher — see below** | — | Retired effective 2026-06-15 |

## The retired dataset, and what it cost this track

CMS's Medicare chronic-conditions statistics — the only public file that carried **dementia
prevalence and dementia-attributable spending for Medicare beneficiaries at county grain** — were
**retired effective 2026-06-15**, roughly two months before this repo's health track was built. CMS
directs users to `data.cms.gov`, which does not carry a replacement.

This is a publisher decision, not a discovery failure, which matters for how it is handled: there is
nothing to keep looking for.

| Route | Result, checked 2026-08-11 and re-checked 2026-08-13 |
|---|---|
| `www2.ccwdata.org` chronic-condition tables and charts | **404** — retired effective 2026-06-15 |
| `data.cms.gov/data.json` (CMS's own DCAT catalogue, and its stated replacement) | 159 datasets; zero matching `dementia`, `alzheim`, `chronic`, `prevalence`, `multiple chronic` or `mcc` |
| `healthdata.gov/data.json` (HHS-wide) | 777 CMS-published datasets, zero matching |
| `cms.gov/.../chronic-conditions` and the legacy `CC_Main` page | **404**; the legacy URL redirects to a dead page |
| CMS metastore, search, and the Mapping Medicare Disparities tool | client-rendered shells, no machine-readable index and no file links |
| Medicare Geographic Variation (ingested above) | **zero** condition-level columns out of 246 — the closest are PQI admission rates for diabetes, COPD, hypertension, CHF, pneumonia, UTI, asthma and amputation |

**It cost two questions, not one.** HLT-E2 asked for dementia prevalence; HLT-E3 asked what a
beneficiary *with* dementia costs against one without. Both need the same condition dimension, and
nothing public still publishes it.

**Alternatives considered and rejected**, each against what the questions actually require —
dementia-specific, Medicare, Michigan county grain, a valid five-year direction, and current:

| Candidate | Why not |
|---|---|
| CDC PLACES `COGNITION` | Cognitive *disability* among adults 18+, not dementia and not Medicare. Model-based BRFSS estimates, and CDC advises against comparing releases across years — which invalidates the five-year direction. It also duplicates HLT-E1. |
| CDC Alzheimer's mortality (`alzheimer_disease_g30`, Socrata) | Genuinely dementia-specific and genuinely trended, but measures **mortality, not prevalence**, is published by state with no county grain, and the series ends in 2023. |
| Chronic Conditions Data Warehouse | Beneficiary-level restricted data under a data-use agreement — barred by this track's ethics rule. |
| A guessed or archived static URL | Barred by CLAUDE.md rule 3, and archived data is stale by construction. |

**Resolution: HLT-E2 and HLT-E3 were re-scoped onto the Medicare Geographic Variation data already
ingested here.** The IDs are unchanged, per CLAUDE.md section 2. Both keep every structural property
the originals had — Medicare, Michigan county grain, a national benchmark, five-year direction and
constant dollars — and lose only condition specificity. The track's genuinely cognitive content
lives in HLT-E1 and HLT-E4, both from the CDC source.

> **Read this before quoting either measure.** Skilled-nursing and home-health use are a *proxy* for
> long-term-care burden. Dementia is the leading driver of that burden, but it is not the only one,
> and **nothing in the re-scoped HLT-E2 or HLT-E3 measures dementia.** Neither can support a claim
> about dementia prevalence, dementia cost, or a change in either. Describing them as anything more
> than Medicare care-burden and cost measures would be the kind of conclusion CLAUDE.md section 8
> forbids.

The `slow` tripwire test in `tests/test_cms.py` still asserts that no chronic-conditions dataset
appears in CMS's catalogue. It now watches a retired dataset for republication rather than
confirming a failed search, and it fails the day CMS brings it back — which is the signal to revisit
this decision.

## Executive questions

- **HLT-E1** — What share of MI adults 50 and older report subjective cognitive decline, and is MI
  significantly better or worse than national — CI-aware, not ranked noise?
- **HLT-E2** — Are MI Medicare beneficiaries using skilled-nursing and home-health care at rates
  above or below national, and which direction over five years? *(Re-scoped — see the retired
  dataset above. A long-term-care burden measure, not a dementia measure.)*
- **HLT-E3** — What does a MI Medicare beneficiary aged 65+ cost against national in constant
  dollars, and is the gap widening or narrowing? *(Re-scoped — see above.)*
- **HLT-E4** — Are MI caregiver-burden indicators improving or deteriorating?
- **HLT-E5** — Given the growth in the 65+ population, is MI's need growing faster than its
  screening and care indicators? *(Worded as "projections" originally. **ACS publishes an estimate
  of the population that already exists, not a forecast**, and nothing in this repo projects one, so
  the question is answered from observed growth across two non-overlapping survey windows.)*

What answers the two re-scoped questions, so the claim is checkable rather than asserted:

| Question | Table | Columns | Filter |
|---|---|---|---|
| HLT-E2 | `raw.hlt_cms_geographic_variation` | `SNF_MDCR_STDZD_PYMT_PC`, `BENES_SNF_PCT`, `HH_MDCR_STDZD_PYMT_PC`, `BENES_HH_PCT` | `BENE_GEO_LVL='County'`, `BENE_GEO_CD LIKE '26%'` |
| HLT-E3 | `raw.hlt_cms_geographic_variation` + `raw.ref_cpi_u` | `TOT_MDCR_STDZD_PYMT_PC`, deflated by `CUUR0000SA0` period `M13` | `BENE_AGE_LVL='>=65'`, MI vs `National` |

Standardized payments are used throughout: CMS's `_STDZD_` columns remove geographic wage and
payment-policy differences, which is what makes a Michigan-vs-national comparison mean anything.
The CPI-U deflator was ingested for exactly this — see `data/raw/shared/cpi_u/`.

## Answered by

Every question resolves to a contracted mart in [`transform/models/marts/health/`](../../transform/models/marts/health/),
materialised into the `mart_hlt` schema. The IDs travel with the work (CLAUDE.md section 2):
each model's own description names the question it answers, and a test refuses to let a health
mart cite another track's ID.

| Question | Mart | Grain |
|---|---|---|
| HLT-E1 · HLT-E4 | `fct_hlt_cdc_mi_vs_national` | cycle × question × age group, MI beside national |
| HLT-E2 | `fct_hlt_medicare_service_county` | MI county × year × service × measure |
| HLT-E3 | `fct_hlt_medicare_cost_annual` | jurisdiction × year × age level |
| HLT-E5 | `fct_hlt_mi_population_65_plus` · `rpt_hlt_need_vs_capacity` | MI county × ACS vintage; indicator × comparison window |

`fct_hlt_cdc_indicators` sits under HLT-E1, HLT-E4 and HLT-E5's indicator side as the level
table -- every CDC cell, typed, catalogued and flagged.

**Both re-scoped questions say so in their own model descriptions.** `fct_hlt_medicare_service_county`
and `fct_hlt_medicare_cost_annual` state in the text a reader sees first that nothing in them
measures dementia.

### What the models found

- **HLT-E1 returns "no significant difference" in all 24 comparable cells** — nine cycles by
  three age groups, minus the three where Michigan published no 2016 row. Michigan's point
  estimate sits above national in most cycles (12.7 against 11.6 in 2019) and never
  significantly so. That is the finding, not a failure to find one.
- **HLT-E3's gap changed sign.** In constant 2025 dollars Michigan sat **$250 above** the
  national figure for 65-and-over beneficiaries in 2019 and **$374 below** it in 2024, crossing
  zero in 2022. A movement in relative standardized cost, and not a statement about dementia,
  about quality of care, or about Medicare Advantage beneficiaries, whose spending is not in
  this file.
- **HLT-E2 runs below national, not above.** Michigan counties' median ratio to the national
  rate is 0.751 for skilled nursing and 0.739 for home health.
- **HLT-E4's dementia-caregiving indicator is "not comparable", and that is the honest
  answer.** CDC's `**` footnote marks estimates from 2019 on as incomparable with earlier ones
  because the survey questions changed, and Michigan's only two usable points straddle it.
- **HLT-E5**: the 65+ population grew **12.6%** between the non-overlapping 2015–2019 and
  2020–2024 windows, while 38 of 57 indicator and age-group combinations read "need outpacing
  capacity".

### One constraint on HLT-E5 worth stating

Census restructured the ACS `S0101` subject table at the **2017 vintage**, and the line this
repo reads is *median age* before it — across all 3,220 counties the pre-2017 values run 18.0
to 66.0 against a median near 5,000 after. `stg_ref__acs5_subject` therefore publishes the 65+
estimate only from 2017 onward, and HLT-E5 runs on the eight vintages that are real rather than
fifteen with seven quietly wrong.

**And there is no pre-2017 line to switch to.** The 2016 vintage publishes the 65-and-over
population as a *share*, not a count — `S0101_C01_028E` reads 13.8 for Wayne County where the
2017 count reads 253,640. Recovering a count would mean multiplying that share by total
population, or summing the twelve `B01001` age brackets: a derived figure presented as the
publisher's, or the aggregation this track already refuses. Eight vintages is what ACS
supports, the 2019-to-2024 comparison sits inside them, and this is settled rather than
outstanding.

## Drill bank

Latest-cycle indicator values with confidence intervals · per-beneficiary and per-capita spend ·
county spend variation · survey-cycle/annual grain only · multi-cycle trend vs. stated baseline ·
indicator→stratification (age band, sex, race/ethnicity; age-adjusted vs. crude) · MI vs. Great
Lakes neighbors vs. national · service category (inpatient, SNF, home health, hospice) ·
age band (`<65`, `>=65`, `All`) · standardized vs. unstandardized payment · CI-aware comparisons
that return *"no significant difference"* when intervals overlap.

Condition-level drills — dementia combined with diabetes or CHF — are **not** in the bank. Nothing
ingested carries a condition dimension; see the retired dataset above.

## Three warnings that gate every model in this track

**1 · Aggregate grain.** These are aggregate files. **A row is a cell — a profile or geography × year
— not a beneficiary.** Nothing in them sums to patient counts. Suppression markers (`*`, blanks)
appear wherever counts are small; they are information, not nulls, and the all-varchar raw rule
preserves them exactly as published.

**2 · Ethics.** Public aggregate data only. **No PHI. No re-identification attempts. No
individual-level inference.** Nothing in this repo describes any person.

**3 · Honest grain.** Annual and survey-cycle only. No structure that invites fake daily or quarterly
reporting on annual data, and no interpolation to manufacture a trend line.

**4 · Two traps in the CDC file specifically.** Its `rowid` column is **not a key** — 284,142 rows
carry 36,046 distinct values, because it encodes the stratification categories and not their values.
Never test it for uniqueness and never join on it; the real grain is recorded in that source's
manifest. And `locationabbr` mixes 51 states and DC, three territories, four census regions and a
national `US` row **as peers** — summing across it double counts every state up to three times. The
rollups are kept deliberately, because HLT-E1 compares Michigan against them.

---

## Confidence intervals are not decoration

HLT-E1 asks whether Michigan is *significantly* better or worse than national. The CDC file publishes
`low_confidence_limit` and `high_confidence_limit` on every cell, and BRFSS is a self-reported
telephone survey — state estimates carry real sampling error. Ranking point estimates would
manufacture a story out of noise, so every comparison in this track uses the intervals and is allowed
to return **"no significant difference"** when they overlap. That is a finding, not a failure to find
one, and the executive page shows it as such rather than forcing a direction arrow.

A third of the file — 91,334 rows — has an empty `data_value` with a footnote symbol giving the
reason. Suppression is information, not a null:

| Symbol | Meaning |
|---|---|
| `****` | sample size too small to age-standardize |
| `~` | no data available |
| `#` | fewer than 50 states reporting |
| `&` | regional estimate may not represent every state in the region |

## CMS: original Medicare only, and three grains in one column

**The spending is FFS spending.** Beneficiaries in Medicare Advantage are counted in
`BENES_MA_CNT`, but their utilization and spending are *not* in this file. So per-capita spend is per
**fee-for-service** beneficiary, and a county with high MA penetration will look different for
reasons that have nothing to do with cost of care. HLT-E2 and HLT-E3 both state this.

**Use the standardized payment columns.** The `_STDZD_` columns remove geographic wage adjustment and
are the ones that make a Michigan-vs-national comparison mean anything. The unstandardized twins
measure something real but different, and mixing them is an easy, invisible error.

**Age levels double count.** County rows are `All` only, while state and national rows also appear as
`<65` and `>=65` — which sum to their own `All`. Every measure filters to one age level; forgetting
to is how a state total ends up twice its true size.

**One unassigned-county bucket.** `26000` / `MI-UNKNOWN` holds Michigan beneficiaries whose county
could not be assigned — one row per year, eleven in total. Real people, kept in raw. A county-level
denominator either includes them explicitly or states that it does not; the loader separates them
from the 83-county roster so neither choice happens by accident.

## Dataset-version provenance

CMS republishes this file annually under a new URL, and the DCAT `temporal` field names only the
newest year (`2024-01-01/2024-12-31`) even though the file covers 2014–2024. So the manifest records
the resolved URL, the DCAT `modified` date, the identifier, the accrual periodicity and the other
distributions CMS offers — enough to say months later exactly which publication a number came from.
The URL is resolved at runtime from `data.cms.gov/data.json` and never hardcoded, so next year's
republication is picked up rather than missed.

---

> The Excel-distribution notes this file used to promise are not coming: the legacy CMS PUF expected
> to arrive as a workbook never materialised, every landed source is CSV, TSV, JSON or parquet, and
> `openpyxl` has been dropped.
