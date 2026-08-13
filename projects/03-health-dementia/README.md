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

- **HLT-E1** — What share of MI adults 45+ report subjective cognitive decline, and is MI
  significantly better or worse than national — CI-aware, not ranked noise?
- **HLT-E2** — Are MI Medicare beneficiaries using skilled-nursing and home-health care at rates
  above or below national, and which direction over five years? *(Re-scoped — see the retired
  dataset above. A long-term-care burden measure, not a dementia measure.)*
- **HLT-E3** — What does a MI Medicare beneficiary aged 65+ cost against national in constant
  dollars, and is the gap widening or narrowing? *(Re-scoped — see above.)*
- **HLT-E4** — Are MI caregiver-burden indicators improving or deteriorating?
- **HLT-E5** — Given 65+ population projections, is MI's need growing faster than its screening and
  care indicators?

What answers the two re-scoped questions, so the claim is checkable rather than asserted:

| Question | Table | Columns | Filter |
|---|---|---|---|
| HLT-E2 | `raw.hlt_cms_geographic_variation` | `SNF_MDCR_STDZD_PYMT_PC`, `BENES_SNF_PCT`, `HH_MDCR_STDZD_PYMT_PC`, `BENES_HH_PCT` | `BENE_GEO_LVL='County'`, `BENE_GEO_CD LIKE '26%'` |
| HLT-E3 | `raw.hlt_cms_geographic_variation` + `raw.ref_cpi_u` | `TOT_MDCR_STDZD_PYMT_PC`, deflated by `CUUR0000SA0` period `M13` | `BENE_AGE_LVL='>=65'`, MI vs `National` |

Standardized payments are used throughout: CMS's `_STDZD_` columns remove geographic wage and
payment-policy differences, which is what makes a Michigan-vs-national comparison mean anything.
The CPI-U deflator was ingested for exactly this — see `data/raw/shared/cpi_u/`.

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

> Confidence-interval handling for HLT-E1 and the CMS dataset-version provenance record are written
> up in the `docs/readmes` PR at the end of session 1. The Excel-distribution notes this line used to
> promise are not coming: the legacy CMS PUF that was expected to arrive as a workbook never
> materialised, every landed source is CSV, TSV, JSON or parquet, and `openpyxl` has been dropped.
