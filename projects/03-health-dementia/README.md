# Project 3 — Health / Alzheimer's & Dementia

**Cognitive-health indicators and Medicare dementia prevalence and spend.**

No insurance or fintech data appears anywhere in this track. Build with `just health`.

## Datasets

| Source | Scope | Path | Access |
|---|---|---|---|
| CDC Alzheimer's Disease & Healthy Aging (`hfr9-rurv`) | Full dataset, 284,142 cells, 2015–2022 | `data/raw/health/cdc_healthy_aging/locationabbr=XX/` | Socrata resource API, paged by `:id`; dataset id resolved from the catalogue |
| CMS Medicare Geographic Variation | National / state / county, 36,994 rows × 246 cols, 2014–2024 | `data/raw/health/cms_geographic_variation/BENE_GEO_LVL=…/` | Discovered from `data.cms.gov/data.json`, bulk CSV |
| CMS Medicare Chronic Conditions — prevalence / spending | **not ingested — see below** | — | Absent from CMS's catalogue |

**The chronic-conditions gap.** The CMS Medicare chronic-conditions public files are not retrievable
from any discoverable route. Checked on 2026-08-11:

| Route | Result |
|---|---|
| `data.cms.gov/data.json` (CMS's own DCAT catalogue) | 159 datasets, no chronic-conditions entry |
| `healthdata.gov/data.json` (HHS-wide catalogue) | 777 CMS-published datasets, zero matching chronic condition / dementia / prevalence |
| `cms.gov/.../chronic-conditions` and the legacy `CC_Main` page | **404**; the legacy URL redirects to a dead page |
| CMS metastore, search, and the Mapping Medicare Disparities tool | client-rendered shells, no machine-readable index and no file links |
| Medicare Geographic Variation (ingested above) | no condition-level detail at all — only PQI admission rates for diabetes, COPD, hypertension, CHF, pneumonia, UTI, asthma and amputation |

So **HLT-E2 (dementia prevalence among MI Medicare beneficiaries) has no source**, and the decision on
it is open. What remains would be a guessed static URL (CLAUDE.md rule 3 forbids it), the Chronic
Conditions Data Warehouse under a data-use agreement (beneficiary-level restricted data — the ethics
rule forbids it), or a different measure entirely. The `slow` tripwire test in `tests/test_cms.py`
fails the day CMS republishes the dataset, so the gap cannot be quietly forgotten.

## Executive questions

- **HLT-E1** — What share of MI adults 45+ report subjective cognitive decline, and is MI
  significantly better or worse than national — CI-aware, not ranked noise?
- **HLT-E2** — What is dementia prevalence among MI Medicare beneficiaries and which direction has it
  moved over five years vs. national?
- **HLT-E3** — What does a MI beneficiary with dementia cost vs. without (constant dollars), and is
  that gap wider or narrower than national and than prior years?
- **HLT-E4** — Are MI caregiver-burden indicators improving or deteriorating?
- **HLT-E5** — Given 65+ population projections, is MI's need growing faster than its screening and
  care indicators?

## Drill bank

Latest-cycle indicator values with confidence intervals · per-beneficiary and per-capita spend ·
county spend variation · survey-cycle/annual grain only · multi-cycle trend vs. stated baseline ·
indicator→stratification (age band, sex, race/ethnicity; age-adjusted vs. crude) · MI vs. Great
Lakes neighbors vs. national · condition combinations (dementia + diabetes, + CHF) · service
category (inpatient, SNF, home health) · CI-aware comparisons that return *"no significant
difference"* when intervals overlap.

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

> Confidence-interval handling for HLT-E1, the CMS dataset-version provenance record, and the Excel
> distribution handling notes are written up in the `docs/readmes` PR at the end of session 1.
