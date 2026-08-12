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

> Scope decisions, the national-policies rationale, territory handling, the MI-seasonality note, the
> county-geography-quality caveat, and the small-cell display rule are written up in the
> `docs/readmes` PR at the end of session 1.
