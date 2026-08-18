{{ config(materialized = 'table') }}

/*
  **HLT-E2.** Michigan county use of long-term-care services against the national rate, by
  year, service and measure.

  **A row is a geographic aggregate, never a beneficiary.** Nothing here sums to a patient
  count. Public aggregate data only: no PHI, no re-identification, no individual-level
  inference.

  ## This is not a dementia measure

  HLT-E2 originally asked for dementia prevalence. CMS retired the only public file carrying
  it at county grain, effective 2026-06-15, and the question was re-scoped onto this data
  keeping its ID. Skilled nursing, home health, hospice, inpatient rehabilitation and
  long-term care hospital use are a **long-term-care burden measure**. Dementia is a leading
  driver of that burden and is not the only one, and **nothing in this file measures
  dementia** -- there is no condition dimension in any of its 246 columns. No number in this
  model can support a claim about dementia prevalence, dementia cost, or a change in either.

  ## Every comparison here is county 'All ages' against national 'All ages'

  CMS publishes county rows at `age_level = 'All'` only; `<65` and `>=65` exist at state and
  national level, where they sum to their own `All`. So the benchmark is taken at `All` on
  both sides and `age_level` is a column rather than an assumption. HLT-E3, which does need
  `>=65`, is a state-versus-national question for exactly this reason.

  ## Standardized payments, and fee-for-service only

  `per_capita` uses CMS's `_STDZD_` columns, which remove geographic wage and payment-policy
  differences and are the only ones that make a Michigan-versus-national comparison mean
  anything. `user_pct` has no standardized twin and is carried as published.

  The spending is **fee-for-service**. Medicare Advantage beneficiaries are counted in the
  file but their utilization and spending are not in it, so `ma_participation_rate` is carried
  beside every rate: a county with high MA penetration looks different for reasons that have
  nothing to do with cost of care.

  ## Suppressed and zero are different facts, and both are common

  Measured over the 924 Michigan county-years in each series:

      SNF      13 suppressed     0 true zeros
      HH       11               0
      HOSPC    25               0
      IRF     161               2
      LTCH    462             125

  **Half of the long-term-care-hospital cells are suppressed** -- LTCHs are rare enough that
  most Michigan counties never have one, so that series is not usable at county grain and
  `is_small_cell_service` says so. A true zero means the service was not used; a suppressed
  cell means CMS withheld a small count. Averaging over either without knowing which is a
  different mistake, so they are separate columns and neither is coerced to the other.

  ## The unassigned county stays

  `26000` / `MI-UNKNOWN` holds Michigan beneficiaries whose county could not be assigned --
  one row per year, eleven in total, real people. `is_unassigned_county` marks them so a
  county rollup includes them explicitly or excludes them explicitly, and never by accident.
*/

WITH long_term_care AS (

    SELECT
        m.measure_year,
        m.period_key,
        m.period_grain,
        m.geo_level,
        m.geo_code,
        m.geo_description,
        m.county_fips,
        m.is_michigan_county,
        m.age_level,
        m.service_code,
        m.service_name,
        m.measure_kind,
        m.is_standardized,
        m.measure_value,
        m.is_suppressed
    FROM {{ ref('int_hlt__cms_service_measures') }} m
    WHERE m.is_long_term_care
      -- County grain publishes 'All' only; the benchmark is taken at the same level.
      AND m.age_level = 'All'
      AND (
            (m.measure_kind = 'per_capita' AND m.is_standardized)
         OR (m.measure_kind = 'user_pct'   AND NOT m.is_standardized)
      )

),

national AS (

    SELECT
        measure_year,
        service_code,
        measure_kind,
        measure_value       AS national_value,
        is_suppressed       AS national_is_suppressed
    FROM long_term_care
    WHERE geo_level = 'National'

),

michigan AS (

    SELECT * FROM long_term_care WHERE is_michigan_county

),

-- How much of each service CMS withholds statewide. A series suppressed in most counties is
-- not a series, and the flag travels on every row of it rather than living in a footnote.
service_coverage AS (

    SELECT
        service_code,
        measure_kind,
        COUNT(*)                                            AS county_year_cells,
        COUNT(*) FILTER (WHERE is_suppressed)                AS suppressed_cells,
        100.0 * COUNT(*) FILTER (WHERE is_suppressed) / COUNT(*) AS suppressed_pct
    FROM michigan
    GROUP BY 1, 2

),

-- Fee-for-service context, carried per county-year rather than recomputed downstream.
enrolment AS (

    SELECT
        measure_year,
        county_fips,
        ma_participation_rate,
        ffs_beneficiary_denominator,
        is_unassigned_county
    FROM {{ ref('stg_hlt__cms_geographic_variation') }}
    WHERE is_county_row AND age_level = 'All'

)

SELECT
    m.measure_year,
    m.period_key,
    m.period_grain,

    m.county_fips,
    g.county_name,
    m.geo_description                                       AS cms_county_label,
    COALESCE(g.is_michigan, FALSE)                          AS is_michigan,
    COALESCE(e.is_unassigned_county, FALSE)                 AS is_unassigned_county,

    -- Stated, never assumed: both sides of the comparison are all-ages.
    m.age_level,

    m.service_code,
    m.service_name,
    m.measure_kind,
    m.is_standardized,

    m.measure_value                                         AS county_value,
    n.national_value,
    CASE
        WHEN n.national_value > 0 AND NOT m.is_suppressed
        THEN m.measure_value / n.national_value
    END                                                     AS ratio_to_national,
    CASE
        WHEN NOT m.is_suppressed THEN m.measure_value - n.national_value
    END                                                     AS gap_to_national,

    -- Suppressed, zero and absent are three different facts.
    m.is_suppressed,
    COALESCE(NOT m.is_suppressed AND m.measure_value = 0, FALSE)     AS is_true_zero,
    COALESCE(n.national_is_suppressed, FALSE)               AS national_is_suppressed,

    sc.suppressed_cells                                     AS service_suppressed_cells,
    ROUND(sc.suppressed_pct, 2)                             AS service_suppressed_pct,
    -- Over a third of a series withheld is not a series worth ranking counties on.
    sc.suppressed_pct >= 33.0                               AS is_small_cell_service,

    -- Fee-for-service caveat, on every row so a mart consumer cannot forget it.
    e.ma_participation_rate,
    e.ffs_beneficiary_denominator

FROM michigan m
LEFT JOIN national n
    ON  n.measure_year = m.measure_year
    AND n.service_code = m.service_code
    AND n.measure_kind = m.measure_kind
LEFT JOIN service_coverage sc
    ON  sc.service_code = m.service_code
    AND sc.measure_kind = m.measure_kind
LEFT JOIN enrolment e
    ON  e.measure_year = m.measure_year
    AND e.county_fips  = m.county_fips
LEFT JOIN {{ ref('dim_geography_county') }} g
    ON g.county_fips = m.county_fips
