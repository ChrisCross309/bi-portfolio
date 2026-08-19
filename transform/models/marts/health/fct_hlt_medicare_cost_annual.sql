{{ config(materialized = 'table') }}

/*
  **HLT-E3.** Standardized Medicare cost per fee-for-service beneficiary, by jurisdiction,
  year and age level, in nominal and constant dollars.

  **A row is a geographic aggregate, never a beneficiary.** Public aggregate data only: no
  PHI, no re-identification, no individual-level inference.

  ## This is not a dementia measure

  HLT-E3 originally asked what a Medicare beneficiary *with* dementia costs against one
  without. CMS retired the only public file carrying that, effective 2026-06-15, and the
  question was re-scoped onto this data keeping its ID. What remains is **cost per
  fee-for-service beneficiary** -- Medicare, Michigan, a national benchmark, five-year
  direction and constant dollars, and no condition specificity whatever. **Nothing in this
  model measures dementia**, and no number in it can support a claim about dementia cost.

  ## Why this is a state question and not a county one

  The 65-and-over slice only exists at state and national level: CMS publishes county rows at
  `age_level = 'All'` only. So HLT-E3 is answered at state grain against the national row, and
  every row here carries its `age_level` explicitly. **The three levels double count** --
  `<65` and `>=65` sum to their own `All` -- so a consumer filters to one or gets a total
  twice its true size. All three are kept because the drill bank asks for the age split.

  ## Two rows at 'State' level are not states

  CMS's state roster is 55 values: 50 states, DC, Puerto Rico, the US Virgin Islands, and two
  aggregates -- `Territory` and `ZZ` -- both published with a **NULL geo_code**. They are kept
  because dropping a publisher's own rollup quietly is how a total stops reconciling, and
  `is_jurisdiction` is how they are excluded on purpose.

  ## Constant dollars, and why the base year is a variable

  Nominal dollars would measure inflation as much as cost of care, so the comparison runs on
  CPI-U's unadjusted annual average through `deflate_to_constant_dollars`. INS-E2 uses the
  same macro against the same series: a deflator applied even slightly differently to two
  measures makes them incomparable while looking fine.

  The finding this produces is the one worth stating carefully. In constant dollars Michigan
  sat **$250 above** the national figure for 65-and-over beneficiaries in 2019 and **$374
  below** it in 2024 -- the gap changed sign, crossing zero in 2022. That is a real movement
  in relative standardized cost and it is **not** a statement about dementia, about quality of
  care, or about Medicare Advantage beneficiaries, whose spending is not in this file at all.
*/

WITH cost AS (

    SELECT
        m.measure_year,
        m.period_key,
        m.period_grain,
        m.geo_level,
        m.geo_code,
        m.geo_description,
        m.age_level,
        m.measure_value     AS cost_per_capita,
        m.is_suppressed
    FROM {{ ref('int_hlt__cms_service_measures') }} m
    WHERE m.service_code = 'TOT'
      AND m.measure_kind = 'per_capita'
      -- Standardized only: the unstandardized twin measures something real and different,
      -- and mixing them across geographies is the easiest invisible error in this track.
      AND m.is_standardized
      AND m.geo_level IN ('State', 'National')

),

national AS (

    SELECT measure_year, age_level, cost_per_capita AS national_cost_per_capita
    FROM cost
    WHERE geo_level = 'National'

),

enrolment AS (

    SELECT
        measure_year,
        geo_level,
        geo_code,
        geo_description,
        age_level,
        ma_participation_rate,
        ffs_beneficiary_denominator,
        benes_total_cnt,
        bene_avg_age,
        bene_dual_eligible_pct
    FROM {{ ref('stg_hlt__cms_geographic_variation') }}
    WHERE geo_level IN ('State', 'National')

),

joined AS (

    SELECT
        c.measure_year,
        c.period_key,
        c.period_grain,
        c.geo_level,
        c.geo_code,
        c.geo_description,
        c.age_level,
        c.cost_per_capita,
        c.is_suppressed,
        n.national_cost_per_capita,

        -- 50 states, DC, PR and VI are jurisdictions; `Territory` and `ZZ` are the
        -- publisher's own aggregates and carry no FIPS.
        c.geo_level = 'State' AND c.geo_code IS NOT NULL    AS is_jurisdiction,
        COALESCE(c.geo_code = '26', FALSE)                  AS is_michigan,

        e.ma_participation_rate,
        e.ffs_beneficiary_denominator,
        e.benes_total_cnt,
        e.bene_avg_age,
        e.bene_dual_eligible_pct

    FROM cost c
    LEFT JOIN national n
        ON  n.measure_year = c.measure_year
        AND n.age_level    = c.age_level
    LEFT JOIN enrolment e
        ON  e.measure_year = c.measure_year
        AND e.geo_level    = c.geo_level
        AND e.age_level    = c.age_level
        -- Not geo_code: CMS publishes `Territory` and `ZZ` at State level with a NULL code,
        -- so a NULL-tolerant key matches each of them against both and fans the join out.
        -- geo_description is distinct across all 55 State rows and the National row.
        AND e.geo_description = c.geo_description

)

SELECT
    j.measure_year,
    j.period_key,
    'year:' || j.period_key                                  AS period_id,
    j.period_grain,

    j.geo_level,
    j.geo_code,
    j.geo_description,
    s.state_name,
    j.is_jurisdiction,
    j.is_michigan,

    -- The three levels sum into each other; filter to one or double count.
    j.age_level,

    j.cost_per_capita,
    j.national_cost_per_capita,
    j.cost_per_capita - j.national_cost_per_capita           AS gap_to_national,

    {{ deflate_to_constant_dollars('j.cost_per_capita', 'j.measure_year') }}
                                                             AS cost_per_capita_constant,
    {{ deflate_to_constant_dollars('j.national_cost_per_capita', 'j.measure_year') }}
                                                             AS national_cost_per_capita_constant,
    -- Parenthesised deliberately. The macro interpolates its argument bare, so an
    -- unbracketed `a - b` deflates only `b` and the difference comes out wrong by the whole
    -- of `a` -- silently, because the result is still a plausible dollar figure.
    {{ deflate_to_constant_dollars('(j.cost_per_capita - j.national_cost_per_capita)', 'j.measure_year') }}
                                                             AS gap_to_national_constant,
    {{ var('deflator_base_year') }}                          AS constant_dollar_base_year,

    -- Fee-for-service context. Spending for Medicare Advantage beneficiaries is not in this
    -- file, so a jurisdiction with high MA penetration differs for reasons unrelated to cost.
    j.ma_participation_rate,
    j.ffs_beneficiary_denominator,
    j.benes_total_cnt,
    j.bene_avg_age,
    j.bene_dual_eligible_pct,

    j.is_suppressed

FROM joined j
LEFT JOIN {{ ref('dim_state') }} s
    ON s.state_fips = j.geo_code
