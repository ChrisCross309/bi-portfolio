{{ config(materialized = 'table') }}

/*
  Flood claims aggregated to county and loss month, national. Answers INS-E1 and INS-E2, and
  carries the seasonality and top-county drills.

  National rather than Michigan-only, because INS-E2 compares Michigan severity against the
  national median across states and INS-E1's benchmark is the same. Michigan is a filter on
  `is_michigan`, applied where a reader can see it, not baked into the grain.

  ## Two different questions about "complete"

  `is_complete_loss_year` asks whether the year is over -- the file runs to July 2026, so 2026
  is a partial year and plotting it beside a full one shows a collapse that is an artifact of
  the download date.

  `is_development_mature` asks something harder and more useful. A flood claim is not paid the
  day it is filed: nationally the median gap from loss to last payment is 45-60 days and the
  90th percentile runs 140-220. So a loss year keeps accumulating paid dollars for roughly a
  year after it ends, and **the most recent complete year always understates itself**. Twelve
  months past year end is the threshold used here. With data through July 2026 that makes 2025
  complete but not yet mature: its paid total will still rise.

  INS-E1 asks for "the latest complete loss year". Both flags are published so the answer can
  say which sense it means, and a chart that ignores the second will show a decline that is
  really a settlement lag.

  ## Constant dollars

  INS-E2 compares severity over time, so nominal dollars would measure inflation as much as
  flood cost. The deflated columns use CPI-U's unadjusted annual average, and stop where BLS
  does -- there is no 2026 annual average, so 2026 has no constant-dollar figure at all rather
  than a silently nominal one.

  ## Gross and net

  Both are carried, because they answer different questions and staging refuses to choose.
  Gross skips the 567,044 claims that closed without payment; net counts them as zero. The
  county roster is unaffected either way.

  ## The building characteristics, and what they do to the grain

  Flood zone, occupancy, pre/post-FIRM and CRS class were only ever available in the
  Michigan detail table, which left every one of those drills -- "is the non-SFHA share
  growing?", occupancy mix, the CRS discount -- answerable for Michigan and nowhere else,
  even though the national file carries all of them. They are here now, and the grain is
  county x loss month x those six columns: 568,839 rows against 147,131. Every prior
  measure still aggregates to exactly what it did before, because nothing was filtered --
  only split.

  ## county_population is context, not a measure

  It is the denominator the per-capita drill needs, and it repeats across every
  characteristic row in a county-month, because population is a property of the county and
  not of the flood zone. **Summing it multiplies it.** Take MAX or ANY_VALUE within a
  county-month, or divide inside the row and aggregate the rate's components instead.
  `assert_denormalised_denominators_are_constant` proves the value never varies within its
  group, so the repetition is safe to collapse rather than something to verify by hand.

  Frequency per 1,000 policies needs no column here: `fct_ins_policies_mi_monthly` already
  is that denominator at county and month, and joins on `county_fips` and `loss_year_month`.
  It is Michigan-only, because raw policies are.
*/

WITH claims AS (

    SELECT * FROM {{ ref('stg_ins__nfip_claims') }}

),

data_extent AS (

    SELECT
        MAX(date_of_loss)                       AS max_loss_date,
        CAST(year(MAX(date_of_loss)) AS SMALLINT) AS max_loss_year
    FROM claims

),

aggregated AS (

    SELECT
        c.year_of_loss                          AS loss_year,
        c.loss_month,
        c.loss_year_month,
        c.state_code,
        c.county_fips,
        c.is_state_unknown,

        -- National for the first time. These were Michigan-only until now, because they
        -- lived solely in the detail table.
        c.rated_flood_zone,
        c.flood_zone_current,
        c.occupancy_type,
        c.pre_firm_indicator,
        c.post_firm_construction_indicator,
        c.crs_class_code,

        COUNT(*)                                        AS claim_count,
        COUNT(*) FILTER (WHERE c.is_closed_without_payment)  AS closed_without_payment_count,
        COUNT(*) FILTER (WHERE c.is_zero_paid)          AS zero_paid_count,
        COUNT(*) FILTER (WHERE NOT c.has_usable_county) AS no_county_count,

        SUM(c.amount_paid_on_building_claim)            AS paid_building,
        SUM(c.amount_paid_on_contents_claim)            AS paid_contents,
        SUM(c.amount_paid_on_icc_claim)                 AS paid_icc,
        SUM(c.total_amount_paid)                        AS paid_total,
        SUM(c.total_net_payment)                        AS net_paid_total,

        SUM(c.total_building_insurance_coverage)        AS building_coverage,
        SUM(c.total_contents_insurance_coverage)        AS contents_coverage

    FROM claims c
    GROUP BY ALL

),

population AS (

    -- One row per county per vintage. ACS 5-year windows **overlap** -- 2018 falls inside
    -- five of them -- so matching on "the window contains this year" returns many rows and
    -- fans the join out, repeating every measure once per vintage. One vintage per year: the
    -- estimate ending in that year, clamped to the published range.
    SELECT county_fips, vintage_year, vintage_window_start, vintage_window_end, total_population
    FROM {{ ref('stg_ref__acs5_detailed') }}
    WHERE geo_level = 'county'
      AND total_population IS NOT NULL

),

vintage_bounds AS (

    -- NFIP claims start in 1978 and ACS at the 2010 vintage, so most loss years have no
    -- contemporaneous estimate at all. Clamping gives them the nearest published one and
    -- `population_is_extrapolated` says which rows those are -- which is nearly all of them
    -- before 2010, and a per-capita rate on a 1980s loss year should be read accordingly.
    SELECT MIN(vintage_year) AS earliest_vintage, MAX(vintage_year) AS latest_vintage
    FROM population

)

SELECT
    a.loss_year,
    a.loss_month,
    a.loss_year_month,
    CAST(a.loss_year AS VARCHAR)                        AS loss_year_key,

    a.state_code,
    a.county_fips,
    g.county_name,
    g.state_name,
    COALESCE(g.is_michigan, FALSE)                      AS is_michigan,
    a.is_state_unknown,

    a.rated_flood_zone,
    a.flood_zone_current,
    a.occupancy_type,
    a.pre_firm_indicator,
    a.post_firm_construction_indicator,
    a.crs_class_code,

    a.claim_count,
    a.closed_without_payment_count,
    a.zero_paid_count,
    a.no_county_count,

    a.paid_building,
    a.paid_contents,
    a.paid_icc,
    a.paid_total,
    a.net_paid_total,
    a.building_coverage,
    a.contents_coverage,

    -- Severity. NULL rather than zero where nothing paid, so an average over it is an average
    -- of claims that actually paid.
    CASE WHEN a.claim_count > 0 THEN a.paid_total / a.claim_count END
                                                        AS avg_paid_per_claim,

    -- INS-E2 in constant dollars. NULL for any year BLS has not published an annual average
    -- for, which is deliberate: a nominal figure sitting in a constant-dollar column is worse
    -- than a gap.
    {{ deflate_to_constant_dollars('a.paid_total', 'a.loss_year') }}
                                                        AS paid_total_constant,
    CASE
        WHEN a.claim_count > 0
        THEN {{ deflate_to_constant_dollars('a.paid_total', 'a.loss_year') }} / a.claim_count
    END                                                 AS avg_paid_per_claim_constant,

    -- completeness ---------------------------------------------------------------
    a.loss_year < e.max_loss_year                       AS is_complete_loss_year,
    -- Twelve months past year end, which is roughly where claim payment stops developing.
    e.max_loss_date >= make_date(a.loss_year + 1, 12, 31)
                                                        AS is_development_mature,

    -- Denominator for the per-capita drill. Repeats across the characteristic rows of a
    -- county-month by construction -- see the header. Never sum it.
    p.total_population                                  AS county_population,
    p.vintage_window_start || '-' || p.vintage_window_end
                                                        AS population_vintage_window,
    a.loss_year NOT BETWEEN b.earliest_vintage AND b.latest_vintage
                                                        AS population_is_extrapolated,

    -- The small-cell display rule the insurance README states: show the count beside the
    -- rate, and grey a rate built on fewer than ten claims rather than publishing a number
    -- that looks precise.
    a.claim_count < 10                                  AS is_small_cell

FROM aggregated a
CROSS JOIN data_extent e
LEFT JOIN {{ ref('dim_geography_county') }} g
    ON g.county_fips = a.county_fips
CROSS JOIN vintage_bounds b
LEFT JOIN population p
    ON  p.county_fips = a.county_fips
    AND p.vintage_year = LEAST(GREATEST(a.loss_year, b.earliest_vintage), b.latest_vintage)
