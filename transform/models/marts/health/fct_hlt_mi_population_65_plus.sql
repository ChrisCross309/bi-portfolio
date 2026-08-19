{{ config(materialized = 'table') }}

/*
  **HLT-E5, the need side.** Michigan's population aged 65 and over by county and ACS
  vintage, with each county's share of the state total.

  ## These are estimates, not projections

  HLT-E5 is worded as "given 65+ population projections". **ACS does not publish a
  projection**, and nothing in this repo does either. What is here is the American Community
  Survey's 5-year *estimate* of the population that already exists, measured over a moving
  window. Presenting an observed change as a forecast would be inventing structure the source
  does not support, so this model measures what happened and the question is answered from
  observed growth. `is_projection` does not exist as a column because there is nothing true
  to put in it.

  ## Overlapping vintages are not a time series

  Consecutive vintages share four years of sample -- the 2023 vintage covers 2019-2023 and the
  2024 covers 2020-2024 -- so plotting them as a trend shows mostly the same survey responses
  moving against themselves. Only every fifth vintage is independent.
  `is_non_overlapping_with_prior` marks the ones that are, and `rpt_hlt_need_vs_capacity`
  compares only those. All published vintages are carried, because each is the right
  denominator for its own window.

  ## The vintage floor is a publisher change, not a gap in the download

  Census restructured the S0101 subject table at the 2017 vintage and line 030 changed
  meaning: before it, the column this model reads is **median age in years**. Michigan's 83
  counties sum to 3,480 in the 2010 vintage against 1,575,233 in 2017, and the boundary is
  exactly the same in every state. `stg_ref__acs5_subject` publishes the estimate only from
  2017 onward and this model reads only what it publishes, so the eight usable vintages here
  are eight real ones rather than fifteen with seven quietly wrong.

  Eight vintages still spans the non-overlapping 2015-2019 and 2020-2024 windows, which is
  what HLT-E5's growth comparison needs.
*/

WITH population AS (

    SELECT
        a.vintage,
        a.vintage_year,
        a.vintage_window_start,
        a.vintage_window_end,
        a.vintage_label,
        a.county_fips,
        a.geography_name,
        a.population_65_plus,
        a.population_65_plus_margin,
        a.is_controlled_estimate
    FROM {{ ref('stg_ref__acs5_subject') }} a
    WHERE a.geo_level = 'county'
      AND a.is_michigan
      -- Only the vintages where line 030 is a population. See the note above.
      AND a.population_65_plus_is_published

),

statewide AS (

    SELECT
        vintage_year,
        SUM(population_65_plus)     AS mi_population_65_plus,
        COUNT(*)                    AS counties_reporting
    FROM population
    GROUP BY 1

),

vintages AS (

    SELECT
        vintage_year,
        vintage_window_start                                 AS window_start,
        LAG(vintage_window_end) OVER (ORDER BY vintage_year) AS prior_window_end
    FROM (
        SELECT DISTINCT vintage_year, vintage_window_start, vintage_window_end
        FROM population
    )

)

SELECT
    p.vintage,
    'year:' || p.vintage                        AS period_id,
    p.vintage_year,
    p.vintage_window_start,
    p.vintage_window_end,
    p.vintage_label,

    p.county_fips,
    COALESCE(g.county_name, p.geography_name)   AS county_name,
    COALESCE(g.is_michigan, FALSE)              AS is_michigan,

    p.population_65_plus,
    p.population_65_plus_margin,
    p.is_controlled_estimate,

    s.mi_population_65_plus                     AS state_population_65_plus,
    s.counties_reporting,
    CASE
        WHEN s.mi_population_65_plus > 0
        THEN 100.0 * p.population_65_plus / s.mi_population_65_plus
    END                                         AS county_share_of_state_pct,

    -- TRUE only where this vintage's window shares no sample year with the previous one.
    COALESCE(v.window_start > v.prior_window_end, FALSE)
                                                AS is_non_overlapping_with_prior

FROM population p
JOIN statewide s
    ON s.vintage_year = p.vintage_year
LEFT JOIN vintages v
    ON v.vintage_year = p.vintage_year
LEFT JOIN {{ ref('dim_geography_county') }} g
    ON g.county_fips = p.county_fips
