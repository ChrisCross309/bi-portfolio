/*
  The health marts must not lose or invent a cell.

  Same reasoning as `assert_insurance_marts_tie_to_staging`: an aggregation that drops rows on
  a bad join still produces a plausible table, and nobody knows the right total by heart. It
  matters more here than in the other two tracks, because these sources are already
  aggregates -- a lost row is a lost geography-year, not a lost record, and it will not look
  like anything.

  Each check states the arithmetic it expects rather than a bare number, so a figure that
  moves because the publisher republished can be told apart from one that moves because a
  join broke.
*/

WITH checks AS (

    -- Every CDC cell reaches the mart. The mart enriches and flags; it does not filter.
    SELECT
        'cdc_cells'                                         AS measure,
        (SELECT COUNT(*) FROM {{ ref('fct_hlt_cdc_indicators') }})           AS mart_value,
        (SELECT COUNT(*) FROM {{ ref('stg_hlt__cdc_healthy_aging') }})       AS staging_value

    UNION ALL

    -- Michigan county-years x five long-term-care services x two measure kinds.
    SELECT
        'ltc_county_measures',
        (SELECT COUNT(*) FROM {{ ref('fct_hlt_medicare_service_county') }}),
        (SELECT COUNT(*) FROM {{ ref('int_hlt__cms_service_measures') }}
         WHERE is_long_term_care
           AND age_level = 'All'
           AND is_michigan_county
           AND ((measure_kind = 'per_capita' AND is_standardized)
                OR (measure_kind = 'user_pct' AND NOT is_standardized)))

    UNION ALL

    -- One row per jurisdiction-or-national, year and age level. No fan-out: `Territory` and
    -- `ZZ` both carry a NULL geo_code, so a NULL-tolerant join key silently doubles them.
    SELECT
        'cost_geography_years',
        (SELECT COUNT(*) FROM {{ ref('fct_hlt_medicare_cost_annual') }}),
        (SELECT COUNT(*) FROM {{ ref('int_hlt__cms_service_measures') }}
         WHERE service_code = 'TOT'
           AND measure_kind = 'per_capita'
           AND is_standardized
           AND geo_level IN ('State', 'National'))

    UNION ALL

    -- Every Michigan county in every vintage where the 65+ estimate is actually published.
    SELECT
        'mi_population_county_vintages',
        (SELECT COUNT(*) FROM {{ ref('fct_hlt_mi_population_65_plus') }}),
        (SELECT COUNT(*) FROM {{ ref('stg_ref__acs5_subject') }}
         WHERE geo_level = 'county'
           AND is_michigan
           AND population_65_plus_is_published)

    UNION ALL

    -- The 65+ total must survive the county rollup exactly.
    SELECT
        'mi_population_latest_vintage',
        (SELECT SUM(population_65_plus) FROM {{ ref('fct_hlt_mi_population_65_plus') }}
         WHERE vintage_year = {{ var('acs_growth_to_vintage') }}),
        (SELECT SUM(population_65_plus) FROM {{ ref('stg_ref__acs5_subject') }}
         WHERE geo_level = 'county'
           AND is_michigan
           AND vintage = '{{ var('acs_growth_to_vintage') }}')

)

SELECT measure, mart_value, staging_value, mart_value - staging_value AS gap
FROM checks
WHERE mart_value IS DISTINCT FROM staging_value
