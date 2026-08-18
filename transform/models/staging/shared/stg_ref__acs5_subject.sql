{{ config(materialized = 'view') }}

/*
  ACS 5-year subject tables: population aged 65 and over. HLT-E5's denominator.

  The 65+ figure is the publisher's own `S0101_C01_030E`. It is deliberately not summed from
  the twelve `B01001` age components -- that would be aggregation, and it would disagree with
  the number the Census Bureau publishes.

  Every caveat in `stg_ref__acs5_detailed` applies here identically: overlapping vintages are
  not a time series, a 5-year estimate describes its whole window rather than its final year,
  and `-555555555` in the margin is a controlled-estimate annotation rather than a
  measurement.
*/

SELECT
    vintage,
    CAST(vintage AS SMALLINT)                       AS vintage_year,
    CAST(vintage AS SMALLINT) - 4                   AS vintage_window_start,
    CAST(vintage AS SMALLINT)                       AS vintage_window_end,
    (CAST(vintage AS SMALLINT) - 4) || '-' || vintage AS vintage_label,

    geo_level,
    state                                           AS state_fips,
    county                                          AS county_fips_3,
    CASE WHEN geo_level = 'county' THEN state || county END AS county_fips,
    NAME                                            AS geography_name,
    COALESCE(state = '26', FALSE)                   AS is_michigan,

    TRY_CAST(S0101_C01_030E AS BIGINT)              AS population_65_plus,
    CASE WHEN TRY_CAST(S0101_C01_030M AS BIGINT) >= 0 THEN TRY_CAST(S0101_C01_030M AS BIGINT) END
                                                    AS population_65_plus_margin,
    COALESCE(TRY_CAST(S0101_C01_030M AS BIGINT) < 0, FALSE)
                                                    AS is_controlled_estimate

FROM {{ source('raw_shared', 'acs5_subject') }}
