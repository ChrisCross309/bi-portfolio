{{ config(materialized = 'view') }}

/*
  ACS 5-year subject tables: population aged 65 and over. HLT-E5's denominator.

  The 65+ figure is the publisher's own `S0101_C01_030E`. It is deliberately not summed from
  the twelve `B01001` age components -- that would be aggregation, and it would disagree with
  the number the Census Bureau publishes.

  ## S0101 was restructured at the 2017 vintage, and line 030 changed meaning

  **In the 2010-2016 layout `S0101_C01_030E` is median age in years, not a population.** It
  was read into this model as a count for both, and a `CAST(... AS BIGINT)` turned Wayne
  County's `36.5` into `36` without complaint. Measured across all 3,220 counties in raw:

      vintage      min     median         max
      2010-2016   18.0       40.0        66.0     <- age, in years
      2017-2024      9    ~5,000     1,487,700    <- people

  Michigan's 83 counties sum to 3,480 in the 2010 vintage and 1,575,233 in 2017. The
  boundary is exactly 2017, in every state, with no exceptions.

  So the estimate and its margin are published here **only from the 2017 vintage onward**,
  and `population_65_plus_is_published` says so on every row rather than leaving a consumer
  to infer it from a NULL. The pre-2017 column is not re-pointed at the correct line: that
  would mean re-fetching a different variable from the Census API, which is ingestion's job
  and not a typing decision. The vintages are landed and the boundary is recorded; what
  changed is that the wrong half is no longer offered as a population.

  Every caveat in `stg_ref__acs5_detailed` applies here identically: overlapping vintages are
  not a time series, a 5-year estimate describes its whole window rather than its final year,
  and `-555555555` in the margin is a controlled-estimate annotation rather than a
  measurement.
*/

{# The first vintage whose S0101 line 030 is the 65-and-over population. #}
{% set first_population_vintage = 2017 %}

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

    -- Whether line 030 means what this model's column name says on this vintage.
    CAST(vintage AS SMALLINT) >= {{ first_population_vintage }}
                                                    AS population_65_plus_is_published,

    CASE
        WHEN CAST(vintage AS SMALLINT) >= {{ first_population_vintage }}
        THEN TRY_CAST(S0101_C01_030E AS BIGINT)
    END                                             AS population_65_plus,
    CASE
        WHEN CAST(vintage AS SMALLINT) >= {{ first_population_vintage }}
             AND TRY_CAST(S0101_C01_030M AS BIGINT) >= 0
        THEN TRY_CAST(S0101_C01_030M AS BIGINT)
    END                                             AS population_65_plus_margin,
    COALESCE(
        CAST(vintage AS SMALLINT) >= {{ first_population_vintage }}
            AND TRY_CAST(S0101_C01_030M AS BIGINT) < 0,
        FALSE
    )                                               AS is_controlled_estimate

FROM {{ source('raw_shared', 'acs5_subject') }}
