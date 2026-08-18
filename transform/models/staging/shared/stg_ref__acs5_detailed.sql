{{ config(materialized = 'view') }}

/*
  ACS 5-year detailed tables: total population and total housing units, 2010-2024 vintages.
  The denominator under every per-capita rate in the repo.

  Three properties that make this data easy to misuse, and all three survive into the column
  names rather than living in someone's memory.

  ## These are not a time series

  Consecutive vintages share four years of sample -- the 2020 vintage covers 2016-2020 and
  the 2021 covers 2017-2021 -- so plotting them as a trend shows mostly the same survey
  responses moving against themselves. Only every fifth vintage is independent, and the
  Census Bureau advises against comparing overlapping ones. All fifteen are landed because
  each is the right denominator *for its own window*. `vintage_window_start` and
  `vintage_window_end` are here so a label can say which window it means.

  ## A 5-year estimate describes its window, not its final year

  Calling the 2024 vintage "2024 population" overstates it. `vintage_label` spells the window
  out for exactly that reason.

  ## An annotation is not a measurement

  ACS annotates rather than nulls, and `-555555555` in a margin column means "controlled
  estimate, no margin applies" -- 47,301 occurrences in the population margin alone. Averaged
  as a number it is catastrophic; typed here to NULL with a flag. Only the margin columns
  carry it; the estimates themselves are clean.

  Connecticut's counties became nine planning regions in the 2022 vintage, which is why
  `dim_geography_county` unions every vintage rather than taking the newest.
*/

SELECT
    vintage,
    CAST(vintage AS SMALLINT)                       AS vintage_year,
    -- A 5-year estimate covers the vintage year and the four before it.
    CAST(vintage AS SMALLINT) - 4                   AS vintage_window_start,
    CAST(vintage AS SMALLINT)                       AS vintage_window_end,
    (CAST(vintage AS SMALLINT) - 4) || '-' || vintage AS vintage_label,

    geo_level,
    state                                           AS state_fips,
    county                                          AS county_fips_3,
    CASE WHEN geo_level = 'county' THEN state || county END AS county_fips,
    NAME                                            AS geography_name,
    COALESCE(state = '26', FALSE)                   AS is_michigan,

    TRY_CAST(B01003_001E AS BIGINT)                 AS total_population,
    TRY_CAST(B25001_001E AS BIGINT)                 AS total_housing_units,

    -- -555555555 is an annotation meaning "controlled estimate, no margin applies". It is
    -- not a measurement and must never be averaged.
    CASE WHEN TRY_CAST(B01003_001M AS BIGINT) >= 0 THEN TRY_CAST(B01003_001M AS BIGINT) END
                                                    AS total_population_margin,
    CASE WHEN TRY_CAST(B25001_001M AS BIGINT) >= 0 THEN TRY_CAST(B25001_001M AS BIGINT) END
                                                    AS total_housing_units_margin,
    COALESCE(TRY_CAST(B01003_001M AS BIGINT) < 0, FALSE)
                                                    AS population_is_controlled_estimate,
    COALESCE(TRY_CAST(B25001_001M AS BIGINT) < 0, FALSE)
                                                    AS housing_units_is_controlled_estimate

FROM {{ source('raw_shared', 'acs5_detailed') }}
