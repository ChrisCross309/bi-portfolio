{{ config(materialized = 'view') }}

/*
  HUD's ZIP-to-county crosswalk, typed. The bridge that gives CFPB complaints a county.

  A ZIP is a postal delivery route, not an area, and 34.6% of Michigan's ZIPs cross a county
  line -- so this is an allocation with a stated rule and never a lookup. HUD publishes the
  weights as the share of each ZIP's addresses falling in each county, rebuilt quarterly.

  **The allocation weight and its fallback are both here, and the fallback is not optional.**
  `res_ratio` is the residential address share, which is the right weight for a complaint
  filed by a household. But it sums to exactly zero for 3,571 ZIPs nationally -- the PO-box
  and business-only ones -- and 67 Michigan complaints sit in such a ZIP. Weighting by
  residential share alone sends every one of them nowhere. `tot_ratio` covers all of them,
  which is why `allocation_weight` falls back to it rather than leaving the rows to vanish.

  The ratios are text in raw so HUD's own number formatting survives the round trip; they
  type cleanly here, with every value castable.
*/

WITH typed AS (

    SELECT
        zip                                     AS zip5,
        SUBSTR(zip, 1, 3)                       AS zip3,
        geoid                                   AS county_fips,
        city                                    AS usps_city,
        state                                   AS state_code,
        TRY_CAST(res_ratio AS DOUBLE)           AS res_ratio,
        TRY_CAST(bus_ratio AS DOUBLE)           AS bus_ratio,
        TRY_CAST(oth_ratio AS DOUBLE)           AS oth_ratio,
        TRY_CAST(tot_ratio AS DOUBLE)           AS tot_ratio
    FROM {{ source('raw_shared', 'zip_county_crosswalk') }}

),

with_totals AS (

    SELECT
        typed.*,
        SUM(res_ratio) OVER (PARTITION BY zip5) AS zip_res_ratio_total
    FROM typed

)

SELECT
    zip5,
    zip3,
    county_fips,
    usps_city,
    state_code,
    res_ratio,
    bus_ratio,
    oth_ratio,
    tot_ratio,

    -- The rule, in one column, so no mart restates it: residential share where the ZIP has
    -- residential addresses, every address type where it has none.
    CASE WHEN zip_res_ratio_total > 0 THEN res_ratio ELSE tot_ratio END
                                                AS allocation_weight,
    CASE WHEN zip_res_ratio_total > 0 THEN 'res_ratio' ELSE 'tot_ratio' END
                                                AS allocation_weight_source,
    zip_res_ratio_total = 0                     AS zip_has_no_residential_addresses,

    LENGTH(county_fips) = 5                     AS has_full_county_fips,
    state_code = 'MI'                           AS is_michigan

FROM with_totals
