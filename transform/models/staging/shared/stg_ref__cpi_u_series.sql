{{ config(materialized = 'view') }}

/*
  CPI series definitions. 8,104 series, of which only the 201 in the All-items data file have
  observations landed -- the rest describe series in the other `cu.data.*` files, which no
  question here needs. Definitions without observations are expected, not a gap.

  Padding comes off here for the same reason it does in the observations model: `series_id`
  is space-padded to 17 characters in both files, and a join between them that trims only one
  side silently matches nothing.
*/

SELECT
    TRIM(series_id)                     AS series_id,
    TRIM(area_code)                     AS area_code,
    TRIM(item_code)                     AS item_code,
    TRIM(periodicity_code)              AS periodicity_code,
    TRIM(base_code)                     AS base_code,
    TRIM(base_period)                   AS base_period,
    TRIM(series_title)                  AS series_title,
    NULLIF(TRIM(footnote_codes), '')    AS footnote_codes,
    CAST(begin_year AS SMALLINT)        AS begin_year,
    TRIM(begin_period)                  AS begin_period,
    CAST(end_year AS SMALLINT)          AS end_year,
    TRIM(end_period)                    AS end_period,
    seasonal                            AS seasonal_code,
    seasonal = 'S'                      AS is_seasonally_adjusted
FROM {{ source('raw_shared', 'cpi_u_series') }}
