/*
  The single easiest invisible error in this track.

  CMS publishes each payment twice: standardized, which removes geographic wage and
  payment-policy differences, and unstandardized, which does not. Only the standardized
  columns make a Michigan-versus-national comparison mean anything. The unstandardized twins
  measure something real and different, and a mart built on the wrong one produces a table
  that looks correct in every respect except the one it exists to answer.

  Nothing about a dollar figure reveals which it is, which is why `int_hlt__cms_service_measures`
  carries `is_standardized` as a flag rather than leaving it buried in a column name -- and
  why this test can exist at all.

  Two checks, one per mart that crosses geography.

  HLT-E2's per-beneficiary payments must be standardized. Its `user_pct` measures are
  utilization shares with no standardized twin, so they are correctly unstandardized and are
  not caught here.

  HLT-E3's cost per beneficiary must be the standardized series, value for value. Filtering
  for it in the model is an intention; joining back to the intermediate and comparing the
  numbers is the proof.
*/

WITH unstandardized_payments AS (

    SELECT
        'HLT-E2 per-capita payment is not standardized'  AS claim,
        county_fips                                      AS subject,
        service_code || ' ' || CAST(measure_year AS VARCHAR) AS detail
    FROM {{ ref('fct_hlt_medicare_service_county') }}
    WHERE measure_kind = 'per_capita'
      AND NOT is_standardized

),

cost_not_from_standardized_series AS (

    SELECT
        'HLT-E3 cost does not match the standardized series',
        c.geo_description,
        CAST(c.measure_year AS VARCHAR) || ' ' || c.age_level
    FROM {{ ref('fct_hlt_medicare_cost_annual') }} c
    LEFT JOIN {{ ref('int_hlt__cms_service_measures') }} m
        ON  m.measure_year = c.measure_year
        AND m.geo_level    = c.geo_level
        AND m.geo_description = c.geo_description
        AND m.age_level    = c.age_level
        AND m.service_code = 'TOT'
        AND m.measure_kind = 'per_capita'
        AND m.is_standardized
    WHERE c.cost_per_capita IS DISTINCT FROM m.measure_value

)

SELECT * FROM unstandardized_payments
UNION ALL
SELECT * FROM cost_not_from_standardized_series
