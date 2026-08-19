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

  The service mart now carries **both** flavours, because the drill bank asks to compare
  them, so "every payment is standardized" is no longer the invariant -- an unstandardized
  per-capita row is expected there. What must stay true is that the two are never mixed: a
  county value is benchmarked against a national value from the *same* CMS column, which is
  what `source_column` is in the join key for. Six services publish both covered stays and
  covered days per 1,000 and share every other key, so this is not theoretical.

  HLT-E3's cost per beneficiary must be the standardized series, value for value. Filtering
  for it in the model is an intention; joining back to the intermediate and comparing the
  numbers is the proof.
*/

WITH unstandardized_payments AS (

    -- A payment measure must be identifiable as one flavour or the other. NULL here would
    -- make the standardized/unstandardized split unusable and every cross-geography
    -- comparison built on it unverifiable.
    SELECT
        'payment measure with no standardization flag'    AS claim,
        county_fips                                       AS subject,
        service_code || ' ' || CAST(measure_year AS VARCHAR) AS detail
    FROM {{ ref('fct_hlt_medicare_service_county') }}
    WHERE measure_kind IN ('per_capita', 'per_user', 'amount', 'share_of_pymt')
      AND is_standardized IS NULL

    UNION ALL

    -- HLT-E2's own slice must still resolve to the standardized long-term-care payments it
    -- has always meant, now that those are a filter rather than the whole model.
    SELECT
        'HLT-E2 standardized long-term-care slice is empty',
        'all counties',
        'is_long_term_care AND is_standardized AND per_capita'
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ ref('fct_hlt_medicare_service_county') }}
        WHERE is_long_term_care AND is_standardized AND measure_kind = 'per_capita'
    )

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
