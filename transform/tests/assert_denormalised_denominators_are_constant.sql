/*
  A denominator carried on a fact row must not vary within the group it describes.

  County population belongs to a county and a period, not to a flood zone or a product
  family, so it repeats across every characteristic row in a county-month. That repetition is
  safe to collapse -- take MAX, or divide inside the row -- **only** while the value is
  genuinely constant across those rows. The moment it varies, the denormalization is a bug
  rather than a convenience, and every per-capita rate built on it silently disagrees with
  itself depending on which row a reader lands on.

  That is not hypothetical. Both marts previously joined ACS on "the five-year window
  contains this year", and because the windows overlap, a mid-range year matched five
  vintages with five *different* populations. This check is what makes the fixed version a
  provable property instead of a claim in a comment.

  It also catches the milder version: one vintage picked, but a NULL on some rows and a value
  on others, which averages to something no reader intended.
*/

WITH insurance AS (

    SELECT
        'fct_ins_claims_monthly'                        AS model,
        county_fips || ' ' || loss_year_month           AS grouping_key,
        COUNT(DISTINCT county_population)               AS distinct_values,
        COUNT(DISTINCT population_vintage_window)       AS distinct_windows
    FROM {{ ref('fct_ins_claims_monthly') }}
    WHERE county_fips IS NOT NULL
    GROUP BY 1, 2

),

fintech AS (

    SELECT
        'fct_fin_complaints_monthly_geo'                AS model,
        county_fips || ' ' || year_month                AS grouping_key,
        COUNT(DISTINCT county_population)               AS distinct_values,
        COUNT(DISTINCT population_vintage_window)       AS distinct_windows
    FROM {{ ref('fct_fin_complaints_monthly_geo') }}
    WHERE county_fips IS NOT NULL
    GROUP BY 1, 2

)

SELECT * FROM insurance WHERE distinct_values > 1 OR distinct_windows > 1
UNION ALL
SELECT * FROM fintech WHERE distinct_values > 1 OR distinct_windows > 1
