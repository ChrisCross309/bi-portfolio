{{ config(materialized = 'table') }}

/*
  Michigan policies in force at each month end, by county. Answers INS-E4.

  There is no in-force flag in the NFIP policy file. A policy is in force on a date when that
  date falls between its effective and termination dates, so this expands a month-end spine
  against those ranges. The output is county by month, not policy by month: roughly 84
  counties times the months covered, which stays small even though the join behind it is not.

  Two things that make a naive count wrong.

  A row is not a policy. policy_count runs to 289 because a condominium master policy covers
  many units in one record, so in-force is SUM(policy_count) and never COUNT(*). Michigan's
  book is about 6% larger than its row count, and the gap moves with the condo mix, so the
  error would not even be a constant.

  Four policies terminate on or before they take effect. They are excluded from the in-force
  expansion because a negative-length term cannot be in force on any date, and counted
  separately so the exclusion is visible rather than silent.

  The national benchmark for INS-E4 does not come from here. Raw is Michigan-only by
  decision, so the national side is FEMA's own published state statistics, recorded in the
  policies manifest and surfaced in rpt_ins_reconciliation.
*/

WITH month_ends AS (

    SELECT date_day AS month_end
    FROM {{ ref('dim_date') }}
    WHERE is_month_end
      AND date_day >= (SELECT MIN(policy_effective_date) FROM {{ ref('stg_ins__nfip_policies') }})
      AND date_day <= (SELECT MAX(policy_termination_date) FROM {{ ref('stg_ins__nfip_policies') }})

),

in_force AS (

    SELECT
        m.month_end,
        p.county_fips,
        p.policy_count,
        p.total_insurance_premium_of_the_policy,
        p.total_building_insurance_coverage,
        p.total_contents_insurance_coverage,
        p.policy_cost
    FROM {{ ref('stg_ins__nfip_policies') }} p
    JOIN month_ends m
        ON m.month_end >= p.policy_effective_date
       AND m.month_end <  p.policy_termination_date
    WHERE NOT p.has_invalid_term

)

SELECT
    f.month_end,
    strftime(f.month_end, '%Y-%m')                      AS year_month,
    CAST(year(f.month_end) AS SMALLINT)                 AS calendar_year,
    f.county_fips,
    g.county_name,
    COALESCE(g.is_michigan, FALSE)                      AS is_michigan,

    -- The measure INS-E4 asks for. Sum, never count.
    SUM(f.policy_count)                                 AS policies_in_force,
    COUNT(*)                                            AS policy_records,
    SUM(f.total_insurance_premium_of_the_policy)        AS premium_in_force,
    SUM(f.total_building_insurance_coverage)            AS building_coverage_in_force,
    SUM(f.total_contents_insurance_coverage)            AS contents_coverage_in_force,
    SUM(f.policy_cost)                                  AS policy_cost_in_force,

    SUM(f.policy_count) - COUNT(*)                      AS policies_beyond_records,
    COUNT(*) < 10                                       AS is_small_cell

FROM in_force f
LEFT JOIN {{ ref('dim_geography_county') }} g
    ON g.county_fips = f.county_fips
GROUP BY 1, 2, 3, 4, 5, 6
