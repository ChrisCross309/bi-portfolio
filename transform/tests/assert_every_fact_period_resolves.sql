/*
  Every fact's `period_id` must resolve to exactly one `dim_period` row.

  This is the property a BI relationship rests on, and it is not the same as the period key
  being present. `period_key` is unique only *within* a grain -- 2015 through 2022 exist as
  both calendar years and CDC survey cycles, eight collisions in 1,051 rows -- so a tool that
  needs one column on the one side of a relationship cannot use it. `period_id` prefixes the
  grain, and each fact builds its own from the grain it is actually reported at.

  Two ways that can silently break, and this catches both. A fact could build the key with the
  wrong grain -- `year:2019` on a monthly mart resolves to a real row and filters completely
  wrongly. Or a period could exist in a fact and not in the dimension, which in a BI model
  shows up as a blank row swallowing measures rather than as an error.

  Annual and survey-cycle facts are checked the same way as monthly ones, because the failure
  is identical: it looks like data and behaves like a filter.
*/

WITH fact_periods AS (

    SELECT 'fct_ins_claims_monthly'          AS model, 'month'        AS expected_grain, period_id
    FROM {{ ref('fct_ins_claims_monthly') }}
    UNION
    SELECT 'fct_ins_claims_detail_mi',       'month',        period_id
    FROM {{ ref('fct_ins_claims_detail_mi') }}
    UNION
    SELECT 'fct_ins_policies_mi_monthly',    'month',        period_id
    FROM {{ ref('fct_ins_policies_mi_monthly') }}
    UNION
    SELECT 'fct_fin_complaints_monthly_geo', 'month',        period_id
    FROM {{ ref('fct_fin_complaints_monthly_geo') }}
    UNION
    SELECT 'fct_fin_complaints_monthly_company', 'month',    period_id
    FROM {{ ref('fct_fin_complaints_monthly_company') }}
    UNION
    SELECT 'fct_fin_complaints_daily_state', 'month',        period_id
    FROM {{ ref('fct_fin_complaints_daily_state') }}
    UNION
    SELECT 'rpt_fin_publication_window',     'month',        period_id
    FROM {{ ref('rpt_fin_publication_window') }}
    UNION
    SELECT 'fct_fin_hmda_annual',            'year',         period_id
    FROM {{ ref('fct_fin_hmda_annual') }}
    UNION
    SELECT 'fct_hlt_medicare_cost_annual',   'year',         period_id
    FROM {{ ref('fct_hlt_medicare_cost_annual') }}
    UNION
    SELECT 'fct_hlt_medicare_service_county', 'year',        period_id
    FROM {{ ref('fct_hlt_medicare_service_county') }}
    UNION
    SELECT 'fct_hlt_mi_population_65_plus',  'year',         period_id
    FROM {{ ref('fct_hlt_mi_population_65_plus') }}
    UNION
    SELECT 'fct_hlt_cdc_indicators',         'survey_cycle', period_id
    FROM {{ ref('fct_hlt_cdc_indicators') }}
    UNION
    SELECT 'fct_hlt_cdc_mi_vs_national',     'survey_cycle', period_id
    FROM {{ ref('fct_hlt_cdc_mi_vs_national') }}

)

SELECT
    f.model,
    f.expected_grain,
    f.period_id,
    CASE
        WHEN d.period_id IS NULL       THEN 'no matching dim_period row'
        WHEN d.grain <> f.expected_grain
            THEN 'resolves to grain ' || d.grain || ', which is not this fact''s grain'
    END                                                     AS problem
FROM fact_periods f
LEFT JOIN {{ ref('dim_period') }} d
    ON d.period_id = f.period_id
WHERE f.period_id IS NULL
   OR d.period_id IS NULL
   OR d.grain <> f.expected_grain
