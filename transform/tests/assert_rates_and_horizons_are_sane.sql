/*
  Two classes of wrong number that pass every other check in this repo.

  Contracts pin types, tie tests conserve totals, relationships resolve keys -- and none of
  them notices a percentage of 107% or a month three years past the data. Both shipped, both
  were found by asking the warehouse the questions it was built for, and both are the kind a
  reader hits in the first hour.

  ## A rate above 100%

  CFPB sets its timeliness flag on 592,496 complaints that are still In progress. Counting
  those against a closed denominator gave 107% for one company across the whole archive, and
  8,296 company-months above 100%. A response cannot be timely before it exists, so the
  numerator now counts timely **among closed** -- and this refuses to let the two drift apart
  again. It checks the components rather than the published rate, because a rate can also be
  wrong while staying under 100.

  ## A month past the data

  `fct_ins_policies_mi_monthly` expands a month spine against policy terms, and terms run
  years ahead: the newest termination date is 2029 against a file `as_of_date` of 2026-08-03.
  The spine used to reach the former, so `MAX(year_month)` returned 13 policies in force where
  the real latest month holds 18,727 -- wrong by a factor of 1,440, and wrong in the direction
  nobody questions, because a small number at the end of a series looks like a decline.

  A fact must not describe a period its source does not cover.
*/

WITH rate_components AS (

    SELECT
        'fct_fin_complaints_monthly_company' AS model,
        'timely exceeds closed'              AS claim,
        year_month || ' ' || company_name    AS subject
    FROM {{ ref('fct_fin_complaints_monthly_company') }}
    WHERE timely_count > closed_complaints
       OR relief_count > closed_complaints
       OR closed_complaints + pending_complaints <> complaint_count

    UNION ALL

    SELECT
        'fct_fin_complaints_daily_state',
        'timely exceeds closed',
        CAST(date_received AS VARCHAR) || ' ' || state_code || ' ' || product_family
    FROM {{ ref('fct_fin_complaints_daily_state') }}
    WHERE timely_count > closed_complaints
       OR relief_count > closed_complaints
       OR closed_complaints + pending_complaints <> complaint_count

    UNION ALL

    -- The allocated form of the same thing. Fractional, so it needs a tolerance.
    SELECT
        'fct_fin_complaints_monthly_geo',
        'allocated timely exceeds allocated closed',
        year_month || ' ' || county_fips
    FROM {{ ref('fct_fin_complaints_monthly_geo') }}
    WHERE allocated_timely > allocated_complaints - allocated_pending + 0.000001

    UNION ALL

    -- HMDA's own denominator, for the same reason: a denial cannot exceed a decision.
    SELECT
        'fct_fin_hmda_annual',
        'denials exceed decisions',
        CAST(activity_year AS VARCHAR) || ' ' || COALESCE(county_fips, 'no county')
    FROM {{ ref('fct_fin_hmda_annual') }}
    WHERE denied_count > decided_applications

),

published_rates AS (

    -- Belt and braces: whatever the components say, a published percentage must be a
    -- percentage.
    SELECT 'fct_fin_complaints_monthly_company', 'rate outside 0-100', year_month
    FROM {{ ref('fct_fin_complaints_monthly_company') }}
    WHERE timely_rate_pct NOT BETWEEN 0 AND 100 OR relief_rate_pct NOT BETWEEN 0 AND 100

    UNION ALL

    SELECT 'fct_fin_complaints_daily_state', 'rate outside 0-100', CAST(date_received AS VARCHAR)
    FROM {{ ref('fct_fin_complaints_daily_state') }}
    WHERE timely_rate_pct NOT BETWEEN 0 AND 100 OR relief_rate_pct NOT BETWEEN 0 AND 100

    UNION ALL

    SELECT 'fct_fin_hmda_annual', 'rate outside 0-100', CAST(activity_year AS VARCHAR)
    FROM {{ ref('fct_fin_hmda_annual') }}
    WHERE denial_rate_pct NOT BETWEEN 0 AND 100

),

horizons AS (

    -- No month may sit past the snapshot its source was taken at.
    SELECT
        'fct_ins_policies_mi_monthly',
        'month is past the file as_of_date',
        year_month
    FROM {{ ref('fct_ins_policies_mi_monthly') }}
    WHERE month_end > (SELECT MAX(as_of_date) FROM {{ ref('stg_ins__nfip_policies') }})

    UNION ALL

    -- And the claims mart may not describe a loss month after the newest loss in the file.
    SELECT
        'fct_ins_claims_monthly',
        'loss month is past the newest loss in the file',
        loss_year_month
    FROM {{ ref('fct_ins_claims_monthly') }}
    WHERE loss_year_month > (
        SELECT strftime(MAX(date_of_loss), '%Y-%m') FROM {{ ref('stg_ins__nfip_claims') }}
    )

)

SELECT * FROM rate_components
UNION ALL SELECT * FROM published_rates
UNION ALL SELECT * FROM horizons
