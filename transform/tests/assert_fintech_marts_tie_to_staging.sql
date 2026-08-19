{{ config(severity = coverage_test_severity()) }}

/*
  The fintech marts must not lose or duplicate a complaint.

  This is the test whose absence let a real defect ship. `assert_complaint_allocation_conserves_complaints`
  compares the *intermediate* allocation against staging and passed throughout, because the
  allocation was always right. The mart was not: joining county population on "the ACS
  five-year window contains this year" matched **five** vintages for a mid-range year, because
  the windows overlap, and every measure was repeated once per match. The mart held 7,315,606
  rows against a true grain of 2,330,387, and `SUM(allocated_complaints)` read 28,651,231
  against a true 16,957,516 -- a 1.69x overstatement sitting under FIN-E1 and FIN-E2.

  Nothing caught it. Insurance and health both had a mart-level tie test; fintech had none, so
  the one join between the intermediate and the mart was unwatched. This is that test.

  The row-count checks are the sharp ones: a fanned-out join leaves the measures looking
  plausible per row and only the cardinality gives it away.

  Warns rather than fails against the fixture, for the same reason the allocation test does:
  the committed crosswalk is a stratified sample, so a ZIP that touches four counties may keep
  only one and its weights no longer sum to one.
*/

WITH checks AS (

    -- Every allocated row reaches the mart exactly once. This is the cardinality guard.
    SELECT
        'geo_rows'                                          AS measure,
        (SELECT COUNT(*) FROM {{ ref('fct_fin_complaints_monthly_geo') }})       AS mart_value,
        (SELECT COUNT(*) FROM (
            SELECT received_year_month, state_code, county_fips, product_family, allocation_method
            FROM {{ ref('int_fin__complaint_county_allocation') }}
            GROUP BY ALL
        ))                                                                      AS staging_value

    UNION ALL

    -- And carries the same number of complaints. Rounded, because allocation is fractional
    -- by design and floating-point summation over 17 million rows is not exact.
    SELECT
        'geo_complaints',
        (SELECT ROUND(SUM(allocated_complaints), 0)
         FROM {{ ref('fct_fin_complaints_monthly_geo') }}),
        (SELECT ROUND(SUM(allocated_complaints), 0)
         FROM {{ ref('int_fin__complaint_county_allocation') }})

    UNION ALL

    -- The company mart is state-grained, so it carries every complaint that has a state and
    -- none that does not. 63,546 have no usable state -- a real category, excluded here on
    -- purpose and counted in the geo mart's `state_unassigned` allocation instead.
    SELECT
        'company_complaints',
        (SELECT SUM(complaint_count) FROM {{ ref('fct_fin_complaints_monthly_company') }}),
        (SELECT COUNT(*) FROM {{ ref('stg_fin__cfpb_complaints') }} WHERE state_code IS NOT NULL)

    UNION ALL

    -- HMDA applications, likewise.
    SELECT
        'hmda_applications',
        (SELECT SUM(application_count) FROM {{ ref('fct_fin_hmda_annual') }}),
        (SELECT COUNT(*) FROM {{ ref('stg_fin__hmda_lar') }})

)

SELECT measure, mart_value, staging_value, mart_value - staging_value AS gap
FROM checks
WHERE mart_value IS DISTINCT FROM staging_value
