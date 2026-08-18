{{ config(materialized = 'table') }}

/*
  One row per reporting period, at the grain the publisher actually reports it.

  This is the model that makes honest grain structural rather than remembered. CLAUDE.md
  section 8 forbids building anything that invites fake daily or quarterly reporting on
  annual data, and forbids interpolating between survey cycles to manufacture a trend line.
  A mart declares its grain by joining the rows of that grain here, and an annual measure
  therefore has no monthly key available to be presented at -- not by convention, but
  because one does not exist for it.

  Three grains, each because a landed source reports at it:

    month          CPI-U observations, and monthly rollups of CFPB's daily complaints
    year           CMS Geographic Variation (2014-2024), HMDA (2018-2025), ACS vintages
    survey_cycle   CDC's Alzheimer's & Healthy Aging cycles

  The survey cycles are the reason this model is not just a year table. Eight of CDC's ten
  cycles are single years, but 2019-2022 and 2021-2022 span several -- 9,261 rows between
  them -- and a period key built on `yearstart` alone silently files them under a year they
  do not describe. `period_start` and `period_end` carry the real window, and
  `spans_multiple_years` makes the ones that need a caveat findable.

  The cycles come from a seed rather than from the CDC table, so that a conformed dimension
  is not reaching into a track's source for its own definition. A dbt test in the health
  staging PR asserts the seed still matches what CDC publishes.
*/

WITH months AS (
    SELECT CAST(UNNEST(generate_series(DATE '1953-01-01', DATE '2029-12-01', INTERVAL 1 MONTH)) AS DATE) AS period_start
),

years AS (
    SELECT CAST(UNNEST(generate_series(1913, 2029, 1)) AS INTEGER) AS calendar_year
),

monthly AS (
    SELECT
        'month'                                     AS grain,
        strftime(period_start, '%Y-%m')             AS period_key,
        period_start,
        CAST(last_day(period_start) AS DATE)        AS period_end,
        strftime(period_start, '%Y-%m')             AS period_label,
        CAST(year(period_start) AS SMALLINT)        AS calendar_year,
        FALSE                                       AS spans_multiple_years
    FROM months
),

annual AS (
    /* Back to 1913 because CPI-U does: HLT-E3 and INS-E2 deflate against its annual
       average, and a deflator year with no period row cannot be joined. */
    SELECT
        'year'                                      AS grain,
        CAST(calendar_year AS VARCHAR)              AS period_key,
        make_date(calendar_year, 1, 1)              AS period_start,
        make_date(calendar_year, 12, 31)            AS period_end,
        CAST(calendar_year AS VARCHAR)              AS period_label,
        CAST(calendar_year AS SMALLINT)             AS calendar_year,
        FALSE                                       AS spans_multiple_years
    FROM years
),

cycles AS (
    SELECT
        'survey_cycle'                              AS grain,
        cycle_key                                   AS period_key,
        make_date(CAST(year_start AS INTEGER), 1, 1)   AS period_start,
        make_date(CAST(year_end AS INTEGER), 12, 31)   AS period_end,
        cycle_label                                 AS period_label,
        /* The final year of the window, and only ever a label of convenience: a
           multi-year cycle describes its whole window and must never be plotted as a
           point on that year. `spans_multiple_years` is the flag that says so. */
        CAST(year_end AS SMALLINT)                  AS calendar_year,
        CAST(spans_multiple_years AS BOOLEAN)       AS spans_multiple_years
    FROM {{ ref('seed_survey_cycles') }}
)

SELECT * FROM monthly
UNION ALL SELECT * FROM annual
UNION ALL SELECT * FROM cycles
