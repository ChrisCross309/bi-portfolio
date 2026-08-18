{{ config(materialized = 'table') }}

/*
  A day-grain calendar, and only the sources that are genuinely daily should join it.

  CFPB publishes a complaint's received date to the day, and the NFIP files carry real DATE
  columns. Everything else in this repo is annual or survey-cycle, and joins `dim_period`
  instead -- there is deliberately no path from an annual measure to a day here, because
  CLAUDE.md section 8 forbids structure that invites fake daily reporting on annual data.

  The span is set by what the data actually reaches, not by a round number:

    1953-05-02  the earliest FEMA disaster declaration
    1978-01-01  the earliest NFIP loss date
    2029-08-29  the latest NFIP policy termination date -- policies are written forward

  So 1953 through 2029, which is about 28,100 rows. Ending the calendar at "today" would
  drop every policy whose term runs past it, which is most of the in-force book.
*/

WITH days AS (
    SELECT CAST(UNNEST(generate_series(DATE '1953-01-01', DATE '2029-12-31', INTERVAL 1 DAY)) AS DATE) AS date_day
)

SELECT
    date_day,
    CAST(strftime(date_day, '%Y%m%d') AS INTEGER)      AS date_key,
    CAST(year(date_day) AS SMALLINT)                   AS calendar_year,
    CAST(quarter(date_day) AS TINYINT)                 AS calendar_quarter,
    CAST(month(date_day) AS TINYINT)                   AS calendar_month,
    monthname(date_day)                                AS month_name,
    strftime(date_day, '%Y-%m')                        AS year_month,
    CAST(year(date_day) AS VARCHAR) || '-Q' || CAST(quarter(date_day) AS VARCHAR) AS year_quarter,
    CAST(date_trunc('month', date_day) AS DATE)                      AS month_start,
    CAST(last_day(date_day) AS DATE)                   AS month_end,
    CAST(date_trunc('quarter', date_day) AS DATE)                    AS quarter_start,
    CAST(date_trunc('year', date_day) AS DATE)                       AS year_start,
    CAST(dayofyear(date_day) AS SMALLINT)              AS day_of_year,
    CAST(isodow(date_day) AS TINYINT)                  AS iso_day_of_week,
    dayname(date_day)                                  AS day_name,
    CAST(week(date_day) AS TINYINT)                    AS iso_week,
    isodow(date_day) IN (6, 7)                         AS is_weekend,
    date_day = CAST(last_day(date_day) AS DATE)        AS is_month_end
FROM days
