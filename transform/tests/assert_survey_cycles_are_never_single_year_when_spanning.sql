/*
  A period flagged as spanning multiple years must actually span them, and one not flagged
  must not. This is the guard on the flag every multi-year caveat downstream depends on:
  CDC's 2019-2022 and 2021-2022 cycles are 9,261 rows that a single-year key would file under
  a year they do not describe.
*/

SELECT grain, period_key, period_start, period_end, spans_multiple_years
FROM {{ ref('dim_period') }}
WHERE spans_multiple_years <> (YEAR(period_end) > YEAR(period_start))
