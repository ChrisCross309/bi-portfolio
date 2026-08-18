/*
  Every HMDA application must fall into exactly one action group, and the groups must add up.

  FIN-E4's denial rate is denials over decisions, and the denominator deliberately excludes
  withdrawn applications, files closed for incompleteness, purchased loans and preapproval
  requests. That exclusion is only defensible if the groups are exhaustive -- an action code
  that fell through every bucket would quietly shrink the denominator and inflate the rate.
*/

SELECT
    activity_year,
    SUM(application_count) AS applications,
    SUM(decided_applications + withdrawn_count + closed_incomplete_count
        + purchased_count + preapproval_count) AS grouped
FROM {{ ref('fct_fin_hmda_annual') }}
GROUP BY activity_year
HAVING SUM(application_count) <> SUM(decided_applications + withdrawn_count
    + closed_incomplete_count + purchased_count + preapproval_count)
