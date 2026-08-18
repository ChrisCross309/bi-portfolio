/*
  Every survey cycle CDC publishes must have a row in `seed_survey_cycles`.

  `dim_period` takes its survey-cycle members from that seed rather than from the CDC table,
  so a conformed dimension is not reaching into a health source for its own definition. The
  cost of that choice is that the seed can fall behind the publisher, and this is what pays
  it: CDC adds a cycle, the seed does not have it, and this fails instead of the cycle
  quietly having no period to join to.

  It lives here rather than beside `dim_period` because reading CDC data is natural in the
  health track and not in `conformed/`.
*/

SELECT DISTINCT
    c.cycle_key,
    c.year_start,
    c.year_end
FROM {{ ref('stg_hlt__cdc_healthy_aging') }} c
LEFT JOIN {{ ref('seed_survey_cycles') }} s
    ON s.cycle_key = c.cycle_key
WHERE s.cycle_key IS NULL
