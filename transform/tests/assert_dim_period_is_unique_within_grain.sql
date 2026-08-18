/*
  `period_key` is unique within a grain but deliberately not across grains: 2024 is a valid
  key at `year` grain and at `survey_cycle` grain, and they mean different things. A plain
  unique test on the column would be wrong, so the compound key is tested here.
*/

SELECT grain, period_key, COUNT(*) AS rows
FROM {{ ref('dim_period') }}
GROUP BY grain, period_key
HAVING COUNT(*) > 1
