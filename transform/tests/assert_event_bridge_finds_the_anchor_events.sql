{{ config(severity = coverage_test_severity()) }}

/*
  The event bridge must find the two Michigan floods everyone involved would recognise.

  The 2020 Edenville and Sanford dam failures in Midland County (disasters 3525 and 4547) and
  the 2021 southeast Michigan urban flooding (4607) are recorded as anchor events in the
  declarations manifest, precisely so an attribution rule can be checked against something
  other than its own output. An INS-E3 that misses them is wrong however reasonable its
  overall share looks.

  Warns rather than fails against the fixture, whose stratified claim sample carries only a
  slice of any single event.
*/

WITH expected AS (
    SELECT * FROM (VALUES (3525), (4547), (4607)) AS t(disaster_number)
),

found AS (
    SELECT disaster_number, COUNT(*) AS claim_count
    FROM {{ ref('bridge_ins_claim_declaration') }}
    GROUP BY disaster_number
)

SELECT e.disaster_number, COALESCE(f.claim_count, 0) AS claim_count
FROM expected e
LEFT JOIN found f USING (disaster_number)
WHERE COALESCE(f.claim_count, 0) = 0
