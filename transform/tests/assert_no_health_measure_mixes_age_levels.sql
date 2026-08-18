{{ config(severity = coverage_test_severity()) }}

/*
  CMS's age levels double count, and this is what makes that safe rather than remembered.

  County rows carry `All` only. State and national rows also appear as `<65` and `>=65`,
  **which sum to their own `All`** -- so an aggregate that forgets to filter returns a
  jurisdiction at twice its true size, and the number looks entirely reasonable.

  Two claims are checked, because the marts rest on both.

  **County grain is all-ages, on both sides of the comparison.** HLT-E2 compares a Michigan
  county against the national rate, and it is only a comparison if the two are the same age
  level. A county row at any other level would mean the source changed shape.

  **Every jurisdiction-year carries all three levels.** That is what makes filtering to one a
  complete series rather than a series with holes in it, and it is the property that lets the
  double-counting warning be stated as arithmetic instead of as advice.

  Warns rather than fails against the fixture: the sample keeps Michigan whole but carries
  other states at one or two age levels by construction, so the completeness half of this is
  a question a stratified sample cannot be asked.
*/

WITH county_age_levels AS (

    SELECT
        'county rows must be all-ages'      AS claim,
        county_fips                         AS subject,
        age_level                           AS detail,
        COUNT(*)                            AS rows_found
    FROM {{ ref('fct_hlt_medicare_service_county') }}
    WHERE age_level <> 'All'
    GROUP BY 1, 2, 3

),

jurisdiction_age_levels AS (

    SELECT
        'jurisdiction-years must carry all three age levels',
        geo_description,
        CAST(measure_year AS VARCHAR),
        COUNT(DISTINCT age_level)
    FROM {{ ref('fct_hlt_medicare_cost_annual') }}
    WHERE is_jurisdiction OR geo_level = 'National'
    GROUP BY 1, 2, 3
    HAVING COUNT(DISTINCT age_level) <> 3

)

SELECT * FROM county_age_levels
UNION ALL
SELECT * FROM jurisdiction_age_levels
