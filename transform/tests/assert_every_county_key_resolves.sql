{{ config(severity = coverage_test_severity()) }}

/*
  Every county key any publisher writes must resolve against `dim_geography_county`.

  This is the test the dimension exists for. Built from the latest ACS vintage alone it
  would return 18 rows for NFIP claims, 65 for CMS and 11 for HUD -- Connecticut's pre-2022
  counties, Alaska's splits, the unassigned buckets and the island areas. It returns nothing.

  A failure here is a silent-row-loss bug in waiting: an inner join in a mart would simply
  drop whatever appears below, and the mart would still look plausible.

  `coverage_test_severity()` makes this warn against the fixture warehouse. The ACS fixture
  carries two vintages and forty non-Michigan counties by construction, so a stratified
  sample cannot answer a question about the whole roster -- the same rule
  `reconcile.michigan` follows when it skips the county roster in fixture mode.
*/

WITH referenced AS (

    SELECT 'nfip_claims' AS source_name,
           COALESCE(NULLIF(TRIM(countyCode), ''), NULLIF(SUBSTR(censusGeoid, 1, 5), '')) AS county_fips
    FROM {{ source('raw_insurance', 'nfip_claims') }}

    UNION

    SELECT 'nfip_policies', NULLIF(SUBSTR(censusGeoid, 1, 5), '')
    FROM {{ source('raw_insurance', 'nfip_policies') }}

    UNION

    SELECT 'fema_declarations', fipsStateCode || fipsCountyCode
    FROM {{ source('raw_insurance', 'fema_declarations') }}

    UNION

    SELECT 'cms_geographic_variation', BENE_GEO_CD
    FROM {{ source('raw_health', 'cms_geographic_variation') }}
    WHERE BENE_GEO_LVL = 'County'

    UNION

    SELECT 'hmda_lar', county_code
    FROM {{ source('raw_fintech', 'hmda_lar') }}
    WHERE county_code <> 'NA'

    UNION

    SELECT 'zip_county_crosswalk', geoid
    FROM {{ source('raw_shared', 'zip_county_crosswalk') }}

)

SELECT r.source_name, r.county_fips
FROM referenced r
LEFT JOIN {{ ref('dim_geography_county') }} d
    ON d.county_fips = r.county_fips
WHERE r.county_fips IS NOT NULL
  AND TRIM(r.county_fips) <> ''
  AND d.county_fips IS NULL
