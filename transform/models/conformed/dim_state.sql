{{ config(materialized = 'table') }}

/*
  States, DC and territories, plus the three postal areas that are not any of those.

  The 56 rows in `state_codes.csv` are the reference every L1 partition check already rests
  on, seeded here rather than copied so there is one file. Census region and division come
  with them, which is what INS-E2's "vs the national median" and HLT-E1's Great Lakes peer
  comparison are built on.

  `seed_areas` adds FM, MH and PW -- Micronesia, the Marshall Islands and Palau. They are
  sovereign nations with US postal service rather than US territories, which is exactly why
  they are not in the state reference: the ingestion layer counts them as nonstandard
  partition keys and warns about them, deliberately. But HUD's crosswalk serves them and
  FEMA has declared disasters in them, so rows keyed on them exist and need somewhere to
  join. `is_us_jurisdiction` is the column that keeps the two kinds apart.
*/

SELECT
    state_code,
    state_fips,
    state_name,
    entity_type,
    -- Cast rather than relying on the seed's `+column_types`: these are codes, not
    -- quantities, and the model should state its own types where a contract enforces them.
    CAST(census_region AS VARCHAR)    AS census_region,
    census_region_name,
    CAST(census_division AS VARCHAR)  AS census_division,
    census_division_name,
    TRUE                        AS is_us_jurisdiction,
    state_code = 'MI'           AS is_michigan
FROM {{ ref('state_codes') }}

UNION ALL

SELECT
    state_code,
    state_fips,
    area_name                   AS state_name,
    entity_type,
    CAST(NULL AS VARCHAR)       AS census_region,
    CAST(NULL AS VARCHAR)       AS census_region_name,
    CAST(NULL AS VARCHAR)       AS census_division,
    CAST(NULL AS VARCHAR)       AS census_division_name,
    FALSE                       AS is_us_jurisdiction,
    FALSE                       AS is_michigan
FROM {{ ref('seed_areas') }}

UNION ALL

-- FEMA's own code for a claim whose state it could not determine: 16,441 NFIP claims,
-- mostly pre-1990. Carried for the same reason `dim_geography_county` carries the XX000
-- buckets -- so a state join is total and those rows land somewhere visible instead of
-- vanishing. A national total that silently drops them is wrong by exactly that many
-- claims, which is what INS-E5 would catch against FEMA's published figures.
SELECT
    'UN'                        AS state_code,
    CAST(NULL AS VARCHAR)       AS state_fips,
    'State unavailable'         AS state_name,
    'unknown'                   AS entity_type,
    CAST(NULL AS VARCHAR)       AS census_region,
    CAST(NULL AS VARCHAR)       AS census_region_name,
    CAST(NULL AS VARCHAR)       AS census_division,
    CAST(NULL AS VARCHAR)       AS census_division_name,
    CAST(NULL AS BOOLEAN)       AS is_us_jurisdiction,
    FALSE                       AS is_michigan
