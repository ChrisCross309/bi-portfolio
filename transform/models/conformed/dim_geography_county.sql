{{ config(materialized = 'table') }}

/*
  Every county key any landed source references, national, with what it actually is.

  National rather than Michigan-only on purpose. NFIP claims and CFPB complaints are national
  at county grain and the HUD crosswalk is national, so an 83-row Michigan dimension would
  leave every national county rollup with nothing to join, and the workaround would be a
  `WHERE state = 'MI'` buried in a mart -- the scope bug CLAUDE.md section 4 names as the
  single most likely one in this repo. `is_michigan` is how the Michigan lens gets applied,
  visibly, at the point of use.

  ## Why this is not simply the latest ACS vintage

  The obvious build -- take the current ACS county roster -- leaves real rows unjoinable.
  Measured against what the landed sources actually reference:

      NFIP claims     18 keys absent from the 2024 vintage
      CMS county      65
      HUD crosswalk   11
      HMDA             0

  Connecticut is the sharp edge. The 2022 ACS vintage replaced its eight counties with nine
  planning regions, so a roster built from the current vintage cannot place a flood claim
  filed in Fairfield County in 2011. Alaska has the same problem more quietly: 02261 split
  into two boroughs in 2019, and 02270 was renamed in 2015.

  Unioning all fifteen landed vintages fixes it -- 3,234 counties instead of 3,222, and the
  retired codes resolve to the names they were published under. `first_vintage`,
  `last_vintage` and `is_current_vintage` are what make that legible rather than mysterious:
  a county that stopped appearing after 2021 is a boundary change, not a data error.

  ## The three populations ACS will never contain

  `unknown_county` -- XX000, one per state. Where a publisher could not assign a county but
  the beneficiaries, claims or declarations are real. Michigan's 26000 / MI-UNKNOWN is the
  one the health README documents; every state has one. A county denominator either includes
  these explicitly or states that it does not, and it can do neither if they have no row.

  `island_area` -- the US Virgin Islands, Guam, American Samoa, the Northern Marianas, and
  the Freely Associated States that HUD serves. ACS 5-year county files exclude the island
  areas entirely, and NFIP carries 3,513 claims in them. INS-E5 ties our totals to FEMA's
  published national figures, which include the territories, so dropping them would break
  the reconciliation the whole repo is built to demonstrate. Three are named by CMS and come
  in through `seed_county_exceptions`; the rest carry a NULL name and say why.

  `retired` -- codes retired before the earliest landed vintage, so no publisher here names
  them. Kept, flagged, never silently dropped.

  ## On reading all four tracks

  A conformed dimension is the one place in `models/` that reads across tracks, and that is
  precisely its job: it exists so three domains resolve geography the same way. What it takes
  from each source is a key column, never a measure -- there is no insurance, lending or
  health fact in this table. `tests/test_transform_layout.py` enforces the separation where
  it applies, which is between marts.
*/

WITH acs_counties AS (

    -- All fifteen vintages, not only the newest. See the Connecticut note above.
    SELECT
        county_fips,
        state_fips,
        county_fips_3,
        MIN(vintage)                        AS first_vintage,
        MAX(vintage)                        AS last_vintage,
        arg_max(geography_name, vintage)    AS county_name_full
    FROM {{ ref('stg_ref__acs5_detailed') }}
    WHERE geo_level = 'county'
    GROUP BY 1, 2, 3

),

latest_vintage AS (

    SELECT MAX(vintage) AS vintage
    FROM {{ ref('stg_ref__acs5_detailed') }}

),

referenced AS (

    -- Every county key that exists anywhere in raw. Key columns only, never a measure.
    SELECT DISTINCT
        COALESCE(NULLIF(TRIM(countyCode), ''), NULLIF(SUBSTR(censusGeoid, 1, 5), ''))
            AS county_fips
    FROM {{ source('raw_insurance', 'nfip_claims') }}

    UNION

    SELECT DISTINCT NULLIF(SUBSTR(censusGeoid, 1, 5), '')
    FROM {{ source('raw_insurance', 'nfip_policies') }}

    UNION

    SELECT DISTINCT fipsStateCode || fipsCountyCode
    FROM {{ source('raw_insurance', 'fema_declarations') }}

    UNION

    SELECT DISTINCT BENE_GEO_CD
    FROM {{ source('raw_health', 'cms_geographic_variation') }}
    WHERE BENE_GEO_LVL = 'County'

    UNION

    SELECT DISTINCT county_code
    FROM {{ source('raw_fintech', 'hmda_lar') }}
    WHERE county_code <> 'NA'

    UNION

    SELECT DISTINCT geoid
    FROM {{ source('raw_shared', 'zip_county_crosswalk') }}

    UNION

    -- The subject tables carry counties the detailed tables do not: nineteen Guam election
    -- districts appear in the 2011 vintage alone, with every estimate null. Placeholder rows
    -- the publisher emitted once, and real keys either way -- so they belong in the roster
    -- rather than failing a join from a model that reads them.
    SELECT DISTINCT county_fips
    FROM {{ ref('stg_ref__acs5_subject') }}
    WHERE geo_level = 'county'

),

universe AS (

    SELECT county_fips FROM acs_counties

    UNION

    SELECT county_fips
    FROM referenced
    WHERE county_fips IS NOT NULL
      AND TRIM(county_fips) <> ''

),

classified AS (

    SELECT
        u.county_fips,
        COALESCE(a.state_fips, SUBSTR(u.county_fips, 1, 2))     AS state_fips,
        COALESCE(
            a.county_fips_3,
            CASE WHEN LENGTH(u.county_fips) = 5 THEN SUBSTR(u.county_fips, 3, 3) END
        )                                                       AS county_fips_3,
        a.first_vintage,
        a.last_vintage,
        a.county_name_full,
        e.county_name                                           AS exception_name,
        e.note                                                  AS exception_note,
        CASE
            WHEN a.county_fips IS NOT NULL
                THEN 'county'
            -- XX000 is how every publisher here spells "state known, county not".
            WHEN LENGTH(u.county_fips) = 5 AND RIGHT(u.county_fips, 3) = '000'
                THEN 'unknown_county'
            -- Island areas and the Freely Associated States: ACS county files omit them.
            WHEN SUBSTR(u.county_fips, 1, 2) IN ('60', '64', '66', '68', '69', '70', '78')
                THEN 'island_area'
            ELSE 'retired'
        END                                                     AS geography_kind
    FROM universe u
    LEFT JOIN acs_counties a
        ON a.county_fips = u.county_fips
    LEFT JOIN {{ ref('seed_county_exceptions') }} e
        ON e.county_fips = u.county_fips

)

SELECT
    c.county_fips,
    c.state_fips,
    c.county_fips_3,
    s.state_code,
    s.state_name,

    -- "Alcona County, Michigan" as published; the bare name is what a chart axis wants.
    -- The unassigned bucket gets CMS's own label rather than a NULL, so a chart that shows
    -- it shows what it is instead of a blank the reader has to guess at.
    COALESCE(
        NULLIF(SPLIT_PART(c.county_name_full, ',', 1), ''),
        c.exception_name,
        CASE WHEN c.geography_kind = 'unknown_county' THEN s.state_code || '-UNKNOWN' END
    )   AS county_name,
    c.county_name_full,

    c.geography_kind,
    c.geography_kind = 'county'                 AS is_reportable_county,
    c.state_fips = '26'                         AS is_michigan,

    c.first_vintage,
    c.last_vintage,
    -- FALSE means the boundary moved: Connecticut's counties, Alaska's splits.
    c.last_vintage = (SELECT vintage FROM latest_vintage)
        AS is_current_vintage,

    c.exception_note                            AS geography_note

FROM classified c
LEFT JOIN {{ ref('dim_state') }} s
    ON s.state_fips = c.state_fips
