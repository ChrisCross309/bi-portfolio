{{ config(materialized = 'view') }}

/*
  The 202 service-keyed CMS columns, turned long.

  Medicare Geographic Variation publishes twenty service categories measured four or five
  ways each, one column per combination. Kept wide, the health drill bank -- service
  category, standardized versus unstandardized, payment versus utilization -- is twenty
  separate models or one query nobody can read. Turned long it is a filter, and HLT-E2 and
  the whole service-category drill become a `WHERE` clause.

  The grain is one row per **geography x year x age level x service x measure**.

  ## What is not here

  The 44 columns that are not service-keyed stay wide in the staging model: the dimensions,
  the beneficiary population denominators, the demographic shares, readmissions, payment
  reductions and the prevention quality indicators. Unpivoting those would put a share of
  female beneficiaries and a dollar per beneficiary in the same value column, which is a
  tidier table and a worse one.

  Note also `BENES_TOTAL_CNT`, `BENES_MA_CNT`, `BENES_OM_CNT` and `BENES_WTH_PTAPTB_CNT`.
  They match the `BENES_*_CNT` shape exactly but are population denominators rather than
  service usage, so they are deliberately excluded from the unpivot and kept beside the
  measures they divide.

  ## Two CMS naming inconsistencies, absorbed here

  Every service writes its per-user payment as `_PER_USER` except one: `FQHC_RHC` uses `_PU`.
  And `PTB_OTHR_SRVCS_MDCR_STDZD_PYMT` carries no measure suffix at all where every sibling
  has one. Both are the publisher's own, and both are normalised here so a mart filtering on
  `measure_kind` does not need to know either.

  ## What a value means

  `measure_kind` says which question the number answers, and they are not interchangeable:

      amount        total dollars for the geography
      per_capita    dollars per fee-for-service beneficiary -- the comparison measure
      per_user      dollars per beneficiary who actually used the service
      share_of_pymt this service as a percentage of that geography's total payments
      user_count    beneficiaries who used the service
      user_pct      share of beneficiaries who used it
      per_1000      events, stays, days or visits per 1,000 beneficiaries

  `is_standardized` is the flag that stops the single easiest invisible error in this track.
  CMS's standardized columns remove geographic wage and payment-policy differences, and only
  those make a Michigan-versus-national comparison mean anything. The unstandardized twins
  measure something real and different. Keeping it a flag rather than leaving it buried in a
  column name is what lets a test assert a cross-geography measure used the right one.
*/

WITH source AS (

    SELECT * FROM {{ source('raw_health', 'cms_geographic_variation') }}

),

long AS (

    UNPIVOT (
        SELECT
            YEAR            AS measure_year,
            BENE_GEO_LVL    AS geo_level,
            BENE_GEO_CD     AS geo_code,
            BENE_GEO_DESC   AS geo_description,
            BENE_AGE_LVL    AS age_level,
            COLUMNS(c -> c NOT IN (
                'YEAR', 'BENE_GEO_LVL', 'BENE_GEO_CD', 'BENE_GEO_DESC', 'BENE_AGE_LVL',
                'BENES_TOTAL_CNT', 'BENES_WTH_PTAPTB_CNT', 'BENES_OM_CNT', 'BENES_MA_CNT',
                'MA_PRTCPTN_RATE', 'BENE_AVG_AGE', 'BENE_FEML_PCT', 'BENE_MALE_PCT',
                'BENE_RACE_WHT_PCT', 'BENE_RACE_BLACK_PCT', 'BENE_RACE_HSPNC_PCT',
                'BENE_RACE_OTHR_PCT', 'BENE_DUAL_PCT', 'ACUTE_HOSP_READMSN_CNT',
                'ACUTE_HOSP_READMSN_PCT', 'TOT_PBPMT_RDCTN_AMT', 'TOT_PBPMT_RDCTN_PCC'
            ) AND c NOT LIKE 'PQI%')
        FROM source
    )
    ON COLUMNS(* EXCLUDE (measure_year, geo_level, geo_code, geo_description, age_level))
    INTO NAME source_column VALUE raw_value

),

parsed AS (

    SELECT
        measure_year,
        geo_level,
        geo_code,
        geo_description,
        age_level,
        source_column,
        raw_value,

        -- Standardized payments live behind `_MDCR_STDZD_PYMT`; everything else is not.
        source_column LIKE '%\_MDCR\_STDZD\_PYMT%' ESCAPE '\'      AS is_standardized,

        CASE
            WHEN source_column LIKE '%\_PER\_1000\_BENES' ESCAPE '\'  THEN 'per_1000'
            WHEN source_column LIKE 'BENES\_%\_CNT' ESCAPE '\'        THEN 'user_count'
            WHEN source_column LIKE 'BENES\_%\_PCT' ESCAPE '\'        THEN 'user_pct'
            WHEN source_column LIKE '%\_PYMT\_AMT' ESCAPE '\'         THEN 'amount'
            WHEN source_column LIKE '%\_PYMT\_PC' ESCAPE '\'          THEN 'per_capita'
            WHEN source_column LIKE '%\_PYMT\_PCT' ESCAPE '\'         THEN 'share_of_pymt'
            -- FQHC_RHC writes _PU where every other service writes _PER_USER.
            WHEN source_column LIKE '%\_PYMT\_PER\_USER' ESCAPE '\'
                 OR source_column LIKE '%\_PYMT\_PU' ESCAPE '\'       THEN 'per_user'
            -- PTB_OTHR_SRVCS_MDCR_STDZD_PYMT has no measure suffix at all.
            WHEN source_column LIKE '%\_PYMT' ESCAPE '\'              THEN 'amount'
        END AS measure_kind,

        CASE
            WHEN source_column LIKE 'BENES\_%' ESCAPE '\'
                THEN regexp_replace(
                        regexp_replace(source_column, '^BENES_', ''),
                        '_(CNT|PCT)$', ''
                     )
            WHEN source_column LIKE '%\_PER\_1000\_BENES' ESCAPE '\'
                THEN regexp_replace(
                        source_column,
                        '_(CVRD_STAYS|CVRD_DAYS|EPISODES|VISITS|EVNTS)_PER_1000_BENES$', ''
                     )
            ELSE regexp_replace(source_column, '_MDCR_(STDZD_)?PYMT(_.*)?$', '')
        END AS service_code_raw

    FROM long

),

normalised AS (

    SELECT
        parsed.* EXCLUDE (service_code_raw),
        -- CMS spells three services differently between its measure families.
        CASE service_code_raw
            WHEN 'IP_CVRD_STAY' THEN 'IP'
            WHEN 'ER_VISITS'    THEN 'ER'
            WHEN 'PRCDR'        THEN 'PRCDRS'
            ELSE service_code_raw
        END AS service_code
    FROM parsed

)

SELECT
    CAST(n.measure_year AS SMALLINT)                AS measure_year,
    CAST(n.measure_year AS VARCHAR)                 AS period_key,
    'year'                                          AS period_grain,

    n.geo_level,
    n.geo_code,
    n.geo_description,
    CASE WHEN n.geo_level = 'County' THEN n.geo_code END    AS county_fips,
    COALESCE(n.geo_level = 'County' AND n.geo_code LIKE '26%', FALSE)
                                                    AS is_michigan_county,

    -- County rows are 'All' only; state and national also publish '<65' and '>=65', which
    -- sum to their own 'All'. Combining them doubles a state total.
    n.age_level,

    n.service_code,
    s.service_name,
    s.is_long_term_care,

    n.measure_kind,
    n.is_standardized,

    -- `*` is a suppressed small cell: not zero, not missing data.
    TRY_CAST(NULLIF(n.raw_value, '*') AS DOUBLE)    AS measure_value,
    n.raw_value = '*'                               AS is_suppressed,
    n.source_column

FROM normalised n
LEFT JOIN {{ ref('seed_cms_service_category') }} s
    ON s.service_code = n.service_code
