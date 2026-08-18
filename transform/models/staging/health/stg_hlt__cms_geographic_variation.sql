{{ config(materialized = 'view') }}

/*
  Medicare Geographic Variation, typed. 36,994 rows over 2014-2024, national/state/county.

  **A row is a geographic aggregate, never a beneficiary.** Nothing in this file sums to a
  patient count, and no measure built on it describes any person. Public aggregate data only:
  no PHI, no re-identification, no individual-level inference.

  This model keeps the 44 columns that are not service-keyed -- the dimensions, the
  beneficiary population denominators, the demographic shares, readmissions, and the
  prevention quality indicators. The other 202 columns are the same twenty services measured
  four or five ways each, and they unpivot in `int_hlt__cms_service_measures` rather than
  staying as 202 columns nobody can hold in their head.

  Four things that will produce a wrong answer quietly.

  ## An asterisk is suppression, not a null and not a zero

  686,853 cells across the file carry `*` -- CMS suppressing a small cell. 1,401 of the 1,479
  suppressions in the skilled-nursing per-beneficiary column are county rows, which is where
  the small cells are and exactly where Michigan county analysis lives. Typed to NULL here
  with a companion flag, because a suppressed cell and an absent one are different facts and
  averaging over either without knowing which is a different mistake.

  ## Age levels double count

  County rows carry `All` only. State and national rows also appear as `<65` and `>=65`,
  which sum to their own `All`. Filter to one age level or a state total comes out twice its
  true size. HLT-E3 uses `>=65` deliberately; nothing here picks for it.

  ## The spending is fee-for-service only

  Medicare Advantage beneficiaries are counted in `benes_ma_cnt` but their utilization and
  spending are **not in this file**. Per-capita spend is therefore per fee-for-service
  beneficiary, and a county with high MA penetration looks different for reasons that have
  nothing to do with cost of care. `ma_participation_rate` is carried so that can be seen.

  ## Standardized and unstandardized are not interchangeable

  The `_stdzd_` columns remove geographic wage and payment-policy differences and are the
  ones that make a Michigan-versus-national comparison mean anything. The unstandardized
  twins measure something real but different, and mixing them is an easy invisible error.
  The unpivot carries the distinction as a flag rather than leaving it in a column name.
*/

WITH renamed AS (

    SELECT
        YEAR                        AS measure_year,
        BENE_GEO_LVL                AS geo_level,
        BENE_GEO_CD                 AS geo_code,
        BENE_GEO_DESC               AS geo_description,
        BENE_AGE_LVL                AS age_level,

        -- population denominators; not services, so they do not unpivot
        BENES_TOTAL_CNT             AS benes_total_cnt,
        BENES_WTH_PTAPTB_CNT        AS benes_with_part_a_and_b_cnt,
        BENES_OM_CNT                AS benes_original_medicare_cnt,
        BENES_MA_CNT                AS benes_medicare_advantage_cnt,
        MA_PRTCPTN_RATE             AS ma_participation_rate,

        BENE_AVG_AGE                AS bene_avg_age,
        BENE_FEML_PCT               AS bene_female_pct,
        BENE_MALE_PCT               AS bene_male_pct,
        BENE_RACE_WHT_PCT           AS bene_race_white_pct,
        BENE_RACE_BLACK_PCT         AS bene_race_black_pct,
        BENE_RACE_HSPNC_PCT         AS bene_race_hispanic_pct,
        BENE_RACE_OTHR_PCT          AS bene_race_other_pct,
        BENE_DUAL_PCT               AS bene_dual_eligible_pct,

        ACUTE_HOSP_READMSN_CNT      AS acute_hospital_readmission_cnt,
        ACUTE_HOSP_READMSN_PCT      AS acute_hospital_readmission_pct,
        TOT_PBPMT_RDCTN_AMT         AS total_payment_reduction_amt,
        TOT_PBPMT_RDCTN_PCC         AS total_payment_reduction_pct,

        -- Prevention quality indicators: admissions CMS considers potentially avoidable,
        -- by condition and age band. Not dementia -- the closest this file comes to a
        -- condition dimension, and it does not come close.
        PQI03_DBTS_AGE_LT_65        AS pqi03_diabetes_age_lt_65,
        PQI03_DBTS_AGE_65_74        AS pqi03_diabetes_age_65_74,
        PQI03_DBTS_AGE_GE_75        AS pqi03_diabetes_age_ge_75,
        PQI05_COPD_ASTHMA_AGE_40_64 AS pqi05_copd_asthma_age_40_64,
        PQI05_COPD_ASTHMA_AGE_65_74 AS pqi05_copd_asthma_age_65_74,
        PQI05_COPD_ASTHMA_AGE_GE_75 AS pqi05_copd_asthma_age_ge_75,
        PQI07_HYPRTNSN_AGE_LT_65    AS pqi07_hypertension_age_lt_65,
        PQI07_HYPRTNSN_AGE_65_74    AS pqi07_hypertension_age_65_74,
        PQI07_HYPRTNSN_AGE_GE_75    AS pqi07_hypertension_age_ge_75,
        PQI08_CHF_AGE_LT_65         AS pqi08_heart_failure_age_lt_65,
        PQI08_CHF_AGE_65_74         AS pqi08_heart_failure_age_65_74,
        PQI08_CHF_AGE_GE_75         AS pqi08_heart_failure_age_ge_75,
        PQI11_BCTRL_PNA_AGE_LT_65   AS pqi11_pneumonia_age_lt_65,
        PQI11_BCTRL_PNA_AGE_65_74   AS pqi11_pneumonia_age_65_74,
        PQI11_BCTRL_PNA_AGE_GE_75   AS pqi11_pneumonia_age_ge_75,
        PQI12_UTI_AGE_LT_65         AS pqi12_uti_age_lt_65,
        PQI12_UTI_AGE_65_74         AS pqi12_uti_age_65_74,
        PQI12_UTI_AGE_GE_75         AS pqi12_uti_age_ge_75,
        PQI15_ASTHMA_AGE_LT_40      AS pqi15_asthma_age_lt_40,
        PQI16_LWRXTRMTY_AMPUTN_AGE_LT_65  AS pqi16_amputation_age_lt_65,
        PQI16_LWRXTRMTY_AMPUTN_AGE_65_74  AS pqi16_amputation_age_65_74,
        PQI16_LWRXTRMTY_AMPUTN_AGE_GE_75  AS pqi16_amputation_age_ge_75

    FROM {{ source('raw_health', 'cms_geographic_variation') }}

),

{% set numeric_columns = [
    'benes_total_cnt', 'benes_with_part_a_and_b_cnt', 'benes_original_medicare_cnt',
    'benes_medicare_advantage_cnt', 'ma_participation_rate', 'bene_avg_age',
    'bene_female_pct', 'bene_male_pct', 'bene_race_white_pct', 'bene_race_black_pct',
    'bene_race_hispanic_pct', 'bene_race_other_pct', 'bene_dual_eligible_pct',
    'acute_hospital_readmission_cnt', 'acute_hospital_readmission_pct',
    'total_payment_reduction_amt', 'total_payment_reduction_pct',
    'pqi03_diabetes_age_lt_65', 'pqi03_diabetes_age_65_74', 'pqi03_diabetes_age_ge_75',
    'pqi05_copd_asthma_age_40_64', 'pqi05_copd_asthma_age_65_74', 'pqi05_copd_asthma_age_ge_75',
    'pqi07_hypertension_age_lt_65', 'pqi07_hypertension_age_65_74', 'pqi07_hypertension_age_ge_75',
    'pqi08_heart_failure_age_lt_65', 'pqi08_heart_failure_age_65_74', 'pqi08_heart_failure_age_ge_75',
    'pqi11_pneumonia_age_lt_65', 'pqi11_pneumonia_age_65_74', 'pqi11_pneumonia_age_ge_75',
    'pqi12_uti_age_lt_65', 'pqi12_uti_age_65_74', 'pqi12_uti_age_ge_75',
    'pqi15_asthma_age_lt_40', 'pqi16_amputation_age_lt_65', 'pqi16_amputation_age_65_74',
    'pqi16_amputation_age_ge_75'
] %}

typed AS (

    SELECT
        CAST(measure_year AS SMALLINT)  AS measure_year,
        geo_level,
        geo_code,
        geo_description,
        age_level,

        -- `*` is a suppressed small cell, so it types to NULL and the flag says why.
        {% for c in numeric_columns %}
        TRY_CAST(NULLIF({{ c }}, '*') AS DOUBLE) AS {{ c }},
        {% endfor %}

        -- How much of this row CMS withheld. A row with many suppressed cells is a small
        -- county, and a rate built on it deserves the same caution the insurance track gives
        -- a county with under ten claims.
        (
            {% for c in numeric_columns %}
            CASE WHEN {{ c }} = '*' THEN 1 ELSE 0 END{{ " +" if not loop.last }}
            {% endfor %}
        ) AS suppressed_cell_count

    FROM renamed

)

SELECT
    t.*,

    -- geography ------------------------------------------------------------------
    -- County rows carry a five-digit FIPS; state and national rows do not, so the join key
    -- is only meaningful at county level and says so rather than carrying a partial code.
    CASE WHEN t.geo_level = 'County' THEN t.geo_code END        AS county_fips,
    t.geo_level = 'County'                                      AS is_county_row,
    -- `XX000` is CMS's unassigned bucket -- Michigan's is 26000 / MI-UNKNOWN, eleven rows,
    -- one per year. Real beneficiaries whose county could not be determined. A county
    -- denominator either includes them explicitly or states that it does not.
    COALESCE(t.geo_level = 'County' AND RIGHT(t.geo_code, 3) = '000', FALSE)
                                                                AS is_unassigned_county,
    COALESCE(t.geo_level = 'County' AND t.geo_code LIKE '26%', FALSE)
                                                                AS is_michigan_county,

    -- grain ----------------------------------------------------------------------
    -- Every measure here is annual. There is no finer cadence to present it at, and
    -- CLAUDE.md section 8 forbids inventing one.
    CAST(t.measure_year AS VARCHAR)                             AS period_key,
    'year'                                                      AS period_grain,

    -- The age levels that double count if combined.
    t.age_level = 'All'                                         AS is_all_ages,

    -- Fee-for-service caveat, made a column so a mart cannot forget it.
    t.benes_original_medicare_cnt                               AS ffs_beneficiary_denominator,
    t.suppressed_cell_count > 0                                 AS has_suppressed_cells

FROM typed t
