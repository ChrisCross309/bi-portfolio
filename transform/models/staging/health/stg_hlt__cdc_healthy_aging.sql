{{ config(materialized = 'view') }}

/*
  CDC Alzheimer's Disease and Healthy Aging, typed. 284,142 cells, ten survey cycles.

  **A row is a cell -- an indicator for a place, a cycle and a stratification -- never a
  person.** Nothing here sums to a patient count. Public aggregate data only: no PHI, no
  re-identification, no individual-level inference.

  This is the track's genuinely cognitive source: HLT-E1 comes from its Cognitive Decline
  class and HLT-E4 from Caregiving. Four things about it that mislead.

  ## rowid is not a key

  284,142 rows carry 36,046 distinct values, because it encodes the stratification categories
  and not their values. It is never tested for uniqueness and never joined on. The real grain
  is the one below, and it is recorded in the source's manifest.

  ## locationabbr mixes four kinds of place as peers

  51 states and DC sit beside three territories, four census regions (MDW, NRE, SOU, WEST)
  and a national US row, all in one column. Summing across it counts every state up to three
  times. The rollups are kept deliberately, because HLT-E1 compares Michigan against them --
  but nothing may aggregate over the column without filtering `location_kind` first.

  This source has **no county grain at all**. Michigan county analysis in this track comes
  from CMS.

  ## Suppression and footnotes are different things

  91,334 rows have an empty `data_value` with a footnote giving the reason -- `****` where
  the sample was too small to age-standardize, `~` where no data was available. But a
  footnote can also annotate a value that *is* present: `**`, `#` and `&` appear on populated
  rows. So `is_suppressed` asks whether there is a value, and `footnote_symbol` is carried
  separately rather than being read as the same question.

  `data_value_alt` is byte-identical to `data_value` on every one of the 284,142 rows, so
  only one is carried.

  ## Confidence intervals are not decoration

  HLT-E1 asks whether Michigan is *significantly* better or worse than national. This is a
  self-reported telephone survey and its state estimates carry real sampling error, so
  ranking point estimates would manufacture a story out of noise. The limits are typed here
  so a comparison can return "no significant difference" -- which is a finding, not a failure
  to find one.

  ## Two cycles span several years

  Eight of the ten cycles are single years; 2019-2022 and 2021-2022 are not, and between them
  they carry 9,261 rows. `cycle_key` joins `dim_period` at survey-cycle grain, where
  `spans_multiple_years` marks them, because a multi-year estimate describes its whole window
  and must never be plotted as a point on its final year.
*/

WITH renamed AS (

    SELECT
        rowid                           AS cdc_rowid,
        yearstart                       AS year_start,
        yearend                         AS year_end,
        locationabbr                    AS location_abbr,
        locationdesc                    AS location_desc,
        locationid                      AS location_id,
        datasource                      AS data_source,
        class,
        classid                         AS class_id,
        topic,
        topicid                         AS topic_id,
        question,
        questionid                      AS question_id,
        data_value_unit,
        data_value_type,
        datavaluetypeid                 AS data_value_type_id,
        data_value,
        data_value_footnote_symbol      AS footnote_symbol,
        data_value_footnote             AS footnote_text,
        low_confidence_limit,
        high_confidence_limit,
        stratificationcategory1         AS stratification_category_1,
        stratification1                 AS stratification_1,
        stratificationcategoryid1       AS stratification_category_id_1,
        stratificationid1               AS stratification_id_1,
        stratificationcategory2         AS stratification_category_2,
        stratification2                 AS stratification_2,
        stratificationcategoryid2       AS stratification_category_id_2,
        stratificationid2               AS stratification_id_2,
        geolocation
    FROM {{ source('raw_health', 'cdc_healthy_aging') }}

)

SELECT
    r.cdc_rowid,

    -- cycle ----------------------------------------------------------------------
    CAST(r.year_start AS SMALLINT)                  AS year_start,
    CAST(r.year_end AS SMALLINT)                    AS year_end,
    CASE
        WHEN r.year_start = r.year_end THEN r.year_start
        ELSE r.year_start || '-' || r.year_end
    END                                             AS cycle_key,
    'survey_cycle'                                  AS period_grain,
    r.year_start <> r.year_end                      AS spans_multiple_years,

    -- place ----------------------------------------------------------------------
    r.location_abbr,
    r.location_desc,
    r.location_id,
    -- The classification that stops a sum across states, regions and the nation at once.
    CASE
        WHEN r.location_abbr = 'US'                             THEN 'national'
        WHEN r.location_abbr IN ('MDW', 'NRE', 'SOU', 'WEST')   THEN 'census_region'
        WHEN s.entity_type = 'state'                            THEN 'state'
        WHEN s.entity_type = 'district'                         THEN 'district'
        WHEN s.entity_type = 'territory'                        THEN 'territory'
        ELSE 'other'
    END                                             AS location_kind,
    r.location_abbr = 'MI'                          AS is_michigan,

    -- indicator ------------------------------------------------------------------
    r.class,
    r.class_id,
    r.topic,
    r.topic_id,
    r.question,
    r.question_id,
    r.data_source,
    LOWER(r.class) LIKE '%cognitive decline%'       AS is_cognitive_decline,
    LOWER(r.class) LIKE '%caregiv%'                 AS is_caregiving,

    -- value ----------------------------------------------------------------------
    r.data_value_type,
    r.data_value_type_id,
    r.data_value_unit,
    -- Numeric on every row that has one; the suppressed rows are empty rather than symbolic.
    TRY_CAST(NULLIF(TRIM(r.data_value), '') AS DOUBLE)      AS data_value,
    TRY_CAST(NULLIF(TRIM(r.low_confidence_limit), '') AS DOUBLE)  AS low_confidence_limit,
    TRY_CAST(NULLIF(TRIM(r.high_confidence_limit), '') AS DOUBLE) AS high_confidence_limit,
    -- Suppression is "there is no value here", which is not the same question as "is there a
    -- footnote": 91,334 rows are suppressed, and other rows carry a footnote beside a value.
    COALESCE(NULLIF(TRIM(r.data_value), '') IS NULL, TRUE)  AS is_suppressed,
    r.footnote_symbol,
    r.footnote_text,
    r.footnote_symbol IS NOT NULL                   AS has_footnote,
    -- A comparison is only defensible where both limits exist.
    NULLIF(TRIM(r.low_confidence_limit), '') IS NOT NULL
        AND NULLIF(TRIM(r.high_confidence_limit), '') IS NOT NULL
                                                    AS has_confidence_interval,

    -- stratification --------------------------------------------------------------
    r.stratification_category_1,
    r.stratification_1,
    r.stratification_category_2,
    r.stratification_2,
    r.stratification_1 = 'Overall'                  AS is_overall_age_group,
    COALESCE(r.stratification_category_2 = 'None', TRUE)
                                                    AS is_unstratified_second_dimension,

    r.geolocation

FROM renamed r
LEFT JOIN {{ ref('dim_state') }} s
    ON s.state_code = r.location_abbr
