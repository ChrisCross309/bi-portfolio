{{ config(materialized = 'table') }}

/*
  Every CDC Alzheimer's & Healthy Aging indicator cell, typed, catalogued and flagged.
  The level table behind HLT-E1, HLT-E4 and HLT-E5's indicator side.

  **A row is a cell -- an indicator for a place, a cycle and a stratification -- never a
  person.** Nothing here sums to a patient count. Public aggregate data only: no PHI, no
  re-identification, no individual-level inference.

  ## Adults 50 and older, not 45

  The only age strata CDC publishes in this file are `50-64 years`, `65 years or older` and
  `Overall`, in all ten cycles. Anything describing this source as covering adults 45+ is
  describing a different release.

  ## No county grain exists here at all

  `location_kind` is the whole geography story: 51 states and DC sit beside three territories,
  four census regions and a national row **as peers**, so summing the column counts every
  state up to three times. The rollups are kept because HLT-E1 compares Michigan against them.
  Michigan *county* analysis in this track comes from CMS and never from here.

  ## Three footnotes that change what a number may be used for

  `is_not_comparable_pre_2019` is the sharp one. CDC's `**` footnote reads "Estimate is not
  comparable to those generated using data from years prior to 2019 due to survey question
  changes", and it lands on 1,057 valued rows -- **every one of them in the Caregiving
  class**. Nationally the dementia-caregiving question jumps 13.6 to 28.5 between 2018 and
  2019 for that reason alone. A direction computed across the boundary measures a
  questionnaire change.

  `is_fewer_than_50_states_reporting` (`#`) marks a national estimate built from an
  incomplete set of states, and `is_regional_estimate` (`&`) marks a census-region row that
  may not represent every state in it. Both are published beside the value rather than
  filtered out, because whether they disqualify a comparison depends on the comparison.

  ## Polarity comes from a seed, not from the class

  "Better or worse" cannot be read off a percentage, and two questions break their own class:
  `Q42` sits in Cognitive Decline but asks whether someone *discussed* their decline with a
  professional, where higher is better; `Q22` sits in Screenings and Vaccines but reports
  blood-pressure prevalence, where higher is worse. `higher_is_better` is left NULL wherever
  CDC's wording does not settle the direction -- caregiving prevalence is genuinely
  two-sided -- and every direction downstream is NULL when it is.
*/

SELECT
    -- period ---------------------------------------------------------------------
    c.cycle_key,
    'survey_cycle:' || c.cycle_key                  AS period_id,
    c.year_start,
    c.year_end,
    c.period_grain,
    c.spans_multiple_years,

    -- place ----------------------------------------------------------------------
    c.location_abbr,
    c.location_desc,
    c.location_kind,
    c.is_michigan,

    -- indicator ------------------------------------------------------------------
    c.class_id,
    c.class,
    c.topic,
    c.question_id,
    c.question,
    cat.indicator_family,
    cat.short_label,
    cat.higher_is_better,
    c.is_cognitive_decline,
    c.is_caregiving,
    COALESCE(cat.indicator_family = 'screening', FALSE)     AS is_screening,

    -- stratification --------------------------------------------------------------
    c.stratification_1                                      AS age_group,
    COALESCE(c.stratification_2, 'Overall')                 AS stratification,
    COALESCE(c.stratification_category_2, 'Overall')        AS stratification_category,
    c.is_overall_age_group,
    c.is_unstratified_second_dimension,

    -- value ----------------------------------------------------------------------
    c.data_value_type,
    c.data_value_unit,
    c.data_value,
    c.low_confidence_limit,
    c.high_confidence_limit,
    c.is_suppressed,
    c.has_confidence_interval,

    -- what the footnotes forbid ----------------------------------------------------
    c.footnote_symbol,
    c.footnote_text,
    COALESCE(c.footnote_symbol = '**', FALSE)   AS is_not_comparable_pre_2019,
    COALESCE(c.footnote_symbol = '#', FALSE)    AS is_fewer_than_50_states_reporting,
    COALESCE(c.footnote_symbol = '&', FALSE)    AS is_regional_estimate

FROM {{ ref('stg_hlt__cdc_healthy_aging') }} c
LEFT JOIN {{ ref('seed_cdc_indicator_catalog') }} cat
    ON cat.question_id = c.question_id
