{{ config(materialized = 'table') }}

/*
  **HLT-E1 - HLT-E4.** Michigan beside the national estimate for every CDC indicator, with
  the comparison made on confidence intervals rather than on point estimates.

  **A row is a cell, never a person.** Public aggregate data only.

  ## "No significant difference" is the answer, not a missing answer

  HLT-E1 asks whether Michigan is *significantly* better or worse than national. This is a
  self-reported telephone survey; state estimates carry real sampling error, and ranking point
  estimates would manufacture a story out of noise. Two intervals are treated as
  distinguishable only when they do not overlap:

      overlap  <=>  NOT (mi_low > us_high OR us_low > mi_high)

  On the subjective-cognitive-decline question that produces **"no significant difference" in
  all 27 comparable cells** -- nine cycles by three age groups. Michigan's point estimate is
  above national in most of them (12.7 against 11.6 in 2019) and never significantly so. The
  executive page shows that as the finding it is rather than forcing a direction arrow.

  `significance` states the raw direction; `benchmark_verdict` applies the indicator's
  polarity to it. The two are separate because polarity comes from a seed and is deliberately
  NULL where CDC's wording does not settle it -- a verdict is then unavailable even though
  the difference is real and measured.

  ## What "not comparable" covers

  A suppressed value on either side, a missing confidence limit on either side, or a cycle
  where one of the two has no row at all. Michigan publishes no 2016 row for the
  cognitive-decline question, so 2016 is `not comparable` rather than a silent gap.

  ## The direction columns stop at three boundaries

  A cycle-over-cycle change is computed only when all three hold, and is NULL otherwise:

    1. **Neither endpoint carries CDC's `**` footnote.** It reads "Estimate is not comparable
       to those generated using data from years prior to 2019 due to survey question changes"
       and sits on 1,057 valued rows, every one of them in the Caregiving class. Michigan's
       dementia-caregiving question has exactly two usable points, 2017 and a 2021 that is
       flagged -- so HLT-E4's honest answer for that indicator is "not comparable", and this
       model returns it rather than reporting a doubling that is a questionnaire change.
    2. **Both cycles are single-year.** The 2019-2022 and 2021-2022 cycles share sample with
       the single years around them; a multi-year estimate describes its whole window and is
       carried here as a level with no direction attached.
    3. **The two Michigan intervals do not overlap.** Same test as the benchmark, so a change
       that is within sampling error reports `no significant change`.
*/

WITH indicators AS (

    SELECT * FROM {{ ref('fct_hlt_cdc_indicators') }}
    WHERE location_abbr IN ('MI', 'US')

),

paired AS (

    SELECT
        i.cycle_key,
        MAX(i.year_start)                       AS year_start,
        MAX(i.year_end)                         AS year_end,
        BOOL_OR(i.spans_multiple_years)         AS spans_multiple_years,

        i.question_id,
        MAX(i.question)                         AS question,
        MAX(i.class_id)                         AS class_id,
        MAX(i.class)                            AS class,
        MAX(i.indicator_family)                 AS indicator_family,
        MAX(i.short_label)                      AS short_label,
        BOOL_OR(i.higher_is_better)             AS higher_is_better,
        BOOL_AND(i.higher_is_better IS NULL)    AS polarity_is_undefined,
        BOOL_OR(i.is_cognitive_decline)         AS is_cognitive_decline,
        BOOL_OR(i.is_caregiving)                AS is_caregiving,
        BOOL_OR(i.is_screening)                 AS is_screening,

        i.age_group,
        i.stratification,
        BOOL_OR(i.is_unstratified_second_dimension) AS is_unstratified_second_dimension,

        MAX(i.data_value)            FILTER (WHERE NOT i.is_michigan) AS national_value,
        MAX(i.low_confidence_limit)  FILTER (WHERE NOT i.is_michigan) AS national_low,
        MAX(i.high_confidence_limit) FILTER (WHERE NOT i.is_michigan) AS national_high,
        COALESCE(BOOL_OR(i.is_not_comparable_pre_2019) FILTER (WHERE NOT i.is_michigan), FALSE)
                                                                      AS national_not_comparable_pre_2019,
        COALESCE(BOOL_OR(i.is_fewer_than_50_states_reporting) FILTER (WHERE NOT i.is_michigan), FALSE)
                                                                      AS national_fewer_than_50_states,

        MAX(i.data_value)            FILTER (WHERE i.is_michigan)     AS mi_value,
        MAX(i.low_confidence_limit)  FILTER (WHERE i.is_michigan)     AS mi_low,
        MAX(i.high_confidence_limit) FILTER (WHERE i.is_michigan)     AS mi_high,
        COALESCE(BOOL_OR(i.is_not_comparable_pre_2019) FILTER (WHERE i.is_michigan), FALSE)
                                                                      AS mi_not_comparable_pre_2019

    FROM indicators i
    GROUP BY i.cycle_key, i.question_id, i.age_group, i.stratification

),

compared AS (

    SELECT
        p.*,

        -- Both sides present, both sides carrying an interval. Anything less is not a
        -- comparison, and saying so is the point.
        p.mi_value IS NOT NULL AND p.national_value IS NOT NULL
            AND p.mi_low IS NOT NULL AND p.mi_high IS NOT NULL
            AND p.national_low IS NOT NULL AND p.national_high IS NOT NULL
                                                    AS is_comparable,

        NOT (p.mi_low > p.national_high OR p.national_low > p.mi_high)
                                                    AS intervals_overlap,

        p.mi_value - p.national_value               AS gap_to_national

    FROM paired p

),

with_prior AS (

    SELECT
        c.*,
        -- Single-year cycles only: a multi-year window shares sample with the years inside
        -- it, so it is a level here and never a trend point.
        LAG(c.cycle_key) OVER w                             AS prior_cycle_key,
        LAG(c.mi_value)  OVER w                             AS mi_prior_value,
        LAG(c.mi_low)    OVER w                             AS mi_prior_low,
        LAG(c.mi_high)   OVER w                             AS mi_prior_high,
        LAG(c.mi_not_comparable_pre_2019) OVER w            AS mi_prior_not_comparable
    FROM compared c
    WINDOW w AS (
        PARTITION BY c.question_id, c.age_group, c.stratification
        ORDER BY CASE WHEN c.spans_multiple_years THEN 1 ELSE 0 END, c.year_end
    )

)

SELECT
    w.cycle_key,
    w.year_start,
    w.year_end,
    w.spans_multiple_years,

    w.question_id,
    w.question,
    w.class_id,
    w.class,
    w.indicator_family,
    w.short_label,
    w.higher_is_better,
    w.is_cognitive_decline,
    w.is_caregiving,
    w.is_screening,

    w.age_group,
    w.stratification,
    w.is_unstratified_second_dimension,

    w.mi_value,
    w.mi_low,
    w.mi_high,
    w.national_value,
    w.national_low,
    w.national_high,
    w.gap_to_national,
    w.national_fewer_than_50_states,
    w.mi_not_comparable_pre_2019,
    w.national_not_comparable_pre_2019,
    w.is_comparable,
    w.intervals_overlap,

    -- The benchmark, stated two ways ------------------------------------------------
    CASE
        WHEN NOT w.is_comparable        THEN 'not comparable'
        WHEN w.intervals_overlap        THEN 'no significant difference'
        WHEN w.mi_value > w.national_value THEN 'MI higher'
        ELSE 'MI lower'
    END                                                     AS significance,

    CASE
        WHEN NOT w.is_comparable        THEN 'not comparable'
        WHEN w.intervals_overlap        THEN 'no significant difference'
        WHEN w.polarity_is_undefined    THEN NULL
        WHEN (w.mi_value > w.national_value) = w.higher_is_better THEN 'MI better'
        ELSE 'MI worse'
    END                                                     AS benchmark_verdict,

    -- Direction over the prior single-year cycle -------------------------------------
    w.prior_cycle_key,
    w.mi_prior_value,
    CASE
        WHEN NOT w.spans_multiple_years AND w.mi_value IS NOT NULL AND w.mi_prior_value IS NOT NULL
             AND NOT w.mi_not_comparable_pre_2019 AND NOT COALESCE(w.mi_prior_not_comparable, TRUE)
        THEN w.mi_value - w.mi_prior_value
    END                                                     AS mi_change_from_prior,

    CASE
        WHEN w.spans_multiple_years                                     THEN NULL
        WHEN w.mi_value IS NULL OR w.mi_prior_value IS NULL             THEN 'not comparable'
        WHEN w.mi_not_comparable_pre_2019
             OR COALESCE(w.mi_prior_not_comparable, TRUE)               THEN 'not comparable'
        WHEN w.mi_low IS NULL OR w.mi_high IS NULL
             OR w.mi_prior_low IS NULL OR w.mi_prior_high IS NULL       THEN 'not comparable'
        WHEN NOT (w.mi_low > w.mi_prior_high OR w.mi_prior_low > w.mi_high)
                                                                        THEN 'no significant change'
        WHEN w.polarity_is_undefined                                    THEN NULL
        WHEN (w.mi_value > w.mi_prior_value) = w.higher_is_better       THEN 'improving'
        ELSE 'deteriorating'
    END                                                     AS mi_direction

FROM with_prior w
