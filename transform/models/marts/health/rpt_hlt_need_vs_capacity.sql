{{ config(materialized = 'table') }}

/*
  **HLT-E5.** Michigan's 65-and-over population growth beside the direction of each screening
  and care indicator, so the two can be read against each other without being pretended to be
  the same quantity.

  **A row is an indicator comparison, never a person.** Public aggregate data only.

  ## The two sides are on different scales, and only their directions are compared

  Population growth is a percentage change in a count. An indicator change is a movement in
  percentage *points* of a survey estimate. Dividing one by the other would produce a number
  with no meaning, so this model does not: it publishes both, states each one's window, and
  compares only whether need is rising while capacity fails to rise with it.
  `need_vs_capacity_verdict` is that comparison and nothing more.

  ## The windows do not line up, and the model says so rather than hiding it

  The population side uses two **non-overlapping** ACS vintages -- consecutive vintages share
  four years of sample, so only every fifth is independent. The indicator side uses CDC survey
  cycles, which run on their own schedule and end in 2022. `population_window`,
  `indicator_window` and `windows_are_aligned` are columns because a reader comparing a
  2020-2024 population change against a 2015-2022 indicator change needs to see that mismatch
  at the point of use.

  ## Three things make a verdict unavailable, and each is stated rather than defaulted

    - **Polarity undefined.** `higher_is_better` is NULL in the seed wherever CDC's wording
      does not settle whether a rise is good. There is then no such thing as "improving".
    - **The 2019 comparability break.** CDC's `**` footnote marks estimates not comparable to
      years before 2019 because the survey questions changed. Any indicator whose endpoints
      straddle it returns `not comparable`.
    - **A suppressed or missing endpoint**, or a change inside sampling error, which reports
      `no significant change` rather than a direction invented from noise.

  ## This is not a projection and not a dementia measure

  ACS publishes an estimate of the population that already exists, not a forecast, so "growing"
  here is measured and not projected. And nothing on either side of this comparison measures
  dementia: the indicators are CDC's cognitive, screening and caregiving items, and the
  population is everyone aged 65 and over.
*/

WITH population AS (

    SELECT
        vintage_year,
        MAX(vintage_window_start)       AS window_start,
        MAX(vintage_window_end)         AS window_end,
        MAX(state_population_65_plus)   AS population_65_plus
    FROM {{ ref('fct_hlt_mi_population_65_plus') }}
    WHERE vintage_year IN (
        {{ var('acs_growth_from_vintage') }}, {{ var('acs_growth_to_vintage') }}
    )
    GROUP BY 1

),

population_change AS (

    SELECT
        f.window_start || '-' || f.window_end       AS from_window,
        t.window_start || '-' || t.window_end       AS to_window,
        f.vintage_year                              AS from_vintage,
        t.vintage_year                              AS to_vintage,
        f.population_65_plus                        AS population_from,
        t.population_65_plus                        AS population_to,
        t.population_65_plus - f.population_65_plus AS population_change,
        CASE
            WHEN f.population_65_plus > 0
            THEN 100.0 * (t.population_65_plus - f.population_65_plus) / f.population_65_plus
        END                                         AS population_change_pct,
        -- Proof, not assertion: the later window starts after the earlier one ends.
        t.window_start > f.window_end               AS windows_are_non_overlapping
    FROM population f
    CROSS JOIN population t
    WHERE f.vintage_year = {{ var('acs_growth_from_vintage') }}
      AND t.vintage_year = {{ var('acs_growth_to_vintage') }}

),

-- Michigan's own indicator series, single-year cycles only. A multi-year cycle shares sample
-- with the years inside it and is a level, never a trend endpoint.
indicator_points AS (

    SELECT
        question_id,
        question,
        class_id,
        indicator_family,
        short_label,
        higher_is_better,
        age_group,
        cycle_key,
        year_end,
        mi_value,
        mi_low,
        mi_high,
        mi_not_comparable_pre_2019
    FROM {{ ref('fct_hlt_cdc_mi_vs_national') }}
    WHERE is_unstratified_second_dimension
      AND NOT spans_multiple_years
      AND mi_value IS NOT NULL
      AND indicator_family IN ('screening', 'caregiving', 'cognitive_decline')

),

indicator_change AS (

    SELECT
        question_id,
        ANY_VALUE(question)             AS question,
        ANY_VALUE(class_id)             AS class_id,
        ANY_VALUE(indicator_family)     AS indicator_family,
        ANY_VALUE(short_label)          AS short_label,
        BOOL_OR(higher_is_better)       AS higher_is_better,
        BOOL_AND(higher_is_better IS NULL) AS polarity_is_undefined,
        age_group,

        MIN(year_end)                                       AS indicator_from_year,
        MAX(year_end)                                       AS indicator_to_year,
        arg_min(cycle_key, year_end)                        AS indicator_from_cycle,
        arg_max(cycle_key, year_end)                        AS indicator_to_cycle,
        arg_min(mi_value, year_end)                         AS indicator_from_value,
        arg_max(mi_value, year_end)                         AS indicator_to_value,
        arg_min(mi_low, year_end)                           AS indicator_from_low,
        arg_min(mi_high, year_end)                          AS indicator_from_high,
        arg_max(mi_low, year_end)                           AS indicator_to_low,
        arg_max(mi_high, year_end)                          AS indicator_to_high,
        -- Either endpoint flagged is enough to disqualify the comparison.
        COALESCE(arg_min(mi_not_comparable_pre_2019, year_end), FALSE)
            OR COALESCE(arg_max(mi_not_comparable_pre_2019, year_end), FALSE)
                                                            AS straddles_2019_break,
        COUNT(*)                                            AS cycles_available

    FROM indicator_points
    GROUP BY question_id, age_group

),

assessed AS (

    SELECT
        i.*,
        p.*,
        i.indicator_from_year || '-' || i.indicator_to_year AS indicator_window,
        i.indicator_to_value - i.indicator_from_value       AS indicator_change_pp,

        CASE
            WHEN i.cycles_available < 2                         THEN 'not comparable'
            WHEN i.straddles_2019_break                         THEN 'not comparable'
            WHEN i.indicator_from_low IS NULL OR i.indicator_from_high IS NULL
              OR i.indicator_to_low IS NULL OR i.indicator_to_high IS NULL
                                                                THEN 'not comparable'
            WHEN NOT (i.indicator_to_low > i.indicator_from_high
                   OR i.indicator_from_low > i.indicator_to_high)
                                                                THEN 'no significant change'
            WHEN i.polarity_is_undefined                        THEN NULL
            WHEN (i.indicator_to_value > i.indicator_from_value) = i.higher_is_better
                                                                THEN 'improving'
            ELSE 'deteriorating'
        END                                                 AS indicator_direction

    FROM indicator_change i
    CROSS JOIN population_change p

)

SELECT
    a.question_id,
    a.question,
    a.class_id,
    a.indicator_family,
    a.short_label,
    a.higher_is_better,
    a.age_group,

    -- need ------------------------------------------------------------------------
    a.from_vintage,
    a.to_vintage,
    a.from_window                                   AS population_window_from,
    a.to_window                                     AS population_window_to,
    a.population_from,
    a.population_to,
    a.population_change,
    a.population_change_pct,
    a.windows_are_non_overlapping,

    -- capacity --------------------------------------------------------------------
    a.indicator_from_cycle,
    a.indicator_to_cycle,
    a.indicator_window,
    a.cycles_available,
    a.indicator_from_value,
    a.indicator_to_value,
    a.indicator_change_pp,
    a.straddles_2019_break,
    a.indicator_direction,

    -- An ACS 5-year window and a CDC survey cycle never coincide; this states it per row
    -- rather than leaving a reader to notice.
    a.from_window = a.indicator_from_cycle
        AND a.to_window = a.indicator_to_cycle      AS windows_are_aligned,

    -- The comparison HLT-E5 actually asks for: is need rising while capacity is not?
    CASE
        WHEN a.indicator_direction IS NULL              THEN NULL
        WHEN a.indicator_direction = 'not comparable'   THEN 'not comparable'
        WHEN a.population_change_pct IS NULL            THEN 'not comparable'
        WHEN a.population_change_pct <= 0               THEN 'need not growing'
        WHEN a.indicator_direction = 'improving'        THEN 'capacity keeping pace'
        ELSE 'need outpacing capacity'
    END                                             AS need_vs_capacity_verdict

FROM assessed a
