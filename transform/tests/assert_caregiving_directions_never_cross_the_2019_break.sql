/*
  CDC changed its caregiving survey questions in 2019, and says so in the data.

  The `**` footnote reads "Estimate is not comparable to those generated using data from
  years prior to 2019 due to survey question changes". It sits on 1,057 valued rows, **every
  one of them in the Caregiving class** -- so this is not a general caution, it is a specific
  break in one part of one source, and HLT-E4 is the question that runs straight through it.

  What it costs is visible: nationally the dementia-caregiving question reads 13.6 in 2018 and
  28.5 in 2019. Michigan's only two usable points on that question are 2017 and a 2021 that
  carries the flag. A trend line through either pair would report a doubling of dementia
  caregiving that is a change in how the question was asked.

  So a direction may never be computed from a flagged endpoint. The model withholds it; this
  refuses to let a later edit hand it back. It also covers the second way a false trend gets
  built here -- a multi-year cycle used as a trend point, when 2019-2022 and 2021-2022 share
  sample with the single years inside them.
*/

SELECT
    question_id,
    age_group,
    cycle_key,
    prior_cycle_key,
    mi_change_from_prior,
    mi_direction,
    mi_not_comparable_pre_2019,
    spans_multiple_years
FROM {{ ref('fct_hlt_cdc_mi_vs_national') }}
WHERE
    -- A change computed from an estimate CDC says is not comparable.
    (mi_change_from_prior IS NOT NULL AND mi_not_comparable_pre_2019)

    -- Or a change computed on a cycle that describes a window rather than a year.
    OR (mi_change_from_prior IS NOT NULL AND spans_multiple_years)

    -- Or a direction asserted where the change itself was withheld.
    OR (mi_direction IN ('improving', 'deteriorating') AND mi_change_from_prior IS NULL)
