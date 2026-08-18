/*
  An ACS growth figure across overlapping vintages is mostly the same survey moving against
  itself.

  Consecutive 5-year vintages share four years of sample: the 2023 vintage covers 2019-2023
  and the 2024 covers 2020-2024. Differencing them looks like measuring change and is largely
  measuring the same respondents twice, which is why the Census Bureau advises against
  comparing overlapping releases. Only every fifth vintage is independent.

  HLT-E5 rests entirely on one such difference, so the pair it uses is a pair of variables --
  `acs_growth_from_vintage` and `acs_growth_to_vintage` -- and this is what stops someone
  moving one of them to get a longer series and quietly reintroducing the overlap. Michigan's
  65+ population grew 12.6% between the 2015-2019 and 2020-2024 windows; the same arithmetic
  across 2023 and 2024 would report roughly a tenth of that and mean nothing at all.

  Three claims: the model agrees the windows do not overlap, the windows genuinely do not
  overlap when the dates are checked rather than trusted, and the vintages the variables name
  are actually present in the data.
*/

WITH model_claim AS (

    SELECT
        'model reports overlapping windows'         AS claim,
        population_window_from || ' -> ' || population_window_to AS detail
    FROM {{ ref('rpt_hlt_need_vs_capacity') }}
    WHERE NOT windows_are_non_overlapping
    GROUP BY 1, 2

),

arithmetic AS (

    -- The claim re-derived from the vintage years themselves. A 5-year vintage covers its
    -- own year and the four before it, so the later window starts at `to - 4` and the earlier
    -- one ends at `from`.
    SELECT
        'configured vintages overlap',
        '{{ var('acs_growth_from_vintage') }} -> {{ var('acs_growth_to_vintage') }}'
    WHERE {{ var('acs_growth_to_vintage') }} - 4 <= {{ var('acs_growth_from_vintage') }}

),

vintages_present AS (

    SELECT
        'configured vintage missing from the data',
        CAST(v.vintage_year AS VARCHAR)
    FROM (
        SELECT {{ var('acs_growth_from_vintage') }} AS vintage_year
        UNION ALL
        SELECT {{ var('acs_growth_to_vintage') }}
    ) v
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ ref('fct_hlt_mi_population_65_plus') }} p
        WHERE p.vintage_year = v.vintage_year
    )

)

SELECT * FROM model_claim
UNION ALL
SELECT * FROM arithmetic
UNION ALL
SELECT * FROM vintages_present
