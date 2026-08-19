{{ config(materialized = 'table') }}

/*
  **FIN-E1 · FIN-E2.** Complaints by month, county and product family, national, with the
  population denominator attached.

  Counts are fractional because they are allocated from ZIP, and rounding them would break the
  property that makes the allocation defensible: they re-sum exactly to the state totals.

  ## The denominator is chosen by window, not by recency

  The per-capita rate uses the ACS vintage whose five-year window contains the complaint year,
  not the newest vintage. A 5-year estimate describes its window rather than its final year,
  and consecutive vintages overlap by four years -- so pairing 2013 complaints with a 2024
  population would compare a rate against the wrong denominator while looking entirely normal.

  Complaint years run past the newest ACS window: the archive reaches August 2026 while ACS
  stops at the 2020-2024 vintage. Those years fall back to that newest vintage, because a
  per-capita rate for the latest complete month is the whole of FIN-E1 and refusing to compute
  one would answer the question with a blank. `population_is_extrapolated` marks every row
  where that happened, so the fallback is visible rather than assumed -- 529,950 rows would
  otherwise have simply had no rate.

  ## Two things this measure is not

  Recent months are incomplete by design. Join rpt_fin_publication_window and filter on
  is_publication_complete before trending anything, or the newest month always shows a fall
  that is a publication artifact rather than a change in consumer behaviour.

  And this is complaints **to the CFPB**, not consumer dissatisfaction. Complaints referred to
  other regulators -- depositories under $10 billion among them -- are absent entirely, so the
  per-capita rate measures one channel and not a population's experience.
*/

WITH allocated AS (

    SELECT * FROM {{ ref('int_fin__complaint_county_allocation') }}

),

population AS (

    -- One row per county per vintage. ACS 5-year windows **overlap** -- 2018 falls inside
    -- five of them -- so "the vintage whose window contains this year" matches many rows and
    -- fans the join out, repeating every measure once per matching vintage. The rule is one
    -- vintage per year: the estimate ending in that year, clamped to the published range.
    SELECT county_fips, vintage_year, vintage_window_start, vintage_window_end, total_population
    FROM {{ ref('stg_ref__acs5_detailed') }}
    WHERE geo_level = 'county'
      AND total_population IS NOT NULL

),

vintage_bounds AS (

    SELECT MIN(vintage_year) AS earliest_vintage, MAX(vintage_year) AS latest_vintage
    FROM population

)

SELECT
    a.received_year_month                                   AS year_month,
    'month:' || a.received_year_month                       AS period_id,
    CAST(SUBSTR(a.received_year_month, 1, 4) AS SMALLINT)   AS received_year,
    a.state_code,
    a.county_fips,
    g.county_name,
    COALESCE(g.is_michigan, FALSE)                          AS is_michigan,
    g.geography_kind,
    a.product_family,
    a.allocation_method,

    a.allocated_complaints,
    a.allocated_timely,
    a.allocated_relief,
    a.allocated_pending,
    a.allocated_older_american,

    p.total_population                                      AS county_population,
    p.vintage_window_start || '-' || p.vintage_window_end   AS population_vintage_window,
    CAST(SUBSTR(a.received_year_month, 1, 4) AS SMALLINT)
        NOT BETWEEN b.earliest_vintage AND b.latest_vintage AS population_is_extrapolated,
    CASE
        WHEN p.total_population > 0
        THEN 100000.0 * a.allocated_complaints / p.total_population
    END                                                     AS complaints_per_100k,

    a.allocated_complaints < 10                             AS is_small_cell

FROM allocated a
LEFT JOIN {{ ref('dim_geography_county') }} g
    ON g.county_fips = a.county_fips
CROSS JOIN vintage_bounds b
LEFT JOIN population p
    ON  p.county_fips = a.county_fips
    AND p.vintage_year = LEAST(
            GREATEST(CAST(SUBSTR(a.received_year_month, 1, 4) AS SMALLINT), b.earliest_vintage),
            b.latest_vintage
        )
