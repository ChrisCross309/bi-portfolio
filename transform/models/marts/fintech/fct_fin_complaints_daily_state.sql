{{ config(materialized = 'table') }}

/*
  Complaints by the day they were received, by state, and by CFPB's full product and issue
  taxonomy. The two drills the other complaint marts deliberately cannot carry.

  ## Why this exists rather than widening the marts that were already here

  The fintech drill bank asks for two things neither of them can give. The
  daily-weekly-monthly-quarterly-YTD ladder needs a daily grain, and the
  product -> sub-product -> issue drill needs the taxonomy CFPB publishes underneath
  `product_family`. Adding either to an existing mart was measured and rejected:

    `fct_fin_complaints_monthly_geo` is county-grain and exists so FIN-E1 and FIN-E2 can
    divide by population. It costs 2,330,387 rows at ten product families. The taxonomy is
    127 product/sub-product pairs and 441 issue/sub-issue pairs, so putting it there
    multiplies a county-allocated mart by an order of magnitude to serve a drill that has
    no per-capita question in it.

    `fct_fin_complaints_monthly_company` is state-grain by company, which is already high
    cardinality, and FIN-E3 asks which companies serve Michigan -- a question the product
    family answers and the sub-issue does not.

  So the taxonomy lives here, at state grain, where it costs 3,134,415 rows. Each mart stays
  the shape of the question it answers, and a reader picking between them has one rule:
  **county and per-capita, or company, or time and taxonomy.**

  ## Rolling it up

  Daily is the finest grain CFPB publishes, and every coarser one is a rollup of this table --
  join `dim_date` on `date_received` and aggregate. Nothing here may be presented at a finer
  cadence than a day, and there is none to present.

  **Recent days are incomplete by design.** A complaint publishes only after the company
  responds or fifteen days elapse, so the newest dates always trend down. That is a
  publication artifact, not a fall in complaints, and `rpt_fin_publication_window` is what
  says which months are safe -- filter on it before trending, at any grain.

  ## State grain, and what it excludes

  63,546 complaints carry no usable state. They are absent here, as they are from the company
  mart, and they are counted in the geo mart's `state_unassigned` allocation instead. A total
  taken from this table is a total over complaints whose state is known.

  ## What a complaint is

  A complaint to the CFPB, not a measure of consumer dissatisfaction. Complaints referred to
  other regulators -- including depositories under $10 billion -- are not in the archive at
  all, so a company's absence means the CFPB did not handle its complaints rather than that
  it had none.
*/

SELECT
    c.date_received,
    c.received_year_month                                   AS year_month,
    CAST(year(c.date_received) AS SMALLINT)                 AS received_year,
    CAST(quarter(c.date_received) AS TINYINT)               AS received_quarter,

    c.state_code,
    s.state_name,
    COALESCE(s.is_michigan, FALSE)                          AS is_michigan,
    s.census_division_name,

    -- The stable grouping, and the publisher's own taxonomy beneath it. `product_family`
    -- survives CFPB renaming its products mid-archive; `product` does not.
    c.product_family,
    c.product,
    c.sub_product,
    c.issue,
    c.sub_issue,

    COUNT(*)                                                AS complaint_count,
    COUNT(*) FILTER (WHERE NOT c.is_response_pending)       AS closed_complaints,
    COUNT(*) FILTER (WHERE c.is_response_pending)           AS pending_complaints,

    COUNT(*) FILTER (WHERE c.is_timely_response)            AS timely_count,
    COUNT(*) FILTER (WHERE c.response_grants_relief)        AS relief_count,
    COUNT(*) FILTER (WHERE c.response_family = 'monetary_relief')
                                                            AS monetary_relief_count,
    COUNT(*) FILTER (WHERE c.response_family = 'non_monetary_relief')
                                                            AS non_monetary_relief_count,
    COUNT(*) FILTER (WHERE c.response_family = 'untimely')   AS untimely_count,

    COUNT(*) FILTER (WHERE c.has_tag_older_american)        AS older_american_count,
    COUNT(*) FILTER (WHERE c.has_tag_servicemember)         AS servicemember_count,
    COUNT(*) FILTER (WHERE c.has_narrative)                 AS narrative_count,

    -- Rates over closed complaints only, and NULL rather than zero where nothing has closed.
    -- Recompute from the components before aggregating across days.
    CASE
        WHEN COUNT(*) FILTER (WHERE NOT c.is_response_pending) > 0
        THEN 100.0 * COUNT(*) FILTER (WHERE c.is_timely_response)
             / COUNT(*) FILTER (WHERE NOT c.is_response_pending)
    END                                                     AS timely_rate_pct,
    CASE
        WHEN COUNT(*) FILTER (WHERE NOT c.is_response_pending) > 0
        THEN 100.0 * COUNT(*) FILTER (WHERE c.response_grants_relief)
             / COUNT(*) FILTER (WHERE NOT c.is_response_pending)
    END                                                     AS relief_rate_pct,

    COUNT(*) < 10                                           AS is_small_cell

FROM {{ ref('stg_fin__cfpb_complaints') }} c
LEFT JOIN {{ ref('dim_state') }} s
    ON s.state_code = c.state_code
WHERE c.state_code IS NOT NULL
GROUP BY ALL
