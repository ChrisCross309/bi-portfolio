{{ config(materialized = 'table') }}

/*
  **FIN-E3.** Complaints by month, state, company and product family, with response outcomes.

  State grain rather than county, deliberately. FIN-E3 asks whether companies serving Michigan
  respond timely and grant relief at better or worse rates than a year ago and than nationally,
  and none of that is a county question. Pushing the ZIP allocation through this grain as well
  would multiply the table by every county a company's complainants touch, to answer nothing.

  ## The relief rate needs a denominator that excludes open complaints

  592,496 complaints are still In progress -- not an outcome, just unfinished. A relief rate
  computed over every complaint understates itself, and by more in a recent window than an old
  one, which makes it look like performance is falling when it is only unsettled.
  closed_complaints is the denominator that FIN-E3 means.

  ## The legacy response vocabulary cannot be split

  40,782 complaints carry the pre-2017 categories. Closed with relief and Closed without
  relief record whether relief happened but not whether it was monetary, and plain Closed
  records neither. relief_unspecified_count keeps those visible rather than folding them into
  one side of a ratio and quietly changing what the ratio means before 2017.
*/

SELECT
    c.received_year_month                                   AS year_month,
    'month:' || c.received_year_month                       AS period_id,
    CAST(SUBSTR(c.received_year_month, 1, 4) AS SMALLINT)   AS received_year,
    c.state_code,
    COALESCE(s.is_michigan, FALSE)                          AS is_michigan,
    c.company_name,
    c.product_family,

    COUNT(*)                                                AS complaint_count,
    COUNT(*) FILTER (WHERE NOT c.is_response_pending)       AS closed_complaints,
    COUNT(*) FILTER (WHERE c.is_response_pending)           AS pending_complaints,

    COUNT(*) FILTER (WHERE c.is_timely_response)            AS timely_count,
    COUNT(*) FILTER (WHERE c.response_grants_relief)        AS relief_count,
    COUNT(*) FILTER (WHERE c.response_family = 'monetary_relief')
                                                            AS monetary_relief_count,
    COUNT(*) FILTER (WHERE c.response_family = 'non_monetary_relief')
                                                            AS non_monetary_relief_count,
    COUNT(*) FILTER (WHERE c.response_family IN ('relief_unspecified', 'closed_unspecified'))
                                                            AS relief_unspecified_count,
    COUNT(*) FILTER (WHERE c.response_family = 'untimely')   AS untimely_count,

    COUNT(*) FILTER (WHERE c.has_tag_older_american)        AS older_american_count,
    COUNT(*) FILTER (WHERE c.has_tag_servicemember)         AS servicemember_count,
    COUNT(*) FILTER (WHERE c.has_narrative)                 AS narrative_count,

    -- Rates over closed complaints only, and NULL rather than zero where nothing has closed.
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
