{{ config(materialized = 'table') }}

/*
  Complaints allocated from ZIP to county. The whole of FIN-E1's geography.

  CFPB publishes a complaint's state and ZIP and no county at all. A ZIP is a postal delivery
  route rather than an area, and 34.6% of Michigan's cross a county line, so this is a
  weighted allocation under a stated rule and never a lookup. **Counts here are fractional and
  are never rounded up.**

  ## The rule

    full ZIP          allocated across the counties it touches by the crosswalk's own weight:
                      the residential address share, or all addresses where a ZIP has no
                      residential ones. Staging resolves that fallback, so it is one column
                      here rather than a branch repeated per mart.

    masked to three   allocated across every county any ZIP sharing the prefix touches,
    digits            weighted by the summed address counts of those ZIPs and renormalised.
                      Coarser, and honest about being coarser.

    no usable ZIP     assigned to the state's unassigned-county bucket rather than dropped.
                      Michigan's is 26000, which dim_geography_county already carries for
                      CMS's sake.

    ZIP not in the    the same bucket, flagged separately. HUD publishes the crosswalk for
    crosswalk         the current quarter while CFPB's archive reaches back to 2011, so
                      4,205 ZIPs in the complaint file no longer exist in it -- 23,167
                      complaints, 420 of them Michigan's. Mostly PO-box and corporate ZIPs
                      that USPS has since retired. They are real complaints in real places
                      and the allocation refuses to lose them; it just cannot say which
                      county any more.

  Every complaint is allocated somewhere, so allocated counts re-sum exactly to the state
  totals. A test asserts that, because an allocation that quietly loses rows is the failure
  this whole design exists to prevent.

  ## Why it aggregates first

  Allocating seventeen million complaints one at a time would produce roughly twenty-four
  million rows to aggregate afterwards. Aggregating to month, state, ZIP and product family
  first and then multiplying those counts by the weights is arithmetically identical and two
  orders of magnitude smaller.
*/

WITH complaints AS (

    SELECT
        received_year_month,
        state_code,
        zip5,
        zip3,
        zip_mask_kind,
        product_family,
        COUNT(*)                                        AS complaint_count,
        COUNT(*) FILTER (WHERE is_timely_response)      AS timely_count,
        COUNT(*) FILTER (WHERE response_grants_relief)  AS relief_count,
        COUNT(*) FILTER (WHERE is_response_pending)     AS pending_count,
        COUNT(*) FILTER (WHERE has_tag_older_american)  AS older_american_count
    FROM {{ ref('stg_fin__cfpb_complaints') }}
    WHERE state_code IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6

),

full_zip AS (

    SELECT
        c.received_year_month,
        c.state_code,
        c.product_family,
        x.county_fips,
        'full_zip'                              AS allocation_method,
        x.allocation_weight,
        c.complaint_count,
        c.timely_count,
        c.relief_count,
        c.pending_count,
        c.older_american_count
    FROM complaints c
    JOIN {{ ref('stg_ref__zip_county_crosswalk') }} x
        ON x.zip5 = c.zip5
    WHERE c.zip_mask_kind = 'full'

),

prefix_zip AS (

    SELECT
        c.received_year_month,
        c.state_code,
        c.product_family,
        w.county_fips,
        'zip3_prefix'                           AS allocation_method,
        w.allocation_weight,
        c.complaint_count,
        c.timely_count,
        c.relief_count,
        c.pending_count,
        c.older_american_count
    FROM complaints c
    JOIN {{ ref('int_fin__zip3_county_weights') }} w
        ON w.zip3 = c.zip3
    WHERE c.zip_mask_kind = 'prefix_masked'

),

unplaceable AS (

    -- A fully masked, absent or malformed ZIP still happened somewhere in its state.
    SELECT
        c.received_year_month,
        c.state_code,
        c.product_family,
        s.state_fips || '000'                   AS county_fips,
        'state_unassigned'                      AS allocation_method,
        CAST(1.0 AS DOUBLE)                     AS allocation_weight,
        c.complaint_count,
        c.timely_count,
        c.relief_count,
        c.pending_count,
        c.older_american_count
    FROM complaints c
    JOIN {{ ref('dim_state') }} s
        ON s.state_code = c.state_code
    WHERE c.zip_mask_kind IN ('fully_masked', 'null', 'malformed')
      AND s.state_fips IS NOT NULL

),

retired_zip AS (

    -- A full ZIP that the current crosswalk vintage no longer contains. Without this branch
    -- the allocation silently drops 23,167 complaints and still looks completely plausible.
    SELECT
        c.received_year_month,
        c.state_code,
        c.product_family,
        s.state_fips || '000'                   AS county_fips,
        'zip_not_in_crosswalk'                  AS allocation_method,
        CAST(1.0 AS DOUBLE)                     AS allocation_weight,
        c.complaint_count,
        c.timely_count,
        c.relief_count,
        c.pending_count,
        c.older_american_count
    FROM complaints c
    JOIN {{ ref('dim_state') }} s
        ON s.state_code = c.state_code
    LEFT JOIN (
        SELECT DISTINCT zip5 FROM {{ ref('stg_ref__zip_county_crosswalk') }}
    ) known
        ON known.zip5 = c.zip5
    WHERE c.zip_mask_kind = 'full'
      AND known.zip5 IS NULL
      AND s.state_fips IS NOT NULL

),

combined AS (

    SELECT * FROM full_zip
    UNION ALL
    SELECT * FROM prefix_zip
    UNION ALL
    SELECT * FROM unplaceable
    UNION ALL
    SELECT * FROM retired_zip

)

SELECT
    received_year_month,
    state_code,
    county_fips,
    product_family,
    allocation_method,

    SUM(complaint_count * allocation_weight)        AS allocated_complaints,
    SUM(timely_count * allocation_weight)           AS allocated_timely,
    SUM(relief_count * allocation_weight)           AS allocated_relief,
    SUM(pending_count * allocation_weight)          AS allocated_pending,
    SUM(older_american_count * allocation_weight)   AS allocated_older_american,
    SUM(complaint_count)                            AS source_complaints_before_allocation

FROM combined
GROUP BY 1, 2, 3, 4, 5
