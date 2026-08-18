{{ config(materialized = 'table') }}

/*
  Which CFPB months are safe to report on. Every FIN measure over time depends on this.

  A complaint enters the public database only after the company responds or fifteen days
  elapse, so the most recent months are incomplete by design and a chart that plots them
  always shows a decline that is not real. The fintech README states the trap; this is the
  model that makes it impossible to walk into, because a mart can join here rather than
  someone remembering a cutoff.

  ## Measured, not assumed

  The obvious rule is a fixed fifteen-day lag from the download date. The better one is in
  the data: the share of a month's complaints still marked In progress. It collapses sharply
  once a month closes, and the boundary is visible without knowing anything about CFPB's
  process -- 0.06% pending for May 2026, 17.7% for June, 54.9% for July. A threshold on that
  share moves on its own as the archive refreshes, where a hardcoded month would go stale
  the first time the data did.

  The current month is separately partial: complaints are still being received in it.
*/

{% set threshold = var('publication_pending_threshold_pct') %}

WITH by_month AS (

    SELECT
        received_year_month,
        MIN(date_received)                                      AS first_received,
        MAX(date_received)                                      AS last_received,
        COUNT(*)                                                AS complaint_count,
        COUNT(*) FILTER (WHERE is_response_pending)             AS pending_count,
        100.0 * COUNT(*) FILTER (WHERE is_response_pending) / COUNT(*)
                                                                AS pending_pct
    FROM {{ ref('stg_fin__cfpb_complaints') }}
    WHERE received_year_month IS NOT NULL
    GROUP BY 1

),

extent AS (

    SELECT MAX(date_received) AS max_received FROM {{ ref('stg_fin__cfpb_complaints') }}

)

SELECT
    m.received_year_month                                       AS year_month,
    m.first_received,
    m.last_received,
    m.complaint_count,
    m.pending_count,
    ROUND(m.pending_pct, 4)                                     AS pending_pct,

    -- The calendar month has finished. Necessary but not sufficient.
    strftime(e.max_received, '%Y-%m') > m.received_year_month   AS is_calendar_month_over,
    -- And enough of it has published to report on.
    m.pending_pct < {{ threshold }}                             AS is_publication_complete,
    {{ threshold }}                                             AS pending_threshold_pct,

    -- The one month FIN-E1 means by "the latest complete month".
    m.received_year_month = (
        SELECT MAX(received_year_month) FROM by_month
        WHERE pending_pct < {{ threshold }}
    )                                                           AS is_latest_complete_month

FROM by_month m
CROSS JOIN extent e
