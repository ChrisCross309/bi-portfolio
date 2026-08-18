/*
  The marts must not lose or invent a claim, a dollar or a policy.

  A gap between the publisher and the bulk file is FEMA's timing. A gap between the warehouse
  and a mart is ours, and this is what refuses to let one appear quietly: an aggregation that
  drops rows on a bad join still produces a plausible table, and nobody knows the right total
  by heart.
*/

WITH checks AS (

    SELECT
        'claim_count' AS measure,
        (SELECT SUM(claim_count) FROM {{ ref('fct_ins_claims_monthly') }})  AS mart_value,
        (SELECT COUNT(*) FROM {{ ref('stg_ins__nfip_claims') }})            AS staging_value

    UNION ALL

    SELECT
        'paid_total',
        (SELECT ROUND(SUM(paid_total), 2) FROM {{ ref('fct_ins_claims_monthly') }}),
        (SELECT ROUND(SUM(total_amount_paid), 2) FROM {{ ref('stg_ins__nfip_claims') }})

    UNION ALL

    SELECT
        'michigan_claim_detail',
        (SELECT COUNT(*) FROM {{ ref('fct_ins_claims_detail_mi') }}),
        (SELECT COUNT(*) FROM {{ ref('stg_ins__nfip_claims') }} WHERE state_code = 'MI')

    UNION ALL

    SELECT
        'reconciliation_warehouse_to_mart',
        (SELECT COALESCE(SUM(ABS(warehouse_to_mart_gap)), 0)
         FROM {{ ref('rpt_ins_reconciliation') }}),
        0

)

SELECT measure, mart_value, staging_value, mart_value - staging_value AS gap
FROM checks
WHERE mart_value IS DISTINCT FROM staging_value
