{{ config(materialized = 'table') }}

/*
  County weights for a ZIP masked to its first three digits.

  CFPB masks roughly 8% of Michigan ZIPs to three digits, so those complaints have a real
  location that is coarser than the crosswalk's grain. Rather than dropping them or assigning
  them arbitrarily, they are allocated across every county that any ZIP sharing the prefix
  touches, weighted by the same address counts the full-ZIP rule uses.

  The weight is built by summing the underlying allocation weights across the prefix and
  renormalising, so a three-digit allocation is the population-weighted average of the
  five-digit ones rather than an unweighted spread. A prefix covering one dense county and
  four sparse ones lands mostly in the dense one, which is what the addresses actually say.
*/

WITH prefix_totals AS (

    SELECT
        zip3,
        county_fips,
        SUM(allocation_weight) AS weight_sum
    FROM {{ ref('stg_ref__zip_county_crosswalk') }}
    GROUP BY 1, 2

)

SELECT
    zip3,
    county_fips,
    weight_sum,
    weight_sum / SUM(weight_sum) OVER (PARTITION BY zip3) AS allocation_weight,
    COUNT(*) OVER (PARTITION BY zip3)                    AS counties_in_prefix
FROM prefix_totals
WHERE weight_sum > 0
