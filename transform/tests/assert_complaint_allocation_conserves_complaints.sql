{{ config(severity = coverage_test_severity()) }}

/*
  The ZIP-to-county allocation must not lose or invent a complaint.

  This is the property that makes a fractional, weighted allocation defensible instead of just
  approximate: whatever rule places a complaint, the allocated counts re-sum exactly to the
  state totals they came from. Fractions are fine; a missing complaint is not.

  It is also a regression guard on a real miss. The first version dropped 23,167 complaints,
  because HUD publishes the crosswalk for the current quarter while CFPB's archive reaches
  back to 2011, and 4,205 ZIPs in the complaint file no longer exist in it. Every state total
  still looked entirely plausible. Nothing errored.

  A tenth of a complaint of tolerance, for floating-point summation over 17 million rows.

  Warns rather than fails against the fixture warehouse. The committed crosswalk is a
  stratified sample, so a ZIP that touches four counties may keep only one of them and its
  weights then sum to less than one -- the allocation is arithmetically right and the sample
  is simply not a census. That is the same rule `reconcile.michigan` follows when it skips
  the county roster offline, and it is why this is a coverage test rather than a plain one.
*/

WITH allocated AS (
    SELECT state_code, SUM(allocated_complaints) AS allocated
    FROM {{ ref('int_fin__complaint_county_allocation') }}
    GROUP BY state_code
),

source AS (
    SELECT state_code, COUNT(*) AS complaints
    FROM {{ ref('stg_fin__cfpb_complaints') }}
    WHERE state_code IS NOT NULL
    GROUP BY state_code
)

SELECT
    s.state_code,
    s.complaints,
    a.allocated,
    a.allocated - s.complaints AS gap
FROM source s
LEFT JOIN allocated a USING (state_code)
WHERE a.allocated IS NULL
   OR ABS(a.allocated - s.complaints) > 0.1
