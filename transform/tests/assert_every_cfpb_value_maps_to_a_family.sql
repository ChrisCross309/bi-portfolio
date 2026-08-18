/*
  Every product and every company response must map to a family.

  This is the test that would have caught the mistake that actually happened while building
  this PR: a product label was transcribed into the seed from a truncated console line, and
  2,163,770 complaints -- the whole second era of credit reporting -- silently mapped to
  nothing. Nothing errored. The only visible symptom was a family count that looked slightly
  low, in a file where nobody knows the right answer by heart.

  It doubles as the taxonomy tripwire: CFPB has renamed products three times already, and the
  next rename fails this rather than quietly opening a gap in FIN-E2's series.
*/

SELECT 'product' AS field, product AS value, COUNT(*) AS rows
FROM {{ ref('stg_fin__cfpb_complaints') }}
WHERE product IS NOT NULL AND product_family IS NULL
GROUP BY 1, 2

UNION ALL

SELECT 'company_response', company_response, COUNT(*)
FROM {{ ref('stg_fin__cfpb_complaints') }}
WHERE company_response IS NOT NULL AND response_family IS NULL
GROUP BY 1, 2
