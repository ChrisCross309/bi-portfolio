{{ config(materialized = 'table') }}

/*
  **FIN-E5's honesty column.** How well the company-to-LEI match worked, stated four ways.

  A matched-only table would let FIN-E5 present a peer comparison over whatever happened to
  join, with no way for a reader to know what fell out. This publishes the miss rate beside
  the hit rate so the comparison can be read with its own coverage attached, and so a future
  change to the matching rule shows up as a number moving rather than as a silently different
  answer.

  Four measures, because they disagree and each one is misleading alone:

    name coverage         share of distinct CFPB company names that found an LEI. The lowest
                          number, because the long tail of small firms is mostly not mortgage
                          lenders at all.
    complaint coverage    share of complaints, nationally, whose company matched. Higher,
                          because complaints concentrate in large firms.
    Michigan coverage     the same weighted to Michigan, which is what FIN-E5 actually reports
                          on.
    ambiguous             names where normalisation collapsed two or more LEIs together. These
                          are refused rather than resolved arbitrarily, and they belong in the
                          miss column, not hidden.

  An unmatched company is not a non-lender. Most CFPB companies never appear in HMDA because
  they do not originate mortgages -- credit bureaus, debt collectors, card issuers -- so this
  is a measure of join coverage and never of data quality.
*/

WITH matches AS (

    SELECT * FROM {{ ref('int_fin__company_lei_match') }}

),

totals AS (

    SELECT
        COUNT(*)                                                        AS company_names,
        SUM(complaint_count)                                            AS complaints,
        SUM(michigan_complaint_count)                                   AS michigan_complaints,
        COUNT(*) FILTER (WHERE match_status = 'matched')                AS matched_names,
        SUM(complaint_count) FILTER (WHERE match_status = 'matched')    AS matched_complaints,
        SUM(michigan_complaint_count) FILTER (WHERE match_status = 'matched')
                                                                        AS matched_michigan,
        COUNT(*) FILTER (WHERE match_status = 'ambiguous')              AS ambiguous_names,
        COUNT(*) FILTER (WHERE match_status = 'no_lei_found')           AS unmatched_names,
        COUNT(*) FILTER (WHERE match_status = 'unnormalizable')         AS unnormalizable_names
    FROM matches

)

SELECT 'company_names' AS coverage_basis,
       matched_names AS matched, company_names AS total,
       ROUND(100.0 * matched_names / NULLIF(company_names, 0), 2) AS coverage_pct,
       'Distinct CFPB company names that found exactly one LEI. The lowest of the four, '
       || 'because most CFPB companies are not mortgage lenders at all.' AS interpretation
FROM totals

UNION ALL

SELECT 'complaints_national',
       matched_complaints, complaints,
       ROUND(100.0 * matched_complaints / NULLIF(complaints, 0), 2),
       'Complaints whose company matched, nationally. Higher than name coverage because '
       || 'complaints concentrate in large firms.'
FROM totals

UNION ALL

SELECT 'complaints_michigan',
       matched_michigan, michigan_complaints,
       ROUND(100.0 * matched_michigan / NULLIF(michigan_complaints, 0), 2),
       'The measure FIN-E5 is actually reported on. Michigan complaints are concentrated: '
       || 'the top twenty companies carry roughly 85% of them.'
FROM totals

UNION ALL

SELECT 'ambiguous_names',
       ambiguous_names, company_names,
       ROUND(100.0 * ambiguous_names / NULLIF(company_names, 0), 2),
       'Names where normalisation collapsed two or more LEIs together. Refused rather than '
       || 'resolved arbitrarily, and counted as a miss rather than hidden.'
FROM totals
