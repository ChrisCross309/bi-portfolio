{{ config(materialized = 'table') }}

/*
  Matching a CFPB company name to an HMDA LEI. The join FIN-E5 needs and nobody publishes.

  FIN-E5 asks which large Michigan lenders over- or under-perform peers on complaints per $1B
  originated. That requires putting a complaint count beside an origination volume, and the
  two files identify the same firm two different ways: CFPB writes a free-text company name,
  HMDA writes a Legal Entity Identifier. **Nothing published joins them.**

  So this is entity resolution, and the only honest way to ship it is to report how well it
  worked rather than to present the matched subset as though it were everything.

  ## The rule

  Exact match on the normalised name -- casefolded, punctuation stripped, corporate suffixes
  removed. Both sides go through the same macro, because a normalisation applied even slightly
  differently to each side produces a match rate that looks plausible and is wrong.

  Deliberately no fuzzy matching. Token-set or edit-distance matching would raise the rate and
  would also start silently pairing Citizens Bank with Citizens Business Bank. A conservative
  rule with a published miss rate is defensible in a way that a generous rule with an unknown
  error rate is not.

  ## Read the volume-weighted rate, not the name-weighted one

  Michigan's complaints are concentrated: the top twenty companies carry roughly 85% of them.
  So the share of *names* matched and the share of *complaints* matched are very different
  numbers, and quoting only the first understates the join badly while quoting only the second
  hides a long tail of unmatched small firms. rpt_fin_entity_match_rate publishes both.

  ## An unmatched company is not a non-lender

  Most CFPB companies never appear in HMDA because they do not originate mortgages at all --
  credit bureaus, debt collectors, card issuers. Unmatched means "no LEI found", never "not a
  real firm", and FIN-E5's peer comparison is over matched mortgage lenders only.
*/

WITH complaint_companies AS (

    SELECT
        company_name,
        {{ normalize_company_name('company_name') }}    AS normalized_name,
        COUNT(*)                                        AS complaint_count,
        COUNT(*) FILTER (WHERE state_code = 'MI')       AS michigan_complaint_count
    FROM {{ ref('stg_fin__cfpb_complaints') }}
    WHERE company_name IS NOT NULL
    GROUP BY 1, 2

),

institutions AS (

    -- One row per LEI, taking its most recent published name. An institution renames itself
    -- between filing years and the newest name is the one CFPB is most likely to be using.
    SELECT
        lei,
        arg_max(institution_name, activity_year)            AS institution_name,
        arg_max(institution_name_normalized, activity_year) AS normalized_name,
        MAX(activity_year)                                  AS latest_activity_year
    FROM {{ ref('stg_fin__hmda_institutions') }}
    WHERE institution_name_normalized IS NOT NULL
    GROUP BY lei

),

-- A normalised name can collapse two distinct LEIs together. Where it does, the match is
-- ambiguous and is refused rather than resolved arbitrarily.
name_cardinality AS (

    SELECT
        normalized_name,
        COUNT(*)            AS lei_count,
        MIN(lei)            AS single_lei,
        MIN(institution_name) AS single_institution_name
    FROM institutions
    GROUP BY normalized_name

)

SELECT
    c.company_name,
    c.normalized_name,
    c.complaint_count,
    c.michigan_complaint_count,

    CASE WHEN n.lei_count = 1 THEN n.single_lei END             AS lei,
    CASE WHEN n.lei_count = 1 THEN n.single_institution_name END AS institution_name,

    CASE
        WHEN c.normalized_name IS NULL   THEN 'unnormalizable'
        WHEN n.normalized_name IS NULL   THEN 'no_lei_found'
        WHEN n.lei_count > 1             THEN 'ambiguous'
        ELSE 'matched'
    END                                                         AS match_status,
    COALESCE(n.lei_count, 0)                                    AS candidate_lei_count,
    'exact_normalized_name'                                     AS match_method

FROM complaint_companies c
LEFT JOIN name_cardinality n
    ON n.normalized_name = c.normalized_name
