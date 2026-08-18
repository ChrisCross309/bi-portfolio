{{ config(materialized = 'view') }}

/*
  CFPB consumer complaints, typed and named, national. 17.0M rows, one per complaint.

  Raw is all-varchar because the archive is a CSV and CLAUDE.md rule 2 forbids type inference
  there. This is where that gets paid off, and where four publisher behaviours that would each
  produce a confident wrong answer get made explicit.

  ## The product taxonomy has three eras

  Credit reporting was published as "Credit reporting" until 2017, then "Credit reporting,
  credit repair services, or other personal consumer report" until 2023, and is now "Credit
  reporting or other personal consumer reports" -- 11.5M complaints under the current label
  alone. Group by the publisher's own string and FIN-E2 reports the current label growing from
  nothing in 2023 while its predecessor vanishes: a manufactured 100% growth story on the
  single largest product. Payday lending and card complaints churn the same way. product_family
  from the seed is what makes the series continuous, and the seed records each label's own era
  so the joins are checkable.

  ## The response taxonomy has eras too, and one value is not an outcome

  Modern "Closed with explanation" / "with monetary relief" / "with non-monetary relief" sit
  beside legacy "Closed", "Closed with relief" and "Closed without relief" -- 40,782 rows whose
  monetary split was never recorded. And "In progress" (592,496) is an open complaint, not a
  result. FIN-E3 measures relief rates, so a denominator that includes open complaints
  understates every rate by a share that moves with how recent the window is.
  is_response_pending exists so that choice is made deliberately.

  ## The ZIP is masked more often than the notes suggest

  Michigan splits 337,178 full 5-digit / 27,820 masked to three digits / 1,728 fully masked /
  8 NULL / 1 malformed. Roughly 8% cannot be placed precisely, and PR 8's county allocation
  needs a branch per case rather than a filter -- so zip_mask_kind names all five.

  ## Tags is multi-value

  "Older American, Servicemember" is a real value alongside each alone. The drill bank asks
  whether the older-American share is rising; an equality filter on that tag undercounts by
  the 57,840 complaints carrying both.

  One publisher anomaly is flagged rather than repaired: 7,050 complaints were sent to the
  company before they were received.
*/

WITH renamed AS (

    SELECT
        "Complaint ID"                      AS complaint_id,
        TRY_CAST("Date received" AS DATE)   AS date_received,
        TRY_CAST("Date sent to company" AS DATE) AS date_sent_to_company,
        "Product"                           AS product,
        "Sub-product"                       AS sub_product,
        "Issue"                             AS issue,
        "Sub-issue"                         AS sub_issue,
        "Consumer complaint narrative"      AS consumer_narrative,
        "Company public response"           AS company_public_response,
        "Company"                           AS company_name,
        "State"                             AS state_code,
        "ZIP code"                          AS zip_code_raw,
        "Tags"                              AS tags,
        "Submitted via"                     AS submitted_via,
        "Company response to consumer"      AS company_response,
        "Timely response?"                  AS timely_response_raw,
        "year"                              AS partition_year
    FROM {{ source('raw_fintech', 'cfpb_complaints') }}

)

SELECT
    r.complaint_id,

    -- timing ------------------------------------------------------------------
    r.date_received,
    r.date_sent_to_company,
    strftime(r.date_received, '%Y-%m')              AS received_year_month,
    CAST(year(r.date_received) AS SMALLINT)         AS received_year,
    -- 7,050 complaints. A publisher anomaly, flagged rather than repaired.
    COALESCE(r.date_sent_to_company < r.date_received, FALSE)
                                                    AS was_sent_before_received,

    -- product ------------------------------------------------------------------
    r.product,
    r.sub_product,
    r.issue,
    r.sub_issue,
    -- NULL here means the publisher used a label the seed does not carry, which is a
    -- taxonomy change we have not caught up with. Tested, not assumed.
    pf.product_family,

    -- outcome ------------------------------------------------------------------
    r.company_response,
    rf.response_family,
    rf.grants_relief                                AS response_grants_relief,
    -- "In progress" is not an outcome. A relief rate that counts it in the denominator
    -- understates itself, and by more in a recent window than an old one.
    COALESCE(rf.is_closed = FALSE, FALSE)           AS is_response_pending,
    r.timely_response_raw = 'Yes'                   AS is_timely_response,
    r.company_public_response,

    -- who ----------------------------------------------------------------------
    r.company_name,
    r.tags,
    -- Contains-matching, because the two tags combine into one comma-joined value.
    COALESCE(r.tags LIKE '%Older American%', FALSE) AS has_tag_older_american,
    COALESCE(r.tags LIKE '%Servicemember%', FALSE)  AS has_tag_servicemember,
    r.submitted_via,
    r.consumer_narrative,
    -- Narratives exist only where the consumer consented to publication: 22.5% of the file.
    -- Any text analysis describes that subset and not all complainants.
    r.consumer_narrative IS NOT NULL                AS has_narrative,

    -- geography ----------------------------------------------------------------
    -- 1,386 complaints carry the full name "UNITED STATES MINOR OUTLYING ISLANDS" where a
    -- two-letter code belongs. That is a malformed value rather than a jurisdiction, so it
    -- is nulled out of the join key and flagged instead of being given a dimension row.
    CASE WHEN length(r.state_code) = 2 THEN r.state_code END AS state_code,
    r.state_code                                    AS state_code_raw,
    COALESCE(length(r.state_code) <> 2, FALSE)      AS has_malformed_state,
    r.zip_code_raw,
    CASE
        WHEN r.zip_code_raw IS NULL                             THEN 'null'
        WHEN regexp_matches(r.zip_code_raw, '^[0-9]{5}$')       THEN 'full'
        WHEN regexp_matches(r.zip_code_raw, '^[0-9]{3}XX$')     THEN 'prefix_masked'
        WHEN regexp_matches(r.zip_code_raw, '^X+$')             THEN 'fully_masked'
        ELSE 'malformed'
    END                                             AS zip_mask_kind,
    CASE
        WHEN regexp_matches(r.zip_code_raw, '^[0-9]{5}$') THEN r.zip_code_raw
    END                                             AS zip5,
    -- Populated for a full ZIP as well as a masked one, so PR 8's fallback allocation can
    -- use one column rather than re-deriving the prefix.
    CASE
        WHEN regexp_matches(r.zip_code_raw, '^[0-9]{3}') THEN SUBSTR(r.zip_code_raw, 1, 3)
    END                                             AS zip3,

    r.partition_year

FROM renamed r
LEFT JOIN {{ ref('seed_cfpb_product_family') }} pf
    ON pf.product = r.product
LEFT JOIN {{ ref('seed_cfpb_response_family') }} rf
    ON rf.company_response = r.company_response
