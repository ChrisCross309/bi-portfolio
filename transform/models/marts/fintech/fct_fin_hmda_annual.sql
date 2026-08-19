{{ config(materialized = 'table') }}

/*
  **FIN-E4.** Michigan mortgage applications by year, county, lender and loan characteristics.

  Annual grain because HMDA is annual. There is no finer cadence in the source and CLAUDE.md
  section 8 forbids inventing one.

  ## Denial rate is descriptive, and this is where that has to be said

  HMDA carries **no credit score and no debt-to-income underwriting detail**. A denial-rate
  difference visible here is a gap warranting review and never evidence of discrimination, and
  nothing built on this model can support such a finding. The demographic columns are carried
  because the drill bank needs them and because refusing to publish them would not make
  disparities go away -- but every measure description says what they cannot show.

  ## The denominator excludes what is not a decision

  A denial rate is denials over decisions. Withdrawn applications, files closed for
  incompleteness and purchased loans are not decisions on an application this lender made, so
  they are counted separately and kept out of decided_applications. Including purchased loans
  in particular would be counting someone else's underwriting as this lender's.

  ## Denial reasons and the income bracket

  Both were in raw and in staging and in neither mart, which left the denial-reason mix and
  the income drill unanswerable from the semantic layer even though HMDA publishes them.

  `denial_reason_1_label` is the **primary** reason and is a dimension here. HMDA allows up
  to four; 126,500 denied applications carry a second, and `has_secondary_denial_reason`
  counts them rather than adding three more dimensions to a mart that would multiply by them.

  `applicant_income_bracket` is applicant income against **the area's own median family
  income**, not a dollar band -- $80,000 is a different applicant in Ann Arbor than in
  Alcona. The four brackets are the FFIEC's own CRA income levels, not thresholds invented
  here: low under 50%, moderate 50-80%, middle 80-120%, upper 120% and above. 389,541
  applications report no income and get no bracket.

  Adding both costs 2,815,382 rows to 3,249,311 -- 15%.

  ## The national benchmark is not in this table

  Raw is Michigan-only by decision, so the national side of FIN-E4 comes from the publisher's
  own aggregation endpoint, recorded per year in the HMDA manifest and surfaced in
  rpt_fin_entity_match_rate's sibling reconciliation. Nothing here is national, and nothing
  here should be presented as though it were.
*/

SELECT
    CAST(h.activity_year AS SMALLINT)                       AS activity_year,
    'year:' || CAST(h.activity_year AS VARCHAR)             AS period_id,
    h.county_fips,
    g.county_name,
    COALESCE(g.is_michigan, FALSE)                          AS is_michigan,
    h.has_no_reported_county,

    h.lei,
    i.institution_name,

    h.loan_purpose_label,
    h.loan_type_label,
    h.lien_status_label,
    h.occupancy_type_label,
    h.construction_method_label,

    h.applicant_age_band,
    h.applicant_race_label,
    h.applicant_ethnicity_label,
    h.applicant_sex_label,
    h.dti_band,

    -- Descriptive only, exactly as the demographic columns are. See the note above.
    h.denial_reason_1_label,
    CASE
        WHEN h.income IS NULL OR h.ffiec_msa_md_median_family_income IS NULL THEN NULL
        -- `income` is thousands of dollars as HMDA publishes it; the area median is dollars.
        WHEN 100.0 * h.income * 1000 / h.ffiec_msa_md_median_family_income < 50  THEN 'low'
        WHEN 100.0 * h.income * 1000 / h.ffiec_msa_md_median_family_income < 80  THEN 'moderate'
        WHEN 100.0 * h.income * 1000 / h.ffiec_msa_md_median_family_income < 120 THEN 'middle'
        ELSE 'upper'
    END                                                     AS applicant_income_bracket,

    COUNT(*)                                                AS application_count,

    -- Decisions this lender made on applications brought to it.
    COUNT(*) FILTER (WHERE h.action_taken IN ('1', '2', '3'))
                                                            AS decided_applications,
    COUNT(*) FILTER (WHERE h.action_taken = '1')            AS originated_count,
    COUNT(*) FILTER (WHERE h.action_taken = '2')            AS approved_not_accepted_count,
    COUNT(*) FILTER (WHERE h.action_taken = '3')            AS denied_count,

    -- Not decisions, and deliberately outside the denominator above.
    COUNT(*) FILTER (WHERE h.action_taken = '4')            AS withdrawn_count,
    COUNT(*) FILTER (WHERE h.action_taken = '5')            AS closed_incomplete_count,
    COUNT(*) FILTER (WHERE h.action_taken = '6')            AS purchased_count,
    COUNT(*) FILTER (WHERE h.action_taken IN ('7', '8'))    AS preapproval_count,

    SUM(h.loan_amount) FILTER (WHERE h.action_taken = '1')  AS originated_amount,
    SUM(h.loan_amount)                                      AS application_amount,
    MEDIAN(h.loan_amount) FILTER (WHERE h.action_taken = '1')
                                                            AS median_originated_amount,
    MEDIAN(h.income) FILTER (WHERE h.action_taken = '1')    AS median_applicant_income,

    COUNT(*) FILTER (WHERE h.is_partially_exempt)           AS partially_exempt_count,
    -- HMDA allows up to four reasons; only the primary one is a dimension here.
    COUNT(*) FILTER (WHERE h.denial_reason_2_label IS NOT NULL)
                                                            AS has_secondary_denial_reason,

    -- **Descriptive only.** See the model description: no credit score, no DTI underwriting
    -- detail, so this is a gap warranting review and never a finding.
    CASE
        WHEN COUNT(*) FILTER (WHERE h.action_taken IN ('1', '2', '3')) > 0
        THEN 100.0 * COUNT(*) FILTER (WHERE h.action_taken = '3')
             / COUNT(*) FILTER (WHERE h.action_taken IN ('1', '2', '3'))
    END                                                     AS denial_rate_pct,

    COUNT(*) < 10                                           AS is_small_cell

FROM {{ ref('stg_fin__hmda_lar') }} h
LEFT JOIN {{ ref('dim_geography_county') }} g
    ON g.county_fips = h.county_fips
LEFT JOIN {{ ref('stg_fin__hmda_institutions') }} i
    ON i.lei = h.lei
   AND i.activity_year = h.activity_year
GROUP BY ALL
