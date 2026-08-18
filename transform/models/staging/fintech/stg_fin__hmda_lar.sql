{{ config(materialized = 'view') }}

/*
  HMDA Loan/Application Register, typed. Michigan 2018-2025, 4.04M rows, 99 columns.

  This is the strongest argument in the repo for the all-varchar raw rule, and the model where
  paying it off takes the most care. Four traps, each of which produces a confident wrong
  answer rather than an error.

  ## The partial exemption is spelled two ways

  A small filer claiming the partial exemption writes the literal string Exempt into fourteen
  numeric-looking columns, roughly 68,000 rows each. A numeric cast nulls all of it silently,
  which is exactly why raw keeps it as text.

  Less obviously, the same filers appear in the categorical columns as the numeric code 1111 --
  denial_reason, submission_of_application, business_or_commercial_purpose, reverse_mortgage,
  the open-end line of credit flag and initially_payable_to_institution all carry it at the
  same scale. Handle only the string half and 1111 sits in a denial-reason distribution
  looking like a real reason. Both halves are preserved, and is_partially_exempt is the
  row-level fact underneath them.

  ## TRY_CAST, never a regex gate

  1,108 loan amounts are written in scientific notation -- 1.5065E7 -- and those are precisely
  the largest loans in the file: 10.0M to 950.5M dollars, against 5,000 to 9,995,000 for
  everything else. A guard that validated against a digits-only pattern before casting would
  drop the entire multifamily book and understate FIN-E4's origination volume. rate_spread
  does the same on 103 rows, and lender_credits writes twelve values as .45 rather than 0.45.
  Every value in this file casts. Nothing here needs validating first.

  ## Four columns are banded, not numeric

  debt_to_income_ratio publishes an exact integer between 36 and 49 and a band everywhere
  else: NA on 1,087,886 rows, then <20%, 20%-<30%, 30%-<36%, 50%-60%, >60%. total_units is
  exact for 1 to 4 and banded above it, on 8,030 rows. applicant_age and co-applicant_age are
  bands with two sentinel codes, 8888 and 9999. Each gets three columns -- the band as
  published, the exact value where the publisher gave one, and the sentinel kept apart from
  both -- because collapsing any pair loses information the other two carry.

  ## Codes are not typed values

  A bare 3 in action_taken is an untyped value that happens to parse. The code lists are
  seeded and joined here rather than in each mart, so FIN-E4 and FIN-E5 cannot decode the same
  column two different ways.

  ## Fair lending

  This file carries no credit score and no debt-to-income underwriting detail. Differences
  visible through the demographic columns below are **descriptive gaps warranting review,
  never conclusions**, and nothing here can support a finding of discrimination.
*/

{% set exempt_numeric = [
    'property_value', 'interest_rate', 'loan_term', 'total_loan_costs',
    'total_points_and_fees', 'origination_charges', 'discount_points', 'lender_credits',
    'rate_spread', 'loan_to_value_ratio', 'prepayment_penalty_term', 'intro_rate_period',
    'multifamily_affordable_units'
] %}

{% set plain_numeric = [
    'loan_amount', 'tract_population', 'tract_minority_population_percent',
    'ffiec_msa_md_median_family_income', 'tract_to_msa_income_percentage',
    'tract_owner_occupied_units', 'tract_one_to_four_family_homes',
    'tract_median_age_of_housing_units'
] %}

{# (output prefix, seed list name, source column) #}
{% set decoded = [
    ('action_taken', 'action_taken', 'action_taken'),
    ('loan_purpose', 'loan_purpose', 'loan_purpose'),
    ('loan_type', 'loan_type', 'loan_type'),
    ('lien_status', 'lien_status', 'lien_status'),
    ('occupancy_type', 'occupancy_type', 'occupancy_type'),
    ('construction_method', 'construction_method', 'construction_method'),
    ('preapproval', 'preapproval', 'preapproval'),
    ('hoepa_status', 'hoepa_status', 'hoepa_status'),
    ('business_or_commercial_purpose', 'business_or_commercial_purpose', 'business_or_commercial_purpose'),
    ('reverse_mortgage', 'reverse_mortgage', 'reverse_mortgage'),
    ('open_end_line_of_credit', 'open_end_line_of_credit', 'open_end_line_of_credit'),
    ('purchaser_type', 'purchaser_type', 'purchaser_type'),
    ('conforming_loan_limit', 'conforming_loan_limit', 'conforming_loan_limit'),
    ('submission_of_application', 'submission_of_application', 'submission_of_application'),
    ('initially_payable_to_institution', 'initially_payable_to_institution', 'initially_payable_to_institution'),
    ('applicant_sex', 'sex', 'applicant_sex'),
    ('co_applicant_sex', 'sex', 'co_applicant_sex'),
    ('applicant_ethnicity', 'ethnicity', 'applicant_ethnicity_1'),
    ('co_applicant_ethnicity', 'ethnicity', 'co_applicant_ethnicity_1'),
    ('applicant_race', 'race', 'applicant_race_1'),
    ('co_applicant_race', 'race', 'co_applicant_race_1'),
    ('denial_reason_1', 'denial_reason', 'denial_reason_1'),
    ('denial_reason_2', 'denial_reason', 'denial_reason_2'),
    ('denial_reason_3', 'denial_reason', 'denial_reason_3'),
    ('denial_reason_4', 'denial_reason', 'denial_reason_4')
] %}

WITH renamed AS (

    SELECT
        lei,
        activity_year,
        state_code,
        county_code,
        census_tract,

        -- banded, handled below
        debt_to_income_ratio,
        applicant_age,
        "co-applicant_age" AS co_applicant_age,
        total_units,

        -- carried as published; 38 of these need quoting because HMDA hyphenates them
        "derived_msa-md" AS derived_msa_md,
        conforming_loan_limit,
        derived_loan_product_type,
        derived_dwelling_category,
        derived_ethnicity,
        derived_race,
        derived_sex,
        action_taken,
        purchaser_type,
        preapproval,
        loan_type,
        loan_purpose,
        lien_status,
        reverse_mortgage,
        "open-end_line_of_credit" AS open_end_line_of_credit,
        business_or_commercial_purpose,
        hoepa_status,
        negative_amortization,
        interest_only_payment,
        balloon_payment,
        other_nonamortizing_features,
        construction_method,
        occupancy_type,
        manufactured_home_secured_property_type,
        manufactured_home_land_property_interest,
        applicant_credit_score_type,
        "co-applicant_credit_score_type" AS co_applicant_credit_score_type,
        "applicant_ethnicity-1" AS applicant_ethnicity_1,
        "applicant_ethnicity-2" AS applicant_ethnicity_2,
        "applicant_ethnicity-3" AS applicant_ethnicity_3,
        "applicant_ethnicity-4" AS applicant_ethnicity_4,
        "applicant_ethnicity-5" AS applicant_ethnicity_5,
        "co-applicant_ethnicity-1" AS co_applicant_ethnicity_1,
        "co-applicant_ethnicity-2" AS co_applicant_ethnicity_2,
        "co-applicant_ethnicity-3" AS co_applicant_ethnicity_3,
        "co-applicant_ethnicity-4" AS co_applicant_ethnicity_4,
        "co-applicant_ethnicity-5" AS co_applicant_ethnicity_5,
        applicant_ethnicity_observed,
        "co-applicant_ethnicity_observed" AS co_applicant_ethnicity_observed,
        "applicant_race-1" AS applicant_race_1,
        "applicant_race-2" AS applicant_race_2,
        "applicant_race-3" AS applicant_race_3,
        "applicant_race-4" AS applicant_race_4,
        "applicant_race-5" AS applicant_race_5,
        "co-applicant_race-1" AS co_applicant_race_1,
        "co-applicant_race-2" AS co_applicant_race_2,
        "co-applicant_race-3" AS co_applicant_race_3,
        "co-applicant_race-4" AS co_applicant_race_4,
        "co-applicant_race-5" AS co_applicant_race_5,
        applicant_race_observed,
        "co-applicant_race_observed" AS co_applicant_race_observed,
        applicant_sex,
        "co-applicant_sex" AS co_applicant_sex,
        applicant_sex_observed,
        "co-applicant_sex_observed" AS co_applicant_sex_observed,
        applicant_age_above_62,
        "co-applicant_age_above_62" AS co_applicant_age_above_62,
        submission_of_application,
        initially_payable_to_institution,
        "aus-1" AS aus_1,
        "aus-2" AS aus_2,
        "aus-3" AS aus_3,
        "aus-4" AS aus_4,
        "aus-5" AS aus_5,
        "denial_reason-1" AS denial_reason_1,
        "denial_reason-2" AS denial_reason_2,
        "denial_reason-3" AS denial_reason_3,
        "denial_reason-4" AS denial_reason_4,

        -- kept as text here so the sentinel survives to the next CTE
        {% for c in exempt_numeric %}
        {{ c }} AS {{ c }}_raw,
        {% endfor %}
        income AS income_raw,

        -- nothing but digits and, on 1,108 rows, an exponent
        {% for c in plain_numeric %}
        TRY_CAST({{ c }} AS DOUBLE) AS {{ c }}{{ "," if not loop.last }}
        {% endfor %}

    FROM {{ source('raw_fintech', 'hmda_lar') }}

),

typed AS (

    SELECT
        renamed.* EXCLUDE (
            {% for c in exempt_numeric %}{{ c }}_raw, {% endfor %}income_raw
        ),

        -- Each exemption column keeps a typed value and the sentinel that displaced it, so a
        -- NULL is never ambiguous between "exempt", "not applicable" and "left blank".
        {% for c in exempt_numeric %}
        TRY_CAST({{ c }}_raw AS DOUBLE) AS {{ c }},
        CASE WHEN {{ c }}_raw IN ('Exempt', 'NA') THEN {{ c }}_raw END AS {{ c }}_sentinel,
        {% endfor %}
        TRY_CAST(income_raw AS DOUBLE) AS income,
        CASE WHEN income_raw = 'NA' THEN income_raw END AS income_sentinel,

        -- The exemption's other spelling: 1111 in the categorical columns.
        COALESCE(
            {% for c in exempt_numeric %}{{ c }}_raw = 'Exempt' OR {% endfor %}
            denial_reason_1 = '1111'
            OR submission_of_application = '1111'
            OR initially_payable_to_institution = '1111'
            OR business_or_commercial_purpose = '1111'
            OR reverse_mortgage = '1111'
            OR open_end_line_of_credit = '1111',
            FALSE
        ) AS is_partially_exempt

    FROM renamed

)

SELECT
    t.* EXCLUDE (debt_to_income_ratio, applicant_age, co_applicant_age, total_units),

    -- geography -----------------------------------------------------------------
    -- 44,167 filings say NA. A real filing with no usable county, not a load failure.
    NULLIF(t.county_code, 'NA')                     AS county_fips,
    t.county_code = 'NA'                            AS has_no_reported_county,
    COALESCE(t.county_code LIKE '26%', FALSE)       AS is_michigan_county,
    NULLIF(t.census_tract, 'NA')                    AS census_tract_geoid,
    t.census_tract = 'NA'                           AS has_no_reported_tract,

    -- debt to income: exact between 36 and 49, banded outside it ------------------
    t.debt_to_income_ratio                          AS dti_band,
    TRY_CAST(t.debt_to_income_ratio AS DOUBLE)      AS dti_exact_pct,
    CASE
        WHEN t.debt_to_income_ratio IN ('NA', 'Exempt') THEN t.debt_to_income_ratio
    END                                             AS dti_sentinel,

    -- total units: exact 1-4, banded above ----------------------------------------
    t.total_units                                   AS total_units_band,
    TRY_CAST(t.total_units AS INTEGER)              AS total_units_exact,

    -- age: banded, with two sentinel codes that are not ages ----------------------
    CASE
        WHEN t.applicant_age NOT IN ('8888', '9999') THEN t.applicant_age
    END                                             AS applicant_age_band,
    CASE
        WHEN t.applicant_age IN ('8888', '9999') THEN t.applicant_age
    END                                             AS applicant_age_sentinel,
    CASE
        WHEN t.co_applicant_age NOT IN ('8888', '9999') THEN t.co_applicant_age
    END                                             AS co_applicant_age_band,
    CASE
        WHEN t.co_applicant_age IN ('8888', '9999') THEN t.co_applicant_age
    END                                             AS co_applicant_age_sentinel,

    -- decoded codes ----------------------------------------------------------------
    {% for out_col, list_name, src in decoded %}
    lbl_{{ loop.index }}.label AS {{ out_col }}_label{{ "," if not loop.last }}
    {% endfor %}

FROM typed t
{% for out_col, list_name, src in decoded %}
LEFT JOIN {{ ref('seed_hmda_code_lists') }} lbl_{{ loop.index }}
    ON lbl_{{ loop.index }}.list_name = '{{ list_name }}'
   AND lbl_{{ loop.index }}.code = t.{{ src }}
{% endfor %}
