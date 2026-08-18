/*
  Every code present in a decoded HMDA column must have a row in the seed.

  A missing code produces a NULL label rather than an error, so a denial-reason chart would
  simply have a blank slice and look finished. This is also the guard against the publisher
  adding a code: HMDA has revised its enumerations before, and the day it does again this
  fails instead of quietly dropping the new category.
*/

WITH decoded AS (
    SELECT 'action_taken'      AS list_name, action_taken      AS code FROM {{ ref('stg_fin__hmda_lar') }}
    UNION SELECT 'loan_purpose', loan_purpose                  FROM {{ ref('stg_fin__hmda_lar') }}
    UNION SELECT 'loan_type', loan_type                        FROM {{ ref('stg_fin__hmda_lar') }}
    UNION SELECT 'lien_status', lien_status                    FROM {{ ref('stg_fin__hmda_lar') }}
    UNION SELECT 'occupancy_type', occupancy_type              FROM {{ ref('stg_fin__hmda_lar') }}
    UNION SELECT 'construction_method', construction_method    FROM {{ ref('stg_fin__hmda_lar') }}
    UNION SELECT 'preapproval', preapproval                    FROM {{ ref('stg_fin__hmda_lar') }}
    UNION SELECT 'hoepa_status', hoepa_status                  FROM {{ ref('stg_fin__hmda_lar') }}
    UNION SELECT 'purchaser_type', purchaser_type              FROM {{ ref('stg_fin__hmda_lar') }}
    UNION SELECT 'conforming_loan_limit', conforming_loan_limit FROM {{ ref('stg_fin__hmda_lar') }}
    UNION SELECT 'sex', applicant_sex                          FROM {{ ref('stg_fin__hmda_lar') }}
    UNION SELECT 'sex', co_applicant_sex                       FROM {{ ref('stg_fin__hmda_lar') }}
    UNION SELECT 'ethnicity', applicant_ethnicity_1            FROM {{ ref('stg_fin__hmda_lar') }}
    UNION SELECT 'ethnicity', co_applicant_ethnicity_1         FROM {{ ref('stg_fin__hmda_lar') }}
    UNION SELECT 'race', applicant_race_1                      FROM {{ ref('stg_fin__hmda_lar') }}
    UNION SELECT 'race', co_applicant_race_1                   FROM {{ ref('stg_fin__hmda_lar') }}
    UNION SELECT 'denial_reason', denial_reason_1              FROM {{ ref('stg_fin__hmda_lar') }}
)

SELECT d.list_name, d.code
FROM decoded d
LEFT JOIN {{ ref('seed_hmda_code_lists') }} s
    ON s.list_name = d.list_name AND s.code = d.code
WHERE d.code IS NOT NULL
  AND s.code IS NULL
