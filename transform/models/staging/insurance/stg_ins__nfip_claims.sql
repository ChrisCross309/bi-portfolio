{{ config(materialized = 'view') }}

/*
  NFIP claims, typed and named, national.

  These arrive as parquet with FEMA's own schema embedded, so unlike the CSV sources there is
  no all-varchar rule to pay off here -- the dates are DATEs and the money is DECIMAL already.
  What this model does instead is rename 84 columns from FEMA's camelCase into snake_case
  mechanically, so every name still maps back to the publisher's, and then derive the handful
  of columns that encode a decision no mart should have to make for itself.

  Four things it surfaces rather than smooths over.

  ## County comes from two columns, not one

  FEMA populates countyCode and censusGeoid independently and neither is complete. Coalescing
  them drops the national share with no usable county from 5.03% to 1.63%, and Michigan's
  from 2.49% to 0.98% -- threefold better than censusGeoid alone, which is what the Michigan
  gate in L1 measures. county_fips_source records which column supplied each value, so the
  improvement is auditable rather than asserted.

  ## state = 'UN' is 16,441 real claims

  Mostly pre-1990, whose state FEMA could not determine. They stay, flagged. A state join
  that quietly drops them understates the national total by exactly that much, and INS-E5
  ties our totals to FEMA's published national figures.

  ## A NULL payment is not a missing payment

  567,044 claims carry NULL in all three amount_paid columns, and 556,554 of those name a
  non-payment reason: the claim closed without paying. Only 10,490 are genuinely
  unexplained. is_closed_without_payment keeps that distinct from a zero payment, which is a
  different fact -- 150,629 claims paid exactly nothing on the building.

  ## Gross and net follow different NULL conventions

  amount_paid_on_building_claim is NULL for those 567,044. net_building_payment_amount is
  never NULL and reads 0 for them, and the two differ on a further 9,882 claims where a
  recovery or subrogation was applied. Neither is wrong. A SUM over the gross column silently
  skips the closed-unpaid claims; a SUM over the net column counts them as zero. So a mart
  has to say which question it is asking, and both are carried here rather than one being
  chosen on its behalf.
*/

WITH renamed AS (

    SELECT
        agricultureStructureIndicator AS agriculture_structure_indicator,
        asOfDate AS as_of_date,
        basementEnclosureCrawlspaceType AS basement_enclosure_crawlspace_type,
        policyCount AS policy_count,
        crsClassCode AS crs_class_code,
        dateOfLoss AS date_of_loss,
        elevatedBuildingIndicator AS elevated_building_indicator,
        elevationCertificateIndicator AS elevation_certificate_indicator,
        elevationDifference AS elevation_difference,
        baseFloodElevation AS base_flood_elevation,
        ratedFloodZone AS rated_flood_zone,
        houseWorship AS house_worship,
        locationOfContents AS location_of_contents,
        lowestAdjacentGrade AS lowest_adjacent_grade,
        lowestFloorElevation AS lowest_floor_elevation,
        numberOfFloorsInTheInsuredBuilding AS number_of_floors_in_the_insured_building,
        nonProfitIndicator AS non_profit_indicator,
        obstructionType AS obstruction_type,
        occupancyType AS occupancy_type,
        originalConstructionDate AS original_construction_date,
        originalNBDate AS original_nb_date,
        amountPaidOnBuildingClaim AS amount_paid_on_building_claim,
        amountPaidOnContentsClaim AS amount_paid_on_contents_claim,
        amountPaidOnIncreasedCostOfComplianceClaim AS amount_paid_on_icc_claim,
        postFIRMConstructionIndicator AS post_firm_construction_indicator,
        rateMethod AS rate_method,
        smallBusinessIndicatorBuilding AS small_business_indicator_building,
        totalBuildingInsuranceCoverage AS total_building_insurance_coverage,
        totalContentsInsuranceCoverage AS total_contents_insurance_coverage,
        yearOfLoss AS year_of_loss,
        primaryResidenceIndicator AS primary_residence_indicator,
        buildingDamageAmount AS building_damage_amount,
        buildingDeductibleCode AS building_deductible_code,
        netBuildingPaymentAmount AS net_building_payment_amount,
        buildingPropertyValue AS building_property_value,
        causeOfDamage AS cause_of_damage,
        condominiumCoverageTypeCode AS condominium_coverage_type_code,
        contentsDamageAmount AS contents_damage_amount,
        contentsDeductibleCode AS contents_deductible_code,
        netContentsPaymentAmount AS net_contents_payment_amount,
        contentsPropertyValue AS contents_property_value,
        disasterAssistanceCoverageRequired AS disaster_assistance_coverage_required,
        eventDesignationNumber AS event_designation_number,
        ficoNumber AS fico_number,
        floodCharacteristicsIndicator AS flood_characteristics_indicator,
        floodWaterDuration AS flood_water_duration,
        floodproofedIndicator AS floodproofed_indicator,
        floodEvent AS flood_event,
        iccCoverage AS icc_coverage,
        netIccPaymentAmount AS net_icc_payment_amount,
        nfipRatedCommunityNumber AS nfip_rated_community_number,
        nfipCommunityNumberCurrent AS nfip_community_number_current,
        nfipCommunityName AS nfip_community_name,
        nonPaymentReasonContents AS non_payment_reason_contents,
        nonPaymentReasonBuilding AS non_payment_reason_building,
        numberOfUnits AS number_of_units,
        buildingReplacementCost AS building_replacement_cost,
        contentsReplacementCost AS contents_replacement_cost,
        replacementCostBasis AS replacement_cost_basis,
        stateOwnedIndicator AS state_owned_indicator,
        waterDepth AS water_depth,
        floodZoneCurrent AS flood_zone_current,
        buildingDescriptionCode AS building_description_code,
        rentalPropertyIndicator AS rental_property_indicator,
        reportedCity AS reported_city,
        -- NFIP writes the empty string here where it writes NULL in censusGeoid. Same file,
        -- two conventions; normalised to NULL so a downstream predicate is written once.
        NULLIF(reportedZipCode, '') AS reported_zip_code,
        countyCode AS county_code,
        censusGeoid AS census_geoid,
        latitude,
        longitude,
        foundationType AS foundation_type,
        openDate AS open_date,
        mostRecentRecoveryDate AS most_recent_recovery_date,
        exteriorWaterDepth AS exterior_water_depth,
        interiorWaterDepth AS interior_water_depth,
        mostRecentPaymentDate AS most_recent_payment_date,
        preFirmIndicator AS pre_firm_indicator,
        totalSalvageRecovery AS total_salvage_recovery,
        totalBldgClaimPmtRecovery AS total_bldg_claim_pmt_recovery,
        totalContentsClaimPmtRecovery AS total_contents_claim_pmt_recovery,
        totalIccClaimPmtRecovery AS total_icc_claim_pmt_recovery,
        totalSubrogationRecovery AS total_subrogation_recovery,
        id AS claim_id,
        state AS state_code
    FROM {{ source('raw_insurance', 'nfip_claims') }}

),

with_county AS (

    SELECT
        renamed.*,
        COALESCE(NULLIF(TRIM(county_code), ''), NULLIF(SUBSTR(census_geoid, 1, 5), ''))
            AS county_fips,
        CASE
            WHEN NULLIF(TRIM(county_code), '') IS NOT NULL
                THEN 'county_code'
            WHEN NULLIF(SUBSTR(census_geoid, 1, 5), '') IS NOT NULL
                THEN 'census_geoid'
        END AS county_fips_source
    FROM renamed

)

SELECT
    c.*,

    -- geography -------------------------------------------------------------
    c.county_fips IS NOT NULL                       AS has_usable_county,
    -- FEMA's own code for "state unavailable". Real claims, never dropped.
    c.state_code = 'UN'                             AS is_state_unknown,
    -- 15 Michigan-filed claims carry an out-of-state county FIPS. Surfaced rather than
    -- corrected: which of the two fields is wrong is FEMA's to say, not ours.
    CASE
        WHEN c.county_fips IS NULL OR s.state_fips IS NULL THEN NULL
        ELSE SUBSTR(c.county_fips, 1, 2) = s.state_fips
    END                                             AS county_matches_reported_state,

    -- loss timing -----------------------------------------------------------
    -- year_of_loss agrees with date_of_loss on every one of the 2.7M rows, checked.
    strftime(c.date_of_loss, '%Y-%m')               AS loss_year_month,
    CAST(month(c.date_of_loss) AS TINYINT)          AS loss_month,

    -- payment ---------------------------------------------------------------
    -- The three gross columns are NULL on exactly the same rows, so this is NULL for a
    -- claim closed without payment and never a spurious zero.
    c.amount_paid_on_building_claim
        + c.amount_paid_on_contents_claim
        + c.amount_paid_on_icc_claim                AS total_amount_paid,
    c.net_building_payment_amount
        + c.net_contents_payment_amount
        + c.net_icc_payment_amount                  AS total_net_payment,

    c.amount_paid_on_building_claim IS NULL
        AND (
            c.non_payment_reason_building IS NOT NULL
            OR c.non_payment_reason_contents IS NOT NULL
        )                                           AS is_closed_without_payment,
    c.amount_paid_on_building_claim IS NULL
        AND c.non_payment_reason_building IS NULL
        AND c.non_payment_reason_contents IS NULL   AS is_unexplained_missing_payment,
    COALESCE(
        c.amount_paid_on_building_claim
            + c.amount_paid_on_contents_claim
            + c.amount_paid_on_icc_claim = 0,
        FALSE
    )                                               AS is_zero_paid,
    -- Recoveries and adjustments push a handful of claims negative. Real, and kept.
    COALESCE(c.amount_paid_on_building_claim < 0, FALSE)
        OR COALESCE(c.amount_paid_on_contents_claim < 0, FALSE)
        OR COALESCE(c.amount_paid_on_icc_claim < 0, FALSE)
                                                    AS has_negative_payment

FROM with_county c
LEFT JOIN {{ ref('dim_state') }} s
    ON s.state_code = c.state_code
