{{ config(materialized = 'view') }}

/*
  NFIP policies in force, typed and named. Michigan only, by decision.

  Raw is MI-scoped here and nowhere else in the insurance track, which CLAUDE.md section 4
  allows as a documented size exception: the national file is 74.3M rows against roughly 384k
  for Michigan, and INS-E4 needs a Michigan policy count. The national benchmark comes from
  FEMA's own published state statistics rather than from raw, and the manifest records both
  the reason and the national total. Nothing here re-filters; the scope was set at ingestion
  and L1 fails the run if a non-Michigan row appears.

  Three things a policy-count measure has to get right, all of them made explicit here.

  ## A row is not a policy

  policy_count runs from 1 to 289. A condominium master policy covers many units in one row,
  so INS-E4's "policies in force" is SUM(policy_count) and never COUNT(*). Counting rows
  would understate Michigan's book, and by a different amount each year as the condo mix
  shifts. is_multi_policy_row flags where the two diverge.

  ## In force is a date range, not a status column

  There is no in-force flag: a policy is in force on a date when that date falls between
  policy_effective_date and policy_termination_date. Neither is ever NULL, which makes the
  range usable directly -- but four rows terminate on or before they take effect, and
  has_invalid_term marks them rather than letting them silently drop out of, or double into,
  a month-end count.

  ## Only censusGeoid carries county here

  Unlike the claims file, this one has no countyCode column at all, so the coalesce that
  rescues 3% of claims has nothing to work with. Coverage is 77 of Michigan's 83 counties,
  and that is a fact about where flood policies are sold rather than a load failure -- which
  is why L1 reports the roster for this source instead of requiring all 83.

  Note the null convention differs per column in the same file: census_geoid is NULL when
  absent while reportedZipCode uses the empty string. Normalised to NULL below so a
  downstream predicate is written once.
*/

WITH renamed AS (

    SELECT
        agricultureStructureIndicator AS agriculture_structure_indicator,
        asOfDate AS as_of_date,
        baseFloodElevation AS base_flood_elevation,
        basementEnclosureCrawlspaceType AS basement_enclosure_crawlspace_type,
        cancellationDateOfFloodPolicy AS cancellation_date_of_flood_policy,
        condominiumCoverageTypeCode AS condominium_coverage_type_code,
        construction,
        crsClassCode AS crs_class_code,
        buildingDeductibleCode AS building_deductible_code,
        contentsDeductibleCode AS contents_deductible_code,
        elevatedBuildingIndicator AS elevated_building_indicator,
        elevationCertificateIndicator AS elevation_certificate_indicator,
        elevationDifference AS elevation_difference,
        federalPolicyFee AS federal_policy_fee,
        ratedFloodZone AS rated_flood_zone,
        hfiaaSurcharge AS hfiaa_surcharge,
        houseOfWorshipIndicator AS house_of_worship_indicator,
        locationOfContents AS location_of_contents,
        lowestAdjacentGrade AS lowest_adjacent_grade,
        lowestFloorElevation AS lowest_floor_elevation,
        nonProfitIndicator AS non_profit_indicator,
        numberOfFloorsInInsuredBuilding AS number_of_floors_in_insured_building,
        obstructionType AS obstruction_type,
        occupancyType AS occupancy_type,
        originalConstructionDate AS original_construction_date,
        originalNBDate AS original_nb_date,
        policyCost AS policy_cost,
        policyCount AS policy_count,
        policyEffectiveDate AS policy_effective_date,
        policyTerminationDate AS policy_termination_date,
        policyTermIndicator AS policy_term_indicator,
        postFIRMConstructionIndicator AS post_firm_construction_indicator,
        primaryResidenceIndicator AS primary_residence_indicator,
        rateMethod AS rate_method,
        regularEmergencyProgramIndicator AS regular_emergency_program_indicator,
        smallBusinessIndicatorBuilding AS small_business_indicator_building,
        totalBuildingInsuranceCoverage AS total_building_insurance_coverage,
        totalContentsInsuranceCoverage AS total_contents_insurance_coverage,
        totalInsurancePremiumOfThePolicy AS total_insurance_premium_of_the_policy,
        cancellationVoidanceReasonCode AS cancellation_voidance_reason_code,
        subsidizedRateType AS subsidized_rate_type,
        iccPremium AS icc_premium,
        reserveFundAssessment AS reserve_fund_assessment,
        communityProbationSurcharge AS community_probation_surcharge,
        premiumPaymentIndicator AS premium_payment_indicator,
        buildingReplacementCost AS building_replacement_cost,
        basicBuildingRate AS basic_building_rate,
        additionalBuildingRate AS additional_building_rate,
        basicContentsRate AS basic_contents_rate,
        additionalContentsRate AS additional_contents_rate,
        enclosureTypeCode AS enclosure_type_code,
        buildingDescriptionCode AS building_description_code,
        insuranceToValueCode AS insurance_to_value_code,
        postFirmVzoneIndicator AS post_firm_vzone_indicator,
        floodproofedIndicator AS floodproofed_indicator,
        waitingPeriodType AS waiting_period_type,
        rolloverTransferCode AS rollover_transfer_code,
        endorsementEffectiveDate AS endorsement_effective_date,
        propertyPurchaseDate AS property_purchase_date,
        rentalPropertyIndicator AS rental_property_indicator,
        tenantIndicator AS tenant_indicator,
        stateOwnedIndicator AS state_owned_indicator,
        disasterAssistanceCoverageRequiredCode AS disaster_assistance_coverage_required_code,
        mandatoryPurchaseFlag AS mandatory_purchase_flag,
        grandfatheringTypeCode AS grandfathering_type_code,
        nfipRatedCommunityNumber AS nfip_rated_community_number,
        nfipCommunityNumberCurrent AS nfip_community_number_current,
        nfipCommunityName AS nfip_community_name,
        programTypeIndicator AS program_type_indicator,
        mapPanelNumber AS map_panel_number,
        mapPanelSuffix AS map_panel_suffix,
        floodZoneCurrent AS flood_zone_current,
        femaRegion AS fema_region,
        reportedCity AS reported_city,
        -- Empty string here, NULL in census_geoid. Same publisher, same file.
        NULLIF(reportedZipCode, '') AS reported_zip_code,
        censusGeoid AS census_geoid,
        latitude,
        longitude,
        buildingOnFederalLand AS building_on_federal_land,
        buildingPurpose AS building_purpose,
        seasonallyOccupied AS seasonally_occupied,
        fullRiskPremium AS full_risk_premium,
        buildingOverWaterType AS building_over_water_type,
        foundationType AS foundation_type,
        preFIRMConstructionIndicator AS pre_firm_construction_indicator,
        id AS policy_id,
        propertyState AS state_code
    FROM {{ source('raw_insurance', 'nfip_policies') }}

)

SELECT
    renamed.*,

    -- geography -------------------------------------------------------------
    -- No countyCode column in this file, so censusGeoid is the only source of county.
    NULLIF(SUBSTR(census_geoid, 1, 5), '')          AS county_fips,
    NULLIF(SUBSTR(census_geoid, 1, 5), '') IS NOT NULL
                                                    AS has_usable_county,

    -- term ------------------------------------------------------------------
    CAST(
        date_diff('day', policy_effective_date, policy_termination_date) AS INTEGER
    )                                               AS policy_term_days,
    -- Four rows terminate on or before they begin. Flagged, not filtered: an in-force count
    -- that silently dropped them would differ from one that silently kept them, and neither
    -- would say so.
    policy_termination_date <= policy_effective_date
                                                    AS has_invalid_term,

    -- counting --------------------------------------------------------------
    -- The reason INS-E4 sums rather than counts: one row can be 289 policies.
    policy_count > 1                                AS is_multi_policy_row

FROM renamed
