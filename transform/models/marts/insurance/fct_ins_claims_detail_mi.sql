{{ config(materialized = 'table') }}

/*
  Michigan flood claims at claim grain. Roughly 14,900 rows.

  Every drill in the insurance bank that needs a single claim lives here: flood zone,
  occupancy, CRS class, pre/post-FIRM, foundation, water depth, development maturity. The
  national picture stays aggregated in fct_ins_claims_monthly, because 2.7M claims at claim
  grain would be a mart nobody queries interactively and a Power BI model nobody can refresh.

  That split is the Michigan-lens rule expressed as grain rather than as a filter: detail
  where the analysis is, aggregate where the benchmark is.

  Michigan's flood year is not the national one, and any month-over-month comparison has to
  say so. Michigan peaks March through May on snowmelt and spring convective storms; the
  national profile peaks August through October on hurricanes. Comparing the two by month
  compares two different physical processes, and reads as Michigan being anomalous when it is
  simply inland.
*/

SELECT
    c.claim_id,
    c.county_fips,
    g.county_name,
    c.has_usable_county,
    c.county_matches_reported_state,

    c.date_of_loss,
    c.year_of_loss                              AS loss_year,
    c.loss_month,
    c.loss_year_month,
    'month:' || c.loss_year_month                       AS period_id,

    c.amount_paid_on_building_claim,
    c.amount_paid_on_contents_claim,
    c.amount_paid_on_icc_claim,
    c.total_amount_paid,
    c.total_net_payment,
    c.is_closed_without_payment,
    c.is_zero_paid,
    c.has_negative_payment,

    {{ deflate_to_constant_dollars('c.total_amount_paid', 'c.year_of_loss') }}
                                                AS total_amount_paid_constant,

    -- the drill bank, as published
    c.rated_flood_zone,
    c.flood_zone_current,
    c.occupancy_type,
    c.crs_class_code,
    c.pre_firm_indicator,
    c.post_firm_construction_indicator,
    c.foundation_type,
    c.water_depth,
    c.exterior_water_depth,
    c.interior_water_depth,
    c.building_damage_amount,
    c.contents_damage_amount,
    c.total_building_insurance_coverage,
    c.total_contents_insurance_coverage,
    c.number_of_floors_in_the_insured_building,
    c.primary_residence_indicator,
    c.rental_property_indicator,
    c.elevated_building_indicator,
    c.nfip_community_name,
    c.reported_city,
    c.reported_zip_code,

    -- development maturity: how long the claim took to settle, for the 12/24/36-month drill
    c.open_date,
    c.most_recent_payment_date,
    CAST(date_diff('day', c.date_of_loss, c.most_recent_payment_date) AS INTEGER)
                                                AS days_loss_to_last_payment,

    -- Michigan floods on a different calendar from the nation. Named here so a seasonality
    -- visual has to acknowledge it rather than discover it.
    c.loss_month BETWEEN 3 AND 5                AS is_spring_melt_season

FROM {{ ref('stg_ins__nfip_claims') }} c
LEFT JOIN {{ ref('dim_geography_county') }} g
    ON g.county_fips = c.county_fips
WHERE c.state_code = 'MI'
