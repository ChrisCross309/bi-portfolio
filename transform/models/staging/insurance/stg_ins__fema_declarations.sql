{{ config(materialized = 'view') }}

/*
  FEMA disaster declarations, typed and named, national. The event dimension for INS-E3.

  This is what makes a Michigan flood loss event-driven rather than attritional: a claim
  whose loss date falls inside a declared incident window, in a county the declaration named.
  The model's job is to make that window and that county usable; the matching itself is the
  mart's.

  ## The grain is finer than a disaster, and finer than an area

  70,184 rows carry only 5,239 distinct declaration strings, because FEMA files one row per
  declaration per designated area. So counting declarations means counting distinct
  disaster_number, and counting rows counts area-designations.

  Less obviously, designated_area is **not** unique within a disaster either -- 28 pairs
  repeat. Disaster 4527 designated six different "Lincoln (Township of)" rows, one per county
  that contains a township of that name. A join on the area *name* would fan out silently.
  The stable key is id, and the meaningful grain is disaster plus county plus place code.

  ## A statewide declaration names no county

  1,605 rows carry fips_county_code = '000'. That is a real declaration covering the whole
  state, not a missing value, and is_statewide says so. An event-attribution rule that joins
  on county alone will miss every one of them, which is why the flag exists here rather than
  being rediscovered in the mart.

  ## An open incident has no end date

  599 rows have a NULL incident_end_date: the incident had not been closed when the file was
  generated. A BETWEEN against a NULL end silently matches nothing, so is_incident_open marks
  them and the mart decides whether an open window runs to the present or is excluded.

  Two rows end before they begin. Flagged rather than repaired -- which of the two dates is
  wrong is FEMA's to say.
*/

WITH renamed AS (

    SELECT
        id AS declaration_area_id,
        disasterNumber AS disaster_number,
        femaDeclarationString AS fema_declaration_string,
        declarationType AS declaration_type,
        declarationDate AS declaration_date,
        fyDeclared AS fy_declared,
        incidentType AS incident_type,
        declarationTitle AS declaration_title,
        ihProgramDeclared AS ih_program_declared,
        iaProgramDeclared AS ia_program_declared,
        paProgramDeclared AS pa_program_declared,
        hmProgramDeclared AS hm_program_declared,
        incidentBeginDate AS incident_begin_date,
        incidentEndDate AS incident_end_date,
        disasterCloseoutDate AS disaster_closeout_date,
        tribalRequest AS tribal_request,
        fipsStateCode AS fips_state_code,
        fipsCountyCode AS fips_county_code,
        placeCode AS place_code,
        designatedArea AS designated_area,
        declarationRequestNumber AS declaration_request_number,
        lastIAFilingDate AS last_ia_filing_date,
        incidentId AS incident_id,
        region,
        designatedIncidentTypes AS designated_incident_types,
        lastRefresh AS last_refresh,
        hash,
        state AS state_code
    FROM {{ source('raw_insurance', 'fema_declarations') }}

)

SELECT
    renamed.*,

    -- geography -------------------------------------------------------------
    -- '000' is a statewide designation, so it is a real county key that names no county --
    -- and dim_geography_county carries it as an unknown_county row for exactly that reason.
    fips_state_code || fips_county_code             AS county_fips,
    fips_county_code = '000'                        AS is_statewide,
    fips_county_code <> '000'                       AS has_usable_county,

    -- incident window -------------------------------------------------------
    incident_end_date IS NULL                       AS is_incident_open,
    COALESCE(incident_end_date < incident_begin_date, FALSE)
                                                    AS has_invalid_incident_window,
    CAST(
        date_diff('day', incident_begin_date, incident_end_date) AS INTEGER
    )                                               AS incident_duration_days,

    -- declaration type ------------------------------------------------------
    -- DR major disaster, EM emergency, FM fire management. Kept as published; the
    -- distinction matters because only some carry Individual Assistance.
    declaration_type = 'DR'                         AS is_major_disaster,
    LOWER(incident_type) IN ('flood', 'severe storm', 'dam/levee break')
                                                    AS is_flood_related

FROM renamed
