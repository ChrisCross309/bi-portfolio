{{ config(materialized = 'table') }}

/*
  One row per federally declared disaster. 5,239 of them, from 70,184 declaration-area rows.

  The source publishes a row per declaration per designated area, so this collapses to the
  disaster and keeps the area detail for the bridge to use. Counting declarations here is
  counting rows; counting them in the source is not, and that difference is a factor of
  thirteen.

  The incident window is taken as the earliest begin and latest end across the disaster's
  areas, because a single event can be declared for different counties on different dates.
  A disaster with any open area has no end date at all, and is_incident_open says so rather
  than letting a BETWEEN quietly match nothing.
*/

SELECT
    d.disaster_number,
    MIN(d.fema_declaration_string)              AS fema_declaration_string,
    MIN(d.declaration_type)                     AS declaration_type,
    MIN(d.declaration_title)                    AS declaration_title,
    MIN(d.incident_type)                        AS incident_type,
    MIN(d.declaration_date)                     AS declaration_date,

    MIN(d.incident_begin_date)                  AS incident_begin_date,
    -- NULL if any area of this disaster is still open.
    CASE WHEN COUNT(*) FILTER (WHERE d.is_incident_open) = 0
         THEN MAX(d.incident_end_date) END      AS incident_end_date,
    COUNT(*) FILTER (WHERE d.is_incident_open) > 0
                                                AS is_incident_open,
    BOOL_OR(d.has_invalid_incident_window)      AS has_invalid_incident_window,

    COUNT(*)                                    AS designated_area_count,
    COUNT(DISTINCT d.county_fips)               AS designated_county_count,
    COUNT(*) FILTER (WHERE d.is_statewide)      AS statewide_area_count,
    COUNT(DISTINCT d.state_code)                AS state_count,

    BOOL_OR(d.is_flood_related)                 AS is_flood_related,
    BOOL_OR(d.is_major_disaster)                AS is_major_disaster,
    BOOL_OR(d.state_code = 'MI')                AS touches_michigan,
    BOOL_OR(d.ia_program_declared)              AS ia_program_declared,
    BOOL_OR(d.pa_program_declared)              AS pa_program_declared

FROM {{ ref('stg_ins__fema_declarations') }} d
GROUP BY d.disaster_number
