{{ config(materialized = 'table') }}

/*
  Which Michigan claims fall inside which declared disaster. The whole of INS-E3.

  INS-E3 asks what share of Michigan paid losses is event-driven rather than attritional, and
  nothing published answers it -- FEMA declares disasters and NFIP pays claims, and the two
  files share no key. This is the attribution, and because it is inferred rather than
  published, its rule is stated here and every number built on it inherits the caveat.

  ## The rule

  A claim is attributed to a disaster when all three hold:

    1. the disaster is flood-related -- flood, severe storm, or dam/levee break;
    2. the claim's county was designated by that disaster, or the disaster was declared
       statewide for Michigan -- 1,605 declaration rows name no county and would otherwise
       attribute nothing at all;
    3. the loss date falls between the incident begin date and the incident end date;
    4. extended by a grace window after the incident ends, because a flood that begins on the
       last declared day does not stop that night. The window is a variable rather than a
       literal so its effect can be measured, and the drill bank asks for 30, 60 and 90 days
       precisely because the answer moves with it.

  ## Why the first condition is not optional

  Without it this model reported 100% of Michigan losses as event-driven in 2021 and 2022,
  which looked like a finding and was an artifact. FEMA declared COVID-19 a statewide Michigan
  disaster -- numbers 4494 and 3455, incident type Biological -- with a window running from
  2020-01-20 to 2023-05-11. Statewide, three years, and so it matched every Michigan flood
  claim in that span: 13,541 of them, attributed to a pandemic.

  A declaration is a legal instrument rather than a description of the weather, and a
  federally declared disaster is not automatically a flood event. Restricting attribution to
  flood-related incident types is what makes the resulting share mean what INS-E3 asks.

  ## What it is not

  A claim can match more than one disaster -- overlapping windows in the same county are
  common when a storm system is declared twice. This is a bridge and not a lookup, so a naive
  join through it double counts paid dollars. Sum over distinct claims, or take the earliest
  match, and say which.

  Claims matching no disaster are attritional by definition here, which means "not inside a
  declared window" rather than "not caused by an event". A flood too small to be declared is
  still a flood.

  The 2020 Midland dam failures (disasters 3525 and 4547) and the 2021 southeast Michigan
  flooding (4607) are the anchor events recorded in the declarations manifest, and the tests
  assert this model finds them.
*/

{% set grace_days = var('event_attribution_grace_days') %}

SELECT
    c.claim_id,
    c.county_fips,
    c.date_of_loss,
    c.year_of_loss                                      AS loss_year,
    c.total_amount_paid,

    d.disaster_number,
    a.declaration_type,
    a.declaration_title,
    a.incident_begin_date,
    a.incident_end_date,

    a.incident_type,
    d.is_statewide                                      AS matched_via_statewide_declaration,
    CAST(date_diff('day', a.incident_begin_date, a.incident_end_date) AS INTEGER)
                                                        AS incident_duration_days,
    CAST(date_diff('day', a.incident_begin_date, c.date_of_loss) AS INTEGER)
                                                        AS days_into_incident,
    c.date_of_loss > a.incident_end_date                AS is_in_grace_window,
    {{ grace_days }}                                    AS grace_window_days

FROM {{ ref('stg_ins__nfip_claims') }} c
JOIN {{ ref('stg_ins__fema_declarations') }} d
    -- A statewide declaration designates no county, so it matches every Michigan claim in
    -- its window; a county-specific one matches only its own.
    ON d.state_code = 'MI'
   AND (d.is_statewide OR d.county_fips = c.county_fips)
JOIN {{ ref('dim_ins_disaster_declaration') }} a
    ON a.disaster_number = d.disaster_number
   -- A declared disaster is not automatically a flood event. See the COVID note above.
   AND a.is_flood_related
WHERE c.state_code = 'MI'
  AND c.date_of_loss >= a.incident_begin_date
  -- An open incident has no end, so it runs to the present rather than matching nothing.
  AND c.date_of_loss <= COALESCE(a.incident_end_date, CURRENT_DATE)
                        + INTERVAL ({{ grace_days }}) DAY
