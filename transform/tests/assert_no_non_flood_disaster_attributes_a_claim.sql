/*
  Only a flood-related disaster may attribute a flood loss.

  This is the regression guard on the mistake that made INS-E3 read 100% event-driven in two
  separate years. FEMA declared COVID-19 a statewide Michigan disaster with a window spanning
  2020-01-20 to 2023-05-11, and a rule that matched on geography and dates alone attributed
  13,541 flood claims to a pandemic. The share looked like a finding.

  A declaration is a legal instrument rather than a description of the weather, and nothing
  stops FEMA from declaring another multi-year statewide emergency of some other kind.
*/

SELECT DISTINCT incident_type
FROM {{ ref('bridge_ins_claim_declaration') }}
WHERE LOWER(incident_type) NOT IN ('flood', 'severe storm', 'dam/levee break')
