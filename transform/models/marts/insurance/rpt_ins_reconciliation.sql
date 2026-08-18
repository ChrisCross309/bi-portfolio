{{ config(materialized = 'table') }}

/*
  INS-E5: do our totals tie to FEMA's published figures?

  This is the question the whole repo is arranged around, and it is the only one whose answer
  lives outside the warehouse. FEMA's own count of its claims is not a column in any table --
  it is what the API reported at the moment the bulk file was downloaded, and the ingestion
  layer wrote it into the source's manifest for exactly this purpose. So this model reads the
  manifests, which are the provenance record, and puts the publisher's number beside ours.

  ## The gap is a fact about the publisher, not a defect

  FEMA reported 2,724,656 claims while the bulk file it served contained 2,724,014 -- 642
  fewer. Counts and bulk files are generated at different moments, so a small gap is a timing
  observation. L1 classifies it that way already, using the two refresh timestamps: identical
  timestamps with different counts would be a failure, differing ones are lag. What this model
  adds is the same comparison stated as a reportable number rather than a check result, so the
  executive page can show a reconciliation certificate rather than assert that one passed.

  A gap anywhere downstream of landing is ours and is a defect. Those rows read zero here, and
  a test asserts they stay zero.

  ## Michigan policies have no publisher count at all

  The policies endpoint refuses a count for a broad filter -- it times out server-side and
  returns 503 -- so there is nothing to tie the Michigan slice to. What the manifest carries
  instead is FEMA's published national total, which is what INS-E4's national benchmark uses,
  and the paging invariants that stand in for a count. Stated as such rather than left as a
  suspicious NULL.

  The manifest path follows the target, the same way ingest.common.paths_for does in Python:
  the live warehouse reads data/raw and CI reads the fixture tree, so this model builds
  offline against sample manifests whose numbers are sample-sized and internally consistent.
*/

{% set manifest_root = 'data/_fixture/raw' if target.name == 'ci' else 'data/raw' %}

WITH claims_manifest AS (

    SELECT
        source_reported_count                   AS publisher_count,
        rows_landing,
        rows_raw,
        rows_duckdb,
        retrieved_at,
        source_last_refresh,
        source_metadata_refresh
    FROM read_json_auto('{{ manifest_root }}/insurance/nfip_claims/manifest.json')

),

declarations_manifest AS (

    SELECT
        source_reported_count                   AS publisher_count,
        rows_landing,
        rows_raw,
        rows_duckdb,
        retrieved_at,
        source_last_refresh
    FROM read_json_auto('{{ manifest_root }}/insurance/fema_declarations/manifest.json')

),

policies_manifest AS (

    SELECT
        source_reported_national                AS publisher_national_count,
        rows_landing,
        rows_raw,
        rows_duckdb,
        retrieved_at,
        source_last_refresh
    FROM read_json_auto('{{ manifest_root }}/insurance/nfip_policies/manifest.json')

),

warehouse AS (

    SELECT
        (SELECT COUNT(*) FROM {{ ref('stg_ins__nfip_claims') }})            AS claims_staged,
        (SELECT SUM(claim_count) FROM {{ ref('fct_ins_claims_monthly') }})  AS claims_in_mart,
        (SELECT COUNT(*) FROM {{ ref('stg_ins__fema_declarations') }})      AS declaration_areas_staged,
        (SELECT COUNT(*) FROM {{ ref('dim_ins_disaster_declaration') }})    AS disasters_in_mart,
        (SELECT COUNT(*) FROM {{ ref('stg_ins__nfip_policies') }})          AS policies_staged

)

SELECT
    'nfip_claims'                                   AS source_name,
    'claim records'                                 AS measure_label,
    c.publisher_count                               AS publisher_figure,
    c.rows_landing,
    c.rows_raw,
    c.rows_duckdb,
    w.claims_staged                                 AS rows_staged,
    w.claims_in_mart                                AS rows_in_mart,
    c.publisher_count - c.rows_landing              AS publisher_to_landing_gap,
    w.claims_in_mart - c.rows_duckdb                AS warehouse_to_mart_gap,
    c.retrieved_at,
    c.source_last_refresh,
    c.source_metadata_refresh,
    'A publisher count and the bulk file it serves are generated at different moments, so a '
        || 'gap here is timing. A gap between the warehouse and the mart is ours.'
                                                    AS interpretation
FROM claims_manifest c
CROSS JOIN warehouse w

UNION ALL

SELECT
    'fema_declarations',
    'declaration-area records',
    d.publisher_count,
    d.rows_landing,
    d.rows_raw,
    d.rows_duckdb,
    w.declaration_areas_staged,
    w.declaration_areas_staged,
    d.publisher_count - d.rows_landing,
    w.declaration_areas_staged - d.rows_duckdb,
    d.retrieved_at,
    d.source_last_refresh,
    NULL,
    'One row per declaration per designated area. The mart collapses these to '
        || CAST(w.disasters_in_mart AS VARCHAR) || ' distinct disasters.'
FROM declarations_manifest d
CROSS JOIN warehouse w

UNION ALL

SELECT
    'nfip_policies',
    'policy records (Michigan slice)',
    NULL,
    p.rows_landing,
    p.rows_raw,
    p.rows_duckdb,
    w.policies_staged,
    w.policies_staged,
    NULL,
    w.policies_staged - p.rows_duckdb,
    p.retrieved_at,
    p.source_last_refresh,
    NULL,
    'No publisher count exists for this slice: the endpoint times out and returns 503 on a '
        || 'count over a broad filter. FEMA''s published national total of '
        || CAST(p.publisher_national_count AS VARCHAR) || ' policies is what INS-E4 '
        || 'benchmarks against instead.'
FROM policies_manifest p
CROSS JOIN warehouse w
