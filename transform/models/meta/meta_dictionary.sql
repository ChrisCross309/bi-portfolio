{{ config(materialized = 'view', schema = 'meta') }}

/*
  The warehouse describing itself: one row per column, with what it is and what it means.

  `+persist_docs` writes every model and column description into DuckDB as a COMMENT, so a
  SQL client already shows them when you inspect a table. This is the other half of that --
  the version you can *query*, so "which table holds the flood zone?" and "what does
  `allocation_method` mean?" are one statement each rather than a hunt through the model
  files.

      SELECT * FROM meta.meta_dictionary WHERE column_name ILIKE '%flood%';
      SELECT * FROM meta.meta_dictionary WHERE column_description ILIKE '%suppress%';

  ## Why a view

  A table would be a snapshot of the schema at the moment it was built, and would go stale
  the first time a model changed without a full rebuild -- which is exactly when a reader is
  most likely to be consulting it. `duckdb_columns()` is evaluated at query time, so this
  cannot disagree with the warehouse it describes.

  ## Why it reads the catalogue rather than the manifest

  dbt's manifest knows what the descriptions *should* be; the catalogue knows what actually
  landed. Those differ whenever a model has been edited but not rebuilt, and the second is
  the honest answer to "what is in the database in front of me".

  Relation comments come from a union of tables and views because staging and intermediate
  models are views, marts and dimensions are tables, and a dictionary that covered only one
  would miss half the layers a reader traces a number through.

  `layer` is the one piece of interpretation here: schema names encode it already
  (`raw`, `stg`, `int`, `conformed`, `mart_*`, `seeds`), and spelling it out saves every
  consumer writing the same CASE expression.
*/

WITH relations AS (

    SELECT schema_name, table_name AS relation_name, 'table' AS relation_kind, comment
    FROM duckdb_tables()

    UNION ALL

    SELECT schema_name, view_name, 'view', comment
    FROM duckdb_views()
    WHERE NOT internal

)

SELECT
    c.schema_name,
    c.table_name                                        AS relation_name,
    r.relation_kind,

    CASE
        WHEN c.schema_name = 'raw'          THEN 'raw'
        WHEN c.schema_name = 'stg'          THEN 'staging'
        WHEN c.schema_name = 'int'          THEN 'intermediate'
        WHEN c.schema_name = 'conformed'    THEN 'conformed'
        WHEN c.schema_name = 'seeds'        THEN 'seed'
        WHEN c.schema_name LIKE 'mart\_%' ESCAPE '\' THEN 'mart'
        WHEN c.schema_name = 'meta'         THEN 'meta'
        ELSE 'other'
    END                                                 AS layer,

    -- Which project owns it. The track prefix is CLAUDE.md rule 9's whole purpose: any
    -- query's domain is visible at a glance.
    CASE
        WHEN c.schema_name = 'mart_ins' OR c.table_name LIKE 'stg\_ins\_\_%' ESCAPE '\'
             OR c.table_name LIKE 'ins\_%' ESCAPE '\'                       THEN 'insurance'
        WHEN c.schema_name = 'mart_fin' OR c.table_name LIKE 'stg\_fin\_\_%' ESCAPE '\'
             OR c.table_name LIKE 'fin\_%' ESCAPE '\'
             OR c.table_name LIKE 'int\_fin\_\_%' ESCAPE '\'                THEN 'fintech'
        WHEN c.schema_name = 'mart_hlt' OR c.table_name LIKE 'stg\_hlt\_\_%' ESCAPE '\'
             OR c.table_name LIKE 'hlt\_%' ESCAPE '\'
             OR c.table_name LIKE 'int\_hlt\_\_%' ESCAPE '\'                THEN 'health'
        WHEN c.table_name LIKE 'stg\_ref\_\_%' ESCAPE '\'
             OR c.table_name LIKE 'ref\_%' ESCAPE '\'                       THEN 'shared'
        ELSE 'shared'
    END                                                 AS track,

    c.column_index                                      AS ordinal_position,
    c.column_name,
    c.data_type,
    c.is_nullable,

    -- Empty string rather than NULL is how a described-but-blank column arrives; both mean
    -- undocumented, and a consumer should not have to know that.
    NULLIF(TRIM(r.comment), '')                         AS relation_description,
    NULLIF(TRIM(c.comment), '')                         AS column_description,
    NULLIF(TRIM(c.comment), '') IS NOT NULL             AS is_documented

FROM duckdb_columns() c
LEFT JOIN relations r
    ON  r.schema_name = c.schema_name
    AND r.relation_name = c.table_name
WHERE c.schema_name NOT IN ('information_schema', 'pg_catalog', 'main')
  AND NOT c.internal
