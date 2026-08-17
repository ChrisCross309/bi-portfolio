{#
  Use a model's `+schema` verbatim instead of appending it to the target schema.

  dbt's default builds `<target.schema>_<custom>`, which would give `main_stg` and
  `main_mart_ins`. Raw is a bare `raw` schema (CLAUDE.md rule 9), so the default would
  leave the warehouse half in one convention and half in another -- and session 3 reads
  these names straight into Power BI, where `main_mart_ins` is a worse thing to look at
  than `mart_ins` for no gain.

  A model with no `+schema` still lands in the target's own schema, so anything
  unconfigured stays in `main` rather than somewhere surprising.

  This is dbt's documented extension point for exactly this, not a workaround:
  https://docs.getdbt.com/docs/build/custom-schemas
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
