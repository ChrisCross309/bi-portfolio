{#
  `warn` against the fixture warehouse, `error` against the real one.

  A coverage claim -- "every county a publisher references resolves", "all 83 Michigan
  counties are present" -- is a statement about the whole dataset, and a stratified sample
  cannot answer one. The fixture carries two ACS vintages and forty non-Michigan counties by
  construction, so a test that fails there would be failing correct data.

  This is the dbt expression of a rule the Python harness already follows in three places:
  `reconcile.michigan` skips the county roster in fixture mode, `reconcile.questions` relaxes
  every minimum above one, and L1's year-coverage checks do the same. A SKIP or a WARN is
  never a silent pass -- it is the honest answer to a question the sample cannot be asked.
#}
{% macro coverage_test_severity() -%}
    {{ 'warn' if target.name == 'ci' else 'error' }}
{%- endmacro %}
