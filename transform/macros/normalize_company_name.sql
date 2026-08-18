{#
  Casefold a company or institution name down to something two publishers might agree on.

  CFPB publishes a company name and HMDA publishes an LEI with its own name, and nothing
  published joins them -- FIN-E5 needs "complaints per $1B originated", which means matching
  the two. Nothing here is clever: lowercase, strip punctuation, collapse whitespace, and drop
  the corporate suffixes that the same firm spells four ways. That is enough to be worth
  doing and shallow enough to explain.

  It lives in a macro because both sides need exactly the same treatment. A normalisation
  applied slightly differently to each side of a join is worse than none: it produces a match
  rate that looks plausible and is wrong, which is the failure FIN-E5 has to be able to
  defend against.

  Deliberately not done here: fuzzy or token-set matching. That belongs with the model that
  reports a match rate and an unmatched bucket, where its cost can be measured.
#}
{% macro normalize_company_name(column) -%}
    NULLIF(
        TRIM(
            regexp_replace(
                regexp_replace(
                    regexp_replace(LOWER({{ column }}), '[^a-z0-9 ]', ' ', 'g'),
                    ' (incorporated|inc|corporation|corp|company|co|limited|ltd|llc|llp|lp|na|n a|plc|holdings|holding|group|bank|banks|bancorp|financial|finance|services|service|usa|us)( |$)',
                    ' ',
                    'g'
                ),
                ' +', ' ', 'g'
            )
        ),
        ''
    )
{%- endmacro %}
