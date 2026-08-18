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

  Deliberately not done here: fuzzy or token-set matching, and any attempt to resolve
  corporate hierarchy. CFPB names the holding company where HMDA names the filing subsidiary
  -- "JPMORGAN CHASE & CO." against "JPMorgan Chase Bank, National Association", "WELLS FARGO
  & COMPANY" against "Wells Fargo Bank, National Association" -- and that is the largest
  single cause of the unmatched bucket, not a shortcoming of the string rule. Widening the
  suffix list to close it was tried and made the match rate worse, because stripping more
  tokens collapses distinct institutions together. rpt_fin_entity_match_rate publishes what
  the gap costs instead of papering over it.
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
