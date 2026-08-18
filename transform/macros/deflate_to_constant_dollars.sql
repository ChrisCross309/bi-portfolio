{#
  Restate a nominal amount in constant dollars of a base year.

  Uses CUUR0000SA0 -- all items, US city average, **not seasonally adjusted** -- at its
  annual average, which BLS files as period M13. That combination is not a preference; it is
  the only correct one here. Seasonal adjustment exists for month-over-month movement and is
  the wrong series for deflating a yearly figure, and averaging across `period` would double
  count because the annual average sits in the same column as the twelve months.

  INS-E2 and HLT-E3 both rest on this, in two different tracks, which is why it is a macro
  rather than a CTE written twice. A deflator applied even slightly differently to two
  measures makes them incomparable while looking fine.

  The base year defaults to the latest annual average BLS has published. That currently means
  2025: there is no 2026 annual average yet, so a 2026 figure cannot be deflated at all --
  which is one of the reasons the marts stop at the latest complete year rather than the
  latest year with data in it.
#}
{% macro deflate_to_constant_dollars(amount_column, year_column, base_year=None) -%}
    {%- set base = base_year or var('deflator_base_year') -%}
    (
        {{ amount_column }}
        * (SELECT index_value FROM {{ ref('stg_ref__cpi_u') }}
           WHERE is_deflator_series AND period_kind = 'annual_average'
             AND calendar_year = {{ base }})
        / (SELECT index_value FROM {{ ref('stg_ref__cpi_u') }} d
           WHERE d.is_deflator_series AND d.period_kind = 'annual_average'
             AND d.calendar_year = {{ year_column }})
    )
{%- endmacro %}
