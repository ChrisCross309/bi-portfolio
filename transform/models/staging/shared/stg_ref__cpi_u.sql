{{ config(materialized = 'view') }}

/*
  CPI-U observations, typed. The deflator INS-E2 and HLT-E3 both rest on.

  Three things BLS does here that break a naive filter, and the first one returns zero rows
  rather than an error.

  ## Every identifier is space-padded to a fixed width

  `series_id` is padded to 17 characters and `value` to 12, in both BLS files. A filter on
  `'CUUR0000SA0'` matches nothing at all -- the stored value is `'CUUR0000SA0      '`. Raw
  keeps the padding because that is what BLS published; this is where it comes off, once, so
  no downstream model has to remember.

  ## The annual average is a thirteenth month

  BLS files it as `period = 'M13'`, in the same column as the twelve real months and the
  semiannual averages `S01`-`S03`. Averaging across `period` therefore double counts.
  Constant-dollar work uses M13, and `period_kind` is what makes choosing it deliberate.

  ## Unadjusted, not seasonally adjusted

  `CUUR0000SA0` is not seasonally adjusted; `CUSR0000SA0` is. Deflating a yearly figure uses
  the **unadjusted** annual average -- seasonal adjustment exists for month-over-month
  movement and is the wrong series here.

  One sentinel: 62 observations carry a padded `-` with footnote code `X`, which is BLS's
  "not available". It types to NULL with the footnote kept.
*/

SELECT
    TRIM(series_id)                             AS series_id,
    TRIM(period)                                AS period_code,
    CAST(year AS SMALLINT)                      AS calendar_year,

    CASE
        WHEN TRIM(period) = 'M13'                   THEN 'annual_average'
        WHEN TRIM(period) LIKE 'M%'                 THEN 'month'
        WHEN TRIM(period) LIKE 'S%'                 THEN 'semiannual'
    END                                         AS period_kind,
    CASE
        WHEN TRIM(period) LIKE 'M%' AND TRIM(period) <> 'M13'
            THEN CAST(SUBSTR(TRIM(period), 2, 2) AS TINYINT)
    END                                         AS calendar_month,
    -- Joins dim_period at month grain for the twelve real months, and at year grain for M13.
    CASE
        WHEN TRIM(period) = 'M13' THEN CAST(year AS VARCHAR)
        WHEN TRIM(period) LIKE 'M%' THEN year || '-' || SUBSTR(TRIM(period), 2, 2)
    END                                         AS period_key,

    -- 62 observations are a padded '-' with footnote X: BLS's "not available".
    TRY_CAST(NULLIF(TRIM(value), '-') AS DOUBLE) AS index_value,
    NULLIF(TRIM(value), '-') IS NULL            AS is_unavailable,
    NULLIF(TRIM(footnote_codes), '')            AS footnote_codes,

    -- The two series any constant-dollar measure has to choose between, named rather than
    -- left as a substring nobody parses at the call site.
    TRIM(series_id) = 'CUUR0000SA0'             AS is_deflator_series,
    SUBSTR(TRIM(series_id), 3, 1) = 'S'         AS is_seasonally_adjusted

FROM {{ source('raw_shared', 'cpi_u') }}
