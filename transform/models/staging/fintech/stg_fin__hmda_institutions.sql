{{ config(materialized = 'view') }}

/*
  The HMDA filer list: which institutions reported in which year, and what they called
  themselves. National, 39,381 rows.

  The grain is one row per LEI per filing year, so an institution that filed in six years
  appears six times. Counting institutions means counting distinct LEIs, not rows -- the
  manifest says the same thing, and it is the kind of mistake that produces a number six
  times too large without looking wrong.

  This is the institution reference because the richer transmittal and panel files are
  published only inside the annual snapshot bundles, whose URLs cannot be resolved from any
  machine-readable index. Taking them would have meant hardcoding a guessed URL.

  normalized_name is here for FIN-E5, which has to match a CFPB company name to an LEI
  because nothing published joins them. The normalisation is shared with the complaint side
  through a macro, because applying it slightly differently to each side of that join would
  give a match rate that looks plausible and is wrong.
*/

SELECT
    lei,
    name                                    AS institution_name,
    {{ normalize_company_name('name') }}    AS institution_name_normalized,
    TRY_CAST(count AS INTEGER)              AS filings_reported,
    period                                  AS activity_year
FROM {{ source('raw_fintech', 'hmda_institutions') }}
