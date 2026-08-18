{{ config(severity = coverage_test_severity()) }}

/*
  Michigan must resolve to exactly its 83 counties, plus the one unassigned bucket.

  Michigan at county grain is the analytical focus of all three tracks (CLAUDE.md section 4),
  so a missing county here would silently shrink every per-capita rate built on it -- the
  same reason `reconcile.michigan` requires the 83-county roster of the sources that act as
  denominators. Warns rather than fails against the fixture, whose ACS sample is not a census.
*/

SELECT
    COUNT(*) FILTER (WHERE is_reportable_county)                        AS reportable_counties,
    COUNT(*) FILTER (WHERE geography_kind = 'unknown_county')           AS unassigned_buckets
FROM {{ ref('dim_geography_county') }}
WHERE is_michigan
HAVING COUNT(*) FILTER (WHERE is_reportable_county) <> 83
    OR COUNT(*) FILTER (WHERE geography_kind = 'unknown_county') <> 1
