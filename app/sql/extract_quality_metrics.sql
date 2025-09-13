/*
 * Extract quality metrics using pg_stats + estimated row counts.
 * Provides per-column estimates for null counts and distinct counts.
 */
WITH base AS (
  SELECT
    current_database()                         AS catalog_name,
    ps.schemaname                              AS schema_name,
    ps.tablename                               AS table_name,
    ps.attname                                 AS column_name,
    ps.null_frac,
    ps.n_distinct,
    COALESCE(pst.n_live_tup::bigint, pc.reltuples::bigint) AS total_rows_estimated
  FROM pg_stats ps
  JOIN pg_namespace n ON n.nspname = ps.schemaname
  JOIN pg_class pc ON pc.relname = ps.tablename AND pc.relnamespace = n.oid
  LEFT JOIN pg_stat_all_tables pst ON pst.relid = pc.oid
  WHERE ps.schemaname NOT LIKE 'pg_%'
    AND ps.schemaname <> 'information_schema'
    AND concat(current_database(), concat('.', ps.schemaname)) !~ '{normalized_exclude_regex}'
    AND concat(current_database(), concat('.', ps.schemaname))  ~ '{normalized_include_regex}'
)
SELECT
  catalog_name,
  schema_name,
  table_name,
  column_name,
  total_rows_estimated,
  null_frac,
  GREATEST(ROUND(null_frac * COALESCE(total_rows_estimated, 0))::bigint, 0) AS null_count_estimated,
  CASE
    WHEN n_distinct >= 0 THEN ROUND(n_distinct)::bigint
    WHEN n_distinct <  0 THEN ROUND((-n_distinct) * COALESCE(total_rows_estimated, 0))::bigint
    ELSE NULL
  END AS distinct_count_estimated,
  n_distinct AS n_distinct_raw
FROM base;

