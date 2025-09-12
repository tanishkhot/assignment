-- List tables with include/exclude filtering and optional temp-table exclusion
SELECT
  table_catalog,
  table_schema,
  table_name,
  CASE WHEN table_type = 'BASE TABLE' THEN 'TABLE' ELSE table_type END AS table_type
FROM information_schema.tables
WHERE table_schema NOT LIKE 'pg_%'
  AND table_schema <> 'information_schema'
  AND concat(table_catalog, concat('.', table_schema)) !~ '{normalized_exclude_regex}'
  AND concat(table_catalog, concat('.', table_schema)) ~ '{normalized_include_regex}'
  {temp_table_regex_sql};
