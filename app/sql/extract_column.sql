-- List columns with include/exclude filtering and optional temp-table exclusion
SELECT
  c.table_catalog,
  c.table_schema,
  c.table_name,
  c.column_name,
  c.ordinal_position,
  c.is_nullable,
  c.data_type
FROM information_schema.columns c
WHERE c.table_schema NOT LIKE 'pg_%'
  AND c.table_schema <> 'information_schema'
  AND concat(c.table_catalog, concat('.', c.table_schema)) !~ '{normalized_exclude_regex}'
  AND concat(c.table_catalog, concat('.', c.table_schema)) ~ '{normalized_include_regex}'
  {temp_table_regex_sql};
