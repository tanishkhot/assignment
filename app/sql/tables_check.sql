-- Count tables matching include/exclude filters to validate scope
SELECT count(*) AS count
FROM information_schema.tables t
WHERE t.table_schema NOT LIKE 'pg_%'
  AND t.table_schema <> 'information_schema'
  AND concat(t.table_catalog, concat('.', t.table_schema)) !~ '{normalized_exclude_regex}'
  AND concat(t.table_catalog, concat('.', t.table_schema)) ~ '{normalized_include_regex}'
  {temp_table_regex_sql};
