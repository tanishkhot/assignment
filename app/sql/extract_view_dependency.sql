/*
 * Extract view dependencies (table -> view) with include/exclude filters
 */
SELECT
  vtu.table_catalog  AS src_catalog_name,
  vtu.table_schema   AS src_schema_name,
  vtu.table_name     AS src_table_name,
  vtu.view_catalog   AS dst_catalog_name,
  vtu.view_schema    AS dst_schema_name,
  vtu.view_name      AS dst_table_name
FROM information_schema.view_table_usage vtu
WHERE (vtu.table_schema NOT LIKE 'pg_%' AND vtu.table_schema <> 'information_schema')
  AND (vtu.view_schema  NOT LIKE 'pg_%' AND vtu.view_schema  <> 'information_schema')
  AND concat(vtu.table_catalog, concat('.', vtu.table_schema)) !~ '{normalized_exclude_regex}'
  AND concat(vtu.table_catalog, concat('.', vtu.table_schema))  ~ '{normalized_include_regex}';

