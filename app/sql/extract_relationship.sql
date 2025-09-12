/*
 * Extract foreign key relationships (column-level) with include/exclude filters
 */
SELECT
  tc.table_catalog        AS src_catalog_name,
  tc.table_schema         AS src_schema_name,
  tc.table_name           AS src_table_name,
  kcu.column_name         AS src_column_name,
  ccu.table_catalog       AS dst_catalog_name,
  ccu.table_schema        AS dst_schema_name,
  ccu.table_name          AS dst_table_name,
  ccu.column_name         AS dst_column_name,
  tc.constraint_name      AS constraint_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
JOIN information_schema.constraint_column_usage AS ccu
  ON ccu.constraint_name = tc.constraint_name
  AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND concat(tc.table_catalog, concat('.', tc.table_schema)) !~ '{normalized_exclude_regex}'
  AND concat(tc.table_catalog, concat('.', tc.table_schema)) ~ '{normalized_include_regex}';

