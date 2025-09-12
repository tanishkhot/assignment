-- List schemas with include/exclude filtering
SELECT
  s.schema_name,
  s.catalog_name
FROM information_schema.schemata s
WHERE s.schema_name NOT LIKE 'pg_%'
  AND s.schema_name <> 'information_schema'
  AND concat(s.catalog_name, concat('.', s.schema_name)) !~ '{normalized_exclude_regex}'
  AND concat(s.catalog_name, concat('.', s.schema_name)) ~ '{normalized_include_regex}';
