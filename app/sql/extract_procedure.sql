-- Optional: extract routines (functions/procedures)
SELECT
  routine_catalog AS table_catalog,
  routine_schema  AS table_schema,
  routine_name    AS procedure_name,
  routine_type
FROM information_schema.routines r
WHERE r.routine_schema NOT LIKE 'pg_%'
  AND r.routine_schema <> 'information_schema'
  AND concat(r.routine_catalog, concat('.', r.routine_schema)) !~ '{normalized_exclude_regex}'
  AND concat(r.routine_catalog, concat('.', r.routine_schema)) ~ '{normalized_include_regex}';

