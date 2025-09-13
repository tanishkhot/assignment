/*
 * Extract table indexes (including primary/unique) with include/exclude filters
 */
SELECT
  current_database()           AS catalog_name,
  n.nspname                    AS schema_name,
  t.relname                    AS table_name,
  i.relname                    AS index_name,
  ix.indisunique               AS is_unique,
  ix.indisprimary              AS is_primary,
  string_agg(a.attname, ',' ORDER BY x.n) AS column_names
FROM pg_class t
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_index ix     ON ix.indrelid = t.oid
JOIN pg_class i      ON i.oid = ix.indexrelid
JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS x(attnum, n) ON true
JOIN pg_attribute a  ON a.attrelid = t.oid AND a.attnum = x.attnum
WHERE t.relkind = 'r'
  AND n.nspname NOT LIKE 'pg_%'
  AND n.nspname <> 'information_schema'
  AND concat(current_database(), concat('.', n.nspname)) !~ '{normalized_exclude_regex}'
  AND concat(current_database(), concat('.', n.nspname))  ~ '{normalized_include_regex}'
GROUP BY catalog_name, schema_name, table_name, index_name, is_unique, is_primary;

