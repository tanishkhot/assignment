-- Placeholder: count tables matching include/exclude filters (to be templated)
-- normalized_include_regex / normalized_exclude_regex / temp_table_regex_sql will be applied by the SDK
SELECT count(*) AS count
FROM information_schema.tables t
WHERE t.table_schema NOT LIKE 'pg_%' AND t.table_schema <> 'information_schema';

