-- Placeholder: filter columns by table name pattern
-- Will be used to exclude columns from temp tables when a regex is provided
AND c.table_name !~ '{exclude_table_regex}'

