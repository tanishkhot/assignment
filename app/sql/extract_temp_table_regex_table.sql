-- Placeholder: filter tables by name pattern
-- Will be used to exclude temp tables when a regex is provided
AND table_name !~ '{exclude_table_regex}'

