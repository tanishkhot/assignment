-- Placeholder: list columns
-- Final query will be set in a later step
SELECT table_catalog, table_schema, table_name, column_name, ordinal_position, is_nullable, data_type
FROM information_schema.columns
WHERE table_schema NOT LIKE 'pg_%' AND table_schema <> 'information_schema';

