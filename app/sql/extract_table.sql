-- Placeholder: list tables
-- Final query will be set in a later step
SELECT table_catalog, table_schema, table_name, table_type
FROM information_schema.tables
WHERE table_schema NOT LIKE 'pg_%' AND table_schema <> 'information_schema';

