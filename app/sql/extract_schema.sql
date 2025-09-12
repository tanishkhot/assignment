-- Placeholder: list schemas
-- Final query will be set in a later step
SELECT schema_name, catalog_name
FROM information_schema.schemata
WHERE schema_name NOT LIKE 'pg_%' AND schema_name <> 'information_schema';

