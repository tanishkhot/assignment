-- Placeholder: basic metadata filter for databases/schemas
SELECT schema_name, catalog_name
FROM information_schema.schemata
WHERE schema_name NOT LIKE 'pg_%' AND schema_name <> 'information_schema';

