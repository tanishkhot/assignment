"""Postgres handler tweaks for metadata keys.

Adjusts key names expected by BaseSQLHandler to match the
lowercased column names returned by the SQL client.
"""

from application_sdk.handlers.sql import BaseSQLHandler


class PostgresHandler(BaseSQLHandler):
    """Customize key names for database/schema fields.

    BaseSQLClient.run_query() lower-cases column names, so we align
    the keys the handler uses to pick values from query results.
    """

    # Align with our filter_metadata.sql aliases and client lower-casing
    database_result_key = "catalog_name"
    schema_result_key = "schema_name"

    async def fetch_databases(self):  # type: ignore[override]
        """Fetch databases using lower-cased row keys from the SQL client.

        Returns list of dicts keyed by self.database_result_key (catalog_name).
        """
        if not self.sql_client:
            raise ValueError("SQL Client not defined")
        if self.metadata_sql is None:
            raise ValueError("metadata_sql is not defined")

        databases = []
        async for batch in self.sql_client.run_query(self.metadata_sql):
            for row in batch:
                # Prefer catalog_name, fallback to table_catalog (lower/upper)
                value = (
                    row.get("catalog_name")
                    or row.get("table_catalog")
                    or row.get("TABLE_CATALOG")
                )
                if value is not None:
                    databases.append({self.database_result_key: value})
        return databases
