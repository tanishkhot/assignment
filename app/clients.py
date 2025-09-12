"""Postgres SQL client implementation.

Defines the SQLAlchemy connection URL template and required fields
for connecting to Postgres using psycopg.
"""

from application_sdk.clients.models import DatabaseConfig
from application_sdk.clients.sql import BaseSQLClient


class SQLClient(BaseSQLClient):
    """SQL client for Postgres.

    Uses psycopg (psycopg3) driver and the standard
    username/password/host/port/database credential fields.
    """

    DB_CONFIG = DatabaseConfig(
        template=(
            "postgresql+psycopg://{username}:{password}@{host}:{port}/{database}"
        ),
        required=["username", "password", "host", "port", "database"],
        # Allow passing connection parameters like sslmode via credentials.extra
        parameters=["sslmode"],
    )

    def add_connection_params(self, connection_string: str, source_connection_params: dict) -> str:  # type: ignore[override]
        """Override to skip None/empty values when appending query params.

        Prevents cases like sslmode=None which psycopg rejects.
        """
        for key, value in source_connection_params.items():
            if value is None or str(value).strip().lower() == "none" or str(value).strip() == "":
                continue
            if "?" not in connection_string:
                connection_string += "?"
            else:
                connection_string += "&"
            connection_string += f"{key}={value}"

        return connection_string
