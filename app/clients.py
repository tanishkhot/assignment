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
