"""Activities for Postgres connector.

Reuses the SDK's BaseSQLMetadataExtractionActivities and pins our
local SQLClient so queries run against Postgres using psycopg.
"""

from typing import Optional, Type

from application_sdk.activities.metadata_extraction.sql import (
    BaseSQLMetadataExtractionActivities,
)

from .clients import SQLClient


class SQLMetadataExtractionActivities(BaseSQLMetadataExtractionActivities):
    """Postgres metadata extraction activities.

    Uses BaseSQLMetadataExtractionActivities logic with the Postgres SQLClient.
    """

    # Ensure our client is used by default
    sql_client_class: Type[SQLClient] = SQLClient

    def __init__(self, multidb: bool = False):
        super().__init__(
            sql_client_class=SQLClient,
            handler_class=None,  # default BaseSQLHandler
            transformer_class=None,  # default QueryBasedTransformer
            multidb=multidb,
        )

