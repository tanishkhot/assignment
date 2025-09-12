"""Activities for Postgres connector.

Reuses the SDK's BaseSQLMetadataExtractionActivities and pins our
local SQLClient so queries run against Postgres using psycopg.
"""

from typing import Type

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

    # Use the base class __init__; the SDK passes
    # sql_client_class/handler_class/transformer_class when instantiating
    # from BaseSQLMetadataExtractionApplication.setup_workflow.
