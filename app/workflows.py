"""Workflow for Postgres metadata extraction.

This workflow reuses the SDK's BaseSQLMetadataExtractionWorkflow
and binds it to our Postgres activities implementation.
"""

from temporalio import workflow

from application_sdk.workflows.metadata_extraction.sql import (
    BaseSQLMetadataExtractionWorkflow,
)

from .activities import SQLMetadataExtractionActivities


@workflow.defn
class SQLMetadataExtractionWorkflow(BaseSQLMetadataExtractionWorkflow):
    """Postgres workflow wired with our activities class."""

    activities_cls = SQLMetadataExtractionActivities
