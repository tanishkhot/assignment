"""
Main entry point for the Postgres metadata extraction application.

This initializes and runs the application using the Atlan Application SDK,
setting up the workflow, worker, and FastAPI server.
"""

import asyncio

from app.activities import SQLMetadataExtractionActivities
from app.clients import SQLClient
from app.handlers import PostgresHandler
from app.workflows import SQLMetadataExtractionWorkflow
from application_sdk.application.metadata_extraction.sql import (
    BaseSQLMetadataExtractionApplication,
)
from application_sdk.workflows.metadata_extraction.sql import (
    BaseSQLMetadataExtractionWorkflow,
)
from application_sdk.common.error_codes import ApiError
from application_sdk.observability.decorators.observability_decorator import (
    observability,
)
from application_sdk.observability.logger_adaptor import get_logger
from application_sdk.observability.metrics_adaptor import get_metrics
from application_sdk.observability.traces_adaptor import get_traces

logger = get_logger(__name__)
metrics = get_metrics()
traces = get_traces()


@observability(logger=logger, metrics=metrics, traces=traces)
async def main():
    try:
        # Initialize the application using the SDK, tying in our Postgres client
        application = BaseSQLMetadataExtractionApplication(
            name="postgres",
            client_class=SQLClient,
            handler_class=PostgresHandler,
        )

        # Register our workflow and activities with the worker
        await application.setup_workflow(
            workflow_and_activities_classes=[
                (SQLMetadataExtractionWorkflow, SQLMetadataExtractionActivities)
            ],
        )

        # Start the worker in background
        await application.start_worker()

        # Setup and start the FastAPI server with our workflow
        await application.setup_server(
            workflow_class=SQLMetadataExtractionWorkflow,
        )
        await application.start_server()

    except ApiError:
        logger.error(f"{ApiError.SERVER_START_ERROR}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
