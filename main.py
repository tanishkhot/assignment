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
import os
from typing import Optional

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

        ui_index_path = os.path.abspath("frontend/static/index.html")
        logger.info(
            f"UI static index exists: {os.path.exists(ui_index_path)} at {ui_index_path}"
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
            has_configmap=True,
        )
        # Expose local output folder and a small helper endpoint for results
        try:
            fastapi_app: Optional[object] = getattr(getattr(application, "server", None), "app", None)
            if fastapi_app:
                from fastapi.staticfiles import StaticFiles  # type: ignore
                from fastapi.responses import PlainTextResponse, JSONResponse  # type: ignore
                from fastapi import APIRouter  # type: ignore

                # Mount static files under /output to serve generated results
                fastapi_app.mount("/output", StaticFiles(directory="output"), name="output")

                # Lightweight results endpoint: /workflows/v1/result/{workflow_id}
                router = APIRouter()

                @router.get("/workflows/v1/result/{workflow_id}")  # type: ignore
                async def get_result(workflow_id: str):
                    path = os.path.join("output", workflow_id, "output.txt")
                    if os.path.exists(path):
                        try:
                            with open(path, "r", encoding="utf-8") as f:
                                return PlainTextResponse(f.read())
                        except Exception:
                            return PlainTextResponse("Failed to read results.", status_code=500)
                    return PlainTextResponse("Results not ready.", status_code=404)

                @router.get("/workflows/v1/latest-output")  # type: ignore
                async def latest_output():
                    base = os.path.join("output")
                    if not os.path.exists(base):
                        return JSONResponse({}, status_code=404)
                    try:
                        candidates = []
                        for name in os.listdir(base):
                            p = os.path.join(base, name)
                            if os.path.isdir(p):
                                out = os.path.join(p, "output.txt")
                                if os.path.exists(out):
                                    candidates.append((name, os.path.getmtime(out)))
                        if not candidates:
                            return JSONResponse({}, status_code=404)
                        candidates.sort(key=lambda x: x[1], reverse=True)
                        return JSONResponse({"workflow_id": candidates[0][0]})
                    except Exception:
                        return JSONResponse({}, status_code=500)

                fastapi_app.include_router(router)
        except Exception:
            # Non-fatal if SDK structure differs; frontend will fallback gracefully
            logger.warning("Could not mount /output or results endpoint", exc_info=True)
        await application.start_server()

    except ApiError:
        logger.error(f"{ApiError.SERVER_START_ERROR}", exc_info=True)
        raise


if __name__ == "__main__":
    asyncio.run(main())
