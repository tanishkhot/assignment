"""Workflow for Postgres metadata extraction with custom output step."""

from temporalio import workflow
from temporalio.common import RetryPolicy

from application_sdk.workflows.metadata_extraction.sql import (
    BaseSQLMetadataExtractionWorkflow,
)
from application_sdk.constants import ENABLE_ATLAN_UPLOAD

from .activities import SQLMetadataExtractionActivities


@workflow.defn
class SQLMetadataExtractionWorkflow(BaseSQLMetadataExtractionWorkflow):
    activities_cls = SQLMetadataExtractionActivities

    @staticmethod
    def get_activities(activities: SQLMetadataExtractionActivities):
        base = list(BaseSQLMetadataExtractionWorkflow.get_activities(activities))
        # Register the custom export activity so the worker knows about it
        base.append(activities.write_text_output)
        return base

    @workflow.run
    async def run(self, workflow_config: dict) -> None:
        # Run base workflow (preflight + fetch + transform)
        await super().run(workflow_config)

        # Retrieve workflow args and run exit activities (text export, optional upload)
        workflow_args = await workflow.execute_activity_method(
            self.activities_cls.get_workflow_args,
            workflow_config,
            retry_policy=RetryPolicy(maximum_attempts=3, backoff_coefficient=2),
            start_to_close_timeout=self.default_start_to_close_timeout,
            heartbeat_timeout=self.default_heartbeat_timeout,
        )
        await self.run_exit_activities(workflow_args)

    async def run_exit_activities(self, workflow_args: dict) -> None:
        retry_policy = RetryPolicy(maximum_attempts=6, backoff_coefficient=2)

        # Write human-readable text output
        await workflow.execute_activity_method(
            self.activities_cls.write_text_output,
            args=[workflow_args],
            retry_policy=retry_policy,
            start_to_close_timeout=self.default_start_to_close_timeout,
            heartbeat_timeout=self.default_heartbeat_timeout,
        )

        # Preserve base behavior: optional Atlan upload
        if ENABLE_ATLAN_UPLOAD:
            await workflow.execute_activity_method(
                self.activities_cls.upload_to_atlan,
                args=[workflow_args],
                retry_policy=retry_policy,
                start_to_close_timeout=self.default_start_to_close_timeout,
                heartbeat_timeout=self.default_heartbeat_timeout,
            )
