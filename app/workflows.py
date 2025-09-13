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
        # Register custom activities so the worker knows about them
        base.append(activities.fetch_indexes)
        base.append(activities.transform_indexes)
        base.append(activities.fetch_quality_metrics)
        base.append(activities.transform_quality_metrics)
        base.append(activities.fetch_view_dependencies)
        base.append(activities.transform_view_dependencies)
        base.append(activities.fetch_relationships)
        base.append(activities.transform_relationships)
        base.append(activities.summarize_outputs)
        base.append(activities.write_json_output)
        base.append(activities.write_text_output)
        base.append(activities.write_excel_output)
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
        # Run view dependency and relationship lineage
        retry_policy = RetryPolicy(maximum_attempts=3, backoff_coefficient=2)
        # Indexes
        await workflow.execute_activity_method(
            self.activities_cls.fetch_indexes,
            args=[workflow_args],
            retry_policy=retry_policy,
            start_to_close_timeout=self.default_start_to_close_timeout,
            heartbeat_timeout=self.default_heartbeat_timeout,
        )
        await workflow.execute_activity_method(
            self.activities_cls.transform_indexes,
            args=[workflow_args],
            retry_policy=retry_policy,
            start_to_close_timeout=self.default_start_to_close_timeout,
            heartbeat_timeout=self.default_heartbeat_timeout,
        )
        # Quality metrics
        await workflow.execute_activity_method(
            self.activities_cls.fetch_quality_metrics,
            args=[workflow_args],
            retry_policy=retry_policy,
            start_to_close_timeout=self.default_start_to_close_timeout,
            heartbeat_timeout=self.default_heartbeat_timeout,
        )
        await workflow.execute_activity_method(
            self.activities_cls.transform_quality_metrics,
            args=[workflow_args],
            retry_policy=retry_policy,
            start_to_close_timeout=self.default_start_to_close_timeout,
            heartbeat_timeout=self.default_heartbeat_timeout,
        )
        await workflow.execute_activity_method(
            self.activities_cls.fetch_view_dependencies,
            args=[workflow_args],
            retry_policy=retry_policy,
            start_to_close_timeout=self.default_start_to_close_timeout,
            heartbeat_timeout=self.default_heartbeat_timeout,
        )
        await workflow.execute_activity_method(
            self.activities_cls.transform_view_dependencies,
            args=[workflow_args],
            retry_policy=retry_policy,
            start_to_close_timeout=self.default_start_to_close_timeout,
            heartbeat_timeout=self.default_heartbeat_timeout,
        )
        await workflow.execute_activity_method(
            self.activities_cls.fetch_relationships,
            args=[workflow_args],
            retry_policy=retry_policy,
            start_to_close_timeout=self.default_start_to_close_timeout,
            heartbeat_timeout=self.default_heartbeat_timeout,
        )
        await workflow.execute_activity_method(
            self.activities_cls.transform_relationships,
            args=[workflow_args],
            retry_policy=retry_policy,
            start_to_close_timeout=self.default_start_to_close_timeout,
            heartbeat_timeout=self.default_heartbeat_timeout,
        )
        await self.run_exit_activities(workflow_args)

        # Summarize outputs for Temporal UI result
        summary = await workflow.execute_activity_method(
            self.activities_cls.summarize_outputs,
            args=[workflow_args],
            retry_policy=RetryPolicy(maximum_attempts=3, backoff_coefficient=2),
            start_to_close_timeout=self.default_start_to_close_timeout,
            heartbeat_timeout=self.default_heartbeat_timeout,
        )
        # Return summary so Temporal UI shows a human-readable result
        return summary

    async def run_exit_activities(self, workflow_args: dict) -> None:
        retry_policy = RetryPolicy(maximum_attempts=6, backoff_coefficient=2)

        # Write JSON output (default/preferred)
        try:
            await workflow.execute_activity_method(
                self.activities_cls.write_json_output,
                args=[workflow_args],
                retry_policy=retry_policy,
                start_to_close_timeout=self.default_start_to_close_timeout,
                heartbeat_timeout=self.default_heartbeat_timeout,
            )
        except Exception:
            # Non-fatal
            pass

        # Write human-readable text output
        try:
            await workflow.execute_activity_method(
                self.activities_cls.write_text_output,
                args=[workflow_args],
                retry_policy=retry_policy,
                start_to_close_timeout=self.default_start_to_close_timeout,
                heartbeat_timeout=self.default_heartbeat_timeout,
            )
        except Exception:
            # Non-fatal
            pass

        # Preserve base behavior: optional Atlan upload
        if ENABLE_ATLAN_UPLOAD:
            try:
                await workflow.execute_activity_method(
                    self.activities_cls.upload_to_atlan,
                    args=[workflow_args],
                    retry_policy=retry_policy,
                    start_to_close_timeout=self.default_start_to_close_timeout,
                    heartbeat_timeout=self.default_heartbeat_timeout,
                )
            except Exception:
                # Non-fatal
                pass

        # Write Excel export (optional convenience)
        try:
            await workflow.execute_activity_method(
                self.activities_cls.write_excel_output,
                args=[workflow_args],
                retry_policy=retry_policy,
                start_to_close_timeout=self.default_start_to_close_timeout,
                heartbeat_timeout=self.default_heartbeat_timeout,
            )
        except Exception:
            # Non-fatal: do not block summary
            pass
