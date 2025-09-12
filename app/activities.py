"""Activities for Postgres connector.

Reuses the SDK's BaseSQLMetadataExtractionActivities and pins our
local SQLClient so queries run against Postgres using psycopg.
"""

from typing import Type

from application_sdk.activities.metadata_extraction.sql import (
    BaseSQLMetadataExtractionActivities,
)
from application_sdk.activities.common.utils import get_workflow_id
from application_sdk.services.objectstore import ObjectStore
from application_sdk.activities.common.utils import get_object_store_prefix
from application_sdk.constants import TEMPORARY_PATH
import os
import glob
from temporalio import activity

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

    @activity.defn
    async def write_text_output(self, workflow_args: dict) -> dict | None:
        """Convert raw parquet chunks to a single text file.

        Writes a unified text file at output/<workflow_id>/output.txt with
        tab-separated values for each asset type found under raw/.
        """
        workflow_id = workflow_args.get("workflow_id", get_workflow_id())
        output_path = workflow_args.get("output_path")
        if not output_path or not workflow_id:
            return None

        # Ensure we have local copies of raw files by downloading from object store
        try:
            raw_prefix = os.path.join(output_path, "raw")
            await ObjectStore.download_prefix(
                source=get_object_store_prefix(raw_prefix),
                destination=TEMPORARY_PATH,
            )
        except Exception:
            # Best effort; continue if already present locally
            pass

        out_dir = os.path.join("output", workflow_id)
        os.makedirs(out_dir, exist_ok=True)
        out_file = os.path.join(out_dir, "output.txt")

        types = ["database", "schema", "table", "column"]
        wrote_any = False
        import pandas as pd

        with open(out_file, "w", encoding="utf-8") as f:
            for t in types:
                pattern = os.path.join(output_path, "raw", t, "chunk-*.parquet")
                files = sorted(glob.glob(pattern))
                if not files:
                    continue
                wrote_any = True
                f.write(f"=== {t.upper()} ===\n")
                wrote_header = False
                for pfile in files:
                    try:
                        df = pd.read_parquet(pfile)
                        if df is None or df.empty:
                            continue
                        if not wrote_header:
                            f.write("\t".join(map(str, df.columns)) + "\n")
                            wrote_header = True
                        for _, row in df.iterrows():
                            f.write("\t".join(map(lambda v: "" if v is None else str(v), row.values)) + "\n")
                    except Exception:
                        continue

        return {"written": wrote_any, "path": out_file}
