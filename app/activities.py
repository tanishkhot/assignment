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

        types = ["database", "schema", "table", "column", "relationship"]
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

    @activity.defn
    async def fetch_relationships(self, workflow_args: dict):
        state = await self._get_state(workflow_args)
        if not state.sql_client or not state.sql_client.engine:
            raise ValueError("SQL client or engine not initialized")
        from application_sdk.common.utils import prepare_query
        query = prepare_query(
            query=self.read_sql_query_from_file("extract_relationship.sql"),
            workflow_args=workflow_args,
        )
        return await self.query_executor(
            sql_engine=state.sql_client.engine,
            sql_query=query,
            workflow_args=workflow_args,
            output_suffix="raw/relationship",
            typename="relationship",
        )

    def read_sql_query_from_file(self, filename: str) -> str:
        base = os.path.join(os.path.dirname(__file__), "sql")
        path = os.path.join(base, filename)
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    @activity.defn
    async def transform_relationships(self, workflow_args: dict):
        """Transforms FK rows into simple lineage edges JSON.

        Output fields: fromQualifiedName, toQualifiedName, typeName
        """
        output_prefix = workflow_args.get("output_prefix")
        output_path = workflow_args.get("output_path")
        typename = "relationship"
        if not (output_prefix and output_path):
            raise ValueError("Missing output paths")

        # Download raw relationship parquet
        raw_dir = os.path.join(output_path, "raw", typename)
        try:
            await ObjectStore.download_prefix(
                source=get_object_store_prefix(raw_dir),
                destination=TEMPORARY_PATH,
            )
        except Exception:
            return {"total_record_count": 0, "chunk_count": 0, "typename": typename}

        import pandas as pd
        from application_sdk.outputs.json import JsonOutput

        files = sorted(glob.glob(os.path.join(raw_dir, "chunk-*.parquet")))
        out = JsonOutput(
            output_path=output_path,
            output_prefix=output_prefix,
            output_suffix="transformed",
            typename=typename,
        )
        total = 0
        for p in files:
            try:
                df = pd.read_parquet(p)
            except Exception:
                continue
            if df is None or df.empty:
                continue
            rows = []
            for _, r in df.iterrows():
                src = f"{workflow_args.get('connection',{}).get('connection_qualified_name','')}/{r.get('src_catalog_name')}/{r.get('src_schema_name')}/{r.get('src_table_name')}/{r.get('src_column_name')}"
                dst = f"{workflow_args.get('connection',{}).get('connection_qualified_name','')}/{r.get('dst_catalog_name')}/{r.get('dst_schema_name')}/{r.get('dst_table_name')}/{r.get('dst_column_name')}"
                rows.append({
                    "fromQualifiedName": src,
                    "toQualifiedName": dst,
                    "typeName": "fk_lineage"
                })
            if rows:
                import pandas as pd
                await out.write_dataframe(pd.DataFrame(rows))
                total += len(rows)

        stats = await out.get_statistics(typename=typename)
        return stats
