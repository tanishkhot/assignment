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
        tmp_file = out_file + ".tmp"

        types = [
            "database",
            "schema",
            "table",
            "column",
            "index",
            "quality_metric",
            "relationship",
            "view_dependency",
        ]
        wrote_any = False
        import pandas as pd

        def _gather_raw_files(typename: str) -> list[str]:
            primary = os.path.join(output_path, "raw", typename, "chunk-*.parquet")
            files = sorted(glob.glob(primary))
            if not files:
                # Fallback: search under TEMPORARY_PATH for any matching raw chunks
                fallback = os.path.join(TEMPORARY_PATH, "**", "raw", typename, "chunk-*.parquet")
                files = sorted(glob.glob(fallback, recursive=True))
            return files

        try:
            with open(tmp_file, "w", encoding="utf-8") as f:
                for t in types:
                    files = _gather_raw_files(t)
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
                try:
                    f.flush()
                    os.fsync(f.fileno())
                except Exception:
                    pass
            os.replace(tmp_file, out_file)
        finally:
            try:
                if os.path.exists(tmp_file):
                    os.remove(tmp_file)
            except Exception:
                pass

        return {"written": wrote_any, "path": out_file}

    @activity.defn
    async def write_json_output(self, workflow_args: dict) -> dict | None:
        """Write a unified JSON file consolidating transformed outputs.

        Produces a single JSON object with per-type arrays at
        output/<workflow_id>/output.json
        """
        import json
        import pandas as pd

        workflow_id = workflow_args.get("workflow_id", get_workflow_id())
        output_path = workflow_args.get("output_path")
        if not output_path or not workflow_id:
            return None

        # Ensure we have local copies of transformed files by downloading from object store
        try:
            transformed_prefix = os.path.join(output_path, "transformed")
            await ObjectStore.download_prefix(
                source=get_object_store_prefix(transformed_prefix),
                destination=TEMPORARY_PATH,
            )
        except Exception:
            # Best effort; continue if already present locally
            pass

        out_dir = os.path.join("output", workflow_id)
        os.makedirs(out_dir, exist_ok=True)
        out_file = os.path.join(out_dir, "output.json")
        tmp_file = out_file + ".tmp"

        types = [
            "database",
            "schema",
            "table",
            "column",
            "index",
            "quality_metric",
            "relationship",
            "view_dependency",
        ]

        def _sanitize_record(rec: dict) -> dict:
            # Replace NaN/NaT with None; stringify unsupported types
            out: dict = {}
            for k, v in rec.items():
                try:
                    if v is None:
                        out[k] = None
                    else:
                        # pandas may give us numpy types; json.dumps can handle ints/floats
                        # but not NaN; ensure finite
                        if isinstance(v, float) and (v != v):  # NaN
                            out[k] = None
                        else:
                            out[k] = v
                except Exception:
                    out[k] = str(v)
            return out

        # Stream writer to avoid holding entire dataset in memory
        def _gather_transformed_files(typename: str) -> list[str]:
            base = os.path.join(output_path, "transformed", typename)
            patterns = [
                os.path.join(base, "chunk-*.jsonl"),
                os.path.join(base, "chunk-*.json.ignore"),
                os.path.join(base, "chunk-*.parquet"),
            ]
            files: list[str] = []
            for pat in patterns:
                files.extend(glob.glob(pat))
            files = sorted(files)
            if not files:
                # Fallback: scan TEMPORARY_PATH recursively for transformed chunks
                for ext in ("jsonl", "json.ignore", "parquet"):
                    pat = os.path.join(TEMPORARY_PATH, "**", "transformed", typename, f"chunk-*.{ext}")
                    files.extend(glob.glob(pat, recursive=True))
            return sorted(files)

        try:
            with open(tmp_file, "w", encoding="utf-8") as f:
                f.write("{\n")
                first_type = True
                for t in types:
                    if not first_type:
                        f.write(",\n")
                    first_type = False
                    f.write(f"  \"{t}\": [\n")
                    wrote_any = False
                    files = _gather_transformed_files(t)
                    for p in files:
                        try:
                            if p.endswith(".jsonl"):
                                with open(p, "r", encoding="utf-8") as jf:
                                    for line in jf:
                                        line = line.strip()
                                        if not line:
                                            continue
                                        try:
                                            rec = json.loads(line)
                                        except Exception:
                                            continue
                                        rec = _sanitize_record(rec)
                                        f.write(("    " if not wrote_any else ",\n    ") + json.dumps(rec))
                                        wrote_any = True
                            elif p.endswith(".json.ignore"):
                                with open(p, "r", encoding="utf-8") as jf:
                                    for line in jf:
                                        line = line.strip()
                                        if not line:
                                            continue
                                        try:
                                            rec = json.loads(line)
                                        except Exception:
                                            continue
                                        rec = _sanitize_record(rec)
                                        f.write(("    " if not wrote_any else ",\n    ") + json.dumps(rec))
                                        wrote_any = True
                            else:
                                df = pd.read_parquet(p)
                                if df is None or df.empty:
                                    continue
                                for rec in df.to_dict(orient="records"):
                                    rec = _sanitize_record(rec)
                                    f.write(("    " if not wrote_any else ",\n    ") + json.dumps(rec))
                                    wrote_any = True
                        except Exception:
                            continue
                    # Optional fallback: if we didn't write any records, try raw parquet before closing the array
                    if not wrote_any:
                        try:
                            raw_base = os.path.join(output_path, "raw", t)
                            raw_files = []
                            raw_files.extend(glob.glob(os.path.join(raw_base, "chunk-*.parquet")))
                            if not raw_files:
                                raw_files.extend(glob.glob(os.path.join(TEMPORARY_PATH, "**", "raw", t, "chunk-*.parquet"), recursive=True))
                            for rp in sorted(raw_files):
                                try:
                                    df = pd.read_parquet(rp)
                                    if df is None or df.empty:
                                        continue
                                    for rec in df.to_dict(orient="records"):
                                        rec = _sanitize_record(rec)
                                        f.write(("    " if not wrote_any else ",\n    ") + json.dumps(rec))
                                        wrote_any = True
                                except Exception:
                                    continue
                        except Exception:
                            pass
                    # Close the array
                    f.write("\n  ]")
                # trailing metadata (always close JSON)
                f.write(",\n  \"_meta\": {\n")
                f.write(f"    \"workflow_id\": \"{workflow_id}\"\n")
                # Close _meta and root object
                f.write("  }\n}\n")
                try:
                    f.flush()
                    os.fsync(f.fileno())
                except Exception:
                    pass
            os.replace(tmp_file, out_file)
            # Post-write guard: ensure the root object is closed
            try:
                with open(out_file, "rb+") as chk:
                    chk.seek(0, os.SEEK_END)
                    size = chk.tell()
                    back = min(256, size)
                    chk.seek(-back, os.SEEK_END)
                    tail = chk.read().decode("utf-8", errors="ignore")
                    if tail.rstrip().endswith("}") is False:
                        chk.seek(0, os.SEEK_END)
                        chk.write(b"\n}\n")
                        chk.flush()
                        os.fsync(chk.fileno())
            except Exception:
                pass
        finally:
            try:
                if os.path.exists(tmp_file):
                    os.remove(tmp_file)
            except Exception:
                pass
        return {"written": True, "path": out_file}

    @activity.defn
    async def write_excel_output(self, workflow_args: dict) -> dict | None:
        """Export results to Excel, with CSV zip fallback if Excel engine is unavailable.

        Preferred: output/<workflow_id>/output.xlsx
        Fallback:  output/<workflow_id>/output.zip (CSV files per type)
        """
        import pandas as pd
        excel_available = True
        try:
            import openpyxl  # noqa: F401  (ensure engine available)
        except Exception:
            excel_available = False

        workflow_id = workflow_args.get("workflow_id", get_workflow_id())
        output_path = workflow_args.get("output_path")
        if not output_path or not workflow_id:
            return None

        out_dir = os.path.join("output", workflow_id)
        os.makedirs(out_dir, exist_ok=True)
        xlsx_path = os.path.join(out_dir, "output.xlsx")
        xlsx_tmp = xlsx_path + ".tmp"
        zip_path = os.path.join(out_dir, "output.zip")
        zip_tmp = zip_path + ".tmp"

        # Ensure transformed and raw outputs are locally available
        try:
            await ObjectStore.download_prefix(
                source=get_object_store_prefix(os.path.join(output_path, "transformed")),
                destination=TEMPORARY_PATH,
            )
        except Exception:
            pass
        try:
            await ObjectStore.download_prefix(
                source=get_object_store_prefix(os.path.join(output_path, "raw")),
                destination=TEMPORARY_PATH,
            )
        except Exception:
            pass

        types = [
            "database",
            "schema",
            "table",
            "column",
            "index",
            "quality_metric",
            "relationship",
            "view_dependency",
        ]

        def _gather_transformed_files(typename: str) -> list[str]:
            base = os.path.join(output_path, "transformed", typename)
            patterns = [
                os.path.join(base, "chunk-*.jsonl"),
                os.path.join(base, "chunk-*.json.ignore"),
                os.path.join(base, "chunk-*.parquet"),
            ]
            files: list[str] = []
            for pat in patterns:
                files.extend(glob.glob(pat))
            files = sorted(files)
            if not files:
                for ext in ("jsonl", "json.ignore", "parquet"):
                    pat = os.path.join(TEMPORARY_PATH, "**", "transformed", typename, f"chunk-*.{ext}")
                    files.extend(glob.glob(pat, recursive=True))
            return sorted(files)

        def _read_as_dataframe(files: list[str]) -> pd.DataFrame:
            frames: list[pd.DataFrame] = []
            for p in files:
                try:
                    if p.endswith(".jsonl") or p.endswith(".json.ignore"):
                        rows = []
                        with open(p, "r", encoding="utf-8") as jf:
                            for line in jf:
                                line = line.strip()
                                if not line:
                                    continue
                                try:
                                    import json
                                    rows.append(json.loads(line))
                                except Exception:
                                    continue
                        if rows:
                            frames.append(pd.DataFrame(rows))
                    else:
                        df = pd.read_parquet(p)
                        if df is not None and not df.empty:
                            frames.append(df)
                except Exception:
                    continue
            if not frames:
                return pd.DataFrame()
            try:
                return pd.concat(frames, ignore_index=True)
            except Exception:
                return frames[0]

        def _write_csv_zip(dfs: dict[str, pd.DataFrame]) -> bool:
            import io, zipfile
            try:
                with zipfile.ZipFile(zip_tmp, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                    # README first
                    zf.writestr("README.txt", "Generated by Postgres connector. One CSV per metadata type.\n")
                    # Include master text if present
                    try:
                        master_text_path = os.path.join(out_dir, "output.txt")
                        if os.path.exists(master_text_path):
                            with open(master_text_path, "r", encoding="utf-8") as mf:
                                zf.writestr("MASTER_TEXT.txt", mf.read())
                    except Exception:
                        pass
                    for name, df in dfs.items():
                        try:
                            stream = io.StringIO()
                            if df is None or df.empty:
                                df = pd.DataFrame({"info": ["no rows found"]})
                            df.to_csv(stream, index=False)
                            zf.writestr(f"{name}.csv", stream.getvalue())
                        except Exception:
                            # write a tiny placeholder
                            try:
                                zf.writestr(f"{name}.csv", "error\nfailed to write\n")
                            except Exception:
                                pass
                os.replace(zip_tmp, zip_path)
                return True
            finally:
                try:
                    if os.path.exists(zip_tmp):
                        os.remove(zip_tmp)
                except Exception:
                    pass

        # Build dataframes for all types once
        dfs: dict[str, pd.DataFrame] = {}
        for t in types:
            files = _gather_transformed_files(t)
            dfs[t] = _read_as_dataframe(files)

        # Try Excel first if engine is available
        if excel_available:
            try:
                written_any = False
                with pd.ExcelWriter(xlsx_tmp, engine="openpyxl") as writer:
                    for t, df in dfs.items():
                        if df is None or df.empty:
                            df = pd.DataFrame({"info": ["no rows found"]})
                        sheet = t[:31]
                        try:
                            df.to_excel(writer, sheet_name=sheet, index=False)
                            written_any = written_any or (not df.empty)
                        except Exception:
                            try:
                                pd.DataFrame({"error": ["failed to write"]}).to_excel(writer, sheet_name=sheet, index=False)
                            except Exception:
                                pass
                    # Add MASTER_TEXT from output.txt if present
                    try:
                        master_text_path = os.path.join(out_dir, "output.txt")
                        if os.path.exists(master_text_path):
                            with open(master_text_path, "r", encoding="utf-8") as mf:
                                lines = [ln.rstrip("\n") for ln in mf.readlines()]
                            if lines:
                                pd.DataFrame({"text": lines}).to_excel(writer, sheet_name="MASTER_TEXT", index=False)
                                written_any = True
                        else:
                            # Fallback note
                            pd.DataFrame({"text": ["output.txt not found"]}).to_excel(writer, sheet_name="MASTER_TEXT", index=False)
                    except Exception:
                        pass
                    try:
                        pd.DataFrame({
                            "about": [
                                "This workbook is generated by the Postgres connector.",
                                "Each sheet corresponds to a metadata type.",
                                "Cells may contain estimates for quality metrics."
                            ]
                        }).to_excel(writer, sheet_name="README", index=False)
                    except Exception:
                        pass
                try:
                    with open(xlsx_tmp, "rb") as _f:
                        pass
                except Exception:
                    pass
                os.replace(xlsx_tmp, xlsx_path)
                return {"written": written_any, "path": xlsx_path, "format": "xlsx"}
            except Exception:
                # Fall through to CSV zip
                try:
                    if os.path.exists(xlsx_tmp):
                        os.remove(xlsx_tmp)
                except Exception:
                    pass

        # Fallback: CSV zip (engine not available or Excel write failed)
        wrote_zip = _write_csv_zip(dfs)
        return {"written": wrote_zip, "path": (zip_path if wrote_zip else None), "format": "zip"}

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

    # ---------------------
    # Indexes
    # ---------------------

    @activity.defn
    async def fetch_indexes(self, workflow_args: dict):
        state = await self._get_state(workflow_args)
        if not state.sql_client or not state.sql_client.engine:
            raise ValueError("SQL client or engine not initialized")
        from application_sdk.common.utils import prepare_query
        query = prepare_query(
            query=self.read_sql_query_from_file("extract_index.sql"),
            workflow_args=workflow_args,
        )
        return await self.query_executor(
            sql_engine=state.sql_client.engine,
            sql_query=query,
            workflow_args=workflow_args,
            output_suffix="raw/index",
            typename="index",
        )

    @activity.defn
    async def transform_indexes(self, workflow_args: dict):
        """Pass-through transform: parquet -> JSON rows for indexes."""
        output_prefix = workflow_args.get("output_prefix")
        output_path = workflow_args.get("output_path")
        typename = "index"
        if not (output_prefix and output_path):
            raise ValueError("Missing output paths")

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
            # No additional mapping required; write as-is
            await out.write_dataframe(df)
            total += len(df)

        stats = await out.get_statistics(typename=typename)
        return stats

    # ---------------------
    # Quality metrics (per column)
    # ---------------------

    @activity.defn
    async def fetch_quality_metrics(self, workflow_args: dict):
        state = await self._get_state(workflow_args)
        if not state.sql_client or not state.sql_client.engine:
            raise ValueError("SQL client or engine not initialized")
        from application_sdk.common.utils import prepare_query
        query = prepare_query(
            query=self.read_sql_query_from_file("extract_quality_metrics.sql"),
            workflow_args=workflow_args,
        )
        return await self.query_executor(
            sql_engine=state.sql_client.engine,
            sql_query=query,
            workflow_args=workflow_args,
            output_suffix="raw/quality_metric",
            typename="quality_metric",
        )

    @activity.defn
    async def transform_quality_metrics(self, workflow_args: dict):
        """Pass-through transform: parquet -> JSON rows for quality metrics."""
        output_prefix = workflow_args.get("output_prefix")
        output_path = workflow_args.get("output_path")
        typename = "quality_metric"
        if not (output_prefix and output_path):
            raise ValueError("Missing output paths")

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
            await out.write_dataframe(df)
            total += len(df)

        stats = await out.get_statistics(typename=typename)
        return stats

    @activity.defn
    async def summarize_outputs(self, workflow_args: dict) -> dict:
        """Summarize transformed outputs into a small JSON for Temporal result.

        Collects statistics.json.ignore for each typename and returns counts plus
        the human-readable export file path.
        """
        import json
        summary: dict = {"types": {}}
        output_prefix = workflow_args.get("output_prefix")
        output_path = workflow_args.get("output_path")
        workflow_id = workflow_args.get("workflow_id")
        if not (output_prefix and output_path and workflow_id):
            return summary

        transformed_dir = os.path.join(output_path, "transformed")
        # Download transformed folder index so stats files are present locally
        try:
            await ObjectStore.download_prefix(
                source=get_object_store_prefix(transformed_dir),
                destination=TEMPORARY_PATH,
            )
        except Exception:
            pass

        for typename in [
            "database",
            "schema",
            "table",
            "column",
            "index",
            "quality_metric",
            "view_dependency",
            "relationship",
        ]:
            stats_path = os.path.join(
                output_path, "transformed", typename, "statistics.json.ignore"
            )
            try:
                # Ensure latest copy locally
                await ObjectStore.download_file(
                    source=get_object_store_prefix(stats_path),
                    destination=stats_path,
                )
            except Exception:
                continue
            try:
                with open(stats_path, "r", encoding="utf-8") as f:
                    stats = json.load(f)
                summary["types"][typename] = {
                    "total_record_count": stats.get("total_record_count", 0),
                    "chunk_count": stats.get("chunk_count", 0),
                }
            except Exception:
                continue

        # Add convenient paths
        summary["output_text"] = os.path.join("output", workflow_id, "output.txt")
        summary["output_json"] = os.path.join("output", workflow_id, "output.json")
        summary["objectstore_prefix"] = get_object_store_prefix(output_path)

        # Persist a copy locally for the UI to fetch
        try:
            out_dir = os.path.join("output", workflow_id)
            os.makedirs(out_dir, exist_ok=True)
            import json as _json
            sum_path = os.path.join(out_dir, "summary.json")
            tmp_sum = sum_path + ".tmp"
            with open(tmp_sum, "w", encoding="utf-8") as f:
                _json.dump(summary, f, indent=2)
                try:
                    f.flush(); os.fsync(f.fileno())
                except Exception:
                    pass
            os.replace(tmp_sum, sum_path)
            summary["summary_path"] = os.path.join("output", workflow_id, "summary.json")
        except Exception:
            pass

        return summary

    @activity.defn
    async def fetch_view_dependencies(self, workflow_args: dict):
        state = await self._get_state(workflow_args)
        if not state.sql_client or not state.sql_client.engine:
            raise ValueError("SQL client or engine not initialized")
        from application_sdk.common.utils import prepare_query
        query = prepare_query(
            query=self.read_sql_query_from_file("extract_view_dependency.sql"),
            workflow_args=workflow_args,
        )
        return await self.query_executor(
            sql_engine=state.sql_client.engine,
            sql_query=query,
            workflow_args=workflow_args,
            output_suffix="raw/view-dependency",
            typename="view_dependency",
        )

    @activity.defn
    async def transform_view_dependencies(self, workflow_args: dict):
        """Transforms view dependency rows into table->view lineage edges JSON."""
        output_prefix = workflow_args.get("output_prefix")
        output_path = workflow_args.get("output_path")
        typename = "view_dependency"
        if not (output_prefix and output_path):
            raise ValueError("Missing output paths")

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
                src = f"{workflow_args.get('connection',{}).get('connection_qualified_name','')}/{r.get('src_catalog_name')}/{r.get('src_schema_name')}/{r.get('src_table_name')}"
                dst = f"{workflow_args.get('connection',{}).get('connection_qualified_name','')}/{r.get('dst_catalog_name')}/{r.get('dst_schema_name')}/{r.get('dst_table_name')}"
                rows.append({
                    "fromQualifiedName": src,
                    "toQualifiedName": dst,
                    "typeName": "view_dependency"
                })
            if rows:
                await out.write_dataframe(pd.DataFrame(rows))
                total += len(rows)

        stats = await out.get_statistics(typename=typename)
        return stats
