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
                from fastapi.responses import PlainTextResponse, JSONResponse, Response  # type: ignore
                from fastapi import APIRouter, Body  # type: ignore

                # Resolve absolute path regardless of current working directory
                repo_root = os.path.dirname(os.path.abspath(__file__))
                outputs_dir = os.path.join(repo_root, "output")

                # Mount static files under /output to serve generated results
                # Mount even if the folder doesn't exist yet (created later by workflow)
                fastapi_app.mount(
                    "/output",
                    StaticFiles(directory=outputs_dir, check_dir=False),
                    name="output",
                )

                # Lightweight results endpoint: /workflows/v1/result/{workflow_id}
                router = APIRouter()

                @router.get("/workflows/v1/result/{workflow_id}")  # type: ignore
                async def get_result(workflow_id: str):
                    path = os.path.join(outputs_dir, workflow_id, "output.txt")
                    if os.path.exists(path):
                        try:
                            with open(path, "r", encoding="utf-8") as f:
                                return PlainTextResponse(f.read())
                        except Exception:
                            return PlainTextResponse("Failed to read results.", status_code=500)
                    return PlainTextResponse("Results not ready.", status_code=404)

                @router.get("/workflows/v1/latest-output")  # type: ignore
                async def latest_output():
                    base = outputs_dir
                    if not os.path.exists(base):
                        return JSONResponse({}, status_code=404)
                    try:
                        candidates = []
                        for name in os.listdir(base):
                            p = os.path.join(base, name)
                            if os.path.isdir(p):
                                txt = os.path.join(p, "output.txt")
                                jsn = os.path.join(p, "output.json")
                                latest_mtime = None
                                if os.path.exists(txt):
                                    latest_mtime = os.path.getmtime(txt)
                                if os.path.exists(jsn):
                                    m = os.path.getmtime(jsn)
                                    latest_mtime = max(latest_mtime, m) if latest_mtime else m
                                if latest_mtime is not None:
                                    candidates.append((name, latest_mtime))
                        if not candidates:
                            return JSONResponse({}, status_code=404)
                        candidates.sort(key=lambda x: x[1], reverse=True)
                        return JSONResponse({"workflow_id": candidates[0][0]})
                    except Exception:
                        return JSONResponse({}, status_code=500)

                @router.get("/workflows/v1/summary/{workflow_id}")  # type: ignore
                async def get_summary(workflow_id: str):
                    path = os.path.join(outputs_dir, workflow_id, "summary.json")
                    if os.path.exists(path):
                        try:
                            with open(path, "r", encoding="utf-8") as f:
                                import json
                                return JSONResponse(json.load(f))
                        except Exception:
                            return JSONResponse({}, status_code=500)
                    return JSONResponse({}, status_code=404)

                @router.get("/workflows/v1/result-json/{workflow_id}")  # type: ignore
                async def get_result_json(workflow_id: str):
                    path = os.path.join(outputs_dir, workflow_id, "output.json")
                    if os.path.exists(path):
                        try:
                            with open(path, "r", encoding="utf-8") as f:
                                text = f.read()
                                import json
                                try:
                                    return JSONResponse(json.loads(text))
                                except Exception:
                                    # Attempt simple repair by appending a closing brace
                                    try:
                                        repaired = text.rstrip() + "\n}\n"
                                        return JSONResponse(json.loads(repaired))
                                    except Exception:
                                        # Return raw content so frontend can at least render text
                                        return Response(content=text, media_type="application/json")
                        except Exception:
                            return JSONResponse({}, status_code=500)
                    return JSONResponse({}, status_code=404)

                # Excel download endpoint removed for now

                # --- Lineage diagram generation via Groq (Mermaid) ---
                @router.post("/workflows/v1/lineage-mermaid/{workflow_id}")  # type: ignore
                async def lineage_mermaid(
                    workflow_id: str,
                    model: str = Body(default="llama-3.1-8b-instant"),
                    max_chars: int = Body(default=120000),
                    candidates: list[str] | None = Body(default=None),
                ):
                    import json
                    import re
                    import httpx

                    groq_api_key = os.getenv("GROQ_API_KEY", "").strip()
                    if not groq_api_key:
                        return JSONResponse(
                            {"error": "GROQ_API_KEY not configured on server"},
                            status_code=400,
                        )

                    # Read the human-readable output text
                    path = os.path.join(outputs_dir, workflow_id, "output.txt")
                    if not os.path.exists(path):
                        return JSONResponse({"error": "output.txt not found"}, status_code=404)
                    try:
                        with open(path, "r", encoding="utf-8", errors="ignore") as f:
                            output_text = f.read()
                    except Exception:
                        return JSONResponse({"error": "failed to read output.txt"}, status_code=500)

                    # Truncate to keep request size reasonable
                    if max_chars and len(output_text) > max_chars:
                        output_text = output_text[:max_chars]

                    system_prompt = (
                        "You are an expert data lineage summarizer. Given a human-readable export "
                        "of database metadata that includes sections for DATABASE, SCHEMA, TABLE, COLUMN, INDEX, "
                        "QUALITY_METRIC, RELATIONSHIP (foreign keys at column level) and VIEW_DEPENDENCY, "
                        "produce a super simple Mermaid flowchart that captures only the most important lineage edges.\n\n"
                        "Keep-It-Simple constraints:\n"
                        "- Show at most 20 edges and 20 nodes in total.\n"
                        "- Include ONLY FK (column->column) and view dependencies (table->view).\n"
                        "- No styling, titles, comments, subgraphs, classes, links, or annotations.\n"
                        "- Prefer short labels: schema.table or table; use column only when needed for clarity.\n"
                        "- Avoid duplicates; skip minor or repetitive edges.\n"
                        "- Layout MUST be left-to-right: flowchart LR.\n\n"
                        "Output formatting:\n"
                        "- Wrap the diagram EXACTLY as:\n"
                        "<MERMAID>\nflowchart LR\n...\n</MERMAID>\n"
                        "- Return exactly one block; do NOT include the literal <MERMAID> tags inside the code body."
                    )

                    user_prompt = (
                        "Build a lineage diagram from the following export text. "
                        "Include FK edges (src_column -> dst_column) and view dependencies (table -> view).\n\n"
                        "EXPORT_TEXT_BEGIN\n" + output_text + "\nEXPORT_TEXT_END\n"
                    )

                    messages = [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ]
                    model_list = candidates or [model]
                    content = ""
                    used_model = None
                    try:
                        async with httpx.AsyncClient(timeout=60) as client:
                            last_err_text = None
                            last_status = None
                            for m in model_list:
                                resp = await client.post(
                                    "https://api.groq.com/openai/v1/chat/completions",
                                    headers={
                                        "Authorization": f"Bearer {groq_api_key}",
                                        "Content-Type": "application/json",
                                    },
                                    json={"model": m, "temperature": 0.2, "max_tokens": 800, "messages": messages},
                                )
                                if resp.status_code == 200:
                                    data = resp.json()
                                    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                                    used_model = m
                                    break
                                # Retry on rate limit or server error
                                last_err_text = resp.text
                                last_status = resp.status_code
                                if resp.status_code not in (429, 500, 502, 503):
                                    break
                            else:
                                return JSONResponse({"error": f"groq error {last_status}", "details": last_err_text}, status_code=502)
                    except Exception as e:
                        return JSONResponse({"error": "request failed", "details": str(e)}, status_code=502)

                    # Extract and sanitize Mermaid code (flowchart)
                    def _sanitize_lineage(text: str) -> str:
                        code = text or ""
                        # Prefer explicit markers
                        m = re.search(r"<MERMAID>\s*([\s\S]*?)\s*</MERMAID>", code, re.IGNORECASE)
                        if m:
                            code = m.group(1)
                        # Strip markers/fences and normalize whitespace
                        code = re.sub(r"</?MERMAID>", "", code, flags=re.IGNORECASE)
                        code = re.sub(r"```(?:mermaid)?", "", code, flags=re.IGNORECASE)
                        code = code.replace("\r", "\n")
                        # Focus from first flowchart
                        fm = re.search(r"\bflowchart\b", code, re.IGNORECASE)
                        if fm:
                            code = code[fm.start():]
                        # Collect edges and rebuild compact version
                        import re as _re
                        edge_pat = _re.compile(r"([A-Za-z0-9_./:-]+)\s*[-.]{1,3}>\s*([A-Za-z0-9_./:-]+)")
                        edges: list[tuple[str, str]] = []
                        seen: set[tuple[str, str]] = set()
                        flat = " ".join(line.strip() for line in code.splitlines())
                        for a, b in edge_pat.findall(flat):
                            if not a or not b or a == b:
                                continue
                            key = (a, b)
                            if key in seen:
                                continue
                            seen.add(key)
                            edges.append((a, b))
                            if len(edges) >= 20:
                                break
                        header = "flowchart LR"
                        if not edges:
                            return header
                        out = [header]
                        out.extend(f"{a} --> {b}" for a, b in edges)
                        return "\n".join(out)

                    mermaid_code = _sanitize_lineage(content or "")

                    return JSONResponse({
                        "workflow_id": workflow_id,
                        "model": used_model or model,
                        "mermaid": mermaid_code,
                    })

                # --- AI Insight: metadata-specific summary (plain text) ---
                @router.post("/workflows/v1/ai-summary/{workflow_id}")  # type: ignore
                async def ai_summary(
                    workflow_id: str,
                    model: str = Body(default="gemma2-9b-it"),
                    max_chars: int = Body(default=160000),
                    candidates: list[str] | None = Body(default=None),
                ):
                    import httpx
                    import json
                    import re

                    groq_api_key = os.getenv("GROQ_API_KEY", "").strip()
                    if not groq_api_key:
                        return JSONResponse({"error": "GROQ_API_KEY not configured on server"}, status_code=400)

                    # Prefer structured JSON if available
                    json_path = os.path.join(outputs_dir, workflow_id, "output.json")
                    txt_path = os.path.join(outputs_dir, workflow_id, "output.txt")

                    output_json = None
                    if os.path.exists(json_path):
                        try:
                            with open(json_path, "r", encoding="utf-8") as f:
                                output_json = json.load(f)
                        except Exception:
                            output_json = None

                    output_text = ""
                    if os.path.exists(txt_path):
                        try:
                            with open(txt_path, "r", encoding="utf-8", errors="ignore") as f:
                                output_text = f.read()
                        except Exception:
                            output_text = ""
                    if not output_json and not output_text:
                        return JSONResponse({"error": "no outputs found"}, status_code=404)

                    # Build a compact ASSETS context from structured JSON (preferred)
                    def safe_list(key: str) -> list:
                        v = []
                        if isinstance(output_json, dict) and key in output_json and isinstance(output_json[key], list):
                            v = output_json[key]
                        return v

                    assets_lines: list[str] = []
                    if output_json:
                        # Collect top tables
                        tables = []
                        for it in safe_list("table")[:30]:
                            sch = it.get("table_schema") or it.get("schema_name")
                            tbl = it.get("table_name")
                            if sch and tbl:
                                tables.append(f"{sch}.{tbl}")
                        if tables:
                            assets_lines.append("Tables: " + ", ".join(sorted(set(tables))[:20]))

                        # Relationships (edges)
                        rels = []
                        for it in safe_list("relationship")[:60]:
                            sc = it.get("src_schema_name") or it.get("table_schema")
                            st = it.get("src_table_name") or it.get("table_name")
                            scc = it.get("src_column_name")
                            dc = it.get("dst_schema_name")
                            dt = it.get("dst_table_name")
                            dcc = it.get("dst_column_name")
                            if sc and st and scc and dc and dt and dcc:
                                rels.append(f"{sc}.{st}.{scc} -> {dc}.{dt}.{dcc}")
                        if rels:
                            assets_lines.append("FKs: " + ", ".join(rels[:20]))

                        # View deps (table -> view)
                        vdeps = []
                        for it in safe_list("view_dependency")[:40]:
                            sc = it.get("src_schema_name")
                            st = it.get("src_table_name")
                            dc = it.get("dst_schema_name")
                            dt = it.get("dst_table_name")
                            if sc and st and dc and dt:
                                vdeps.append(f"{sc}.{st} -> {dc}.{dt}")
                        if vdeps:
                            assets_lines.append("Views: " + ", ".join(vdeps[:15]))

                        # Quality metrics
                        qms = []
                        for it in safe_list("quality_metric")[:40]:
                            sc = it.get("schema_name")
                            st = it.get("table_name")
                            col = it.get("column_name")
                            nf = it.get("null_frac")
                            nd = it.get("distinct_count_estimated") or it.get("n_distinct_raw")
                            if sc and st and col:
                                metric = f"{sc}.{st}.{col}"
                                extras = []
                                if nf is not None:
                                    try:
                                        extras.append(f"null%~{round(float(nf)*100,1)}")
                                    except Exception:
                                        pass
                                if nd is not None:
                                    extras.append(f"distinct~{nd}")
                                if extras:
                                    metric += " (" + ", ".join(map(str, extras[:2])) + ")"
                                qms.append(metric)
                        if qms:
                            assets_lines.append("Quality: " + ", ".join(qms[:15]))

                        # Indexes
                        idxs = []
                        for it in safe_list("index")[:40]:
                            sc = it.get("schema_name")
                            st = it.get("table_name")
                            ix = it.get("index_name")
                            if sc and st and ix:
                                idxs.append(f"{sc}.{st}:{ix}")
                        if idxs:
                            assets_lines.append("Indexes: " + ", ".join(idxs[:15]))

                    if not assets_lines and output_text:
                        # Fallback: heuristics over text sections
                        tbls = re.findall(r"^\s*(?:===\s*TABLES?\s*===|TABLE)\s*|^(?!===).*?\b([a-zA-Z0-9_]+)\s*$", output_text, re.MULTILINE)
                        if tbls:
                            assets_lines.append("Tables: " + ", ".join(list(dict.fromkeys(tbls))[:20]))

                    # Build a dataset-specific prompt
                    ASSETS = "\n".join(assets_lines[:6])
                    system_prompt = (
                        "You are a senior data engineer and AI practitioner. "
                        "Write a dataset-specific, practical summary of how THIS metadata can be used. "
                        "Refer to the given ASSETS by name (tables, schemas, relations, metrics). Do not invent names.\n\n"
                        "Output format (strict):\n"
                        "<INSIGHT>\n"
                        "- bullet 1\n"
                        "- bullet 2\n"
                        "...\n"
                        "</INSIGHT>\n\n"
                        "Rules:\n"
                        "- 6–10 bullets max, each 'thing -> use' (e.g., schema.table -> feature sourcing for model X).\n"
                        "- Mention at least 3 concrete assets from ASSETS (e.g., schemas/tables/relations) and one quality metric/index.\n"
                        "- Cover discovery/governance, lineage impact analysis, feature & label sourcing, data quality & drift monitoring, PII/compliance, troubleshooting.\n"
                        "- Plain text bullets only; no code blocks, headers, or links.\n"
                        "- Keep under ~1200 characters."
                    )

                    user_prompt = (
                        "ASSETS:\n" + (ASSETS or "(no structured list)") + "\n\n"
                        "If needed, you may also use the raw export below to ground references.\n\n"
                        + ("EXPORT_TEXT_BEGIN\n" + (output_text[:max_chars] if output_text else "") + "\nEXPORT_TEXT_END\n")
                    )

                    messages = [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ]
                    model_list = candidates or [model]
                    used_model = None
                    content = ""
                    try:
                        async with httpx.AsyncClient(timeout=60) as client:
                            last_err_text = None
                            last_status = None
                            for m in model_list:
                                resp = await client.post(
                                    "https://api.groq.com/openai/v1/chat/completions",
                                    headers={
                                        "Authorization": f"Bearer {groq_api_key}",
                                        "Content-Type": "application/json",
                                    },
                                    json={"model": m, "temperature": 0.2, "max_tokens": 700, "messages": messages},
                                )
                                if resp.status_code == 200:
                                    data = resp.json()
                                    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                                    used_model = m
                                    break
                                last_err_text = resp.text
                                last_status = resp.status_code
                                if resp.status_code not in (429, 500, 502, 503):
                                    break
                            else:
                                return JSONResponse({"error": f"groq error {last_status}", "details": last_err_text}, status_code=502)
                    except Exception as e:
                        return JSONResponse({"error": "request failed", "details": str(e)}, status_code=502)

                    # Extract between <INSIGHT> markers; sanitize to bullets only
                    summary = content or ""
                    m = re.search(r"<INSIGHT>\s*([\s\S]*?)\s*</INSIGHT>", summary, re.IGNORECASE)
                    if m:
                        summary = m.group(1)
                    # Keep only lines starting with '-' and trim
                    lines = []
                    for ln in summary.splitlines():
                        ln2 = ln.strip()
                        if ln2.startswith("- "):
                            lines.append(ln2)
                    if not lines:
                        # fallback: take first 10 non-empty lines
                        for ln in summary.splitlines():
                            ln2 = ln.strip()
                            if ln2:
                                lines.append("- " + ln2)
                            if len(lines) >= 10:
                                break
                    summary_out = "\n".join(lines[:10])
                    if len(summary_out) > 1400:
                        summary_out = summary_out[:1400].rstrip() + "…"

                    return JSONResponse({
                        "workflow_id": workflow_id,
                        "model": used_model or model,
                        "summary": summary_out,
                    })

                # --- ER diagram generation via Groq (Mermaid erDiagram) ---
                @router.post("/workflows/v1/er-mermaid/{workflow_id}")  # type: ignore
                async def er_mermaid(
                    workflow_id: str,
                    model: str = Body(default="llama-3.1-8b-instant"),
                    max_chars: int = Body(default=120000),
                    candidates: list[str] | None = Body(default=None),
                    detail: str = Body(default="rich"),
                ):
                    import json
                    import re
                    import httpx

                    groq_api_key = os.getenv("GROQ_API_KEY", "").strip()
                    if not groq_api_key:
                        return JSONResponse(
                            {"error": "GROQ_API_KEY not configured on server"},
                            status_code=400,
                        )

                    # Read the human-readable output text
                    path = os.path.join(outputs_dir, workflow_id, "output.txt")
                    if not os.path.exists(path):
                        return JSONResponse({"error": "output.txt not found"}, status_code=404)
                    try:
                        with open(path, "r", encoding="utf-8", errors="ignore") as f:
                            output_text = f.read()
                    except Exception:
                        return JSONResponse({"error": "failed to read output.txt"}, status_code=500)

                    # Truncate to keep request size reasonable
                    if max_chars and len(output_text) > max_chars:
                        output_text = output_text[:max_chars]

                    system_prompt = (
                        "You are an expert data modeler. Given a human-readable export of database metadata "
                        "that includes sections for DATABASE, SCHEMA, TABLE, COLUMN, and RELATIONSHIP (foreign keys), "
                        "produce a super simple Mermaid ER diagram that captures only the most important tables and relationships.\n\n"
                        "Keep-It-Simple constraints:\n"
                        "- Show at most 8 tables.\n"
                        "- For each table, list only primary key and foreign key columns (omit other columns).\n"
                        "- No styling, titles, comments, subgraphs, classes, links, or annotations.\n"
                        "- Use standard Mermaid ER syntax only (erDiagram).\n"
                        "- Relationship format example: TABLE_A ||--o{ TABLE_B : \"fk_name\".\n\n"
                        "Output formatting:\n"
                        "- Wrap the diagram EXACTLY as:\n"
                        "<MERMAID>\n"
                        "erDiagram\n"
                        "...\n"
                        "</MERMAID>\n"
                        "- Return exactly one block; do NOT include the literal <MERMAID> tags inside the code body."
                    )

                    user_prompt = (
                        "Build a compact ER diagram from the following export text.\n\n"
                        "EXPORT_TEXT_BEGIN\n" + output_text + "\nEXPORT_TEXT_END\n"
                    )

                    messages = [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ]
                    model_list = candidates or [model]
                    content = ""
                    used_model = None
                    try:
                        async with httpx.AsyncClient(timeout=60) as client:
                            last_err_text = None
                            last_status = None
                            for m in model_list:
                                resp = await client.post(
                                    "https://api.groq.com/openai/v1/chat/completions",
                                    headers={
                                        "Authorization": f"Bearer {groq_api_key}",
                                        "Content-Type": "application/json",
                                    },
                                    json={"model": m, "temperature": 0.2, "max_tokens": 800, "messages": messages},
                                )
                                if resp.status_code == 200:
                                    data = resp.json()
                                    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                                    used_model = m
                                    break
                                last_err_text = resp.text
                                last_status = resp.status_code
                                if resp.status_code not in (429, 500, 502, 503):
                                    break
                            else:
                                return JSONResponse({"error": f"groq error {last_status}", "details": last_err_text}, status_code=502)
                    except Exception as e:
                        return JSONResponse({"error": "request failed", "details": str(e)}, status_code=502)

                    # Extract and sanitize Mermaid ER code
                    def _sanitize_er(text: str, detail_level: str) -> str:
                        code = text or ""
                        # Extract inside markers if present
                        m = re.search(r"<MERMAID>\s*([\s\S]*?)\s*</MERMAID>", code, re.IGNORECASE)
                        if m:
                            code = m.group(1)
                        # Strip leftover markers / fences
                        code = re.sub(r"</?MERMAID>", "", code, flags=re.IGNORECASE)
                        code = re.sub(r"```(?:mermaid)?", "", code, flags=re.IGNORECASE)
                        # Normalize unicode dashes and quotes, collapse whitespace
                        trans = str.maketrans({"–": "-", "—": "-", "−": "-", "“": '"', "”": '"', "’": "'"})
                        code = code.translate(trans)
                        code = re.sub(r"\s+", " ", code)
                        # Focus from first erDiagram forward
                        em = re.search(r"\berDiagram\b", code, re.IGNORECASE)
                        if em:
                            code = code[em.start():]
                        code = re.sub(r"^(?:\s*erDiagram\s*)+", "erDiagram\n", code.strip(), flags=re.IGNORECASE)

                        # Extract relationships and rebuild a compact diagram
                        rel_pat = re.compile(
                            r"(?i)\b([A-Za-z][A-Za-z0-9_]{0,127})\b\s*"  # left entity
                            r"([|o}{]{0,2}[-.]{2,3}[|o}{]{0,2})\s*"        # cardinality token (hyphens/dots)
                            r"\b([A-Za-z][A-Za-z0-9_]{0,127})\b"           # right entity
                        )
                        pairs: list[tuple[str, str, str]] = []
                        seen: set[tuple[str, str]] = set()
                        max_pairs = 8 if detail_level == "minimal" else (20 if detail_level == "standard" else 40)
                        for match in rel_pat.finditer(code):
                            left, token, right = match.groups()
                            if not left or not right:
                                continue
                            if left.upper() == right.upper():
                                continue
                            key = tuple(sorted((left.upper(), right.upper())))
                            if key in seen:
                                continue
                            seen.add(key)
                            norm = token.replace("–", "-").replace("—", "-").replace("−", "-")
                            if not norm:
                                norm = "||--o{"
                            pairs.append((left, norm, right))
                            if len(pairs) >= max_pairs:
                                break

                        if not pairs:
                            # last-resort minimal stub to avoid parser error
                            return "erDiagram\nA ||--o{ B : rel"

                        out_lines = ["erDiagram"]
                        for left, token, right in pairs:
                            out_lines.append(f"{left} {token} {right} : rel")
                        return "\n".join(out_lines)

                    mermaid_code = _sanitize_er(content or "", detail)

                    return JSONResponse({
                        "workflow_id": workflow_id,
                        "model": used_model or model,
                        "mermaid": mermaid_code,
                    })

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
