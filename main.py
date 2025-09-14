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

                # --- ER diagram generation via Groq (Mermaid erDiagram) ---
                @router.post("/workflows/v1/er-mermaid/{workflow_id}")  # type: ignore
                async def er_mermaid(
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
                    def _sanitize_er(text: str) -> str:
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
                        for match in rel_pat.finditer(code):
                            left, token, right, _label = match.groups()
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
                            if len(pairs) >= 8:  # tighter cap
                                break

                        if not pairs:
                            # last-resort minimal stub to avoid parser error
                            return "erDiagram\nA ||--o{ B"

                        out_lines = ["erDiagram"]
                        for left, token, right in pairs:
                            out_lines.append(f"{left} {token} {right}")
                        return "\n".join(out_lines)

                    mermaid_code = _sanitize_er(content or "")

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
