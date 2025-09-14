<div align="center">

# SourceSense — Postgres Metadata Extraction (Atlan SDK)

Explore and export Postgres metadata with a clean, reliable developer experience. Built on the Atlan Application SDK (Temporal + Dapr).

</div>

## Why This Repo

This is a focused assignment implementation for a backend engineering role. It showcases:

- A small, robust service that extracts Postgres metadata (db/schema/table/column) and lineage (FK + view dependencies)
- A pragmatic UI for auth, filters, preflight checks, and a Results view with JSON/Text toggle
- Thoughtful reliability hardening (atomic outputs, resilient summary, consistent endpoints)


## Checklist

- [x] Connection form with URL parser & password toggle  
- [x] Include/exclude filters with cross-validation  
- [x] Preflight checks (db/schema/tables/version)  
- [x] Extraction (db/schema/table/column)  
- [x] Lineage (FK + view dependencies)  
- [x] Outputs: text + JSON + summary  
- [x] Results page with JSON/Text toggle  
- [x] AI-generated ER/lineage diagrams (Experimental)
- [x] Metadata metrics

## At a Glance

- Language/runtime: Python 3.11 (pinned via `.python-version` → 3.11.9)
- Orchestration: Temporal (dev server for local runs)
- Sidecar runtime: Dapr CLI/Runtime 1.13.6
- Frontend: Static HTML/CSS/JS served by FastAPI wrapper in the SDK server
- Outputs: human‑readable text + structured JSON under `output/<workflow_id>/`

## Quick Start (macOS)

1) Copy environment defaults and ensure Python 3.11 is active

```bash
cp .env.example .env  # Paste in the Groq API key in .env (if this is not done, app will be avialable on 0.0.0.0:8000)
python --version   # should be 3.11.x

```

2) Install/prepare dependencies (optional if you already have them)

```bash
# Dapr 1.13.6 (For macOS, other versions might work)
brew uninstall dapr-cli || true
brew install dapr/tap/dapr@1.13
brew link --overwrite dapr@1.13
dapr uninstall --all
dapr init --version 1.13.6

# Temporal dev server
brew install temporal || true
```

3) Start the local sidecars (Dapr + Temporal) in one terminal

```bash
uv run poe download-components    # fetch Dapr component yamls compatible with the SDK (one-time)
uv run poe start-deps             # starts Dapr + Temporal dev server
```

4) Start the application in another terminal

```bash
uv run main.py
```

Open the UI at: http://localhost:3000/
(If .env is not copied):
Open UI atL 0.0.0.0:8000/

Endpoints live under: http://localhost:3000/workflows/v1

## Features

- Clean onboarding: connection form, URL parser, password toggle
- Smart filters: include/exclude DB+schemas with cross‑validation
- Preflight checks: database/schema/tables/version, checks them all before heavy lifting
- Extraction: database, schema, table, column, relationships, index, quality metrics
- Lineage: foreign keys + view dependencies
- Outputs: unified text export, JSON export, resilient summary (AI generated)
- Results page: JSON/Text toggle, “latest output” discovery, summary panel 
- AI diagrams: one‑click Mermaid lineage + ER diagrams (Groq), model selector with fallbacks

## System Design: Simplified

```mermaid
flowchart LR
  User[Engineer] --> UI["Frontend (static)"]
  UI -->|HTTP| API[FastAPI Server]
  API -->|Start/Signal| Temporal[Temporal Worker]
  Temporal -->|Activities| PG["(Postgres)"]
  Temporal -->|ObjectStore| DaprObj["(Dapr Objectstore)"]
  Temporal -->|StateStore| DaprState["(Dapr Statestore)"]
  API -->|Serve files| Outputs[/output/<workflow_id>/*/]
  UI -->|Poll| API
```

### Detialed System & Control Flow

```mermaid
graph TD
    subgraph "Your Computer"
        subgraph "Browser (Client-Side)"
            A[Frontend UI localhost:8000]
        end

        subgraph "Backend Processes"
            B["Python Web Server"]
            C["Dapr Sidecar"]
            D["Dapr State Store"]
            E["Temporal Worker"]
            F["Temporal Server localhost:8233"]
            H["Local Storage (output/<workflow_id>)"]
        end
    end

    subgraph "External Service"
        G["PostgreSQL DB"]
    end

    %% --- Flows ---
    A -- "1. User enters credentials & clicks 'Run'" --> B
    B -- "2. Stores credentials securely" --> C
    C -- "3. Writes to a file" --> D
    C -- "4. Returns credential_guid" --> B
    B -- "5. Sends 'credential_guid' to Frontend" --> A
    A -- "6. Sends 'credential_guid' to start workflow" --> B
    B -- "7. Tells Temporal to start the work" --> F
    F -- "8. Assigns work to an available worker" --> E
    E -- "9. Asks Dapr for credentials using 'credential_guid'" --> C
    C -- "10. Reads credentials from file" --> D
    D -- "11. Returns credentials" --> C
    C -- "12. Gives credentials to worker" --> E
    E -- "13. Connects to DB to extract metadata" --> G
    E -- "14. Writes results" --> H
    H -- "15. Results served via API" --> B
    B -- "16. UI fetches results (JSON/Text)" --> A
```
### Swimlane (Sequence) Diagram:

```mermaid
sequenceDiagram
  participant U as User
  participant UI as Frontend
  participant API as FastAPI
  participant TW as Temporal Worker
  participant PG as Postgres
  participant OS as ObjectStore

  U->>UI: Enter credentials + filters
  UI->>API: POST /workflows/v1/check
  API->>TW: Preflight activities
  TW->>PG: Tables/version checks
  TW-->>API: Preflight result

  UI->>API: POST /workflows/v1/start
  API->>TW: Start workflow
  TW->>PG: Fetch metadata (db/schema/table/column)
  TW->>PG: Fetch lineage (FK + view deps)
  TW->>OS: Write raw + transformed outputs
  TW-->>API: Summarize + export

  UI->>API: GET /workflows/v1/latest-output
  UI->>API: GET /workflows/v1/result-json/{id}
  API-->>UI: JSON/Text output + Summary
```

### API Surface

| Method | Path                                   | Purpose                              |
|-------:|----------------------------------------|--------------------------------------|
| POST   | /workflows/v1/auth                     | Validate credentials                 |
| POST   | /workflows/v1/metadata                 | List databases/schemas for filters   |
| POST   | /workflows/v1/check                    | Preflight checks                     |
| POST   | /workflows/v1/start                    | Start extraction workflow            |
| GET    | /workflows/v1/latest-output            | Discover latest workflow with output |
| GET    | /workflows/v1/result/{workflow_id}     | Text output                          |
| GET    | /workflows/v1/result-json/{workflow_id}| JSON output                          |
| GET    | /workflows/v1/summary/{workflow_id}    | Summary JSON                         |

## How It Works

- `main.py` starts the Temporal worker + SDK server and adds a thin FastAPI layer for results and static `/output` serving
- `app/workflows.py` orchestrates preflight → fetch → transform → summarize → export (text + JSON)
- `app/activities.py` implements Postgres‑specific queries and transformations, writes atomic outputs, and saves a `summary.json`
- SQL lives in `app/sql/` for portability and clarity
- Dapr components in `components/` give a local object store and statestore

## Endpoints

- `POST /workflows/v1/auth` — validate credentials
- `POST /workflows/v1/metadata` — fetch db/schema list (for include/exclude UI)
- `POST /workflows/v1/check` — preflight checks
- `POST /workflows/v1/start` — start a workflow
- `GET /workflows/v1/latest-output` — discover latest workflow id with results
- `GET /workflows/v1/result/{workflow_id}` — text output
- `GET /workflows/v1/result-json/{workflow_id}` — JSON output
- `GET /workflows/v1/summary/{workflow_id}` — summary JSON

## Configuration

Copy defaults and edit as needed:

```bash
cp .env.example .env
```

Include/Exclude filters accept a JSON object of regex patterns per database with regex arrays of schemas, for example:

```json
{
  "^neondb$": ["^public$", "^raw$", "^staging$", "^ref$", "^mart$"]
}
```

The app writes outputs locally under `output/<workflow_id>/`:

- `output.txt` — human‑readable, combined sections for each type
- `summary.json` — counts per type and convenient paths
- `output.json` — consolidated structured data for the UI JSON view

## Development

- Python: 3.11.x only (repo sets `.python-version` to 3.11.9)
- Dapr: 1.13.6 (explicitly linked via Homebrew commands above)
- Temporal: dev server is started by `poe start-deps`
- Task runner: `poethepoet` via `uv run poe ...`

Useful commands:

```bash
uv run poe download-components   # fetch Dapr component yamls
uv run poe start-deps            # start Dapr + Temporal
uv run main.py                   # run the app server
uv run poe stop-deps             # kill common ports if needed
```

## Credentials Helper (Reviewer UX)

- On the first screen there’s a button “Get the credentials” - it redirects to a Google Doc.
- Values: a Google Doc with the following sections:
  - Groq: API key (bearer token) - To be pasted in .env
  - NeonDB/Postgres: host, port, database, username, password, sslmode=require (For the main assignment)

This button only opens the document; nothing is read automatically from it.

## Groq Setup (LLM)

- Set your Groq key in the server environment:

```bash
echo 'GROQ_API_KEY=your_key_here' >> .env
```

- Start the app normally. If the key is not set, diagram endpoints will return `400 GROQ_API_KEY not configured`.

Optional frontend tweaks:
- Model list (ordered fallbacks) can be edited in `frontend/static/index.html` under `window.env.LLM_MODELS`.
  - Default: `gemma2-9b-it,llama-3.1-8b-instant`.

## Generating Diagrams (Mermaid)

1) Run an extraction (auth → connection → filters → Run) and go to Results.
2) In “Results” actions, choose a model in the “Model” dropdown (defaults provided).
3) Click:
   - “Generate Lineage Diagram” for FK + view‑dependency flowchart, or
   - “Generate ER Diagram” for a compact ER view.
4) A diagram panel appears below Results. Use “Copy Mermaid” to copy the source.

Notes:
- The UI renders Mermaid client‑side.
- "Warning: AI‑generated diagrams are experimental.”

## How Diagrams Are Built (Server‑side)

- Lineage endpoint: `POST /workflows/v1/lineage-mermaid/{workflow_id}`
  - Reads `output/<id>/output.txt`, prompts Groq to produce a simple flowchart.
  - Server sanitizes and rebuilds a compact `flowchart LR` with up to 20 edges.
- ER endpoint: `POST /workflows/v1/er-mermaid/{workflow_id}`
  - Reads `output/<id>/output.txt`, prompts Groq for Mermaid `erDiagram`.
  - Server sanitizes and rebuilds a compact `erDiagram` with up to 8 relations.
- Model selection and fallback:
  - Frontend sends `{model, candidates}`.
  - Server automatically retries candidates on 429/5xx until success.

## Reviewer Flow (End‑to‑End)

1) Open the UI and click “Get the credentials” to retrieve Groq + NeonDB creds.
2) Fill Connection (host/port/db/user/pass); keep SSL mode `require` for Neon.
3) Test Connection → Next → pick a connection name.
4) Choose include/exclude filters → Preflight “Check” → “Run”.
5) Go to Results → switch JSON/Text view if needed.
6) Choose a model → “Generate Lineage Diagram” or “Generate ER Diagram”.

## Troubleshooting (Diagrams)

- “GROQ_API_KEY not configured”: set it in `.env` and restart.
- “Rate limit exceeded”: pick a different model in the dropdown; server also retries fallbacks automatically.
- “Mermaid parse error”: server sanitizes aggressively; re‑click Generate. If it persists, copy the Mermaid from the panel and share; we’ll extend the sanitizer.
- Empty diagram: ensure an extraction finished and `output/<id>/output.txt` exists; wait a few seconds and click “Try Again”.

## Troubleshooting

- “Results not ready” — wait a few seconds; the Results page polls and tries `/latest-output` and `/result-json/{id}` as fallbacks. There is front-end side polling which polls every 5 seconds, so you can try waiting.
- Empty sections — verify include filters match your database/schemas
- Port conflicts — use `uv run poe stop-deps`
- JSON parse errors — the app writes outputs atomically and the result endpoint repairs transient partial reads (should be rare)

## License

Apache License 2.0 — see `LICENSE`.
