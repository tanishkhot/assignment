<div align="center">

# SourceSense — Postgres Metadata Extraction (Atlan SDK)

Explore and export Postgres metadata with a clean, reliable developer experience. Built on the Atlan Application SDK (Temporal + Dapr)!

</div>

## Why This Repo

This is a focused assignment implementation for a backend engineering role. It showcases:

- A small, robust service that extracts Postgres metadata (db/schema/table/column) and lineage (FK + view dependencies)
- A pragmatic UI for auth, filters, preflight checks, and a Results view with JSON/Text toggle
- Thoughtful reliability hardening (atomic outputs, resilient summary, consistent endpoints)

## At a Glance

- Language/runtime: Python 3.11 (pinned via `.python-version` → 3.11.9)
- Orchestration: Temporal (dev server for local runs)
- Sidecar runtime: Dapr CLI/Runtime 1.13.6
- Frontend: Static HTML/CSS/JS served by FastAPI wrapper in the SDK server
- Outputs: human‑readable text + structured JSON under `output/<workflow_id>/`

## Quick Start (macOS)

1) Copy environment defaults and ensure Python 3.11 is active

```bash
cp .env.example .env
python --version   # should be 3.11.x
```

2) Install/prepare dependencies (optional if you already have them)

```bash
# Dapr 1.13.6
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
uv run poe download-components    # fetch Dapr component yamls compatible with the SDK
uv run poe start-deps             # starts Dapr + Temporal dev server
```

4) Start the application in another terminal

```bash
uv run main.py
```

Open the UI at: http://localhost:3000/index.html

Endpoints live under: http://localhost:3000/workflows/v1

## Features

- Clean onboarding: connection form, URL parser, password toggle
- Smart filters: include/exclude DB+schemas with cross‑validation
- Preflight checks: database/schema/tables/version
- Extraction: database, schema, table, column
- Lineage: foreign keys + view dependencies
- Outputs: unified text export, JSON export, resilient summary
- Results page: JSON/Text toggle, “latest output” discovery, summary panel
- AI diagrams: one‑click Mermaid lineage + ER diagrams (Groq), model selector with fallbacks

## System Design

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
<!-- 
If Mermaid doesn’t render in your viewer, here are static diagrams you can expand inline:

![System Architecture (Static)](diagrams/architecture.png)

![End‑to‑End Flow (Static)](diagrams/flowchart.png)

![API Surface (Static)](diagrams/apis.png)
-->
### Detailed Flow (Mermaid, styled)

```mermaid
%%{init:{
  "theme":"default",
  "themeCSS": ".mermaid, svg { background-color:#ffffff !important; }"
}}%%
flowchart LR
  %% --- Lanes ---
  subgraph U["User"]
    U1[Enter credentials & filters]
  end

  subgraph UI["Frontend (static)"]
    UI1[POST /workflows/v1/check]
    UI2[POST /workflows/v1/start]
    UI3[GET /workflows/v1/latest-output]
    UI4[GET /workflows/v1/result-json/]
  end

  subgraph API["FastAPI Server"]
    API1[Preflight request]
    API2[Start workflow]
    API3["Serve outputs (JSON / text)"]
  end

  subgraph TW["Temporal Worker"]
    TW1[Run preflight activities]
    TW2["Fetch metadata (db/schema/table/column)"]
    TW3["Derive lineage (FK + view deps)"]
    TW4[Write outputs]
  end

  subgraph PG["Postgres DB"]
    PG1[(Tables & version checks)]
    PG2[(Metadata & lineage reads)]
  end

  subgraph OS["ObjectStore"]
    OS1[(Raw & transformed outputs)]
  end

  %% --- Flow: Preflight ---
  U1 --> UI1
  UI1 --> API1
  API1 --> TW1
  TW1 --> PG1
  TW1 --> API1R[Preflight result]
  API1R --> UI1

  %% --- Flow: Start & Extract ---
  U1 --> UI2
  UI2 --> API2
  API2 --> TW2
  TW2 --> PG2
  TW2 --> TW3
  TW3 --> PG2
  TW3 --> TW4
  TW4 --> OS1

  %% --- Flow: Read Outputs ---
  UI3 --> API3
  UI4 --> API3
  API3 --> UI3
  API3 --> UI4
```

### Credential & Control Flow

```mermaid
graph TD
    subgraph "Your Computer"
        subgraph "Browser"
            A[Frontend UI <br> localhost:8000]
        end

        subgraph "Backend Processes"
            B["Python Web Server <br>"]
            C["Dapr Sidecar <br> "]
            D["Dapr State Store <br>"]
            E["Temporal Worker <br>"]
            F["Temporal Server<br> localhost:8233"]
        end
    end

    subgraph "External Service"
        G["PostgreSQL DB <br>"]
    end

    A -- "1. User enters credentials & clicks 'Run'" --> B
    B -- "2. Stores credentials securely" --> C
    C -- "3. Writes to a file" --> D
    B -- "4. Returns a 'credential_guid' (claim ticket)" --> A
    A -- "5. Sends 'credential_guid' to start workflow" --> B
    B -- "6. Tells Temporal to start the recipe" --> F
    F -- "7. Assigns recipe to an available worker" --> E
    E -- "8. Asks Dapr for credentials using 'credential_guid'" --> C
    C -- "9. Reads credentials from file" --> D
    D -- "10. Returns credentials" --> C
    C -- "11. Gives credentials to worker" --> E
    E -- "12. Connects to DB to extract metadata" --> G
```

### Workflow Sequence

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
  - Default: `llama-3.1-8b-instant,llama-3.1-70b-versatile,mixtral-8x7b-32768,gemma2-9b-it`.

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

- “Results not ready” — wait a few seconds; the Results page polls and tries `/latest-output` and `/result-json/{id}` as fallbacks
- Empty sections — verify include filters match your database/schemas
- Port conflicts — use `uv run poe stop-deps`
- JSON parse errors — the app writes outputs atomically and the result endpoint repairs transient partial reads (should be rare)

## License

Apache License 2.0 — see `LICENSE`.
