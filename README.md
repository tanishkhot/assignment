# Postgres Connector — Environment Pinning

This subproject pins Python and Dapr to stable versions for the Atlan Application SDK workflows.

## Requirements

- Python 3.11.x (pinned here via `.python-version` → `3.11.9`)
- Dapr CLI/Runtime 1.13.6
- Temporal dev server (recommended for local testing)

## Set Python 3.11

Using pyenv (recommended):

```bash
pyenv install 3.11.9 # if not already installed
pyenv local 3.11.9   # respects ./postgres/.python-version
python --version     # should show 3.11.x
```

Create and activate a venv (done later when building):

```bash
python -m venv .venv
source .venv/bin/activate
```

## Install Dapr 1.13.6

Using Homebrew on macOS:

```bash
brew uninstall dapr-cli || true
brew install dapr/tap/dapr@1.13
brew link --overwrite dapr@1.13
dapr uninstall --all
dapr init --version 1.13.6
dapr --version  # CLI/Runtime 1.13.x
```

Or install CLI via script:

```bash
curl -L https://raw.githubusercontent.com/dapr/cli/v1.13.6/install/install.sh | /bin/bash
dapr uninstall --all
dapr init --version 1.13.6
```

## Temporal (optional for local runs)

```bash
brew install temporal
temporal server start-dev
```

Once these are in place, proceed to scaffolding the app.

## Quick start (2 commands)

From the `postgres/` folder:

1) Start dependencies (Dapr sidecar + Temporal dev server):

```bash
uv run poe start-deps
```

2) Start the application server:

```bash
uv run main.py
```

Notes:
- The app uses Dapr for object/state store via the components in `components/`.
- Endpoints are under `http://localhost:3000/workflows/v1` (e.g., `/auth`, `/check`, `/start`).
- Outputs land in `local/dapr/objectstore/...` and the human‑readable export is written to `output/<workflow_id>/output.txt`.

