#!/usr/bin/env bash
set -euo pipefail

# Ensure data dir exists for dapr components
mkdir -p /data/objectstore /data/eventstore

# Start daprd sidecar
daprd \
  --app-id app \
  --app-port ${ATLAN_APP_HTTP_PORT:-3000} \
  --dapr-http-port 3500 \
  --dapr-grpc-port 50001 \
  --resources-path /components \
  --log-level info &

# Give sidecar a moment
sleep 1

# Start application
exec python /app/main.py
