#!/usr/bin/env bash
# Run ON THE VM from the project root (same folder as docker-compose.yml).
# Writes logs/monitoring-summary.json on the host (./logs is mounted into the API container).

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

mkdir -p logs

echo "Starting full monitoring baseline (this can take 30–90+ minutes)..."
docker compose exec -T api python -m scripts.run_monitoring \
  --all \
  --concurrency 32 \
  --per-host 4 \
  --output-summary /app/logs/monitoring-summary.json

echo ""
echo "Done. Summary on the VM:"
echo "  $ROOT/logs/monitoring-summary.json"
