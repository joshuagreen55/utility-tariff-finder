#!/usr/bin/env bash
# Weekly monitoring: baseline check + deterministic remediation + AI agent remediation.
# Run ON THE VM from the project root (same folder as docker-compose.yml).
#
# What it does:
#   1. Checks all 8k+ monitoring source URLs (30-90 min)
#   2. Attempts deterministic URL fixes for any errors (HTTP/HTTPS swap, redirects, common paths)
#   3. Runs OpenClaw AI agent to research and fix remaining errors via web search
#   4. Writes timestamped logs to ./logs/
#
# Usage:
#   ./deploy/weekly-monitoring.sh              # full run
#   ./deploy/weekly-monitoring.sh --dry-run    # probe but don't PATCH (skips OpenClaw step)
#   ./deploy/weekly-monitoring.sh --no-agent   # skip the OpenClaw agent step

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

mkdir -p logs
TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)
SUMMARY="logs/monitoring-summary.json"
REMEDIATION_REPORT="logs/remediation-${TIMESTAMP}.json"
DRY_RUN_FLAG=""
SKIP_AGENT=false

for arg in "$@"; do
    case "$arg" in
        --dry-run)  DRY_RUN_FLAG="--dry-run" ;;
        --no-agent) SKIP_AGENT=true ;;
    esac
done

if [ -n "$DRY_RUN_FLAG" ]; then
    echo "=== DRY-RUN MODE ==="
fi

echo "[$TIMESTAMP] Starting weekly monitoring..."
echo ""

echo "--- Step 1: Full baseline check ---"
docker compose exec -T api python -m scripts.run_monitoring \
    --all \
    --concurrency 32 \
    --per-host 4 \
    --output-summary /app/logs/monitoring-summary.json

echo ""
echo "--- Step 2: Deterministic URL remediation ---"
docker compose exec -T api python -m scripts.remediate_urls \
    --summary /app/logs/monitoring-summary.json \
    --output /app/logs/remediation-${TIMESTAMP}.json \
    $DRY_RUN_FLAG

echo ""
echo "--- Step 3: OpenClaw AI agent remediation ---"
if [ "$SKIP_AGENT" = true ] || [ -n "$DRY_RUN_FLAG" ]; then
    echo "Skipping OpenClaw agent (--dry-run or --no-agent)."
else
    if command -v openclaw &>/dev/null; then
        echo "Invoking OpenClaw agent to fix remaining errors..."
        openclaw agent \
            --message "Use the tariff-remediation skill. Fetch up to 50 errored monitoring sources and try to find working replacement URLs using web search. Log every action (fixed or skipped) to logs/agent-audit.log. Stop after 50 sources or if you have been running for more than 30 minutes." \
            --local \
            2>&1 | tee "logs/openclaw-${TIMESTAMP}.log" || echo "OpenClaw agent exited with an error — see logs/openclaw-${TIMESTAMP}.log"
    else
        echo "OpenClaw not installed — skipping AI agent step."
        echo "Install with: bash deploy/openclaw/setup-vm.sh"
    fi
fi

DONE_TS=$(date -u +%Y%m%dT%H%M%SZ)
echo ""
echo "[$DONE_TS] Weekly monitoring complete."
echo "  Summary:      $ROOT/$SUMMARY"
echo "  Remediation:  $ROOT/$REMEDIATION_REPORT"
echo "  Agent log:    $ROOT/logs/openclaw-${TIMESTAMP}.log (if agent ran)"
echo "  Audit trail:  $ROOT/logs/agent-audit.log"
