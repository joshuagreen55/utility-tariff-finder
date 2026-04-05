#!/usr/bin/env bash
# Monthly monitoring: baseline check + OpenClaw AI agent remediation.
# Run ON THE VM from the project root (same folder as docker-compose.yml).
#
# What it does:
#   1. Checks all 8k+ monitoring source URLs (60-90 min)
#   2. Runs OpenClaw AI agent to research and fix errors via web search (30-60 min)
#   3. Writes timestamped logs to ./logs/
#
# Usage:
#   ./deploy/monthly-monitoring.sh              # full run
#   ./deploy/monthly-monitoring.sh --no-agent   # baseline only, skip OpenClaw

set -euo pipefail
export PATH="/home/josh/.npm-global/bin:$PATH"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

mkdir -p logs
TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)
SKIP_AGENT=false

for arg in "$@"; do
    case "$arg" in
        --no-agent) SKIP_AGENT=true ;;
    esac
done

echo "[$TIMESTAMP] Starting monthly monitoring..."
echo ""

echo "--- Step 1: Full baseline check ---"
docker compose exec -T api python -m scripts.run_monitoring \
    --all \
    --concurrency 32 \
    --per-host 4 \
    --output-summary /app/logs/monitoring-summary.json

ERRORS=$(curl -sS -H "X-Admin-Key: $(grep '^ADMIN_API_KEY=' .env | cut -d= -f2)" \
    http://127.0.0.1:8000/api/admin/monitoring/stats 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('errors',0))" 2>/dev/null || echo "?")
echo ""
echo "Baseline complete. Errors found: $ERRORS"

echo ""
echo "--- Step 2: OpenClaw AI agent remediation ---"
if [ "$SKIP_AGENT" = true ]; then
    echo "Skipping OpenClaw agent (--no-agent)."
else
    if command -v openclaw &>/dev/null; then
        echo "Invoking OpenClaw agent to fix errors (up to 200 sources)..."
        openclaw agent --agent main \
            --message "Use the tariff-remediation skill. Process up to 200 errored monitoring sources.

CRITICAL: You MUST use web_search for EVERY source. Do NOT guess URLs.

Environment: export UTILITY_TARIFF_API_BASE=http://127.0.0.1:8000 and export UTILITY_TARIFF_ADMIN_KEY=$(grep '^ADMIN_API_KEY=' .env | cut -d= -f2)

Log every action to $ROOT/logs/agent-audit.log. When done, print a summary." \
            --local \
            2>&1 | tee "logs/openclaw-${TIMESTAMP}.log" || echo "OpenClaw agent exited with an error — see logs/openclaw-${TIMESTAMP}.log"
    else
        echo "OpenClaw not installed — skipping AI agent step."
        echo "Install with: bash deploy/openclaw/setup-vm.sh"
    fi
fi

echo ""
echo "--- Step 3: Dead utility cleanup ---"
if [ "$SKIP_AGENT" = true ]; then
    echo "Skipping cleanup (--no-agent)."
else
    if command -v openclaw &>/dev/null; then
        echo "Checking for dead utilities (100% error rate)..."
        openclaw agent --agent main \
            --message "Use the tariff-remediation skill — specifically the Dead Utility Cleanup section. Fetch dead utilities (where ALL monitoring sources are in error status). For each one, use web_search to determine if the utility still exists. If it was merged, dissolved, or absorbed, deactivate it. If it still exists but just has no web presence, skip it.

CRITICAL: You MUST use web_search for EVERY utility. Do NOT guess.

Environment: export UTILITY_TARIFF_API_BASE=http://127.0.0.1:8000 and export UTILITY_TARIFF_ADMIN_KEY=$(grep '^ADMIN_API_KEY=' .env | cut -d= -f2)

Log every action to $ROOT/logs/agent-audit.log. When done, print a summary." \
            --local \
            2>&1 | tee "logs/openclaw-cleanup-${TIMESTAMP}.log" || echo "Cleanup agent exited with an error — see logs/openclaw-cleanup-${TIMESTAMP}.log"
    else
        echo "OpenClaw not installed — skipping cleanup step."
    fi
fi

DONE_TS=$(date -u +%Y%m%dT%H%M%SZ)
FINAL_ERRORS=$(curl -sS -H "X-Admin-Key: $(grep '^ADMIN_API_KEY=' .env | cut -d= -f2)" \
    http://127.0.0.1:8000/api/admin/monitoring/stats 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('errors',0))" 2>/dev/null || echo "?")
echo ""
echo "[$DONE_TS] Monthly monitoring complete."
echo "  Errors before: $ERRORS"
echo "  Errors after:  $FINAL_ERRORS"
echo "  Summary:       $ROOT/logs/monitoring-summary.json"
echo "  Agent log:     $ROOT/logs/openclaw-${TIMESTAMP}.log"
echo "  Cleanup log:   $ROOT/logs/openclaw-cleanup-${TIMESTAMP}.log"
echo "  Audit trail:   $ROOT/logs/agent-audit.log"
