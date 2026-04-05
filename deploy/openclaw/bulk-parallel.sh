#!/usr/bin/env bash
# Launch multiple parallel bulk remediation workers.
# Each worker is an independent loop that fetches unattempted errors,
# runs OpenClaw, and logs results. They share the same audit log for
# deduplication, so they won't re-attempt the same sources.
#
# Usage: nohup bash deploy/openclaw/bulk-parallel.sh [NUM_WORKERS] &

set -uo pipefail
export PATH="/home/josh/.npm-global/bin:$PATH"
source /home/josh/.config/utility-tariff.env 2>/dev/null || true

NUM_WORKERS="${1:-3}"
API_BASE="http://127.0.0.1:8000"
API_KEY="342c07bb5a17f4547fea2006e1da04d2cc7b19c692b66ab6a8a299b614f470af"
LOG_DIR="/home/josh/utility-tariff-finder/logs"
AUDIT_LOG="${LOG_DIR}/agent-audit.log"
SKILL_DIR="/home/josh/.openclaw/skills/tariff-remediation"
MAIN_LOG="${LOG_DIR}/bulk-parallel.log"

mkdir -p "$LOG_DIR"

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" | tee -a "$MAIN_LOG"; }

get_error_count() {
  curl -sS -H "X-Admin-Key: $API_KEY" "${API_BASE}/api/admin/monitoring/stats" 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('errors',0))" 2>/dev/null || echo "0"
}

run_worker() {
  local WORKER_ID="$1"
  local WORKER_LOG="${LOG_DIR}/worker-${WORKER_ID}.log"
  local BATCH=0
  local CONSECUTIVE_FAILURES=0

  log "[W${WORKER_ID}] Started"

  while true; do
    BATCH=$((BATCH + 1))

    export UTILITY_TARIFF_API_BASE="$API_BASE"
    export UTILITY_TARIFF_ADMIN_KEY="$API_KEY"
    UNATTEMPTED=$("$SKILL_DIR/fetch-errors.sh" 10 2>/dev/null \
      | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d))" 2>/dev/null || echo "0")

    if [ "$UNATTEMPTED" -le 0 ] 2>/dev/null; then
      log "[W${WORKER_ID}] No unattempted sources left. Exiting."
      break
    fi

    AUDIT_BEFORE=$(wc -l < "$AUDIT_LOG" 2>/dev/null || echo "0")
    BATCH_LOG="${LOG_DIR}/worker-${WORKER_ID}-batch-${BATCH}.log"

    timeout 900 openclaw agent --agent main \
      --message "Use the tariff-remediation skill to fix errored monitoring sources.

RULES:
1. Run fetch-errors.sh first. It returns ONLY unattempted sources.
2. For EVERY source, you MUST call web_search. No exceptions.
3. Do NOT guess URLs. Every fix must come from a web_search result.
4. If web_search returns nothing useful after 2 queries, SKIP the source.
5. Log EVERY action to the audit log.

Environment:
  source /home/josh/.config/utility-tariff.env

When done, print: SUMMARY: fixed=N skipped=N" \
      --local \
      2>&1 > "$BATCH_LOG"

    AUDIT_AFTER=$(wc -l < "$AUDIT_LOG" 2>/dev/null || echo "0")
    NEW_ENTRIES=$((AUDIT_AFTER - AUDIT_BEFORE))

    if [ "$NEW_ENTRIES" -gt 0 ]; then
      CONSECUTIVE_FAILURES=0
      FIXED=$(tail -"$NEW_ENTRIES" "$AUDIT_LOG" | grep -c 'action=fixed' || echo "0")
      SKIPPED=$(tail -"$NEW_ENTRIES" "$AUDIT_LOG" | grep -c 'action=skipped' || echo "0")
      log "[W${WORKER_ID}] Batch $BATCH: fixed=$FIXED skipped=$SKIPPED (${NEW_ENTRIES} entries)"
    else
      CONSECUTIVE_FAILURES=$((CONSECUTIVE_FAILURES + 1))
      BACKOFF=$((30 * CONSECUTIVE_FAILURES))
      if [ "$BACKOFF" -gt 180 ]; then BACKOFF=180; fi
      log "[W${WORKER_ID}] Batch $BATCH: FAILED (0 entries). Streak $CONSECUTIVE_FAILURES. Backoff ${BACKOFF}s"
      if [ "$CONSECUTIVE_FAILURES" -ge 10 ]; then
        log "[W${WORKER_ID}] 10 consecutive failures. Exiting."
        break
      fi
      sleep "$BACKOFF"
    fi

    sleep 5
  done

  log "[W${WORKER_ID}] Worker finished."
}

ERRORS=$(get_error_count)
log "============================================="
log "=== Parallel bulk remediation: $NUM_WORKERS workers ==="
log "=== Starting errors: $ERRORS ==="
log "============================================="

PIDS=()
for i in $(seq 1 "$NUM_WORKERS"); do
  run_worker "$i" &
  PIDS+=($!)
  sleep 3
done

log "Workers launched: PIDs=${PIDS[*]}"

for pid in "${PIDS[@]}"; do
  wait "$pid" 2>/dev/null
done

FINAL_ERRORS=$(get_error_count)
TOTAL_FIXED=$(grep -c 'action=fixed' "$AUDIT_LOG" 2>/dev/null || echo "0")
TOTAL_SKIPPED=$(grep -c 'action=skipped' "$AUDIT_LOG" 2>/dev/null || echo "0")
TOTAL_ATTEMPTED=$(grep -oP 'source_id=\d+' "$AUDIT_LOG" | sort -u | wc -l)

log "============================================="
log "=== Parallel remediation complete ==="
log "Errors remaining: $FINAL_ERRORS"
log "Total fixed: $TOTAL_FIXED | Total skipped: $TOTAL_SKIPPED"
log "Unique sources attempted: $TOTAL_ATTEMPTED"
log "============================================="
