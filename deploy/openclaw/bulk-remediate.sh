#!/usr/bin/env bash
# Bulk remediation: launches OpenClaw agent in batches until all unattempted
# error sources are exhausted. Automatically skips sources already tried
# (the fetch-errors.sh script reads agent-audit.log to filter them out).
#
# Handles Gemini API rate limits with exponential backoff.
# Detects success by checking whether the audit log grew (not by grepping
# for rate-limit strings, which can appear even in successful batches).
#
# Usage: nohup bash deploy/openclaw/bulk-remediate.sh &

set -uo pipefail
export PATH="/home/josh/.npm-global/bin:$PATH"
source /home/josh/.config/utility-tariff.env 2>/dev/null || true

API_BASE="http://127.0.0.1:8000"
API_KEY="342c07bb5a17f4547fea2006e1da04d2cc7b19c692b66ab6a8a299b614f470af"
BATCH_SIZE=5
LOG_DIR="/home/josh/utility-tariff-finder/logs"
BULK_LOG="${LOG_DIR}/bulk-remediate.log"
AUDIT_LOG="${LOG_DIR}/agent-audit.log"
SKILL_DIR="/home/josh/.openclaw/skills/tariff-remediation"
BATCH_NUM=0
CONSECUTIVE_EMPTY=0
CONSECUTIVE_FAILURES=0
BATCH_DELAY=10

mkdir -p "$LOG_DIR"

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) $*" | tee -a "$BULK_LOG"; }

get_error_count() {
  curl -sS -H "X-Admin-Key: $API_KEY" "${API_BASE}/api/admin/monitoring/stats" 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('errors',0))" 2>/dev/null || echo "0"
}

count_unattempted() {
  export UTILITY_TARIFF_API_BASE="$API_BASE"
  export UTILITY_TARIFF_ADMIN_KEY="$API_KEY"
  RESULT=$("$SKILL_DIR/fetch-errors.sh" "$BATCH_SIZE" 2>/dev/null)
  echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d))" 2>/dev/null || echo "0"
}

audit_line_count() {
  wc -l < "$AUDIT_LOG" 2>/dev/null || echo "0"
}

log "============================================="
log "=== Bulk remediation started (v5 — Claude Haiku) ==="
log "Batch size: $BATCH_SIZE"
log "============================================="

while true; do
  BATCH_NUM=$((BATCH_NUM + 1))
  ERRORS=$(get_error_count)
  UNATTEMPTED=$(count_unattempted)

  log "--- Batch $BATCH_NUM | Total errors: $ERRORS | Unattempted: $UNATTEMPTED ---"

  if [ "$ERRORS" -le 0 ] 2>/dev/null; then
    log "No more errors! Done."
    break
  fi

  if [ "$UNATTEMPTED" -le 0 ] 2>/dev/null; then
    CONSECUTIVE_EMPTY=$((CONSECUTIVE_EMPTY + 1))
    if [ "$CONSECUTIVE_EMPTY" -ge 3 ]; then
      log "No unattempted sources remaining after 3 checks. All $ERRORS remaining errors have been tried. Done."
      break
    fi
    log "No unattempted sources found (check $CONSECUTIVE_EMPTY/3). Sleeping 60s before recheck..."
    sleep 60
    continue
  fi
  CONSECUTIVE_EMPTY=0

  BATCH_LOG="${LOG_DIR}/openclaw-batch-${BATCH_NUM}.log"
  BATCH_START=$(date +%s)
  AUDIT_BEFORE=$(audit_line_count)

  log "Launching OpenClaw agent for batch $BATCH_NUM ($UNATTEMPTED sources available)..."

  timeout 900 openclaw agent --agent main \
    --message "Use the tariff-remediation skill. Process sources ONE AT A TIME using this exact loop:

1. Run: source /home/josh/.config/utility-tariff.env
2. Run: /home/josh/.openclaw/skills/tariff-remediation/fetch-errors.sh $BATCH_SIZE
   (Do NOT call the API directly — the script handles deduplication)
3. For EACH source in the result, do these steps IN ORDER:
   a. Call web_search for the utility name + electric rates/tariff
   b. If found: run patch-source.sh to fix it
   c. IMMEDIATELY log to audit file:
      echo \"\$(date -u +%Y-%m-%dT%H:%M:%SZ) | source_id=<ID> | action=<fixed|skipped> | old_url=<OLD> | new_url=<NEW_OR_NONE> | reason=<BRIEF>\" >> /home/josh/utility-tariff-finder/logs/agent-audit.log
   d. Move to the next source
4. After all sources: print SUMMARY: fixed=N skipped=N

CRITICAL: Log EACH source IMMEDIATELY after processing it. Do not batch logs at the end." \
    --local \
    2>&1 | tee "$BATCH_LOG"

  EXIT_CODE=$?
  BATCH_END=$(date +%s)
  BATCH_DURATION=$((BATCH_END - BATCH_START))
  AUDIT_AFTER=$(audit_line_count)
  NEW_ENTRIES=$((AUDIT_AFTER - AUDIT_BEFORE))

  log "Batch $BATCH_NUM finished in ${BATCH_DURATION}s (exit=$EXIT_CODE). New audit entries: $NEW_ENTRIES"

  if [ "$NEW_ENTRIES" -gt 0 ]; then
    CONSECUTIVE_FAILURES=0
    FIXED_THIS=$(tail -"$NEW_ENTRIES" "$AUDIT_LOG" | grep -c 'action=fixed' || echo "0")
    SKIPPED_THIS=$(tail -"$NEW_ENTRIES" "$AUDIT_LOG" | grep -c 'action=skipped' || echo "0")
    log "Batch $BATCH_NUM SUCCESS: fixed=$FIXED_THIS skipped=$SKIPPED_THIS"
    log "Sleeping ${BATCH_DELAY}s before next batch..."
    sleep "$BATCH_DELAY"
  else
    CONSECUTIVE_FAILURES=$((CONSECUTIVE_FAILURES + 1))
    BACKOFF=$((120 * CONSECUTIVE_FAILURES))
    if [ "$BACKOFF" -gt 600 ]; then BACKOFF=600; fi
    log "Batch $BATCH_NUM FAILED (no audit entries). Streak: $CONSECUTIVE_FAILURES. Backing off ${BACKOFF}s..."
    if [ "$CONSECUTIVE_FAILURES" -ge 10 ]; then
      log "10 consecutive failures. Stopping — likely a persistent API issue."
      break
    fi
    sleep "$BACKOFF"
  fi
done

FINAL_ERRORS=$(get_error_count)
TOTAL_FIXED=$(grep -c 'action=fixed' "$AUDIT_LOG" 2>/dev/null || echo "0")
TOTAL_SKIPPED=$(grep -c 'action=skipped' "$AUDIT_LOG" 2>/dev/null || echo "0")
TOTAL_ATTEMPTED=$(grep -oP 'source_id=\d+' "$AUDIT_LOG" | sort -u | wc -l)

log "============================================="
log "=== Bulk remediation complete ==="
log "Errors remaining: $FINAL_ERRORS"
log "Total fixed: $TOTAL_FIXED | Total skipped: $TOTAL_SKIPPED"
log "Unique sources attempted: $TOTAL_ATTEMPTED"
log "============================================="
