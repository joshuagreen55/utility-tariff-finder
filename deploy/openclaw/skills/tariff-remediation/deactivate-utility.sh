#!/usr/bin/env bash
# Deactivate a utility (mark is_active=false).
# Usage: ./deactivate-utility.sh <UTILITY_ID> "<REASON>"
#
# Requires UTILITY_TARIFF_API_BASE and UTILITY_TARIFF_ADMIN_KEY env vars.

set -euo pipefail

UTILITY_ID="${1:?Usage: deactivate-utility.sh <UTILITY_ID> \"<REASON>\"}"
REASON="${2:-no reason given}"
BASE="${UTILITY_TARIFF_API_BASE:?Set UTILITY_TARIFF_API_BASE}"
KEY="${UTILITY_TARIFF_ADMIN_KEY:?Set UTILITY_TARIFF_ADMIN_KEY}"

ENCODED_REASON=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$REASON'))")

RESP=$(curl -sS -w "\n%{http_code}" \
  -X PATCH \
  -H "X-Admin-Key: ${KEY}" \
  "${BASE}/api/admin/monitoring/utilities/${UTILITY_ID}/deactivate?reason=${ENCODED_REASON}")

HTTP_CODE=$(echo "$RESP" | tail -1)
BODY=$(echo "$RESP" | sed '$d')

if [ "$HTTP_CODE" -ne 200 ]; then
  echo "DEACTIVATE FAILED (HTTP ${HTTP_CODE}): ${BODY}"
  exit 1
fi

echo "DEACTIVATED: ${BODY}"
