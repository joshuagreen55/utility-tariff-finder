#!/usr/bin/env bash
# Patch a monitoring source URL and re-check it.
# Usage: ./patch-source.sh <SOURCE_ID> <NEW_URL>
#
# Requires UTILITY_TARIFF_API_BASE and UTILITY_TARIFF_ADMIN_KEY env vars.
# Source /home/josh/.config/utility-tariff.env first.

set -euo pipefail

SOURCE_ID="${1:?Usage: patch-source.sh <SOURCE_ID> <NEW_URL>}"
NEW_URL="${2:?Usage: patch-source.sh <SOURCE_ID> <NEW_URL>}"
BASE="${UTILITY_TARIFF_API_BASE:?Set UTILITY_TARIFF_API_BASE}"
KEY="${UTILITY_TARIFF_ADMIN_KEY:?Set UTILITY_TARIFF_ADMIN_KEY}"

echo "--- Patching source ${SOURCE_ID} → ${NEW_URL}"

PATCH_RESP=$(curl -sS -w "\n%{http_code}" \
  -X PATCH \
  -H "Content-Type: application/json" \
  -H "X-Admin-Key: ${KEY}" \
  -d "{\"url\": \"${NEW_URL}\"}" \
  "${BASE}/api/admin/monitoring/sources/${SOURCE_ID}")

HTTP_CODE=$(echo "$PATCH_RESP" | tail -1)
BODY=$(echo "$PATCH_RESP" | sed '$d')

if [ "$HTTP_CODE" -ne 200 ]; then
  echo "PATCH FAILED (HTTP ${HTTP_CODE}): ${BODY}"
  exit 1
fi

echo "PATCHED — source ${SOURCE_ID} URL updated."

echo "--- Re-checking source ${SOURCE_ID}..."

CHECK_RESP=$(curl -sS -w "\n%{http_code}" \
  -X POST \
  -H "X-Admin-Key: ${KEY}" \
  "${BASE}/api/admin/monitoring/sources/${SOURCE_ID}/check")

CHECK_CODE=$(echo "$CHECK_RESP" | tail -1)
CHECK_BODY=$(echo "$CHECK_RESP" | sed '$d')

if [ "$CHECK_CODE" -ne 200 ]; then
  echo "RE-CHECK FAILED (HTTP ${CHECK_CODE}): ${CHECK_BODY}"
  exit 1
fi

echo "Re-check result: ${CHECK_BODY}"
