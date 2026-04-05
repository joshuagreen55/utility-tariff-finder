#!/usr/bin/env bash
# Fetch utilities where ALL monitoring sources are in error status.
# Usage: ./fetch-dead-utilities.sh [LIMIT]
#
# Requires UTILITY_TARIFF_API_BASE and UTILITY_TARIFF_ADMIN_KEY env vars.

set -euo pipefail

LIMIT="${1:-30}"
BASE="${UTILITY_TARIFF_API_BASE:?Set UTILITY_TARIFF_API_BASE}"
KEY="${UTILITY_TARIFF_ADMIN_KEY:?Set UTILITY_TARIFF_ADMIN_KEY}"

curl -sS \
  -H "X-Admin-Key: ${KEY}" \
  "${BASE}/api/admin/monitoring/dead-utilities?limit=${LIMIT}" \
  | python3 -m json.tool
