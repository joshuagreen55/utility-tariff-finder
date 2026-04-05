#!/usr/bin/env bash
# Fetch monitoring sources with status=error from the admin API,
# excluding sources already attempted (logged in agent-audit.log).
#
# Paginates through API results until LIMIT unattempted sources are found.
#
# Usage: ./fetch-errors.sh [LIMIT]
#
# Requires UTILITY_TARIFF_API_BASE and UTILITY_TARIFF_ADMIN_KEY env vars.

set -euo pipefail

LIMIT="${1:-10}"
BASE="${UTILITY_TARIFF_API_BASE:?Set UTILITY_TARIFF_API_BASE}"
KEY="${UTILITY_TARIFF_ADMIN_KEY:?Set UTILITY_TARIFF_ADMIN_KEY}"
AUDIT_LOG="/home/josh/utility-tariff-finder/logs/agent-audit.log"

if [ -f "$AUDIT_LOG" ]; then
  ATTEMPTED=$(grep -oP 'source_id=\K\d+' "$AUDIT_LOG" | sort -u | tr '\n' ',' | sed 's/,$//')
else
  ATTEMPTED=""
fi

python3 -c "
import sys, json, urllib.request

base = '${BASE}'
key = '${KEY}'
attempted_str = '${ATTEMPTED}'
wanted = ${LIMIT}

attempted = set()
if attempted_str:
    attempted = {int(x) for x in attempted_str.split(',')}

collected = []
offset = 0
page_size = 200

while len(collected) < wanted:
    url = f'{base}/api/admin/monitoring/sources?status=error&limit={page_size}&offset={offset}'
    req = urllib.request.Request(url, headers={'X-Admin-Key': key})
    with urllib.request.urlopen(req) as resp:
        page = json.loads(resp.read())
    if not page:
        break
    for s in page:
        if s['id'] not in attempted:
            collected.append(s)
            if len(collected) >= wanted:
                break
    offset += page_size

print(json.dumps(collected, indent=2))
print(f'# {len(collected)} unattempted sources (skipped {len(attempted)} already attempted, scanned offset={offset})', file=sys.stderr)
"
