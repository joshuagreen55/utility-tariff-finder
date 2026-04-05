"""Revert monitoring source URLs that were changed to homepage-only URLs by the
OpenClaw bulk run.  Reads the agent audit log to find fixes where new_url is
just a domain root ("/"), then PATCHes them back to their original URL via the
admin API.  Specific-page fixes (deeper paths) are left untouched.

Usage:
    python3 revert_homepage_fixes.py [--dry-run]
"""

import argparse
import json
import os
import sys
import time
import urllib.request
from urllib.parse import urlparse

API = os.environ.get("API_URL", "http://127.0.0.1:8000")
KEY = os.environ.get("ADMIN_API_KEY", "")
AUDIT_LOG = os.environ.get(
    "AUDIT_LOG", "/home/josh/utility-tariff-finder/logs/agent-audit.log"
)


def parse_audit_log():
    """Return list of (source_id, old_url, new_url) for homepage-only fixes."""
    reverts = []
    seen_ids = set()

    with open(AUDIT_LOG) as f:
        for line in f:
            if "action=fixed" not in line:
                continue
            try:
                sid = int(line.split("source_id=")[1].split(" ")[0].split("|")[0].strip())
                old_url = line.split("old_url=")[1].split(" |")[0].split("| new")[0].strip()
                new_url = line.split("new_url=")[1].split(" |")[0].split("|")[0].strip()
            except (IndexError, ValueError):
                continue

            path = urlparse(new_url).path.rstrip("/")
            is_homepage = path == "" or path == "/"
            if not is_homepage:
                continue

            if sid in seen_ids:
                continue
            seen_ids.add(sid)
            reverts.append((sid, old_url, new_url))

    return reverts


def revert_source(source_id: int, old_url: str, dry_run: bool) -> bool:
    if dry_run:
        return True
    body = json.dumps({"url": old_url}).encode()
    url = f"{API}/api/admin/monitoring/sources/{source_id}"
    req = urllib.request.Request(
        url, data=body, method="PATCH",
        headers={"X-Admin-Key": KEY, "Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req)
        return True
    except Exception as e:
        print(f"  ERROR reverting {source_id}: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.dry_run and not KEY:
        sys.exit("ADMIN_API_KEY env var required (or use --dry-run)")

    reverts = parse_audit_log()
    print(f"Found {len(reverts)} homepage-only fixes to revert")
    if args.dry_run:
        print("DRY RUN — no changes will be made")
        for sid, old, new in reverts[:10]:
            print(f"  #{sid}: {new} → {old[:80]}")
        return

    ok = 0
    fail = 0
    for i, (sid, old_url, new_url) in enumerate(reverts):
        if revert_source(sid, old_url, args.dry_run):
            ok += 1
        else:
            fail += 1
        if (i + 1) % 100 == 0:
            print(f"  Progress: {i+1}/{len(reverts)} (ok={ok}, fail={fail})")
            time.sleep(0.5)

    print(f"\nDone. Reverted {ok}, failed {fail} out of {len(reverts)}")


if __name__ == "__main__":
    main()
