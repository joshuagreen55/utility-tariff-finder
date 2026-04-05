"""
Automated URL remediation for monitoring sources that errored.

Reads logs/monitoring-summary.json (from run_monitoring --all), attempts
deterministic fixes (HTTP→HTTPS, follow redirects, strip fragments,
try common tariff page paths on the utility domain), PATCHes the API
with corrected URLs, and re-checks only fixed sources.

Usage (inside the API container):
    python -m scripts.remediate_urls --summary /app/logs/monitoring-summary.json
    python -m scripts.remediate_urls --summary /app/logs/monitoring-summary.json --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse

import httpx

API_BASE = "http://127.0.0.1:8000"
ADMIN_KEY: str | None = None

TARIFF_PATH_CANDIDATES = [
    "/rates",
    "/electric-rates",
    "/residential-rates",
    "/tariffs",
    "/schedules",
    "/pricing",
    "/electric/rates",
    "/services/electric-rates",
    "/rates-and-tariffs",
    "/rate-schedules",
]

TIMEOUT = httpx.Timeout(20.0, connect=10.0)
USER_AGENT = "UtilityTariffRemediation/0.1"


def _api_headers() -> dict[str, str]:
    h = {"Content-Type": "application/json"}
    if ADMIN_KEY:
        h["X-Admin-Key"] = ADMIN_KEY
    return h


async def _probe_url(client: httpx.AsyncClient, url: str) -> tuple[bool, str | None]:
    """Check if a URL returns 200. Returns (ok, final_url_or_None)."""
    try:
        resp = await client.get(url, follow_redirects=True)
        if resp.status_code == 200:
            return True, str(resp.url)
        return False, None
    except Exception:
        return False, None


async def _try_fixes(client: httpx.AsyncClient, original_url: str, error_msg: str) -> str | None:
    """Try a series of deterministic URL fixes. Returns a working URL or None."""
    parsed = urlparse(original_url)

    # 1. HTTP → HTTPS
    if parsed.scheme == "http":
        https_url = urlunparse(parsed._replace(scheme="https"))
        ok, final = await _probe_url(client, https_url)
        if ok:
            return final

    # 2. HTTPS → HTTP (some utility sites don't have certs)
    if parsed.scheme == "https":
        http_url = urlunparse(parsed._replace(scheme="http"))
        ok, final = await _probe_url(client, http_url)
        if ok:
            return final

    # 3. Strip fragment and query, try base path
    if parsed.query or parsed.fragment:
        clean = urlunparse(parsed._replace(query="", fragment=""))
        ok, final = await _probe_url(client, clean)
        if ok:
            return final

    # 4. www. toggle
    netloc = parsed.netloc
    if netloc.startswith("www."):
        alt_netloc = netloc[4:]
    else:
        alt_netloc = "www." + netloc
    for scheme in ("https", "http"):
        alt = urlunparse(parsed._replace(scheme=scheme, netloc=alt_netloc))
        ok, final = await _probe_url(client, alt)
        if ok:
            return final

    # 5. Try site root (the page might have moved)
    root = f"{parsed.scheme}://{parsed.netloc}/"
    ok, final = await _probe_url(client, root)
    if ok and final and final.rstrip("/") != root.rstrip("/"):
        return final

    # 6. Try common tariff paths on the same domain
    for path in TARIFF_PATH_CANDIDATES:
        for scheme in ("https", "http"):
            candidate = f"{scheme}://{parsed.netloc}{path}"
            ok, final = await _probe_url(client, candidate)
            if ok:
                return final

    return None


async def _patch_source_url(client: httpx.AsyncClient, source_id: int, new_url: str) -> bool:
    try:
        resp = await client.patch(
            f"{API_BASE}/api/admin/monitoring/sources/{source_id}",
            json={"url": new_url},
            headers=_api_headers(),
            timeout=15.0,
        )
        return resp.status_code == 200
    except Exception:
        return False


async def _recheck_sources(client: httpx.AsyncClient, source_ids: list[int]) -> dict | None:
    if not source_ids:
        return None
    try:
        resp = await client.post(
            f"{API_BASE}/api/admin/monitoring/sources/check-ids?wait=true",
            json={"source_ids": source_ids},
            headers=_api_headers(),
            timeout=300.0,
        )
        if resp.status_code == 200:
            return resp.json()
        return None
    except Exception:
        return None


async def run(summary_path: str, *, dry_run: bool = False, max_errors: int = 0) -> dict:
    with open(summary_path) as f:
        summary = json.load(f)

    errors = summary.get("error_details", [])
    if not errors:
        print("No errors in summary — nothing to remediate.")
        return {"fixed": 0, "skipped": 0, "total_errors": 0}

    if max_errors > 0:
        errors = errors[:max_errors]

    print(f"Attempting remediation for {len(errors)} errored sources...")
    fixed: list[dict] = []
    skipped: list[dict] = []

    async with httpx.AsyncClient(
        timeout=TIMEOUT,
        headers={"User-Agent": USER_AGENT},
        follow_redirects=False,
    ) as probe_client:
        for i, err in enumerate(errors):
            sid = err.get("source_id")
            url = err.get("url", "")
            msg = err.get("message", "")

            print(f"  [{i+1}/{len(errors)}] source {sid}: {url[:80]}...")

            new_url = await _try_fixes(probe_client, url, msg)

            if new_url and new_url.rstrip("/") != url.rstrip("/"):
                entry = {
                    "source_id": sid,
                    "old_url": url,
                    "new_url": new_url,
                    "error_was": msg,
                }
                if dry_run:
                    print(f"    DRY-RUN fix → {new_url}")
                    fixed.append(entry)
                else:
                    async with httpx.AsyncClient() as api_client:
                        ok = await _patch_source_url(api_client, sid, new_url)
                    if ok:
                        print(f"    PATCHED → {new_url}")
                        fixed.append(entry)
                    else:
                        print(f"    PATCH failed for source {sid}")
                        skipped.append({"source_id": sid, "url": url, "reason": "PATCH failed"})
            else:
                skipped.append({"source_id": sid, "url": url, "reason": "no working alternative found"})

    recheck_result = None
    if fixed and not dry_run:
        print(f"\nRe-checking {len(fixed)} fixed sources...")
        fixed_ids = [f["source_id"] for f in fixed]
        async with httpx.AsyncClient() as api_client:
            recheck_result = await _recheck_sources(api_client, fixed_ids)
        if recheck_result:
            rc = recheck_result.get("counts", {})
            print(f"  Re-check: {rc.get('unchanged',0)} ok, {rc.get('errors',0)} still erroring")

    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "summary_file": summary_path,
        "total_errors_processed": len(errors),
        "fixed": len(fixed),
        "skipped": len(skipped),
        "dry_run": dry_run,
        "fixes": fixed,
        "recheck": recheck_result,
    }

    print(f"\nDone: {len(fixed)} fixed, {len(skipped)} skipped out of {len(errors)} errors.")
    return result


def main():
    parser = argparse.ArgumentParser(description="Remediate errored monitoring source URLs")
    parser.add_argument("--summary", required=True, help="Path to monitoring-summary.json")
    parser.add_argument("--dry-run", action="store_true", help="Probe URLs but don't PATCH")
    parser.add_argument("--max-errors", type=int, default=0, help="Limit errors to process (0 = all)")
    parser.add_argument("--api-key", type=str, default=None, help="Admin API key (or reads ADMIN_API_KEY env)")
    parser.add_argument("--output", type=str, default=None, help="Write remediation report JSON")
    args = parser.parse_args()

    import os
    global ADMIN_KEY
    ADMIN_KEY = args.api_key or os.environ.get("ADMIN_API_KEY", "")

    result = asyncio.run(run(args.summary, dry_run=args.dry_run, max_errors=args.max_errors))

    if args.output:
        with open(args.output, "w") as f:
            json.dump(result, f, indent=2)
        print(f"Report written to {args.output}")


if __name__ == "__main__":
    main()
