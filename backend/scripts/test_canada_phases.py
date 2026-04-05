"""
Phased Canada tariff pipeline testing.

Phase 1: NL only (2 utilities)
Phase 2: Atlantic Canada — NL, NS, NB, PE (5 utilities)
Phase 3: All Canada (33 utilities) — writes to DB if --commit

Usage:
    python -m scripts.test_canada_phases --phase 1
    python -m scripts.test_canada_phases --phase 2
    python -m scripts.test_canada_phases --phase 3
    python -m scripts.test_canada_phases --phase 3 --commit   # writes to DB
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

from sqlalchemy import select, func
from sqlalchemy.orm import Session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("canada_test")

PHASE_PROVINCES = {
    1: ["NL"],
    2: ["NL", "NS", "NB", "PE"],
    3: ["NL", "NS", "NB", "PE", "ON", "QC", "MB", "SK", "AB", "BC", "YT", "NT", "NU"],
}

PHASE_NAMES = {
    1: "Newfoundland & Labrador",
    2: "Atlantic Canada",
    3: "All Canada",
}


def get_utilities(provinces: list[str]) -> list[dict]:
    from app.db.session import get_sync_engine
    from app.models import Utility

    engine = get_sync_engine()
    with Session(engine) as s:
        utils = s.execute(
            select(Utility).where(
                Utility.country == "CA",
                Utility.state_province.in_(provinces),
                Utility.is_active == True,
            ).order_by(Utility.state_province, Utility.name)
        ).scalars().all()

        return [
            {
                "id": u.id,
                "name": u.name,
                "state": u.state_province,
                "country": "CA",
                "website_url": u.website_url,
            }
            for u in utils
        ]


def run_phase(phase: int, commit: bool = False):
    from scripts.tariff_pipeline import run_pipeline

    provinces = PHASE_PROVINCES[phase]
    phase_name = PHASE_NAMES[phase]
    dry_run = not commit

    log.info(f"{'=' * 60}")
    log.info(f"PHASE {phase}: {phase_name}")
    log.info(f"Provinces: {', '.join(provinces)}")
    log.info(f"Mode: {'COMMIT (write to DB)' if commit else 'DRY RUN (no DB writes)'}")
    log.info(f"{'=' * 60}")

    utilities = get_utilities(provinces)
    log.info(f"Found {len(utilities)} utilities to process")
    print()

    results = []
    start = time.time()

    for i, util in enumerate(utilities, 1):
        log.info(f"[{i}/{len(utilities)}] {util['name']} ({util['state']})")
        try:
            result = run_pipeline(util["id"], dry_run=dry_run)
            tariff_count = result.phase4_validation.get("valid", 0)
            source = result.phase4_validation.get("source", "pipeline")
            errors = result.errors

            results.append({
                "utility_id": util["id"],
                "name": util["name"],
                "province": util["state"],
                "tariffs_found": tariff_count,
                "source": source,
                "rate_page": result.phase1_rate_page_url,
                "errors": errors,
                "status": "OK" if tariff_count > 0 else ("ERROR" if errors else "NO_TARIFFS"),
            })
        except Exception as e:
            log.error(f"  Pipeline crashed: {e}")
            results.append({
                "utility_id": util["id"],
                "name": util["name"],
                "province": util["state"],
                "tariffs_found": 0,
                "source": "error",
                "rate_page": "",
                "errors": [str(e)],
                "status": "CRASH",
            })

    elapsed = time.time() - start

    # Summary
    print()
    print("=" * 80)
    print(f"PHASE {phase} RESULTS: {phase_name}")
    print(f"Mode: {'COMMITTED' if commit else 'DRY RUN'} | Time: {elapsed:.0f}s")
    print("=" * 80)
    print()
    print(f"{'Province':<5} {'Utility':<40} {'Tariffs':<10} {'Source':<15} {'Status'}")
    print("-" * 85)

    by_province = {}
    total_tariffs = 0
    total_ok = 0

    for r in results:
        prov = r["province"]
        status_icon = {
            "OK": "OK",
            "NO_TARIFFS": "NO DATA",
            "ERROR": "FAILED",
            "CRASH": "CRASH",
        }.get(r["status"], r["status"])

        err_msg = ""
        if r["errors"]:
            err_msg = f" -- {r['errors'][0][:40]}"

        print(f"{prov:<5} {r['name'][:39]:<40} {r['tariffs_found']:<10} {r['source'][:14]:<15} {status_icon}{err_msg}")

        if prov not in by_province:
            by_province[prov] = {"ok": 0, "fail": 0, "tariffs": 0}
        if r["status"] == "OK":
            by_province[prov]["ok"] += 1
            total_ok += 1
        else:
            by_province[prov]["fail"] += 1
        by_province[prov]["tariffs"] += r["tariffs_found"]
        total_tariffs += r["tariffs_found"]

    print("-" * 85)
    print()
    print("PROVINCE SUMMARY:")
    for prov in sorted(by_province.keys()):
        d = by_province[prov]
        total = d["ok"] + d["fail"]
        print(f"  {prov}: {d['ok']}/{total} utilities OK, {d['tariffs']} tariffs")

    print()
    pct = (total_ok / len(results) * 100) if results else 0
    print(f"TOTAL: {total_ok}/{len(results)} utilities OK ({pct:.0f}%), {total_tariffs} tariffs found")
    if not commit:
        print()
        print("This was a DRY RUN. No data was written to the database.")
        print("To commit results, re-run with --commit")

    # Write results JSON — use /app/logs in Docker, fallback to ./logs locally
    log_dir = "/app/logs" if os.path.isdir("/app/logs") else "logs"
    os.makedirs(log_dir, exist_ok=True)
    output_file = f"{log_dir}/canada-phase{phase}-{'commit' if commit else 'dryrun'}.json"
    with open(output_file, "w") as f:
        json.dump({
            "phase": phase,
            "phase_name": phase_name,
            "committed": commit,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "elapsed_seconds": round(elapsed, 1),
            "summary": {
                "total_utilities": len(results),
                "utilities_ok": total_ok,
                "total_tariffs": total_tariffs,
            },
            "by_province": by_province,
            "results": results,
        }, f, indent=2)
    log.info(f"Results written to {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Phased Canada tariff testing")
    parser.add_argument("--phase", type=int, required=True, choices=[1, 2, 3])
    parser.add_argument("--commit", action="store_true", help="Write results to database (default is dry run)")
    args = parser.parse_args()

    run_phase(args.phase, commit=args.commit)


if __name__ == "__main__":
    main()
