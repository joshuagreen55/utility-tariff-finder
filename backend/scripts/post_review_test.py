"""
Post-review validation test (parallelized).

1. Re-runs the pipeline on the 6 LG&E-corrupted utilities (sequential).
2. Runs the pipeline on 100 randomly selected active utilities using
   concurrent workers to validate the post-review fixes.

Usage:
    python -m scripts.post_review_test
    python -m scripts.post_review_test --dry-run
    python -m scripts.post_review_test --workers 4 --count 100
"""

import argparse
import json
import logging
import os
import random
import sys
import time
from concurrent.futures import as_completed
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("post_review_test")

CORRUPTED_IDS = [50, 768, 906, 1228, 1571, 1799]


def _run_one(uid: int, dry_run: bool = False, force: bool = False) -> dict:
    """Run pipeline for a single utility in a worker process."""
    import time as _time
    from scripts.tariff_pipeline import cleanup_between_utilities, run_pipeline

    t0 = _time.time()
    try:
        result = run_pipeline(uid, dry_run=dry_run, force_extract=force)
        valid = (result.phase4_validation or {}).get("valid", 0)
        elapsed = _time.time() - t0
        return {
            "utility_id": uid,
            "utility_name": result.utility_name,
            "state": result.state,
            "tariffs_found": valid,
            "errors": result.errors,
            "success": valid > 0,
            "elapsed_sec": round(elapsed, 1),
        }
    except Exception as e:
        elapsed = _time.time() - t0
        return {
            "utility_id": uid,
            "utility_name": f"#{uid}",
            "state": "",
            "tariffs_found": 0,
            "errors": [str(e)],
            "success": False,
            "elapsed_sec": round(elapsed, 1),
        }
    finally:
        cleanup_between_utilities()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-corrupted", action="store_true",
                        help="Skip re-running the 6 corrupted utilities")
    parser.add_argument("--count", type=int, default=100,
                        help="Number of random utilities to test (default 100)")
    parser.add_argument("--workers", type=int, default=4,
                        help="Number of parallel workers (default 4)")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility")
    args = parser.parse_args()

    from sqlalchemy import select
    from sqlalchemy.orm import Session
    from app.db.session import get_sync_engine
    from app.models import Utility

    engine = get_sync_engine()

    # Phase A: Fix 6 corrupted utilities (sequential — small batch)
    if not args.skip_corrupted:
        log.info("=" * 60)
        log.info("PHASE A: Re-running 6 LG&E-corrupted utilities")
        log.info("=" * 60)
        for uid in CORRUPTED_IDS:
            log.info(f"\n--- Corrupted utility {uid} ---")
            entry = _run_one(uid, dry_run=args.dry_run, force=True)
            log.info(
                f"  {entry['utility_name']} ({entry['state']}): "
                f"{entry['tariffs_found']} tariffs in {entry['elapsed_sec']}s"
            )
            if entry["errors"]:
                log.warning(f"  Errors: {entry['errors']}")

    # Phase B: 100 random active utilities (parallel)
    log.info("\n" + "=" * 60)
    log.info(f"PHASE B: Running pipeline on {args.count} utilities with {args.workers} workers")
    log.info("=" * 60)

    with Session(engine) as session:
        all_active_ids = list(
            session.execute(
                select(Utility.id)
                .where(Utility.is_active.is_(True))
                .where(Utility.id.notin_(CORRUPTED_IDS))
            ).scalars().all()
        )

    random.seed(args.seed)
    test_ids = sorted(random.sample(all_active_ids, min(args.count, len(all_active_ids))))
    log.info(f"Selected {len(test_ids)} utilities from {len(all_active_ids)} active")

    results = []
    start_time = time.time()

    for i, uid in enumerate(test_ids, 1):
        entry = _run_one(uid, dry_run=args.dry_run)
        results.append(entry)
        status = "OK" if entry["success"] else "FAIL"
        log.info(
            f"  [{i}/{len(test_ids)}] {status} "
            f"{entry['utility_name']} ({entry['state']}): "
            f"{entry['tariffs_found']} tariffs in {entry['elapsed_sec']}s"
        )
        if entry["errors"]:
            log.warning(f"    Errors: {entry['errors'][:2]}")

    total_elapsed = time.time() - start_time
    successes = sum(1 for r in results if r["success"])
    total_tariffs = sum(r["tariffs_found"] for r in results)
    failures = [r for r in results if not r["success"]]

    log.info("\n" + "=" * 60)
    log.info("SUMMARY")
    log.info("=" * 60)
    log.info(f"  Utilities tested: {len(results)}")
    log.info(f"  Successes: {successes} ({successes/max(len(results),1)*100:.1f}%)")
    log.info(f"  Failures: {len(failures)}")
    log.info(f"  Total tariffs found: {total_tariffs}")
    log.info(f"  Total wall time: {total_elapsed/60:.1f} minutes")
    log.info(f"  Avg time per utility: {total_elapsed/max(len(results),1):.1f}s")

    if failures:
        log.info("\nFailed utilities:")
        for f in sorted(failures, key=lambda x: x["utility_id"]):
            log.info(f"  {f['utility_id']} {f['utility_name']}: {f['errors'][:1]}")

    report_path = os.path.join(
        os.environ.get("APP_LOG_DIR", "/app/logs"),
        f"post_review_test_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json",
    )
    report = {
        "test_date": datetime.now(timezone.utc).isoformat(),
        "count": len(results),
        "workers": args.workers,
        "success_rate": round(successes / max(len(results), 1) * 100, 1),
        "total_tariffs": total_tariffs,
        "elapsed_minutes": round(total_elapsed / 60, 1),
        "corrupted_rerun": not args.skip_corrupted,
        "seed": args.seed,
        "results": sorted(results, key=lambda r: r["utility_id"]),
    }
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w") as fh:
        json.dump(report, fh, indent=2)
    log.info(f"\nReport saved to {report_path}")


if __name__ == "__main__":
    main()
