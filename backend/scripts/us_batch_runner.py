"""
US Batch Runner — runs the tariff pipeline for US utilities in regional batches.

Usage:
    python3 -m scripts.us_batch_runner --batch 1 --dry-run
    python3 -m scripts.us_batch_runner --batch 1 --comprehensive
    python3 -m scripts.us_batch_runner --batch 1 --comprehensive --workers 4
    python3 -m scripts.us_batch_runner --list-batches

Each batch targets a geographic region of roughly 80-140 utilities,
similar in size to the full Canada run (~107 utilities).
"""
import argparse
import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import Utility

BATCHES = {
    1:  ("Pacific NW + Islands",  ["WA", "OR", "AK", "HI"]),
    2:  ("California + Nevada",   ["CA", "NV"]),
    3:  ("Mountain West",         ["AZ", "CO", "ID", "MT", "NM", "UT", "WY"]),
    4:  ("Texas",                 ["TX"]),
    5:  ("Northern Plains",       ["MN", "ND", "SD", "NE"]),
    6:  ("Heartland",             ["IA", "KS", "MO", "OK"]),
    7:  ("South Central",         ["AR", "LA", "MS"]),
    8:  ("Deep South",            ["AL", "GA"]),
    9:  ("Southeast",             ["FL", "NC", "SC"]),
    10: ("Upper South",           ["TN", "KY", "WV"]),
    11: ("Great Lakes",           ["WI", "MI"]),
    12: ("Ohio Valley",           ["OH", "IN", "IL"]),
    13: ("Mid-Atlantic",          ["PA", "NJ", "VA", "MD", "DE", "DC"]),
    14: ("New York",              ["NY"]),
    15: ("New England",           ["MA", "CT", "ME", "VT", "NH", "RI"]),
}


def list_batches():
    engine = get_sync_engine()
    with Session(engine) as session:
        print(f"{'#':>2s}  {'Region':<25s}  {'States':<30s}  {'Utilities':>9s}  {'w/Tariffs':>9s}  {'Missing':>7s}")
        print("-" * 90)
        total_utils = 0
        total_with = 0
        total_missing = 0
        for num, (name, states) in sorted(BATCHES.items()):
            placeholders = ",".join(f":s{i}" for i in range(len(states)))
            params = {f"s{i}": s for i, s in enumerate(states)}
            row = session.execute(text(f"""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN tc.cnt > 0 THEN 1 ELSE 0 END) as with_tariffs
                FROM utilities u
                LEFT JOIN (SELECT utility_id, COUNT(*) as cnt FROM tariffs GROUP BY utility_id) tc
                    ON tc.utility_id = u.id
                WHERE u.country = 'US' AND u.is_active = true
                  AND u.state_province IN ({placeholders})
            """), params).fetchone()
            total = row[0]
            with_t = row[1] or 0
            missing = total - with_t
            total_utils += total
            total_with += with_t
            total_missing += missing
            states_str = ", ".join(states)
            print(f"{num:2d}  {name:<25s}  {states_str:<30s}  {total:9d}  {with_t:9d}  {missing:7d}")
        print("-" * 90)
        print(f"    {'TOTAL':<25s}  {'':30s}  {total_utils:9d}  {total_with:9d}  {total_missing:7d}")


def get_batch_utility_ids(batch_num: int) -> list[int]:
    if batch_num not in BATCHES:
        print(f"Error: batch {batch_num} not found. Valid: 1-{len(BATCHES)}")
        sys.exit(1)

    name, states = BATCHES[batch_num]
    engine = get_sync_engine()
    with Session(engine) as session:
        stmt = (
            select(Utility.id)
            .where(Utility.country == "US")
            .where(Utility.is_active.is_(True))
            .where(Utility.state_province.in_(states))
            .order_by(Utility.state_province, Utility.name)
        )
        ids = list(session.execute(stmt).scalars().all())
    print(f"Batch {batch_num}: {name} — {len(ids)} utilities in {', '.join(states)}")
    return ids


_print_lock = threading.Lock()


def _safe_print(*args, **kwargs):
    with _print_lock:
        print(*args, **kwargs, flush=True)


def _process_one_utility(uid, index_label, dry_run, comprehensive):
    """Process a single utility — designed to run in a thread pool."""
    from scripts.tariff_pipeline import run_pipeline, cleanup_between_utilities

    _safe_print(f"\n[{index_label}] Utility {uid}")
    try:
        result = run_pipeline(
            uid,
            dry_run=dry_run,
            comprehensive=comprehensive,
        )
        return asdict(result)
    except Exception as e:
        _safe_print(f"  CRASHED: {e}")
        return {
            "utility_id": uid,
            "utility_name": str(uid),
            "errors": [f"Unhandled crash: {e}"],
            "phase4_validation": {},
        }
    finally:
        cleanup_between_utilities()


def main():
    parser = argparse.ArgumentParser(description="US regional batch runner")
    parser.add_argument("--list-batches", action="store_true", help="Show all batches with counts")
    parser.add_argument("--batch", type=int, help="Batch number to run (1-15)")
    parser.add_argument("--comprehensive", action="store_true", help="Include specialty tariff search")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to database")
    parser.add_argument("--limit", type=int, default=0, help="Limit utilities per batch (0=all)")
    parser.add_argument("--skip", type=int, default=0,
                        help="Skip first N utilities (resume after crash)")
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of parallel workers (default: 1)")
    parser.add_argument("--output", type=str, help="Write results JSON to file")
    args = parser.parse_args()

    if args.list_batches:
        list_batches()
        return

    if not args.batch:
        print("Error: --batch N is required (or use --list-batches)")
        sys.exit(1)

    ids = get_batch_utility_ids(args.batch)
    if args.skip > 0:
        print(f"  Skipping first {args.skip} utilities (already processed)")
        ids = ids[args.skip:]
    if args.limit > 0:
        ids = ids[:args.limit]
        print(f"  Limited to first {args.limit} utilities")

    offset = args.skip
    total = len(ids) + offset
    workers = max(1, min(args.workers, len(ids)))

    if workers > 1:
        print(f"  Running with {workers} parallel workers")

    # Build (uid, label) pairs so each task knows its display index
    tasks = [(uid, f"{i + offset}/{total}") for i, uid in enumerate(ids, 1)]

    results = [None] * len(tasks)

    if workers == 1:
        for idx, (uid, label) in enumerate(tasks):
            results[idx] = _process_one_utility(uid, label, args.dry_run, args.comprehensive)
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_to_idx = {
                pool.submit(_process_one_utility, uid, label, args.dry_run, args.comprehensive): idx
                for idx, (uid, label) in enumerate(tasks)
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    results[idx] = future.result()
                except Exception as e:
                    uid, label = tasks[idx]
                    _safe_print(f"  [{label}] Worker exception: {e}")
                    results[idx] = {
                        "utility_id": uid,
                        "utility_name": "Unknown",
                        "errors": [f"Worker exception: {e}"],
                        "phase4_validation": {},
                    }

    if args.output and results:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nResults written to {args.output}")

    # Summary
    print("\n" + "=" * 60)
    batch_name = BATCHES[args.batch][0]
    print(f"BATCH {args.batch} SUMMARY: {batch_name}")
    if offset > 0:
        print(f"  (Resumed from utility {offset + 1})")
    if workers > 1:
        print(f"  (Ran with {workers} parallel workers)")
    print("=" * 60)
    success = 0
    failed = 0
    total_tariffs = 0
    for r in results:
        tariff_count = r.get("phase4_validation", {}).get("valid", 0)
        errors = r.get("errors", [])
        name = r.get("utility_name", "Unknown")[:40]
        if errors:
            failed += 1
            status = f"FAILED: {errors[0][:50]}"
        else:
            success += 1
            total_tariffs += tariff_count
            status = f"{tariff_count} tariffs"
        print(f"  {name:40s} {status}")

    print(f"\n  Processed: {len(results)}, Success: {success}, Failed: {failed}, Total tariffs: {total_tariffs}")
    if args.dry_run:
        print("  (DRY RUN — nothing written to database)")


if __name__ == "__main__":
    main()
