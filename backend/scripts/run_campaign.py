"""Run the full stale-OpenEI cleanup in one unattended session.

Phases:
  1. Bulk Track B on every utility that has both fresh and old rows
  2. Extract in auto-batches (Celery) until no utility has stale OpenEI rows
  3. Retry known hard cases (merged overrides from chunk definitions)
  4. Final bulk Track B

Usage:
  python /app/scripts/run_campaign.py --apply
  python /app/scripts/run_campaign.py --apply --batch-size 25
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone

from celery import chord, group
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import RefreshRun, RefreshType, Utility
from app.tasks.refresh import (
    _count_tariffs,
    finalize_refresh_run,
    log_chord_failure,
    process_utility,
)
from scripts.chunk_campaign import merged_extract_overrides, merged_retry_overrides
from scripts.run_chunk import (
    TARIFF_COUNTS_SQL,
    track_b_utilities,
    wait_for_refresh_run,
)
from scripts.chunk_campaign import ChunkSpec, RetryOverride
from scripts.tariff_pipeline import run_pipeline

NEXT_BATCH_SQL = text("""
  SELECT u.id, u.name,
         COUNT(*) FILTER (
           WHERE t.last_verified_at IS NULL
             AND t.openei_id IS NOT NULL
             AND t.superseded_by_tariff_id IS NULL
         ) AS old_count
  FROM utilities u
  JOIN tariffs t ON t.utility_id = u.id
  WHERE u.is_active
    AND t.openei_id IS NOT NULL
    AND u.id != ALL(:processed)
  GROUP BY u.id, u.name
  HAVING COUNT(*) FILTER (
           WHERE t.last_verified_at IS NULL
             AND t.openei_id IS NOT NULL
             AND t.superseded_by_tariff_id IS NULL
         ) >= :min_stale
  ORDER BY old_count DESC, u.id
  LIMIT :limit
""")

TRACK_B_ALL_SQL = text("""
  SELECT t.utility_id
  FROM tariffs t
  JOIN utilities u ON u.id = t.utility_id AND u.is_active
  WHERE t.openei_id IS NOT NULL
  GROUP BY t.utility_id
  HAVING COUNT(*) FILTER (
           WHERE t.last_verified_at IS NOT NULL
             AND t.superseded_by_tariff_id IS NULL
         ) > 0
     AND COUNT(*) FILTER (
           WHERE t.last_verified_at IS NULL
             AND t.openei_id IS NOT NULL
             AND t.superseded_by_tariff_id IS NULL
         ) > 0
  ORDER BY t.utility_id
""")


def campaign_snapshot(session: Session) -> dict:
    row = session.execute(
        text("""
          SELECT
            COUNT(*) FILTER (
              WHERE last_verified_at IS NOT NULL AND superseded_by_tariff_id IS NULL
            ) AS fresh,
            COUNT(*) FILTER (
              WHERE last_verified_at IS NULL
                AND openei_id IS NOT NULL
                AND superseded_by_tariff_id IS NULL
            ) AS old,
            COUNT(DISTINCT utility_id) FILTER (
              WHERE last_verified_at IS NULL
                AND openei_id IS NOT NULL
                AND superseded_by_tariff_id IS NULL
            ) AS utils_with_old
          FROM tariffs
        """)
    ).first()
    return {
        "fresh": int(row[0]),
        "old": int(row[1]),
        "utilities_with_old": int(row[2]),
        "pct_verified": round(
            100.0 * row[0] / max(row[0] + row[1], 1), 1
        ),
    }


def all_track_b_ids(session: Session) -> list[int]:
    return list(session.execute(TRACK_B_ALL_SQL).scalars().all())


def fetch_next_batch(
    session: Session,
    processed: set[int],
    *,
    batch_size: int,
    min_stale: int,
) -> list[tuple[int, str, int]]:
    rows = session.execute(
        NEXT_BATCH_SQL,
        {
            "processed": list(processed) or [0],
            "min_stale": min_stale,
            "limit": batch_size,
        },
    ).all()
    return [(int(r[0]), str(r[1]), int(r[2])) for r in rows]


def apply_overrides(session: Session, utility_ids: list[int], overrides: dict[int, str]) -> None:
    for uid in utility_ids:
        url = overrides.get(uid)
        if not url:
            continue
        util = session.get(Utility, uid)
        if util:
            util.rate_page_url_override = url
    session.commit()


def dispatch_batch(
    session: Session,
    batch: list[tuple[int, str, int]],
    *,
    batch_num: int,
    overrides: dict[int, str],
) -> tuple[int, object]:
    utility_ids = [uid for uid, _, _ in batch]
    apply_overrides(session, utility_ids, overrides)

    before_detail = {
        uid: {"name": name, "stale": old_count}
        for uid, name, old_count in batch
    }
    run = RefreshRun(refresh_type=RefreshType.manual)
    session.add(run)
    session.flush()
    run_id = run.id
    before_counts = _count_tariffs(session, utility_ids)
    run.utilities_targeted = len(utility_ids)
    run.summary_json = {
        "campaign": "all_stale_cleanup",
        "batch": batch_num,
        "utilities": [{"id": uid, "name": name, "old": old} for uid, name, old in batch],
        "before": before_detail,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    session.commit()

    before_json = json.dumps({str(k): v for k, v in before_counts.items()})
    print(
        f"\n=== Batch {batch_num}: RefreshRun #{run_id} "
        f"({len(utility_ids)} utilities, {sum(x[2] for x in batch)} old tariffs) ==="
    )
    for uid, name, old in batch[:5]:
        print(f"  {uid:>5}  {name[:40]:<40}  old={old}")
    if len(batch) > 5:
        print(f"  ... and {len(batch) - 5} more")

    task_group = group(process_utility.s(uid, False) for uid in utility_ids)
    callback = finalize_refresh_run.s(run_id, before_json).on_error(log_chord_failure.s())
    chord_result = chord(task_group)(callback)
    print(f"  Dispatched chord {chord_result.id}")
    return run_id, chord_result


def retry_hard_cases(session: Session, apply: bool) -> list[dict]:
    """Retry utilities with known PDF overrides that still have old rows."""
    overrides = merged_retry_overrides()
    remaining: dict[int, RetryOverride] = {}
    for uid, cfg in overrides.items():
        row = session.execute(TARIFF_COUNTS_SQL, {"uid": uid}).first()
        if row[1] > 0:  # old count
            remaining[uid] = cfg
    if not remaining:
        print("\n=== Retry hard cases: none remaining ===")
        return []

    print(f"\n=== Retry hard cases ({len(remaining)} utilities) ===")
    spec = ChunkSpec(number=0, utilities=[], retry_overrides=remaining)
    results = []
    for uid, cfg in remaining.items():
        util = session.get(Utility, uid)
        if not util:
            continue
        print(f"\n--- Retry utility {uid}: {util.name} ---")
        util.rate_page_url_override = cfg.url
        if cfg.website and not util.website_url:
            util.website_url = cfg.website
        session.commit()

        before = session.execute(TARIFF_COUNTS_SQL, {"uid": uid}).first()
        print(f"  Before: fresh={before[0]}, old={before[1]}")

        t0 = time.time()
        pipeline_result = run_pipeline(
            uid, dry_run=False, comprehensive=False, skip_search=True
        )
        valid = (pipeline_result.phase4_validation or {}).get("valid", 0)
        elapsed = time.time() - t0

        after = session.execute(TARIFF_COUNTS_SQL, {"uid": uid}).first()
        print(
            f"  After:  fresh={after[0]}, old={after[1]}  "
            f"({valid} valid, {elapsed:.0f}s)"
        )
        results.append(
            {
                "utility_id": uid,
                "fresh_before": before[0],
                "fresh_after": after[0],
                "old_before": before[1],
                "old_after": after[1],
                "valid_extracted": valid,
                "errors": pipeline_result.errors,
            }
        )
    return results


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Full stale-OpenEI cleanup campaign")
    p.add_argument("--apply", action="store_true", help="Apply Track B supersede links")
    p.add_argument("--batch-size", type=int, default=20, help="Utilities per extract batch")
    p.add_argument("--min-stale", type=int, default=1, help="Min old tariffs to include")
    p.add_argument(
        "--wait-timeout",
        type=int,
        default=10800,
        help="Max seconds to wait per batch (default 3h)",
    )
    p.add_argument(
        "--skip-initial-track-b",
        action="store_true",
        help="Skip opening bulk Track B pass",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv or sys.argv[1:])
    engine = get_sync_engine()
    started = datetime.now(timezone.utc)

    out: dict = {
        "campaign": "all_stale_cleanup",
        "started_at": started.isoformat(),
        "batch_size": args.batch_size,
        "applied": args.apply,
        "batches": [],
    }

    with Session(engine) as session:
        out["baseline"] = campaign_snapshot(session)
        print("=== FULL STALE CLEANUP CAMPAIGN ===")
        print(json.dumps(out["baseline"], indent=2))

    if not args.skip_initial_track_b:
        with Session(engine) as session:
            ids = all_track_b_ids(session)
            print(f"\n=== Phase 1: bulk Track B ({len(ids)} utilities) ===")
            out["initial_track_b"] = track_b_utilities(
                session, ids, apply=args.apply
            )

    processed: set[int] = set()
    overrides = merged_extract_overrides()
    batch_num = 0

    while True:
        with Session(engine) as session:
            batch = fetch_next_batch(
                session,
                processed,
                batch_size=args.batch_size,
                min_stale=args.min_stale,
            )
        if not batch:
            print("\n=== Extract loop complete — no utilities left ===")
            break

        batch_num += 1
        utility_ids = [uid for uid, _, _ in batch]
        processed.update(utility_ids)

        with Session(engine) as session:
            run_id, chord_result = dispatch_batch(
                session, batch, batch_num=batch_num, overrides=overrides
            )

        batch_out = wait_for_refresh_run(
            run_id,
            chord_result,
            timeout_sec=args.wait_timeout,
        )

        with Session(engine) as session:
            batch_out["track_b"] = track_b_utilities(
                session, utility_ids, apply=args.apply
            )
            batch_out["after"] = campaign_snapshot(session)

        out["batches"].append(batch_out)
        print(f"  Campaign progress: {json.dumps(batch_out['after'])}")

    with Session(engine) as session:
        out["retries"] = retry_hard_cases(session, apply=args.apply)

    with Session(engine) as session:
        ids = all_track_b_ids(session)
        print(f"\n=== Final bulk Track B ({len(ids)} utilities) ===")
        out["final_track_b"] = track_b_utilities(session, ids, apply=args.apply)
        out["final"] = campaign_snapshot(session)

    out["finished_at"] = datetime.now(timezone.utc).isoformat()
    out["batches_run"] = batch_num
    out["utilities_processed"] = len(processed)
    print("\n=== CAMPAIGN DONE ===")
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
