"""Unified chunk orchestrator: extract → wait/finalize → retry → Track B → summary.

Usage:
  python /app/scripts/run_chunk.py --chunk 5 --apply
  python /app/scripts/run_chunk.py --chunk 4 --retry-only --apply
  python /app/scripts/run_chunk.py --chunk 4 --track-b-only --apply
  python /app/scripts/run_chunk.py --chunk 4 --extract-only
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone

import anthropic
from celery import chord, group
from sqlalchemy import text, update as sa_update
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import RefreshRun, RefreshType, Utility
from app.tasks.refresh import (
    _count_tariffs,
    _finalize_run_from_audit,
    finalize_refresh_run,
    log_chord_failure,
    process_utility,
)
from scripts.chunk_campaign import ChunkSpec, get_chunk
from scripts.supersede_via_llm import CONF_RANK, _pair_one_utility
from scripts.tariff_pipeline import run_pipeline

TARIFF_COUNTS_SQL = text("""
  SELECT COUNT(*) FILTER (WHERE last_verified_at IS NOT NULL
                          AND superseded_by_tariff_id IS NULL),
         COUNT(*) FILTER (WHERE last_verified_at IS NULL
                          AND openei_id IS NOT NULL
                          AND superseded_by_tariff_id IS NULL),
         COUNT(*)
  FROM tariffs WHERE utility_id = :uid
""")

DETAIL_SQL = text("""
  SELECT COUNT(*),
         COUNT(*) FILTER (WHERE last_verified_at IS NULL
                          AND openei_id IS NOT NULL
                          AND superseded_by_tariff_id IS NULL),
         COUNT(*) FILTER (WHERE last_verified_at IS NOT NULL
                          AND superseded_by_tariff_id IS NULL),
         MAX(last_verified_at)
  FROM tariffs WHERE utility_id = :uid
""")


def _counts(session: Session, uid: int) -> tuple[int, int, int]:
    row = session.execute(TARIFF_COUNTS_SQL, {"uid": uid}).first()
    return int(row[0]), int(row[1]), int(row[2])


def print_before_state(session: Session, spec: ChunkSpec) -> dict[int, dict]:
    print(f"=== CHUNK {spec.number} — before state ===")
    print(f"{'ID':>5}  {'Utility':<36}  {'Total':>5}  {'Old':>5}  {'Fresh':>5}  Last verified")
    print("-" * 88)
    before_detail: dict[int, dict] = {}
    stale_total = 0
    for uid, name in spec.utilities:
        row = session.execute(DETAIL_SQL, {"uid": uid}).first()
        before_detail[uid] = {
            "name": name,
            "total": row[0],
            "stale": row[1],
            "fresh": row[2],
            "last_verified": row[3].isoformat() if row[3] else None,
        }
        stale_total += row[1]
        last = row[3].strftime("%Y-%m-%d") if row[3] else "never"
        print(f"  {uid:>5}  {name[:36]:<36}  {row[0]:>5}  {row[1]:>5}  {row[2]:>5}  {last}")
    print(f"\n  Old tariffs at stake: {stale_total}")
    return before_detail


def print_after_state(session: Session, spec: ChunkSpec, before: dict[int, dict]) -> None:
    print(f"\n=== CHUNK {spec.number} — after state ===")
    print(f"{'ID':>5}  {'Utility':<28}  {'Fresh':>5}  {'Old':>5}  {'Δ fresh':>7}  {'Δ old':>7}")
    print("-" * 72)
    for uid, name in spec.utilities:
        fresh, old, _ = _counts(session, uid)
        b = before.get(uid, {})
        df = fresh - int(b.get("fresh", 0))
        do = old - int(b.get("stale", 0))
        print(f"  {uid:>5}  {name[:28]:<28}  {fresh:>5}  {old:>5}  {df:>+7}  {do:>+7}")


def apply_extract_overrides(session: Session, spec: ChunkSpec) -> None:
    if not spec.extract_overrides:
        return
    print("\n=== Extract overrides ===")
    for uid, url in spec.extract_overrides.items():
        util = session.get(Utility, uid)
        if util:
            print(f"  util_id={uid}: {url[:80]}...")
            util.rate_page_url_override = url
    session.commit()


def dispatch_extract(session: Session, spec: ChunkSpec, before_detail: dict[int, dict]) -> tuple[int, object]:
    apply_extract_overrides(session, spec)

    run = RefreshRun(refresh_type=RefreshType.manual)
    session.add(run)
    session.flush()
    run_id = run.id
    before_counts = _count_tariffs(session, spec.utility_ids)
    run.utilities_targeted = len(spec.utility_ids)
    run.summary_json = {
        "chunk": spec.number,
        "campaign": "stale_openei_cleanup",
        "utilities": [{"id": uid, "name": name} for uid, name in spec.utilities],
        "before": before_detail,
        "extract_overrides": spec.extract_overrides,
        "retry_overrides": {
            str(k): {"url": v.url, "website": v.website}
            for k, v in spec.retry_overrides.items()
        },
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    session.commit()

    before_json = json.dumps({str(k): v for k, v in before_counts.items()})
    print(f"\n=== Dispatching RefreshRun #{run_id} ({len(spec.utility_ids)} utilities) ===")
    task_group = group(process_utility.s(uid, False) for uid in spec.utility_ids)
    callback = finalize_refresh_run.s(run_id, before_json).on_error(log_chord_failure.s())
    chord_result = chord(task_group)(callback)
    print(f"Dispatched. Chord id: {chord_result.id}")
    return run_id, chord_result


def wait_for_refresh_run(
    run_id: int,
    chord_result,
    *,
    poll_sec: int = 30,
    timeout_sec: int = 10800,
) -> dict:
    """Poll until RefreshRun is finalized or chord completes / times out."""
    print(f"\n=== Waiting for RefreshRun #{run_id} (timeout {timeout_sec // 60}m) ===")
    deadline = time.time() + timeout_sec
    engine = get_sync_engine()
    header_done = False

    while time.time() < deadline:
        with Session(engine) as session:
            run = session.get(RefreshRun, run_id)
            if run and run.finished_at:
                print(f"  RefreshRun #{run_id} finalized at {run.finished_at.isoformat()}")
                return {
                    "run_id": run_id,
                    "finalized": True,
                    "utilities_processed": run.utilities_processed,
                    "errors": run.errors,
                    "tariffs_added": run.tariffs_added,
                }

        if chord_result.parent and chord_result.parent.ready():
            header_done = True
        if header_done and chord_result.ready():
            break

        time.sleep(poll_sec)

    with Session(engine) as session:
        run = session.get(RefreshRun, run_id)
        if run and run.finished_at:
            return {
                "run_id": run_id,
                "finalized": True,
                "utilities_processed": run.utilities_processed,
                "errors": run.errors,
                "tariffs_added": run.tariffs_added,
            }
        if run:
            print(f"  Chord did not finalize RefreshRun #{run_id} — reconstructing from audit trail")
            summary = _finalize_run_from_audit(session, run)
            session.commit()
            return {"run_id": run_id, "finalized": "audit", **summary}

    raise TimeoutError(f"RefreshRun #{run_id} did not complete within {timeout_sec}s")


def retry_utilities(session: Session, spec: ChunkSpec) -> list[dict]:
    if not spec.retry_overrides:
        print("\n=== Retry phase (no retry overrides configured) ===")
        return []

    print(f"\n=== Retry phase ({len(spec.retry_overrides)} utilities) ===")
    results = []
    for uid, cfg in spec.retry_overrides.items():
        util = session.get(Utility, uid)
        if not util:
            continue
        print(f"\n--- Retry utility {uid}: {util.name} ---")
        print(f"  Override: {cfg.url[:80]}...")
        util.rate_page_url_override = cfg.url
        if cfg.website and not util.website_url:
            util.website_url = cfg.website
        session.commit()

        fresh_b, old_b, _ = _counts(session, uid)
        print(f"  Before: fresh={fresh_b}, old={old_b}")

        t0 = time.time()
        pipeline_result = run_pipeline(
            uid, dry_run=False, comprehensive=False, skip_search=True
        )
        valid = (pipeline_result.phase4_validation or {}).get("valid", 0)
        elapsed = time.time() - t0

        fresh_a, old_a, _ = _counts(session, uid)
        print(
            f"  After:  fresh={fresh_a}, old={old_a}  "
            f"({valid} valid extracted, {elapsed:.0f}s)"
        )
        if pipeline_result.errors:
            print(f"  Errors: {pipeline_result.errors[:2]}")

        results.append(
            {
                "utility_id": uid,
                "utility_name": util.name,
                "fresh_before": fresh_b,
                "fresh_after": fresh_a,
                "old_before": old_b,
                "old_after": old_a,
                "valid_extracted": valid,
                "errors": pipeline_result.errors,
            }
        )
    return results


def track_b_utilities(
    session: Session,
    utility_ids: list[int],
    *,
    apply: bool,
    min_conf: str = "high",
) -> dict:
    from app.models.tariff import Tariff

    min_rank = CONF_RANK[min_conf]
    client = anthropic.Anthropic()
    print(f"\n=== Track B ({len(utility_ids)} utilities, min_conf={min_conf}) ===")

    total_proposed = 0
    total_applied = 0

    for uid in utility_ids:
        util = session.get(Utility, uid)
        if not util:
            continue

        fresh_q = session.execute(
            text("""
              SELECT id, name, customer_class, rate_type FROM tariffs
              WHERE utility_id = :uid AND last_verified_at IS NOT NULL
                AND superseded_by_tariff_id IS NULL
              ORDER BY customer_class, name
            """),
            {"uid": uid},
        ).all()
        stranded_q = session.execute(
            text("""
              SELECT id, name, customer_class, rate_type FROM tariffs
              WHERE utility_id = :uid AND last_verified_at IS NULL
                AND openei_id IS NOT NULL AND superseded_by_tariff_id IS NULL
              ORDER BY customer_class, name
            """),
            {"uid": uid},
        ).all()
        if not fresh_q or not stranded_q:
            print(
                f"  util_id={uid} {util.name[:30]:30s}  "
                f"skip (fresh={len(fresh_q)}, old={len(stranded_q)})"
            )
            continue

        fresh = [dict(r._mapping) for r in fresh_q]
        stranded = [dict(r._mapping) for r in stranded_q]

        try:
            pairings = _pair_one_utility(
                client, util.name, util.state_province, fresh, stranded
            )
        except Exception as e:
            print(f"  util_id={uid} FAIL: {e}")
            continue

        fresh_ids = {f["id"] for f in fresh}
        stranded_ids = {s["id"] for s in stranded}
        applied = 0
        non_null = [p for p in pairings if p.get("fresh_id") is not None]
        for p in non_null:
            if CONF_RANK.get(p.get("confidence", "low"), 0) < min_rank:
                continue
            if p["fresh_id"] not in fresh_ids or p["stranded_id"] not in stranded_ids:
                continue
            applied += 1
            if apply:
                session.execute(
                    sa_update(Tariff)
                    .where(Tariff.id == p["stranded_id"])
                    .values(
                        superseded_by_tariff_id=p["fresh_id"],
                        supersede_reason="llm_absorb",
                    )
                )

        total_proposed += len(non_null)
        total_applied += applied
        if apply:
            session.commit()
        print(
            f"  util_id={uid} {util.state_province} {util.name[:28]:28s}  "
            f"fresh={len(fresh):>3}  old={len(stranded):>3}  "
            f"proposed={len(non_null):>3}  applied={applied:>3}"
        )

    return {"proposed": total_proposed, "applied": total_applied}


def track_b_candidates(session: Session, spec: ChunkSpec) -> list[int]:
    """Utilities in this chunk that have both fresh and old rows."""
    ids = []
    for uid, _ in spec.utilities:
        fresh, old, _ = _counts(session, uid)
        if fresh > 0 and old > 0:
            ids.append(uid)
    return ids


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a stale-data cleanup chunk end-to-end")
    parser.add_argument("--chunk", type=int, required=True, help="Chunk number (see chunk_campaign.py)")
    parser.add_argument("--apply", action="store_true", help="Apply Track B supersede links")
    parser.add_argument("--extract-only", action="store_true", help="Only dispatch Celery extraction")
    parser.add_argument("--retry-only", action="store_true", help="Only run retry overrides")
    parser.add_argument("--track-b-only", action="store_true", help="Only run Track B on chunk utilities")
    parser.add_argument(
        "--wait-timeout",
        type=int,
        default=10800,
        help="Max seconds to wait for extraction chord (default 3h)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv or sys.argv[1:])
    spec = get_chunk(args.chunk)

    phases = {"extract": True, "retry": True, "track_b": True}
    if args.extract_only:
        phases = {"extract": True, "retry": False, "track_b": False}
    elif args.retry_only:
        phases = {"extract": False, "retry": True, "track_b": False}
    elif args.track_b_only:
        phases = {"extract": False, "retry": False, "track_b": True}

    engine = get_sync_engine()
    out: dict = {
        "chunk": spec.number,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "phases": phases,
        "applied": args.apply,
    }

    with Session(engine) as session:
        before_detail = print_before_state(session, spec)

    if phases["extract"]:
        with Session(engine) as session:
            run_id, chord_result = dispatch_extract(session, spec, before_detail)
        out["run_id"] = run_id
        out["extract"] = wait_for_refresh_run(
            run_id, chord_result, timeout_sec=args.wait_timeout
        )

    if phases["retry"]:
        with Session(engine) as session:
            out["retries"] = retry_utilities(session, spec)

    if phases["track_b"]:
        with Session(engine) as session:
            candidates = track_b_candidates(session, spec)
            out["track_b"] = track_b_utilities(
                session, candidates, apply=args.apply
            )

    with Session(engine) as session:
        print_after_state(session, spec, before_detail)

    out["finished_at"] = datetime.now(timezone.utc).isoformat()
    print("\n=== DONE ===")
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
