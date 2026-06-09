"""Chunk 2: fresh extractions on 10 big utilities with stale OpenEI data.

Focus: utilities we haven't successfully refreshed yet (0 or few fresh
tariffs, lots of old seed data). Re-runs SCE and Duke FL from Chunk 1
failures; adds ConEd, PG&E, Xcel MN, etc.

Dispatches as a manual RefreshRun via Celery chord.
"""
import json
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

# Big utilities with lots of old data and little/no fresh extraction.
CHUNK = [
    (1064, "Southern California Edison"),
    (870,  "Pacific Gas & Electric"),
    (246,  "Consolidated Edison"),
    (819,  "Northern States Power MN (Xcel)"),
    (858,  "Otter Tail Power"),
    (382,  "Duke Energy Florida"),
    (316,  "Duke Energy Carolinas"),
    (850,  "Orange & Rockland"),
    (1020, "City of Seattle"),
    (238,  "Commonwealth Edison (ComEd IL)"),
]

# Force the pipeline to the official rate pages for utilities that
# previously pulled junk from search results.
RATE_PAGE_OVERRIDES = {
    1064: "https://www.sce.com/residential/rates",
    870:  "https://www.pge.com/tariffs/electric.shtml",
}

UTILITY_IDS = [u[0] for u in CHUNK]
engine = get_sync_engine()


def _capture_before(session: Session) -> dict:
    detail = {}
    print(f"{'ID':>5}  {'Utility':<34}  {'Total':>5}  {'Stale':>5}  {'Fresh':>5}  Last verified")
    print("-" * 85)
    for uid, name in CHUNK:
        row = session.execute(
            text("""
              SELECT COUNT(*),
                     COUNT(*) FILTER (WHERE last_verified_at IS NULL
                                      AND openei_id IS NOT NULL
                                      AND superseded_by_tariff_id IS NULL),
                     COUNT(*) FILTER (WHERE last_verified_at IS NOT NULL
                                      AND superseded_by_tariff_id IS NULL),
                     MAX(last_verified_at)
              FROM tariffs WHERE utility_id = :uid
            """),
            {"uid": uid},
        ).first()
        detail[uid] = {
            "name": name,
            "total": row[0],
            "stale": row[1],
            "fresh": row[2],
            "last_verified": row[3].isoformat() if row[3] else None,
        }
        last = row[3].strftime("%Y-%m-%d") if row[3] else "never"
        print(f"  {uid:>5}  {name[:34]:<34}  {row[0]:>5}  {row[1]:>5}  {row[2]:>5}  {last}")
    return detail


def _apply_overrides(session: Session) -> None:
    for uid, url in RATE_PAGE_OVERRIDES.items():
        util = session.get(Utility, uid)
        if not util:
            continue
        old = util.rate_page_url_override
        util.rate_page_url_override = url
        print(f"  override util_id={uid}: {old or '(none)'} -> {url}")


def main() -> None:
    print("=== CHUNK 2 — before state ===")
    with Session(engine) as session:
        before_detail = _capture_before(session)
        stale_total = sum(d["stale"] for d in before_detail.values())
        print(f"\n  Stale tariffs at stake: {stale_total}")

        print("\n=== Rate page overrides ===")
        _apply_overrides(session)
        session.commit()

        run = RefreshRun(refresh_type=RefreshType.manual)
        session.add(run)
        session.flush()
        run_id = run.id
        before_counts = _count_tariffs(session, UTILITY_IDS)
        run.utilities_targeted = len(UTILITY_IDS)
        run.summary_json = {
            "chunk": 2,
            "campaign": "stale_openei_cleanup",
            "utilities": [{"id": uid, "name": name} for uid, name in CHUNK],
            "before": before_detail,
            "overrides": RATE_PAGE_OVERRIDES,
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        session.commit()

    before_json = json.dumps({str(k): v for k, v in before_counts.items()})

    print(f"\n=== Dispatching RefreshRun #{run_id} ({len(UTILITY_IDS)} utilities) ===")
    task_group = group(process_utility.s(uid, False) for uid in UTILITY_IDS)
    callback = finalize_refresh_run.s(run_id, before_json).on_error(log_chord_failure.s())
    result = chord(task_group)(callback)
    print(f"Dispatched. Chord id: {result.id}")
    print(f"Monitor: GET /api/admin/monitoring/refresh-runs/{run_id}")


if __name__ == "__main__":
    main()
