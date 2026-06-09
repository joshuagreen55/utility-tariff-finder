"""Chunk 4: fresh extractions on 10 untouched high-stale utilities."""
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

CHUNK = [
    (872,  "PacifiCorp"),
    (249,  "Consumers Energy Co (MI)"),
    (122,  "Broad River Electric Coop"),
    (1339, "Southeastern Power Admin"),
    (553,  "Evergy Metro"),
    (1309, "Evergy Kansas Central"),
    (344,  "Elkhorn Rural Public Pwr Dist"),
    (51,   "Arizona Public Service"),
    (304,  "DTE Electric Company"),
    (10,   "Alabama Power Co"),
]

# Direct PDF / rate-book URLs where Phase 1 search is slow or unreliable.
RATE_PAGE_OVERRIDES = {
    249: (
        "https://www.consumersenergy.com/-/media/CE/Documents/rates/"
        "electric-rate-book.ashx?la=en&hash=3A1AD0"
    ),
    304: (
        "https://www.michigan.gov/-/media/Project/Websites/mpsc/consumer/"
        "rate-books/electric/dte/dtee1cur.pdf"
    ),
}

UTILITY_IDS = [u[0] for u in CHUNK]
engine = get_sync_engine()


def main() -> None:
    print("=== CHUNK 4 — before state ===")
    with Session(engine) as session:
        print(f"{'ID':>5}  {'Utility':<36}  {'Total':>5}  {'Old':>5}  {'Fresh':>5}  Last verified")
        print("-" * 88)
        before_detail = {}
        stale_total = 0
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

        print("\n=== Rate page overrides ===")
        for uid, url in RATE_PAGE_OVERRIDES.items():
            util = session.get(Utility, uid)
            if util:
                print(f"  util_id={uid}: {url[:80]}...")
                util.rate_page_url_override = url
        session.commit()

        run = RefreshRun(refresh_type=RefreshType.manual)
        session.add(run)
        session.flush()
        run_id = run.id
        before_counts = _count_tariffs(session, UTILITY_IDS)
        run.utilities_targeted = len(UTILITY_IDS)
        run.summary_json = {
            "chunk": 4,
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


if __name__ == "__main__":
    main()
