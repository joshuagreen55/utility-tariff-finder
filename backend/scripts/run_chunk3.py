"""Chunk 3: fresh extractions on 10 utilities (9 new + SCE re-run with PDF).

SCE gets a forced URL to the official residential rates fact-sheet PDF
(same approach that worked for Duke Florida).
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

CHUNK = [
    (1064, "Southern California Edison (re-run)"),
    (933,  "New York Power Authority"),
    (1053, "South Plains Electric Coop TX"),
    (326,  "East Mississippi Elec Pwr Assn"),
    (844,  "Oklahoma Gas & Electric"),
    (338,  "El Paso Electric"),
    (675,  "Montana-Dakota Utilities"),
    (514,  "Indiana Michigan Power"),
    (1261, "Wheatland Electric Coop KS"),
    (1314, "Tucson Electric Power"),
]

RATE_PAGE_OVERRIDES = {
    # Official SCE residential rates fact sheet (Schedule D / TOU-D plans).
    1064: "https://www.sce.com/sites/default/files/custom-files/PDF_Files/Residential%20Rates%20Fact%20Sheet%20English%20FINAL%20WCAG%20August%202023_edits.pdf",
}

UTILITY_IDS = [u[0] for u in CHUNK]
engine = get_sync_engine()


def main() -> None:
    print("=== CHUNK 3 — before state ===")
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
            "chunk": 3,
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
