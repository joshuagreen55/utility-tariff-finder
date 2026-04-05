"""Clean LG&E-contaminated utilities and re-extract their tariffs.

These 6 utilities had their tariffs overwritten with LG&E and KU data
due to a bug where run_pipeline() was called without utility name/state.
"""
from sqlalchemy import delete as sa_delete, select
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models.tariff import Tariff, RateComponent
from scripts.tariff_pipeline import run_pipeline

CORRUPTED_IDS = [50, 768, 906, 1228, 1571, 1799]


def main():
    engine = get_sync_engine()

    print("=== Cleaning corrupted utilities ===")
    with Session(engine) as session:
        for uid in CORRUPTED_IDS:
            tariff_ids = list(
                session.execute(
                    select(Tariff.id).where(Tariff.utility_id == uid)
                ).scalars().all()
            )
            if tariff_ids:
                session.execute(
                    sa_delete(RateComponent).where(RateComponent.tariff_id.in_(tariff_ids))
                )
                session.execute(
                    sa_delete(Tariff).where(Tariff.id.in_(tariff_ids))
                )
                print(f"  Deleted {len(tariff_ids)} tariffs for utility {uid}")
            else:
                print(f"  No tariffs found for utility {uid}")
        session.commit()

    print("\n=== Re-extracting ===")
    results = []
    for uid in CORRUPTED_IDS:
        print(f"\n--- Utility {uid} ---", flush=True)
        try:
            r = run_pipeline(uid, dry_run=False)
            tariffs = r.phase4_validation.get("valid", 0) if r.phase4_validation else 0
            status = "OK" if tariffs > 0 else "FAIL"
            print(f"  {status}: {tariffs} tariffs", flush=True)
            results.append((uid, status, tariffs))
        except Exception as e:
            print(f"  ERROR: {e}", flush=True)
            results.append((uid, "ERROR", 0))

    print("\n=== SUMMARY ===")
    for uid, status, count in results:
        print(f"  {uid}: {status} ({count} tariffs)")


if __name__ == "__main__":
    main()
