"""
One-time script to backfill monitoring_sources for active utilities that don't
have any monitoring source entries yet.

For each uncovered utility:
  1. Use the source_url from its most recent tariff (preferred)
  2. Fall back to the utility's website_url
  3. Skip if neither is available

Usage:
    python -m scripts.seed_monitoring_sources --dry-run
    python -m scripts.seed_monitoring_sources
"""

import argparse

from sqlalchemy import distinct, func, select
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import MonitoringSource, Tariff, Utility


def seed(dry_run: bool) -> None:
    engine = get_sync_engine()

    with Session(engine) as session:
        covered_ids = set(
            session.execute(
                select(distinct(MonitoringSource.utility_id))
            ).scalars().all()
        )

        uncovered = session.execute(
            select(Utility)
            .where(Utility.is_active.is_(True))
            .where(~Utility.id.in_(covered_ids) if covered_ids else True)
            .order_by(Utility.state_province, Utility.name)
        ).scalars().all()

        print(f"Active utilities without monitoring sources: {len(uncovered)}")

        created = 0
        skipped = 0

        for u in uncovered:
            best_url = _best_url(session, u)
            if not best_url:
                print(f"  SKIP  {u.name} ({u.state_province}) — no source_url or website_url")
                skipped += 1
                continue

            if not dry_run:
                ms = MonitoringSource(
                    utility_id=u.id,
                    url=best_url,
                    check_frequency_days=30,
                )
                session.add(ms)

            print(f"  {'DRY ' if dry_run else ''}ADD  {u.name} ({u.state_province}) — {best_url[:80]}")
            created += 1

        if not dry_run:
            session.commit()

        print(f"\nDone. Created: {created}, Skipped: {skipped}")
        if dry_run:
            print("(DRY RUN — no changes made)")


def _best_url(session: Session, utility: Utility) -> str | None:
    """Pick the best URL to monitor for a utility."""
    tariff_url = session.execute(
        select(Tariff.source_url)
        .where(Tariff.utility_id == utility.id)
        .where(Tariff.source_url.isnot(None))
        .where(Tariff.source_url != "")
        .order_by(Tariff.updated_at.desc().nullslast())
        .limit(1)
    ).scalar_one_or_none()

    if tariff_url:
        return tariff_url

    if utility.website_url:
        return utility.website_url

    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed monitoring sources for uncovered utilities")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, don't create")
    args = parser.parse_args()
    seed(args.dry_run)


if __name__ == "__main__":
    main()
