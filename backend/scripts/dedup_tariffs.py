"""
Deduplicate tariffs by keeping only the most recent version of each
(utility_id, name, customer_class) group.

Older versions are deleted along with their rate_components (via CASCADE).

Usage:
    python -m scripts.dedup_tariffs [--dry-run]
"""

import argparse

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine


def dedup_tariffs(session: Session, dry_run: bool = False) -> dict:
    total = session.execute(text("SELECT COUNT(*) FROM tariffs")).scalar()

    keep_ids = session.execute(text("""
        SELECT DISTINCT ON (utility_id, name, customer_class) id
        FROM tariffs
        ORDER BY utility_id, name, customer_class,
                 effective_date DESC NULLS LAST,
                 end_date DESC NULLS FIRST,
                 id DESC
    """)).scalars().all()

    keep_set = set(keep_ids)
    to_delete = total - len(keep_set)

    print(f"Total tariffs:        {total:,}")
    print(f"Unique rate plans:    {len(keep_set):,}")
    print(f"Older duplicates:     {to_delete:,} ({to_delete/total*100:.1f}%)")

    if to_delete == 0:
        print("Nothing to purge.")
        return {"total": total, "kept": len(keep_set), "deleted": 0}

    if dry_run:
        print("\n[DRY RUN] No changes made.")
        return {"total": total, "kept": len(keep_set), "deleted": 0}

    rc_deleted = session.execute(text("""
        DELETE FROM rate_components
        WHERE tariff_id NOT IN (
            SELECT DISTINCT ON (utility_id, name, customer_class) id
            FROM tariffs
            ORDER BY utility_id, name, customer_class,
                     effective_date DESC NULLS LAST,
                     end_date DESC NULLS FIRST,
                     id DESC
        )
    """)).rowcount

    t_deleted = session.execute(text("""
        DELETE FROM tariffs
        WHERE id NOT IN (
            SELECT DISTINCT ON (utility_id, name, customer_class) id
            FROM tariffs
            ORDER BY utility_id, name, customer_class,
                     effective_date DESC NULLS LAST,
                     end_date DESC NULLS FIRST,
                     id DESC
        )
    """)).rowcount

    session.commit()

    remaining = session.execute(text("SELECT COUNT(*) FROM tariffs")).scalar()
    rc_remaining = session.execute(text("SELECT COUNT(*) FROM rate_components")).scalar()

    print(f"\nDeleted {t_deleted:,} tariffs and {rc_deleted:,} rate components.")
    print(f"Remaining: {remaining:,} tariffs, {rc_remaining:,} rate components.")

    return {"total": total, "kept": remaining, "deleted": t_deleted}


def main():
    parser = argparse.ArgumentParser(description="Deduplicate tariffs")
    parser.add_argument("--dry-run", action="store_true", help="Preview without deleting")
    args = parser.parse_args()

    engine = get_sync_engine()
    with Session(engine) as session:
        dedup_tariffs(session, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
