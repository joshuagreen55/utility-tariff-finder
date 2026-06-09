"""
Deduplicate tariffs within each (utility_id, name, customer_class) group.

The keeper is chosen by: verified first (last_verified_at IS NOT NULL),
then newest effective_date, then newest end_date, then highest id. The
old ordering used effective_date alone, which made fresh extractions
(often NULL effective_date) lose to 2017 URDB seeds.

Losers are soft-superseded (superseded_by_tariff_id -> keeper,
supersede_reason='dup_exact_name') instead of deleted, matching the
API's live-tariff filter and keeping the audit trail.

Usage:
    python -m scripts.dedup_tariffs [--dry-run]
"""

import argparse

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine

RANKED_CTE = """
    WITH ranked AS (
        SELECT id,
               FIRST_VALUE(id) OVER (
                   PARTITION BY utility_id, name, customer_class
                   ORDER BY (last_verified_at IS NOT NULL) DESC,
                            effective_date DESC NULLS LAST,
                            end_date DESC NULLS FIRST,
                            id DESC
               ) AS keeper_id
        FROM tariffs
        WHERE superseded_by_tariff_id IS NULL
          AND supersede_reason IS NULL
    )
"""


def dedup_tariffs(session: Session, dry_run: bool = False) -> dict:
    total_live = session.execute(text(
        "SELECT COUNT(*) FROM tariffs "
        "WHERE superseded_by_tariff_id IS NULL AND supersede_reason IS NULL"
    )).scalar()

    dup_count = session.execute(text(
        RANKED_CTE + "SELECT COUNT(*) FROM ranked WHERE id <> keeper_id"
    )).scalar()

    print(f"Live tariffs:         {total_live:,}")
    print(f"Exact-name dupes:     {dup_count:,}"
          + (f" ({dup_count/total_live*100:.1f}%)" if total_live else ""))

    if dup_count == 0:
        print("Nothing to retire.")
        return {"total": total_live, "kept": total_live, "retired": 0}

    if dry_run:
        print("\n[DRY RUN] No changes made.")
        return {"total": total_live, "kept": total_live - dup_count, "retired": 0}

    retired = session.execute(text(
        RANKED_CTE +
        """
        UPDATE tariffs t
        SET superseded_by_tariff_id = r.keeper_id,
            supersede_reason = 'dup_exact_name'
        FROM ranked r
        WHERE t.id = r.id AND r.id <> r.keeper_id
        """
    )).rowcount

    session.commit()

    remaining = session.execute(text(
        "SELECT COUNT(*) FROM tariffs "
        "WHERE superseded_by_tariff_id IS NULL AND supersede_reason IS NULL"
    )).scalar()

    print(f"\nRetired {retired:,} duplicate tariffs (soft supersede).")
    print(f"Live remaining: {remaining:,} tariffs.")

    return {"total": total_live, "kept": remaining, "retired": retired}


def main():
    parser = argparse.ArgumentParser(description="Deduplicate tariffs")
    parser.add_argument("--dry-run", action="store_true", help="Preview without changing rows")
    args = parser.parse_args()

    engine = get_sync_engine()
    with Session(engine) as session:
        dedup_tariffs(session, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
