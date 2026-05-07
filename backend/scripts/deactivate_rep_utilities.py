"""Deactivate retail electric provider (REP) utility records.

The EIA classification `RETAIL_MARKETER` covers competitive electricity
suppliers — companies like XOOM Energy, Spark Energy, Octopus Energy,
TriEagle, Tara, etc. — that sell electricity in deregulated markets but
do NOT operate the wires, do NOT have a default-service tariff, and
publish promotional offers that change frequently.

These records pollute our default-service coverage in two ways:
  1. Their "rate sheets" are TDU-territory-specific marketing pages
     that legitimately reference Oncor / CenterPoint / AEP Texas / etc.,
     which the attribution validator now (correctly) rejects.
  2. They inflate the utility count without representing utilities a
     residential customer would actually be assigned to by default.

This script flips `is_active = false` on every active RETAIL_MARKETER
record. Tariff rows are left in place (so the data is preserved in case
we ever want to expose competitive offers as a separate feature), but
they are no longer counted toward coverage stats and no longer surface
in the address-lookup or tariff-browse endpoints (which filter by
`is_active = true`).

Dry-run by default. Pass `--apply` to actually deactivate.
"""
import argparse
import sys

from sqlalchemy import text, update
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import Utility


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually deactivate (default is dry-run)",
    )
    args = parser.parse_args()

    eng = get_sync_engine()
    with Session(eng) as s:
        rows = s.execute(
            text("""
                SELECT u.id, u.name, u.state_province, u.country,
                       (SELECT COUNT(*) FROM tariffs t WHERE t.utility_id = u.id) AS tariff_count,
                       (SELECT COUNT(*) FROM monitoring_sources ms WHERE ms.utility_id = u.id) AS sources
                FROM utilities u
                WHERE u.is_active = true
                  AND u.utility_type = 'RETAIL_MARKETER'
                ORDER BY u.state_province, u.name
            """)
        ).fetchall()

        total_count = len(rows)
        with_tariffs = sum(1 for r in rows if r[4] > 0)
        total_tariffs = sum(r[4] for r in rows)
        total_sources = sum(r[5] for r in rows)

        # Pre-action coverage snapshot
        coverage_sql = text("""
            SELECT
              (SELECT COUNT(*) FROM utilities WHERE is_active = true)         AS active_utilities,
              (SELECT COUNT(DISTINCT utility_id) FROM tariffs t
                JOIN utilities u ON u.id = t.utility_id
                WHERE u.is_active = true)                                      AS with_tariffs
        """)
        before = s.execute(coverage_sql).first()

        print()
        print("=" * 78)
        print(f"REP DEACTIVATION  ({'APPLY MODE' if args.apply else 'DRY-RUN'})")
        print("=" * 78)
        print(f"  Active RETAIL_MARKETER utilities:       {total_count:>4d}")
        print(f"    of which currently have tariffs:      {with_tariffs:>4d}")
        print(f"  Tariffs that will become orphaned:      {total_tariffs:>4d}")
        print(f"  Monitoring sources that will go dark:   {total_sources:>4d}")
        print()
        print("--- Sample (top 25 by state) ---")
        for r in rows[:25]:
            print(f"  id={r[0]:<5d} {r[2] or '?':<3s} {r[1][:55]:<55s} tariffs={r[4]:<3d} sources={r[5]}")
        if total_count > 25:
            print(f"  ... and {total_count - 25} more")

        if not args.apply:
            print()
            print(f"DRY-RUN: no changes made. Coverage today: "
                  f"{before.with_tariffs} / {before.active_utilities} = "
                  f"{100*before.with_tariffs/before.active_utilities:.2f}%")
            print("Re-run with --apply to deactivate.")
            return 0

        # Apply
        print()
        print("Deactivating...")
        ids = [r[0] for r in rows]
        result = s.execute(
            update(Utility)
            .where(Utility.id.in_(ids))
            .values(is_active=False)
        )
        s.commit()

        after = s.execute(coverage_sql).first()
        print(f"  Deactivated {result.rowcount or 0} utilities")
        print()
        print("=" * 78)
        print("DONE")
        print("=" * 78)
        print(f"  Active utilities before:  {before.active_utilities}")
        print(f"  Active utilities after:   {after.active_utilities}")
        print(f"  Coverage before:          {before.with_tariffs} / {before.active_utilities} = "
              f"{100*before.with_tariffs/before.active_utilities:.2f}%")
        print(f"  Coverage after:           {after.with_tariffs} / {after.active_utilities} = "
              f"{100*after.with_tariffs/after.active_utilities:.2f}%")
        return 0


if __name__ == "__main__":
    sys.exit(main())
