"""One-time cleanup: remove duplicate and incomplete tariffs from the database.

Two passes:
  1. Remove tariffs that have ZERO energy/fixed/demand components (rate riders only).
  2. Merge prefix-duplicate names within the same utility + customer class,
     keeping the version with the most components and deleting the rest.

Usage:
  # Dry run for Nova Scotia only
  python -m scripts.cleanup_duplicate_tariffs --province NS --dry-run

  # Commit for Nova Scotia
  python -m scripts.cleanup_duplicate_tariffs --province NS

  # Dry run for all of Canada
  python -m scripts.cleanup_duplicate_tariffs --country CA --dry-run

  # Dry run for US Batch 1 (by state list)
  python -m scripts.cleanup_duplicate_tariffs --country US --states ME,NH,VT,MA,CT,RI --dry-run
"""
import argparse
import re
import sys
from collections import defaultdict

from sqlalchemy import text
from sqlalchemy.orm import Session
from app.db.session import get_sync_engine


def _normalize(name: str) -> str:
    n = name.lower().strip()
    n = re.sub(r"[^a-z0-9\s]", "", n)
    return " ".join(n.split())


def _build_where(country: str | None, province: str | None, states: list[str] | None) -> tuple[str, dict]:
    """Build a WHERE clause fragment and params for filtering utilities."""
    clauses = []
    params: dict = {}
    if country:
        clauses.append("u.country = :country")
        params["country"] = country
    if province:
        clauses.append("u.state_province = :province")
        params["province"] = province
    if states:
        clauses.append("u.state_province = ANY(:states)")
        params["states"] = states
    where = " AND ".join(clauses) if clauses else "1=1"
    return where, params


def run_cleanup(country: str | None, province: str | None, states: list[str] | None, dry_run: bool):
    where, params = _build_where(country, province, states)
    engine = get_sync_engine()

    with Session(engine) as session:
        # ----- PASS 1: Remove tariffs with no core components -----
        print("\n=== PASS 1: Tariffs with no energy/fixed/demand components ===\n")

        rows = session.execute(text(f"""
            SELECT t.id, t.name, t.customer_class::text, u.name as utility_name,
                   u.state_province,
                   COUNT(rc.id) as total_comps,
                   COUNT(rc.id) FILTER (
                       WHERE LOWER(rc.component_type::text) IN ('energy', 'fixed', 'demand')
                   ) as core_comps,
                   STRING_AGG(DISTINCT rc.component_type::text, ', ') as comp_types
            FROM tariffs t
            JOIN utilities u ON u.id = t.utility_id
            LEFT JOIN rate_components rc ON rc.tariff_id = t.id
            WHERE {where}
            GROUP BY t.id, t.name, t.customer_class, u.name, u.state_province
            HAVING COUNT(rc.id) FILTER (
                WHERE LOWER(rc.component_type::text) IN ('energy', 'fixed', 'demand')
            ) = 0
            ORDER BY u.state_province, u.name, t.name
        """), params).fetchall()

        no_core_ids = [r[0] for r in rows]
        print(f"Found {len(no_core_ids)} tariffs with no core components:")
        for r in rows:
            print(f"  [{r[4]}] {r[3]:<35s} | {r[1]:<45s} | {r[2]:<12s} | comps: {r[5]} ({r[7] or 'none'})")

        # ----- PASS 2: Prefix-duplicate merge -----
        print(f"\n=== PASS 2: Prefix-duplicate tariffs (same utility + class) ===\n")

        all_tariffs = session.execute(text(f"""
            SELECT t.id, t.utility_id, t.name, t.customer_class::text,
                   u.name as utility_name, u.state_province,
                   COUNT(rc.id) as comp_count,
                   COUNT(rc.id) FILTER (
                       WHERE LOWER(rc.component_type::text) IN ('energy', 'fixed', 'demand')
                   ) as core_count
            FROM tariffs t
            JOIN utilities u ON u.id = t.utility_id
            LEFT JOIN rate_components rc ON rc.tariff_id = t.id
            WHERE {where}
            GROUP BY t.id, t.utility_id, t.name, t.customer_class, u.name, u.state_province
            ORDER BY u.state_province, u.name, t.name
        """), params).fetchall()

        groups: dict[tuple, list] = defaultdict(list)
        for r in all_tariffs:
            key = (r[1], r[3])  # (utility_id, customer_class)
            groups[key].append(r)

        dup_ids_to_delete: list[int] = []
        for key, group in groups.items():
            if len(group) < 2:
                continue
            norms = [(_normalize(r[2]), r) for r in group]
            absorbed: set[int] = set()

            for i, (ni, ri) in enumerate(norms):
                if ri[0] in absorbed:
                    continue
                for j, (nj, rj) in enumerate(norms):
                    if j <= i or rj[0] in absorbed:
                        continue
                    if ni == nj:
                        is_dup = True
                    elif ni.startswith(nj) or nj.startswith(ni):
                        shorter, longer = (ni, nj) if len(ni) <= len(nj) else (nj, ni)
                        suffix = longer[len(shorter):].strip()
                        # Require suffix to be 4+ chars to avoid merging
                        # Quebec-style rate codes (D→DM, G→G9, etc.)
                        is_dup = len(suffix) >= 4
                    else:
                        is_dup = False

                    if not is_dup:
                        continue

                    keep, lose = (ri, rj) if (ri[7], ri[6]) >= (rj[7], rj[6]) else (rj, ri)
                    absorbed.add(lose[0])
                    dup_ids_to_delete.append(lose[0])
                    print(f"  [{lose[5]}] {lose[4]:<35s} | REMOVE '{lose[2]}' ({lose[6]} comp, {lose[7]} core)")
                    print(f"  {'':35s}   KEEP   '{keep[2]}' ({keep[6]} comp, {keep[7]} core)")
                    if lose[0] == ri[0]:
                        break  # outer item absorbed, stop comparing it

        # ----- Combine and execute -----
        all_delete_ids = list(set(no_core_ids + dup_ids_to_delete))
        # Avoid double-counting: some pass-1 tariffs may also be pass-2 duplicates
        pass1_only = set(no_core_ids) - set(dup_ids_to_delete)
        pass2_only = set(dup_ids_to_delete) - set(no_core_ids)
        both = set(no_core_ids) & set(dup_ids_to_delete)

        print(f"\n=== SUMMARY ===")
        print(f"  Pass 1 (no core components):  {len(no_core_ids)}")
        print(f"  Pass 2 (prefix duplicates):   {len(dup_ids_to_delete)}")
        print(f"  Overlap (in both passes):     {len(both)}")
        print(f"  Total unique to delete:       {len(all_delete_ids)}")

        if not all_delete_ids:
            print("\nNothing to delete!")
            return

        if dry_run:
            print(f"\n  DRY RUN — no changes made. Re-run without --dry-run to commit.\n")
            return

        # Delete rate components first, then tariffs
        print(f"\n  Deleting {len(all_delete_ids)} tariffs and their components...")
        session.execute(text(
            "DELETE FROM rate_components WHERE tariff_id = ANY(:ids)"
        ), {"ids": all_delete_ids})
        result = session.execute(text(
            "DELETE FROM tariffs WHERE id = ANY(:ids)"
        ), {"ids": all_delete_ids})
        session.commit()
        print(f"  Deleted {result.rowcount} tariffs. Done.\n")


def main():
    parser = argparse.ArgumentParser(description="Clean up duplicate/incomplete tariffs")
    parser.add_argument("--country", help="Filter by country code (CA, US)")
    parser.add_argument("--province", help="Filter by single province/state code (e.g. NS, ON)")
    parser.add_argument("--states", help="Comma-separated state codes (e.g. ME,NH,VT)")
    parser.add_argument("--dry-run", action="store_true", help="Preview only, don't delete")
    args = parser.parse_args()

    if not args.country and not args.province and not args.states:
        print("Error: provide at least --country, --province, or --states")
        sys.exit(1)

    states_list = [s.strip() for s in args.states.split(",")] if args.states else None
    run_cleanup(args.country, args.province, states_list, args.dry_run)


if __name__ == "__main__":
    main()
