"""Cleanup: retire duplicate and incomplete tariffs (soft supersede).

Two passes:
  1. Retire tariffs that have ZERO energy/fixed/demand components (rate
     riders only) — supersede_reason='no_core_components'.
  2. Merge prefix-duplicate names within the same utility + customer class,
     keeping the best version (protected first, then verified, then most
     core/total components) and pointing the rest at it via
     superseded_by_tariff_id (supersede_reason='dup_cleanup').

Protected rows (approved / repair / manual — see
app.services.tariff_history.is_protected) are never retired by either
pass; when both rows of a duplicate pair are protected the pair is left
for a human. Rows are never hard-deleted: the soft-supersede columns keep
the audit trail, each retirement is logged to tariff_change_events, and the
API filters retired rows out. Already-superseded rows and rows that other
tariffs point at (absorb targets) are excluded from both passes.

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
from app.models import Tariff
from app.services.tariff_history import is_protected, supersede_tariff


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


def _protected(approved, confidence_factors) -> bool:
    return is_protected({"approved": approved, "confidence_factors": confidence_factors})


def run_cleanup(country: str | None, province: str | None, states: list[str] | None, dry_run: bool):
    where, params = _build_where(country, province, states)
    engine = get_sync_engine()

    with Session(engine) as session:
        # Rows other tariffs point at must keep existing (absorb targets).
        successor_ids = {
            r[0] for r in session.execute(text(
                "SELECT DISTINCT superseded_by_tariff_id FROM tariffs "
                "WHERE superseded_by_tariff_id IS NOT NULL"
            )).fetchall()
        }

        # ----- PASS 1: Retire tariffs with no core components -----
        print("\n=== PASS 1: Tariffs with no energy/fixed/demand components ===\n")

        rows = session.execute(text(f"""
            SELECT t.id, t.name, t.customer_class::text, u.name as utility_name,
                   u.state_province,
                   COUNT(rc.id) as total_comps,
                   COUNT(rc.id) FILTER (
                       WHERE LOWER(rc.component_type::text) IN ('energy', 'fixed', 'demand')
                   ) as core_comps,
                   STRING_AGG(DISTINCT rc.component_type::text, ', ') as comp_types,
                   t.approved, t.confidence_factors
            FROM tariffs t
            JOIN utilities u ON u.id = t.utility_id
            LEFT JOIN rate_components rc ON rc.tariff_id = t.id
            WHERE {where}
              AND t.superseded_by_tariff_id IS NULL
              AND t.supersede_reason IS NULL
            GROUP BY t.id, t.name, t.customer_class, u.name, u.state_province
            HAVING COUNT(rc.id) FILTER (
                WHERE LOWER(rc.component_type::text) IN ('energy', 'fixed', 'demand')
            ) = 0
            ORDER BY u.state_province, u.name, t.name
        """), params).fetchall()
        rows = [
            r for r in rows
            if r[0] not in successor_ids and not _protected(r[8], r[9])
        ]

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
                   ) as core_count,
                   (t.last_verified_at IS NOT NULL) as is_verified,
                   t.approved, t.confidence_factors
            FROM tariffs t
            JOIN utilities u ON u.id = t.utility_id
            LEFT JOIN rate_components rc ON rc.tariff_id = t.id
            WHERE {where}
              AND t.superseded_by_tariff_id IS NULL
              AND t.supersede_reason IS NULL
            GROUP BY t.id, t.utility_id, t.name, t.customer_class, u.name,
                     u.state_province, t.last_verified_at
            ORDER BY u.state_province, u.name, t.name
        """), params).fetchall()
        all_tariffs = [r for r in all_tariffs if r[0] not in successor_ids]

        groups: dict[tuple, list] = defaultdict(list)
        for r in all_tariffs:
            key = (r[1], r[3])  # (utility_id, customer_class)
            groups[key].append(r)

        dup_pairs: list[tuple[int, int]] = []  # (lose_id, keep_id)
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

                    # Keeper: protected (approved / repair / manual) beats
                    # scraped, then verified beats unverified (a fresh
                    # extraction must never lose to a stale seed), then most
                    # core components, then most total components. Two
                    # protected rows are never auto-merged.
                    pi, pj = _protected(ri[9], ri[10]), _protected(rj[9], rj[10])
                    if pi and pj:
                        print(f"  [{ri[5]}] {ri[4]:<35s} | HOLD both protected: '{ri[2]}' / '{rj[2]}'")
                        continue
                    keep, lose = (ri, rj) if (pi, ri[8], ri[7], ri[6]) >= (pj, rj[8], rj[7], rj[6]) else (rj, ri)
                    absorbed.add(lose[0])
                    dup_pairs.append((lose[0], keep[0]))
                    print(f"  [{lose[5]}] {lose[4]:<35s} | RETIRE '{lose[2]}' ({lose[6]} comp, {lose[7]} core, verified={lose[8]})")
                    print(f"  {'':35s}   KEEP   '{keep[2]}' ({keep[6]} comp, {keep[7]} core, verified={keep[8]})")
                    if lose[0] == ri[0]:
                        break  # outer item absorbed, stop comparing it

        # ----- Combine and execute (soft supersede, never DELETE) -----
        dup_lose_ids = {lose for lose, _ in dup_pairs}
        both = set(no_core_ids) & dup_lose_ids
        # When a row is in both passes, the dup pointer wins (it names a
        # surviving successor instead of just retiring the row).
        pass1_only_ids = list(set(no_core_ids) - dup_lose_ids)
        total = len(pass1_only_ids) + len(dup_pairs)

        print(f"\n=== SUMMARY ===")
        print(f"  Pass 1 (no core components):  {len(no_core_ids)}")
        print(f"  Pass 2 (prefix duplicates):   {len(dup_pairs)}")
        print(f"  Overlap (in both passes):     {len(both)}")
        print(f"  Total unique to retire:       {total}")

        if not total:
            print("\nNothing to retire!")
            return

        if dry_run:
            print(f"\n  DRY RUN — no changes made. Re-run without --dry-run to commit.\n")
            return

        print(f"\n  Retiring {total} tariffs (soft supersede)...")
        event_kw = {"actor_type": "cleanup", "actor_id": "cleanup_duplicate_tariffs"}
        for tid in pass1_only_ids:
            supersede_tariff(
                session, session.get(Tariff, tid), reason="no_core_components", **event_kw
            )
        for lose_id, keep_id in dup_pairs:
            supersede_tariff(
                session, session.get(Tariff, lose_id),
                successor_id=keep_id, reason="dup_cleanup", **event_kw,
            )
        session.commit()
        print(f"  Retired {total} tariffs. Done.\n")


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
