"""One-time / on-demand repair: soft-supersede older rate-book vintages
and dedupe fixed/minimum rate components on the keeper.

Addresses the Newfoundland Power Rate #1.1 incident where two live
residential Domestic Flat tariffs both showed Flux "Current":

  - Rate #1.1 Domestic Service, effective 2026-07-01  (keeper)
  - Domestic Service (Flat), effective 2025-07-01     (stale rate book)

and amp-tier basic charges were stored twice (fixed + equal minimum).

Soft-supersede only — never hard-delete (AGENTS.md live invariant).
Dry-run is the default; pass ``--apply`` to write.

Usage:
  # Preview Newfoundland Power only (default dry-run)
  python -m scripts.repair_vintage_tariffs --utility-name "Newfoundland Power"

  # Apply for Newfoundland Power
  python -m scripts.repair_vintage_tariffs --utility-name "Newfoundland Power" --apply

  # Preview / apply by utility id
  python -m scripts.repair_vintage_tariffs --utility-id 123 --apply

  # Run the same safe rule across all utilities (still soft-supersede only)
  python -m scripts.repair_vintage_tariffs --all --apply
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.db.session import get_sync_engine
from app.models import Tariff, Utility
from app.models.tariff import RateComponent
from scripts.tariff_pipeline import (
    _component_dedupe_key,
    choose_vintage_keeper,
    dedupe_rate_components,
    group_live_tariffs_by_vintage,
    supersede_older_vintages,
)


def _component_as_dict(rc: RateComponent) -> dict:
    ctype = (
        rc.component_type.value
        if hasattr(rc.component_type, "value")
        else str(rc.component_type)
    )
    return {
        "component_type": ctype,
        "unit": rc.unit,
        "rate_value": float(rc.rate_value) if rc.rate_value is not None else 0.0,
        "tier_label": rc.tier_label,
        "period_label": rc.period_label,
        "season": rc.season,
        "tier_min_kwh": rc.tier_min_kwh,
        "tier_max_kwh": rc.tier_max_kwh,
        "period_start_time": rc.period_start_time,
        "period_end_time": rc.period_end_time,
        "day_type": rc.day_type,
        "season_start_month": rc.season_start_month,
        "season_start_day": rc.season_start_day,
        "season_end_month": rc.season_end_month,
        "season_end_day": rc.season_end_day,
        "_orm_id": rc.id,
    }


def dedupe_keeper_components(session: Session, tariff: Tariff, dry_run: bool) -> int:
    """Collapse fixed/minimum twins on a keeper tariff. Returns rows removed."""
    comps = list(tariff.rate_components or [])
    if len(comps) < 2:
        return 0

    as_dicts = [_component_as_dict(rc) for rc in comps]
    before = len(as_dicts)
    plain = [{k: v for k, v in d.items() if k != "_orm_id"} for d in as_dicts]
    deduped = dedupe_rate_components(plain)
    if len(deduped) == before:
        return 0

    kept_keys: list[tuple] = []
    for d in deduped:
        ctype = str(d.get("component_type") or "").lower()
        kept_keys.append((ctype, _component_dedupe_key(d)))

    orm_by_key: dict[tuple, list[RateComponent]] = defaultdict(list)
    for rc, d in zip(comps, as_dicts):
        ctype = str(d["component_type"]).lower()
        orm_by_key[(ctype, _component_dedupe_key(d))].append(rc)

    keep_ids: set[int] = set()
    for ctype, key in kept_keys:
        candidates = orm_by_key.get((ctype, key)) or []
        if not candidates and ctype == "fixed":
            # Cross-type collapse preferred fixed; ORM row may still be minimum.
            candidates = orm_by_key.get(("minimum", key)) or []
            if candidates:
                # Retype the surviving row to fixed so Flux renders one fixed charge.
                if not dry_run:
                    from app.models.tariff import ComponentType

                    candidates[0].component_type = ComponentType.FIXED
        if candidates:
            keep_ids.add(candidates[0].id)

    remove = [rc for rc in comps if rc.id not in keep_ids]
    if not remove:
        return 0

    print(
        f"    Component dedupe on tariff {tariff.id} '{tariff.name}': "
        f"remove {len(remove)} of {before} rows"
    )
    for rc in remove:
        ctype = (
            rc.component_type.value
            if hasattr(rc.component_type, "value")
            else rc.component_type
        )
        label = rc.tier_label or rc.period_label or ""
        print(f"      - id={rc.id} {ctype} {rc.rate_value} {label}")

    if not dry_run:
        for rc in remove:
            session.delete(rc)
    return len(remove)


def repair_utility(session: Session, utility: Utility, dry_run: bool) -> dict:
    """Soft-supersede older vintages + dedupe components on keepers."""
    live = session.execute(
        select(Tariff)
        .where(
            Tariff.utility_id == utility.id,
            Tariff.superseded_by_tariff_id.is_(None),
            Tariff.supersede_reason.is_(None),
        )
        .options(selectinload(Tariff.rate_components))
    ).scalars().all()

    by_class: dict = defaultdict(list)
    for t in live:
        by_class[t.customer_class].append(t)

    superseded = 0
    components_removed = 0
    keeper_ids: set[int] = set()

    for cc, rows in by_class.items():
        groups = group_live_tariffs_by_vintage(rows)
        for group in groups:
            keeper = choose_vintage_keeper(group)
            keeper_ids.add(keeper.id)
            print(
                f"  [{utility.name}] {cc} vintage group ({len(group)} live): "
                f"KEEP id={keeper.id} '{keeper.name}' "
                f"eff={keeper.effective_date} code={keeper.code!r}"
            )
            for loser in group:
                if loser.id == keeper.id:
                    continue
                print(
                    f"    SUPERSEDE id={loser.id} '{loser.name}' "
                    f"eff={loser.effective_date} → keeper {keeper.id} "
                    f"(reason=vintage)"
                )
                if not dry_run:
                    loser.superseded_by_tariff_id = keeper.id
                    loser.supersede_reason = "vintage"
                superseded += 1

    # Prefer keepers; also scan domestic/#1.1 singletons so a lone keeper
    # still gets component cleanup after a prior partial repair.
    targets = [t for t in live if t.id in keeper_ids]
    seen = {t.id for t in targets}
    for t in live:
        if t.id in seen:
            continue
        name_l = (t.name or "").lower()
        code_l = (t.code or "").lower()
        if "1.1" in name_l or code_l in {"1.1", "#1.1"} or "domestic" in name_l:
            targets.append(t)

    if not targets:
        targets = list(live)

    for t in targets:
        components_removed += dedupe_keeper_components(session, t, dry_run)

    # When applying, also run the shared write-time helper so behavior stays
    # identical to store_tariffs (idempotent if we already supersede above).
    if not dry_run and superseded == 0:
        superseded = supersede_older_vintages(session, utility.id)

    return {
        "utility_id": utility.id,
        "utility_name": utility.name,
        "superseded": superseded,
        "components_removed": components_removed,
    }


def resolve_utilities(session: Session, args: argparse.Namespace) -> list[Utility]:
    if args.all:
        return list(session.execute(select(Utility).order_by(Utility.id)).scalars().all())
    if args.utility_id is not None:
        u = session.get(Utility, args.utility_id)
        if not u:
            raise SystemExit(f"No utility with id={args.utility_id}")
        return [u]
    if args.utility_name:
        rows = session.execute(
            select(Utility).where(Utility.name.ilike(f"%{args.utility_name}%"))
        ).scalars().all()
        if not rows:
            raise SystemExit(f"No utility matching name {args.utility_name!r}")
        return list(rows)
    raise SystemExit("Provide --utility-name, --utility-id, or --all")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Soft-supersede older rate-book vintages and dedupe "
            "fixed/minimum components"
        )
    )
    parser.add_argument(
        "--utility-name",
        help="Substring match on utility name (e.g. 'Newfoundland Power')",
    )
    parser.add_argument("--utility-id", type=int, help="Exact utility id")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run for every utility (still soft-supersede only)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes (default is dry-run)",
    )
    args = parser.parse_args(argv)

    dry_run = not args.apply
    mode = "DRY RUN" if dry_run else "APPLY"
    print(f"=== repair_vintage_tariffs ({mode}) ===\n")

    engine = get_sync_engine()
    totals = {"superseded": 0, "components_removed": 0, "utilities": 0}

    with Session(engine) as session:
        utilities = resolve_utilities(session, args)
        for utility in utilities:
            print(f"-- {utility.name} (id={utility.id})")
            result = repair_utility(session, utility, dry_run=dry_run)
            totals["superseded"] += result["superseded"]
            totals["components_removed"] += result["components_removed"]
            totals["utilities"] += 1
            if result["superseded"] == 0 and result["components_removed"] == 0:
                print("    (nothing to repair)")

        if dry_run:
            session.rollback()
            print(
                f"\nDRY RUN complete — would supersede {totals['superseded']} tariffs "
                f"and remove {totals['components_removed']} components across "
                f"{totals['utilities']} utilities."
            )
            print("Re-run with --apply to write changes.")
        else:
            session.commit()
            print(
                f"\nApplied — superseded {totals['superseded']} tariffs, "
                f"removed {totals['components_removed']} components across "
                f"{totals['utilities']} utilities."
            )

        if any("newfoundland" in (u.name or "").lower() for u in utilities):
            print(
                "\nExpected NF outcome after --apply:\n"
                "  KEEP  Rate #1.1 Domestic Service (effective 2026-07-01)\n"
                "  SUPERSEDE older Domestic Service (Flat) vintages "
                "(reason=vintage)\n"
                "  Dedupe amp-tier fixed/minimum twins on the keeper"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
