"""One-shot repair: Newfoundland Power Rate #1.1S Domestic Seasonal.

Creates or updates a live 2026-07-01 Rate #1.1S with two all-in ENERGY
rows (Winter Dec–Apr, Non-Winter May–Nov) and soft-supersedes the stale
2025 seasonal row. Soft-supersede only — never hard-delete.

Root cause: the 2025 extraction stored base ENERGY + seasonal ADJUSTMENT
riders. Flux / Lookup group ENERGY by season and skip ADJUSTMENT, so
Non-Winter never appeared and Winter showed the wrong rate.

Target all-in (2026 Rate #1.1 base 0.15587 $/kWh ± published riders):
  Winter (Dec–Apr):     0.16540 $/kWh  (= 0.15587 + 0.00953)
  Non-Winter (May–Nov): 0.14290 $/kWh  (= 0.15587 - 0.01297)

Fixed / amp-tier customer charges are NOT copied from Rate #1.1: the
#1.1S schedule page only restates energy charges subject to seasonal
adjustments. Customers on #1.1S remain served under #1.1 for customer
charges, but inventing fixed rows here would be a guess.

Dry-run is the default; pass ``--apply`` to write.

Usage:
  python -m scripts.repair_nf_seasonal_11s --utility-name "Newfoundland Power"
  python -m scripts.repair_nf_seasonal_11s --utility-name "Newfoundland Power" --apply
  python -m scripts.repair_nf_seasonal_11s --utility-id 123 --apply
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.db.session import get_sync_engine
from app.models import Tariff, Utility
from app.models.tariff import (
    ComponentType,
    CustomerClass,
    RateComponent,
    RateType,
)

# Published 2026 Rate #1.1 energy + Rate #1.1S seasonal riders (dollars).
NF_11_BASE_ENERGY_2026 = 0.15587
NF_11S_WINTER_PREMIUM = 0.00953   # +0.953 ¢/kWh
NF_11S_NONWINTER_CREDIT = -0.01297  # (1.297) ¢/kWh

NF_11S_EFFECTIVE = date(2026, 7, 1)
NF_11S_NAME = "Rate #1.1S Domestic Seasonal - Optional"
NF_11S_CODE = "1.1S"
NF_11S_WINTER_SEASON = "Winter (Dec–Apr)"
NF_11S_NONWINTER_SEASON = "Non-Winter (May–Nov)"


def build_nf_11s_all_in_components(
    base_energy: float = NF_11_BASE_ENERGY_2026,
    *,
    winter_premium: float = NF_11S_WINTER_PREMIUM,
    nonwinter_credit: float = NF_11S_NONWINTER_CREDIT,
) -> list[dict]:
    """Pure helper: two all-in ENERGY rows for Rate #1.1S."""
    winter = round(base_energy + winter_premium, 6)
    nonwinter = round(base_energy + nonwinter_credit, 6)
    return [
        {
            "component_type": "energy",
            "unit": "$/kWh",
            "rate_value": winter,
            "tier_label": NF_11S_WINTER_SEASON,
            "season": NF_11S_WINTER_SEASON,
        },
        {
            "component_type": "energy",
            "unit": "$/kWh",
            "rate_value": nonwinter,
            "tier_label": NF_11S_NONWINTER_SEASON,
            "season": NF_11S_NONWINTER_SEASON,
        },
    ]


def _is_live(t: Tariff) -> bool:
    return t.superseded_by_tariff_id is None and t.supersede_reason is None


def _name_l(t: Tariff) -> str:
    return (t.name or "").lower()


def _code_l(t: Tariff) -> str:
    return (t.code or "").lower().lstrip("#")


def is_nf_11s_candidate(t: Tariff) -> bool:
    """Match live Domestic Seasonal / Rate #1.1S rows."""
    name = _name_l(t)
    code = _code_l(t)
    if code in {"1.1s", "11s"}:
        return True
    if "1.1s" in name.replace(" ", ""):
        return True
    if "seasonal" in name and "domestic" in name:
        return True
    if "domestic seasonal" in name:
        return True
    return False


def is_nf_11_base(t: Tariff) -> bool:
    """Match live Rate #1.1 Domestic Service (not 1.1S / TOU / heating)."""
    name = _name_l(t)
    code = _code_l(t)
    if "1.1s" in name.replace(" ", "") or code == "1.1s":
        return False
    if any(tok in name for tok in ("tou", "heating", "seasonal", "optional")):
        return False
    if code == "1.1":
        return True
    if "1.1" in name and "domestic" in name:
        return True
    return False


def pick_source_url(live: list[Tariff], seasonal_rows: list[Tariff]) -> str | None:
    """Prefer the 2026 Rate #1.1 source_url; fall back to any #1.1S URL."""
    elevens = [
        t for t in live
        if is_nf_11_base(t) and t.effective_date == NF_11S_EFFECTIVE and t.source_url
    ]
    if elevens:
        elevens.sort(
            key=lambda t: (t.last_verified_at or datetime.min.replace(tzinfo=timezone.utc)),
            reverse=True,
        )
        return elevens[0].source_url
    for t in seasonal_rows:
        if t.source_url:
            return t.source_url
    return None


def find_base_energy(live: list[Tariff]) -> float:
    """Use live 2026 Rate #1.1 ENERGY if present; else published constant."""
    elevens = [
        t for t in live
        if is_nf_11_base(t) and t.effective_date == NF_11S_EFFECTIVE
    ]
    for t in elevens:
        for rc in t.rate_components or []:
            ctype = (
                rc.component_type.value
                if hasattr(rc.component_type, "value")
                else str(rc.component_type)
            )
            if ctype.lower() != "energy":
                continue
            unit = (rc.unit or "").lower().replace(" ", "")
            if "kwh" not in unit:
                continue
            try:
                return round(float(rc.rate_value), 6)
            except (TypeError, ValueError):
                continue
    return NF_11_BASE_ENERGY_2026


def _components_match_target(tariff: Tariff, target: list[dict]) -> bool:
    energy = []
    for rc in tariff.rate_components or []:
        ctype = (
            rc.component_type.value
            if hasattr(rc.component_type, "value")
            else str(rc.component_type)
        )
        if ctype.lower() != "energy":
            continue
        energy.append(
            (
                round(float(rc.rate_value), 6),
                (rc.season or "").strip().lower(),
            )
        )
    expected = {
        (round(float(c["rate_value"]), 6), str(c["season"]).strip().lower())
        for c in target
    }
    return set(energy) == expected and tariff.effective_date == NF_11S_EFFECTIVE


def repair_utility(session: Session, utility: Utility, dry_run: bool) -> dict:
    live = session.execute(
        select(Tariff)
        .where(
            Tariff.utility_id == utility.id,
            Tariff.superseded_by_tariff_id.is_(None),
            Tariff.supersede_reason.is_(None),
        )
        .options(selectinload(Tariff.rate_components))
    ).scalars().all()

    seasonal = [t for t in live if is_nf_11s_candidate(t)]
    if not seasonal and not any(is_nf_11_base(t) for t in live):
        print(f"  [{utility.name}] no #1.1 / #1.1S candidates found — skip")
        return {
            "utility_id": utility.id,
            "created": 0,
            "updated": 0,
            "superseded": 0,
            "skipped": True,
        }

    base_energy = find_base_energy(live)
    target_comps = build_nf_11s_all_in_components(base_energy)
    source_url = pick_source_url(live, seasonal)

    print(f"  [{utility.name}] base ENERGY={base_energy} $/kWh")
    for c in target_comps:
        print(
            f"    target ENERGY {c['season']}: {c['rate_value']} $/kWh"
        )
    if source_url:
        print(f"    source_url: {source_url}")
    else:
        print("    source_url: (none found on live #1.1 / #1.1S)")

    # Prefer an already-correct 2026 keeper; else create one.
    keeper = None
    for t in seasonal:
        if _components_match_target(t, target_comps):
            keeper = t
            break
    if keeper is None:
        for t in seasonal:
            if t.effective_date == NF_11S_EFFECTIVE:
                keeper = t
                break

    created = 0
    updated = 0
    superseded = 0

    if keeper is None:
        print(
            f"    CREATE '{NF_11S_NAME}' code={NF_11S_CODE} "
            f"eff={NF_11S_EFFECTIVE}"
        )
        if not dry_run:
            keeper = Tariff(
                utility_id=utility.id,
                name=NF_11S_NAME,
                code=NF_11S_CODE,
                customer_class=CustomerClass.RESIDENTIAL,
                rate_type=RateType.SEASONAL,
                description=(
                    "Optional domestic seasonal rate: Rate #1.1 energy "
                    "charges subject to winter premium and non-winter credit."
                ),
                effective_date=NF_11S_EFFECTIVE,
                source_url=source_url,
                last_verified_at=datetime.now(timezone.utc),
                approved=True,
                confidence_score=0.95,
                confidence_factors={
                    "repair": "repair_nf_seasonal_11s",
                    "note": "all-in ENERGY from #1.1 base ± #1.1S riders",
                },
            )
            for c in target_comps:
                keeper.rate_components.append(
                    RateComponent(
                        component_type=ComponentType.ENERGY,
                        unit=c["unit"],
                        rate_value=c["rate_value"],
                        tier_label=c.get("tier_label"),
                        season=c.get("season"),
                    )
                )
            session.add(keeper)
            session.flush()
        created = 1
    elif not _components_match_target(keeper, target_comps):
        print(
            f"    UPDATE id={keeper.id} '{keeper.name}' → "
            f"eff={NF_11S_EFFECTIVE}, two all-in ENERGY seasons"
        )
        if not dry_run:
            keeper.name = NF_11S_NAME
            keeper.code = NF_11S_CODE
            keeper.rate_type = RateType.SEASONAL
            keeper.effective_date = NF_11S_EFFECTIVE
            if source_url:
                keeper.source_url = source_url
            keeper.last_verified_at = datetime.now(timezone.utc)
            # Soft-repair: replace components in place (cascade orphan deletes
            # the old rows). Soft-supersede is used for sibling tariffs only.
            keeper.rate_components.clear()
            for c in target_comps:
                keeper.rate_components.append(
                    RateComponent(
                        component_type=ComponentType.ENERGY,
                        unit=c["unit"],
                        rate_value=c["rate_value"],
                        tier_label=c.get("tier_label"),
                        season=c.get("season"),
                    )
                )
            factors = dict(keeper.confidence_factors or {})
            factors["repair"] = "repair_nf_seasonal_11s"
            keeper.confidence_factors = factors
        updated = 1
    else:
        print(
            f"    KEEP id={keeper.id} '{keeper.name}' "
            f"(already matches 2026 all-in ENERGY shape)"
        )

    # Soft-supersede older / incomplete seasonal siblings.
    for loser in seasonal:
        if keeper is not None and loser.id == getattr(keeper, "id", None):
            continue
        if keeper is None:
            # Dry-run create path: treat all existing seasonal rows as losers.
            print(
                f"    SUPERSEDE id={loser.id} '{loser.name}' "
                f"eff={loser.effective_date} → new 2026 #1.1S "
                f"(reason=vintage)"
            )
            superseded += 1
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

    print(
        "    Note: Rate #1.1 amp-tier fixed charges are NOT attached to "
        "#1.1S (schedule restates energy only)."
    )

    return {
        "utility_id": utility.id,
        "utility_name": utility.name,
        "created": created,
        "updated": updated,
        "superseded": superseded,
        "skipped": False,
        "base_energy": base_energy,
        "target_components": target_comps,
    }


def resolve_utilities(session: Session, args: argparse.Namespace) -> list[Utility]:
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
    raise SystemExit("Provide --utility-name or --utility-id")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Create/update Newfoundland Power Rate #1.1S with all-in "
            "seasonal ENERGY and soft-supersede the stale 2025 row"
        )
    )
    parser.add_argument(
        "--utility-name",
        default="Newfoundland Power",
        help="Substring match on utility name (default: Newfoundland Power)",
    )
    parser.add_argument("--utility-id", type=int, help="Exact utility id")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes (default is dry-run)",
    )
    args = parser.parse_args(argv)

    dry_run = not args.apply
    mode = "DRY RUN" if dry_run else "APPLY"
    print(f"=== repair_nf_seasonal_11s ({mode}) ===\n")

    engine = get_sync_engine()
    totals = {"created": 0, "updated": 0, "superseded": 0, "utilities": 0}

    with Session(engine) as session:
        utilities = resolve_utilities(session, args)
        for utility in utilities:
            print(f"-- {utility.name} (id={utility.id})")
            result = repair_utility(session, utility, dry_run=dry_run)
            if result.get("skipped"):
                continue
            totals["created"] += result["created"]
            totals["updated"] += result["updated"]
            totals["superseded"] += result["superseded"]
            totals["utilities"] += 1

        if dry_run:
            session.rollback()
            print(
                f"\nDRY RUN complete — would create {totals['created']}, "
                f"update {totals['updated']}, supersede {totals['superseded']} "
                f"across {totals['utilities']} utilities."
            )
            print("Re-run with --apply to write changes.")
        else:
            session.commit()
            print(
                f"\nApplied — created {totals['created']}, "
                f"updated {totals['updated']}, superseded {totals['superseded']} "
                f"across {totals['utilities']} utilities."
            )

        print(
            "\nExpected NF outcome after --apply:\n"
            f"  LIVE  {NF_11S_NAME} eff {NF_11S_EFFECTIVE}\n"
            f"        ENERGY {NF_11S_WINTER_SEASON} "
            f"{round(NF_11_BASE_ENERGY_2026 + NF_11S_WINTER_PREMIUM, 6)} $/kWh\n"
            f"        ENERGY {NF_11S_NONWINTER_SEASON} "
            f"{round(NF_11_BASE_ENERGY_2026 + NF_11S_NONWINTER_CREDIT, 6)} $/kWh\n"
            "  SUPERSEDE older Domestic Seasonal / #1.1S vintages "
            "(reason=vintage)\n"
            "  Fixed amp-tier charges left on Rate #1.1 only"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
