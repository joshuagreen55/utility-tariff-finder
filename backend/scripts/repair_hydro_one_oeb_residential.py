"""Repair: Hydro One (Ontario) residential RPP → OEB seasonal TOU clocks.

Soft-supersedes incomplete live residential keepers (label-only TOU prices
with no structured clock windows / season dates) and creates keepers matching
the Ontario Energy Board Regulated Price Plan effective Nov 1, 2025:

  https://www.oeb.ca/consumer-information-and-protection/electricity-rates

Gold commodity ENERGY ($/kWh from ¢/kWh):

  TOU: Off-Peak 0.098 / Mid-Peak 0.157 / On-Peak 0.203
  Tiered: Tier 1 0.120 / Tier 2 0.142 (summer threshold 600 kWh, winter 1000)
  ULO: Overnight 0.039 / Weekend Off-Peak 0.098 / Mid-Peak 0.157 / On-Peak 0.391

TOU seasons + weekday clocks come from the in-repo OEB schedule constants
(``WINTER_TOU_SCHEDULE`` / ``SUMMER_TOU_SCHEDULE`` in scrape_oeb_rates) —
overnight off-peak is stored as 00:00–07:00 + 19:00–24:00 (≡ 19:00–07:00 wrap).
Weekends and holidays are Off-Peak all day (00:00–00:00) both seasons.

FIXED / delivery charges are Hydro One–specific and are **not** on the OEB
commodity table. This repair never invents FIXED; if a live incomplete keeper
already has FIXED rows, they are copied onto the new keeper.

Scoped to utility name **Hydro One** only (not Remote Communities, not other
Ontario LDCs). Soft-supersede only (``supersede_reason='vintage'``).

Dry-run is the default; pass ``--apply`` to write. Idempotent when live
keepers already match the structured OEB target shape.

Usage:
  python -m scripts.repair_hydro_one_oeb_residential
  python -m scripts.repair_hydro_one_oeb_residential --plan tou
  python -m scripts.repair_hydro_one_oeb_residential --plan tou --apply
  python -m scripts.repair_hydro_one_oeb_residential --plan all --apply
  python -m scripts.repair_hydro_one_oeb_residential --utility-id 123 --apply
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import date, datetime, time, timezone
from typing import Any

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
from app.services.tariff_history import record_event, supersede_tariff
from app.services.tou_seasonal_completeness import (
    evaluate_tariff_completeness,
    is_complete,
)
from scripts.scrape_oeb_rates import (
    OEBRateSet,
    SOURCE_URL as OEB_HISTORICAL_URL,
    TieredRates,
    TOURates,
    ULORates,
    build_tariff_entries,
)

# ---------------------------------------------------------------------------
# OEB Nov 1, 2025 gold (consumer rates page)
# ---------------------------------------------------------------------------

HYDRO_ONE_NAME = "Hydro One"
# Audit / prod narrative: residential TOU keeper with On/Mid/Off labels only.
KNOWN_STALE_TOU_ID = 46746

OEB_EFFECTIVE = date(2025, 11, 1)
OEB_CONSUMER_URL = (
    "https://www.oeb.ca/consumer-information-and-protection/electricity-rates"
)

# TOU ¢ → $
OEB_TOU_OFF = 0.098   # 9.8 ¢
OEB_TOU_MID = 0.157   # 15.7 ¢
OEB_TOU_ON = 0.203    # 20.3 ¢

# Tiered
OEB_TIER_LOWER = 0.120   # 12.0 ¢
OEB_TIER_HIGHER = 0.142  # 14.2 ¢
OEB_TIER_SUMMER_KWH = 600
OEB_TIER_WINTER_KWH = 1000

# ULO
OEB_ULO_OVERNIGHT = 0.039   # 3.9 ¢
OEB_ULO_WEEKEND_OFF = 0.098  # 9.8 ¢
OEB_ULO_MID = 0.157          # 15.7 ¢
OEB_ULO_ON = 0.391           # 39.1 ¢

PLAN_KEYS = ("tou", "tiered", "ulo")

_EVENT_KW = {"actor_type": "script", "actor_id": "repair_hydro_one_oeb_residential"}


def gold_oeb_rate_set() -> OEBRateSet:
    """Published Nov 1, 2025 RPP commodity rates (no invented figures)."""
    eff = OEB_EFFECTIVE.isoformat()
    return OEBRateSet(
        tou=TOURates(
            effective_date=eff,
            off_peak=OEB_TOU_OFF,
            mid_peak=OEB_TOU_MID,
            on_peak=OEB_TOU_ON,
        ),
        tiered=TieredRates(
            effective_date=eff,
            lower_tier_price=OEB_TIER_LOWER,
            higher_tier_price=OEB_TIER_HIGHER,
            summer_threshold_kwh=OEB_TIER_SUMMER_KWH,
            winter_threshold_kwh=OEB_TIER_WINTER_KWH,
        ),
        ulo=ULORates(
            effective_date=eff,
            ultra_low_overnight=OEB_ULO_OVERNIGHT,
            weekend_off_peak=OEB_ULO_WEEKEND_OFF,
            mid_peak=OEB_ULO_MID,
            on_peak=OEB_ULO_ON,
        ),
    )


def build_oeb_residential_entries(
    rates: OEBRateSet | None = None,
) -> dict[str, dict]:
    """Map plan key → scrape_oeb_rates tariff entry (residential only)."""
    rates = rates or gold_oeb_rate_set()
    entries = build_tariff_entries(rates, "residential")
    by_code = {e["code"]: e for e in entries}
    return {
        "tou": by_code["OEB-RPP-TOU"],
        "tiered": by_code["OEB-RPP-TIERED"],
        "ulo": by_code["OEB-RPP-ULO"],
    }


def build_plan_components(plan_key: str) -> list[dict]:
    return list(build_oeb_residential_entries()[plan_key]["components"])


def plan_meta(plan_key: str) -> dict[str, Any]:
    entry = build_oeb_residential_entries()[plan_key]
    rate_type = {
        "seasonal_tou": RateType.SEASONAL_TOU,
        "seasonal_tiered": RateType.SEASONAL_TIERED,
        "tou": RateType.TOU,
        "tiered": RateType.TIERED,
    }[entry["rate_type"]]
    return {
        "name": entry["name"],
        "code": entry["code"],
        "rate_type": rate_type,
        "description": entry["description"],
        "source_url": OEB_CONSUMER_URL,
        "components": entry["components"],
    }


# ---------------------------------------------------------------------------
# Classification / matching (pure helpers — unit-testable)
# ---------------------------------------------------------------------------

def classify_residential_plan(t: Tariff | Any) -> str | None:
    """Map a live residential tariff to tou / tiered / ulo, or None."""
    name = (getattr(t, "name", None) or "").lower()
    code = (getattr(t, "code", None) or "").lower().replace(" ", "")
    rt = getattr(t, "rate_type", None)
    rt_val = rt.value if hasattr(rt, "value") else str(rt or "").lower()

    if (
        "ultra-low" in name
        or re.search(r"\bulo\b", name)
        or "oeb-rpp-ulo" in code
        or code == "ulo"
    ):
        return "ulo"
    if "tiered" in name or "oeb-rpp-tiered" in code:
        return "tiered"
    if (
        "time-of-use" in name
        or "time of use" in name
        or re.search(r"\btou\b", name)
        or "oeb-rpp-tou" in code
        or code == "tou"
    ):
        return "tou"
    # rate_type fallback for untitled / scraped variants
    if rt_val in {"tou", "seasonal_tou"} and "tier" not in name:
        # Prefer ULO when period labels say so
        labels = " ".join(
            (getattr(rc, "period_label", None) or "")
            for rc in (getattr(t, "rate_components", None) or [])
        ).lower()
        if "ultra-low" in labels or "overnight" in labels:
            return "ulo"
        return "tou"
    if rt_val in {"tiered", "seasonal_tiered"}:
        return "tiered"
    return None


def _ctype(rc: Any) -> str:
    raw = getattr(rc, "component_type", None) if not isinstance(rc, dict) else rc.get("component_type")
    if hasattr(raw, "value"):
        return str(raw.value).lower()
    return str(raw or "").lower()


def _energy_signature(tariff: Any) -> set[tuple]:
    """Comparable ENERGY fingerprint incl. structured clocks / seasons."""
    sig: set[tuple] = set()
    for rc in getattr(tariff, "rate_components", None) or []:
        if _ctype(rc) != "energy":
            continue
        get = (lambda k, d=None: rc.get(k, d)) if isinstance(rc, dict) else (
            lambda k, d=None: getattr(rc, k, d)
        )
        pst = get("period_start_time")
        pet = get("period_end_time")
        if isinstance(pst, time):
            pst = pst.strftime("%H:%M")
        if isinstance(pet, time):
            pet = pet.strftime("%H:%M")
        sig.add(
            (
                round(float(get("rate_value")), 6),
                str(get("period_label") or "").strip().lower(),
                str(pst or ""),
                str(pet or ""),
                str(get("day_type") or "").strip().lower(),
                str(get("season") or "").strip().lower(),
                get("season_start_month"),
                get("season_start_day"),
                get("season_end_month"),
                get("season_end_day"),
                get("tier_min_kwh"),
                get("tier_max_kwh"),
            )
        )
    return sig


def _target_energy_signature(comps: list[dict]) -> set[tuple]:
    fake = type("T", (), {"rate_components": comps})()
    return _energy_signature(fake)


def extract_fixed_components(tariff: Any) -> list[dict]:
    """Copy FIXED/MINIMUM rows from a live keeper (never invent delivery)."""
    out: list[dict] = []
    for rc in getattr(tariff, "rate_components", None) or []:
        ctype = _ctype(rc)
        if ctype not in {"fixed", "minimum"}:
            continue
        get = (lambda k, d=None: rc.get(k, d)) if isinstance(rc, dict) else (
            lambda k, d=None: getattr(rc, k, d)
        )
        out.append(
            {
                "component_type": ctype,
                "unit": get("unit") or "$/month",
                "rate_value": float(get("rate_value")),
                "tier_label": get("tier_label"),
            }
        )
    return out


def components_match_target(tariff: Any, target_comps: list[dict]) -> bool:
    if getattr(tariff, "effective_date", None) != OEB_EFFECTIVE:
        return False
    if _energy_signature(tariff) != _target_energy_signature(target_comps):
        return False
    # Completeness must pass for the plan's rate_type
    return True


def is_incomplete_keeper(tariff: Any, plan_key: str) -> bool:
    meta = plan_meta(plan_key)
    return not is_complete(meta["rate_type"], getattr(tariff, "rate_components", None) or [])


# ---------------------------------------------------------------------------
# Persist
# ---------------------------------------------------------------------------

def _parse_time_str(value: Any) -> time | None:
    if value is None or value == "":
        return None
    if isinstance(value, time):
        return value
    try:
        from scripts.tariff_pipeline import _parse_period_time

        return _parse_period_time(value)
    except Exception:
        s = str(value).strip()
        if s in ("24:00", "24:00:00"):
            return time(0, 0)
        parts = s.split(":")
        try:
            return time(int(parts[0]), int(parts[1]) if len(parts) > 1 else 0)
        except (ValueError, IndexError):
            return None


def _make_keeper(
    utility: Utility,
    plan_key: str,
    energy_comps: list[dict],
    fixed_comps: list[dict] | None = None,
) -> Tariff:
    meta = plan_meta(plan_key)
    keeper = Tariff(
        utility_id=utility.id,
        name=meta["name"],
        code=meta["code"],
        customer_class=CustomerClass.RESIDENTIAL,
        rate_type=meta["rate_type"],
        description=meta["description"],
        effective_date=OEB_EFFECTIVE,
        source_url=OEB_CONSUMER_URL,
        last_verified_at=datetime.now(timezone.utc),
        approved=True,
        confidence_score=0.95,
        confidence_factors={
            "repair": "repair_hydro_one_oeb_residential",
            "plan": plan_key,
            "oeb_effective": OEB_EFFECTIVE.isoformat(),
            "note": (
                "OEB RPP commodity ENERGY with structured season + clock "
                "windows; FIXED copied from prior keeper only when present"
            ),
            "historical_source": OEB_HISTORICAL_URL,
        },
    )
    for c in (fixed_comps or []):
        keeper.rate_components.append(
            RateComponent(
                component_type=ComponentType(str(c["component_type"]).lower()),
                unit=c["unit"],
                rate_value=c["rate_value"],
                tier_label=c.get("tier_label"),
            )
        )
    for c in energy_comps:
        ctype = str(c["component_type"]).lower()
        if ctype != "energy":
            # build_tariff_entries only emits ENERGY for RPP; skip stray
            continue
        keeper.rate_components.append(
            RateComponent(
                component_type=ComponentType.ENERGY,
                unit=c.get("unit", "$/kWh"),
                rate_value=c["rate_value"],
                tier_min_kwh=c.get("tier_min_kwh"),
                tier_max_kwh=c.get("tier_max_kwh"),
                tier_label=c.get("tier_label"),
                period_label=c.get("period_label"),
                period_start_time=_parse_time_str(c.get("period_start_time")),
                period_end_time=_parse_time_str(c.get("period_end_time")),
                day_type=c.get("day_type"),
                season=c.get("season"),
                season_start_month=c.get("season_start_month"),
                season_start_day=c.get("season_start_day"),
                season_end_month=c.get("season_end_month"),
                season_end_day=c.get("season_end_day"),
            )
        )
    return keeper


def _fmt_comps(comps: list[dict]) -> list[str]:
    lines = []
    for c in comps:
        ctype = c["component_type"]
        extras = " ".join(
            str(x)
            for x in (
                c.get("season") or "",
                c.get("day_type") or "",
                c.get("period_label") or "",
                (
                    f"{c.get('period_start_time')}–{c.get('period_end_time')}"
                    if c.get("period_start_time") is not None
                    else ""
                ),
                c.get("tier_label") or "",
            )
            if x
        ).strip()
        lines.append(
            f"      {ctype:10s} {c['rate_value']:>10} {c.get('unit', ''):10s} {extras}"
        )
    return lines


def repair_utility(
    session: Session,
    utility: Utility,
    dry_run: bool,
    plan_filter: set[str] | None = None,
    *,
    force_plans: set[str] | None = None,
) -> dict:
    """Repair Hydro One residential RPP plans.

    ``plan_filter`` limits which plans are considered. ``force_plans`` marks
    keys the operator explicitly requested (``--plan``) so absent/complete
    tiered/ULO may still be created/rewritten. Default mode always includes
    ``tou`` (required) and only rewrites tiered/ULO when incomplete.
    """
    if "remote" in (utility.name or "").lower():
        raise SystemExit(
            f"Refusing utility {utility.name!r} (id={utility.id}) — "
            "this repair is scoped to Hydro One only, not Remote Communities"
        )

    live = session.execute(
        select(Tariff)
        .where(
            Tariff.utility_id == utility.id,
            Tariff.customer_class == CustomerClass.RESIDENTIAL,
            Tariff.superseded_by_tariff_id.is_(None),
            Tariff.supersede_reason.is_(None),
        )
        .options(selectinload(Tariff.rate_components))
    ).scalars().all()

    force_plans = force_plans or set()
    plans_to_run = {
        k: plan_meta(k)
        for k in PLAN_KEYS
        if plan_filter is None or k in plan_filter
    }
    if not plans_to_run:
        raise SystemExit(
            f"No plans matched filter {sorted(plan_filter or [])}; "
            f"valid keys: {list(PLAN_KEYS)}"
        )

    print(f"  [{utility.name}] {len(live)} live residential tariff(s)")
    for t in live:
        plan = classify_residential_plan(t) or "?"
        complete = (
            is_complete(plans_to_run[plan]["rate_type"], t.rate_components)
            if plan in plans_to_run
            else None
        )
        flag = ""
        if complete is False:
            flag = " [INCOMPLETE structured clocks/seasons]"
        elif complete is True:
            flag = " [complete]"
        print(
            f"    live id={t.id} plan={plan} '{t.name}' "
            f"code={t.code!r} rate_type={getattr(t.rate_type, 'value', t.rate_type)} "
            f"eff={t.effective_date}{flag}"
        )

    print(
        f"  Expected prod TOU supersede (audit): id≈{KNOWN_STALE_TOU_ID} "
        "label-only On/Mid/Off ENERGY, no period_* / season_*"
    )

    by_plan: dict[str, list[Tariff]] = {k: [] for k in plans_to_run}
    for t in live:
        key = classify_residential_plan(t)
        if key and key in plans_to_run:
            by_plan[key].append(t)

    created = 0
    superseded = 0
    kept = 0
    skipped_complete = 0
    plan_results: dict[str, Any] = {}

    for plan_key, meta in plans_to_run.items():
        target = list(meta["components"])
        candidates = by_plan[plan_key]
        print(f"\n  -- plan {plan_key}: {meta['name']} (code {meta['code']})")
        print(
            f"    target effective={OEB_EFFECTIVE} "
            f"rate_type={meta['rate_type'].value}"
        )
        for line in _fmt_comps(target):
            print(line)

        # Already-matching keeper?
        keeper = None
        for t in candidates:
            if components_match_target(t, target) and is_complete(
                meta["rate_type"], t.rate_components
            ):
                keeper = t
                break

        if keeper is not None:
            print(
                f"    KEEP id={keeper.id} '{keeper.name}' "
                "(already matches OEB structured shape)"
            )
            kept += 1
            # Soft-supersede any other incomplete duplicates
            for loser in candidates:
                if loser.id == keeper.id:
                    continue
                print(
                    f"    SUPERSEDE id={loser.id} '{loser.name}' "
                    f"→ keeper {keeper.id} (reason=vintage)"
                )
                if not dry_run:
                    supersede_tariff(
                        session, loser, successor=keeper, reason="vintage", **_EVENT_KW
                    )
                superseded += 1
            plan_results[plan_key] = {
                "action": "keep",
                "keeper_id": keeper.id,
                "completeness": evaluate_tariff_completeness(
                    meta["rate_type"], keeper.rate_components
                ).complete,
            }
            continue

        # tou is always required. tiered/ulo: only when incomplete, or when
        # the operator explicitly passed --plan for that key / --plan all.
        incomplete = [t for t in candidates if is_incomplete_keeper(t, plan_key)]
        explicit = plan_key in force_plans
        if plan_key != "tou" and not incomplete and not explicit:
            if not candidates:
                print(
                    f"    SKIP — no live {plan_key} keeper to repair "
                    f"(pass --plan {plan_key} to create from OEB gold)"
                )
                skipped_complete += 1
                plan_results[plan_key] = {"action": "skip_absent"}
                continue
            all_complete = all(
                is_complete(meta["rate_type"], t.rate_components)
                for t in candidates
            )
            if all_complete:
                print(
                    f"    SKIP — live {plan_key} already structurally complete "
                    f"({len(candidates)} keeper(s)); pass --plan {plan_key} "
                    "to force OEB Nov 2025 rewrite"
                )
                skipped_complete += 1
                plan_results[plan_key] = {"action": "skip_complete"}
                continue

        # Preserve FIXED from the best incomplete / any candidate (do not invent)
        fixed_source = (incomplete or candidates or [None])[0]
        fixed_comps = (
            extract_fixed_components(fixed_source) if fixed_source is not None else []
        )
        if fixed_comps:
            print(
                f"    PRESERVE {len(fixed_comps)} FIXED/MINIMUM from "
                f"id={fixed_source.id} (not invented)"
            )
        else:
            print("    No FIXED on live keeper — commodity ENERGY only (OEB RPP)")

        print(
            f"    CREATE '{meta['name']}' code={meta['code']} "
            f"eff={OEB_EFFECTIVE}"
            + (
                f"  # supersede prod id≈{KNOWN_STALE_TOU_ID}"
                if plan_key == "tou"
                else ""
            )
        )
        if not dry_run:
            keeper = _make_keeper(utility, plan_key, target, fixed_comps)
            session.add(keeper)
            session.flush()
            record_event(
                session,
                decision="insert",
                reason="repair",
                utility_id=utility.id,
                after_tariff_id=keeper.id,
                source_url=OEB_CONSUMER_URL,
                **_EVENT_KW,
            )
        created += 1

        for loser in candidates:
            dest = (
                f"keeper {keeper.id}"
                if keeper is not None and getattr(keeper, "id", None)
                else "new OEB keeper"
            )
            print(
                f"    SUPERSEDE id={loser.id} '{loser.name}' "
                f"eff={loser.effective_date} → {dest} (reason=vintage)"
            )
            if not dry_run and keeper is not None:
                supersede_tariff(
                    session, loser, successor=keeper, reason="vintage", **_EVENT_KW
                )
            superseded += 1

        completeness = evaluate_tariff_completeness(meta["rate_type"], target)
        plan_results[plan_key] = {
            "action": "create",
            "keeper_id": getattr(keeper, "id", None) if keeper else None,
            "completeness": completeness.complete,
            "energy_count": completeness.energy_count,
            "preserved_fixed": len(fixed_comps),
        }

    return {
        "utility_id": utility.id,
        "utility_name": utility.name,
        "created": created,
        "kept": kept,
        "superseded": superseded,
        "skipped": skipped_complete,
        "plans": plan_results,
        "live_before": len(live),
        "plan_filter": sorted(plan_filter) if plan_filter else None,
    }


def resolve_utility(session: Session, args: argparse.Namespace) -> Utility:
    if args.utility_id is not None:
        u = session.get(Utility, args.utility_id)
        if not u:
            raise SystemExit(f"No utility with id={args.utility_id}")
        if "remote" in (u.name or "").lower():
            raise SystemExit(
                f"Refusing utility id={u.id} name={u.name!r} — "
                "not Hydro One proper"
            )
        return u

    name = (args.utility_name or HYDRO_ONE_NAME).strip()
    rows = session.execute(
        select(Utility).where(Utility.name.ilike(f"%{name}%"))
    ).scalars().all()
    # Prefer exact "Hydro One"; never Remote Communities
    exact = [
        u for u in rows
        if (u.name or "").strip().lower() == HYDRO_ONE_NAME.lower()
    ]
    if exact:
        return exact[0]
    filtered = [u for u in rows if "remote" not in (u.name or "").lower()]
    if not filtered:
        raise SystemExit(f"No utility matching name {name!r} (excl. Remote)")
    if len(filtered) > 1:
        raise SystemExit(
            f"Ambiguous utility name {name!r}: {[u.name for u in filtered]}"
        )
    return filtered[0]


def print_expected_outcome(plan_filter: set[str] | None = None) -> None:
    print(
        "\nExpected outcome after --apply (Hydro One only):\n"
        f"  Source: {OEB_CONSUMER_URL}\n"
        f"  Effective: {OEB_EFFECTIVE} (OEB RPP)\n"
        "  Soft-supersede incomplete residential TOU (audit id≈46746) → "
        "new SEASONAL_TOU with structured clocks + seasons\n"
        f"  TOU ENERGY: Off={OEB_TOU_OFF} Mid={OEB_TOU_MID} On={OEB_TOU_ON} $/kWh\n"
        "  Winter Nov 1–Apr 30 / Summer May 1–Oct 31; "
        "weekends+holidays Off-Peak all day\n"
        "  FIXED: preserve from prior keeper only — never invent delivery\n"
        "  Tiered/ULO: repaired only when incomplete (or --plan forces)\n"
        "  Do NOT touch other Ontario LDCs / HQ / NB / PE / ranking\n"
    )
    if plan_filter:
        print(f"  Plan filter: {sorted(plan_filter)}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Repair Hydro One residential RPP TOU (and optionally tiered/ULO) "
            "to OEB Nov 2025 structured seasonal clocks (soft-supersede only)."
        )
    )
    parser.add_argument(
        "--utility-name",
        default=None,
        help=f"Substring match (default: exact {HYDRO_ONE_NAME!r})",
    )
    parser.add_argument(
        "--utility-id",
        type=int,
        default=None,
        help="Exact utility id (must resolve to Hydro One proper)",
    )
    parser.add_argument(
        "--plan",
        action="append",
        choices=[*PLAN_KEYS, "all"],
        default=None,
        help=(
            "Limit repair to one or more plan keys (repeatable). "
            "Default: tou (+ tiered/ulo only if incomplete). "
            "Use --plan all to force all three."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes (default is dry-run)",
    )
    args = parser.parse_args(argv)

    dry_run = not args.apply
    mode = "DRY RUN" if dry_run else "APPLY"

    # Default: consider all three; force only what --plan names (or all).
    # Without --plan, tou is always repaired; tiered/ulo only if incomplete.
    if args.plan is None:
        plan_filter = set(PLAN_KEYS)
        force_plans: set[str] = set()
    elif "all" in args.plan:
        plan_filter = set(PLAN_KEYS)
        force_plans = set(PLAN_KEYS)
    else:
        plan_filter = set(args.plan)
        force_plans = set(args.plan)

    print(f"=== repair_hydro_one_oeb_residential ({mode}) ===\n")
    print(f"Plan filter: {sorted(plan_filter)}")
    if force_plans:
        print(f"Force create/rewrite: {sorted(force_plans)}")
    print()

    engine = get_sync_engine()
    with Session(engine) as session:
        utility = resolve_utility(session, args)
        print(f"-- {utility.name} (id={utility.id})")
        result = repair_utility(
            session,
            utility,
            dry_run=dry_run,
            plan_filter=plan_filter,
            force_plans=force_plans,
        )

        if dry_run:
            session.rollback()
            print(
                f"\nDRY RUN complete — would create {result['created']}, "
                f"keep {result['kept']}, supersede {result['superseded']}, "
                f"skip {result['skipped']} "
                f"(live residential before: {result['live_before']})."
            )
            print("Re-run with --apply to write changes.")
        else:
            session.commit()
            print(
                f"\nApplied — created {result['created']}, "
                f"kept {result['kept']}, superseded {result['superseded']}."
            )

        print_expected_outcome(plan_filter)
    return 0


if __name__ == "__main__":
    sys.exit(main())
