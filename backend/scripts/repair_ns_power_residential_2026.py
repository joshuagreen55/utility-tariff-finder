"""Repair: Nova Scotia Power residential tariffs → May 2026 book.

Soft-supersedes incorrect live residential keepers and creates 2026 keepers
matching the official tariff book (Tariffs May 2026):

  https://www.nspower.ca/docs/default-source/regulatory/tariff-book-2026.pdf

Gold (Board's Order / Energy Charge — NOT Jan 1 2027 escalate columns):

  Domestic Service (02/03/04):
    fixed/min $20.08/mo; base energy 18.324 c/kWh
    riders: FAM AA/BA 0.156 + DSM DCRR 0.648 + Storm SCRR 0.000
    → all-in ENERGY 19.128 c/kWh ($0.19128/kWh)

  Domestic TOD (05/06): seasonal/TOU schedule + same riders on energy
  Domestic CPP (70): interim = standard Domestic energy (no CPP events)
  Domestic TOU (80): **ENERGY CHARGE seasonal TOU** (not flat interim):
    Non-winter Apr–Oct all hours 12.860¢ base → $0.13664 all-in
    Winter Nov–Mar on-peak 36.517¢ → $0.37321; off-peak 18.324¢ → $0.19128
    (Winter weekends/holidays = off-peak — Note 1; documented on tariff.)
  MURB TOU (89): General-class riders on seasonal TOU

Post-PR #6 prod (utility **1739**): Domestic/TOD/CPP/MURB keepers may
already match. Live code **80** keeper **67033** was wrongly flattened to
a single interim ENERGY $0.19128 (rate_type=TOU). This repair soft-supersedes
67033 and creates SEASONAL_TOU matching the 4-page Energy Charge schedule.

Why Flux ENERGY must be all-in: Lookup / TariffDetail only render ENERGY
(and fixed/demand). ADJUSTMENT-only FAM/DSM/Storm riders are invisible —
same product pattern as NL Rate #1.1S / PR #6 all-in ENERGY work.

Soft-supersede only (``supersede_reason='vintage'``). Never hard-delete.
Dry-run is the default; pass ``--apply`` to write. Idempotent when live
keepers already match the target shapes. Scoped to Nova Scotia Power
(default utility id 1739). Optional ``--plan tou`` limits to rate code 80.

Usage:
  python -m scripts.repair_ns_power_residential_2026
  python -m scripts.repair_ns_power_residential_2026 --plan tou
  python -m scripts.repair_ns_power_residential_2026 --plan tou --apply
  python -m scripts.repair_ns_power_residential_2026 --utility-id 1739 --apply
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import date, datetime, timezone
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

# ---------------------------------------------------------------------------
# Published May 2026 book figures (cents → dollars at use sites)
# ---------------------------------------------------------------------------

NS_POWER_NAME = "Nova Scotia Power"
NS_POWER_UTILITY_ID = 1739
NS_TARIFF_BOOK_2026_URL = (
    "https://www.nspower.ca/docs/default-source/regulatory/tariff-book-2026.pdf"
)
NS_STALE_SOURCE_FRAGMENT = "tariff-book-20250326.pdf"
NS_EFFECTIVE = date(2026, 5, 1)  # Board Order / rates-in-effect date

# Expected supersede targets printed in dry-run. After PR #6 apply, most
# plans may already match; code 80 keeper **67033** is the flat-interim
# miss this repair corrects. Pre-PR #6 ids (46886/46887/60200/…) remain
# documented for historical dry-run narratives.
KNOWN_STALE_LIVE = {
    "cpp": {
        "id": 46886,
        "name": "Domestic Service Critical Peak Pricing Tariff",
        "code": "C",
        "note": "FIXED $19.17; ENERGY Critical $1.42256 / Non-Critical $0.14222",
    },
    "tou": {
        "id": 67033,
        "name": "Domestic Service Time of Use Tariff",
        "code": "80",
        "note": (
            "PR #6 interim flatten: FIXED $20.08; single ENERGY $0.19128 "
            "(all hours). Predecessor 46887 was SEASONAL_TOU — restore "
            "Energy Charge seasonal TOU + FAM/DSM all-in."
        ),
    },
    "domestic": {
        "id": 60200,
        "name": "Domestic Service Tariff Optional Green Power Rider",
        "code": "02, 03, 04",
        "note": (
            "NOT clean Domestic — FIXED $19.17; ENERGY $0.15744 + $5 Green "
            "Power ADJUSTMENT. Soft-supersede when creating Domestic Service."
        ),
    },
    "tod": {
        "id": 60201,
        "name": "Domestic Service Time-Of-Day Tariff (Optional)",
        "code": "05, 06",
        "note": "FIXED $19.17; seasonal ENERGY (Summer/Winter)",
    },
    "murb": {
        "id": 60202,
        "name": "Multi-Unit Residential Buildings Time-of-Use Tariff",
        "code": "MURB",
        "note": "FIXED $21.28; seasonal ENERGY",
    },
}

# Rate code 80 Energy Charge bases (¢/kWh) — NOT Interim, NOT Jan 1 2027.
NS_TOU_NONWINTER_BASE_CENTS = 12.860   # Eff Apr 1 2027 (only non-winter Energy Charge row)
NS_TOU_WINTER_ONPEAK_CENTS = 36.517    # Eff Nov 1 2026
NS_TOU_WINTER_OFFPEAK_CENTS = 18.324   # Eff Nov 1 2026
NS_TOU_NONWINTER_SEASON = "Non-winter (Apr 1–Oct 31)"
NS_TOU_WINTER_SEASON = "Winter (Nov 1–Mar 31)"
NS_TOU_WEEKEND_NOTE = (
    "Note 1: In Winter, off-peak also applies all hours on Saturdays, "
    "Sundays, and holidays (Jan 1, NS Heritage Day, Good Friday, Easter "
    "Monday, Nov 11, Dec 25–26; observed weekday if weekend)."
)

# Domestic-class stacking riders (¢/kWh → $/kWh)
NS_DOMESTIC_FAM = 0.00156   # 0.156 ¢
NS_DOMESTIC_DSM = 0.00648   # 0.648 ¢
NS_DOMESTIC_STORM = 0.0     # 0.000 ¢
NS_DOMESTIC_RIDER_SUM = NS_DOMESTIC_FAM + NS_DOMESTIC_DSM + NS_DOMESTIC_STORM

# MURB shares General-class riders
NS_MURB_FAM = 0.00207       # 0.207 ¢
NS_MURB_DSM = 0.00749       # 0.749 ¢ (PCR 0.735 + BA 0.015)
NS_MURB_STORM = 0.0
NS_MURB_RIDER_SUM = NS_MURB_FAM + NS_MURB_DSM + NS_MURB_STORM

NS_DOMESTIC_BASE_ENERGY = 0.18324   # 18.324 ¢
NS_DOMESTIC_ALL_IN = round(NS_DOMESTIC_BASE_ENERGY + NS_DOMESTIC_RIDER_SUM, 6)  # 0.19128
NS_CUSTOMER_CHARGE = 20.08


def cents_to_dollars(cents: float) -> float:
    return round(float(cents) / 100.0, 6)


def all_in_domestic(base_cents: float) -> float:
    """Base energy ¢/kWh + Domestic FAM/DSM/Storm → $/kWh."""
    return round(cents_to_dollars(base_cents) + NS_DOMESTIC_RIDER_SUM, 6)


def all_in_murb(base_cents: float) -> float:
    return round(cents_to_dollars(base_cents) + NS_MURB_RIDER_SUM, 6)


# ---------------------------------------------------------------------------
# Target component builders (pure — unit-testable without DB)
# ---------------------------------------------------------------------------

def build_domestic_service_components() -> list[dict]:
    return [
        {
            "component_type": "fixed",
            "unit": "$/month",
            "rate_value": NS_CUSTOMER_CHARGE,
            "tier_label": "Customer Charge",
        },
        {
            "component_type": "energy",
            "unit": "$/kWh",
            "rate_value": NS_DOMESTIC_ALL_IN,
            "tier_label": "All-in (base + FAM + DSM)",
        },
    ]


def build_domestic_tod_components() -> list[dict]:
    """Domestic Service Time-Of-Day (codes 05/06) — Board’s Order column."""
    # Winter Dec–Feb weekdays (¢): 7–12 24.384, 12–4 19.459, 4–11 24.384, 11–7 11.632
    # Mar–Nov weekdays: 7–11 19.459, 11–7 11.632; weekends/holidays = off-peak
    winter = "Winter (Dec–Feb)"
    nonwinter = "Mar–Nov"
    comps: list[dict] = [
        {
            "component_type": "fixed",
            "unit": "$/month",
            "rate_value": NS_CUSTOMER_CHARGE,
            "tier_label": "Customer Charge",
        },
    ]
    winter_periods = [
        ("On-Peak Morning (7am–12pm)", 24.384),
        ("Mid-Day (12pm–4pm)", 19.459),
        ("On-Peak Evening (4pm–11pm)", 24.384),
        ("Off-Peak (11pm–7am)", 11.632),
    ]
    for label, cents in winter_periods:
        comps.append(
            {
                "component_type": "energy",
                "unit": "$/kWh",
                "rate_value": all_in_domestic(cents),
                "period_label": label,
                "season": winter,
                "tier_label": label,
            }
        )
    for label, cents in [
        ("Day (7am–11pm)", 19.459),
        ("Off-Peak (11pm–7am)", 11.632),
    ]:
        comps.append(
            {
                "component_type": "energy",
                "unit": "$/kWh",
                "rate_value": all_in_domestic(cents),
                "period_label": label,
                "season": nonwinter,
                "tier_label": label,
            }
        )
    return comps


def build_domestic_cpp_interim_components() -> list[dict]:
    """CPP code 70 — interim energy = standard Domestic; CPP events n/a."""
    return [
        {
            "component_type": "fixed",
            "unit": "$/month",
            "rate_value": NS_CUSTOMER_CHARGE,
            "tier_label": "Customer Charge",
        },
        {
            "component_type": "energy",
            "unit": "$/kWh",
            "rate_value": NS_DOMESTIC_ALL_IN,
            "period_label": "Interim (all hours; no CPP events)",
            "tier_label": "Interim Non-Critical (= Domestic standard)",
        },
    ]


def build_domestic_tou_interim_components() -> list[dict]:
    """DEPRECATED — PR #6 interim flatten. Prefer seasonal Energy Charge.

    Kept for regression tests that assert the old wrong shape must not
    match the live target. Do not wire this into ``NS_RESIDENTIAL_PLANS``.
    """
    return [
        {
            "component_type": "fixed",
            "unit": "$/month",
            "rate_value": NS_CUSTOMER_CHARGE,
            "tier_label": "Customer Charge",
        },
        {
            "component_type": "energy",
            "unit": "$/kWh",
            "rate_value": NS_DOMESTIC_ALL_IN,
            "period_label": "Interim (all hours)",
            "tier_label": "Interim (= Domestic standard offer)",
        },
    ]


def build_domestic_tou_seasonal_components() -> list[dict]:
    """TOU code 80 — Energy Charge seasonal TOU + Domestic FAM/DSM all-in.

    Uses the approved Energy Charge tables (4-page schedule), not the
    Interim Energy Charge flat/standard-offer layer. Winter on/off-peak
    from the Nov 1 2026 column (not Jan 1 2027 escalate). Non-winter
    all-hours from the Apr 1 2027 Energy Charge row (sole non-winter
    Energy Charge figure in the book).
    """
    comps: list[dict] = [
        {
            "component_type": "fixed",
            "unit": "$/month",
            "rate_value": NS_CUSTOMER_CHARGE,
            "tier_label": "Customer Charge",
        },
        {
            "component_type": "energy",
            "unit": "$/kWh",
            "rate_value": all_in_domestic(NS_TOU_NONWINTER_BASE_CENTS),
            "period_label": "All hours",
            "season": NS_TOU_NONWINTER_SEASON,
            "tier_label": "Energy Charge non-winter (eff Apr 1 2027)",
        },
    ]
    winter_periods = [
        ("On-peak morning (7am–11am)", NS_TOU_WINTER_ONPEAK_CENTS),
        ("Off-peak midday (11am–5pm)", NS_TOU_WINTER_OFFPEAK_CENTS),
        ("On-peak evening (5pm–9pm)", NS_TOU_WINTER_ONPEAK_CENTS),
        ("Off-peak night (9pm–7am)", NS_TOU_WINTER_OFFPEAK_CENTS),
    ]
    for label, cents in winter_periods:
        comps.append(
            {
                "component_type": "energy",
                "unit": "$/kWh",
                "rate_value": all_in_domestic(cents),
                "period_label": label,
                "season": NS_TOU_WINTER_SEASON,
                "tier_label": "Energy Charge winter (eff Nov 1 2026)",
            }
        )
    return comps


def build_murb_tou_components() -> list[dict]:
    """MURB TOU code 89 — Board’s Order column + General-class riders."""
    comps: list[dict] = [
        {
            "component_type": "fixed",
            "unit": "$/month",
            "rate_value": 22.00,
            "tier_label": "Minimum Monthly Charge",
        },
    ]
    # Non-winter Apr–Oct: Off-peak 11.826, On-peak 14.043
    nonwinter = "Non-Winter (Apr–Oct)"
    for label, cents in [
        ("Off-Peak (9pm–7am)", 11.826),
        ("On-Peak (7am–9pm)", 14.043),
    ]:
        comps.append(
            {
                "component_type": "energy",
                "unit": "$/kWh",
                "rate_value": all_in_murb(cents),
                "period_label": label,
                "season": nonwinter,
                "tier_label": label,
            }
        )
    # Winter Nov–Mar: On-peak morning 29.565, Mid 14.782, On-peak evening 29.565, Off 12.565
    winter = "Winter (Nov–Mar)"
    for label, cents in [
        ("On-Peak Morning (7am–11am)", 29.565),
        ("Mid-Peak (11am–5pm)", 14.782),
        ("On-Peak Evening (5pm–9pm)", 29.565),
        ("Off-Peak (9pm–7am)", 12.565),
    ]:
        comps.append(
            {
                "component_type": "energy",
                "unit": "$/kWh",
                "rate_value": all_in_murb(cents),
                "period_label": label,
                "season": winter,
                "tier_label": label,
            }
        )
    return comps


# Plan catalog: key → metadata + component builder
NS_RESIDENTIAL_PLANS: dict[str, dict[str, Any]] = {
    "domestic": {
        "name": "Domestic Service Tariff",
        "code": "02/03/04",
        "rate_type": RateType.FLAT,
        "description": (
            "Standard residential Domestic Service (rate codes 02, 03, 04). "
            "All-in ENERGY includes base + FAM AA/BA + DSM DCRR (Storm SCRR 0)."
        ),
        "matcher": "domestic_service",
        "build": build_domestic_service_components,
    },
    "tod": {
        "name": "Domestic Service Time-Of-Day Tariff (Optional)",
        "code": "05/06",
        "rate_type": RateType.SEASONAL_TOU,
        "description": (
            "Optional Domestic TOD for ETS / thermal-storage heating "
            "(codes 05, 06). All-in ENERGY includes FAM + DSM riders."
        ),
        "matcher": "tod",
        "build": build_domestic_tod_components,
    },
    "cpp": {
        "name": "Domestic Service Critical Peak Pricing Tariff",
        "code": "70",
        "rate_type": RateType.TOU,
        "description": (
            "Domestic CPP (code 70). Interim energy charge tracks Domestic "
            "standard offer; no CPP events while TVP systems are down."
        ),
        "matcher": "cpp",
        "build": build_domestic_cpp_interim_components,
    },
    "tou": {
        "name": "Domestic Service Time of Use Tariff",
        "code": "80",
        "rate_type": RateType.SEASONAL_TOU,
        "description": (
            "Domestic TOU (code 80). Energy Charge seasonal TOU: Non-winter "
            f"{NS_TOU_NONWINTER_SEASON} all hours; Winter "
            f"{NS_TOU_WINTER_SEASON} on-peak morning/evening and off-peak "
            "midday/night. All-in ENERGY includes FAM + DSM. "
            f"{NS_TOU_WEEKEND_NOTE} Not the Interim Energy Charge layer."
        ),
        "matcher": "tou",
        "build": build_domestic_tou_seasonal_components,
    },
    "murb": {
        "name": "Multi-Unit Residential Buildings Time of Use Tariff",
        "code": "89",
        "rate_type": RateType.SEASONAL_TOU,
        "description": (
            "MURB TOU (code 89). All-in ENERGY includes General-class "
            "FAM AA/BA + DSM DCRR (Storm SCRR 0)."
        ),
        "matcher": "murb",
        "build": build_murb_tou_components,
    },
}


def classify_residential_plan(t: Tariff) -> str | None:
    """Map a live residential tariff to one of the five plan keys, or None.

    Prod id 60200 ("…Optional Green Power Rider", codes 02/03/04) is the
    mis-labeled Domestic stand-in — classify as ``domestic`` so the repair
    creates a clean Domestic Service keeper and soft-supersedes 60200.
    """
    name = (t.name or "").lower()
    code = (t.code or "").lower().replace(" ", "")
    code_nosep = code.replace(",", "").replace("/", "")

    if (
        "murb" in name
        or "multi-unit" in name
        or "multi unit" in name
        or code in {"89", "rate89", "ratecode89", "murb"}
        or code_nosep == "89"
    ):
        return "murb"
    # TOD before TOU — "time-of-day" must not fall through to "time of use"
    if (
        "time-of-day" in name
        or "time of day" in name
        or code in {"05", "06", "05/06", "05,06", "05,06"}
        or code_nosep in {"05", "06", "0506"}
    ):
        return "tod"
    if (
        "critical peak" in name
        or re.search(r"\bcpp\b", name)
        or code in {"70", "rate70", "c"}
        or code_nosep == "70"
    ):
        return "cpp"
    if (
        "time of use" in name
        or "time-of-use" in name
        or re.search(r"\btou\b", name)
        or code in {"80", "rate80", "d"}
        or code_nosep == "80"
    ):
        return "tou"
    # Green Power mis-baked as Domestic base (prod 60200) → domestic bucket
    if "green power" in name:
        return "domestic"
    if (
        "domestic" in name
        or "standard residential" in name
        or code in {"02", "03", "04", "02/03/04", "02,03,04"}
        or code_nosep in {"02", "03", "04", "020304"}
    ):
        return "domestic"
    return None


def _is_live(t: Tariff) -> bool:
    return t.superseded_by_tariff_id is None and t.supersede_reason is None


def _energy_signature(tariff: Tariff) -> set[tuple]:
    """Comparable ENERGY fingerprint: (rate, season, period)."""
    sig: set[tuple] = set()
    for rc in tariff.rate_components or []:
        ctype = (
            rc.component_type.value
            if hasattr(rc.component_type, "value")
            else str(rc.component_type)
        )
        if ctype.lower() != "energy":
            continue
        sig.add(
            (
                round(float(rc.rate_value), 6),
                (rc.season or "").strip().lower(),
                (rc.period_label or "").strip().lower(),
            )
        )
    return sig


def _target_energy_signature(comps: list[dict]) -> set[tuple]:
    return {
        (
            round(float(c["rate_value"]), 6),
            str(c.get("season") or "").strip().lower(),
            str(c.get("period_label") or "").strip().lower(),
        )
        for c in comps
        if str(c.get("component_type") or "").lower() == "energy"
    }


def _fixed_matches(tariff: Tariff, comps: list[dict]) -> bool:
    target_fixed = [
        round(float(c["rate_value"]), 2)
        for c in comps
        if str(c.get("component_type") or "").lower() in {"fixed", "minimum"}
    ]
    if not target_fixed:
        return True
    live_fixed = []
    for rc in tariff.rate_components or []:
        ctype = (
            rc.component_type.value
            if hasattr(rc.component_type, "value")
            else str(rc.component_type)
        )
        if ctype.lower() in {"fixed", "minimum"}:
            live_fixed.append(round(float(rc.rate_value), 2))
    return sorted(live_fixed) == sorted(target_fixed)


def components_match_target(tariff: Tariff, target: list[dict]) -> bool:
    if tariff.effective_date != NS_EFFECTIVE:
        return False
    if _energy_signature(tariff) != _target_energy_signature(target):
        return False
    return _fixed_matches(tariff, target)


def _make_keeper(
    utility: Utility,
    plan_key: str,
    target_comps: list[dict],
) -> Tariff:
    meta = NS_RESIDENTIAL_PLANS[plan_key]
    keeper = Tariff(
        utility_id=utility.id,
        name=meta["name"],
        code=meta["code"],
        customer_class=CustomerClass.RESIDENTIAL,
        rate_type=meta["rate_type"],
        description=meta["description"],
        effective_date=NS_EFFECTIVE,
        source_url=NS_TARIFF_BOOK_2026_URL,
        last_verified_at=datetime.now(timezone.utc),
        approved=True,
        confidence_score=0.95,
        confidence_factors={
            "repair": "repair_ns_power_residential_2026",
            "note": "May 2026 tariff book all-in ENERGY (base + FAM + DSM)",
            "plan": plan_key,
            **(
                {
                    "energy_section": "Energy Charge (not Interim)",
                    "weekend_holiday": NS_TOU_WEEKEND_NOTE,
                }
                if plan_key == "tou"
                else {}
            ),
        },
    )
    for c in target_comps:
        ctype = str(c["component_type"]).lower()
        keeper.rate_components.append(
            RateComponent(
                component_type=ComponentType(ctype),
                unit=c["unit"],
                rate_value=c["rate_value"],
                tier_label=c.get("tier_label"),
                period_label=c.get("period_label"),
                season=c.get("season"),
            )
        )
    return keeper


def _fmt_comps(comps: list[dict]) -> list[str]:
    lines = []
    for c in comps:
        ctype = c["component_type"]
        extras = " ".join(
            x
            for x in (
                c.get("season") or "",
                c.get("period_label") or "",
                c.get("tier_label") or "",
            )
            if x
        ).strip()
        lines.append(
            f"      {ctype:10s} {c['rate_value']:>10} {c['unit']:10s} {extras}"
        )
    return lines


def repair_utility(
    session: Session,
    utility: Utility,
    dry_run: bool,
    plan_filter: set[str] | None = None,
) -> dict:
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

    plans_to_run = {
        k: v
        for k, v in NS_RESIDENTIAL_PLANS.items()
        if plan_filter is None or k in plan_filter
    }
    if not plans_to_run:
        raise SystemExit(
            f"No plans matched filter {sorted(plan_filter or [])}; "
            f"valid keys: {sorted(NS_RESIDENTIAL_PLANS)}"
        )

    print(f"  [{utility.name}] {len(live)} live residential tariff(s)")
    for t in live:
        plan = classify_residential_plan(t) or "?"
        src = (t.source_url or "")
        stale = NS_STALE_SOURCE_FRAGMENT in src
        print(
            f"    live id={t.id} plan={plan} '{t.name}' "
            f"code={t.code!r} eff={t.effective_date}"
            f"{' [stale Mar-2025 book]' if stale else ''}"
        )

    print("  Expected prod supersede map (utility 1739):")
    for plan_key, info in KNOWN_STALE_LIVE.items():
        if plan_key not in plans_to_run:
            continue
        print(
            f"    {plan_key}: id={info['id']} '{info['name']}' "
            f"({info['code']}) — {info['note']}"
        )

    by_plan: dict[str, list[Tariff]] = {k: [] for k in plans_to_run}
    unclassified: list[Tariff] = []
    for t in live:
        key = classify_residential_plan(t)
        if key and key in plans_to_run:
            by_plan[key].append(t)
        elif key is None and plan_filter is None:
            unclassified.append(t)
        # When --plan filters, leave other live plans untouched.

    created = 0
    superseded = 0
    kept = 0
    plan_results: dict[str, Any] = {}

    for plan_key, meta in plans_to_run.items():
        target = meta["build"]()
        candidates = by_plan[plan_key]
        print(f"\n  -- plan {plan_key}: {meta['name']} (code {meta['code']})")
        print(f"    target effective={NS_EFFECTIVE} rate_type={meta['rate_type'].value}")
        for line in _fmt_comps(target):
            print(line)

        keeper = None
        for t in candidates:
            if components_match_target(t, target):
                keeper = t
                break

        if keeper is None:
            expected = KNOWN_STALE_LIVE.get(plan_key)
            extra = ""
            if plan_key == "domestic":
                extra = (
                    "  # missing clean Domestic; supersede Green Power "
                    "stand-in (prod 60200)"
                )
            elif plan_key == "tou":
                extra = (
                    "  # supersede prod id 67033 (flat interim) → "
                    "Energy Charge seasonal TOU"
                )
            elif expected:
                extra = f"  # supersede prod id {expected['id']}"
            print(
                f"    CREATE '{meta['name']}' code={meta['code']} "
                f"eff={NS_EFFECTIVE}{extra}"
            )
            if not dry_run:
                keeper = _make_keeper(utility, plan_key, target)
                session.add(keeper)
                session.flush()
            created += 1
        else:
            print(
                f"    KEEP id={keeper.id} '{keeper.name}' "
                "(already matches May 2026 all-in shape)"
            )
            kept += 1

        for loser in candidates:
            if keeper is not None and loser.id == getattr(keeper, "id", None):
                continue
            dest = (
                f"keeper {keeper.id}"
                if keeper is not None and getattr(keeper, "id", None)
                else "new 2026 keeper"
            )
            print(
                f"    SUPERSEDE id={loser.id} '{loser.name}' "
                f"eff={loser.effective_date} → {dest} (reason=vintage)"
            )
            if not dry_run and keeper is not None:
                loser.superseded_by_tariff_id = keeper.id
                loser.supersede_reason = "vintage"
            superseded += 1

        plan_results[plan_key] = {
            "name": meta["name"],
            "code": meta["code"],
            "target_components": target,
            "keeper_id": getattr(keeper, "id", None) if keeper else None,
        }

    # Soft-supersede unclassified live residential only when running the
    # full plan set (not a scoped --plan filter).
    domestic_keeper_id = (plan_results.get("domestic") or {}).get("keeper_id")
    for loser in unclassified:
        dest = (
            f"Domestic keeper {domestic_keeper_id}"
            if domestic_keeper_id
            else "new Domestic keeper"
        )
        print(
            f"\n  SUPERSEDE unclassified id={loser.id} '{loser.name}' "
            f"eff={loser.effective_date} → {dest} (reason=vintage)"
        )
        if not dry_run and domestic_keeper_id:
            loser.superseded_by_tariff_id = domestic_keeper_id
            loser.supersede_reason = "vintage"
        superseded += 1

    return {
        "utility_id": utility.id,
        "utility_name": utility.name,
        "created": created,
        "kept": kept,
        "superseded": superseded,
        "plans": plan_results,
        "live_before": len(live),
        "plan_filter": sorted(plan_filter) if plan_filter else None,
    }


def resolve_utility(session: Session, args: argparse.Namespace) -> Utility:
    if args.utility_id is not None:
        u = session.get(Utility, args.utility_id)
        if not u:
            raise SystemExit(f"No utility with id={args.utility_id}")
        return u
    # Default: exact prod Nova Scotia Power id when present
    u = session.get(Utility, NS_POWER_UTILITY_ID)
    if u and "nova scotia" in (u.name or "").lower():
        return u
    name = args.utility_name or NS_POWER_NAME
    rows = session.execute(
        select(Utility).where(Utility.name.ilike(f"%{name}%"))
    ).scalars().all()
    if not rows:
        raise SystemExit(f"No utility matching name {name!r}")
    for u in rows:
        if u.name.strip().lower() == NS_POWER_NAME.lower():
            return u
    if len(rows) > 1:
        raise SystemExit(
            f"Ambiguous utility name {name!r}: {[u.name for u in rows]}"
        )
    return rows[0]


def print_expected_outcome(plan_filter: set[str] | None = None) -> None:
    scoped = plan_filter == {"tou"}
    print(
        "\nExpected outcome after --apply (Nova Scotia Power id=1739):\n"
        f"  Source: {NS_TARIFF_BOOK_2026_URL}\n"
        f"  Effective: {NS_EFFECTIVE} (Board's Order / May 2026 book)\n"
        f"  Customer charge: ${NS_CUSTOMER_CHARGE}/mo\n"
    )
    if scoped or plan_filter is None or "tou" in (plan_filter or set()):
        print(
            "  Rate code 80 (Domestic TOU) — Energy Charge seasonal TOU:\n"
            f"    SUPERSEDE 67033 (flat interim ENERGY $0.19128) → new "
            f"SEASONAL_TOU keeper\n"
            f"    Non-winter all hours: "
            f"{all_in_domestic(NS_TOU_NONWINTER_BASE_CENTS)} $/kWh "
            f"(= {NS_TOU_NONWINTER_BASE_CENTS:.3f} + 0.804 ¢)\n"
            f"    Winter on-peak: "
            f"{all_in_domestic(NS_TOU_WINTER_ONPEAK_CENTS)} $/kWh "
            f"(= {NS_TOU_WINTER_ONPEAK_CENTS:.3f} + 0.804 ¢)\n"
            f"    Winter off-peak: "
            f"{all_in_domestic(NS_TOU_WINTER_OFFPEAK_CENTS)} $/kWh "
            f"(= {NS_TOU_WINTER_OFFPEAK_CENTS:.3f} + 0.804 ¢)\n"
            "    Do NOT use Jan 1 2027 winter column (38.281 / 19.067 ¢)\n"
            f"    {NS_TOU_WEEKEND_NOTE}"
        )
    if not scoped:
        print(
            f"  Domestic ENERGY all-in: {NS_DOMESTIC_ALL_IN} $/kWh "
            f"(= 18.324 + 0.156 + 0.648 ¢)\n"
            "  Other plans (Domestic / TOD / CPP interim / MURB): KEEP when "
            "already matching; CREATE+SUPERSEDE when not\n"
            "  Code 70 CPP remains interim-only in this repair (Energy Charge "
            "CPP events out of scope unless reopened)"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Repair Nova Scotia Power (id 1739) residential tariffs to "
            "May 2026 tariff-book all-in ENERGY (soft-supersede only). "
            "Rate 80 uses Energy Charge seasonal TOU (not flat interim)."
        )
    )
    parser.add_argument(
        "--utility-name",
        default=None,
        help=f"Substring match (default: {NS_POWER_NAME!r} / id {NS_POWER_UTILITY_ID})",
    )
    parser.add_argument(
        "--utility-id",
        type=int,
        default=None,
        help=f"Exact utility id (default: {NS_POWER_UTILITY_ID} when present)",
    )
    parser.add_argument(
        "--plan",
        action="append",
        choices=sorted(NS_RESIDENTIAL_PLANS.keys()),
        default=None,
        help=(
            "Limit repair to one or more plan keys (repeatable). "
            "Use --plan tou to scope to rate code 80 only."
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
    plan_filter = set(args.plan) if args.plan else None
    print(f"=== repair_ns_power_residential_2026 ({mode}) ===\n")
    if plan_filter:
        print(f"Plan filter: {sorted(plan_filter)}\n")

    engine = get_sync_engine()
    with Session(engine) as session:
        utility = resolve_utility(session, args)
        print(f"-- {utility.name} (id={utility.id})")
        result = repair_utility(
            session, utility, dry_run=dry_run, plan_filter=plan_filter
        )

        if dry_run:
            session.rollback()
            print(
                f"\nDRY RUN complete — would create {result['created']}, "
                f"keep {result['kept']}, supersede {result['superseded']} "
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
