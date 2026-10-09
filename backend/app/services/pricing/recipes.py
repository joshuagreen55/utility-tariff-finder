"""Market recipes: how component kinds combine into all-in $/kWh.

Each recipe is a small closed function over Decimal amounts. No floats, no
LLM. Ambiguous percent bases or missing required sides raise ``RecipeError``
(hold — never guess).
"""
from __future__ import annotations

from decimal import Decimal
from typing import Callable

from app.services.pricing.policy import (
    OER_FORBIDDEN_CODES,
    OER_FORBIDDEN_NAME_FRAGMENTS,
    TEXAS_TDU_RECIPE,
)
from app.services.pricing.types import (
    CellKey,
    ComponentInput,
    money,
    to_dollars_per_kwh,
)

ZERO = Decimal("0")


class RecipeError(ValueError):
    """Blocking recipe failure — composition must be held, not published."""


def _reject_oer_in_per_kwh(components: list[ComponentInput]) -> None:
    """OER is bill-level only — never a priced per-kWh component."""
    for c in components:
        code = (c.code or "").strip().lower()
        name = (c.name or "").strip().lower()
        if code in OER_FORBIDDEN_CODES:
            raise RecipeError(
                f"OER component {c.code!r} must be a bill-level note, "
                f"not a per-kWh input"
            )
        if any(frag in name for frag in OER_FORBIDDEN_NAME_FRAGMENTS):
            raise RecipeError(
                f"OER component {c.name!r} must be a bill-level note, "
                f"not a per-kWh input"
            )


def _index(components: list[ComponentInput]) -> dict[str, ComponentInput]:
    out: dict[str, ComponentInput] = {}
    for c in components:
        if c.code in out:
            raise RecipeError(f"duplicate component code in composition: {c.code!r}")
        out[c.code] = c
    return out


def _cell_union(
    components: list[ComponentInput],
    *,
    primary_kinds: set[str] | None = None,
) -> list[CellKey]:
    """Union of priced cells.

    When ``primary_kinds`` is set (e.g. ``{base_energy}``), only those
    components define the grid. Rider-only wildcard cells (``all/all/all``)
    must not create phantom all-in rows of "just the rider".
    """
    skip = {
        "season_calendar", "tou_schedule", "holiday_list",
        "tier_structure", "fixed_charge", "excluded_item", "event_day",
        "multiplier", "rider_percent",
    }
    keys: set[CellKey] = set()
    for c in components:
        if c.kind in skip:
            continue
        if primary_kinds is not None and c.kind not in primary_kinds:
            continue
        keys.update(c.amounts_by_cell())
    if not keys:
        raise RecipeError("no priced cells on applying components")
    return sorted(keys)


def _lookup(comp: ComponentInput, key: CellKey) -> Decimal | None:
    """Resolve a cell with wildcards: exact → season/period/tier fallbacks → all."""
    amounts = comp.amounts_by_cell()
    if key in amounts:
        return amounts[key]
    # Progressive relaxation: try replacing day_type, then period, then season, then tier.
    candidates = [
        key,
        CellKey(key.season, key.period, "all", key.tier),
        CellKey(key.season, "all", key.day_type, key.tier),
        CellKey(key.season, "all", "all", key.tier),
        CellKey("all", key.period, key.day_type, key.tier),
        CellKey("all", key.period, "all", key.tier),
        CellKey("all", "all", "all", key.tier),
        CellKey(key.season, key.period, key.day_type, "all"),
        CellKey(key.season, "all", "all", "all"),
        CellKey("all", "all", "all", "all"),
    ]
    for cand in candidates:
        if cand in amounts:
            return amounts[cand]
    # Single-cell components apply everywhere.
    if len(amounts) == 1:
        return next(iter(amounts.values()))
    return None


def _as_dollars(comp: ComponentInput, key: CellKey) -> Decimal:
    raw = _lookup(comp, key)
    if raw is None:
        return ZERO
    if comp.kind == "rider_percent":
        return raw  # percent points
    if comp.kind == "multiplier":
        return raw  # dimensionless factor
    return to_dollars_per_kwh(raw, comp.unit)


def _percent_effect(
    riders: list[ComponentInput],
    by_code: dict[str, ComponentInput],
    key: CellKey,
) -> Decimal:
    """Sum of percent-of-base riders for one cell. Bases may include other riders."""
    # Evaluate in dependency order (percent bases that are themselves percent
    # are not supported in release 1 — hold).
    effect = ZERO
    for r in riders:
        if r.kind != "rider_percent":
            continue
        pct = _as_dollars(r, key)  # percent points, e.g. 13.0205
        if not r.percent_base_codes:
            raise RecipeError(
                f"percent rider {r.code!r} has no percent_base_codes (ambiguous base)"
            )
        base = ZERO
        for code in r.percent_base_codes:
            if code not in by_code:
                raise RecipeError(
                    f"percent rider {r.code!r} base {code!r} missing from composition"
                )
            base_comp = by_code[code]
            if base_comp.kind == "rider_percent":
                raise RecipeError(
                    f"percent rider {r.code!r} bases on another percent rider "
                    f"{code!r}; hold for human (no percent DAG in release 1)"
                )
            base += _as_dollars(base_comp, key)
        effect += base * (pct / Decimal("100"))
    return effect


def recipe_bundled(components: list[ComponentInput]) -> dict[CellKey, Decimal]:
    """All-in = base_energy + Σ rider_per_kwh + Σ percent(base) + credits.

    Percentage riders scale only the named base set (usually ``base_energy``).
    Credits are negative adders. Event-day / excluded / fixed are ignored here.
    """
    _reject_oer_in_per_kwh(components)
    by_code = _index(components)
    applying = [
        c for c in components
        if c.kind in {
            "base_energy", "rider_per_kwh", "rider_percent", "credit",
        }
    ]
    if not any(c.kind == "base_energy" for c in applying):
        raise RecipeError("bundled recipe requires at least one base_energy component")
    out: dict[CellKey, Decimal] = {}
    for key in _cell_union(applying, primary_kinds={"base_energy"}):
        base = sum(
            (_as_dollars(c, key) for c in applying if c.kind == "base_energy"),
            ZERO,
        )
        adders = sum(
            (
                _as_dollars(c, key)
                for c in applying
                if c.kind in {"rider_per_kwh", "credit"}
            ),
            ZERO,
        )
        pct = _percent_effect(
            [c for c in applying if c.kind == "rider_percent"], by_code, key
        )
        out[key] = base + pct + adders
    return out


def recipe_deregulated(components: list[ComponentInput]) -> dict[CellKey, Decimal]:
    """All-in = delivery + default_supply (explicit charge map, no double count).

    Requires both sides. Transmission / regulatory sit where the composition's
    ``charge_category`` places them (delivery vs supply). Half-plans raise.

    Markets with no default supply (Texas competitive) use ``texas_tdu`` instead.
    """
    _reject_oer_in_per_kwh(components)
    delivery = [
        c for c in components
        if c.kind == "delivery_per_kwh"
        or c.charge_category in {"delivery", "transmission_delivery", "regulatory"}
        or (c.kind == "rider_per_kwh" and c.charge_category == "delivery")
    ]
    supply = [
        c for c in components
        if c.kind == "default_supply"
        or c.charge_category in {"supply", "default_supply", "transmission_supply"}
        or (c.kind == "rider_per_kwh" and c.charge_category == "supply")
    ]
    # Also allow base_energy tagged as delivery (some books print delivery as energy).
    delivery += [
        c for c in components
        if c.kind == "base_energy" and c.charge_category == "delivery"
    ]
    if not delivery:
        raise RecipeError("deregulated recipe missing delivery side (missing_delivery)")
    if not supply:
        raise RecipeError("deregulated recipe missing default supply (missing_supply)")

    priced = delivery + supply
    # Deduplicate by code if a component matched both filters.
    seen: set[str] = set()
    unique: list[ComponentInput] = []
    for c in priced:
        if c.code in seen:
            continue
        seen.add(c.code)
        unique.append(c)

    # Grid from delivery + supply cores (not rider-only wildcards).
    primary = {
        c.kind for c in unique
        if c.kind in {"delivery_per_kwh", "default_supply", "base_energy"}
    }
    out: dict[CellKey, Decimal] = {}
    for key in _cell_union(unique, primary_kinds=primary or None):
        total = ZERO
        for c in unique:
            if c.kind == "rider_percent":
                continue
            total += _as_dollars(c, key)
        by_code = _index(components)
        total += _percent_effect(
            [c for c in components if c.kind == "rider_percent"], by_code, key
        )
        out[key] = total
    return out


def recipe_texas_tdu(components: list[ComponentInput]) -> dict[CellKey, Decimal]:
    """Delivery-only (TDU) for markets with no default supply.

    Joshua 2026-10-09: store TDU delivery charges; mark supply
    ``choose_a_retailer``; never invent a supply price; never publish all-in.

    Returns **delivery** $/kWh cells only. The compiler sets
    ``has_all_in=False`` and ``supply_status=choose_a_retailer``.
    """
    _reject_oer_in_per_kwh(components)
    supply = [
        c for c in components
        if c.kind == "default_supply"
        or c.charge_category in {"supply", "default_supply", "transmission_supply"}
    ]
    if supply:
        raise RecipeError(
            "texas_tdu must not include a supply price "
            "(choose_a_retailer — do not invent supply)"
        )
    delivery = [
        c for c in components
        if c.kind == "delivery_per_kwh"
        or c.charge_category in {"delivery", "transmission_delivery", "regulatory"}
        or (c.kind == "rider_per_kwh" and c.charge_category == "delivery")
        or (c.kind == "base_energy" and c.charge_category == "delivery")
    ]
    if not delivery:
        raise RecipeError("texas_tdu missing delivery / TDU charges")

    seen: set[str] = set()
    unique: list[ComponentInput] = []
    for c in delivery:
        if c.code in seen:
            continue
        seen.add(c.code)
        unique.append(c)

    primary = {
        c.kind for c in unique
        if c.kind in {"delivery_per_kwh", "base_energy"}
    }
    out: dict[CellKey, Decimal] = {}
    for key in _cell_union(unique, primary_kinds=primary or None):
        total = sum((_as_dollars(c, key) for c in unique), ZERO)
        out[key] = total
    return out


def recipe_provincial_ontario(
    components: list[ComponentInput],
) -> dict[CellKey, Decimal]:
    """All-in = (RPP commodity × LF) + DC + (Net+Conn+WMSR+RRRP) × LF.

    Commodity is ``regulated_commodity``. Loss factor is a ``multiplier``
    targeting commodity + loss_sensitive delivery lines. Distribution
    volumetric (``loss_sensitive=False``) is added without LF.

    OER is **not** applied here (bill-level note, like taxes).
    """
    _reject_oer_in_per_kwh(components)
    commodity = [c for c in components if c.kind == "regulated_commodity"]
    if not commodity:
        raise RecipeError("ontario recipe missing regulated_commodity")
    delivery = [c for c in components if c.kind == "delivery_per_kwh"]
    if not delivery:
        raise RecipeError("ontario recipe missing delivery_per_kwh")
    multipliers = [c for c in components if c.kind == "multiplier"]
    if len(multipliers) != 1:
        raise RecipeError(
            f"ontario recipe expects exactly one loss-factor multiplier, "
            f"got {len(multipliers)}"
        )
    lf_comp = multipliers[0]
    lf_amounts = lf_comp.amounts_by_cell()
    if len(lf_amounts) != 1:
        raise RecipeError("loss factor must be a single dimensionless cell")
    lf = next(iter(lf_amounts.values()))

    out: dict[CellKey, Decimal] = {}
    for key in _cell_union(commodity + delivery, primary_kinds={"regulated_commodity"}):
        comm = sum((_as_dollars(c, key) for c in commodity), ZERO)
        dist = ZERO
        loss_sens = ZERO
        for c in delivery:
            amt = _as_dollars(c, key)
            if c.loss_sensitive:
                loss_sens += amt
            else:
                dist += amt
        out[key] = (comm * lf) + dist + (loss_sens * lf)
    return out


def recipe_provincial_alberta(
    components: list[ComponentInput],
) -> dict[CellKey, Decimal]:
    """All-in = RoLR / default supply + distributor delivery + riders.

    Multipliers (e.g. Fortis BTAR on transmission) scale their target codes
    before summing. Municipal franchise / local access fees stay excluded.
    """
    _reject_oer_in_per_kwh(components)
    supply = [c for c in components if c.kind == "default_supply"]
    delivery = [
        c for c in components
        if c.kind in {"delivery_per_kwh", "rider_per_kwh", "credit", "base_energy"}
    ]
    if not supply:
        raise RecipeError("alberta recipe missing default_supply")
    if not delivery:
        raise RecipeError("alberta recipe missing delivery components")

    by_code = _index(components)
    multipliers = [c for c in components if c.kind == "multiplier"]

    def _scaled(comp: ComponentInput, key: CellKey) -> Decimal:
        amt = _as_dollars(comp, key)
        for m in multipliers:
            if comp.code in (m.multiplier_target_codes or []):
                factor = _as_dollars(m, key)
                # factor is stored as the remaining share (e.g. 0.9941 = 1 − 0.59%)
                # or as a percent-reduction if unit is percent — require dimensionless.
                if m.unit.lower() not in {"dimensionless", "factor", "x"}:
                    raise RecipeError(
                        f"alberta multiplier {m.code!r} must be dimensionless, "
                        f"got {m.unit!r}"
                    )
                amt = amt * factor
        return amt

    priced = supply + [
        c for c in delivery
        if c.kind != "rider_percent"
    ]
    out: dict[CellKey, Decimal] = {}
    for key in _cell_union(priced, primary_kinds={"default_supply", "delivery_per_kwh", "base_energy"}):
        total = sum((_scaled(c, key) for c in priced), ZERO)
        total += _percent_effect(
            [c for c in components if c.kind == "rider_percent"], by_code, key
        )
        out[key] = total
    return out


RECIPES: dict[str, Callable[[list[ComponentInput]], dict[CellKey, Decimal]]] = {
    "bundled": recipe_bundled,
    "deregulated": recipe_deregulated,
    TEXAS_TDU_RECIPE: recipe_texas_tdu,
    "provincial_ontario": recipe_provincial_ontario,
    "provincial_alberta": recipe_provincial_alberta,
}


def get_recipe(
    code: str,
) -> Callable[[list[ComponentInput]], dict[CellKey, Decimal]]:
    try:
        return RECIPES[code]
    except KeyError as e:
        raise RecipeError(f"unknown market recipe: {code!r}") from e
