"""Market recipes: how component kinds combine into all-in $/kWh.

Each recipe is a small closed function over Decimal amounts. No floats, no
LLM. Ambiguous percent bases or missing required sides raise ``RecipeError``
(hold — never guess).
"""
from __future__ import annotations

from decimal import Decimal
from typing import Callable, Iterable

from app.services.pricing.policy import (
    OER_FORBIDDEN_CODES,
    OER_FORBIDDEN_NAME_FRAGMENTS,
    TEXAS_TDU_RECIPE,
)
from app.services.pricing.types import (
    CellKey,
    ComponentInput,
    energy_dollars_per_kwh,
    money,
)

ZERO = Decimal("0")
_DIMS = ("season", "period", "day_type", "tier")

# Kinds that never enter the per-kWh sum (schedules, fixed / demand charges,
# exclusions). Anything else a recipe does not consume is a hold.
NON_PRICED_KINDS = frozenset({
    "season_calendar", "tou_schedule", "holiday_list", "tier_structure",
    "fixed_charge", "fixed_monthly", "customer_charge", "demand_charge",
    "excluded_item", "event_day",
})


class RecipeError(ValueError):
    """Blocking recipe failure — composition must be held, not published."""


def _require_consumed(
    components: list[ComponentInput],
    consumed: Iterable[ComponentInput],
    recipe: str,
) -> None:
    """Every priced component must be used by the recipe — never dropped."""
    used = {c.code for c in consumed}
    for c in components:
        if c.kind in NON_PRICED_KINDS:
            continue
        if c.code not in used:
            raise RecipeError(
                f"{recipe} recipe does not price component {c.code!r} "
                f"(kind {c.kind!r}, category {c.charge_category!r}); hold"
            )


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


def _cell_matches(cell: CellKey, key: CellKey) -> bool:
    return all(
        getattr(cell, d) in ("all", getattr(key, d)) for d in _DIMS
    )


def _specificity(cell: CellKey) -> int:
    return sum(1 for d in _DIMS if getattr(cell, d) != "all")


def _resolve(comp: ComponentInput, key: CellKey) -> Decimal | None:
    """Most specific cell of ``comp`` that covers ``key`` (``all`` = wildcard).

    A single unlabelled cell applies everywhere. Two equally specific
    matching cells with different amounts are ambiguous → ``RecipeError``.
    """
    amounts = comp.amounts_by_cell()
    if len(amounts) == 1:
        only_key, only_amt = next(iter(amounts.items()))
        if _specificity(only_key) == 0:
            return only_amt
    matches = [(k, v) for k, v in amounts.items() if _cell_matches(k, key)]
    if not matches:
        return None
    best = max(_specificity(k) for k, _ in matches)
    top = {v for k, v in matches if _specificity(k) == best}
    if len(top) > 1:
        raise RecipeError(
            f"component {comp.code!r} is ambiguous for cell {key.as_dict()}"
        )
    return top.pop()


def _compatible(a: CellKey, b: CellKey) -> bool:
    return all(
        getattr(a, d) == getattr(b, d) or "all" in (getattr(a, d), getattr(b, d))
        for d in _DIMS
    )


def _meet(a: CellKey, b: CellKey) -> CellKey:
    return CellKey(*(
        getattr(a, d) if getattr(a, d) != "all" else getattr(b, d) for d in _DIMS
    ))


def _build_grid(
    components: list[ComponentInput],
    *,
    primary_kinds: set[str],
) -> list[CellKey]:
    """Price grid: start from one wildcard cell and split it wherever a
    priced component distinguishes a label (primaries first).

    A flat base with a seasonal rider becomes summer/winter rows; a
    weekday-only on-peak never grows a weekend on-peak row; a winter rate
    that is flat across tiers stays one row. Every grid key must then be
    covered by every component, and every labelled cell must be used by some
    key. Either miss means mismatched labels or a missing cell → hold, never
    a silent zero.
    """
    priced = [
        c for c in components
        if c.kind not in NON_PRICED_KINDS and c.kind != "multiplier"
    ]
    cells_by_code = {c.code: c.amounts_by_cell() for c in priced}
    if not any(cells_by_code.values()):
        raise RecipeError("no priced cells on applying components")

    ordered = sorted(priced, key=lambda c: c.kind not in primary_kinds)
    grid: list[CellKey] = [CellKey()]
    for i, c in enumerate(ordered):
        cells = list(cells_by_code[c.code])
        refined: list[CellKey] = []
        for key in grid:
            meets = {_meet(key, cell) for cell in cells if _compatible(cell, key)}
            if not meets:
                refined.append(key)
                continue
            if len(meets) > 1 and key in meets:
                raise RecipeError(
                    f"component {c.code!r} mixes a catch-all cell with "
                    f"specific cells for {key.as_dict()} (ambiguous)"
                )
            if i and len(meets) == 1 and key not in meets:
                # A summer-only rider on a flat base would shrink the grid
                # to summer and drop the rest of the year.
                raise RecipeError(
                    f"component {c.code!r} prices only "
                    f"{next(iter(meets)).as_dict()} of {key.as_dict()} "
                    f"(partial coverage)"
                )
            refined.extend(sorted(meets))
        grid = list(dict.fromkeys(refined))

    for c in priced:
        for key in grid:
            if _resolve(c, key) is None:
                raise RecipeError(
                    f"component {c.code!r} has no amount for cell "
                    f"{key.as_dict()} (labels do not line up with the base)"
                )
        for cell in cells_by_code[c.code]:
            if _specificity(cell) and not any(
                _cell_matches(cell, key) for key in grid
            ):
                raise RecipeError(
                    f"component {c.code!r} cell {cell.as_dict()} matches no "
                    f"base price cell (label mismatch)"
                )
    return grid


def _as_dollars(comp: ComponentInput, key: CellKey) -> Decimal:
    raw = _resolve(comp, key)
    if raw is None:
        raise RecipeError(
            f"component {comp.code!r} has no amount for cell {key.as_dict()}"
        )
    if comp.kind == "rider_percent":
        if (comp.unit or "").strip().lower() not in {"percent", "%", "pct"}:
            raise RecipeError(
                f"percent rider {comp.code!r} must use a percent unit, "
                f"got {comp.unit!r}"
            )
        return raw  # percent points
    if comp.kind == "multiplier":
        if (comp.unit or "").strip().lower() not in {"dimensionless", "factor", "x"}:
            raise RecipeError(
                f"multiplier {comp.code!r} must be dimensionless, got {comp.unit!r}"
            )
        return raw  # dimensionless factor
    try:
        dollars = energy_dollars_per_kwh(raw, comp.unit)
    except ValueError as e:
        raise RecipeError(f"component {comp.code!r}: {e}") from e
    if comp.kind == "credit":
        # A credit lowers the price whichever sign the document printed.
        return -abs(dollars)
    return dollars


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
    _require_consumed(components, applying, "bundled")
    out: dict[CellKey, Decimal] = {}
    for key in _build_grid(applying, primary_kinds={"base_energy"}):
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


def _is_priced(c: ComponentInput) -> bool:
    return c.kind not in NON_PRICED_KINDS and c.kind != "multiplier"


def _split_categories(
    components: list[ComponentInput],
) -> tuple[list[ComponentInput], list[ComponentInput]]:
    """Delivery vs supply sides for wires/supply recipes (explicit, no guess)."""
    delivery_cats = {"delivery", "transmission_delivery", "regulatory"}
    supply_cats = {"supply", "default_supply", "transmission_supply"}
    delivery: list[ComponentInput] = []
    supply: list[ComponentInput] = []
    for c in components:
        if not _is_priced(c):
            continue
        is_delivery = (
            c.kind == "delivery_per_kwh" or c.charge_category in delivery_cats
        )
        is_supply = c.kind == "default_supply" or c.charge_category in supply_cats
        if is_delivery and is_supply:
            raise RecipeError(
                f"component {c.code!r} is tagged both delivery and supply "
                f"(kind {c.kind!r}, category {c.charge_category!r})"
            )
        if is_delivery:
            delivery.append(c)
        elif is_supply:
            supply.append(c)
    return delivery, supply


def _sum_cell(
    priced: list[ComponentInput],
    components: list[ComponentInput],
    key: CellKey,
) -> Decimal:
    by_code = _index(components)
    total = sum(
        (_as_dollars(c, key) for c in priced if c.kind != "rider_percent"),
        ZERO,
    )
    total += _percent_effect(
        [c for c in priced if c.kind == "rider_percent"], by_code, key
    )
    return total


def recipe_deregulated(components: list[ComponentInput]) -> dict[CellKey, Decimal]:
    """All-in = delivery + default_supply (explicit charge map, no double count).

    Requires both sides. Transmission / regulatory sit where the composition's
    ``charge_category`` places them (delivery vs supply). Half-plans raise, and
    so does any priced component with no side (it would otherwise be dropped).

    Markets with no default supply (Texas competitive) use ``texas_tdu`` instead.
    """
    _reject_oer_in_per_kwh(components)
    _index(components)
    delivery, supply = _split_categories(components)
    if not delivery:
        raise RecipeError("deregulated recipe missing delivery side (missing_delivery)")
    if not supply:
        raise RecipeError("deregulated recipe missing default supply (missing_supply)")
    priced = delivery + supply
    _require_consumed(components, priced, "deregulated")
    out: dict[CellKey, Decimal] = {}
    grid = _build_grid(
        priced, primary_kinds={"delivery_per_kwh", "default_supply", "base_energy"},
    )
    for key in grid:
        out[key] = _sum_cell(priced, components, key)
    return out


def recipe_texas_tdu(components: list[ComponentInput]) -> dict[CellKey, Decimal]:
    """Delivery-only (TDU) for markets with no default supply.

    Joshua 2026-10-09: store TDU delivery charges; mark supply
    ``choose_a_retailer``; never invent a supply price; never publish all-in.

    Returns **delivery** $/kWh cells only. The compiler sets
    ``has_all_in=False`` and ``supply_status=choose_a_retailer``.
    """
    _reject_oer_in_per_kwh(components)
    _index(components)
    delivery, supply = _split_categories(components)
    if supply:
        raise RecipeError(
            "texas_tdu must not include a supply price "
            "(choose_a_retailer — do not invent supply)"
        )
    if not delivery:
        raise RecipeError("texas_tdu missing delivery / TDU charges")
    _require_consumed(components, delivery, "texas_tdu")
    out: dict[CellKey, Decimal] = {}
    grid = _build_grid(delivery, primary_kinds={"delivery_per_kwh", "base_energy"})
    for key in grid:
        out[key] = _sum_cell(delivery, components, key)
    return out


def recipe_provincial_ontario(
    components: list[ComponentInput],
) -> dict[CellKey, Decimal]:
    """All-in = (RPP commodity × LF) + DC + (Net+Conn+WMSR+RRRP) × LF.

    Commodity is ``regulated_commodity``. Loss factor is a ``multiplier``
    targeting commodity + loss_sensitive delivery lines. Distribution
    volumetric (``loss_sensitive=False``) is added without LF. Per-kWh rate
    riders and credits are delivery lines (same ``loss_sensitive`` rule).

    OER is **not** applied here (bill-level note, like taxes).
    """
    _reject_oer_in_per_kwh(components)
    _index(components)
    commodity = [c for c in components if c.kind == "regulated_commodity"]
    if not commodity:
        raise RecipeError("ontario recipe missing regulated_commodity")
    delivery = [
        c for c in components
        if c.kind in {"delivery_per_kwh", "rider_per_kwh", "credit"}
    ]
    if not any(c.kind == "delivery_per_kwh" for c in delivery):
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
    lf = _as_dollars(lf_comp, CellKey())
    _require_consumed(
        components, commodity + delivery + multipliers, "provincial_ontario",
    )

    out: dict[CellKey, Decimal] = {}
    for key in _build_grid(commodity + delivery, primary_kinds={"regulated_commodity"}):
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
    by_code = _index(components)
    supply = [c for c in components if c.kind == "default_supply"]
    delivery = [
        c for c in components
        if c.kind in {
            "delivery_per_kwh", "rider_per_kwh", "credit", "base_energy",
            "rider_percent",
        }
    ]
    if not supply:
        raise RecipeError("alberta recipe missing default_supply")
    if not delivery:
        raise RecipeError("alberta recipe missing delivery components")

    multipliers = [c for c in components if c.kind == "multiplier"]
    for m in multipliers:
        targets = list(m.multiplier_target_codes or [])
        if not targets:
            raise RecipeError(f"alberta multiplier {m.code!r} has no target codes")
        missing = [t for t in targets if t not in by_code]
        if missing:
            raise RecipeError(
                f"alberta multiplier {m.code!r} targets missing codes {missing}"
            )

    def _scaled(comp: ComponentInput, key: CellKey) -> Decimal:
        amt = _as_dollars(comp, key)
        for m in multipliers:
            if comp.code in (m.multiplier_target_codes or []):
                # Stored as the remaining share (e.g. 0.9941 = 1 − 0.59%).
                amt = amt * _as_dollars(m, key)
        return amt

    priced = supply + delivery
    _require_consumed(components, priced + multipliers, "provincial_alberta")
    out: dict[CellKey, Decimal] = {}
    grid = _build_grid(
        priced, primary_kinds={"default_supply", "delivery_per_kwh", "base_energy"},
    )
    for key in grid:
        total = sum(
            (_scaled(c, key) for c in priced if c.kind != "rider_percent"),
            ZERO,
        )
        total += _percent_effect(
            [c for c in priced if c.kind == "rider_percent"], by_code, key
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
