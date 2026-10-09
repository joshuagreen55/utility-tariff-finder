"""Compile a plan composition into all-in $/kWh cells (exact Decimal)."""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

from app.services.pricing.policy import (
    OER_BILL_LEVEL_NOTE,
    SUPPLY_STATUS_CHOOSE_RETAILER,
    TEXAS_TDU_RECIPE,
)
from app.services.pricing.recipes import RecipeError, get_recipe
from app.services.pricing.types import (
    CellKey,
    ComponentInput,
    PlanInput,
    money,
)

GOLDEN_DIR = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "pricing_golden"


@dataclass(frozen=True)
class CompiledCell:
    key: CellKey
    dollars_per_kwh: Decimal

    @property
    def cents_per_kwh(self) -> Decimal:
        return self.dollars_per_kwh * Decimal("100")


@dataclass
class CompiledPlan:
    plan_key: str
    recipe_code: str
    cells: list[CompiledCell]
    # False for texas_tdu: cells are delivery-only, not a household all-in.
    has_all_in: bool = True
    supply_status: str | None = None
    bill_level_notes: list[str] = field(default_factory=list)
    breakdown_note: str | None = None

    def dollars_sorted(self) -> list[Decimal]:
        """Unique cell $/kWh values, sorted (all-in or delivery-only)."""
        return sorted({c.dollars_per_kwh for c in self.cells})

    def cents_sorted(self, places: int | None = None) -> list[Decimal]:
        vals = sorted({c.cents_per_kwh for c in self.cells})
        if places is None:
            return vals
        q = Decimal(10) ** -places
        return sorted({v.quantize(q) for v in vals})


def _bill_notes_for(plan: PlanInput) -> list[str]:
    notes = list(plan.bill_level_notes)
    if plan.recipe_code == "provincial_ontario":
        if OER_BILL_LEVEL_NOTE not in notes:
            notes.append(OER_BILL_LEVEL_NOTE)
    return notes


def compile_plan(plan: PlanInput) -> CompiledPlan:
    """Run the market recipe. Raises ``RecipeError`` on incomplete compositions."""
    if not plan.components:
        raise RecipeError(f"plan {plan.plan_key!r} has no applying components")
    recipe = get_recipe(plan.recipe_code)
    raw = recipe(plan.components)
    cells = [
        CompiledCell(key=k, dollars_per_kwh=v)
        for k, v in sorted(raw.items(), key=lambda kv: kv[0])
    ]
    has_all_in = plan.recipe_code != TEXAS_TDU_RECIPE
    supply_status = (
        SUPPLY_STATUS_CHOOSE_RETAILER if plan.recipe_code == TEXAS_TDU_RECIPE else None
    )
    return CompiledPlan(
        plan_key=plan.plan_key,
        recipe_code=plan.recipe_code,
        cells=cells,
        has_all_in=has_all_in,
        supply_status=supply_status,
        bill_level_notes=_bill_notes_for(plan),
        breakdown_note=(
            "delivery_only; supply=choose_a_retailer"
            if not has_all_in
            else None
        ),
    )


def component_from_dict(raw: dict[str, Any]) -> ComponentInput:
    return ComponentInput(
        code=str(raw["code"]),
        kind=str(raw["kind"]),
        unit=str(raw["unit"]),
        cells=list(raw.get("cells") or []),
        name=str(raw.get("name") or raw["code"]),
        charge_category=raw.get("charge_category"),
        percent_base_codes=list(raw.get("percent_base_codes") or []),
        multiplier_target_codes=list(raw.get("multiplier_target_codes") or []),
        loss_sensitive=bool(raw.get("loss_sensitive") or False),
        source_page=raw.get("source_page"),
        source_quote=raw.get("source_quote"),
    )


def plan_from_dict(raw: dict[str, Any]) -> PlanInput:
    comps = [component_from_dict(c) for c in raw.get("components") or []]
    # Only dispositions that apply feed the compiler (PR B will enforce census).
    applying = []
    for c, meta in zip(comps, raw.get("components") or []):
        disp = str(meta.get("disposition") or "applies")
        if disp == "applies":
            applying.append(c)
    official = [money(x) for x in (raw.get("official_cents") or [])]
    delivery_official = [money(x) for x in (raw.get("official_delivery_cents") or [])]
    expected = [money(x) for x in (raw.get("expected_dollars") or [])]
    return PlanInput(
        plan_key=str(raw["plan_key"]),
        name=str(raw["name"]),
        recipe_code=str(raw["recipe_code"]),
        components=applying,
        code=raw.get("code"),
        rate_type=raw.get("rate_type"),
        utility_name=raw.get("utility_name"),
        official_cents=official,
        official_delivery_cents=delivery_official,
        expected_dollars=expected,
        bill_level_notes=list(raw.get("bill_level_notes") or []),
        notes=raw.get("notes"),
        clocks=list(raw.get("clocks") or []),
        seasons=list(raw.get("seasons") or []),
    )


def load_golden_plans(path: Path | None = None) -> list[PlanInput]:
    """Load hand-verified golden plans from ``plans.json``."""
    import json

    root = path or GOLDEN_DIR
    data = json.loads((root / "plans.json").read_text())
    return [plan_from_dict(p) for p in data["plans"]]


__all__ = [
    "CompiledCell",
    "CompiledPlan",
    "GOLDEN_DIR",
    "RecipeError",
    "compile_plan",
    "component_from_dict",
    "load_golden_plans",
    "plan_from_dict",
]
