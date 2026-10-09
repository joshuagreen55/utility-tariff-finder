"""Deterministic all-in price compiler (no LLM).

Public surface:
- ``compile_plan`` — recipe + applying components → all-in $/kWh cells
  (or delivery-only for ``texas_tdu``)
- ``load_golden_plans`` / ``GOLDEN_DIR`` — hand-verified fixtures
"""
from app.services.pricing.compiler import CompiledPlan, compile_plan, load_golden_plans
from app.services.pricing.policy import (
    OER_BILL_LEVEL_NOTE,
    SUPPLY_STATUS_CHOOSE_RETAILER,
    TEXAS_TDU_RECIPE,
)
from app.services.pricing.types import (
    CellKey,
    ComponentInput,
    PlanInput,
    money,
    to_dollars_per_kwh,
)

__all__ = [
    "CellKey",
    "CompiledPlan",
    "ComponentInput",
    "OER_BILL_LEVEL_NOTE",
    "PlanInput",
    "SUPPLY_STATUS_CHOOSE_RETAILER",
    "TEXAS_TDU_RECIPE",
    "compile_plan",
    "load_golden_plans",
    "money",
    "to_dollars_per_kwh",
]
