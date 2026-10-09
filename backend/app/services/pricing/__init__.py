"""Deterministic all-in price compiler (no LLM).

Public surface:
- ``compile_plan`` — recipe + applying components → all-in $/kWh cells
- ``load_golden_plan`` / ``GOLDEN_DIR`` — hand-verified fixtures for PR A
"""
from app.services.pricing.compiler import CompiledPlan, compile_plan
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
    "PlanInput",
    "compile_plan",
    "money",
    "to_dollars_per_kwh",
]
