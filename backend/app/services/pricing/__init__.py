"""Deterministic all-in price compiler (no LLM).

Public surface:
- ``compile_plan`` — recipe + applying components → all-in $/kWh cells
  (or delivery-only for ``texas_tdu``)
- ``load_golden_plans`` / ``GOLDEN_DIR`` — hand-verified fixtures
- ``build_document_set`` — per-utility official document bundle
- ``discover_document_set`` — Phase 1+2 crawl → filtered document set
"""
from app.services.pricing.compiler import CompiledPlan, compile_plan, load_golden_plans
from app.services.pricing.discover import (
    DiscoveryResult,
    discover_document_set,
)
from app.services.pricing.document_set import (
    DocumentCandidate,
    DocumentSetResult,
    build_document_set,
    build_golden_document_set,
    r27_coverage_report,
)
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
    "DocumentCandidate",
    "DocumentSetResult",
    "OER_BILL_LEVEL_NOTE",
    "PlanInput",
    "SUPPLY_STATUS_CHOOSE_RETAILER",
    "TEXAS_TDU_RECIPE",
    "DiscoveryResult",
    "build_document_set",
    "build_golden_document_set",
    "compile_plan",
    "discover_document_set",
    "load_golden_plans",
    "money",
    "r27_coverage_report",
    "to_dollars_per_kwh",
]
