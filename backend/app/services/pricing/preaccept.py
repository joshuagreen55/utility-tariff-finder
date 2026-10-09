"""Pre-accept gates G0–G6 for component compositions (PR C).

Deterministic only. Any failure → hold (do not store). Typical-bill /
all-in tables are checks, never stored values.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Iterable
from urllib.parse import urlparse

from app.services.pricing.compiler import CompiledPlan, compile_plan
from app.services.pricing.quote_verifier import verify_component_quote
from app.services.pricing.rider_census import (
    CensusResult,
    DispositionInput,
    InventoryRider,
    evaluate_rider_census,
)
from app.services.pricing.types import ComponentInput, PlanInput, money
from app.services.source_type import THIRD_PARTY_DOMAINS


# Edition markers that must never become live/current (PRC-10 / E12 / E13).
_FORBIDDEN_EDITION_MARKERS = (
    "proposed", "draft", "pro forma", "proforma", "redline",
    "illustrative", "subject to approval", "typical bill",
)


@dataclass
class GateFailure:
    gate: str  # G0..G6
    reason: str
    detail: str = ""


@dataclass
class PreAcceptResult:
    accepted: bool
    failures: list[GateFailure] = field(default_factory=list)
    compiled: CompiledPlan | None = None
    census: CensusResult | None = None

    @property
    def hold(self) -> bool:
        return not self.accepted


def _host(url: str | None) -> str:
    if not url:
        return ""
    try:
        return (urlparse(url).hostname or "").lower()
    except Exception:
        return ""


def gate_admissibility(
    *,
    source_url: str | None,
    official_hosts: Iterable[str],
) -> list[GateFailure]:
    """G0: document domain on official allowlist (or labelled third-party later)."""
    host = _host(source_url)
    if not host:
        return [GateFailure("G0", "missing_source_url")]
    # Hard-block known aggregators.
    for blocked in THIRD_PARTY_DOMAINS:
        if host == blocked or host.endswith("." + blocked):
            return [GateFailure("G0", "third_party_blocked", host)]
    allow = {h.lower().lstrip(".") for h in official_hosts if h}
    if allow and not any(host == a or host.endswith("." + a) for a in allow):
        return [GateFailure("G0", "domain_not_allowlisted", host)]
    return []


def gate_edition(
    *,
    edition_label: str | None,
    document_text: str | None = None,
) -> list[GateFailure]:
    """G1: reject draft / pro-forma / proposed / typical-bill-as-plan."""
    hay = " ".join(
        x for x in (edition_label or "", (document_text or "")[:2000]) if x
    ).lower()
    for marker in _FORBIDDEN_EDITION_MARKERS:
        if marker in hay:
            return [GateFailure("G1", "forbidden_edition_marker", marker)]
    return []


def gate_agreement(
    extract_a: list[ComponentInput],
    extract_b: list[ComponentInput],
) -> list[GateFailure]:
    """G2: two blind extractions agree after canonicalisation."""
    def _canon(comps: list[ComponentInput]) -> dict[str, tuple]:
        out = {}
        for c in comps:
            cells = tuple(
                sorted(
                    (
                        str(cell.get("season") or "all"),
                        str(cell.get("period") or "all"),
                        str(cell.get("day_type") or "all"),
                        str(cell.get("tier") or "all"),
                        str(money(cell["amount"])),
                    )
                    for cell in c.cells
                )
            )
            out[c.code] = (c.kind, c.unit, cells)
        return out

    a, b = _canon(extract_a), _canon(extract_b)
    if a != b:
        only_a = sorted(set(a) - set(b))
        only_b = sorted(set(b) - set(a))
        disagree = sorted(k for k in set(a) & set(b) if a[k] != b[k])
        return [GateFailure(
            "G2",
            "extractors_disagree",
            f"only_a={only_a}; only_b={only_b}; disagree={disagree}",
        )]
    return []


def gate_grounding(
    components: list[ComponentInput],
    document_text: str,
) -> list[GateFailure]:
    """G3: every priced value's quote is verbatim in the document with unit."""
    failures: list[GateFailure] = []
    for c in components:
        if c.kind in {
            "season_calendar", "tou_schedule", "holiday_list",
            "tier_structure", "excluded_item", "event_day",
        }:
            continue
        result = verify_component_quote(
            document_text, quote=c.source_quote, unit=c.unit
        )
        if not result.ok:
            failures.append(GateFailure(
                "G3", f"grounding_failed:{c.code}", result.reason
            ))
    return failures


def gate_structure(plan: PlanInput, compiled: CompiledPlan) -> list[GateFailure]:
    """G4: structural coverage + all-in equals sum of parts (recompiled)."""
    failures: list[GateFailure] = []
    if not compiled.cells and compiled.has_all_in:
        failures.append(GateFailure("G4", "no_priced_cells"))
    # Recompile independently — arithmetic identity check.
    again = compile_plan(plan)
    if again.dollars_sorted() != compiled.dollars_sorted():
        failures.append(GateFailure("G4", "recompile_mismatch"))
    if again.has_all_in != compiled.has_all_in:
        failures.append(GateFailure("G4", "all_in_flag_mismatch"))
    return failures


def gate_census(
    inventory: list[InventoryRider],
    dispositions: list[DispositionInput],
) -> tuple[list[GateFailure], CensusResult]:
    """G5: closed-world rider census fully dispositioned."""
    census = evaluate_rider_census(inventory, dispositions)
    if census.complete:
        return [], census
    return [GateFailure("G5", "census_incomplete", ";".join(census.reasons))], census


def gate_oracle(
    compiled: CompiledPlan,
    *,
    typical_bill_cents_per_kwh: Decimal | None = None,
    tolerance_cents: Decimal = Decimal("0.05"),
) -> list[GateFailure]:
    """G6: optional typical-bill / published all-in cross-check (never stored)."""
    if typical_bill_cents_per_kwh is None:
        return []
    if not compiled.has_all_in:
        return []  # texas_tdu has no all-in to reconcile
    cells = compiled.cents_sorted()
    if len(cells) != 1:
        # Multi-cell plans: oracle must name the cell; skip rather than guess.
        return []
    delta = abs(cells[0] - typical_bill_cents_per_kwh)
    if delta > tolerance_cents:
        return [GateFailure(
            "G6",
            "typical_bill_mismatch",
            f"compiled={cells[0]} oracle={typical_bill_cents_per_kwh} delta={delta}",
        )]
    return []


def run_preaccept(
    plan: PlanInput,
    *,
    document_text: str,
    source_url: str | None,
    official_hosts: Iterable[str],
    extract_a: list[ComponentInput],
    extract_b: list[ComponentInput],
    inventory: list[InventoryRider] | None = None,
    dispositions: list[DispositionInput] | None = None,
    edition_label: str | None = None,
    typical_bill_cents_per_kwh: Decimal | None = None,
) -> PreAcceptResult:
    """Run G0–G6. Accepted only when every gate passes."""
    failures: list[GateFailure] = []
    failures.extend(gate_admissibility(
        source_url=source_url, official_hosts=official_hosts
    ))
    failures.extend(gate_edition(
        edition_label=edition_label, document_text=document_text
    ))
    failures.extend(gate_agreement(extract_a, extract_b))
    failures.extend(gate_grounding(plan.components, document_text))

    compiled: CompiledPlan | None = None
    try:
        compiled = compile_plan(plan)
        failures.extend(gate_structure(plan, compiled))
        failures.extend(gate_oracle(
            compiled, typical_bill_cents_per_kwh=typical_bill_cents_per_kwh
        ))
    except Exception as e:  # RecipeError and friends
        failures.append(GateFailure("G4", "compile_failed", str(e)))

    census = None
    if inventory is not None:
        f, census = gate_census(inventory, dispositions or [])
        failures.extend(f)

    return PreAcceptResult(
        accepted=not failures,
        failures=failures,
        compiled=compiled,
        census=census,
    )
