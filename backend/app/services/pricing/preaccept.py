"""Pre-accept gates G0–G6 for component compositions (PR C).

Deterministic only. Any failure → hold (do not store). Typical-bill /
all-in tables are checks, never stored values.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable
from urllib.parse import urlparse

from app.services.pricing.compiler import CompiledPlan, compile_plan
from app.services.pricing.quote_verifier import verify_component_quote
from app.services.pricing.recipes import NON_PRICED_KINDS
from app.services.pricing.rider_census import (
    CensusResult,
    DispositionInput,
    InventoryRider,
    evaluate_rider_census,
)
from app.services.pricing.types import (
    ComponentInput,
    PlanInput,
    energy_dollars_per_kwh,
    money,
    normalize_cell_label,
)
from app.services.source_type import THIRD_PARTY_DOMAINS


# Edition markers that must never become live/current (PRC-10 / E12 / E13).
_FORBIDDEN_EDITION_MARKERS = (
    "proposed", "draft", "pro forma", "proforma", "redline",
    "illustrative", "subject to approval", "typical bill",
)

# Plausible compiled $/kWh. Below the floor or above the ceiling is almost
# always a cents/dollars (100×) slip. The ceiling leaves room for labelled
# critical-peak prices (NS Power CPP ~$1.82).
ALL_IN_MIN_DOLLARS = Decimal("0.01")
DELIVERY_MIN_DOLLARS = Decimal("0")
MAX_DOLLARS_PER_KWH = Decimal("2.50")


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


# Metadata / schedule kinds — disagreement here must not hold the plan (R29-4).
# Every other kind can move the price and must agree across models.
_METADATA_KINDS = NON_PRICED_KINDS


def _agreement_amount(c: ComponentInput, raw: Any) -> str:
    """Comparable amount: energy in $/kWh, percents/factors as printed."""
    try:
        amt = money(raw)
    except Exception:
        return f"unparsed:{raw!r}"
    if c.kind == "credit":
        amt = abs(amt)
    if c.kind in {"rider_percent", "multiplier"}:
        return f"{c.kind}:{format(amt.normalize(), 'f')}"
    try:
        dollars = energy_dollars_per_kwh(amt, c.unit)
    except ValueError:
        unit = (c.unit or "").strip().lower().replace(" ", "")
        return f"{unit}:{format(amt.normalize(), 'f')}"
    return f"$/kwh:{format(dollars.normalize(), 'f')}"


def _agreement_canon(comps: list[ComponentInput]) -> dict[str, tuple]:
    out: dict[str, tuple] = {}
    for c in comps:
        if c.kind in _METADATA_KINDS:
            continue
        cells = tuple(sorted(
            (
                *(normalize_cell_label(cell.get(d)) for d in
                  ("season", "period", "day_type", "tier")),
                _agreement_amount(c, cell.get("amount")),
            )
            for cell in c.cells
        ))
        out[c.code.strip().lower()] = (c.kind, cells)
    return out


def gate_agreement(
    extract_a: list[ComponentInput],
    extract_b: list[ComponentInput],
    *,
    recipe_code: str | None = None,
) -> list[GateFailure]:
    """G2: the two blind extractions' **applying** components agree.

    Every priced kind (including loss-factor multipliers) must match by
    code, kind, cell labels and amount (¢ ↔ $ normalized). With a
    ``recipe_code``, both sides are also compiled and must price every cell
    identically — that catches disagreements on ``loss_sensitive``, percent
    bases, multiplier targets and charge categories. Metadata-only
    differences (TOU clocks, season calendars, fixed monthly charges,
    display names) do not hold the plan (R29-4).
    """
    a, b = _agreement_canon(extract_a), _agreement_canon(extract_b)
    if a != b:
        only_a = sorted(set(a) - set(b))
        only_b = sorted(set(b) - set(a))
        disagree = sorted(k for k in set(a) & set(b) if a[k] != b[k])
        return [GateFailure(
            "G2",
            "extractors_disagree",
            f"only_a={only_a}; only_b={only_b}; disagree={disagree}",
        )]
    if not recipe_code:
        return []

    def _compiled(comps: list[ComponentInput]) -> dict:
        plan = PlanInput(
            plan_key="g2", name="g2", recipe_code=recipe_code, components=comps,
        )
        return {c.key: c.dollars_per_kwh for c in compile_plan(plan).cells}

    try:
        priced_a = _compiled(extract_a)
    except Exception:
        return []  # G4 reports model A's compile failure
    try:
        priced_b = _compiled(extract_b)
    except Exception as e:
        return [GateFailure("G2", "extract_b_not_compilable", str(e))]
    if priced_a != priced_b:
        diff = sorted(
            k.as_dict().__repr__() for k in set(priced_a) | set(priced_b)
            if priced_a.get(k) != priced_b.get(k)
        )
        return [GateFailure("G2", "compiled_disagree", "; ".join(diff))]
    return []


def gate_grounding(
    components: list[ComponentInput],
    document_text: str,
    *,
    require_row_col: bool = True,
) -> list[GateFailure]:
    """G3: every priced value's quote is verbatim with unit + row/col context.

    Unit may come from a table column/row header or section heading.
    When cells carry season/period/day_type, the quote must sit in a row
    or column that names those labels; the quoted number must match a
    stored cell amount.
    """
    failures: list[GateFailure] = []
    for c in components:
        if c.kind in {
            "season_calendar", "tou_schedule", "holiday_list",
            "tier_structure", "excluded_item", "event_day",
            "fixed_charge", "fixed_monthly", "customer_charge",
        }:
            continue
        cells = list(c.cells or [])
        if not cells:
            result = verify_component_quote(
                document_text,
                quote=c.source_quote,
                unit=c.unit,
                component_name=c.name or c.code,
                require_row_col=False,
            )
            if not result.ok:
                failures.append(GateFailure(
                    "G3", f"grounding_failed:{c.code}", result.reason
                ))
            continue
        # Prefer the cell whose amount appears in the quote; else first cell.
        quote = c.source_quote or ""
        matched_cell = None
        for cell in cells:
            amt = str(cell.get("amount") or "")
            if amt and amt in quote:
                matched_cell = cell
                break
        if matched_cell is None:
            matched_cell = cells[0]
        result = verify_component_quote(
            document_text,
            quote=c.source_quote,
            unit=c.unit,
            amount=matched_cell.get("amount"),
            cell=matched_cell,
            component_name=c.name or c.code,
            require_row_col=require_row_col,
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
    floor = ALL_IN_MIN_DOLLARS if compiled.has_all_in else DELIVERY_MIN_DOLLARS
    for cell in compiled.cells:
        value = cell.dollars_per_kwh
        if value < floor or value > MAX_DOLLARS_PER_KWH:
            failures.append(GateFailure(
                "G4", "implausible_price",
                f"{cell.key.as_dict()} = ${value}/kWh outside "
                f"[{floor}, {MAX_DOLLARS_PER_KWH}]",
            ))
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
    typical_bill_oracles: list | None = None,
    tolerance_cents: Decimal = Decimal("0.05"),
) -> list[GateFailure]:
    """G6: optional typical-bill / published all-in cross-check (never stored).

    Accepts a single ``typical_bill_cents_per_kwh`` and/or a list of
    ``TypicalBillOracle``-like objects (``.cents_per_kwh``) extracted from
    the document set. For single-cell plans, *every* provided oracle must
    match within tolerance. Multi-cell plans skip rather than guess.
    """
    if not compiled.has_all_in:
        return []  # texas_tdu has no all-in to reconcile
    oracles: list[Decimal] = []
    if typical_bill_cents_per_kwh is not None:
        oracles.append(money(typical_bill_cents_per_kwh))
    for o in typical_bill_oracles or []:
        cents = getattr(o, "cents_per_kwh", None)
        label = getattr(o, "label", None)
        if isinstance(o, dict):
            cents = o.get("cents_per_kwh", cents)
            label = o.get("label", label)
        if label == "typical_bill":
            continue  # bill ÷ kWh includes fixed charges; not an all-in rate
        if cents is not None:
            oracles.append(money(cents))
    if not oracles:
        return []
    cells = compiled.cents_sorted()
    if len(cells) != 1:
        # Multi-cell plans: oracle must name the cell; skip rather than guess.
        return []
    failures: list[GateFailure] = []
    for oracle in oracles:
        delta = abs(cells[0] - oracle)
        if delta > tolerance_cents:
            failures.append(GateFailure(
                "G6",
                "typical_bill_mismatch",
                f"compiled={cells[0]} oracle={oracle} delta={delta}",
            ))
    return failures


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
    typical_bill_oracles: list | None = None,
) -> PreAcceptResult:
    """Run G0–G6. Accepted only when every gate passes.

    ``extract_a`` / ``extract_b`` are each blind model's **applying**
    components (G2 compares them and compiles both).

    Pass a non-empty ``inventory`` to exercise G5. Pass typical-bill oracles
    (from ``inventory_from_docs.extract_typical_bill_oracles``) to exercise G6.
    """
    failures: list[GateFailure] = []
    failures.extend(gate_admissibility(
        source_url=source_url, official_hosts=official_hosts
    ))
    failures.extend(gate_edition(
        edition_label=edition_label, document_text=document_text
    ))
    failures.extend(gate_agreement(
        extract_a, extract_b, recipe_code=plan.recipe_code,
    ))
    failures.extend(gate_grounding(plan.components, document_text))

    compiled: CompiledPlan | None = None
    try:
        compiled = compile_plan(plan)
        failures.extend(gate_structure(plan, compiled))
        failures.extend(gate_oracle(
            compiled,
            typical_bill_cents_per_kwh=typical_bill_cents_per_kwh,
            typical_bill_oracles=typical_bill_oracles,
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
