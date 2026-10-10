"""Pre-accept gates G0–G6 for component compositions (PR C).

Deterministic only. Any failure → hold (do not store). Typical-bill /
all-in tables are checks, never stored values.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Iterable
from urllib.parse import urlparse

from app.services.pricing.compiler import CompiledPlan, compile_plan
from app.services.pricing.quote_verifier import verify_component_cells
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
from app.services.source_type import (
    OFFICIAL,
    UtilitySourceContext,
    classify_source,
    has_official_host,
    is_state_supply_publisher_host,
    is_third_party_host,
    normalize_host,
)


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
    official_hosts: Iterable[str] = (),
    source_ctx: UtilitySourceContext | None = None,
) -> list[GateFailure]:
    """G0: the source document is published by the utility or a sanctioned board.

    Admitted when ``classify_source`` calls it official for the utility's
    known site (``source_ctx``: website, configured rate URLs, and the
    jurisdiction's rate-publishing board), when it is a state supply
    publisher, or when its host is in the caller's ``official_hosts``. The
    allowlist must come from what is known about the utility, never from
    the URLs that were fetched; with nothing known the plan holds.
    """
    host = _host(source_url)
    if not host:
        return [GateFailure("G0", "missing_source_url")]
    if is_third_party_host(host):
        return [GateFailure("G0", "third_party_blocked", host)]
    if is_state_supply_publisher_host(host):
        return []
    allow = {normalize_host(h) or h.lower().lstrip(".") for h in official_hosts if h}
    if any(host == a or host.endswith("." + a) for a in allow):
        return []
    if source_ctx is not None:
        cls = classify_source(source_url, source_ctx)
        if cls.source_type == OFFICIAL:
            return []
        if not allow and not has_official_host(source_ctx) and cls.reason != "generic_host":
            return [GateFailure("G0", "no_official_host", host)]
        return [GateFailure("G0", "domain_not_official", f"{host}:{cls.reason}")]
    if not allow:
        return [GateFailure("G0", "no_official_host", host)]
    return [GateFailure("G0", "domain_not_allowlisted", host)]


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
    """G2: the two blind extractions price the plan identically.

    With a ``recipe_code`` both sides are compiled and must produce the same
    multiset of all-in $/kWh prices. Codes, kinds, names and cell labels are
    each model's own words and are not compared — only what they price.
    That still catches a disagreement on any amount, rider, percent base,
    multiplier, ``loss_sensitive`` or charge side, and ignores metadata
    (clocks, calendars, fixed monthly charges) (R29-4).

    Without a recipe, the priced components must match by kind, cell labels
    and amount.
    """
    if not recipe_code:
        a = sorted(_agreement_canon(extract_a).values())
        b = sorted(_agreement_canon(extract_b).values())
        if a != b:
            return [GateFailure("G2", "extractors_disagree", f"a={a}; b={b}")]
        return []

    def _compiled(comps: list[ComponentInput]) -> list[Decimal]:
        plan = PlanInput(
            plan_key="g2", name="g2", recipe_code=recipe_code, components=comps,
        )
        return sorted(
            c.dollars_per_kwh.quantize(Decimal("0.000001"))
            for c in compile_plan(plan).cells
        )

    try:
        priced_a = _compiled(extract_a)
    except Exception:
        return []  # G4 reports model A's compile failure
    try:
        priced_b = _compiled(extract_b)
    except Exception as e:
        return [GateFailure("G2", "extract_b_not_compilable", str(e))]
    if priced_a != priced_b:
        return [GateFailure(
            "G2", "compiled_disagree",
            f"a={[str(v) for v in priced_a]}; b={[str(v) for v in priced_b]}",
        )]
    return []


def gate_grounding(
    components: list[ComponentInput],
    document_text: str,
) -> list[GateFailure]:
    """G3: every priced value's quote is verbatim and prints its amount + unit.

    Which row a cell is (season / period / tier) is the model's call,
    cross-checked by G2; see ``quote_verifier``.
    """
    failures: list[GateFailure] = []
    for c in components:
        if c.kind in {
            "season_calendar", "tou_schedule", "holiday_list",
            "tier_structure", "excluded_item", "event_day",
            "fixed_charge", "fixed_monthly", "customer_charge",
        }:
            continue
        result = verify_component_cells(document_text, c)
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


_PER_KWH_PRICED = frozenset({
    "base_energy", "rider_per_kwh", "rider_percent", "credit",
    "delivery_per_kwh", "default_supply", "regulated_commodity",
})


def _named_in(document_norm: str, c: ComponentInput) -> bool:
    for label in (c.name, c.code):
        words = re.sub(r"[^a-z0-9]+", " ", (label or "").lower()).strip()
        if len(words) >= 3 and f" {words} " in document_norm:
            return True
    return False


_RIDER_KINDS = frozenset({"rider_per_kwh", "rider_percent", "credit"})


def gate_completeness(
    document_text: str,
    extracts: Iterable[list[ComponentInput]],
    *,
    recipe_code: str | None = None,
    inventory_closed: bool = False,
) -> list[GateFailure]:
    """G5: the all-in leaves out nothing the models themselves know about.

    Checked on each model's full extract (every disposition):

    * a per-kWh charge marked ``not_found`` whose name or code is printed in
      the document — the model knows the charge exists but could not read
      its value, so any all-in would be understated;
    * a full-bill recipe where neither model dispositioned any rider,
      adjustment or credit as anything but ``applies`` — it priced what it
      happened to see but never enumerated the rider list (no
      not_applicable / optional / not_found decisions) and no non-empty
      document-derived rider inventory was closed, so the all-in cannot be
      shown complete.
    """
    failures: list[GateFailure] = []
    extracts = list(extracts)
    doc_norm = " " + re.sub(r"[^a-z0-9]+", " ", (document_text or "").lower()) + " "
    for i, comps in enumerate(extracts):
        side = "ab"[i] if i < 2 else str(i)
        for c in comps:
            disp = (c.disposition or "applies").strip().lower()
            if disp == "not_found" and c.kind in _PER_KWH_PRICED and _named_in(doc_norm, c):
                failures.append(GateFailure(
                    "G5", "charge_value_not_found", f"model_{side}:{c.code}",
                ))
    if recipe_code in _FULL_BILL_RECIPES and not inventory_closed and not any(
        c.kind in _RIDER_KINDS
        and (c.disposition or "applies").strip().lower() != "applies"
        for comps in extracts for c in comps
    ):
        failures.append(GateFailure("G5", "no_rider_census"))
    return failures


_FULL_BILL_RECIPES = frozenset({"bundled", "deregulated", "provincial_alberta"})


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
    source_ctx: UtilitySourceContext | None = None,
    full_extracts: list[list[ComponentInput]] | None = None,
) -> PreAcceptResult:
    """Run G0–G6. Accepted only when every gate passes.

    G0 needs what is known about the utility: ``source_ctx`` (production:
    ``source_type.context_from_utility``) and/or explicit ``official_hosts``.

    ``extract_a`` / ``extract_b`` are each blind model's **applying**
    components (G2 compares them and compiles both).

    Pass a non-empty ``inventory`` to exercise G5. Pass typical-bill oracles
    (from ``inventory_from_docs.extract_typical_bill_oracles``) to exercise G6.
    """
    failures: list[GateFailure] = []
    failures.extend(gate_admissibility(
        source_url=source_url, official_hosts=official_hosts, source_ctx=source_ctx,
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

    if full_extracts is not None:
        failures.extend(gate_completeness(
            document_text, full_extracts, recipe_code=plan.recipe_code,
            inventory_closed=bool(inventory) and census is not None and census.complete,
        ))

    return PreAcceptResult(
        accepted=not failures,
        failures=failures,
        compiled=compiled,
        census=census,
    )
