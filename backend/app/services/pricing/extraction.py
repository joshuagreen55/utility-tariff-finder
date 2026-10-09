"""Dual-blind component extraction (PR C). Feature-flagged OFF by default.

Two models read the same retained document independently — neither sees
prior stored values nor the other model's output. Every number must carry
page + verbatim quote. Disagreement or failed quote → hold (no store).

Model IDs are the repo's existing ones (never invent new ids):
  claude-haiku-5-5 / claude-sonnet-5-5 / claude-opus-5-5
"""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from decimal import InvalidOperation
from typing import Any, Callable

from app.services.pricing.amount_parse import (
    normalize_amount_string,
    sanitize_extract_amounts,
)
from app.services.pricing.extract_schema import (
    applying_raw_components,
    disposition_of,
    validate_extract_schema,
)
from app.services.pricing.preaccept import PreAcceptResult, run_preaccept
from app.services.pricing.quote_verifier import verify_component_cells
from app.services.pricing.rider_census import DispositionInput, InventoryRider
from app.services.pricing.types import ComponentInput, PlanInput

# Existing model ids only — do not add or rename.
HAIKU_MODEL = os.environ.get("HAIKU_MODEL", "claude-haiku-5-5")
SONNET_MODEL = os.environ.get("SONNET_MODEL", "claude-sonnet-5-5")
OPUS_MODEL = os.environ.get("OPUS_MODEL", "claude-opus-5-5")

# Master kill switch for the new path (PR C). Default off.
COMPONENT_EXTRACTION_ENABLED = os.environ.get(
    "COMPONENT_EXTRACTION_ENABLED", "0"
).strip().lower() in {"1", "true", "yes", "on"}


def component_extraction_enabled() -> bool:
    return COMPONENT_EXTRACTION_ENABLED


ExtractFn = Callable[[str, str, dict[str, Any]], list[dict[str, Any]]]
# (document_text, model_id, context) -> list of raw component dicts


@dataclass
class ExtractionHold:
    reason: str
    detail: str = ""
    extract_a: list[ComponentInput] = field(default_factory=list)
    extract_b: list[ComponentInput] = field(default_factory=list)
    preaccept: PreAcceptResult | None = None


@dataclass
class ExtractionAccept:
    plan: PlanInput
    preaccept: PreAcceptResult
    model_a: str
    model_b: str
    # Full extracts (incl. tou_schedule / season_calendar meta) for clock scoring.
    extract_a: list[ComponentInput] = field(default_factory=list)
    extract_b: list[ComponentInput] = field(default_factory=list)


def _raw_to_component(raw: dict[str, Any]) -> ComponentInput:
    cells = []
    for cell in raw.get("cells") or []:
        if not isinstance(cell, dict):
            continue
        cell = dict(cell)
        if "amount" in cell:
            norm = normalize_amount_string(cell["amount"])
            if norm is None:
                continue
            cell["amount"] = norm
        cells.append(cell)
    return ComponentInput(
        code=str(raw["code"]),
        kind=str(raw["kind"]),
        unit=str(raw["unit"]),
        cells=cells,
        name=str(raw.get("name") or raw["code"]),
        charge_category=raw.get("charge_category"),
        percent_base_codes=list(raw.get("percent_base_codes") or []),
        multiplier_target_codes=list(raw.get("multiplier_target_codes") or []),
        loss_sensitive=bool(raw.get("loss_sensitive") or False),
        source_page=raw.get("source_page") or raw.get("page"),
        source_quote=raw.get("source_quote") or raw.get("quote"),
    )


_CENSUS_KINDS = frozenset({
    "rider_per_kwh", "rider_percent", "credit", "excluded_item", "event_day",
})


def _dispositions_from_raw(
    raw_components: list[dict[str, Any]],
) -> list[DispositionInput]:
    """Build census dispositions from extract disposition fields (riders only)."""
    out: list[DispositionInput] = []
    for raw in raw_components:
        kind = str(raw.get("kind") or "")
        if kind not in _CENSUS_KINDS:
            continue
        disp = disposition_of(raw)
        if not disp:
            continue
        page = str(
            raw.get("source_page") or raw.get("disposition_page") or ""
        ).strip()
        if not page and disp in {"not_found", "not_applicable"}:
            page = disp  # auditable marker when no value page exists
        quote = str(
            raw.get("source_quote")
            or raw.get("disposition_quote")
            or raw.get("name")
            or raw["code"]
        )
        out.append(DispositionInput(
            rider_code=str(raw["code"]),
            disposition=disp,
            disposition_page=page or None,
            disposition_quote=quote,
        ))
    return out


def _require_citations(comps: list[ComponentInput]) -> str | None:
    """Citations required only for priced (applies) components."""
    for c in comps:
        if c.kind in {
            "season_calendar", "tou_schedule", "holiday_list",
            "tier_structure", "excluded_item", "event_day",
        }:
            continue
        if not c.source_quote or not str(c.source_quote).strip():
            return f"missing_quote:{c.code}"
        if not c.source_page or not str(c.source_page).strip():
            return f"missing_page:{c.code}"
    return None


def dual_extract_components(
    document_text: str,
    *,
    plan_meta: dict[str, Any],
    extract_fn: ExtractFn,
    model_a: str | None = None,
    model_b: str | None = None,
    official_hosts: list[str] | None = None,
    source_url: str | None = None,
    inventory: list[InventoryRider] | None = None,
    dispositions: list[DispositionInput] | None = None,
    edition_label: str | None = None,
    typical_bill_cents_per_kwh: Any = None,
    force: bool = False,
) -> ExtractionAccept | ExtractionHold:
    """Run two blind extractions and pre-accept gates.

    Returns ``ExtractionAccept`` only when both extracts agree, every quote
    grounds, and G0–G6 pass. Otherwise ``ExtractionHold`` (caller must not
    store). Respects ``COMPONENT_EXTRACTION_ENABLED`` unless ``force=True``
    (tests).
    """
    if not force and not component_extraction_enabled():
        return ExtractionHold(
            reason="feature_flag_off",
            detail="COMPONENT_EXTRACTION_ENABLED is off (default)",
        )

    m_a = model_a or HAIKU_MODEL
    m_b = model_b or SONNET_MODEL
    if m_a == m_b:
        return ExtractionHold(
            reason="models_not_independent",
            detail="dual extraction requires two different model ids",
        )

    # Blind: each call gets the document only — no prior values in context.
    ctx = {
        "plan_key": plan_meta.get("plan_key"),
        "recipe_code": plan_meta.get("recipe_code"),
        "utility_name": plan_meta.get("utility_name"),
        # Explicitly omit any prior rate values.
        "blind": True,
    }

    _ENERGY_KINDS = {
        "base_energy", "delivery_energy", "supply_energy", "commodity_energy",
        "regulated_commodity", "default_supply", "delivery_per_kwh",
        "supply_per_kwh", "energy",
    }

    def _has_energy(comps: list[ComponentInput]) -> bool:
        return any(
            c.kind in _ENERGY_KINDS
            or c.kind.endswith("_energy")
            or c.kind.endswith("_per_kwh")
            or "energy" in c.kind
            or "commodity" in c.kind
            or "supply" in c.kind
            for c in comps
        )

    def _pull(model: str, *, energy_retry: bool = False) -> list[dict[str, Any]]:
        call_ctx = dict(ctx)
        if energy_retry:
            # One-shot nudge only — still blind to prior numeric values.
            call_ctx["require_applying_energy"] = True
            call_ctx["retry_reason"] = "missing_energy_charge"
        return sanitize_extract_amounts(
            list(extract_fn(document_text, model, call_ctx) or [])
        )

    raw_a = _pull(m_a)
    raw_b = _pull(m_b)

    # Schema first — numbers only, unique codes/names, explicit dispositions.
    for label, raw in (("a", raw_a), ("b", raw_b)):
        schema_err = validate_extract_schema(raw)
        if schema_err:
            return ExtractionHold(
                reason="schema_invalid",
                detail=f"model_{label}:{schema_err}",
            )

    try:
        comps_a = [_raw_to_component(r) for r in raw_a]
        comps_b = [_raw_to_component(r) for r in raw_b]
        applying_a = [
            _raw_to_component(r) for r in applying_raw_components(raw_a)
        ]
        applying_b = [
            _raw_to_component(r) for r in applying_raw_components(raw_b)
        ]
    except (KeyError, TypeError, ValueError, InvalidOperation) as e:
        return ExtractionHold(reason="malformed_extract", detail=str(e))

    # R29-3: every plan needs ≥1 applying energy (/kWh) charge. One retry
    # per side when the first pass returned only fixed/meta rows.
    retried_a = retried_b = False
    if not _has_energy(applying_a):
        raw_a = _pull(m_a, energy_retry=True)
        schema_err = validate_extract_schema(raw_a)
        if schema_err:
            return ExtractionHold(
                reason="schema_invalid",
                detail=f"model_a:retry:{schema_err}",
            )
        try:
            comps_a = [_raw_to_component(r) for r in raw_a]
            applying_a = [
                _raw_to_component(r) for r in applying_raw_components(raw_a)
            ]
        except (KeyError, TypeError, ValueError, InvalidOperation) as e:
            return ExtractionHold(reason="malformed_extract", detail=str(e))
        retried_a = True
    if not _has_energy(applying_b):
        raw_b = _pull(m_b, energy_retry=True)
        schema_err = validate_extract_schema(raw_b)
        if schema_err:
            return ExtractionHold(
                reason="schema_invalid",
                detail=f"model_b:retry:{schema_err}",
            )
        try:
            comps_b = [_raw_to_component(r) for r in raw_b]
            applying_b = [
                _raw_to_component(r) for r in applying_raw_components(raw_b)
            ]
        except (KeyError, TypeError, ValueError, InvalidOperation) as e:
            return ExtractionHold(reason="malformed_extract", detail=str(e))
        retried_b = True

    if not _has_energy(applying_a):
        return ExtractionHold(
            reason="missing_energy_charge",
            detail="model_a:no_applying_energy_component"
            + (":after_retry" if retried_a else ""),
            extract_a=comps_a, extract_b=comps_b,
        )
    if not _has_energy(applying_b):
        return ExtractionHold(
            reason="missing_energy_charge",
            detail="model_b:no_applying_energy_component"
            + (":after_retry" if retried_b else ""),
            extract_a=comps_a, extract_b=comps_b,
        )

    # Citations + quote grounding only for PRICED (applies) components.
    # not_found / not_applicable / optional may omit values without holding
    # the whole extract (R28-1).
    cite_a = _require_citations(applying_a)
    if cite_a:
        return ExtractionHold(
            reason="citation_incomplete", detail=f"model_a:{cite_a}",
            extract_a=comps_a, extract_b=comps_b,
        )
    cite_b = _require_citations(applying_b)
    if cite_b:
        return ExtractionHold(
            reason="citation_incomplete", detail=f"model_b:{cite_b}",
            extract_a=comps_a, extract_b=comps_b,
        )

    for label, comps in (("a", applying_a), ("b", applying_b)):
        for c in comps:
            if c.kind in {
                "season_calendar", "tou_schedule", "holiday_list",
                "tier_structure", "excluded_item", "event_day",
            }:
                continue
            vr = verify_component_cells(document_text, c, require_row_col=True)
            if not vr.ok:
                return ExtractionHold(
                    reason="quote_verify_failed",
                    detail=f"model_{label}:{c.code}:{vr.reason}",
                    extract_a=comps_a, extract_b=comps_b,
                )

    # Compiler and G2 see only applying components: a rider one model calls
    # optional / not_applicable must not agree with one the other prices.
    plan = PlanInput(
        plan_key=str(plan_meta["plan_key"]),
        name=str(plan_meta.get("name") or plan_meta["plan_key"]),
        recipe_code=str(plan_meta["recipe_code"]),
        components=applying_a,
        code=plan_meta.get("code"),
        rate_type=plan_meta.get("rate_type"),
        utility_name=plan_meta.get("utility_name"),
    )

    # Prefer caller-supplied dispositions; else derive from extract fields.
    disps = dispositions
    if disps is None:
        disps = _dispositions_from_raw(raw_a)

    pre = run_preaccept(
        plan,
        document_text=document_text,
        source_url=source_url or plan_meta.get("source_url"),
        official_hosts=official_hosts or [],
        extract_a=applying_a,
        extract_b=applying_b,
        inventory=inventory,
        dispositions=disps,
        edition_label=edition_label or plan_meta.get("edition_label"),
        typical_bill_cents_per_kwh=typical_bill_cents_per_kwh,
    )
    if pre.hold:
        reasons = ",".join(f"{f.gate}:{f.reason}" for f in pre.failures)
        return ExtractionHold(
            reason="preaccept_failed",
            detail=reasons,
            extract_a=comps_a,
            extract_b=comps_b,
            preaccept=pre,
        )

    return ExtractionAccept(
        plan=plan,
        preaccept=pre,
        model_a=m_a,
        model_b=m_b,
        extract_a=comps_a,
        extract_b=comps_b,
    )


def parse_tool_components(payload: str | dict) -> list[dict[str, Any]]:
    """Parse a model tool-call / JSON payload into raw component dicts."""
    if isinstance(payload, str):
        payload = json.loads(payload)
    if isinstance(payload, dict) and "components" in payload:
        return list(payload["components"])
    if isinstance(payload, list):
        return list(payload)
    raise ValueError("expected components list or {components: [...]}")
