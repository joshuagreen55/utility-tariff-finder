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
from typing import Any, Callable

from app.services.pricing.preaccept import PreAcceptResult, run_preaccept
from app.services.pricing.quote_verifier import verify_component_quote
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


def _raw_to_component(raw: dict[str, Any]) -> ComponentInput:
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
        source_page=raw.get("source_page") or raw.get("page"),
        source_quote=raw.get("source_quote") or raw.get("quote"),
    )


def _require_citations(comps: list[ComponentInput]) -> str | None:
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
    raw_a = extract_fn(document_text, m_a, dict(ctx))
    raw_b = extract_fn(document_text, m_b, dict(ctx))

    try:
        comps_a = [_raw_to_component(r) for r in (raw_a or [])]
        comps_b = [_raw_to_component(r) for r in (raw_b or [])]
    except (KeyError, TypeError, ValueError) as e:
        return ExtractionHold(reason="malformed_extract", detail=str(e))

    cite_a = _require_citations(comps_a)
    if cite_a:
        return ExtractionHold(
            reason="citation_incomplete", detail=f"model_a:{cite_a}",
            extract_a=comps_a, extract_b=comps_b,
        )
    cite_b = _require_citations(comps_b)
    if cite_b:
        return ExtractionHold(
            reason="citation_incomplete", detail=f"model_b:{cite_b}",
            extract_a=comps_a, extract_b=comps_b,
        )

    # Early grounding on each extract independently (hold on either failure).
    for label, comps in (("a", comps_a), ("b", comps_b)):
        for c in comps:
            if c.kind in {
                "season_calendar", "tou_schedule", "holiday_list",
                "tier_structure", "excluded_item", "event_day",
            }:
                continue
            cell = (c.cells or [{}])[0] if c.cells else {}
            # Prefer the cell whose amount is literally in the quote.
            for cand in c.cells or []:
                amt = str(cand.get("amount") or "")
                if amt and c.source_quote and amt in str(c.source_quote):
                    cell = cand
                    break
            vr = verify_component_quote(
                document_text,
                quote=c.source_quote,
                unit=c.unit,
                amount=cell.get("amount"),
                cell=cell,
                component_name=c.name or c.code,
                require_row_col=True,
            )
            if not vr.ok:
                return ExtractionHold(
                    reason="quote_verify_failed",
                    detail=f"model_{label}:{c.code}:{vr.reason}",
                    extract_a=comps_a, extract_b=comps_b,
                )

    plan = PlanInput(
        plan_key=str(plan_meta["plan_key"]),
        name=str(plan_meta.get("name") or plan_meta["plan_key"]),
        recipe_code=str(plan_meta["recipe_code"]),
        components=comps_a,  # agreed set; G2 checks a==b before accept
        code=plan_meta.get("code"),
        rate_type=plan_meta.get("rate_type"),
        utility_name=plan_meta.get("utility_name"),
    )

    pre = run_preaccept(
        plan,
        document_text=document_text,
        source_url=source_url or plan_meta.get("source_url"),
        official_hosts=official_hosts or [],
        extract_a=comps_a,
        extract_b=comps_b,
        inventory=inventory,
        dispositions=dispositions,
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
        plan=plan, preaccept=pre, model_a=m_a, model_b=m_b
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
