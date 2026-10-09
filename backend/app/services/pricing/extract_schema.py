"""Strict schema checks on dual-blind extraction payloads (PR R27-4 / R28-1).

Rules:
1. Every *priced* (disposition=applies) cell ``amount`` is a decimal *string*.
2. Component ``code`` values are unique within one extract.
3. Component ``name`` values are unique within one extract (case-insensitive).
4. Every non-meta component carries an explicit disposition.
5. Components with no value use ``not_found`` / ``not_applicable`` (empty
   cells allowed) — that must NOT reject the whole extract (R28). A plan
   is accepted when every *applies* component it needs is found and verified.
"""
from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any

from app.services.pricing.rider_census import (
    UNPRICED_DISPOSITIONS,
    VALID_DISPOSITIONS,
)

# Kinds that do not need a numeric amount / disposition for the energy
# compiler. ``tier_structure`` holds kWh breakpoints (R28). Fixed monthly
# charges use real amounts + $/month units but are skipped by recipes.
_META_KINDS = frozenset({
    "season_calendar", "tou_schedule", "holiday_list",
    "tier_structure", "excluded_item", "event_day",
})

# Dispositions that may omit cells (no amount found / does not apply).
_EMPTY_CELLS_OK = frozenset({"not_found", "not_applicable"}) | frozenset({
    # optional / event_day / location_fee may also lack everyday amounts
    "optional", "event_day", "location_fee_or_tax",
})

_AMOUNT_OK = re.compile(r"^-?\d+(\.\d+)?$")


def _is_numeric_amount(value: Any) -> tuple[bool, str]:
    """Return (ok, reason). Rejects float, bool, blank, and non-decimal text."""
    if isinstance(value, bool):
        return False, "amount_is_bool"
    if isinstance(value, float):
        return False, "amount_is_float"
    if value is None:
        return False, "amount_missing"
    if isinstance(value, int):
        return True, "ok"
    if isinstance(value, Decimal):
        return True, "ok"
    s = str(value).strip()
    if not s:
        return False, "amount_blank"
    s = s.replace(",", "")
    if not _AMOUNT_OK.match(s):
        return False, f"amount_not_numeric:{s[:40]}"
    try:
        Decimal(s)
    except (InvalidOperation, ValueError):
        return False, f"amount_not_decimal:{s[:40]}"
    return True, "ok"


def normalize_amount_string(value: Any) -> str:
    """Canonical decimal string for a validated amount."""
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    s = str(value).strip().replace(",", "")
    return format(Decimal(s), "f")


def disposition_of(raw: dict[str, Any]) -> str:
    return str(raw.get("disposition") or "").strip().lower()


def is_priced_component(raw: dict[str, Any]) -> bool:
    """True when the component must carry verified cells for acceptance."""
    kind = str(raw.get("kind") or "").strip()
    if kind in _META_KINDS:
        return False
    return disposition_of(raw) == "applies"


def validate_extract_schema(
    raw_components: list[dict[str, Any]] | None,
    *,
    require_disposition: bool = True,
) -> str | None:
    """Return a reason string on failure, else None.

    Checked per extract (model A and model B independently).
    """
    comps = list(raw_components or [])
    if not comps:
        return "empty_extract"

    codes: list[str] = []
    names: list[str] = []
    priced = 0
    applies = 0

    for i, raw in enumerate(comps):
        if not isinstance(raw, dict):
            return f"component_not_object:{i}"
        code = str(raw.get("code") or "").strip()
        if not code:
            return f"missing_code:{i}"
        codes.append(code.lower())

        name = str(raw.get("name") or code).strip()
        names.append(name.lower())

        kind = str(raw.get("kind") or "").strip()
        if not kind:
            return f"missing_kind:{code}"

        is_meta = kind in _META_KINDS
        cells = raw.get("cells")
        if cells is None:
            cells = []
        if not isinstance(cells, list):
            return f"cells_not_list:{code}"

        disp = disposition_of(raw) if not is_meta else ""

        if not is_meta:
            if require_disposition:
                if not disp:
                    return f"missing_disposition:{code}"
                if disp not in VALID_DISPOSITIONS:
                    return f"invalid_disposition:{code}:{disp}"
                if disp == "applies":
                    applies += 1
                    priced += 1

            # Empty cells: OK for not_found / not_applicable / optional / …
            # Applies must still carry at least one numeric cell.
            if disp == "applies" or (not require_disposition and not cells):
                if not cells:
                    return f"missing_cells:{code}"
                for j, cell in enumerate(cells):
                    if not isinstance(cell, dict):
                        return f"cell_not_object:{code}:{j}"
                    ok, reason = _is_numeric_amount(cell.get("amount"))
                    if not ok:
                        return f"{reason}:{code}:cell{j}"
            elif cells:
                # Unpriced disposition but cells present — amounts must still
                # be numeric if given (audit trail).
                for j, cell in enumerate(cells):
                    if not isinstance(cell, dict):
                        return f"cell_not_object:{code}:{j}"
                    if "amount" not in cell:
                        continue
                    ok, reason = _is_numeric_amount(cell.get("amount"))
                    if not ok:
                        return f"{reason}:{code}:cell{j}"
            elif disp and disp not in _EMPTY_CELLS_OK and disp not in UNPRICED_DISPOSITIONS:
                return f"missing_cells:{code}"

    # Uniqueness
    seen_c: set[str] = set()
    for c in codes:
        if c in seen_c:
            return f"duplicate_code:{c}"
        seen_c.add(c)

    seen_n: set[str] = set()
    for n in names:
        if n in seen_n:
            return f"duplicate_name:{n}"
        seen_n.add(n)

    if require_disposition and priced > 0 and applies == 0:
        return "no_applies_disposition"
    # At least one applies is required when any non-meta row exists.
    if require_disposition and applies == 0:
        non_meta = sum(
            1 for r in comps
            if str(r.get("kind") or "") not in _META_KINDS
        )
        if non_meta > 0:
            return "no_applies_disposition"

    return None


def applying_raw_components(
    raw_components: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Filter to disposition=applies (meta kinds never compile)."""
    out = []
    for raw in raw_components:
        kind = str(raw.get("kind") or "")
        if kind in _META_KINDS:
            continue
        disp = disposition_of(raw)
        # Blank disposition treated as applies for legacy callers that
        # disable require_disposition; live path always sets it explicitly.
        if disp in {"", "applies"}:
            out.append(raw)
    return out


# JSON-schema-ish tool shape for the live harness / extractors.
EXTRACTION_TOOL_SCHEMA: dict[str, Any] = {
    "name": "record_components",
    "description": (
        "Record tariff pricing components copied from the document. "
        "Amounts must be decimal strings. Codes and names must be unique. "
        "Every component needs an explicit disposition. If a listed "
        "component has no value in the documents, set disposition to "
        "not_found (or not_applicable) with empty cells — do not omit it "
        "and do not invent an amount."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "components": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "code": {"type": "string"},
                        "kind": {"type": "string"},
                        "unit": {"type": "string"},
                        "name": {"type": "string"},
                        "disposition": {
                            "type": "string",
                            "enum": sorted(VALID_DISPOSITIONS),
                        },
                        "source_page": {"type": "string"},
                        "source_quote": {"type": "string"},
                        "charge_category": {"type": "string"},
                        "percent_base_codes": {
                            "type": "array", "items": {"type": "string"},
                        },
                        "multiplier_target_codes": {
                            "type": "array", "items": {"type": "string"},
                        },
                        "loss_sensitive": {"type": "boolean"},
                        "cells": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "season": {"type": "string"},
                                    "period": {"type": "string"},
                                    "day_type": {"type": "string"},
                                    "tier": {"type": "string"},
                                    "amount": {
                                        "type": "string",
                                        "description": "Decimal string only, e.g. '0.098'",
                                    },
                                },
                                "required": ["amount"],
                            },
                        },
                    },
                    "required": [
                        "code", "kind", "unit",
                        "disposition",
                    ],
                },
            }
        },
        "required": ["components"],
    },
}


__all__ = [
    "EXTRACTION_TOOL_SCHEMA",
    "applying_raw_components",
    "disposition_of",
    "is_priced_component",
    "normalize_amount_string",
    "validate_extract_schema",
]
