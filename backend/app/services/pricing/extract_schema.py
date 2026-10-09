"""Strict schema checks on dual-blind extraction payloads (PR R27-4).

R27 format errors (~8 plans): non-numeric amounts, duplicate component
codes, and missing applies/not dispositions. These checks run *before*
quote grounding and G0–G6 so bad shapes never reach the calculator.

Rules:
1. Every cell ``amount`` is a decimal *string* (no float, no prose).
2. Component ``code`` values are unique within one extract.
3. Component ``name`` values are unique within one extract (case-insensitive).
4. Every priced component carries an explicit disposition
   (applies / not_applicable / optional / location_fee_or_tax / event_day).
"""
from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any

from app.services.pricing.rider_census import VALID_DISPOSITIONS

# Kinds that do not need a numeric amount / disposition for compile.
_META_KINDS = frozenset({
    "season_calendar", "tou_schedule", "holiday_list",
    "tier_structure", "excluded_item", "event_day",
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
        # Integers are fine as rates sometimes (e.g. 13); coerce via str later.
        return True, "ok"
    if isinstance(value, Decimal):
        return True, "ok"
    s = str(value).strip()
    if not s:
        return False, "amount_blank"
    # Strip thin wrappers models sometimes add.
    s = s.replace(",", "")  # thousands separators — still must be pure digits after
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
    # Normalize -0 / trailing zeros lightly via Decimal.
    return format(Decimal(s), "f")


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

        if not is_meta:
            priced += 1
            if not cells:
                return f"missing_cells:{code}"
            for j, cell in enumerate(cells):
                if not isinstance(cell, dict):
                    return f"cell_not_object:{code}:{j}"
                ok, reason = _is_numeric_amount(cell.get("amount"))
                if not ok:
                    return f"{reason}:{code}:cell{j}"

        if require_disposition and not is_meta:
            disp = str(raw.get("disposition") or "").strip().lower()
            if not disp:
                return f"missing_disposition:{code}"
            if disp not in VALID_DISPOSITIONS:
                return f"invalid_disposition:{code}:{disp}"
            if disp == "applies":
                applies += 1

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

    return None


def applying_raw_components(
    raw_components: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Filter to disposition=applies (default applies when absent for meta)."""
    out = []
    for raw in raw_components:
        kind = str(raw.get("kind") or "")
        if kind in _META_KINDS:
            continue
        disp = str(raw.get("disposition") or "applies").strip().lower()
        if disp == "applies":
            out.append(raw)
    return out


# JSON-schema-ish tool shape for the live harness / extractors (documentation
# + optional runtime use). Amounts are strings; disposition is required.
EXTRACTION_TOOL_SCHEMA: dict[str, Any] = {
    "name": "record_components",
    "description": (
        "Record tariff pricing components copied from the document. "
        "Amounts must be decimal strings. Codes and names must be unique. "
        "Every priced component needs an explicit disposition."
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
                        "code", "kind", "unit", "cells",
                        "source_quote", "disposition",
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
    "normalize_amount_string",
    "validate_extract_schema",
]
