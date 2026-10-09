"""Unit normalization table for quote grounding + fixed-charge metadata (R29-2).

Maps messy model / tariff wording onto a small set of canonical families.
Daily and yearly fixed charges convert to monthly equivalents when comparing
or storing fixed metadata (energy $/kWh families are never converted).
"""
from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

# Canonical family → regex that grounds the unit in document text / headers.
UNIT_PATTERNS: dict[str, re.Pattern[str]] = {
    "$/kwh": re.compile(
        r"\$\s*/\s*kwh|\$\/kwh|dollars?\s+per\s+kwh|"
        r"\$\s*\d[\d.,]*\s*per\s*kwh|\$\s*\d[\d.,]*\s*/\s*kwh|"
        # "Energy Charge per kWh … $0.10815" (unit words before the $).
        # Require a $ figure nearby — bare "charge per kWh" alone is too
        # loose (PEC TOU quoted $ rates under a ¢ unit and would accept-wrong).
        r"per\s*kwh[^¢\n]{0,80}\$\s*\d|"
        r"\$\s*\d[\d.,]*[^¢\n]{0,80}per\s*kwh",
        re.I,
    ),
    "cents/kwh": re.compile(
        r"¢\s*/\s*kwh|¢\s+per\s+kwh|c/\s*kwh|cents?\s*/\s*kwh|cents?\s+per\s+kwh|"
        r"¢/kwh|¢\s*per\s*kilowatt|\bcents?\b|"
        r"\d[\d.,]*\s*¢\s*per\s*kwh|\d[\d.,]*\s*cents?\s*per\s*kwh",
        re.I,
    ),
    "percent": re.compile(r"%|percent(?:age)?\s+of|per\s*cent", re.I),
    "mills/kwh": re.compile(r"mills?\s*/\s*kwh|mills?\s+per\s+kwh", re.I),
    "dimensionless": re.compile(r"factor|multiplier|×|x\s+\d", re.I),
    "$/month": re.compile(
        r"\$\s*/\s*mo(?:nth)?|\$\s*per\s*mo(?:nth)?|per\s*month|/mo\b|"
        r"monthly\s+charge|customer\s+charge|basic\s+charge|service\s+charge|"
        r"per\s+billing\s+period|billing\s+period|meter[\s-]?month|"
        r"\$\s*/\s*meter[\s-]?mo|dollars?\s+per\s+month|usd\s*/\s*mo",
        re.I,
    ),
    "$/day": re.compile(
        r"\$\s*/\s*day|\$\s*per\s*day|per\s*day|daily\s+charge|¢\s*/\s*day|"
        r"dollars?\s+per\s+day",
        re.I,
    ),
    "$/year": re.compile(
        r"\$\s*/\s*yr|\$\s*/\s*year|\$\s*per\s*year|per\s*year|annual(?:ly)?|"
        r"dollars?\s+per\s+year|/yr\b",
        re.I,
    ),
    "$/kw/month": re.compile(
        r"\$\s*/\s*kw\s*/\s*mo(?:nth)?|\$\s*per\s*kw[\s-]*mo|"
        r"per\s*kw[\s-]*month|\$/kw/mo|demand\s+charge",
        re.I,
    ),
    "kwh": re.compile(
        r"\bkwh\b|kilowatt[\s-]?hours?|kwh/day|kwh\s+per\s+day|"
        r"first\s+\d+|block\s+size|tier\s+threshold",
        re.I,
    ),
}

# A per-kWh unit with no currency marker; resolved from the quote or held.
AMBIGUOUS_PER_KWH = "per_kwh"

# Aliases → canonical family.
_ALIAS: dict[str, str] = {}
for _canon in UNIT_PATTERNS:
    _ALIAS[_canon] = _canon
for _a, _c in [
    ("$/kwh", "$/kwh"), ("usd/kwh", "$/kwh"), ("cad/kwh", "$/kwh"),
    ("$/kw h", "$/kwh"), ("dollar/kwh", "$/kwh"), ("dollars/kwh", "$/kwh"),
    ("¢/kwh", "cents/kwh"), ("c/kwh", "cents/kwh"), ("cents/kwh", "cents/kwh"),
    ("cent/kwh", "cents/kwh"), ("¢", "cents/kwh"), ("cents", "cents/kwh"),
    ("percent", "percent"), ("%", "percent"), ("pct", "percent"),
    ("mills/kwh", "mills/kwh"), ("mill/kwh", "mills/kwh"),
    ("dimensionless", "dimensionless"), ("factor", "dimensionless"), ("x", "dimensionless"),
    ("$/month", "$/month"), ("$/mo", "$/month"), ("usd/month", "$/month"),
    ("cad/month", "$/month"), ("$/mo.", "$/month"), ("per month", "$/month"),
    ("per_month", "$/month"), ("usd per month", "$/month"),
    ("dollars per month", "$/month"), ("$ per month", "$/month"),
    ("per billing period", "$/month"), ("$/billing period", "$/month"),
    ("meter-month", "$/month"), ("meter month", "$/month"), ("$/meter-month", "$/month"),
    ("$/day", "$/day"), ("usd/day", "$/day"), ("per day", "$/day"), ("per_day", "$/day"),
    ("¢/day", "$/day"), ("cents/day", "$/day"), ("daily", "$/day"),
    ("$/year", "$/year"), ("$/yr", "$/year"), ("per year", "$/year"), ("per_year", "$/year"),
    ("annual", "$/year"), ("annually", "$/year"),
    ("$/kw/month", "$/kw/month"), ("$/kw/mo", "$/kw/month"), ("per kw month", "$/kw/month"),
    ("$/kw-mo", "$/kw/month"),
    ("kwh", "kwh"), ("kwh/day", "kwh"),
]:
    _ALIAS[_a.replace(" ", "")] = _c


def normalize_unit(unit: str | None) -> str:
    """Map a model/tariff unit string onto a canonical family key."""
    if not unit:
        return ""
    raw = unit.strip().lower()
    compact = re.sub(r"\s+", " ", raw)
    nospace = compact.replace(" ", "")
    if nospace in _ALIAS:
        return _ALIAS[nospace]
    if compact in _ALIAS:
        return _ALIAS[compact]
    # Soft contains checks.
    if "billing period" in compact or "meter" in compact and "month" in compact:
        return "$/month"
    if "per month" in compact or "/mo" in nospace or "monthly" in compact:
        return "$/month"
    if "per day" in compact or "/day" in nospace or compact == "daily":
        return "$/day"
    if "per year" in compact or "/yr" in nospace or "annual" in compact:
        return "$/year"
    if "kw" in nospace and "mo" in nospace and "kwh" not in nospace:
        return "$/kw/month"
    if "cent" in compact or "¢" in nospace:
        return "cents/kwh"
    if "kwh" in nospace:
        if "mill" in nospace:
            return "mills/kwh"
        if any(t in nospace for t in ("$", "dollar", "usd", "cad")):
            return "$/kwh"
        if nospace.startswith(("c/", "c per")):
            return "cents/kwh"
        # "per kWh" with no currency could be ¢ or $ — never guess (100×).
        return AMBIGUOUS_PER_KWH
    return nospace


def to_monthly(amount: Decimal, unit_family: str) -> Decimal | None:
    """Convert a fixed-charge amount into $/month. None if not a fixed family."""
    if unit_family == "$/month":
        return amount
    if unit_family == "$/day":
        # Average month length used by many tariff books.
        return amount * Decimal("30.4167")
    if unit_family == "$/year":
        return amount / Decimal("12")
    return None


def unit_pattern(unit_or_family: str | None) -> re.Pattern[str] | None:
    fam = normalize_unit(unit_or_family)
    return UNIT_PATTERNS.get(fam)


__all__ = [
    "AMBIGUOUS_PER_KWH",
    "UNIT_PATTERNS",
    "normalize_unit",
    "to_monthly",
    "unit_pattern",
]
