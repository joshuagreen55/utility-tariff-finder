"""Robust amount parsing for messy model extracts (PR R29-1).

Handles ``$0.12``, ``1,234.56``, ``(1.297)`` negatives, en-dashes used as
minus, and blank / n/a / em-dash / ranges. A blank or unparseable amount is
a missing piece (caller marks ``not_found``), never a whole-plan crash.
"""
from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any

from app.services.pricing.units import AMBIGUOUS_PER_KWH, normalize_unit

_PER_KWH_FAMILIES = frozenset({"cents/kwh", "$/kwh", "mills/kwh", AMBIGUOUS_PER_KWH})

# Truly missing — no number to price.
_BLANK_TOKENS = frozenset({
    "", "-", "—", "–", "−", "n/a", "na", "none", "null", "nil",
    ".", "..", "…", "tbd", "tba", "see tariff", "see above",
})

# Currency / thousand separators / accounting negatives.
_CURRENCY = re.compile(r"[$€£¢]")
_PARENS_NEG = re.compile(r"^\((.+)\)$")
_RANGE = re.compile(
    r"^("
    r"-?\d+(?:[.,]\d+)?|\.\d+"
    r")\s*[-–—−to]+\s*("
    r"-?\d+(?:[.,]\d+)?|\.\d+"
    r")$",
    re.I,
)
_PLAIN = re.compile(r"^-?\d+(?:\.\d+)?$")
_TRAILING_TEXT = re.compile(
    r"^(?P<num>-?\d+(?:[.,]\d+)?|\.\d+)\s*"
    r"(?:per\s+kwh|per\s+month|/kwh|/mo(?:nth)?|¢|cents?|\$)?\s*$",
    re.I,
)


def is_blank_amount(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, bool):
        return False
    s = str(value).strip().lower()
    return s in _BLANK_TOKENS


_THOUSANDS_GROUPS = re.compile(r"^-?\d{1,3}(?:,\d{3})+$")


def _normalize_separators(s: str, *, single_group_decimal: bool) -> str:
    """Resolve , / . separators ("1,234.56", "1.234,56", "9,8", "0,704").

    A lone ",ddd" ("6,704") is ambiguous: thousands unless the caller knows
    the value is a per-kWh price (``single_group_decimal``), where 6 704 is
    impossible and the French decimal comma is meant.
    """
    if "," not in s:
        return s
    if "." in s:
        if s.rfind(",") > s.rfind("."):
            return s.replace(".", "").replace(",", ".")  # 1.234,56
        return s.replace(",", "")  # 1,234.56
    if s.count(",") == 1 and not _THOUSANDS_GROUPS.match(s):
        return s.replace(",", ".")  # 9,8 / 0,0987
    if s.count(",") == 1 and (single_group_decimal or s.lstrip("-").startswith("0,")):
        return s.replace(",", ".")
    return s.replace(",", "")


def parse_amount(value: Any, *, per_kwh: bool = False) -> Decimal | None:
    """Return a Decimal, or None when the amount is blank / unparseable.

    Never raises ``ConversionSyntax``. Ranges are treated as missing
    (ambiguous). Accounting ``(1.297)`` → -1.297. Decimal commas are
    understood; pass ``per_kwh=True`` for energy prices so "6,704" means
    6.704.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        # Reject binary floats — model outputs must be strings.
        return None
    if value is None:
        return None

    s = str(value).strip()
    if not s or s.lower() in _BLANK_TOKENS:
        return None

    # Normalize dashes used as minus / separators.
    s = (
        s.replace("\u2212", "-")
        .replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\ufeff", "")
    )
    s = _CURRENCY.sub("", s).strip()
    s = s.replace(" ", "").replace("\u00a0", "").replace("\u202f", "")

    # Accounting negative.
    m = _PARENS_NEG.match(s)
    if m:
        inner = parse_amount(m.group(1), per_kwh=per_kwh)
        return None if inner is None else -inner

    # Ambiguous range → missing.
    if _RANGE.match(s):
        return None

    # "0.12perkwh" / "12.5¢" after currency strip.
    m = _TRAILING_TEXT.match(s)
    if m:
        s = m.group("num")
    s = _normalize_separators(s, single_group_decimal=per_kwh)

    if not _PLAIN.match(s):
        # Last chance: leading number.
        m = re.match(r"^(-?\d+(?:\.\d+)?)", s)
        if not m:
            return None
        s = m.group(1)

    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def normalize_amount_string(value: Any) -> str | None:
    """Canonical decimal string, or None when blank/unparseable."""
    d = parse_amount(value)
    if d is None:
        return None
    return format(d, "f")


# $ figures at or above this are fixed / demand charges, never a $/kWh price.
_MAX_DOLLARS_PER_KWH_FIGURE = Decimal("2")
_ONE = Decimal("1")
_HUNDRED = Decimal("100")
_HUNDREDTH = Decimal("0.01")


def _quote_dollar_amounts(quote: str) -> list[Decimal]:
    """Per-kWh-sized dollar figures in the quote (``$0.10815``).

    ``$14.50`` (a customer charge in the same sentence) is ignored.
    """
    out: list[Decimal] = []
    for m in re.finditer(r"\$\s*(\d+(?:\.\d+)?)", quote or ""):
        try:
            d = Decimal(m.group(1))
        except InvalidOperation:
            continue
        if 0 < d < _MAX_DOLLARS_PER_KWH_FIGURE:
            out.append(d)
    return out


def _reconcile_energy_unit(
    amounts: list[Decimal],
    unit: str,
    quote: str | None,
) -> tuple[Decimal, str]:
    """One (scale factor, unit) for a whole component, from its quote.

    Common model error: ``unit=¢/kWh`` with amount ``0.10815`` while the
    quote shows ``$0.10815`` (or the reverse with ``9.8`` under ``$/kWh``).
    Every cell gets the same factor; cells implying different factors are
    left untouched so grounding / plausibility gates hold them.
    """
    fam = normalize_unit(unit)
    q = quote or ""
    dollars = _quote_dollar_amounts(q)
    has_cent = bool(re.search(r"¢|\bcents?\b", q, re.I))

    if fam == AMBIGUOUS_PER_KWH:
        if has_cent and not dollars:
            return _ONE, "¢/kWh"
        if not dollars or has_cent:
            return _ONE, unit
        fam = "$/kwh"
    if fam not in {"cents/kwh", "$/kwh"} or not amounts:
        return _ONE, unit

    if dollars:
        factors: set[Decimal] = set()
        for a in amounts:
            for d in dollars:
                if a == d:
                    factors.add(_ONE)
                elif a == d * _HUNDRED:
                    factors.add(_HUNDREDTH)
                elif a == d / _HUNDRED:
                    factors.add(_HUNDRED)
        if len(factors) == 1:
            return factors.pop(), "$/kWh"
        if factors:
            return _ONE, unit

    # Unit claims cents, every amount looks like dollars, quote shows $.
    if (fam == "cents/kwh" and dollars and not has_cent
            and all(a < _MAX_DOLLARS_PER_KWH_FIGURE for a in amounts)):
        return _ONE, "$/kWh"
    # Unit claims dollars, every amount looks like printed ¢.
    if (fam == "$/kwh" and not dollars
            and all(a >= _MAX_DOLLARS_PER_KWH_FIGURE for a in amounts)):
        return _HUNDREDTH, "$/kWh"
    if fam == "$/kwh" and normalize_unit(unit) == AMBIGUOUS_PER_KWH:
        return _ONE, "$/kWh"
    return _ONE, unit


def sanitize_extract_amounts(
    raw_components: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Rewrite a raw extract so blank/messy amounts do not crash the plan.

    - Parseable amounts → canonical decimal strings.
    - ``applies`` components whose cells are all blank/unparseable →
      ``not_found`` with empty cells (one missing piece, not a plan hold).
    - Disposition strings with trailing commentary are clipped to the
      first token (``applies; …`` → ``applies``).
    - Energy $ vs ¢ mismatches reconciled from the quote.
    """
    out: list[dict[str, Any]] = []
    for raw in raw_components or []:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        disp = str(row.get("disposition") or "").strip().lower()
        if disp:
            # "applies; commentary" / "applies. Document prints…" — keep the
            # leading disposition token only (earliest punctuation wins).
            m = re.match(
                r"^(applies|not_found|not_applicable|optional|"
                r"location_fee_or_tax|event_day)\b",
                disp,
            )
            if m:
                disp = m.group(1)
            else:
                cut = len(disp)
                for sep in (";", ".", ",", " —", " -", ":"):
                    i = disp.find(sep)
                    if 0 <= i < cut:
                        cut = i
                disp = disp[:cut].strip()
            row["disposition"] = disp

        quote = str(row.get("source_quote") or row.get("quote") or "")
        unit = str(row.get("unit") or "")
        per_kwh = normalize_unit(unit) in _PER_KWH_FAMILIES
        cells_in = row.get("cells")
        if not isinstance(cells_in, list):
            cells_in = []
        cells_out: list[dict[str, Any]] = []
        parsed_amounts: list[Decimal] = []
        for cell in cells_in:
            if not isinstance(cell, dict):
                continue
            cell = dict(cell)
            if "amount" in cell:
                parsed = parse_amount(cell.get("amount"), per_kwh=per_kwh)
                if parsed is None:
                    # Drop blank cell; do not keep unparseable text.
                    continue
                cell["amount"] = parsed
                parsed_amounts.append(parsed)
            cells_out.append(cell)
        any_ok = bool(parsed_amounts)

        factor, unit_out = _reconcile_energy_unit(parsed_amounts, unit, quote)
        for cell in cells_out:
            if "amount" in cell:
                cell["amount"] = format(cell["amount"] * factor, "f")

        if unit_out != unit:
            row["unit"] = unit_out
        if disp == "applies" and not any_ok:
            row["disposition"] = "not_found"
            row["cells"] = []
        else:
            row["cells"] = cells_out
        out.append(row)
    return out


__all__ = [
    "is_blank_amount",
    "normalize_amount_string",
    "parse_amount",
    "sanitize_extract_amounts",
]
