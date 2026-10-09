"""Robust amount parsing for messy model extracts (PR R29-1).

Handles ``$0.12``, ``1,234.56``, ``(1.297)`` negatives, en-dashes used as
minus, and blank / n/a / em-dash / ranges. A blank or unparseable amount is
a missing piece (caller marks ``not_found``), never a whole-plan crash.
"""
from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any

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


def parse_amount(value: Any) -> Decimal | None:
    """Return a Decimal, or None when the amount is blank / unparseable.

    Never raises ``ConversionSyntax``. Ranges are treated as missing
    (ambiguous). Accounting ``(1.297)`` → -1.297.
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
    s = s.replace(",", "").replace(" ", "")

    # Accounting negative.
    m = _PARENS_NEG.match(s)
    if m:
        inner = parse_amount(m.group(1))
        return None if inner is None else -inner

    # Ambiguous range → missing.
    if _RANGE.match(s):
        return None

    # "0.12perkwh" / "12.5¢" after currency strip.
    m = _TRAILING_TEXT.match(s)
    if m:
        s = m.group("num").replace(",", "")

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


def _quote_dollar_amounts(quote: str) -> list[Decimal]:
    """Dollar figures printed in the quote (``$0.10815``, ``$ 9.75``)."""
    out: list[Decimal] = []
    for m in re.finditer(r"\$\s*(\d+(?:\.\d+)?)", quote or ""):
        try:
            out.append(Decimal(m.group(1)))
        except InvalidOperation:
            continue
    return out


def _reconcile_energy_unit(
    amount: Decimal,
    unit: str,
    quote: str | None,
) -> tuple[Decimal, str]:
    """Fix $ vs ¢ mismatches using the quote as the ground truth.

    Common model error: ``unit=¢/kWh`` with amount ``0.10815`` while the
    quote shows ``$0.10815`` (or the reverse with ``9.8`` under ``$/kWh``).
    """
    from app.services.pricing.units import normalize_unit

    fam = normalize_unit(unit)
    q = quote or ""
    has_dollar = bool(re.search(r"\$\s*\d", q))
    has_cent = bool(re.search(r"¢|\bcents?\b", q, re.I))
    dollars = _quote_dollar_amounts(q)

    # Prefer an exact $ figure from the quote when the stored amount is a
    # 100× / 1× mis-scale of it (Xcel CIP: amount 0.1813 vs $0.001813).
    if fam in {"cents/kwh", "$/kwh"} and dollars:
        for d in dollars:
            if d == 0:
                continue
            if amount == d:
                return d, "$/kWh"
            if amount == d * Decimal("100") or amount == d / Decimal("100"):
                return d, "$/kWh"

    # Unit claims cents, amount looks like dollars, quote shows $X.XX.
    if fam == "cents/kwh" and amount < Decimal("2") and has_dollar:
        return amount, "$/kWh"
    # Unit claims dollars, amount looks like cents (≥ 2) — residential
    # $/kWh never exceeds ~$1; treat as printed ¢ and convert.
    if fam == "$/kwh" and amount >= Decimal("2"):
        return amount / Decimal("100"), "$/kWh"
    return amount, unit


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
        cells_in = row.get("cells")
        if not isinstance(cells_in, list):
            cells_in = []
        cells_out: list[dict[str, Any]] = []
        any_ok = False
        unit_out = unit
        for cell in cells_in:
            if not isinstance(cell, dict):
                continue
            cell = dict(cell)
            if "amount" in cell:
                parsed = parse_amount(cell.get("amount"))
                if parsed is None:
                    # Drop blank cell; do not keep unparseable text.
                    continue
                parsed, unit_out = _reconcile_energy_unit(parsed, unit_out, quote)
                cell["amount"] = format(parsed, "f")
                any_ok = True
            cells_out.append(cell)

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
