"""Verbatim quote grounding for priced component values (G3).

The model chooses the row: its quote and its season / period / day-type /
tier labels say which printed figure a cell is, and the two blind
extractions must agree on those labels (G2). The verifier only checks what
the document text can prove:

1. The quote appears in the retained document text.
2. A figure printed in the quote equals the stored amount. A label-only
   quote may point at the figure on the next lines of a reflowed row.
   Printed signs ("-", parentheses) count; ¢ ↔ $ conversion is allowed only
   when the document prints that scale at the figure.
3. The unit is printed: the currency scale nearest the figure ($, ¢, %,
   mills) matches the stored unit, and the stored unit's denominator (kWh,
   kW, day, month, year) appears on the figure's row or the headings above.
"""
from __future__ import annotations

import re
from contextvars import ContextVar
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

_HEADER_LINE_LOOKBACK = 12
_TABLE_LINES = 25  # other rows of the table a multi-cell quote cites

# One printed figure: 9.8, -1.297, 1,234.56 (grouped), 9,8 / 0,0982 (decimal
# comma), or an integer. "6,605" is grouped in an English document and a
# decimal in a French one; ``_figure_value`` decides by document locale.
_NUMBER_RE = re.compile(
    r"(?<![\d.,])-?(?:\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+[.,]\d+|\d+)(?![\d.]|,\d)"
)
_GROUPED_RE = re.compile(r"-?\d{1,3}(?:,\d{3})+(?:\.\d+)?")
_COMMA_ONLY_DECIMAL_RE = re.compile(r"(?<![\d.,])\d+,(?:\d{1,2}|\d{4,})(?![\d.,])")
_COMMA_BEFORE_UNIT_RE = re.compile(r"(?<![\d.,])\d+,\d+\s*(?:¢|\$|cents?\b)", re.I)
_DOT_DECIMAL_RE = re.compile(r"(?<![\d.,])\d+\.\d+(?![\d.,])")
# PDF text extraction sometimes splits a decimal: "$0. 14053".
_SPLIT_DECIMAL_RE = re.compile(r"(\d)\. (\d+)")

_COMMA_DECIMAL: ContextVar[bool] = ContextVar("quote_comma_decimal", default=False)

_SCALE_TOKENS: dict[str, re.Pattern[str]] = {
    "$": re.compile(r"\$|\bdollars?\b", re.I),
    "¢": re.compile(r"¢|\bcents?\b|\bc/kwh\b", re.I),
    "%": re.compile(r"%|\bper\s?cent\b", re.I),
    "mills": re.compile(r"\bmills?\b", re.I),
}
_DENOM_TOKENS: dict[str, re.Pattern[str]] = {
    "kwh": re.compile(r"kwh|kw\s*h\b|kilowatt[\s-]?hours?|kilowattheures?", re.I),
    "kw": re.compile(r"\bkw\b(?!\s*h)|kilowatts?\b(?![\s-]?h)|\bdemand\b", re.I),
    "month": re.compile(r"month|\bmo\b|\bmois\b|billing\s+(?:period|cycle)|30\s+days", re.I),
    "day": re.compile(r"\bdays?\b|daily|\bjours?\b", re.I),
    "year": re.compile(r"\byears?\b|annual|\byr\b", re.I),
}


@dataclass(frozen=True)
class UnitSpec:
    """A stored unit as currency scale + denominator."""

    scale: str | None  # "$" | "¢" | "%" | "mills" | "factor" | None
    denom: str | None  # "kwh" | "kw" | "day" | "month" | "year" | None
    ambiguous: bool = False  # "per kWh" with no currency (¢ or $?)


def parse_unit(unit: str | None) -> UnitSpec | None:
    """Read any model unit spelling; None when it names neither scale nor denominator."""
    u = re.sub(r"[_\s]+", " ", (unit or "").lower()).strip()
    if not u:
        return None
    if re.search(r"factor|multiplier|dimensionless|ratio", u) or u in {"x", "×"}:
        return UnitSpec("factor", None)
    if re.search(r"%|percent|\bpct\b", u):
        return UnitSpec("%", None)
    if "mill" in u:
        scale: str | None = "mills"
    elif re.search(r"¢|cent|^c ?(?:/|per)", u):
        scale = "¢"
    elif re.search(r"\$|dollar|\busd\b|\bcad\b", u):
        scale = "$"
    else:
        scale = None
    if re.search(r"kwh|kilowatt[\s-]?hour", u):
        denom: str | None = "kwh"
    elif re.search(r"\bkw\b|kilowatt", u):
        denom = "kw"
    elif re.search(r"30 ?days?|month|\bmo\b|billing", u):
        denom = "month"
    elif re.search(r"\bdays?\b|daily", u):
        denom = "day"
    elif re.search(r"year|annual|\byr\b", u):
        denom = "year"
    else:
        denom = None
    if scale is None and denom is None:
        return None
    if scale is None and denom == "kwh":
        return UnitSpec(None, "kwh", ambiguous=bool(re.search(r"(?:per|/) ?kwh", u)))
    if scale is None:
        scale = "$"  # fixed / demand charges with no currency word are dollars
    return UnitSpec(scale, denom)


def is_decimal_comma_document(text: str) -> bool:
    """True when the document writes decimals with a comma (``9,8 ¢``, ``0,0982 $``).

    Evidence is figures that can only be comma decimals (one, two or 4+
    digits after the comma) or a comma figure directly before ¢ / $ (French
    order), counted against dot-decimal figures.
    """
    if not text:
        return False
    comma = len(_COMMA_ONLY_DECIMAL_RE.findall(text)) + len(_COMMA_BEFORE_UNIT_RE.findall(text))
    return comma > len(_DOT_DECIMAL_RE.findall(text))


def _figure_value(tok: str) -> Decimal | None:
    """Numeric value of a ``_NUMBER_RE`` token under the current document locale."""
    try:
        if "," not in tok:
            return Decimal(tok)
        if _GROUPED_RE.fullmatch(tok):
            if _COMMA_DECIMAL.get() and tok.count(",") == 1 and "." not in tok:
                return Decimal(tok.replace(",", "."))
            return Decimal(tok.replace(",", ""))
        return Decimal(tok.replace(",", "."))
    except InvalidOperation:
        return None


def _is_decimal_figure(tok: str) -> bool:
    """A rate-like figure with a fractional part as printed (not a bare integer)."""
    if "." in tok:
        return True
    if "," not in tok:
        return False
    return not _GROUPED_RE.fullmatch(tok) or (
        _COMMA_DECIMAL.get() and tok.count(",") == 1
    )


# Normalize common PDF / HTML artifacts before verbatim search.
_DASHES = re.compile(r"[\u2010-\u2015\u2212\ufe58\ufe63\uff0d]")
_SPACES = re.compile(r"[\u00a0\u2000-\u200b\u202f\u205f\u3000]+")


def _normalize_text(text: str) -> str:
    t = _DASHES.sub("-", text)
    t = _SPACES.sub(" ", t)
    return t


@dataclass(frozen=True)
class QuoteVerifyResult:
    ok: bool
    reason: str
    quote_index: int | None = None

    @property
    def passed(self) -> bool:
        return self.ok


def _line_spans(document_text: str) -> list[tuple[int, int, str]]:
    spans: list[tuple[int, int, str]] = []
    start = 0
    for line in document_text.splitlines(keepends=True):
        end = start + len(line)
        # Same-length repair keeps column offsets valid.
        text = _SPLIT_DECIMAL_RE.sub(
            lambda m: f"{m[1]}.{m[2]} ", line.rstrip("\n\r"),
        )
        spans.append((start, end, text))
        start = end
    if not spans and document_text:
        spans.append((0, len(document_text), document_text))
    return spans


def _line_index_at(spans: list[tuple[int, int, str]], offset: int) -> int:
    lo, hi = 0, len(spans) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if spans[mid][0] <= offset:
            lo = mid
        else:
            hi = mid - 1
    return max(0, lo)


def _all_indices(hay: str, needle: str) -> list[int]:
    out: list[int] = []
    if not needle:
        return out
    i = hay.find(needle)
    while i >= 0:
        out.append(i)
        i = hay.find(needle, i + 1)
    return out


def _collapse_with_map(text: str) -> tuple[str, list[int]]:
    """Collapse whitespace runs to one space, remembering original offsets."""
    out: list[str] = []
    pos: list[int] = []
    in_ws = False
    for i, ch in enumerate(text):
        if ch.isspace():
            if not in_ws:
                out.append(" ")
                pos.append(i)
            in_ws = True
        else:
            out.append(ch)
            pos.append(i)
            in_ws = False
    return "".join(out), pos


def _find_quote_all(document_text: str, quote: str) -> tuple[str, list[tuple[int, int]]]:
    """Every (start, end) span of the quote in a line-preserving document.

    Tries verbatim, then dash/space-normalized, then whitespace-collapsed
    (mapped back to real line offsets). PDF reflows often break a model
    quote across lines or reorder a header around the number; as a last
    resort an amount-anchored window that still contains most quote tokens
    is used.
    """
    q = str(quote)
    hits = _all_indices(document_text, q)
    if hits:
        return document_text, [(i, i + len(q)) for i in hits]

    norm_doc = _normalize_text(document_text)
    norm_q = _normalize_text(q)
    hits = _all_indices(norm_doc, norm_q)
    if hits:
        return norm_doc, [(i, i + len(norm_q)) for i in hits]

    collapsed_doc, pos = _collapse_with_map(norm_doc)
    collapsed_q = re.sub(r"\s+", " ", norm_q).strip()
    hits = _all_indices(collapsed_doc, collapsed_q)
    if hits:
        return norm_doc, [
            (pos[i], pos[i + len(collapsed_q) - 1] + 1) for i in hits
        ]

    # Amount-anchored fuzzy: locate a *decimal* rate figure from the quote
    # (skip bare integers like clock hours), require ≥60% of significant quote
    # tokens within ±180 chars (PDF header reflow).
    nums = [n for n in _NUMBER_RE.findall(collapsed_q) if _is_decimal_figure(n)]
    tokens = [
        t for t in re.findall(r"[A-Za-z0-9.]+", collapsed_q.lower())
        if len(t) >= 3 and t not in {"the", "and", "per", "for", "with"}
    ]
    spans: list[tuple[int, int]] = []
    if nums and tokens:
        need = max(2, int(round(len(tokens) * 0.6)))
        for num in nums:
            for j in _all_indices(collapsed_doc, num):
                lo = max(0, j - 180)
                hi = min(len(collapsed_doc), j + len(num) + 180)
                window = collapsed_doc[lo:hi].lower()
                if sum(1 for t in tokens if t in window) >= need:
                    spans.append((pos[lo], pos[hi - 1] + 1))
    return norm_doc, list(dict.fromkeys(spans))


@dataclass(frozen=True)
class _Figure:
    line: int
    start: int
    end: int
    value: Decimal
    signed: bool  # the document prints the sign ("-1.41", "(1.297)")


def _figures_on_line(spans: list[tuple[int, int, str]], li: int) -> list[_Figure]:
    text = spans[li][2]
    out: list[_Figure] = []
    for m in _NUMBER_RE.finditer(text):
        v = _figure_value(m.group(0))
        if v is None:
            continue
        before = text[max(0, m.start() - 4):m.start()]
        after = text[m.end():m.end() + 10]
        signed = v < 0
        if not signed:
            lead = before.replace("$", "").rstrip()
            if lead.endswith(("-", "−", "–")):
                signed, v = True, -v
            elif lead.endswith("(") and re.match(r"\s*(?:¢|\$|%|cents?)?\s*\)", after, re.I):
                signed, v = True, -v
        out.append(_Figure(li, m.start(), m.end(), v, signed))
    return out


def _adjacent_scale(text: str, start: int, end: int) -> str | None:
    before = text[max(0, start - 4):start]
    after = text[end:end + 8]
    if re.search(r"\$\s*\(?\s*[-−–]?\s*$", before) or re.match(r"\s*\$", after):
        return "$"
    if re.match(r"\s*(?:¢|cents?\b)", after, re.I):
        return "¢"
    if re.match(r"\s*%", after):
        return "%"
    if re.match(r"\s*mills?\b", after, re.I):
        return "mills"
    return None


def _lines_outward(li: int, n_lines: int, lookback: int = _HEADER_LINE_LOOKBACK) -> list[int]:
    """The figure's line, the next line, then the lines above (headings)."""
    order = [li]
    if li + 1 < n_lines:
        order.append(li + 1)
    order.extend(j for j in range(li - 1, max(-1, li - 1 - lookback), -1))
    return order


def _printed_scale(spans: list[tuple[int, int, str]], fig: _Figure) -> str | None:
    """The currency scale that governs a printed figure.

    A marker touching the figure wins; otherwise the nearest scale token on
    the figure's row, the next line, or the headings above.
    """
    text = spans[fig.line][2]
    adj = _adjacent_scale(text, fig.start, fig.end)
    if adj:
        return adj
    for j in _lines_outward(fig.line, len(spans)):
        line = spans[j][2]
        hits = [
            (abs(m.start() - fig.start), scale)
            for scale, pat in _SCALE_TOKENS.items()
            for m in pat.finditer(line)
            if not _marks_another_figure(line, m.start(), m.end(), fig if j == fig.line else None)
        ]
        if hits:
            return min(hits)[1]
    return None


def _marks_another_figure(line: str, start: int, end: int, own: _Figure | None) -> bool:
    """A "$" / "¢" touching some other figure is that figure's unit, not a header."""
    for m in _NUMBER_RE.finditer(line):
        if own is not None and m.start() == own.start:
            continue
        if 0 <= m.start() - end <= 2 or 0 <= start - m.end() <= 1:
            return True
    return False


def _denominator_printed(
    spans: list[tuple[int, int, str]], li: int, denom: str, quote: str,
) -> bool:
    pat = _DENOM_TOKENS[denom]
    if pat.search(quote):
        return True
    # A table's unit row can sit well above its last data row.
    return any(
        pat.search(spans[j][2])
        for j in _lines_outward(li, len(spans), lookback=_TABLE_LINES)
    )


def _in_stored_scale(value: Decimal, printed: str | None, spec: UnitSpec) -> Decimal | None:
    """The printed figure expressed in the stored unit's scale, or None if it cannot be."""
    stored = spec.scale
    if stored in (None, "factor"):
        return value
    if printed == stored:
        return value
    if stored == "$" and printed == "¢":
        return value / Decimal("100")
    if stored == "¢" and printed == "$":
        return value * Decimal("100")
    return None


def _related(value: Decimal, want: Decimal) -> bool:
    """Same magnitude in some scale — used only to name the failure."""
    a, w = abs(value), abs(want)
    return a == w or a == w * 100 or a * 100 == w


def _ground_figures(
    spans: list[tuple[int, int, str]],
    figures: list[_Figure],
    *,
    want: Decimal,
    spec: UnitSpec | None,
    require_unit: bool,
    quote: str,
) -> str | None:
    """'ok' when a figure grounds amount + unit, else a failure reason, None if unrelated."""
    reason: str | None = None
    for fig in figures:
        if not _related(fig.value, want):
            continue
        if not require_unit or spec is None:
            if fig.value == want or (not fig.signed and fig.value == -want):
                return "ok"
            reason = reason or "amount_not_in_quote"
            continue
        printed = _printed_scale(spans, fig)
        got = _in_stored_scale(fig.value, printed, spec)
        if got is None:
            reason = "unit_not_in_context"
            continue
        if got != want and not (not fig.signed and got == -want):
            reason = reason or ("unit_not_in_context" if abs(got) != abs(want) else "sign_mismatch")
            continue
        if spec.denom and not _denominator_printed(spans, fig.line, spec.denom, quote):
            reason = "unit_not_in_context"
            continue
        return "ok"
    return reason


def verify_quote(
    document_text: str,
    quote: str,
    *,
    unit: str | None = None,
    require_unit: bool = True,
    amount: str | Decimal | None = None,
    reflow_lines: int = 2,
) -> QuoteVerifyResult:
    if not quote or not str(quote).strip():
        return QuoteVerifyResult(False, "empty_quote")
    if document_text is None:
        return QuoteVerifyResult(False, "missing_document_text")

    token = _COMMA_DECIMAL.set(is_decimal_comma_document(document_text))
    try:
        return _verify_quote(
            document_text, quote, unit=unit, require_unit=require_unit,
            amount=amount, reflow_lines=reflow_lines,
        )
    finally:
        _COMMA_DECIMAL.reset(token)


def _verify_quote(
    document_text: str,
    quote: str,
    *,
    unit: str | None,
    require_unit: bool,
    amount: str | Decimal | None,
    reflow_lines: int,
) -> QuoteVerifyResult:
    spec = parse_unit(unit) if unit else None
    if require_unit and unit:
        if spec is None:
            return QuoteVerifyResult(False, f"unsupported_unit:{unit}")
        if spec.ambiguous:
            return QuoteVerifyResult(False, f"ambiguous_unit:{unit}")

    doc, found = _find_quote_all(document_text, quote)
    if not found:
        return QuoteVerifyResult(False, "quote_not_found")

    # A quote can occur more than once (the same price in two seasons); any
    # occurrence that grounds amount and unit is enough.
    first_failure: QuoteVerifyResult | None = None
    spans = _line_spans(doc)
    for start, end in found:
        result = _verify_at(
            doc, spans, start, end, spec=spec, require_unit=require_unit,
            amount=amount, reflow_lines=reflow_lines,
        )
        if result.ok:
            return result
        first_failure = first_failure or result
    assert first_failure is not None
    return first_failure


def _verify_at(
    document_text: str,
    spans: list[tuple[int, int, str]],
    start: int,
    end: int,
    *,
    spec: UnitSpec | None,
    require_unit: bool,
    amount: str | Decimal | None,
    reflow_lines: int,
) -> QuoteVerifyResult:
    q = document_text[start:end]
    first = _line_index_at(spans, start)
    last = _line_index_at(spans, max(start, end - 1))

    if amount is None or str(amount).strip() == "":
        if require_unit and spec is not None:
            region = " ".join(spans[j][2] for j in _lines_outward(first, len(spans)))
            scale_pat = _SCALE_TOKENS.get(spec.scale or "")
            if scale_pat is not None and not scale_pat.search(region):
                return QuoteVerifyResult(False, "unit_not_in_context", start)
            if spec.denom and not _denominator_printed(spans, first, spec.denom, q):
                return QuoteVerifyResult(False, "unit_not_in_context", start)
        return QuoteVerifyResult(True, "ok", start)

    try:
        want = amount if isinstance(amount, Decimal) else Decimal(str(amount).strip())
    except (InvalidOperation, ValueError):
        return QuoteVerifyResult(False, "amount_unparseable", start)

    in_quote = [
        f for li in range(first, last + 1) for f in _figures_on_line(spans, li)
        if start <= spans[li][0] + f.start < end
    ]
    reason = _ground_figures(
        spans, in_quote, want=want, spec=spec, require_unit=require_unit, quote=q,
    )
    if reason == "ok":
        return QuoteVerifyResult(True, "ok", start)

    # A single-row quote that prints its own (different) price cites that
    # price; borrowing a neighbouring row's figure would ground the wrong cell.
    owns_figure = any(
        _is_decimal_figure(spans[f.line][2][f.start:f.end]) for f in in_quote
    )
    if reason is None and not (reflow_lines <= 2 and owns_figure):
        lo, hi = max(0, first - reflow_lines), min(len(spans), last + reflow_lines + 1)
        band = [
            f for li in range(lo, hi) for f in _figures_on_line(spans, li)
            if not (start <= spans[li][0] + f.start < end)
        ]
        reason = _ground_figures(
            spans, band, want=want, spec=spec, require_unit=require_unit, quote=q,
        )
        if reason == "ok":
            return QuoteVerifyResult(True, "ok", start)
    return QuoteVerifyResult(False, reason or "amount_not_in_quote", start)


def verify_component_quote(
    document_text: str,
    *,
    quote: str | None,
    unit: str | None,
    amount: str | Decimal | None = None,
    reflow_lines: int = 2,
) -> QuoteVerifyResult:
    if not quote:
        return QuoteVerifyResult(False, "missing_quote")
    return verify_quote(
        document_text,
        quote,
        unit=unit,
        require_unit=True,
        amount=amount,
        reflow_lines=reflow_lines,
    )


def verify_component_cells(document_text: str, component: Any) -> QuoteVerifyResult:
    """Ground **every** cell of a component, not just one.

    Each cell's amount must be printed in its quote, or — for a multi-cell
    component whose single quote cites one row — elsewhere in the cited
    table (``_TABLE_LINES`` of the quote). A cell may carry its own
    ``source_quote``; otherwise the component quote is used.
    """
    cells = list(getattr(component, "cells", None) or [])
    quote = getattr(component, "source_quote", None)
    unit = getattr(component, "unit", None)
    if not cells:
        return verify_component_quote(document_text, quote=quote, unit=unit)
    for i, cell in enumerate(cells):
        own_quote = cell.get("source_quote")
        result = verify_component_quote(
            document_text,
            quote=own_quote or quote,
            unit=unit,
            amount=cell.get("amount"),
            reflow_lines=2 if own_quote or len(cells) == 1 else _TABLE_LINES,
        )
        if not result.ok:
            if len(cells) > 1:
                return QuoteVerifyResult(
                    False, f"cell{i}:{result.reason}", result.quote_index,
                )
            return result
    return QuoteVerifyResult(True, "ok")


__all__ = [
    "QuoteVerifyResult",
    "UnitSpec",
    "is_decimal_comma_document",
    "parse_unit",
    "verify_component_cells",
    "verify_component_quote",
    "verify_quote",
]
