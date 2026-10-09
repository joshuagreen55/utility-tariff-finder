"""Verbatim quote grounding for priced component values (PR B / G3).

Every stored number must cite a page + verbatim span. The verifier checks
that the quote appears in the retained document text and that the unit
token is present in a local window around that span. No LLM.
"""
from __future__ import annotations

import re
from dataclasses import dataclass


# Context window (chars) around the quote for unit-token grounding.
_UNIT_WINDOW = 120

_UNIT_PATTERNS: dict[str, re.Pattern[str]] = {
    "$/kwh": re.compile(r"\$\s*/\s*kwh|\$\/kwh|dollars?\s+per\s+kwh", re.I),
    "cents/kwh": re.compile(
        r"¢\s*/\s*kwh|¢\s+per\s+kwh|c/\s*kwh|cents?\s*/\s*kwh|cents?\s+per\s+kwh",
        re.I,
    ),
    "percent": re.compile(r"%|percent(?:age)?\s+of|per\s*cent", re.I),
    "mills/kwh": re.compile(r"mills?\s*/\s*kwh|mills?\s+per\s+kwh", re.I),
    "dimensionless": re.compile(r"factor|multiplier|×|x\s+\d", re.I),
}


def _normalize_unit(unit: str | None) -> str:
    u = (unit or "").strip().lower().replace(" ", "")
    if u in {"$/kwh", "usd/kwh", "cad/kwh"}:
        return "$/kwh"
    if u in {"¢/kwh", "c/kwh", "cents/kwh"} or "cent" in u or u.startswith("¢"):
        return "cents/kwh"
    if u in {"percent", "%", "pct"}:
        return "percent"
    if u in {"mills/kwh", "mill/kwh"}:
        return "mills/kwh"
    if u in {"dimensionless", "factor", "x"}:
        return "dimensionless"
    return u


@dataclass(frozen=True)
class QuoteVerifyResult:
    ok: bool
    reason: str
    quote_index: int | None = None  # start offset in document_text when found

    @property
    def passed(self) -> bool:
        return self.ok


def verify_quote(
    document_text: str,
    quote: str,
    *,
    unit: str | None = None,
    require_unit: bool = True,
) -> QuoteVerifyResult:
    """Check that ``quote`` appears verbatim in ``document_text``.

    When ``require_unit`` and ``unit`` are set, a unit token must also appear
    within ``_UNIT_WINDOW`` characters of the quote match (same local
    context — catches ¢-vs-$ confusion).
    """
    if not quote or not str(quote).strip():
        return QuoteVerifyResult(False, "empty_quote")
    if document_text is None:
        return QuoteVerifyResult(False, "missing_document_text")

    q = str(quote)
    idx = document_text.find(q)
    if idx < 0:
        # Try a collapsed-whitespace variant for HTML→text churn, but the
        # stored quote itself must still be a contiguous substring after
        # the same collapse — we only collapse both sides identically.
        collapsed_doc = re.sub(r"\s+", " ", document_text)
        collapsed_q = re.sub(r"\s+", " ", q).strip()
        idx2 = collapsed_doc.find(collapsed_q)
        if idx2 < 0:
            return QuoteVerifyResult(False, "quote_not_found")
        # Grounding passed on collapsed text; report index in collapsed form.
        idx = idx2
        document_text = collapsed_doc
        q = collapsed_q

    if require_unit and unit:
        norm = _normalize_unit(unit)
        pat = _UNIT_PATTERNS.get(norm)
        if pat is None:
            return QuoteVerifyResult(False, f"unsupported_unit:{unit}", idx)
        start = max(0, idx - _UNIT_WINDOW)
        end = min(len(document_text), idx + len(q) + _UNIT_WINDOW)
        window = document_text[start:end]
        if not pat.search(window):
            return QuoteVerifyResult(False, "unit_not_in_context", idx)

    return QuoteVerifyResult(True, "ok", idx)


def verify_component_quote(
    document_text: str,
    *,
    quote: str | None,
    unit: str | None,
) -> QuoteVerifyResult:
    """Convenience for a priced component version's evidence fields."""
    if not quote:
        return QuoteVerifyResult(False, "missing_quote")
    return verify_quote(document_text, quote, unit=unit, require_unit=True)
