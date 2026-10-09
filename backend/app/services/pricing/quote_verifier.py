"""Verbatim quote grounding for priced component values (G3).

Every stored number must cite a page + verbatim span. The verifier checks:

1. The quote appears in the retained document text.
2. The unit token is grounded nearby — in the local window **or** in a
   table column/row header or section heading that scopes the number.
3. Optionally, the quoted number matches the stored amount, and the quote
   sits in a row/column consistent with the component's season / period /
   day_type / tier labels.

R28: tier/season matching is synonym- and span-aware (merged headers);
Ontario RPP ($/kWh stored, ¢ printed) and similar unit conversions are
accepted only when the *other* unit is grounded in context.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any


_UNIT_WINDOW = 120
_HEADER_LINE_LOOKBACK = 12
_LABEL_LINE_LOOKBACK = 30  # merged season headers can sit well above the row

_UNIT_PATTERNS: dict[str, re.Pattern[str]] = {
    "$/kwh": re.compile(r"\$\s*/\s*kwh|\$\/kwh|dollars?\s+per\s+kwh", re.I),
    "cents/kwh": re.compile(
        r"¢\s*/\s*kwh|¢\s+per\s+kwh|c/\s*kwh|cents?\s*/\s*kwh|cents?\s+per\s+kwh|"
        r"¢/kwh|¢\s*per\s*kilowatt|\bcents?\b",
        re.I,
    ),
    "percent": re.compile(r"%|percent(?:age)?\s+of|per\s*cent", re.I),
    "mills/kwh": re.compile(r"mills?\s*/\s*kwh|mills?\s+per\s+kwh", re.I),
    "dimensionless": re.compile(r"factor|multiplier|×|x\s+\d", re.I),
}

_SEASON_ALIASES: dict[str, tuple[str, ...]] = {
    "summer": (
        "summer", "jun", "july", "aug", "may through", "june through",
        "may 1", "may –", "may-", "jun-", "summer season", "summer period",
    ),
    "winter": (
        "winter", "nov", "dec", "jan", "feb", "oct through", "november",
        "nov 1", "nov –", "nov-", "winter season", "winter period",
        "december", "january", "february",
    ),
    "non_winter": (
        "non-winter", "non winter", "nonwinter", "summer", "shoulder",
        "apr", "may", "june", "july", "aug", "sep", "oct",
        "april through", "may through", "non-heating",
    ),
    "all": (),
}
_PERIOD_ALIASES: dict[str, tuple[str, ...]] = {
    "on_peak": ("on-peak", "on peak", "onpeak", "peak", "high", "on–peak"),
    "off_peak": ("off-peak", "off peak", "offpeak", "low", "off–peak"),
    "mid_peak": (
        "mid-peak", "mid peak", "midpeak", "shoulder", "mid–peak",
        "partial-peak", "partial peak",
    ),
    "super_off_peak": ("super off", "super-off", "overnight", "ulo", "ultra-low"),
    "ulo": ("ulo", "ultra-low", "ultra low", "overnight"),
    "weekend_off": ("weekend off", "weekend off-peak", "weekend off peak"),
    "all": (),
}
_DAY_ALIASES: dict[str, tuple[str, ...]] = {
    "weekday": ("weekday", "week day", "monday", "business day", "weekdays"),
    "weekend": ("weekend", "saturday", "sunday", "holiday", "weekends"),
    "holiday": ("holiday", "statutory", "stat holiday"),
    "all": (),
}

_NUMBER_RE = re.compile(
    r"(?<![\d.])(?:-(?:\d+\.\d+|\d+)|\d+\.\d+|\d+)(?![\d.])"
)

# Normalize common PDF / HTML artifacts before verbatim search.
_DASHES = re.compile(r"[\u2010-\u2015\u2212\ufe58\ufe63\uff0d]")
_SPACES = re.compile(r"[\u00a0\u2000-\u200b\u202f\u205f\u3000]+")


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
        spans.append((start, end, line.rstrip("\n\r")))
        start = end
    if not spans and document_text:
        spans.append((0, len(document_text), document_text))
    return spans


def _line_index_at(spans: list[tuple[int, int, str]], offset: int) -> int:
    for i, (s, e, _t) in enumerate(spans):
        if s <= offset < e or (offset == e and i == len(spans) - 1):
            return i
        if offset < s:
            return max(0, i - 1)
    return max(0, len(spans) - 1)


def _unit_in_headers(
    spans: list[tuple[int, int, str]],
    line_idx: int,
    quote_col: int,
    unit_pat: re.Pattern[str],
) -> bool:
    if line_idx < 0 or line_idx >= len(spans):
        return False
    same = spans[line_idx][2]
    left = same[: max(0, quote_col)]
    if unit_pat.search(left):
        return True

    for back in range(1, _HEADER_LINE_LOOKBACK + 1):
        j = line_idx - back
        if j < 0:
            break
        header = spans[j][2]
        if not header.strip():
            continue
        for m in unit_pat.finditer(header):
            if abs(m.start() - quote_col) <= 24 or len(header.strip()) <= 60:
                return True
            if re.search(r"kwh|charge|rate|¢|cent|\$|price", header, re.I):
                return True
    return False


def _context_unit_family(
    document_text: str,
    spans: list[tuple[int, int, str]],
    idx: int,
    qlen: int,
) -> set[str]:
    """Which unit families appear near the quote (window + headers)."""
    start = max(0, idx - _UNIT_WINDOW)
    end = min(len(document_text), idx + qlen + _UNIT_WINDOW)
    window = document_text[start:end]
    found: set[str] = set()
    for family, pat in _UNIT_PATTERNS.items():
        if family in {"percent", "dimensionless", "mills/kwh"}:
            continue
        if pat.search(window):
            found.add(family)
    line_idx = _line_index_at(spans, idx)
    quote_col = idx - spans[line_idx][0]
    for family, pat in _UNIT_PATTERNS.items():
        if family in {"percent", "dimensionless", "mills/kwh"}:
            continue
        if _unit_in_headers(spans, line_idx, quote_col, pat):
            found.add(family)
    return found


def _amount_candidates(
    amount: str | Decimal,
    stored_unit: str | None,
    context_units: set[str],
) -> list[Decimal]:
    """Exact amount plus unit-aware ¢↔$ forms when the other unit is in context.

    Does **not** blindly accept 100× (that was the R27 SDG&E false pass).
    """
    try:
        want = amount if isinstance(amount, Decimal) else Decimal(str(amount))
    except (InvalidOperation, ValueError):
        return []
    out = [want]
    norm = _normalize_unit(stored_unit)
    # Stored $/kWh, document shows cents → accept cents form.
    if norm == "$/kwh" and "cents/kwh" in context_units:
        out.append(want * Decimal("100"))
    # Stored ¢/kWh, document shows dollars → accept dollars form.
    if norm == "cents/kwh" and "$/kwh" in context_units:
        out.append(want / Decimal("100"))
    return out


def _amount_in_quote(
    quote: str,
    amount: str | Decimal | None,
    *,
    unit: str | None = None,
    context_units: set[str] | None = None,
) -> bool:
    if amount is None or amount == "":
        return True
    nums = list(_NUMBER_RE.finditer(quote))
    if not nums:
        return True
    wants = _amount_candidates(amount, unit, context_units or set())
    if not wants:
        return True
    for m in nums:
        try:
            got = Decimal(m.group(0))
        except InvalidOperation:
            continue
        if any(got == w for w in wants):
            return True
    return False


def _label_aliases(kind: str, value: str | None) -> tuple[str, ...]:
    if not value or value == "all":
        return ()
    v = value.strip().lower().replace(" ", "_").replace("-", "_")
    table = {
        "season": _SEASON_ALIASES,
        "period": _PERIOD_ALIASES,
        "day_type": _DAY_ALIASES,
    }.get(kind, {})
    aliases = table.get(v, ())
    raw = value.strip().lower()
    extras = (
        raw,
        raw.replace("_", "-"),
        raw.replace("_", " "),
        raw.replace("_", "–"),
        v,
        v.replace("_", "-"),
        v.replace("_", " "),
    )
    return tuple(dict.fromkeys([a for a in aliases + extras if a]))


def _tier_aliases(tier: str) -> tuple[str, ...]:
    """Synonyms for tier / block / step labels (R28 FPL, HQ, BCH, GA)."""
    t = tier.strip().lower()
    out: list[str] = [t, t.replace("_", " "), t.replace("_", "-")]

    # step1 / step 1 / tier 1
    m = re.match(r"(?:step|tier|block|level)\s*[_-]?\s*(\d+)", t)
    if m:
        n = m.group(1)
        out.extend([
            f"step {n}", f"step{n}", f"tier {n}", f"tier{n}",
            f"block {n}", f"level {n}", f"step {n} energy",
        ])

    # 0-1000 / 1000+ / 0-650
    m = re.match(r"(\d+)\s*[-–to]+\s*(\d+)(\+)?", t.replace(" ", ""))
    if m:
        a, b, plus = m.group(1), m.group(2), m.group(3) or ""
        out.extend([
            f"{a}-{b}", f"{a} – {b}", f"{a} to {b}",
            f"first {b}", f"first {b} kwh", f"0-{b}",
            f"{a} through {b}",
        ])
        if plus or t.endswith("+"):
            out.extend([f"over {b}", f"above {b}", f">{b}+", f"more than {b}"])

    # 1000+
    m = re.match(r"(\d+)\s*\+", t)
    if m:
        n = m.group(1)
        out.extend([f"{n}+", f"over {n}", f"above {n}", f"more than {n}",
                    f"over {n} kwh", f"above {n} kwh"])

    # 0-40kwh_day / 40kwh_day+
    m = re.match(r"(\d+)\s*kwh[_\s-]*day", t)
    if m:
        n = m.group(1)
        out.extend([
            f"{n} kwh", f"{n} kwh per day", f"first {n}",
            f"first {n} kwh", f"first {n} kwh/day", f"0-{n}",
        ])
    m = re.match(r"(\d+)\s*kwh[_\s-]*day\+", t)
    if m:
        n = m.group(1)
        out.extend([f"over {n}", f"above {n}", f"remaining", f"balance",
                    f"additional", f"{n}+"])

    return tuple(dict.fromkeys(a for a in out if a))


def _context_has_label(hay: str, aliases: tuple[str, ...]) -> bool:
    if not aliases:
        return True
    low = hay.lower()
    return any(a and a in low for a in aliases)


def _header_band(header: str, quote_col: int) -> str:
    """Slice around the quote column; keep the full line when short/merged."""
    if len(header) <= 80:
        return header
    lo = max(0, quote_col - 30)
    hi = min(len(header), quote_col + 40)
    # Also keep leading row-header tokens (merged-cell leftovers).
    lead = header[: min(40, len(header))]
    return f"{lead} {header[lo:hi]}"


def _row_col_labels_ok(
    spans: list[tuple[int, int, str]],
    line_idx: int,
    quote_col: int,
    *,
    season: str | None,
    period: str | None,
    day_type: str | None,
    tier: str | None,
    component_name: str | None,
) -> tuple[bool, str]:
    same_line = spans[line_idx][2] if 0 <= line_idx < len(spans) else ""

    col_bits: list[str] = []
    for back in range(1, _HEADER_LINE_LOOKBACK + 1):
        j = line_idx - back
        if j < 0:
            break
        header = spans[j][2]
        if not header.strip():
            continue
        looks_header = bool(re.search(
            r"¢\s*/\s*kwh|cents?\s*/?\s*kwh|\$\s*/\s*kwh|\bperiod\b|\bcharge\b|"
            r"\brate\b|\benergy charge\b|\bprice\b|\btier\b|\bstep\b|\bblock\b|"
            r"\bseason\b|\bsummer\b|\bwinter\b",
            header,
            re.I,
        ))
        is_data_row = (
            len(_NUMBER_RE.findall(header)) >= 1
            and re.search(
                r"peak|off|weekend|weekday|tier|block|summer|winter|step",
                header,
                re.I,
            )
        )
        if is_data_row and not looks_header:
            continue
        if not looks_header:
            continue
        col_bits.append(_header_band(header, quote_col))
        # Keep collecting a second header row (multi-row / merged headers).
        if len(col_bits) >= 2:
            break
    col_hay = " ".join(col_bits)

    # Section headings: season banners, merged cells above the table.
    sections: list[str] = []
    for back in range(1, _LABEL_LINE_LOOKBACK + 1):
        j = line_idx - back
        if j < 0:
            break
        line = spans[j][2].strip()
        if not line:
            continue
        if len(_NUMBER_RE.findall(line)) >= 1 and re.search(
            r"peak|off|weekend|weekday|tier|block|step", line, re.I
        ):
            continue
        if len(line) <= 100 and len(_NUMBER_RE.findall(line)) <= 1:
            sections.append(line)
            if len(sections) >= 3:
                break
    section = " ".join(sections)

    row_or_col = f"{same_line} {col_hay}"
    broad = f"{row_or_col} {section}"

    period_aliases = _label_aliases("period", period)
    if period_aliases and not _context_has_label(row_or_col, period_aliases):
        # Period may also sit in a merged header span above the row.
        if not _context_has_label(broad, period_aliases):
            return False, "label_not_in_row_col:period"

    day_aliases = _label_aliases("day_type", day_type)
    if day_aliases and not _context_has_label(row_or_col, day_aliases):
        if not _context_has_label(broad, day_aliases):
            return False, "label_not_in_row_col:day_type"

    season_aliases = _label_aliases("season", season)
    if season_aliases and not _context_has_label(broad, season_aliases):
        return False, "label_not_in_row_col:season"

    if tier and tier not in {"all", "1"}:
        tier_aliases = _tier_aliases(tier)
        if not _context_has_label(broad, tier_aliases):
            return False, "label_not_in_row_col:tier"

    _ = component_name
    return True, "ok"


def _find_quote(document_text: str, quote: str) -> tuple[int, str, str]:
    """Return (index, doc_used, quote_used). Tries normalized dash/space forms."""
    q = str(quote)
    idx = document_text.find(q)
    if idx >= 0:
        return idx, document_text, q

    norm_doc = _normalize_text(document_text)
    norm_q = _normalize_text(q)
    idx = norm_doc.find(norm_q)
    if idx >= 0:
        return idx, norm_doc, norm_q

    collapsed_doc = re.sub(r"\s+", " ", norm_doc)
    collapsed_q = re.sub(r"\s+", " ", norm_q).strip()
    idx = collapsed_doc.find(collapsed_q)
    if idx >= 0:
        return idx, collapsed_doc, collapsed_q

    return -1, document_text, q


def verify_quote(
    document_text: str,
    quote: str,
    *,
    unit: str | None = None,
    require_unit: bool = True,
    amount: str | Decimal | None = None,
    season: str | None = None,
    period: str | None = None,
    day_type: str | None = None,
    tier: str | None = None,
    component_name: str | None = None,
    require_row_col: bool = False,
) -> QuoteVerifyResult:
    if not quote or not str(quote).strip():
        return QuoteVerifyResult(False, "empty_quote")
    if document_text is None:
        return QuoteVerifyResult(False, "missing_document_text")

    idx, document_text, q = _find_quote(document_text, quote)
    if idx < 0:
        return QuoteVerifyResult(False, "quote_not_found")

    spans = _line_spans(document_text)
    line_idx = _line_index_at(spans, idx)
    quote_col = idx - spans[line_idx][0]
    context_units = _context_unit_family(document_text, spans, idx, len(q))

    if amount is not None and not _amount_in_quote(
        q, amount, unit=unit, context_units=context_units,
    ):
        return QuoteVerifyResult(False, "amount_not_in_quote", idx)

    if require_unit and unit:
        norm = _normalize_unit(unit)
        pat = _UNIT_PATTERNS.get(norm)
        if pat is None:
            return QuoteVerifyResult(False, f"unsupported_unit:{unit}", idx)
        start = max(0, idx - _UNIT_WINDOW)
        end = min(len(document_text), idx + len(q) + _UNIT_WINDOW)
        window = document_text[start:end]
        unit_ok = bool(pat.search(window)) or _unit_in_headers(
            spans, line_idx, quote_col, pat,
        )
        # Ontario RPP / similar: stored $/kWh, page prints ¢/kWh (or reverse).
        # Accept the sibling energy unit only when an amount was supplied and
        # the quote already matched via the 100× conversion (keeps bare
        # "18.324" + $/kWh in a cents-only doc as a hold).
        if not unit_ok and norm in {"$/kwh", "cents/kwh"} and amount is not None:
            sibling = "cents/kwh" if norm == "$/kwh" else "$/kwh"
            spat = _UNIT_PATTERNS[sibling]
            sibling_grounded = bool(spat.search(window)) or _unit_in_headers(
                spans, line_idx, quote_col, spat,
            )
            if sibling_grounded:
                try:
                    want = (
                        amount if isinstance(amount, Decimal)
                        else Decimal(str(amount))
                    )
                except (InvalidOperation, ValueError):
                    want = None
                converted = [
                    c for c in _amount_candidates(
                        amount, unit, context_units | {sibling},
                    )
                    if want is not None and c != want
                ]
                # Quote must cite the converted figure (9.8 for 0.098), not
                # the stored dollars amount sitting in a cents column.
                if converted and any(
                    _amount_in_quote(q, c, unit=None, context_units=set())
                    for c in converted
                ):
                    unit_ok = True
                    context_units.add(sibling)
        if not unit_ok:
            return QuoteVerifyResult(False, "unit_not_in_context", idx)

    need_row_col = require_row_col or any(
        v and v != "all" for v in (season, period, day_type, tier)
    ) or bool(component_name)
    if need_row_col:
        ok, reason = _row_col_labels_ok(
            spans,
            line_idx,
            quote_col,
            season=season,
            period=period,
            day_type=day_type,
            tier=tier,
            component_name=component_name,
        )
        if not ok:
            return QuoteVerifyResult(False, reason, idx)

    return QuoteVerifyResult(True, "ok", idx)


def verify_component_quote(
    document_text: str,
    *,
    quote: str | None,
    unit: str | None,
    amount: str | Decimal | None = None,
    cell: dict[str, Any] | None = None,
    component_name: str | None = None,
    require_row_col: bool = False,
) -> QuoteVerifyResult:
    if not quote:
        return QuoteVerifyResult(False, "missing_quote")
    cell = cell or {}
    return verify_quote(
        document_text,
        quote,
        unit=unit,
        require_unit=True,
        amount=amount,
        season=cell.get("season"),
        period=cell.get("period"),
        day_type=cell.get("day_type"),
        tier=cell.get("tier"),
        component_name=component_name,
        require_row_col=require_row_col,
    )


__all__ = [
    "QuoteVerifyResult",
    "verify_component_quote",
    "verify_quote",
]
