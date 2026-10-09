"""Verbatim quote grounding for priced component values (G3).

Every stored number must cite a page + verbatim span. The verifier checks:

1. The quote appears in the retained document text.
2. The unit token is grounded nearby — in the local window **or** in a
   table column/row header or section heading that scopes the number
   (R27: 8 correct reads were held because ¢/kWh sat only in the header).
3. Optionally, the quoted number matches the stored amount, and the quote
   sits in a row/column consistent with the component's season / period /
   day_type / tier labels (R27: 5 wrong reads passed loose grounding).

No LLM.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any


# Local char window around the quote for unit-token grounding.
_UNIT_WINDOW = 120
# Lines above the quote searched for column headers / section headings.
_HEADER_LINE_LOOKBACK = 12
# Lines above searched for season/period section headings.
_LABEL_LINE_LOOKBACK = 20

_UNIT_PATTERNS: dict[str, re.Pattern[str]] = {
    "$/kwh": re.compile(r"\$\s*/\s*kwh|\$\/kwh|dollars?\s+per\s+kwh", re.I),
    "cents/kwh": re.compile(
        r"¢\s*/\s*kwh|¢\s+per\s+kwh|c/\s*kwh|cents?\s*/\s*kwh|cents?\s+per\s+kwh|"
        r"¢/kwh|¢\s*per\s*kilowatt",
        re.I,
    ),
    "percent": re.compile(r"%|percent(?:age)?\s+of|per\s*cent", re.I),
    "mills/kwh": re.compile(r"mills?\s*/\s*kwh|mills?\s+per\s+kwh", re.I),
    "dimensionless": re.compile(r"factor|multiplier|×|x\s+\d", re.I),
}

# Synonyms so "winter" matches "Winter Season" / "Oct–May", etc.
_SEASON_ALIASES: dict[str, tuple[str, ...]] = {
    "summer": ("summer", "jun", "july", "aug", "may through", "june through"),
    "winter": ("winter", "nov", "dec", "jan", "feb", "oct through", "november"),
    "all": (),
}
_PERIOD_ALIASES: dict[str, tuple[str, ...]] = {
    "on_peak": ("on-peak", "on peak", "onpeak", "peak", "high"),
    "off_peak": ("off-peak", "off peak", "offpeak", "low"),
    "mid_peak": ("mid-peak", "mid peak", "midpeak", "shoulder"),
    "super_off_peak": ("super off", "super-off", "overnight", "ulo"),
    "all": (),
}
_DAY_ALIASES: dict[str, tuple[str, ...]] = {
    "weekday": ("weekday", "week day", "monday", "business day"),
    "weekend": ("weekend", "saturday", "sunday", "holiday"),
    "all": (),
}

# Leading minus is part of the token (don't let ``-0.0049`` match as ``0.0049``).
_NUMBER_RE = re.compile(
    r"(?<![\d.])(?:-(?:\d+\.\d+|\d+)|\d+\.\d+|\d+)(?![\d.])"
)


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


def _line_spans(document_text: str) -> list[tuple[int, int, str]]:
    """Return (start, end, line_text) for each line including the newline."""
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
    """True when the unit appears in a column header, row header, or section heading.

    Column header: a lookback line whose unit token sits near ``quote_col``.
    Row header: unit on the same line left of the quote (rare but real).
    Section heading: a short lookback line that is mostly label + unit.
    """
    if line_idx < 0 or line_idx >= len(spans):
        return False
    # Same-line row header region (left of the number).
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
            # Blank line — still allow one more step for section titles.
            continue
        for m in unit_pat.finditer(header):
            # Column alignment: unit token within ~24 chars of quote column,
            # or the whole header line is short (section / column title).
            if abs(m.start() - quote_col) <= 24 or len(header.strip()) <= 60:
                return True
            # Multi-column header row: any unit on a line that looks like a
            # header (contains "kwh" / "charge" / "rate") counts.
            if re.search(r"kwh|charge|rate|¢|cent|\$", header, re.I):
                return True
    return False


def _amount_in_quote(quote: str, amount: str | Decimal | None) -> bool:
    """True when a number in ``quote`` equals ``amount`` (exact Decimal).

    No 100× fuzziness — quoting ``0.52`` for stored ``52`` must fail (R27 SDG&E).
    Quotes with no numeric token at all (document-label citations) skip this
    check; row/col + unit grounding still apply.
    """
    if amount is None or amount == "":
        return True
    nums = list(_NUMBER_RE.finditer(quote))
    if not nums:
        return True
    try:
        want = amount if isinstance(amount, Decimal) else Decimal(str(amount))
    except (InvalidOperation, ValueError):
        return True  # don't fail grounding on unparseable expected amount
    for m in nums:
        try:
            got = Decimal(m.group(0))
        except InvalidOperation:
            continue
        if got == want:
            return True
    return False


def _label_aliases(kind: str, value: str | None) -> tuple[str, ...]:
    if not value or value == "all":
        return ()
    v = value.strip().lower().replace(" ", "_")
    table = {
        "season": _SEASON_ALIASES,
        "period": _PERIOD_ALIASES,
        "day_type": _DAY_ALIASES,
    }.get(kind, {})
    aliases = table.get(v, ())
    # Always include the raw token and dash/space variants.
    raw = value.strip().lower()
    extras = (raw, raw.replace("_", "-"), raw.replace("_", " "), v)
    return tuple(dict.fromkeys(aliases + extras))


def _context_has_label(hay: str, aliases: tuple[str, ...]) -> bool:
    if not aliases:
        return True
    low = hay.lower()
    return any(a and a in low for a in aliases)


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
    """Require season/period/day_type evidence in the quote's row or column.

    Period / day_type must appear on the **same line** (row label) or in the
    column header at the quote's column — not merely somewhere else in the
    table (that let On-Peak numbers pass as Off-Peak in R27). Season may
    also come from the nearest section heading above. Component name is
    advisory only (not required): extractors often quote the number alone.
    """
    same_line = spans[line_idx][2] if 0 <= line_idx < len(spans) else ""

    # Column header slice near the quote — only lines that look like headers
    # (unit / "Period" / "Charge"), never sibling data rows (those carry the
    # *other* period's label and would false-pass row checks).
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
            r"\brate\b|\benergy charge\b",
            header,
            re.I,
        ))
        # A numeric rate row (period label + amount) is not a header.
        is_data_row = (
            len(_NUMBER_RE.findall(header)) >= 1
            and re.search(r"peak|off|weekend|weekday|tier|block|summer|winter", header, re.I)
        )
        if is_data_row and not looks_header:
            continue
        if not looks_header:
            continue
        lo = max(0, quote_col - 18)
        hi = min(len(header), quote_col + 24)
        col_bits.append(header[lo:hi] if len(header) > 50 else header)
        break
    col_hay = " ".join(col_bits)

    # Nearest section heading (short, ≤1 number) above — for season.
    # Skip sibling rate rows; do not stop the search on them.
    section = ""
    for back in range(1, _LABEL_LINE_LOOKBACK + 1):
        j = line_idx - back
        if j < 0:
            break
        line = spans[j][2].strip()
        if not line:
            continue
        if len(_NUMBER_RE.findall(line)) >= 1 and re.search(
            r"peak|off|weekend|weekday|tier|block", line, re.I
        ):
            continue  # sibling data row — keep looking for a season heading
        if len(line) <= 80 and len(_NUMBER_RE.findall(line)) <= 1:
            section = line
            break

    row_or_col = f"{same_line} {col_hay}"

    period_aliases = _label_aliases("period", period)
    if period_aliases and not _context_has_label(row_or_col, period_aliases):
        return False, "label_not_in_row_col:period"

    day_aliases = _label_aliases("day_type", day_type)
    if day_aliases and not _context_has_label(row_or_col, day_aliases):
        return False, "label_not_in_row_col:day_type"

    season_aliases = _label_aliases("season", season)
    if season_aliases and not _context_has_label(
        f"{row_or_col} {section}", season_aliases
    ):
        return False, "label_not_in_row_col:season"

    if tier and tier not in {"all", "1"}:
        tier_aliases = (tier.lower(), f"tier {tier}", f"block {tier}", f"step {tier}")
        if not _context_has_label(f"{row_or_col} {section}", tier_aliases):
            return False, "label_not_in_row_col:tier"

    # component_name intentionally not required — see docstring.
    _ = component_name
    return True, "ok"


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
    """Check that ``quote`` appears verbatim in ``document_text``.

    Unit grounding accepts the local window **or** a table column/row header
    / section heading that scopes the number. When ``require_row_col`` or any
    cell label is provided, the quote's row/column context must mention those
    labels. When ``amount`` is set, a number inside the quote must equal it.
    """
    if not quote or not str(quote).strip():
        return QuoteVerifyResult(False, "empty_quote")
    if document_text is None:
        return QuoteVerifyResult(False, "missing_document_text")

    q = str(quote)
    idx = document_text.find(q)
    if idx < 0:
        collapsed_doc = re.sub(r"\s+", " ", document_text)
        collapsed_q = re.sub(r"\s+", " ", q).strip()
        idx2 = collapsed_doc.find(collapsed_q)
        if idx2 < 0:
            return QuoteVerifyResult(False, "quote_not_found")
        idx = idx2
        document_text = collapsed_doc
        q = collapsed_q

    if amount is not None and not _amount_in_quote(q, amount):
        return QuoteVerifyResult(False, "amount_not_in_quote", idx)

    spans = _line_spans(document_text)
    line_idx = _line_index_at(spans, idx)
    quote_col = idx - spans[line_idx][0]

    if require_unit and unit:
        norm = _normalize_unit(unit)
        pat = _UNIT_PATTERNS.get(norm)
        if pat is None:
            return QuoteVerifyResult(False, f"unsupported_unit:{unit}", idx)
        start = max(0, idx - _UNIT_WINDOW)
        end = min(len(document_text), idx + len(q) + _UNIT_WINDOW)
        window = document_text[start:end]
        if not pat.search(window):
            if not _unit_in_headers(spans, line_idx, quote_col, pat):
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
    """Convenience for a priced component version's evidence fields."""
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
