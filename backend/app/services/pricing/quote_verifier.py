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

from app.services.pricing.types import normalize_cell_label
from app.services.pricing.units import UNIT_PATTERNS as _UNIT_PATTERNS
from app.services.pricing.units import normalize_unit as _normalize_unit_table

_UNIT_WINDOW = 120
_HEADER_LINE_LOOKBACK = 12
_LABEL_LINE_LOOKBACK = 30  # merged season headers can sit well above the row
_TABLE_LINES = 25  # other rows of the table a multi-cell quote cites

# Matched on word boundaries (see ``_alias_regex``), so "dec" never matches
# "decrease". Bare "may" is left out: it is usually the verb.
_SEASON_ALIASES: dict[str, tuple[str, ...]] = {
    "summer": (
        "summer", "jun", "june", "july", "aug", "august", "may through",
        "june through", "may 1", "may –", "may-", "may to", "jun-",
        "summer season", "summer period",
    ),
    "winter": (
        "winter", "nov", "dec", "jan", "feb", "oct through", "november",
        "nov 1", "nov –", "nov-", "winter season", "winter period",
        "december", "january", "february",
    ),
    "non_winter": (
        "non-winter", "non winter", "nonwinter", "summer", "shoulder",
        "apr", "april", "june", "july", "aug", "august", "sep", "september",
        "oct", "october", "april through", "may through", "may 1", "may-",
        "may –", "may to", "non-heating",
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
    return _normalize_unit_table(unit)


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
    *,
    quote: str | None = None,
) -> list[Decimal]:
    """Exact amount plus unit-aware ¢↔$ forms when the other unit is in context.

    Does **not** blindly accept 100× (that was the R27 SDG&E false pass)
    unless the quote itself shows the sibling unit marker ($ vs ¢).
    """
    try:
        want = amount if isinstance(amount, Decimal) else Decimal(str(amount))
    except (InvalidOperation, ValueError):
        return []
    out = [want]
    norm = _normalize_unit(stored_unit)
    q = (quote or "").lower()
    quote_has_dollar = bool(re.search(r"\$\s*\d", q))
    quote_has_cent = bool(re.search(r"¢|\bcents?\b", q))
    # Stored $/kWh, document shows cents → accept cents form.
    if norm == "$/kwh" and ("cents/kwh" in context_units or quote_has_cent):
        out.append(want * Decimal("100"))
    # Stored ¢/kWh, document shows dollars → accept dollars form.
    if norm == "cents/kwh" and ("$/kwh" in context_units or quote_has_dollar):
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
        return False
    wants = _amount_candidates(
        amount, unit, context_units or set(), quote=quote,
    )
    if not wants:
        return False
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
    for canon in sorted(_wanted_values(kind, value) or ()) if table else ():
        if canon != v:
            aliases = aliases + table.get(canon, ())
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


def _alias_regex(aliases: tuple[str, ...] | list[str]) -> re.Pattern[str]:
    """Whole-word alternation, longest alias first (leftmost-longest wins)."""
    parts = []
    for a in sorted({a.lower() for a in aliases if a}, key=len, reverse=True):
        pre = r"(?<![a-z0-9])" if a[0].isalnum() else ""
        post = r"(?![a-z0-9])" if a[-1].isalnum() else ""
        parts.append(f"{pre}{re.escape(a)}{post}")
    return re.compile("|".join(parts) or r"(?!x)x", re.I)


def _context_has_label(hay: str, aliases: tuple[str, ...]) -> bool:
    if not aliases:
        return True
    return bool(_alias_regex(aliases).search(hay))


# Label words that decide which value a number belongs to when several sit
# on one line or in one header row. Months, "high"/"low" and other loose
# hints are presence evidence only, never governing.
_GOVERNING: dict[str, dict[str, frozenset[str]]] = {
    "season": {
        "summer": frozenset({"summer", "non_winter"}),
        "winter": frozenset({"winter"}),
        "non-winter": frozenset({"non_winter"}),
        "non winter": frozenset({"non_winter"}),
        "nonwinter": frozenset({"non_winter"}),
    },
    "period": {
        **{a: frozenset({"on_peak"}) for a in
           ("on-peak", "on peak", "onpeak", "on–peak", "peak")},
        **{a: frozenset({"off_peak"}) for a in
           ("off-peak", "off peak", "offpeak", "off–peak")},
        **{a: frozenset({"mid_peak"}) for a in
           ("mid-peak", "mid peak", "midpeak", "mid–peak", "shoulder",
            "partial-peak", "partial peak")},
        **{a: frozenset({"super_off_peak", "ulo"}) for a in
           ("super off-peak", "super off peak", "super-off-peak", "ulo",
            "ultra-low", "ultra low", "overnight")},
        **{a: frozenset({"weekend_off"}) for a in
           ("weekend off-peak", "weekend off peak", "weekend off")},
    },
    "day_type": {
        **{a: frozenset({"weekday"}) for a in
           ("weekday", "weekdays", "week day", "monday", "business day")},
        **{a: frozenset({"weekend"}) for a in
           ("weekend", "weekends", "saturday", "sunday")},
        "holiday": frozenset({"weekend", "holiday"}),
        "holidays": frozenset({"weekend", "holiday"}),
    },
}
_GOVERNING_RE = {dim: _alias_regex(list(t)) for dim, t in _GOVERNING.items()}
_DECIMAL_RE = re.compile(r"\d\.\d")


def _wanted_values(dim: str, value: str | None) -> frozenset[str] | None:
    """Canonical values a cell label stands for (``None`` = no constraint)."""
    v = normalize_cell_label(value)
    if v == "all":
        return None
    out = {v}
    table = {"season": _SEASON_ALIASES, "period": _PERIOD_ALIASES,
             "day_type": _DAY_ALIASES}[dim]
    forms = {v, v.replace("_", "-"), v.replace("_", " ")}
    for canon, aliases in table.items():
        if forms & set(aliases):
            out.add(canon)
    for word, vals in _GOVERNING[dim].items():
        if word in forms:
            out |= vals
    # Sub-periods name their family: mid_peak_a is a mid_peak.
    for vals in _GOVERNING[dim].values():
        out |= {c for c in vals if v.startswith(c + "_")}
    return frozenset(out)


_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}
_MONTH_WORD = (
    r"(january|february|march|april|may|june|july|august|september|october|"
    r"november|december|jan|feb|mar|apr|jun|jul|aug|sept|sep|oct|nov|dec)\.?"
)
_MONTH_RANGE_RE = re.compile(
    rf"(?<![a-z]){_MONTH_WORD}(?:\s+\d{{1,2}})?\s*(?:-|–|through|thru|to)\s*"
    rf"{_MONTH_WORD}(?![a-z])",
    re.I,
)


def _month(word: str) -> int:
    w = word.lower().rstrip(".")
    return _MONTHS.get(w[:4] if w.startswith("sept") else w[:3], 0)


def _range_seasons(first: str, last: str) -> frozenset[str]:
    """Seasons a month range stands for ("June - September" → summer)."""
    a, b = _month(first), _month(last)
    if not a or not b:
        return frozenset()
    months = {((a - 1 + i) % 12) + 1 for i in range((b - a) % 12 + 1)}
    out: set[str] = set()
    if months & {12, 1, 2}:
        out.add("winter")
    else:
        out.add("non_winter")
    if months & {6, 7, 8}:
        out.add("summer")
    return frozenset(out)


def _month_range_tokens(text: str) -> list[tuple[int, int, frozenset[str]]]:
    out = []
    for m in _MONTH_RANGE_RE.finditer(text):
        seasons = _range_seasons(m.group(1), m.group(2))
        if seasons:
            out.append((m.start(), m.end(), seasons))
    return out


def _tokens(dim: str, text: str) -> list[tuple[int, int, frozenset[str]]]:
    toks = [
        (m.start(), m.end(), _GOVERNING[dim][m.group(0).lower()])
        for m in _GOVERNING_RE[dim].finditer(text)
    ]
    if dim == "season":
        ranges = _month_range_tokens(text)
        toks = [
            t for t in toks
            if not any(r[0] <= t[0] < r[1] for r in ranges)
        ] + ranges
        toks.sort()
    return toks


def _governing_on_line(
    dim: str, line: str, num_start: int, num_end: int,
) -> frozenset[str] | None:
    """Label governing the number at ``line[num_start:num_end]``.

    The line's layout decides direction: if its first label comes before its
    first number, labels lead their numbers ("Off-peak 9.8; On-peak 20.3");
    otherwise they trail them ("9.8 off-peak; 20.3 on-peak").
    """
    toks = _tokens(dim, line)
    if not toks:
        return None
    nums = [(m.start(), m.end()) for m in _NUMBER_RE.finditer(line)]
    if not nums or toks[0][0] < nums[0][0]:
        prev_end = max((e for s, e in nums if e <= num_start), default=0)
        seg = [t for t in toks if t[0] >= prev_end and t[1] <= num_start]
        if seg:
            return seg[-1][2]
        before = [t for t in toks if t[1] <= num_start]
        return before[-1][2] if before else None
    next_start = min((s for s, e in nums if s >= num_end), default=len(line))
    seg = [t for t in toks if t[0] >= num_end and t[1] <= next_start]
    if seg:
        return seg[0][2]
    after = [t for t in toks if t[0] >= num_end]
    return after[0][2] if after else None


def _governing_above(
    dim: str,
    spans: list[tuple[int, int, str]],
    line_idx: int,
    num_start: int,
) -> frozenset[str] | None:
    """Nearest header / heading above that names a ``dim`` label.

    Data rows (lines with a decimal rate) are skipped. A header naming
    several values is a column header: the number's ordinal on its row picks
    the column when counts line up, else the nearest column.
    """
    row = spans[line_idx][2]
    row_nums = [m for m in _NUMBER_RE.finditer(row) if "." in m.group(0)]
    for back in range(1, _LABEL_LINE_LOOKBACK + 1):
        j = line_idx - back
        if j < 0:
            break
        line = spans[j][2]
        if _DECIMAL_RE.search(line):
            continue
        toks = _tokens(dim, line)
        if not toks:
            continue
        common = frozenset.intersection(*(t[2] for t in toks))
        if common:
            # One heading, e.g. "Non-Winter (May–November)".
            return common
        ordinal = next(
            (i for i, m in enumerate(row_nums) if m.start() == num_start), None,
        )
        if ordinal is not None and len(row_nums) == len(toks):
            return toks[ordinal][2]
        return min(toks, key=lambda t: abs(t[0] - num_start))[2]
    return None


def _label_conflict(
    spans: list[tuple[int, int, str]],
    line_idx: int,
    num_start: int,
    num_end: int,
    labels: dict[str, str | None],
) -> str | None:
    """First dimension whose governing label contradicts the cell, if any."""
    line = spans[line_idx][2]
    for dim in ("period", "day_type", "season"):
        wanted = _wanted_values(dim, labels.get(dim))
        if wanted is None:
            continue
        gov = _governing_on_line(dim, line, num_start, num_end)
        if gov is None:
            gov = _governing_above(dim, spans, line_idx, num_start)
        if gov is not None and not (gov & wanted):
            return dim
    return None


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
        in_ranges = frozenset().union(*(t[2] for t in _month_range_tokens(broad)))
        if not (in_ranges & (_wanted_values("season", season) or frozenset())):
            return False, "label_not_in_row_col:season"

    if tier and tier not in {"all", "1"}:
        tier_aliases = _tier_aliases(tier)
        if not _context_has_label(broad, tier_aliases):
            return False, "label_not_in_row_col:tier"

    _ = component_name
    return True, "ok"


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
    (mapped back to real line offsets so row / column checks still see
    lines). PDF reflows often break a model quote across lines or reorder a
    header around the number; as a last resort an amount-anchored window
    that still contains most quote tokens is used.
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
    nums = [n for n in _NUMBER_RE.findall(collapsed_q) if "." in n]
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


def _amount_anchors(
    document_text: str,
    spans: list[tuple[int, int, str]],
    start: int,
    end: int,
    amount: str | Decimal,
    unit: str | None,
    context_units: set[str],
    reflow_lines: int = 2,
) -> list[tuple[int, int, int]] | None:
    """(line, col_start, col_end) of each printed figure matching ``amount``.

    Figures inside the quote win; otherwise the figure may sit within
    ``reflow_lines`` of the quote (PDF reflow, or another row of the cited
    table). ``None`` means the amount itself is unparseable.
    """
    q = document_text[start:end]
    wants = _amount_candidates(amount, unit, context_units, quote=q)
    if not wants:
        return None

    def _matches(text: str, wanted: list[Decimal]) -> list[re.Match[str]]:
        out = []
        for m in _NUMBER_RE.finditer(text):
            try:
                if Decimal(m.group(0)) in wanted:
                    out.append(m)
            except InvalidOperation:
                continue
        return out

    # In-quote figures first; nearby rows follow, so a figure the quote
    # shows under another label (same price, other season) can still be
    # grounded on its own row.
    anchors: list[tuple[int, int, int]] = []
    for m in _matches(q, wants):
        li = _line_index_at(spans, start + m.start())
        col = start + m.start() - spans[li][0]
        anchors.append((li, col, col + len(m.group(0))))

    first = _line_index_at(spans, start)
    last = _line_index_at(spans, max(start, end - 1))
    lo, hi = max(0, first - reflow_lines), min(len(spans), last + reflow_lines + 1)
    band = "\n".join(spans[i][2] for i in range(lo, hi))
    wants_band = _amount_candidates(amount, unit, context_units, quote=band)
    for li in range(lo, hi):
        for m in _matches(spans[li][2], wants_band):
            anchors.append((li, m.start(), m.end()))
    return list(dict.fromkeys(anchors))


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
    reflow_lines: int = 2,
) -> QuoteVerifyResult:
    if not quote or not str(quote).strip():
        return QuoteVerifyResult(False, "empty_quote")
    if document_text is None:
        return QuoteVerifyResult(False, "missing_document_text")

    doc, found = _find_quote_all(document_text, quote)
    if not found:
        return QuoteVerifyResult(False, "quote_not_found")

    # A quote can occur more than once (the same price in two seasons); any
    # occurrence that grounds amount, unit and labels is enough.
    first_failure: QuoteVerifyResult | None = None
    spans = _line_spans(doc)
    for start, end in found:
        result = _verify_at(
            doc, spans, start, end,
            unit=unit, require_unit=require_unit, amount=amount,
            labels={"season": season, "period": period, "day_type": day_type,
                    "tier": tier},
            component_name=component_name, require_row_col=require_row_col,
            reflow_lines=reflow_lines,
        )
        if result.ok:
            return result
        first_failure = first_failure or result
    assert first_failure is not None
    return first_failure


def _verify_at(
    document_text: str,
    spans: list[tuple[int, int, str]],
    idx: int,
    end: int,
    *,
    unit: str | None,
    require_unit: bool,
    amount: str | Decimal | None,
    labels: dict[str, str | None],
    component_name: str | None,
    require_row_col: bool,
    reflow_lines: int,
) -> QuoteVerifyResult:
    q = document_text[idx:end]
    line_idx = _line_index_at(spans, idx)
    quote_col = idx - spans[line_idx][0]
    context_units = _context_unit_family(document_text, spans, idx, len(q))

    anchors: list[tuple[int, int, int]] = [(line_idx, quote_col, quote_col)]
    if amount is not None and str(amount).strip() != "":
        found = _amount_anchors(
            document_text, spans, idx, end, amount, unit, context_units,
            reflow_lines=reflow_lines,
        )
        if found is None:
            return QuoteVerifyResult(False, "amount_unparseable", idx)
        if not found:
            return QuoteVerifyResult(False, "amount_not_in_quote", idx)
        anchors = found

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
                # The printed figure must be the converted one (9.8 for
                # 0.098), not the stored dollars amount in a cents column.
                printed = [
                    Decimal(spans[li][2][c0:c1]) for li, c0, c1 in anchors
                    if c1 > c0
                ]
                if converted and any(p in converted for p in printed):
                    unit_ok = True
                    context_units.add(sibling)
                    anchors = [
                        a for a, p in zip(
                            [a for a in anchors if a[2] > a[1]], printed,
                        )
                        if p in converted
                    ]
        if not unit_ok:
            return QuoteVerifyResult(False, "unit_not_in_context", idx)

    need_row_col = require_row_col or any(
        v and v != "all" for v in labels.values()
    ) or bool(component_name)
    if not need_row_col:
        return QuoteVerifyResult(True, "ok", idx)

    # Labels are checked where the figure is printed, not where the quote
    # starts, and the label governing that figure must not contradict them.
    reason = "ok"
    for li, col, col_end in anchors:
        if amount is not None and col_end > col:
            dim = _label_conflict(spans, li, col, col_end, labels)
            if dim:
                reason = f"label_conflict:{dim}"
                continue
        ok, reason = _row_col_labels_ok(
            spans, li, col,
            season=labels.get("season"),
            period=labels.get("period"),
            day_type=labels.get("day_type"),
            tier=labels.get("tier"),
            component_name=component_name,
        )
        if ok:
            return QuoteVerifyResult(True, "ok", idx)
    return QuoteVerifyResult(False, reason, idx)


def verify_component_quote(
    document_text: str,
    *,
    quote: str | None,
    unit: str | None,
    amount: str | Decimal | None = None,
    cell: dict[str, Any] | None = None,
    component_name: str | None = None,
    require_row_col: bool = False,
    reflow_lines: int = 2,
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
        reflow_lines=reflow_lines,
    )


def verify_component_cells(
    document_text: str,
    component: Any,
    *,
    require_row_col: bool = True,
) -> QuoteVerifyResult:
    """Ground **every** cell of a component, not just one.

    Each cell's amount must be printed under labels that fit that cell: in
    the quote, or — for a multi-cell component whose single quote cites one
    row — elsewhere in the cited table (``_TABLE_LINES`` of the quote). A
    cell may carry its own ``source_quote``; otherwise the component quote
    is used.
    """
    cells = list(getattr(component, "cells", None) or [])
    name = getattr(component, "name", None) or getattr(component, "code", None)
    quote = getattr(component, "source_quote", None)
    unit = getattr(component, "unit", None)
    if not cells:
        return verify_component_quote(
            document_text, quote=quote, unit=unit,
            component_name=name, require_row_col=False,
        )
    for i, cell in enumerate(cells):
        own_quote = cell.get("source_quote")
        result = verify_component_quote(
            document_text,
            quote=own_quote or quote,
            unit=unit,
            amount=cell.get("amount"),
            cell=cell,
            component_name=name,
            require_row_col=require_row_col,
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
    "verify_component_cells",
    "verify_component_quote",
    "verify_quote",
]
