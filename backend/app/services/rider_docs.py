"""R22: read a rider's current residential per-kWh amount from its document,
deterministically (no LLM). Returns None unless the document states it
unambiguously; callers then fall back to the LLM rider-mode extract.

Recognised shapes (all seen on official rider sheets):
* "Secondary Distribution customers ....... 3.8069¢ per kWh", grouped under
  season headings ("June through September:")  — Georgia FCR
* "E. Secondary Voltage   1.00866   0.018521" under a "($/kWh)" column — El Paso FFF
* "Current Annual FARSec   =   ($0.00021)" — Evergy FAC
* a "Residential" row block whose "Total" line ends in the total $/kWh — Evergy DSIM
* "13.0205% of their base bill" — percent-of-base riders (Georgia ECCR / DSM-R)
"""
from __future__ import annotations

import re

_VAL_RE = re.compile(
    r"(?P<neg>\()?\s*(?P<minus>-)?\s*(?:(?P<dollar>\$)\s*(?P<d>\d*\.\d{3,6})|(?P<c>\d+\.\d{2,5})\s*(?:¢|cents?\b))\s*\)?"
)
_BARE_RE = re.compile(r"(?P<neg>\()?(?P<minus>-)?(?P<v>0?\.\d{4,6})\)?(?!\d)")
_SEASON_HEAD_RE = re.compile(
    r"^\s*((?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+(?:through|thru|to|-|–)\s+"
    r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*|summer|winter|non[\s-]*summer)\b[^0-9$¢]*:?\s*$",
    re.I,
)
_HIGH_V = re.compile(r"\bprimary\b|transmission|sub[\s-]*transmission", re.I)
_PCT_RE = re.compile(r"(\d{1,2}\.\d{2,5})\s*%\s+of\s+(?:their|the|its|all)?\s*base\s+(?:bill|revenue|charge)", re.I)


def _values(line: str, dollar_column: bool = False) -> list[float]:
    out = []
    for m in _VAL_RE.finditer(line):
        if m.group("d"):
            v = float(m.group("d"))
        else:
            v = float(m.group("c")) / 100.0
        if m.group("neg") or m.group("minus"):
            v = -v
        out.append(v)
    if not out and dollar_column:
        for m in _BARE_RE.finditer(line):
            v = float(m.group("v"))
            out.append(-v if (m.group("neg") or m.group("minus")) else v)
    return out


def _season_for(lines: list[str], i: int) -> str | None:
    for j in range(i - 1, max(-1, i - 8), -1):
        m = _SEASON_HEAD_RE.match(lines[j])
        if m:
            return m.group(1).strip()
    return None


def parse_rider_text(text: str) -> dict | None:
    """{'per_kwh': [{'rate_value', 'season', 'label'}], 'pct': float|None} or None."""
    mt = parse_monthly_factor_table(text)
    if mt:
        return mt
    # "... increased by 0.1765 cents per kilowatt-\nhour." (wrapped statement)
    text = re.sub(r"(?i)kilowatt-\s*\n\s*hour", "kilowatt-hour", text or "")
    text = re.sub(r"(?i)\b(cents|¢)\s*\n\s*(per\s+kilowatt)", r"\1 \2", text)
    lines = [ln for ln in text.splitlines()]
    # Per-schedule rider tables ("Schedules 1, 1G, 1P ... 1.2730¢/kWh"): return
    # every row tagged with its schedule codes; the fold picks the plan's row.
    sched_rows = []
    for i, ln in enumerate(lines):
        m = re.match(r"^\s*(?:rate\s+)?schedules?\s+(.+?)\s+(?=\(?-?[$\d.]+\s*(?:¢|cents?)?\s*/?\s*kwh|\(?-?\$?\d*\.\d+\s*¢)", ln, re.I)
        if not m:
            continue
        kwh = re.search(r"(\()?(-)?\s*(?:\$\s*(\d*\.\d{3,6})|(\d+\.\d{2,5})\s*¢)\s*\)?\s*/?\s*(?:kwh)?", ln[m.end():], re.I)
        if not kwh:
            continue
        v = float(kwh.group(3)) if kwh.group(3) else float(kwh.group(4)) / 100.0
        if kwh.group(1) or kwh.group(2):
            v = -v
        codes = [c.strip().upper() for c in re.split(r",|\band\b", re.sub(r"\(.*?\)", "", m.group(1))) if c.strip()]
        sched_rows.append({"rate_value": round(v, 8), "season": _season_for(lines, i), "schedules": codes,
                           "label": ln.strip()[:80]})
    if len(sched_rows) >= 2:
        return {"per_kwh": sched_rows, "pct": None, "by_schedule": True}
    pcts = {float(m.group(1)) for m in _PCT_RE.finditer(text or "")}
    per: list[dict] = []
    col_dollar_until = -1
    for i, ln in enumerate(lines):
        if re.search(r"\(\s*\$\s*/\s*kwh\s*\)|\$/kwh", ln, re.I) and not _values(ln):
            col_dollar_until = i + 25
        low = ln.lower()
        if _HIGH_V.search(ln) and not re.search(r"\bsecondary\b", low):
            continue
        tier = None
        if re.search(r"\bsecondary\b", low):
            tier = "secondary"
        elif re.search(r"current\s+annual\s+far\s*sec|annual\s+.*\bsec(?:ondary)?\b", low):
            tier = "secondary"
        if tier is None:
            continue
        if re.search(r"\bschedules?\s+\w", ln, re.I) and not re.search(r"residential", ln, re.I):
            continue  # "Schedule 10 (Secondary)" is another class's row
        vals = _values(ln, dollar_column=i <= col_dollar_until)
        if not vals:
            continue
        per.append({"rate_value": vals[-1], "season": _season_for(lines, i), "label": ln.strip()[:80]})
    # "Residential" row block with a Total line (multi-line table cells), or a
    # one-value "01 Residential Service Rate  $0.000687" class row.
    for i, ln in enumerate(lines):
        if re.search(r"(?<!non-)(?<!non )\bresidential\b", ln, re.I) and not re.search(r"non[\s-]*residential", ln, re.I):
            vals = _values(ln)
            if len(vals) == 1 and re.search(r"residential\s+(?:service|rate|class)|^\W*\d{1,3}\W", ln, re.I):
                per.append({"rate_value": vals[0], "season": _season_for(lines, i), "label": ln.strip()[:80]})
                continue
            for j in range(i + 1, min(len(lines), i + 4)):
                if re.match(r"^\s*(?:service\s+)?total\b", lines[j], re.I) and _values(lines[j]):
                    per.append({"rate_value": _values(lines[j])[-1], "season": None, "label": "Residential total"})
                    break
    if not per and not pcts:
        stmt = [(_values(ln)[0], i) for i, ln in enumerate(lines)
                if re.search(r"per\s+kilowatt[\s-]*hour|per\s+kwh|/\s*kwh", ln, re.I)
                and re.search(r"increased|decreased|charge|factor|rate", ln, re.I) and len(_values(ln)) == 1]
        if len({round(v, 8) for v, _ in stmt}) == 1:
            per.append({"rate_value": stmt[0][0], "season": None, "label": "single rider amount"})
    by_season: dict = {}
    for p in per:
        by_season.setdefault(p["season"], set()).add(round(p["rate_value"], 6))
    if any(len(v) != 1 for v in by_season.values()):
        return None  # conflicting amounts → never guess
    if len(by_season) > 1 and None in by_season:
        return None
    if len(pcts) > 1:
        return None
    if not by_season and not pcts:
        return None
    return {
        "per_kwh": [{"rate_value": next(iter(v)), "season": s} for s, v in by_season.items()],
        "pct": next(iter(pcts)) if pcts else None,
    }


def rider_components(parsed: dict, label: str) -> list[dict]:
    """ADJUSTMENT rows ($/kWh or % of base) for a rider-only ExtractedTariff."""
    rows = []
    for p in parsed.get("per_kwh") or []:
        row = {"component_type": "adjustment", "unit": "$/kWh", "rate_value": p["rate_value"],
               "season": p["season"], "period_label": f"{label} - Secondary/Residential"}
        if p.get("schedules"):
            row["applies_to_schedules"] = p["schedules"]
            row["period_label"] = f"{label} - Schedules {', '.join(p['schedules'])}"[:120]
        rows.append(row)
    if parsed.get("pct") is not None:
        rows.append({"component_type": "adjustment", "unit": "% of base bill", "rate_value": parsed["pct"],
                     "period_label": f"{label} percentage of base bill"})
    return rows


def find_rider_sections(text: str, heading_re: re.Pattern, window: int = 70) -> list[str]:
    """Windows of a tariff book that start at a heading naming the rider
    (``heading_re`` should be case-sensitive UPPERCASE, i.e. a sheet title)."""
    lines = (text or "").splitlines()
    out = []
    for i, ln in enumerate(lines):
        if heading_re.search(ln):
            out.append("\n".join(lines[i:i + window]))
    return out


_STOP = {"rider", "schedule", "rate", "no", "the", "and", "of", "for", "tx", "residential", "service",
         "amount", "amounts", "charge", "charges", "factor", "clause", "recovery", "cost", "adjustment", "incl"}
_YEAR_RE = re.compile(r"(?<!\d)(20\d{2})(?!\d)")


def _hint_tokens(hint: str) -> tuple[set[str], set[str]]:
    """(code tokens, word tokens) from a rider hint."""
    h = re.sub(r"[()]", " ", str(hint or "")).lower()
    codes = set(re.findall(r"\bno\.?\s*(\d{1,3}[a-z]?)\b", h))
    codes |= {t for t in re.findall(r"\b([a-z]{2,5}(?:-[a-z0-9]{1,3})?)\b", h)
              if t.upper() == t.upper() and t in re.findall(r"\b[a-z]{2,5}(?:-[a-z0-9]{1,3})?\b", h)
              and re.search(rf"\b{re.escape(t.upper())}\b", str(hint or ""))}
    words = {w for w in re.findall(r"[a-z]{3,}", h) if w not in _STOP}
    allw = [w for w in re.findall(r"[a-z]+", re.sub(r"\bno\.?\s*\d+\w*", " ", h)) if w not in {"rate", "schedule", "no", "the", "of", "and", "tx"}]
    if len(allw) >= 3:
        codes.add("".join(w[0] for w in allw))  # "Energy Efficiency Cost Recovery Factor" -> eecrf
    return codes, words


def rider_link_candidates(hint: str, links: list[str], limit: int = 2, *, year: int | None = None) -> list[str]:
    """Links (already harvested from the utility's own pages) that name this
    rider: an explicit code ("No. 98" → schedule-98, "FCR", "ECCR") or at
    least two distinctive words of its title in the URL. Newest year first."""
    from datetime import date

    from app.services.price_basis import _keys

    codes, words = _hint_tokens(hint)
    fam = _keys(hint)
    if not codes and len(words) < 2 and not fam:
        return []
    this_year = str(year or date.today().year)
    scored = []
    for url in dict.fromkeys(links or []):
        path = re.sub(r"%20|[_\-./]+", " ", url.lower().split("?")[0].split("://", 1)[-1])
        toks = set(path.split())
        code_hit = any(c.lower() in toks or f"schedule {c}" in path for c in codes)
        word_hits = sum(1 for w in words if w in toks)
        # current-year factor sheets ("bill-calculation-factors-2026.pdf") carry
        # the amounts for formula-only riders (fuel / energy cost recovery)
        factor_sheet = bool(fam & {"fuel", "power_cost"}) and "factors" in toks and this_year in toks
        if not code_hit and word_hits < 2 and not factor_sheet:
            continue
        if not re.search(r"\.pdf\b|tariff|rider|schedule|rate", url, re.I):
            continue
        years = [int(y) for y in _YEAR_RE.findall(url)]
        scored.append((-(int(code_hit) * 3 + word_hits), -(max(years) if years else 0), url))
    scored.sort()
    return [u for *_, u in scored[:limit]]


_MONTH_NAMES = ["january", "february", "march", "april", "may", "june", "july", "august",
                "september", "october", "november", "december"]


def parse_monthly_factor_table(text: str) -> dict | None:
    """A 12-month factor table with a secondary-voltage column ("SEC.") in
    mills or cents per kWh (Alabama Power "Bill Calculation Factors").
    Months with equal values are grouped into seasons ("June-September")."""
    lines = (text or "").splitlines()
    unit = None
    if re.search(r"mills?\s+per\s+kwh|mills?/kwh", text or "", re.I):
        unit = 1000.0
    elif re.search(r"cents?\s+per\s+kwh|¢\s*/\s*kwh", text or "", re.I):
        unit = 100.0
    if unit is None:
        return None
    pos = None
    for ln in lines:
        if "|" in ln:
            continue
        m = re.search(r"\bSEC(?:ONDARY|\.)?(?=\s|$)(.*)$", ln)
        if m:
            pos = 1 + len(m.group(1).split())
            break
    if not pos:
        return None
    vals: dict[int, float] = {}
    for ln in lines:
        if "|" in ln:
            continue
        m = re.match(r"^\s*([A-Za-z]+)\b(.*)$", ln)
        if not m or m.group(1).lower() not in _MONTH_NAMES:
            continue
        nums = re.findall(r"-?\d+\.\d+", m.group(2))
        if len(nums) < pos:
            continue
        mi = _MONTH_NAMES.index(m.group(1).lower()) + 1
        v = round(float(nums[-pos]) / unit, 6)
        if mi in vals and vals[mi] != v:
            return None
        vals[mi] = v
    if len(vals) != 12:
        return None
    groups: list[list] = []  # [value, [months]] in calendar order, wrap-around merged
    for mi in range(1, 13):
        if groups and groups[-1][0] == vals[mi]:
            groups[-1][1].append(mi)
        else:
            groups.append([vals[mi], [mi]])
    if len(groups) > 1 and groups[0][0] == groups[-1][0]:
        groups[0][1] = groups[-1][1] + groups[0][1]
        groups.pop()
    if len({g[0] for g in groups}) != len(groups):
        return None  # same value in non-adjacent seasons — keep it simple, no guess
    cap = [m.capitalize() for m in _MONTH_NAMES]
    per = []
    for v, ms in groups:
        season = None if len(groups) == 1 else f"{cap[ms[0] - 1]}-{cap[ms[-1] - 1]}"
        per.append({"rate_value": v, "season": season})
    return {"per_kwh": per, "pct": None}


# --- R22c: "Exhibit of Applicable Riders" (Dominion VA) ---------------------

EXHIBIT_HINT_RE = re.compile(r"(?i)\b(?:exhibit\s+of\s+)?applicable\s+riders\b")
_EXHIBIT_SECTION_RE = re.compile(r"(?m)^(I{1,3}|IV|VI{0,3}|IX|X)\.\s+(The riders listed below.*)$")
_EXHIBIT_ROW_RE = re.compile(r"^([A-Z][A-Z0-9]{0,4})(?:\s+\|)?\s+(\S.*)$")
_DATE_RE = re.compile(r"\b\d{2}-\d{2}-\d{2,4}\b")


def _schedule_list(text: str) -> set[str]:
    text = re.sub(r"\b(GS|DP)-\s+(\d)", r"\1-\2", text)
    return {m.upper() for m in re.findall(r"\b([A-Z]{0,3}-?\d{1,2}[A-Z]{0,3}|[A-Z]{2,4}(?:-[A-Z0-9]+)?)\b", text)}


def parse_applicable_riders_exhibit(text: str) -> list[dict]:
    """Sections of an 'Exhibit of Applicable Riders' that say the listed riders
    ARE applicable to named rate schedules, plus non-bypassable sections.

    Returns [{"schedules": {"1", "1G", ...}, "riders": [(code, description)],
    "non_bypassable": bool}]. Circumstantial 'may apply' sections (economic
    development, green programs ...) are never returned. Non-bypassable
    'may apply' riders are returned flagged: the caller includes one only
    when its own sheet says it applies to all retail customers."""
    if not re.search(r"(?i)EXHIBIT\s+OF\s+APPLICABLE\s+RIDERS", text or ""):
        return []
    heads = list(_EXHIBIT_SECTION_RE.finditer(text))
    out = []
    for i, m in enumerate(heads):
        end = heads[i + 1].start() if i + 1 < len(heads) else len(text)
        body = text[m.start():end]
        header, _, table = body.partition("Rider Description")
        nonbyp = bool(re.search(r"(?i)\bnon-?bypassable\b", header))
        if not table:
            continue
        if re.search(r"(?i)\bmay\s+apply\b", header) and not nonbyp:
            continue  # circumstantial riders (economic development, green programs ...)
        if not nonbyp and not re.search(r"(?i)\bare\s+applicable\b", header):
            continue
        sched_txt = re.split(r"(?i)\bas well as\b|\.\s*$", header.split("Schedules", 1)[-1])[0] if not nonbyp else ""
        riders, seen = [], set()
        for ln in table.splitlines():
            ln = ln.strip()
            if re.match(r"(?i)^\(continued\)|^filed\b", ln):
                break
            rm = _EXHIBIT_ROW_RE.match(ln)
            if not rm or rm.group(1) in seen:
                continue
            desc = _DATE_RE.sub("", rm.group(2))
            desc = re.sub(r"(?i)\b(through and|including)\b|\|", " ", desc)
            desc = re.sub(r"(?i)(?:^|\s+)(electricity supply|distribution|non-?bypassable)\s*$", "", desc.strip())
            seen.add(rm.group(1))
            riders.append((rm.group(1), re.sub(r"\s+", " ", desc).strip()))
        if riders:
            out.append({"schedules": _schedule_list(sched_txt), "riders": riders, "non_bypassable": nonbyp})
    return out


UNIVERSAL_NONBYPASSABLE_RE = re.compile(r"(?i)applicable\s+to\s+all\s+retail\s+customers\b[\s\S]{0,120}?non-?bypassable")
# Generic rider hints an exhibit expansion answers (no family of their own).
GENERIC_EXHIBIT_HINT_RE = re.compile(
    r"(?i)^\W*(?:(?:applicable\s+)?riders?(?:\s+amounts?)?(?:\s*\(exhibit of applicable riders\))?|non-?bypassable\s+charges?)\W*$")


def exhibit_rider_url(code: str, links: list[str], exhibit_url: str = "") -> tuple[str, bool]:
    """URL of rider ``code``'s own sheet: a site link named rider-<code>.pdf,
    else the sibling of the exhibit PDF (returned with verified=False — the
    caller must confirm the fetched sheet is headed RIDER <CODE>)."""
    pat = re.compile(rf"(?i)/rider[-_]{re.escape(code)}\.pdf(?:$|\?)")
    for u in links or []:
        if pat.search(u or ""):
            return u, True
    if exhibit_url and "/" in exhibit_url:
        return exhibit_url.rsplit("/", 1)[0] + f"/rider-{code.lower()}.pdf", False
    return "", False


def is_rider_sheet(text: str, code: str) -> bool:
    return bool(re.search(rf"(?m)^\s*(?:[A-Z ]+\s)?RIDER\s+{re.escape(code)}\b", (text or "")[:600]))
