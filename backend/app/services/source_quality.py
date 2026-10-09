"""R21 fix 5: official tariff over retail offers and marketing pages.

General rules (no per-utility lists):

* ``is_retail_offer`` — a competitive retailer's contract offer (fixed term
  "2-year Fixed", "24-month ... plan", "Guaranteed Rate Plan", electricity +
  natural-gas bundles, sign-up / offers / plan-selector pages). A retailer's
  offer is never the utility's tariff, so phase 4 drops it.
* ``marketing_rounded_price`` — a price read from a marketing web page
  (rate-plan overview, "save money", translated copy, blog) whose per-kWh
  prices are all rounded to 0.1 cent. Official tariffs quote $/kWh to 4+
  decimals; a rounded marketing price is not the full tariff price, so the
  plan is flagged and not counted Mysa-complete. Official rate pages that
  legitimately publish 0.1-cent prices (Ontario RPP, co-op /rates pages) are
  NOT marketing paths and are left alone.
"""
from __future__ import annotations

import re
from typing import Any, Iterable
from urllib.parse import urlparse

_TERM_RE = re.compile(
    r"\b(?:\d{1,2}|one|two|three|four|five)\s*[-\s]?\s*(?:years?|yrs?|months?|mo)\b",
    re.I,
)
_CONTRACT_WORD_RE = re.compile(
    r"\b(?:fixed|guarantee[d]?|price[\s-]*lock|locked|term|contract|floating|variable)\b", re.I,
)
_RETAIL_NAME_RE = re.compile(
    r"guaranteed\s+rate\s+plan|floating\s+rate\s+plan|price[\s-]*lock|"
    r"bundle\b.*\b(?:natural\s+)?gas|electricity\s*\+\s*(?:natural\s+)?gas",
    re.I,
)
_RETAIL_PATH_RE = re.compile(
    r"/offers?(?:/|$)|switch[-_]?(?:today|now|provider)?|plan[-_]?selector|"
    r"/sign[-_]?up|/enroll|/providers?/|/shop(?:/|$)|/compare",
    re.I,
)
_TARIFF_SCHEDULE_RE = re.compile(r"\b(?:schedule|rate\s+(?:no\.?|#)|tariff|rider)\b", re.I)


def is_retail_offer(name: str, url: str = "", description: str = "") -> bool:
    """A competitive retailer's contract offer, not a utility tariff."""
    nm = str(name or "")
    if _TARIFF_SCHEDULE_RE.search(nm):
        return False  # "Schedule R", "Rate No. 1" — a tariff
    path = urlparse(str(url or "")).path or ""
    if _RETAIL_NAME_RE.search(nm):
        return True
    term = bool(_TERM_RE.search(nm)) and bool(_CONTRACT_WORD_RE.search(nm) or re.search(r"\bplan\b", nm, re.I))
    if term:
        return True
    if _RETAIL_PATH_RE.search(path) and _CONTRACT_WORD_RE.search(f"{nm} {description or ''}"):
        return True
    return False


_MARKETING_PATH_RE = re.compile(
    r"save[-_]?money|pricing[-_]?plans|rate[-_]?plan[-_]?options|"
    r"residential[-_]?rate[-_]?plans|time[-_]?based[-_]?rate[-_]?plans|"
    r"/blogs?/|/news/|/articles?/|/insider|/learn/|/innovation/|plan[-_]?selector|"
    r"/offers?/|/compare",
    re.I,
)
_LOCALE_SEG_RE = re.compile(r"^/(?:[a-z]{2}(?:-[a-z]{2})?)/", re.I)
_LOCALE_SKIP = {"/en/", "/us/", "/ca/"}


def is_marketing_page(url: str) -> bool:
    u = str(url or "")
    p = urlparse(u)
    path = p.path or ""
    if path.lower().split("?")[0].endswith(".pdf"):
        return False
    if _MARKETING_PATH_RE.search(path):
        return True
    m = _LOCALE_SEG_RE.match(path)
    if m and m.group(0).lower() not in _LOCALE_SKIP and re.search(r"rate|plan|pric", path, re.I):
        return True  # translated copy of a rate-plan page (sce.com/fr/...)
    return False


def _g(c: Any, k: str):
    return c.get(k) if isinstance(c, dict) else getattr(c, k, None)


def _ctype(c: Any) -> str:
    v = _g(c, "component_type")
    v = getattr(v, "value", v)
    return str(v or "").lower()


def marketing_rounded_price(url: str, components: Iterable[Any] | None) -> bool:
    """Marketing page and every per-kWh ENERGY price rounded to 0.1 cent."""
    if not is_marketing_page(url):
        return False
    vals = []
    for c in components or []:
        if _ctype(c) != "energy":
            continue
        try:
            v = float(_g(c, "rate_value"))
        except (TypeError, ValueError):
            continue
        if v > 0:
            vals.append(v)
    if not vals:
        return False
    return all(abs(v * 1000 - round(v * 1000)) < 1e-6 for v in vals)
