"""R23: reach the rate schedules behind a utility's own tariff index.

Pure helpers (no I/O) used by phase 2 / PDF extraction:

* ``is_document_url`` — rate documents served without a ``.pdf`` suffix
  (Sitecore ``.ashx`` media handlers — PSE&G's B.P.U.N.J. No. 17 tariff —
  ``/download`` handlers, ``?file=`` links).
* ``same_owner`` — same registrable domain, or the utility's own CDN path
  (ConEd serves its PSC 10 tariff from ``*.azurefd.net/-/media/files/coned/``).
* ``prioritize_tariff_links`` — on a tariff index, the utility's own
  residential rate-schedule documents first, then the complete current tariff
  book and tariff sub-index pages, then everything else in its old order.
  A year in the URL never pushes a current schedule behind navigation pages
  (CPS links its current schedules as ``2024_Rate_ResidentialElectric.pdf``;
  R20's newest-first rule sent them past the level-1 cap).
* ``residential_schedule_pages`` — page numbers of residential rate-schedule
  sheets inside a whole tariff book (LIPA SC No. 1 at page 273 of 534, ConEd
  SC 1 at page 617 of 760), so a book longer than the page cap is not cut
  off before its residential section.
* ``residential_schedule_anchors`` — offsets of those sections in text.
"""
from __future__ import annotations

import re
from urllib.parse import urlparse

_DOC_EXT_RE = re.compile(r"\.(?:pdf|ashx|docx?)$", re.I)
_DOC_QUERY_RE = re.compile(r"(?:^|&)(?:file|filename|document|doc|attachment)=", re.I)
_DOC_PATH_HINT_RE = re.compile(r"/(?:download|getmedia|getdocument|documentdownload)(?:/|$)", re.I)
_CDN_HOST_RE = re.compile(r"(?:azurefd\.net|azureedge\.net|cloudfront\.net|akamaized\.net|blob\.core\.windows\.net|"
                          r"ctfassets\.net|amazonaws\.com|sharepoint\.com)$", re.I)

TARIFF_WORD_RE = re.compile(
    r"tariff|rate\s*schedule|\bschedule\s+(?:of\s+)?rates?\b|\brates?\b|rider|service\s+classification|\bleaf\b|"
    r"b\.?\s*p\.?\s*u\.?\s*n\.?\s*j|p\.?\s*s\.?\s*c\.?\s*(?:no|#)|i\.?\s*c\.?\s*c\.?\s*no|statement\s+of|price\s+schedule",
    re.I)
RESIDENTIAL_WORD_RE = re.compile(
    r"residential|domestic|\bRS\b|\bres[_-]|_res\b|service\s+classification\s+(?:no\.?\s*)?1\b(?!\d)|\bsc[-_ ]?1\b(?!\d)",
    re.I)
COMPLETE_TARIFF_RE = re.compile(
    r"(?:current|complete|full|entire)\s+(?:\w+\s+){0,2}tariff|electric\s+tariff|tariff\s+for\s+electric|"
    r"rate\s*book|p\.?\s*s\.?\s*c\.?\s*(?:no\.?|#)?\s*\d+|b\.?\s*p\.?\s*u\.?\s*n\.?\s*j\.?\s*no",
    re.I)
NOT_RESIDENTIAL_DOC_RE = re.compile(
    r"\bgas\b|commercial|industrial|large\s+(?:power|lighting|volume|general)|general\s+service|lighting|"
    r"wholesale|interconnect|purpa|cogenerat|qualifying\s+facilit|avoided\s+cost|net[-_ ]?meter|feed[-_ ]?in|"
    r"historical|archive|superseded|cancell?ed|pending|proposed|rate[-_ ]?case|"
    r"value\s+stack|vder|demand\s+response|telecommunications|trench|underground\s+fac|"
    r"terms\s+and\s+conditions|rules\s*(?:and|&)\s*reg",
    re.I)


_OLD_OR_PENDING_RE = re.compile(r"\b(?:prior|historical|archived?|superseded|pending|proposed)\b", re.I)
_GAS_ONLY_RE = re.compile(r"\bgas\b(?!.*electric)", re.I)


def _host(u: str) -> str:
    return (urlparse(u).netloc or "").lower().split(":")[0]


def _registrable(host: str) -> str:
    parts = [p for p in host.split(".") if p]
    if len(parts) >= 3 and len(parts[-1]) == 2 and parts[-2] in {"co", "com", "org", "net", "gov", "qc", "on", "bc", "ab"}:
        return ".".join(parts[-3:])
    return ".".join(parts[-2:])


_SFDC_DIST_RE = re.compile(r"^https://([a-z0-9-]+)\.my\.salesforce\.com/sfc/p/[^/]+(/a/[A-Za-z0-9]+/[^/?#]+)$", re.I)


def salesforce_distribution(url: str) -> tuple[str, str] | None:
    """(subdomain, '/a/<id>/<token>') for a Salesforce public content
    distribution link (Xcel hosts its rate books there), else None."""
    m = _SFDC_DIST_RE.match((url or "").strip())
    return (m.group(1).lower(), m.group(2)) if m else None


def salesforce_download_url(url: str, viewer_html: str) -> str | None:
    """Direct file URL for a distribution link, from its rendered viewer
    (which carries the org id and content version id)."""
    from urllib.parse import quote

    sd = salesforce_distribution(url)
    vid = re.search(r"versionId=(068[A-Za-z0-9]{12,15})", viewer_html or "")
    oid = re.search(r"\b(00D[A-Za-z0-9]{12,15})\b", viewer_html or "")
    if not (sd and vid and oid):
        return None
    return (f"https://{sd[0]}.my.salesforce.com/sfc/dist/version/download/?oid={oid.group(1)}"
            f"&ids={vid.group(1)}&d={quote(sd[1], safe='')}&operationContext=DELIVERY&asPdf=false")


def same_owner(url: str, base_url: str) -> bool:
    """Same registrable domain, a CDN path carrying the utility's brand, or
    the brand's own Salesforce content-distribution org (xcelnew.my...)."""
    h, b = _host(url), _host(base_url)
    if not h or not b:
        return False
    if _registrable(h) == _registrable(b):
        return True
    sd = salesforce_distribution(url)
    if sd:
        stem = _registrable(b).split(".")[0]
        return len(stem) >= 4 and sd[0].startswith(stem[:4])
    if _CDN_HOST_RE.search(h):
        stem = _registrable(b).split(".")[0]
        return len(stem) >= 4 and re.search(rf"/{re.escape(stem)}(?:/|[-_])", urlparse(url).path.lower()) is not None
    return False


def is_document_url(url: str, text: str = "") -> bool:
    p = urlparse(url)
    path = p.path or ""
    if _DOC_EXT_RE.search(path):
        return True
    if salesforce_distribution(url):
        return bool(re.search(r"\bpdf\b", text or "", re.I) or TARIFF_WORD_RE.search(text or ""))
    if _DOC_QUERY_RE.search(p.query or "") or _DOC_PATH_HINT_RE.search(path):
        return bool(TARIFF_WORD_RE.search(f"{text} {path}"))
    return False


def _blob(url: str, text: str) -> str:
    p = urlparse(url)
    return f"{text or ''} {p.path} {p.query}".replace("%20", " ")


def link_tier(url: str, text: str, base_url: str) -> int:
    """0 own residential rate document; 1 own complete current tariff book;
    2 own tariff sub-index page; 3 other own rate documents; 4 everything
    else (original order); 5 non-residential / historical documents."""
    blob = _blob(url, text)
    own = same_owner(url, base_url)
    doc = is_document_url(url, text)
    if _OLD_OR_PENDING_RE.search(blob):
        return 5
    if doc and NOT_RESIDENTIAL_DOC_RE.search(blob) and (
            not RESIDENTIAL_WORD_RE.search(text or "") or _GAS_ONLY_RE.search(blob)):
        return 5
    if not own:
        return 4
    tariffish = bool(TARIFF_WORD_RE.search(blob))
    if doc and tariffish and RESIDENTIAL_WORD_RE.search(blob):
        return 0
    if doc and COMPLETE_TARIFF_RE.search(blob):
        return 1
    if not doc and (COMPLETE_TARIFF_RE.search(blob) or re.search(r"tariffs?|rate[-_ ]?schedules?", urlparse(url).path, re.I)):
        return 2
    if doc and tariffish:
        return 3
    return 4


def prioritize_tariff_links(links: list[tuple[str, str]], base_url: str) -> list[tuple[str, str]]:
    """Stable sort by ``link_tier`` (original order kept inside each tier)."""
    return [lt for _, lt in sorted(enumerate(links), key=lambda il: (link_tier(il[1][0], il[1][1], base_url), il[0]))]


_RES_HEADING_RE = re.compile(
    r"SERVICE\s+CLASSIFICATION\s+(?:NO\.?|#)\s*1\b(?![\d.])|RATE\s+SCHEDULE\s+R-?S?\b|"
    r"RESIDENTIAL\s+(?:SERVICE|RATE|AND\s+RELIGIOUS)|SCHEDULE\s+(?:R|RS|D|DS)\b|DOMESTIC\s+SERVICE",
)
_AMOUNT_RE = re.compile(r"\$\s*\d*\.\d{3,}|\d*\.\d{3,}\s*(?:¢|cents)|\d+\.\d+\s*¢", re.I)
_KWH_RE = re.compile(r"kwh|kilowatt-?\s?hour", re.I)
_TOC_RE = re.compile(r"\.{6,}|table\s+of\s+contents", re.I)


def residential_schedule_pages(pages: list[str], *, limit: int = 16) -> list[int]:
    """0-based indexes of pages that hold a residential rate schedule with
    per-kWh amounts (plus the following page as a continuation)."""
    hits: list[int] = []
    for i, p in enumerate(pages):
        if len(hits) >= limit:
            break
        if not _RES_HEADING_RE.search(p) or not re.search(r"residential|domestic", p, re.I):
            continue
        if not (_AMOUNT_RE.search(p) and _KWH_RE.search(p)) or len(_TOC_RE.findall(p)) > 3:
            continue
        for j in (i, i + 1):
            if j < len(pages) and j not in hits:
                hits.append(j)
    return sorted(hits)[:limit]


def residential_schedule_anchors(text: str) -> list[int]:
    """Offsets of residential schedule headings followed (within 3000 chars)
    by per-kWh amounts — the sections phase 3 must see."""
    out = []
    for m in _RES_HEADING_RE.finditer(text or ""):
        near = text[m.start():m.start() + 3000]
        if _AMOUNT_RE.search(near) and _KWH_RE.search(near) and re.search(r"residential|domestic", near, re.I):
            if not out or m.start() - out[-1] > 1500:
                out.append(m.start())
    return out
