"""R24: pick the utility's own official starting page in phase 1.

Pure helpers (no network) used by ``scripts.tariff_pipeline.phase1_find_rate_page``:

* ``domain_match_strength`` — does a host belong to the utility?  Generic
  words ("power", "electric") and state names never count, so
  ``mnpower.com`` is not Northern States Power; acronyms (``smud.org``,
  ``sdge.com``) and title abbreviations ("Commonwealth Edison (ComEd)" →
  ``comed.com``) do.
* ``pick_discovered_domain`` — best official-looking domain from the
  "official website" search; government / city hosts are rejected unless
  the utility itself is a city department.
* ``infer_domain_from_candidates`` — when discovery fails, a non-third-party
  candidate whose title carries the utility's legal name and whose path is
  a rate / tariff document names the utility's domain (``xcelenergy.com``
  for "Northern States Power Co - Minnesota").
* ``candidate_tier`` / ``rerank_candidates`` — own-domain current pages
  first; supply / price-to-compare sheets and documents dated two or more
  years back after them; third-party "about this utility" pages last.
* ``official_tariff_hub`` — the operator's published tariff / rate-book
  index, keyed by the operator's registrable domain (so it serves every
  operating company on that domain), with a state slot where the site
  splits by state.
"""
from __future__ import annotations

import re
from datetime import date
from urllib.parse import unquote, urlparse

from app.services.source_type import (
    is_generic_host,
    is_government_host,
    is_third_party_host,
    normalize_host,
    registrable_domain,
)

US_STATES = {
    "AL": "alabama", "AK": "alaska", "AZ": "arizona", "AR": "arkansas", "CA": "california",
    "CO": "colorado", "CT": "connecticut", "DE": "delaware", "DC": "district of columbia",
    "FL": "florida", "GA": "georgia", "HI": "hawaii", "ID": "idaho", "IL": "illinois",
    "IN": "indiana", "IA": "iowa", "KS": "kansas", "KY": "kentucky", "LA": "louisiana",
    "ME": "maine", "MD": "maryland", "MA": "massachusetts", "MI": "michigan", "MN": "minnesota",
    "MS": "mississippi", "MO": "missouri", "MT": "montana", "NE": "nebraska", "NV": "nevada",
    "NH": "new hampshire", "NJ": "new jersey", "NM": "new mexico", "NY": "new york",
    "NC": "north carolina", "ND": "north dakota", "OH": "ohio", "OK": "oklahoma", "OR": "oregon",
    "PA": "pennsylvania", "RI": "rhode island", "SC": "south carolina", "SD": "south dakota",
    "TN": "tennessee", "TX": "texas", "UT": "utah", "VT": "vermont", "VA": "virginia",
    "WA": "washington", "WV": "west virginia", "WI": "wisconsin", "WY": "wyoming",
}
_STATE_WORDS = {w for n in US_STATES.values() for w in n.split()} | {c.lower() for c in US_STATES}

# Words that say nothing about *which* utility a host belongs to.
GENERIC_NAME_WORDS = frozenset({
    "power", "electric", "elec", "electricity", "energy", "utility", "utilities", "util",
    "light", "lt", "gas", "water", "service", "services", "public", "company", "co", "corp",
    "inc", "llc", "ltd", "cooperative", "coop", "co-op", "assn", "association", "district",
    "dist", "municipal", "city", "town", "village", "county", "department", "dept", "board",
    "commission", "authority", "of", "the", "and", "&", "for", "hydro", "rural", "member",
    "members", "system", "systems", "cities", "general", "new",
})
_SUFFIX_WORDS = {"co", "corp", "inc", "llc", "ltd", "company", "corporation", "incorporated",
                 "the", "of", "and", "&", "-", "for"}

# Third-party "about this utility" pages: rate-comparison paths keyed by state.
_AGGREGATOR_PATH_RE = re.compile(
    r"/(?:electricity|electric|energy|power)[-_](?:rates?|prices?|costs?|providers?)/(?:%s)(?:/|$)"
    % "|".join(re.escape(n.replace(" ", "-")) for n in US_STATES.values()),
    re.I,
)
_DIRECTORY_PATH_RE = re.compile(r"/(?:providers?|utility[-_]companies|utilities|companies)/[^/]+", re.I)
_TARIFF_PATH_RE = re.compile(
    r"tariff|rate[-_ %20]*book|rates?[-_ %20]*(?:and|&)[-_ %20]*(?:regulations|tariffs|rules)|"
    r"rate[-_ %20]*schedule|/rates?(?:[-_/.]|$)|regulatory|ratebook|price[-_]?schedule",
    re.I,
)
_SUPPLY_PATH_RE = re.compile(
    r"price[-_ %20]*to[-_ %20]*compare|\bptc\b|customer[-_]choice/[^/]+/price|supply[-_ %20]*(?:price|rate|charge)s?|"
    r"bgs[-_ %20]*(?:rscp|ptc|price)|default[-_ %20]*service[-_ %20]*(?:price|rate)",
    re.I,
)
_YEAR_RE = re.compile(r"(?<!\d)(20[0-3]\d|19[89]\d)(?!\d)")
_MONTH_YEAR_RE = re.compile(
    r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*[-_ %20]*(20[0-3]\d)", re.I,
)

# Operator tariff / rate-book index pages, keyed by registrable domain.
# ``{slug}`` is the state name lower-cased with underscores.  Only states
# whose page was checked are listed for templated hubs.
OFFICIAL_TARIFF_HUBS: dict[str, dict] = {
    "xcelenergy.com": {
        "url": "https://www.xcelenergy.com/company/rates_and_regulations/rates/rate_books",
        "states": None,  # one page; the state is a cookie (set before phase 2)
    },
    "firstenergycorp.com": {
        "url": "https://www.firstenergycorp.com/customer_choice/{slug}/{slug}_tariffs.html",
        "states": {"NJ", "PA", "MD", "WV"},
    },
    "comed.com": {
        "url": "https://www.comed.com/current-rates-tariffs",
        "states": None,
    },
}


def _words(text: str) -> list[str]:
    return [w for w in re.split(r"[^a-z0-9&]+", (text or "").lower()) if w]


def name_tokens(utility_name: str) -> list[str]:
    """All words of the name minus corporate suffixes / parentheticals."""
    name = re.sub(r"\([^)]*\)", " ", utility_name or "")
    return [w for w in _words(name) if w not in _SUFFIX_WORDS]


def distinctive_tokens(utility_name: str) -> list[str]:
    """Name words that identify *this* utility (not generic, not a state)."""
    return [w for w in name_tokens(utility_name)
            if len(w) >= 3 and w not in GENERIC_NAME_WORDS and w not in _STATE_WORDS]


def name_acronyms(utility_name: str) -> set[str]:
    """Initialisms: "San Diego Gas & Electric" → sdge, "Arizona Public
    Service" → aps, "Los Angeles Department of Water & Power" → ladwp."""
    toks = [w for w in name_tokens(utility_name) if w != "&"]
    out = set()
    if len(toks) >= 2:
        out.add("".join(w[0] for w in toks))
    # Parenthetical aliases in the name itself ("Ottawa Hydro (Hydro Ottawa)").
    for alias in re.findall(r"\(([^)]*)\)", utility_name or ""):
        a = [w for w in _words(alias) if w not in _SUFFIX_WORDS]
        if len(a) >= 2:
            out.add("".join(w[0] for w in a))
        if a:
            out.add("".join(a))
    return {a for a in out if len(a) >= 3}


def title_abbreviations(title: str) -> set[str]:
    """"Commonwealth Edison (ComEd" → {"comed"}; "SDGE | San Diego…" → {"sdge"}."""
    out = set()
    for m in re.finditer(r"\(\s*([A-Za-z][A-Za-z&.\-]{1,14})", title or ""):
        out.add(re.sub(r"[^a-z]", "", m.group(1).lower()))
    m = re.match(r"\s*([A-Z][A-Za-z&]{1,9})\s*[|\-–:]", title or "")
    if m:
        out.add(re.sub(r"[^a-z]", "", m.group(1).lower()))
    return {a for a in out if len(a) >= 3}


def _host_labels(host: str) -> list[str]:
    reg = registrable_domain(host)
    base = reg.split(".")[0] if reg else host.split(".")[0]
    return [base, *re.split(r"[-_]", base)]


def domain_match_strength(host_or_url: str, utility_name: str, title: str = "") -> int:
    """2 = host clearly belongs to the utility, 1 = plausible, 0 = no."""
    host = normalize_host(host_or_url)
    if not host:
        return 0
    labels = _host_labels(host)
    base = labels[0]
    parts = labels[1:]
    toks = [w for w in name_tokens(utility_name) if w != "&"]
    dt = distinctive_tokens(utility_name)
    tl = (title or "").lower()
    acr = name_acronyms(utility_name)
    short_acr = "".join(w[0] for w in toks) if len(toks) >= 2 else ""
    if any(l in acr for l in labels) or (len(parts) > 1 and short_acr and short_acr in parts):
        return 2
    abbrevs = title_abbreviations(title)
    if any(l in abbrevs for l in labels) and (not dt or any(t in tl for t in dt)):
        return 2
    hits = [t for t in dt if t in base]
    if len(hits) >= 2 or (hits and len(dt) == 1):
        return 2
    for i in range(len(toks) - 1):  # alabamapower, georgiapower, pplelectric
        if len(toks[i]) >= 3 and toks[i] + toks[i + 1] in base:
            return 2
    if hits:
        return 1
    named = all(t in tl for t in dt) if dt else sum(t in tl for t in toks) >= 2
    if named and title and any(len(t) >= 4 and t in base for t in toks):
        return 1
    return 0


def _is_cityish_name(utility_name: str) -> bool:
    return bool(re.search(r"\b(city|town|village|borough|county|department|dept)\b", utility_name or "", re.I))


def _is_government_like(host: str) -> bool:
    base = registrable_domain(host).split(".")[0]
    return is_government_host(host) or bool(re.match(r"(cityof|townof|countyof|villageof|ci\.)", base))


def acceptable_utility_host(host_or_url: str, utility_name: str) -> bool:
    host = normalize_host(host_or_url)
    if not host or is_third_party_host(host) or is_generic_host(host):
        return False
    if _is_government_like(host) and not _is_cityish_name(utility_name):
        return False
    return True


def pick_discovered_domain(results: list[dict], utility_name: str) -> str | None:
    """Best host from an "official website" search, or None."""
    best = (0, None)
    for i, r in enumerate(results or []):
        url = r.get("url", "")
        host = normalize_host(url)
        if not acceptable_utility_host(host, utility_name):
            continue
        s = domain_match_strength(host, utility_name, r.get("title", ""))
        if s > best[0]:
            best = (s, host)
            if s == 2:
                break
    return best[1]


def infer_domain_from_candidates(results: list[dict], utility_name: str) -> str | None:
    """Host of a candidate that is evidently the utility's own document."""
    dt = distinctive_tokens(utility_name)
    full = " ".join(name_tokens(utility_name))
    for r in results or []:
        url, title = r.get("url", ""), r.get("title", "") or ""
        host = normalize_host(url)
        if not acceptable_utility_host(host, utility_name) or is_about_utility_page(url, utility_name):
            continue
        if domain_match_strength(host, utility_name, title) == 2:
            return host
        t = " ".join(_words(title))
        named = (full and full in t) or (len(dt) >= 2 and all(d in t for d in dt))
        if named and _TARIFF_PATH_RE.search(unquote(urlparse(url).path)):
            return host
    return None


def is_supply_page(url: str) -> bool:
    path = unquote(urlparse(url or "").path)
    if re.search(r"tariff|rate[-_ ]*book", path.rsplit("/", 1)[-1], re.I):
        return False
    return bool(_SUPPLY_PATH_RE.search(path))


def document_year(url: str) -> int | None:
    path = unquote(urlparse(url or "").path)
    years = [int(y) for y in _YEAR_RE.findall(path)] + [int(y) for y in _MONTH_YEAR_RE.findall(path)]
    return max(years) if years else None


def is_stale_document(url: str, today: date | None = None) -> bool:
    if known_stale_document(url):
        return True
    today = today or date.today()
    y = document_year(url)
    return y is not None and y <= today.year - 2


# R26 fixes 5-6: current official books to start from.
#  * PPL / PSE&G: the full current tariff carries both the delivery schedule
#    (RS) and the default-supply price (GSC-1 + TSC / BGS-RSCP) so the run can
#    build the "delivery + default supply" plan (Joshua's default 2). Their
#    file names change every month, so the newest matching link on the
#    official index page is used.
#  * Xcel MN: the current rate book (xe-responsive) instead of the static
#    2019 path; El Paso Electric (TX): the eff 08-01-2026 Schedule 01.
# Con Edison is not listed: its residential supply price is split across
# monthly MSC statements (capacity only on the public statement), so there is
# no single default-supply sheet to pair (R26 report).
PPL_TARIFF_INDEX = "https://www.pplelectric.com/site/more/about-us/electric-rates-and-rules/current-electric-tariff"
PSEG_TARIFF_INDEX = "https://nj.pseg.com/aboutpseg/regulatorypage/electrictariffs"
XCEL_MN_CURRENT_BOOK = (
    "https://www.xcelenergy.com/staticfiles/xe-responsive/Company/Rates%20&%20Regulations/Me_Section_5.pdf"
)
EPE_TX_SCHEDULE_01 = (
    "https://www.epelectric.com/el-paso-electric/uploads/regulatory/"
    "section-1-sheet-040-schedule-01-residential-service-rate-eff_08-01-2026.pdf"
)
CURRENT_BOOKS: list[dict] = [
    {"match": re.compile(r"\bppl electric utilities\b", re.I), "states": {"PA", None},
     "index": PPL_TARIFF_INDEX, "base": "https://www.pplelectric.com",
     "link": re.compile(r"/[^\"'<>\s]*Current-Electric-Tariff/20\d\d/[A-Za-z]+/[^\"'<>\s]*master[^\"'<>\s]*\.pdf", re.I),
     "why": "PPL master tariff: RS delivery + GSC-1 default supply + TSC"},
    {"match": re.compile(r"\bpublic service elec(?:tric)?\.? (?:&|and) gas\b|\bpse&g\b", re.I), "states": {"NJ", None},
     "index": PSEG_TARIFF_INDEX, "base": "https://nj.pseg.com",
     "link": re.compile(r"/-/media/pseg/public-site/documents/current-electric-tariff/electric-tariff-[^\"'<>\s]*?effective-(20\d{6})\.ashx", re.I),
     "why": "PSE&G full electric tariff: RS delivery + BGS-RSCP default supply"},
    {"match": re.compile(r"\bnorthern states power\b.*\bminnesota\b", re.I), "states": {"MN", None},
     "url": XCEL_MN_CURRENT_BOOK, "why": "Xcel MN current rate book (not the static 2019 path)"},
    {"match": re.compile(r"\bel paso electric\b", re.I), "states": {"TX", None},
     "url": EPE_TX_SCHEDULE_01, "why": "El Paso Electric TX Schedule 01 eff 08-01-2026"},
]
KNOWN_STALE_DOCUMENTS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"xcelenergy\.com/staticfiles/xe/Regulatory/Regulatory(?:%20|\s)PDFs/rates/MN/", re.I),
     "Xcel MN static rate book (last updated 2019)"),
    (re.compile(r"epelectric\.com/files/html/Rates_and_Regulatory/", re.I),
     "retired El Paso Electric tariff path"),
]


def known_stale_document(url: str | None) -> str | None:
    for rx, why in KNOWN_STALE_DOCUMENTS:
        if url and rx.search(unquote(url)) or (url and rx.search(url)):
            return why
    return None


def current_book_entry(utility_name: str | None, state: str | None = None) -> dict | None:
    st = (state or "").upper() or None
    for e in CURRENT_BOOKS:
        if e["match"].search(utility_name or "") and (st in e["states"]):
            return e
    return None


def _link_date_key(href: str) -> tuple:
    m = re.search(r"(20\d{2})(\d{2})(\d{2})", href)
    if m:
        return (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
    m = re.search(r"/(20\d{2})/([A-Za-z]+)/", href)
    if m and m.group(2)[:3].lower() in months:
        return (int(m.group(1)), months.index(m.group(2)[:3].lower()) + 1, 0)
    return (0, 0, 0)


def pick_current_book_link(entry: dict, index_html: str) -> str | None:
    """Newest link on the index page matching the entry's pattern (absolute)."""
    links = {m.group(0) for m in entry["link"].finditer(index_html or "")}
    if not links:
        return None
    best = max(links, key=lambda h: (_link_date_key(h), h))
    if best.startswith("http"):
        return best
    return entry["base"].rstrip("/") + "/" + best.lstrip("/")


def resolve_current_book(utility_name: str | None, state: str | None, fetch) -> tuple[str, str] | None:
    """(url, why) of the current official book for this utility, or None.
    ``fetch(url) -> html`` is injected (no network in this module)."""
    e = current_book_entry(utility_name, state)
    if not e:
        return None
    if e.get("url"):
        return e["url"], e["why"]
    try:
        html = fetch(e["index"]) or ""
    except Exception:
        return None
    link = pick_current_book_link(e, html)
    return (link, e["why"]) if link else None


def is_about_utility_page(url: str, utility_name: str, utility_domain: str | None = None) -> bool:
    """Third-party page *about* the utility: comparison paths keyed by
    state, provider directories, or a path naming the utility on a host
    that is not the utility's."""
    host = normalize_host(url)
    if not host:
        return False
    if utility_domain and registrable_domain(host) == registrable_domain(normalize_host(utility_domain)):
        return False
    if domain_match_strength(host, utility_name) == 2:
        return False
    path = unquote(urlparse(url).path).lower()
    if _AGGREGATOR_PATH_RE.search(path) or _DIRECTORY_PATH_RE.search(path):
        return True
    slug = re.sub(r"[^a-z0-9]+", "-", path)
    dt = distinctive_tokens(utility_name)
    if len(dt) >= 2 and all(d in slug for d in dt[:2]):
        return True
    for a in name_acronyms(utility_name) | set(dt[:1]):
        if re.search(rf"(?:^|-)({re.escape(a)})s?(?:-|$)", slug) and len(a) >= 4:
            return True
    return False


OWN, OWN_DEMOTED, OFFICIAL_OTHER, UNKNOWN, THIRD = 0, 2, 1, 3, 4


def candidate_tier(url: str, utility_name: str, utility_domain: str | None,
                   today: date | None = None) -> int:
    host = normalize_host(url)
    if is_third_party_host(host) or is_about_utility_page(url, utility_name, utility_domain):
        return THIRD
    own = bool(utility_domain) and registrable_domain(host) == registrable_domain(normalize_host(utility_domain))
    stale_or_supply = is_supply_page(url) or is_stale_document(url, today)
    if own:
        return OWN_DEMOTED if stale_or_supply else OWN
    if stale_or_supply:
        return THIRD - 0.5  # below unknown, above third-party
    if is_government_host(host):
        return OFFICIAL_OTHER
    return UNKNOWN


def rerank_candidates(scored: list[tuple[float, dict]], utility_name: str,
                      utility_domain: str | None, today: date | None = None) -> list[tuple[float, dict]]:
    """Stable re-order by tier, then score.  Nothing is dropped."""
    return sorted(scored, key=lambda sr: (candidate_tier(sr[1].get("url", ""), utility_name,
                                                          utility_domain, today), -sr[0]))


def official_tariff_hub(utility_domain: str | None, state: str | None) -> str | None:
    host = normalize_host(utility_domain or "")
    if not host:
        return None
    entry = OFFICIAL_TARIFF_HUBS.get(registrable_domain(host))
    if not entry:
        return None
    states = entry.get("states")
    if states is None:
        return entry["url"]
    st = (state or "").upper()
    if st not in states:
        return None
    return entry["url"].format(slug=US_STATES[st].replace(" ", "_"))


def is_bad_start(url: str, utility_name: str, utility_domain: str | None,
                 today: date | None = None) -> bool:
    """A start that should yield to the official hub / a better candidate."""
    return candidate_tier(url, utility_name, utility_domain, today) in (OWN_DEMOTED, THIRD - 0.5, THIRD)
