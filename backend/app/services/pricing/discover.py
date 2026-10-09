"""Official document discovery for the component pricing path (PR R28-4).

Finds the utility's tariff book / rate index, rider sheets, default-supply
document (deregulated / Alberta), provincial commodity (ON), and delivery
charges — without relying on pinned golden URLs.

Pipeline (mirrors r28_general.py, made proper):
1. Phase 1 rate-page search (official hub / preferred book first).
2. Phase 2 crawl one/two levels for candidate pages/PDFs.
3. Reject marketing, FAQs, bill inserts, non-English locale pages when an
   English/official twin exists, and stale editions.
4. ``build_document_set`` classifies roles and picks the current edition.

Network I/O lives here (and in tariff_pipeline). Unit tests inject fake
page lists so CI stays offline.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Callable, Iterable
from urllib.parse import urlparse

from app.services.pricing.document_set import (
    DocumentCandidate,
    DocumentSetResult,
    build_document_set,
    is_marketing_url,
    looks_like_rates_url,
    parse_effective_date,
)
from app.services.source_type import (
    THIRD_PARTY,
    UtilitySourceContext,
    classify_source,
    is_regulator_publisher_host,
    is_third_party_host,
    normalize_host,
)

_CA_PROVINCES = frozenset(
    "AB BC MB NB NL NS NT NU ON PE QC SK YT".split()
)
# State-run retail-choice sites that publish the default-supply price to
# compare. They are not the utility's domain but are the official source.
STATE_SUPPLY_PUBLISHERS = frozenset({
    "pluginillinois.org", "papowerswitch.com", "powertochoose.org",
    "energychoice.ohio.gov", "energizect.com", "energyswitchma.gov",
})
_LANGUAGE_WORDS_RE = re.compile(
    r"\b(?:english|anglais|espa[nñ]ol|spanish|fran[cç]ais|french|en|es|fr)\b"
)

# Extra reject markers beyond document_set._MARKETING_MARKERS.
_FAQ_MARKERS = (
    "faq", "/faqs", "frequently-asked", "help-center", "customer-help",
)
_BILL_INSERT_MARKERS = (
    "bill-insert", "bill_insert", "billinsert", "insert-",
    "newsletter", "customer-newsletter", "bill-message",
)
_LOCALE_MARKERS = (
    "/es/", "/es-", "_es.", "-es.", "/fr/", "/fr-", "_fr.", "-fr.",
    "/zh/", "lang=es", "lang=fr", "locale=es", "locale=fr",
    "spanish", "francais", "français",
)
_ENGLISH_MARKERS = (
    "/en/", "/en-", "_en.", "-en.", "lang=en", "locale=en", "english",
)


@dataclass
class DiscoveredPage:
    """Minimal page record (compatible with tariff_pipeline.RatePage fields)."""

    url: str
    title: str | None = None
    content: str = ""
    content_hash: str | None = None


@dataclass
class DiscoveryResult:
    document_set: DocumentSetResult
    rate_page_url: str | None = None
    candidate_count: int = 0
    rejected: list[dict[str, str]] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "rate_page_url": self.rate_page_url,
            "candidate_count": self.candidate_count,
            "rejected": list(self.rejected),
            "notes": list(self.notes) + list(self.document_set.notes),
            "document_set": self.document_set.to_dict(),
        }


def _hay(url: str, title: str | None = None, snippet: str | None = None) -> str:
    return f"{url} {title or ''} {(snippet or '')[:500]}".lower()


def is_faq_url(url: str, title: str | None = None) -> bool:
    h = _hay(url, title)
    return any(m in h for m in _FAQ_MARKERS)


def is_bill_insert_url(url: str, title: str | None = None, snippet: str | None = None) -> bool:
    h = _hay(url, title, snippet)
    if any(m in h for m in _BILL_INSERT_MARKERS):
        return True
    # "January 2026 bill insert" style titles
    if re.search(r"\bbill\b.{0,20}\binsert\b", h) or re.search(
        r"\binsert\b.{0,20}\bbill\b", h
    ):
        return True
    return False


def is_non_english_locale_url(url: str, title: str | None = None) -> bool:
    h = _hay(url, title)
    return any(m in h for m in _LOCALE_MARKERS)


def is_english_locale_url(url: str, title: str | None = None) -> bool:
    h = _hay(url, title)
    return any(m in h for m in _ENGLISH_MARKERS)


def discovery_source_context(
    *,
    website_url: str | None = None,
    state: str = "",
    rate_page_url: str | None = None,
    official_urls: Iterable[str] = (),
) -> UtilitySourceContext:
    """Source context for discovery: website + configured URLs + Phase-1 rate page.

    The Phase-1 rate page widens a known official set (a tariff book on a
    sister domain) but never defines it alone, and a regulator rate page
    never does: with neither a website nor configured URLs every
    non-blocklisted host stays ``unknown`` and is kept.
    """
    st = (state or "").strip().upper()
    urls = [u for u in official_urls if u]
    if (
        rate_page_url
        and (website_url or urls)
        and not is_third_party_host(rate_page_url)
        and not is_regulator_publisher_host(rate_page_url)
    ):
        urls.append(rate_page_url)
    return UtilitySourceContext(
        website_url=website_url,
        official_urls=tuple(dict.fromkeys(urls)),
        country=("CA" if st in _CA_PROVINCES else "US") if st else None,
        state_province=st or None,
    )


def non_utility_domain(url: str, ctx: UtilitySourceContext | None = None) -> bool:
    """True for blocklisted hosts, and for foreign hosts when the utility's own is known.

    Generic file hosts and government hosts stay (classified ``unknown``), as
    do the jurisdiction's rate-publishing board and state supply publishers.
    """
    if is_third_party_host(url):
        return True
    if ctx is None:
        return False
    host = normalize_host(url)
    if any(host == d or host.endswith("." + d) for d in STATE_SUPPLY_PUBLISHERS):
        return False
    return classify_source(url, ctx).source_type == THIRD_PARTY


def _language_neutral_title(title: str | None) -> str:
    t = _LANGUAGE_WORDS_RE.sub(" ", (title or "").lower())
    return re.sub(r"[\W_]+", " ", t).strip()


def _is_locale_twin(cand: DocumentCandidate, other: DocumentCandidate) -> bool:
    """``other`` is a non-foreign-locale copy of the same document as ``cand``."""
    if other.url == cand.url or is_non_english_locale_url(other.url, other.title):
        return False
    a, b = urlparse(cand.url), urlparse(other.url)
    if normalize_host(cand.url) != normalize_host(other.url):
        return False
    if _similar_path(a.path.lower(), b.path.lower()):
        return True
    ta, tb = _language_neutral_title(cand.title), _language_neutral_title(other.title)
    return bool(ta) and ta == tb


def reject_reason_for_candidate(
    cand: DocumentCandidate,
    *,
    siblings: Iterable[DocumentCandidate] | None = None,
    source_ctx: UtilitySourceContext | None = None,
) -> str | None:
    """Return a reject reason, or None if the candidate may stay."""
    url, title, snip = cand.url, cand.title, cand.text_snippet
    if non_utility_domain(url, source_ctx):
        return "non_utility_domain"
    # More specific rejects first (FAQ / bill-insert also match marketing markers).
    if is_faq_url(url, title):
        return "faq_page"
    if is_bill_insert_url(url, title, snip):
        return "bill_insert"
    if is_marketing_url(url, title) and not looks_like_rates_url(url, title):
        return "marketing_page"
    # A French/Spanish document goes only when a copy of the same document in
    # the default locale is also present: in Québec or a bilingual utility the
    # French document may be the only edition of that schedule.
    if is_non_english_locale_url(url, title) and any(
        _is_locale_twin(cand, s) for s in (siblings or [])
    ):
        return "non_english_locale"
    return None


def _similar_path(a: str, b: str) -> bool:
    """True when paths look like locale variants of the same document."""
    def strip_locale(p: str) -> str:
        p = re.sub(r"/(es|fr|en|zh)(/|$)", "/", p)
        p = re.sub(r"[-_](es|fr|en|zh)(?=\.|/|$)", "", p)
        return re.sub(r"/+", "/", p)
    return strip_locale(a) == strip_locale(b) and a != b


def filter_discovery_candidates(
    candidates: list[DocumentCandidate],
    *,
    source_ctx: UtilitySourceContext | None = None,
) -> tuple[list[DocumentCandidate], list[dict[str, str]]]:
    """Drop foreign-domain / FAQ / bill-insert / marketing / locale twins before classify."""
    kept: list[DocumentCandidate] = []
    rejected: list[dict[str, str]] = []
    for cand in candidates:
        reason = reject_reason_for_candidate(
            cand, siblings=candidates, source_ctx=source_ctx,
        )
        if reason:
            rejected.append({"url": cand.url, "reason": reason})
        else:
            kept.append(cand)
    return kept, rejected


def pages_to_candidates(
    pages: Iterable[DiscoveredPage | Any],
) -> list[DocumentCandidate]:
    """Convert RatePage-like objects into DocumentCandidates."""
    out: list[DocumentCandidate] = []
    for p in pages:
        url = getattr(p, "url", None) or (p.get("url") if isinstance(p, dict) else None)
        if not url:
            continue
        title = getattr(p, "title", None)
        if isinstance(p, dict):
            title = p.get("title")
        content = getattr(p, "content", "") or ""
        if isinstance(p, dict):
            content = p.get("content") or ""
        ch = getattr(p, "content_hash", None)
        if isinstance(p, dict):
            ch = p.get("content_hash")
        out.append(DocumentCandidate(
            url=url,
            title=title,
            text_snippet=(content or "")[:2000],
            content_hash=ch,
            effective_date=parse_effective_date(
                url=url, title=title, text_snippet=content[:1500],
            ),
        ))
    return out


Phase1Fn = Callable[[str, str, str | None], tuple[str | None, int, list[str]]]
Phase2Fn = Callable[[str], list[Any]]


def discover_document_set(
    *,
    utility_name: str,
    state: str = "",
    website_url: str | None = None,
    recipe_code: str = "bundled",
    as_of: date | None = None,
    phase1_fn: Phase1Fn | None = None,
    phase2_fn: Phase2Fn | None = None,
    disable_browser_agent: bool = True,
    official_urls: Iterable[str] = (),
) -> DiscoveryResult:
    """Run Phase 1+2 discovery and assemble a pricing document set.

    ``phase1_fn`` / ``phase2_fn`` default to ``tariff_pipeline`` (live).
    Tests inject fakes. Browser-agent fallback is disabled by default so
    discovery stays deterministic and cheap.
    """
    as_of = as_of or date.today()
    notes: list[str] = []

    if phase1_fn is None or phase2_fn is None:
        from scripts import tariff_pipeline as tp
        if disable_browser_agent:
            tp._try_browser_agent_fallback = lambda *_a, **_k: []
        if phase1_fn is None:
            phase1_fn = tp.phase1_find_rate_page
        if phase2_fn is None:
            phase2_fn = tp.phase2_discover_tariff_pages

    rate_url, _n, alts = phase1_fn(utility_name, state, website_url)
    notes.append(f"phase1_rate_page:{rate_url or 'none'}")
    pages: list[Any] = []
    if rate_url:
        pages = list(phase2_fn(rate_url) or [])
    # Also try a few Phase-1 alternates when the primary crawl is thin.
    if len(pages) < 2 and alts:
        for alt in alts[:3]:
            if not alt or alt == rate_url:
                continue
            try:
                extra = list(phase2_fn(alt) or [])
            except Exception:
                extra = []
            pages.extend(extra)
            if len(pages) >= 6:
                break
        notes.append(f"phase1_alts_tried:{min(3, len(alts))}")

    candidates = pages_to_candidates(pages)
    # Seed the rate page itself when Phase 2 returned nothing but Phase 1
    # found a URL (common for direct PDF tariff books).
    if rate_url and not any(c.url == rate_url for c in candidates):
        candidates.insert(0, DocumentCandidate(url=rate_url, title=utility_name))

    source_ctx = discovery_source_context(
        website_url=website_url,
        state=state,
        rate_page_url=rate_url,
        official_urls=official_urls,
    )
    filtered, rejected = filter_discovery_candidates(candidates, source_ctx=source_ctx)
    if rejected:
        notes.append(f"rejected_pre_classify:{len(rejected)}")

    ds = build_document_set(
        filtered,
        recipe_code=recipe_code,
        as_of=as_of,
        utility_name=utility_name,
    )
    return DiscoveryResult(
        document_set=ds,
        rate_page_url=rate_url,
        candidate_count=len(candidates),
        rejected=rejected,
        notes=notes,
    )


# R28 non-golden utilities used to validate discovery filters offline.
# Names match RESULTS_R28_0729.md (Nevada Power replaced Evergy).
R28_NON_GOLDEN_FIXTURES: dict[str, dict[str, Any]] = {
    "Consumers Energy": {
        "recipe_code": "bundled",
        "state": "MI",
        "pages": [
            {"url": "https://www.consumersenergy.com/residential/rates",
             "title": "Residential rates FAQ", "content": "How do rates work?"},
            {"url": "https://www.consumersenergy.com/marketing/overview",
             "title": "Customer overview brochure", "content": "Welcome"},
            {"url": "https://www.consumersenergy.com/rates/tariff-book-2026.pdf",
             "title": "Rate Book", "content": "Residential Service 12.5 ¢/kWh"},
            {"url": "https://www.consumersenergy.com/rates/riders/fuel-cost.pdf",
             "title": "Fuel Cost Rider", "content": "Fuel rider 0.4 ¢/kWh"},
        ],
        "expect_selected_substr": ["tariff-book", "fuel-cost"],
        "expect_reject_reasons": {"faq_page", "marketing_page"},
    },
    "Nevada Power": {
        "recipe_code": "bundled",
        "state": "NV",
        "pages": [
            {"url": "https://www.nvenergy.com/bill-insert-jan-2026.pdf",
             "title": "January 2026 Bill Insert", "content": "Seasonal tips"},
            {"url": "https://www.nvenergy.com/rates/residential-schedule-rs.pdf",
             "title": "Schedule RS", "content": "Energy Charge 10.2 ¢/kWh"},
        ],
        "expect_selected_substr": ["residential-schedule"],
        "expect_reject_reasons": {"bill_insert"},
    },
    "Arizona Public Service": {
        "recipe_code": "bundled",
        "state": "AZ",
        "pages": [
            {"url": "https://www.aps.com/es/rates/tou-e.pdf",
             "title": "TOU-E (Español)", "content": "Tarifa"},
            {"url": "https://www.aps.com/en/rates/tou-e.pdf",
             "title": "TOU-E English", "content": "On-peak 15 ¢/kWh"},
        ],
        "expect_selected_substr": ["/en/rates/tou-e"],
        "expect_reject_reasons": {"non_english_locale"},
    },
    "Commonwealth Edison": {
        "recipe_code": "deregulated",
        "state": "IL",
        "pages": [
            {"url": "https://www.comed.com/MyAccount/MyBill/Pages/Rates.aspx",
             "title": "Delivery rates", "content": "Delivery 4.2 ¢/kWh"},
            {"url": "https://www.comed.com/rates/rate-of-last-resort.pdf",
             "title": "Rate of Last Resort", "content": "Supply 6.1 ¢/kWh"},
        ],
        "expect_roles": {"delivery", "default_supply"},
        "expect_reject_reasons": set(),
    },
    "Massachusetts Electric": {
        "recipe_code": "deregulated",
        "state": "MA",
        "pages": [
            {"url": "https://www.nationalgridus.com/MA-Home/Rates/Delivery",
             "title": "Delivery", "content": "Distribution charge"},
            {"url": "https://www.nationalgridus.com/MA-Home/Rates/Basic-Service",
             "title": "Basic Service Supply", "content": "Default supply 12¢"},
        ],
        "expect_roles": {"delivery", "default_supply"},
        "expect_reject_reasons": set(),
    },
    "Ameren Missouri": {
        "recipe_code": "bundled",
        "state": "MO",
        "pages": [
            {"url": "https://www.ameren.com/missouri/residential/rates",
             "title": "Rates landing", "content": "See schedules"},
            {"url": "https://www.ameren.com/missouri/rates/schedule-res.pdf",
             "title": "Schedule RES", "content": "Energy Charge 9.1 ¢/kWh"},
        ],
        "expect_selected_substr": ["schedule-res"],
        "expect_reject_reasons": set(),
    },
    "Sacramento Municipal Utility District": {
        "recipe_code": "bundled",
        "state": "CA",
        "pages": [
            {"url": "https://www.smud.org/en/Rate-Information/Residential",
             "title": "Residential rates", "content": "Standard Residential"},
            {"url": "https://www.smud.org/assets/documents/pdf/rate-schedule-R.pdf",
             "title": "Rate Schedule R", "content": "Energy Charge 12.8 ¢/kWh"},
        ],
        "expect_selected_substr": ["rate-schedule-R"],
        "expect_reject_reasons": set(),
    },
    "Kentucky Utilities": {
        "recipe_code": "bundled",
        "state": "KY",
        "pages": [
            {"url": "https://lge-ku.com/sites/default/files/2024-01/tariff-book-2024.pdf",
             "title": "Tariff Book Jan 2024", "content": "Effective January 1, 2024"},
            {"url": "https://lge-ku.com/sites/default/files/2026-07/tariff-book-jul-2026.pdf",
             "title": "Tariff Book Jul 2026", "content": "Effective July 1, 2026 Energy 11¢"},
            {"url": "https://lge-ku.com/rates/faq",
             "title": "Rates FAQ", "content": "Common questions"},
        ],
        "expect_selected_substr": ["tariff-book-jul-2026"],
        "expect_not_selected_substr": ["tariff-book-2024"],
        "expect_reject_reasons": {"faq_page"},
    },
    "London Hydro": {
        "recipe_code": "provincial_ontario",
        "state": "ON",
        "pages": [
            {"url": "https://www.oeb.ca/consumer-information-and-protection/electricity-rates",
             "title": "OEB RPP", "content": "Time-of-use prices ¢/kWh"},
            {"url": "https://www.oeb.ca/_html/calculator/data/BillData.xml",
             "title": "BillData", "content": "Delivery charges XML"},
            {"url": "https://www.londonhydro.com/rates/faq-residential",
             "title": "Residential rates FAQ", "content": "How billing works"},
        ],
        "expect_roles": {"provincial_commodity", "delivery"},
        "expect_reject_reasons": {"faq_page"},
    },
    "Newfoundland Power": {
        "recipe_code": "bundled",
        "state": "NL",
        "pages": [
            {"url": "https://www.newfoundlandpower.com/rates/schedule-of-rates-2026.pdf",
             "title": "Schedule of Rates 2026",
             "content": "Domestic Rate #1.1 Energy Charge 15.587 ¢/kWh"},
            {"url": "https://www.newfoundlandpower.com/customer-newsletter-spring.pdf",
             "title": "Customer newsletter", "content": "Tips for saving"},
        ],
        "expect_selected_substr": ["schedule-of-rates"],
        "expect_reject_reasons": {"bill_insert"},
    },
}


def run_r28_fixture_discovery(name: str) -> DiscoveryResult:
    """Offline discovery for one R28 non-golden fixture (no network)."""
    fix = R28_NON_GOLDEN_FIXTURES[name]
    pages = [DiscoveredPage(**p) for p in fix["pages"]]

    def phase1(u, s, w):
        return (pages[0].url if pages else None, 1, [])

    def phase2(url):
        return pages

    return discover_document_set(
        utility_name=name,
        state=fix.get("state", ""),
        recipe_code=fix["recipe_code"],
        as_of=date(2026, 10, 9),
        phase1_fn=phase1,
        phase2_fn=phase2,
    )


def assert_r28_fixture(name: str) -> DiscoveryResult:
    """Run one fixture and raise AssertionError on expectation miss."""
    fix = R28_NON_GOLDEN_FIXTURES[name]
    result = run_r28_fixture_discovery(name)
    selected = result.document_set.selected_urls()
    reject_reasons = {r["reason"] for r in result.rejected}
    # Stale editions land as reject_reason on members, not pre-filter.
    for m in result.document_set.members:
        if m.reject_reason:
            reject_reasons.add(m.reject_reason)

    for substr in fix.get("expect_selected_substr") or []:
        if not any(substr in u for u in selected):
            raise AssertionError(
                f"{name}: expected selected URL containing {substr!r}, got {selected}"
            )
    for substr in fix.get("expect_not_selected_substr") or []:
        if any(substr in u for u in selected):
            raise AssertionError(
                f"{name}: did not expect selected URL containing {substr!r}, got {selected}"
            )
    expected_roles = fix.get("expect_roles")
    if expected_roles is not None:
        got_roles = {m.role for m in result.document_set.selected()}
        if not expected_roles.issubset(got_roles):
            raise AssertionError(
                f"{name}: expected roles {expected_roles}, got {got_roles}"
            )
    expected_rejects = fix.get("expect_reject_reasons") or set()
    if not expected_rejects.issubset(reject_reasons):
        raise AssertionError(
            f"{name}: expected reject reasons {expected_rejects}, got {reject_reasons}"
        )
    return result


__all__ = [
    "DiscoveredPage",
    "DiscoveryResult",
    "R28_NON_GOLDEN_FIXTURES",
    "STATE_SUPPLY_PUBLISHERS",
    "assert_r28_fixture",
    "discover_document_set",
    "discovery_source_context",
    "filter_discovery_candidates",
    "is_bill_insert_url",
    "is_faq_url",
    "is_non_english_locale_url",
    "non_utility_domain",
    "pages_to_candidates",
    "reject_reason_for_candidate",
    "run_r28_fixture_discovery",
]
