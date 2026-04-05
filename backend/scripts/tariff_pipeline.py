"""
End-to-end tariff discovery and extraction pipeline.

For each utility:
  Phase 1 — Find the main rates page via Brave Search
  Phase 2 — Crawl the rates page to discover tariff sub-pages / PDFs
  Phase 3 — Extract structured tariff data from each page via LLM
  Phase 4 — Validate and store results

Usage:
    # Process a single utility (for testing)
    python -m scripts.tariff_pipeline --utility-id 1714

    # Process all utilities missing tariff data
    python -m scripts.tariff_pipeline --missing-tariffs --limit 50

    # Dry run — search + crawl but don't write to DB
    python -m scripts.tariff_pipeline --utility-id 1714 --dry-run

    # Skip phases (e.g. only crawl+extract if URL already known)
    python -m scripts.tariff_pipeline --utility-id 1714 --skip-search

Requires env vars:
    BRAVE_API_KEY       — for web search
    ANTHROPIC_API_KEY   — for LLM extraction
    ADMIN_API_KEY       — for admin API access
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("tariff_pipeline")

def _load_setting(attr: str, env_var: str, default: str = "") -> str:
    """Load a config value from app.config.settings, falling back to env var."""
    try:
        from app.config import settings as _settings
        val = getattr(_settings, attr, "")
        if val:
            return val
    except Exception:
        pass
    return os.environ.get(env_var, default)


API_BASE = os.environ.get("API_URL", "http://127.0.0.1:8000")
ADMIN_KEY = _load_setting("admin_api_key", "ADMIN_API_KEY")
BRAVE_API_KEY = _load_setting("brave_api_key", "BRAVE_API_KEY")
ANTHROPIC_API_KEY = _load_setting("anthropic_api_key", "ANTHROPIC_API_KEY")
GOOGLE_CSE_API_KEY = _load_setting("google_cse_api_key", "GOOGLE_CSE_API_KEY")
GOOGLE_CSE_CX = _load_setting("google_cse_cx", "GOOGLE_CSE_CX")
GOOGLE_AI_API_KEY = _load_setting("google_ai_api_key", "GOOGLE_AI_API_KEY")
HAIKU_MODEL = "claude-haiku-4-5-20251001"
OPUS_MODEL = "claude-opus-4-6"
GEMINI_MODEL = "gemini-3-flash-preview"

FETCH_TIMEOUT = httpx.Timeout(15.0, connect=8.0)
PDF_TIMEOUT = httpx.Timeout(30.0, connect=10.0)
USER_AGENT = "UtilityTariffFinder/1.0 (tariff research bot)"

import threading
from collections import defaultdict

_thread_local = threading.local()

# Per-domain request throttle: tracks the last request time per domain
# to avoid overwhelming utility websites when running parallel workers.
_domain_last_request: dict[str, float] = defaultdict(float)
_domain_throttle_lock = threading.Lock()
_DOMAIN_MIN_INTERVAL = 1.0  # seconds between requests to the same domain


def _throttle_domain(domain: str):
    """Sleep if needed to maintain minimum interval between requests to a domain."""
    bare = domain.replace("www.", "")
    with _domain_throttle_lock:
        last = _domain_last_request[bare]
        now = time.time()
        wait = _DOMAIN_MIN_INTERVAL - (now - last)
        if wait > 0:
            time.sleep(wait)
        _domain_last_request[bare] = time.time()


def _get_http_client() -> httpx.Client:
    client = getattr(_thread_local, "http_client", None)
    if client is None or client.is_closed:
        client = httpx.Client(
            timeout=FETCH_TIMEOUT,
            follow_redirects=True,
            headers={"User-Agent": USER_AGENT},
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=5),
        )
        _thread_local.http_client = client
    return client


# ---------------------------------------------------------------------------
# Playwright browser manager — reuses a single browser across calls within
# a pipeline run to avoid the massive memory cost of launching a new Chromium
# process for every page fetch / PDF download.
# ---------------------------------------------------------------------------
class _PlaywrightManager:
    """Lazy singleton that keeps one Chromium browser alive and tracks context
    count so we can periodically restart the browser to reclaim leaked memory."""

    _MAX_CONTEXTS = 25  # restart browser after this many contexts

    def __init__(self):
        self._pw = None
        self._browser = None
        self._ctx_count = 0

    def _ensure_browser(self):
        if self._browser and self._browser.is_connected():
            return
        self._cleanup_browser()
        from playwright.sync_api import sync_playwright
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=True,
            args=["--disable-dev-shm-usage", "--disable-gpu", "--no-sandbox",
                  "--disable-extensions"],
        )
        self._ctx_count = 0
        log.info("  [PW] Browser launched")

    def new_context(self, **kwargs):
        """Create a new browser context. Caller MUST close it when done.
        Auto-restarts the browser if it crashed between calls."""
        self._ensure_browser()
        self._ctx_count += 1
        if self._ctx_count > self._MAX_CONTEXTS:
            log.info("  [PW] Recycling browser after %d contexts", self._ctx_count)
            self._cleanup_browser()
            self._ensure_browser()
        try:
            return self._browser.new_context(**kwargs)
        except Exception:
            log.warning("  [PW] Browser died, restarting...")
            self._cleanup_browser()
            self._ensure_browser()
            return self._browser.new_context(**kwargs)

    def _cleanup_browser(self):
        if self._browser:
            try:
                self._browser.close()
            except Exception:
                pass
            self._browser = None
        if self._pw:
            try:
                self._pw.stop()
            except Exception:
                pass
            self._pw = None
        self._ctx_count = 0

    def shutdown(self):
        """Close everything. Called between utilities in batch mode."""
        self._cleanup_browser()
        log.info("  [PW] Browser shut down")

    @property
    def is_available(self) -> bool:
        try:
            from playwright.sync_api import sync_playwright  # noqa: F401
            return True
        except ImportError:
            return False


def _get_pw_mgr() -> _PlaywrightManager:
    mgr = getattr(_thread_local, "pw_mgr", None)
    if mgr is None:
        mgr = _PlaywrightManager()
        _thread_local.pw_mgr = mgr
    return mgr

RATE_PAGE_KEYWORDS = re.compile(
    r"rate|tariff|pricing|schedule|electric.*charge|billing.*rate|"
    r"residential.*rate|commercial.*rate|general.*service|small.*business|"
    r"fee.*schedule|cost.*service|price.*electricity",
    re.IGNORECASE,
)

SKIP_KEYWORDS = re.compile(
    r"irrigation|fleet.*electrif|shore.*power|street.*light|"
    r"unmetered|transmission.*rate|industrial|"
    r"large.*power|large.*general|large.*service|"
    r"interruptible|standby|wholesale|interconnect|"
    r"wheeling|curtailment|generation|supplement.*\d{2,}|"
    r"lighting|outdoor.*light|area.*light|security.*light|"
    r"pumping|mining|smelting|data.center|"
    r"high.voltage|primary.*service|subtransmission",
    re.IGNORECASE,
)

IRRELEVANT_URL_KEYWORDS = re.compile(
    r"natural.gas|gas.rate|gas.tariff|gas.bill|gas.*submission|"
    r"our.gas.utility|gas.bcuc|gas.marketer|"
    r"propane|operating.agreement|"
    r"annual.report|investor|"
    r"careers|job|press.release|news.event|media.centre|"
    r"contact.us|login|sign.in|my.account|"
    r"rebate|incentive|conservation|energy.saving|"
    r"outage|storm|safety|emergency",
    re.IGNORECASE,
)

HOMEPAGE_ONLY_PATH = re.compile(r"^/?$")

# Aggregator / comparison / non-utility domains. Results from these are
# HARD-BLOCKED in search scoring (score = -999) so the pipeline never
# extracts tariffs from them.  Government / regulatory sites are NOT
# included here — they can be legitimate sources.
THIRD_PARTY_DOMAINS = frozenset({
    # Social / directory / review
    "wikipedia.org", "yelp.com", "yellowpages.com", "bbb.org",
    "facebook.com", "twitter.com", "linkedin.com", "reddit.com",
    "nextdoor.com", "glassdoor.com", "mapquest.com",
    # News / press
    "opb.org", "prnewswire.com", "businesswire.com", "koin.com",
    "azfamily.com",
    # Rate comparison / aggregator
    "openei.org", "utility-rates.com", "costcheckusa.com",
    "findenergy.com", "energysage.com", "choosetexaspower.org",
    "electricityplans.com", "saveonenergy.com", "electricrate.com",
    "wattbuy.com", "energybot.com", "electricitylocal.com",
    "energypal.com", "electricchoice.com", "paylesspower.com",
    "texaselectricityratings.com", "powertochoose.org",
    "electricityrates.com", "utilitygenius.com", "switchwise.com",
    "energyrates.ca", "ratehub.ca", "chooseenergy.com",
    "smartenergyusa.com", "gatby.com", "poweroutage.us",
    "njenergyratings.com", "energypricing.com",
    # Solar / green energy marketing
    "solar.com", "nrgcleanpower.com", "greenridgesolar.com",
    "madison.com", "sandboxsolar.com",
})


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class RatePage:
    url: str
    title: str = ""
    page_type: str = ""  # "html" or "pdf"
    content: str = ""
    content_hash: str = ""
    links: list[str] = field(default_factory=list)
    pdf_bytes: bytes | None = None


@dataclass
class ExtractedTariff:
    name: str
    code: str = ""
    customer_class: str = ""
    rate_type: str = ""
    description: str = ""
    source_url: str = ""
    effective_date: str = ""
    components: list[dict] = field(default_factory=list)
    confidence: float = 0.0


@dataclass
class PipelineResult:
    utility_id: int
    utility_name: str = ""
    country: str = ""
    state: str = ""
    phase1_rate_page_url: str = ""
    phase1_search_results: int = 0
    phase2_sub_pages: list[dict] = field(default_factory=list)
    phase3_tariffs: list[dict] = field(default_factory=list)
    phase4_validation: dict = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def api_get(path: str, params: dict | None = None) -> Any:
    headers = {"X-Admin-Key": ADMIN_KEY} if ADMIN_KEY else {}
    url = f"{API_BASE}{path}"
    resp = httpx.get(url, headers=headers, params=params, timeout=15.0)
    resp.raise_for_status()
    return resp.json()


def api_patch(path: str, body: dict) -> Any:
    headers = {"X-Admin-Key": ADMIN_KEY, "Content-Type": "application/json"}
    resp = httpx.patch(f"{API_BASE}{path}", headers=headers, json=body, timeout=15.0)
    resp.raise_for_status()
    return resp.json()


def api_post(path: str, body: dict, timeout: float = 15.0) -> Any:
    headers = {"X-Admin-Key": ADMIN_KEY, "Content-Type": "application/json"}
    resp = httpx.post(f"{API_BASE}{path}", headers=headers, json=body, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def fetch_page(url: str) -> tuple[str, str, int]:
    """Fetch a URL. Returns (content, content_type, status_code)."""
    try:
        resp = _get_http_client().get(url)
        return resp.text, resp.headers.get("content-type", ""), resp.status_code
    except Exception as e:
        log.warning(f"  fetch_page failed for {url[:80]}: {type(e).__name__}: {e}")
        return "", "", 0


def _download_pdf(url: str) -> bytes | None:
    """Download a PDF, return raw bytes or None.
    Falls back to Playwright download for domains that block httpx."""
    domain = urlparse(url).netloc
    bare_domain = domain.replace("www.", "")
    use_browser = bare_domain in _BROWSER_REQUIRED_DOMAINS or domain in _js_rendered_domains

    if not use_browser:
        try:
            resp = _get_http_client().get(url, timeout=PDF_TIMEOUT)
            if resp.status_code != 200:
                return None
            ctype = resp.headers.get("content-type", "")
            if "pdf" not in ctype and not url.lower().endswith(".pdf"):
                return None
            if len(resp.content) > 15_000_000:
                log.warning(f"  PDF too large ({len(resp.content)} bytes), skipping")
                return None
            return resp.content
        except Exception as e:
            log.warning(f"  PDF download failed for {url[:60]}: {e}")
            if "SSL" not in str(e) and "Connect" not in type(e).__name__:
                return None
            log.info(f"  Retrying PDF download via Playwright for {url[:60]}")

    return _download_pdf_playwright(url)


def _download_pdf_playwright(url: str) -> bytes | None:
    """Download a PDF using the shared Playwright browser."""
    try:
        import tempfile as _tmpmod
        context = _get_pw_mgr().new_context(accept_downloads=True)
        try:
            page = context.new_page()
            with page.expect_download(timeout=30000) as dl_info:
                try:
                    page.goto(url, timeout=20000, wait_until="commit")
                except Exception:
                    pass  # navigation "fails" when a download starts
            download = dl_info.value
            with _tmpmod.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                tmp_path = tmp.name
            download.save_as(tmp_path)
            with open(tmp_path, "rb") as f:
                pdf_bytes = f.read()
            os.unlink(tmp_path)
            if len(pdf_bytes) > 15_000_000:
                log.warning(f"  Playwright PDF too large ({len(pdf_bytes)} bytes), skipping")
                return None
            log.info(f"  PDF downloaded via Playwright ({len(pdf_bytes)} bytes)")
            return pdf_bytes
        finally:
            context.close()
    except Exception as e:
        log.warning(f"  Playwright PDF download failed for {url[:60]}: {e}")
        return None


def _extract_pdf_pdfplumber(pdf_bytes: bytes, max_pages: int = 50) -> str:
    """Try extracting text from a PDF using pdfplumber (works for text-based PDFs)."""
    import io
    try:
        import pdfplumber
    except ImportError:
        return ""
    text_parts = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for i, page in enumerate(pdf.pages):
            if i >= max_pages:
                break
            page_text = page.extract_text() or ""
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    cells = [str(c).strip() if c else "" for c in row]
                    page_text += "\n" + " | ".join(cells)
            text_parts.append(page_text)
    return "\n\n".join(text_parts).strip()


_RATE_SCHEDULE_RE = re.compile(
    r"rate\s*#|rate\s*no\.?\s*\d|basic\s*customer\s*charge|"
    r"energy\s*charge.*\$|per\s+k[wW]h.*\$|\$.*per\s+k[wW]h|"
    r"minimum\s+(monthly\s+)?bill|"
    r"service\s*charge.*\d+\.\d{2}|"
    r"domestic\s+service|general\s+service\s+\d|"
    r"residential\s+service|commercial\s+service",
    re.IGNORECASE,
)

_RATE_KEYWORD_RE = re.compile(
    r"kwh|per month|energy charge|customer charge|basic charge|"
    r"demand charge|cents per|kilowatt|"
    r"service charge|monthly charge|minimum bill",
    re.IGNORECASE,
)

_RATE_AMOUNT_RE = re.compile(r"\$\s*\d+\.?\d*|\d+\.\d+\s*(?:cents|¢)")


def _ocr_page_priority(text: str) -> int:
    """Lower = more likely to contain actual rate schedules.
    0 = has rate schedule patterns (Rate #1.1, Energy Charge: $X.XX)
    1 = has dollar amounts + rate keywords together
    2 = has rate keywords only
    3 = filler / rules / legal"""
    if _RATE_SCHEDULE_RE.search(text):
        return 0
    if _RATE_AMOUNT_RE.search(text) and _RATE_KEYWORD_RE.search(text):
        return 1
    if _RATE_KEYWORD_RE.search(text):
        return 2
    return 3


def _extract_pdf_ocr(pdf_bytes: bytes, max_total_pages: int = 40) -> str:
    """Fall back to OCR (Tesseract) for scanned/image-based PDFs.
    Scans all pages, then prioritizes pages with actual rate amounts."""
    try:
        from pdf2image import convert_from_bytes
        import pytesseract
    except ImportError:
        log.warning("  pdf2image/pytesseract not installed — cannot OCR PDF")
        return ""
    try:
        images = convert_from_bytes(
            pdf_bytes, first_page=1, last_page=max_total_pages, dpi=200,
        )
        log.info(f"    OCR: scanning {len(images)} pages...")
        all_pages: list[tuple[int, str, int]] = []
        for i, img in enumerate(images):
            page_text = pytesseract.image_to_string(img)
            priority = _ocr_page_priority(page_text)
            all_pages.append((i + 1, page_text, priority))

        # Sort by priority: pages with dollar amounts first, then rate keywords, then others
        all_pages.sort(key=lambda x: x[2])

        counts = {0: 0, 1: 0, 2: 0, 3: 0}
        for _, _, p in all_pages:
            counts[p] = counts.get(p, 0) + 1
        log.info(f"    OCR: {counts[0]} rate-schedule pages, {counts[1]} rate+amount pages, {counts[2]} keyword-only, {counts[3]} filler")

        # Prefer actual rate schedule pages; fall back to broader set
        kept = [(pg, txt) for pg, txt, pri in all_pages if pri == 0]
        if len(kept) < 3:
            kept = [(pg, txt) for pg, txt, pri in all_pages if pri <= 1]
        if not kept:
            kept = [(pg, txt) for pg, txt, pri in all_pages if pri <= 2]
        if not kept:
            kept = [(pg, txt) for pg, txt, _ in all_pages]

        text_parts = [txt for _, txt in kept]
        return "\n\n".join(text_parts).strip()
    except Exception as e:
        log.warning(f"  OCR extraction failed: {e}")
        return ""


PDF_CACHE_DIR = os.path.join(os.environ.get("APP_LOG_DIR", "/app/logs"), "pdf_ocr_cache")


def _get_pdf_cache(content_hash: str) -> str | None:
    """Return cached OCR text for a PDF content hash, or None if not cached."""
    cache_path = os.path.join(PDF_CACHE_DIR, f"{content_hash}.txt")
    if os.path.isfile(cache_path):
        try:
            with open(cache_path, "r") as f:
                return f.read()
        except OSError:
            return None
    return None


def _set_pdf_cache(content_hash: str, text: str) -> None:
    """Store OCR text keyed by PDF content hash."""
    os.makedirs(PDF_CACHE_DIR, exist_ok=True)
    cache_path = os.path.join(PDF_CACHE_DIR, f"{content_hash}.txt")
    try:
        with open(cache_path, "w") as f:
            f.write(text)
    except OSError as e:
        log.warning(f"Failed to write PDF cache: {e}")


# LLM extraction result cache — avoids re-calling the LLM for the same content
LLM_CACHE_DIR = os.path.join(os.environ.get("APP_LOG_DIR", "/app/logs"), "llm_extraction_cache")
_LLM_PROMPT_VERSION = "v2"


def _get_llm_cache(content_hash: str, model: str) -> list[dict] | None:
    """Return cached extraction result for a content hash + model, or None."""
    cache_key = hashlib.sha256(f"{content_hash}:{model}:{_LLM_PROMPT_VERSION}".encode()).hexdigest()
    cache_path = os.path.join(LLM_CACHE_DIR, f"{cache_key}.json")
    if os.path.isfile(cache_path):
        try:
            with open(cache_path, "r") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return None
    return None


def _set_llm_cache(content_hash: str, model: str, tariffs: list[dict]) -> None:
    """Store LLM extraction result keyed by content hash + model."""
    os.makedirs(LLM_CACHE_DIR, exist_ok=True)
    cache_key = hashlib.sha256(f"{content_hash}:{model}:{_LLM_PROMPT_VERSION}".encode()).hexdigest()
    cache_path = os.path.join(LLM_CACHE_DIR, f"{cache_key}.json")
    try:
        with open(cache_path, "w") as f:
            json.dump(tariffs, f)
    except OSError as e:
        log.warning(f"    Failed to write PDF cache: {e}")


def fetch_pdf_text(url: str) -> str:
    """Download a PDF and extract text. Tries pdfplumber first, falls back to OCR.
    OCR results are cached by content hash so the same PDF is never re-OCR'd."""
    pdf_bytes = _download_pdf(url)
    if not pdf_bytes:
        return ""

    content_hash = hashlib.sha256(pdf_bytes).hexdigest()

    try:
        text = _extract_pdf_pdfplumber(pdf_bytes)
        if len(text.strip()) > 200:
            log.info(f"    PDF text extracted via pdfplumber ({len(text)} chars)")
            return text

        # pdfplumber failed — check OCR cache before running expensive OCR
        cached = _get_pdf_cache(content_hash)
        if cached is not None:
            log.info(f"    PDF OCR cache hit ({len(cached)} chars, hash={content_hash[:12]})")
            return cached[:40000]

        log.info(f"    pdfplumber returned little text ({len(text.strip())} chars), trying OCR...")
        text = _extract_pdf_ocr(pdf_bytes)
        if text:
            log.info(f"    PDF text extracted via OCR ({len(text)} chars)")
            _set_pdf_cache(content_hash, text)
            return text[:40000]

        log.warning(f"    PDF extraction returned no text for {url[:60]}")
        return ""
    except Exception as e:
        log.warning(f"  PDF extraction failed for {url[:60]}: {e}")
        return ""


def fetch_page_js(url: str) -> tuple[str, str]:
    """Fetch a page using the shared Playwright browser for JS-rendered content.
    Returns (html_content, page_title)."""
    if not _get_pw_mgr().is_available:
        log.warning("  Playwright not installed — cannot render JS pages")
        return "", ""
    context = None
    try:
        context = _get_pw_mgr().new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        )
        page = context.new_page()
        page.goto(url, wait_until="networkidle", timeout=20000)
        page.wait_for_timeout(2000)
        title = page.title()
        html = page.content()
        return html, title
    except Exception as e:
        log.warning(f"  Playwright fetch failed for {url[:60]}: {e}")
        return "", ""
    finally:
        if context:
            try:
                context.close()
            except Exception:
                pass


def is_rate_relevant_url(url: str, text: str = "") -> bool:
    """Heuristic: does this URL or link text look like a rates/tariff page?"""
    combined = f"{url} {text}".lower()
    return bool(RATE_PAGE_KEYWORDS.search(combined))


def is_same_domain(url1: str, url2: str) -> bool:
    d1 = urlparse(url1).netloc.replace("www.", "")
    d2 = urlparse(url2).netloc.replace("www.", "")
    return d1 == d2 or d1.endswith(f".{d2}") or d2.endswith(f".{d1}")


def url_is_homepage(url: str) -> bool:
    return bool(HOMEPAGE_ONLY_PATH.match(urlparse(url).path))


# ---------------------------------------------------------------------------
# Phase 1: Find rate page via Brave Search
# ---------------------------------------------------------------------------

BRAVE_CACHE_DIR = os.path.join(os.environ.get("APP_LOG_DIR", "/app/logs"), "brave_cache")
BRAVE_CACHE_TTL_DAYS = 30


def _get_brave_cache(query: str) -> list[dict] | None:
    """Return cached Brave results if fresh, else None."""
    cache_key = hashlib.sha256(query.encode()).hexdigest()
    cache_path = os.path.join(BRAVE_CACHE_DIR, f"{cache_key}.json")
    if os.path.isfile(cache_path):
        try:
            mtime = os.path.getmtime(cache_path)
            age_days = (time.time() - mtime) / 86400
            if age_days < BRAVE_CACHE_TTL_DAYS:
                with open(cache_path, "r") as f:
                    return json.load(f)
        except (OSError, json.JSONDecodeError):
            pass
    return None


def _set_brave_cache(query: str, results: list[dict]) -> None:
    """Cache Brave search results."""
    os.makedirs(BRAVE_CACHE_DIR, exist_ok=True)
    cache_key = hashlib.sha256(query.encode()).hexdigest()
    cache_path = os.path.join(BRAVE_CACHE_DIR, f"{cache_key}.json")
    try:
        with open(cache_path, "w") as f:
            json.dump(results, f)
    except OSError:
        pass


def brave_search(query: str, count: int = 10) -> list[dict]:
    """Call Brave Search API and return results. Uses a 30-day file cache."""
    cached = _get_brave_cache(query)
    if cached is not None:
        log.info(f"    Using cached Brave results for: {query[:60]}")
        return cached

    if not BRAVE_API_KEY:
        raise RuntimeError("BRAVE_API_KEY not set")
    resp = httpx.get(
        "https://api.search.brave.com/res/v1/web/search",
        params={"q": query, "count": count},
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "X-Subscription-Token": BRAVE_API_KEY,
        },
        timeout=10.0,
    )
    resp.raise_for_status()
    data = resp.json()
    results = data.get("web", {}).get("results", [])
    _set_brave_cache(query, results)
    return results


def google_search(query: str, count: int = 10) -> list[dict]:
    """Call Google Custom Search API as a fallback when Brave has no results.

    Returns results in the same format as brave_search for compatibility.
    """
    if not GOOGLE_CSE_API_KEY or not GOOGLE_CSE_CX:
        return []

    try:
        resp = httpx.get(
            "https://www.googleapis.com/customsearch/v1",
            params={
                "key": GOOGLE_CSE_API_KEY,
                "cx": GOOGLE_CSE_CX,
                "q": query,
                "num": min(count, 10),
            },
            timeout=10.0,
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        # Normalize to Brave-compatible format
        return [
            {
                "url": item.get("link", ""),
                "title": item.get("title", ""),
                "description": item.get("snippet", ""),
            }
            for item in items
        ]
    except Exception as e:
        log.warning(f"  Google Custom Search failed: {e}")
        return []


def _utility_name_words(name: str) -> list[str]:
    """Extract significant words from a utility name for matching."""
    cleaned = _clean_utility_name(name).lower()
    stop = {"of", "the", "and", "for", "in", "at", "by", "to", "a", "an",
            "city", "town", "village", "county", "electric", "power",
            "light", "energy", "utility", "utilities", "service", "services",
            "dept", "department", "board", "commission", "authority"}
    return [w for w in cleaned.split() if len(w) > 2 and w not in stop]


def score_search_result(result: dict, utility_name: str, utility_domain: str | None) -> float:
    """Score a Brave Search result for relevance to tariff/rate pages.
    Higher = better."""
    url = result.get("url", "")
    title = result.get("title", "")
    description = result.get("description", "")
    score = 0.0

    if utility_domain and is_same_domain(url, f"https://{utility_domain}"):
        score += 50

    path = urlparse(url).path.lower()
    for kw in ["rate", "pricing", "electric"]:
        if kw in path:
            score += 15
    if "residential" in path:
        score += 10
    if "commercial" in path or "business" in path:
        score += 5
    if "tariff" in path:
        score += 3
    if any(kw in path for kw in ["regulatory", "submission", "filing", "bcuc", "puc.", "docket"]):
        score -= 15
    if path.endswith(".pdf"):
        score -= 30

    combined_text = f"{title} {description}".lower()
    for kw in ["rate", "electric", "residential", "commercial", "pricing"]:
        if kw in combined_text:
            score += 3
    if "tariff" in combined_text and "rate" not in combined_text:
        score -= 5

    # Utility name matching — reward results that mention the target utility
    name_words = _utility_name_words(utility_name)
    if name_words:
        matches = sum(1 for w in name_words if w in combined_text)
        if matches >= 2:
            score += 25
        elif matches == 1 and len(name_words) <= 2:
            score += 15
        elif matches == 0:
            score -= 10

    if url_is_homepage(url):
        score -= 30

    url_domain = urlparse(url).netloc.replace("www.", "")
    if any(url_domain == d or url_domain.endswith(f".{d}") for d in THIRD_PARTY_DOMAINS):
        return -999  # Hard block — never use aggregator/comparison sites

    return score


def _try_direct_rate_pages(website_url: str) -> str | None:
    """Try common rate page URL patterns on the utility's known website."""
    if not website_url:
        return None
    base = website_url.rstrip("/")
    candidates = [
        f"{base}/rates",
        f"{base}/residential/rates",
        f"{base}/electric-rates",
        f"{base}/electricity-rates",
        f"{base}/residential-rates",
        f"{base}/my-account/rates",
        f"{base}/customer-service/rates",
        f"{base}/billing-rates",
        f"{base}/rates-and-tariffs",
    ]
    for url in candidates:
        content, _, status = fetch_page(url)
        if status == 200 and len(content.strip()) > 500:
            return url
    return None


CORP_SUFFIXES = re.compile(
    r"\s+(Co|Corp|Inc|LLC|LLP|Ltd|Company|Corporation|"
    r"Incorporated|Assn|Assoc|Association|Member|Coop|"
    r"PLC|LP|PA|NA)\.?$",
    re.IGNORECASE,
)


def _clean_utility_name(name: str) -> str:
    """Strip corporate suffixes for better search relevance."""
    cleaned = name
    for _ in range(3):
        cleaned = CORP_SUFFIXES.sub("", cleaned).strip().rstrip(",")
    return cleaned


def _discover_utility_domain(utility_name: str, state: str) -> str | None:
    """Find the utility's official website domain via search.

    Skips any domain in THIRD_PARTY_DOMAINS to avoid returning aggregator
    sites like energypal.com when searching for "Duke Energy".
    """
    clean_name = _clean_utility_name(utility_name)
    query = f"{clean_name} electric utility official website {state}"
    log.info(f"  Phase 1: Discovering domain [{query}]")
    try:
        results = brave_search(query, count=5)
    except Exception:
        return None

    name_lower = clean_name.lower().replace(" ", "")
    for r in results:
        url = r["url"]
        domain = urlparse(url).netloc.replace("www.", "")

        if any(domain == d or domain.endswith(f".{d}") for d in THIRD_PARTY_DOMAINS):
            continue

        domain_base = domain.split(".")[0] if "." in domain else domain
        name_words = [w.lower() for w in clean_name.split() if len(w) > 2]
        matching = sum(1 for w in name_words if w in domain_base)
        if matching >= 1 or name_lower[:6] in domain_base:
            log.info(f"  Phase 1: Discovered domain {domain} from {url[:60]}")
            return domain

    return None


def phase1_find_rate_page(utility_name: str, state: str, website_url: str | None) -> tuple[str, int, list[str]]:
    """Search for the utility's rate page. Returns (best_url, num_results, alt_urls)."""
    utility_domain = urlparse(website_url).netloc if website_url else None
    clean_name = _clean_utility_name(utility_name)
    total_searches = 0

    # If we don't know the utility's domain, discover it first
    if not utility_domain:
        utility_domain = _discover_utility_domain(utility_name, state)
        total_searches += 1
        if utility_domain:
            website_url = f"https://{utility_domain}"

    query = f'{clean_name} residential electric rates {state}'
    log.info(f"  Phase 1: Searching [{query}]")

    results = brave_search(query, count=10)
    total_searches += 1
    if not results:
        query_fallback = f"{clean_name} electricity rates"
        results = brave_search(query_fallback, count=10)
        total_searches += 1

    # Google Custom Search fallback when Brave finds nothing or only low-quality results
    scored = []
    if results:
        for r in results:
            s = score_search_result(r, utility_name, utility_domain)
            scored.append((s, r))
        scored.sort(key=lambda x: -x[0])

    if not scored or scored[0][0] < 10:
        log.info("  Phase 1: Brave results insufficient, trying Google Custom Search...")
        google_results = google_search(query, count=10)
        if google_results:
            for r in google_results:
                s = score_search_result(r, utility_name, utility_domain)
                scored.append((s, r))
            scored.sort(key=lambda x: -x[0])
            log.info(f"    Google returned {len(google_results)} additional results")

    # Drop hard-blocked results (3rd party aggregators)
    scored = [(s, r) for s, r in scored if s > -900]

    if not scored:
        return "", 0, []

    for score, r in scored:
        url = r["url"]
        log.info(f"    Candidate: score={score:.0f} {url[:80]}")

    best_score, best = scored[0]
    best_url = best["url"]

    # Build alternative URLs from the remaining results, filtering out news/media
    all_alt_urls = [
        r["url"] for s, r in scored[1:]
        if s > 0 and not any(kw in r["url"].lower() for kw in ["news", "media-centre", "press-release"])
    ]

    # If the top result isn't from the utility's own domain, try harder
    if utility_domain and not is_same_domain(best_url, f"https://{utility_domain}"):
        log.info(f"  Phase 1: Top result not from {utility_domain}, trying direct URL patterns...")

        direct_url = _try_direct_rate_pages(website_url)
        if direct_url:
            log.info(f"  Phase 1: Found direct rate page: {direct_url}")
            return direct_url, len(results), all_alt_urls

        site_query = f"site:{utility_domain} residential rates"
        log.info(f"  Phase 1: Trying site-scoped search [{site_query}]")
        site_results = brave_search(site_query, count=5)
        total_searches += 1
        if site_results:
            for sr in site_results:
                sr_url = sr["url"]
                if not url_is_homepage(sr_url):
                    _, _, sr_status = fetch_page(sr_url)
                    if sr_status == 200:
                        log.info(f"  Phase 1: Found via site search: {sr_url}")
                        return sr_url, len(results) + len(site_results), all_alt_urls

    if best_score < 10:
        log.warning(f"  Phase 1: Best result score too low ({best_score}), skipping")
        return "", len(results), []

    best_domain = urlparse(best_url).netloc.replace("www.", "")

    content, ctype, status = fetch_page(best_url)
    if status != 200:
        log.warning(f"  Phase 1: Best URL returned {status}, trying next")

        # If httpx can't connect (SSL errors, bot blocking), try Playwright
        if status == 0 and best_score >= 50:
            log.info(f"  Phase 1: httpx failed entirely — trying Playwright for {best_url[:60]}")
            html_js, title_js = fetch_page_js(best_url)
            if html_js and len(html_js.strip()) > 200:
                log.info(f"  Phase 1: Playwright succeeded for {best_url[:60]}")
                _js_rendered_domains.add(best_domain)
                return best_url, len(results), all_alt_urls

        for score, r in scored[1:3]:
            alt_url = r["url"]
            _, _, alt_status = fetch_page(alt_url)
            if alt_status == 200:
                best_url = alt_url
                break
        else:
            log.warning("  Phase 1: No reachable result found")
            return "", len(results), []

    log.info(f"  Phase 1: Selected {best_url} (+{len(all_alt_urls)} alternates)")
    return best_url, len(results), all_alt_urls


# ---------------------------------------------------------------------------
# Phase 2: Crawl rate page to discover tariff sub-pages
# ---------------------------------------------------------------------------

STRONG_RATE_SIGNAL = re.compile(
    r"electricity.rate|electric.rate|residential.rate|commercial.rate|"
    r"business.rate|rate.schedule|rate.tariff",
    re.IGNORECASE,
)


def _is_relevant_link(url: str, link_text: str, base_url: str) -> bool:
    """Return True if a link looks like a residential/commercial rate page we want."""
    if not url.startswith("http"):
        return False
    if not is_same_domain(url, base_url):
        return False
    if url_is_homepage(url):
        return False
    combined = f"{url} {link_text}"
    has_strong_rate_signal = bool(STRONG_RATE_SIGNAL.search(combined))
    if not has_strong_rate_signal and IRRELEVANT_URL_KEYWORDS.search(combined):
        return False
    if not is_rate_relevant_url(url, link_text):
        return False
    if SKIP_KEYWORDS.search(combined):
        return False
    return True


def _link_priority(url: str, text: str) -> int:
    """Lower = higher priority. Electricity-specific rate pages first."""
    combined = f"{url} {text}".lower()
    if "electricity" in combined and "rate" in combined:
        return 0
    if "residential" in combined and "rate" in combined:
        return 1
    if "commercial" in combined and "rate" in combined:
        return 1
    if "rate" in combined:
        return 2
    return 3


def _extract_links(soup: BeautifulSoup, base_url: str) -> list[tuple[str, str]]:
    """Extract (url, link_text) pairs for relevant rate page links, sorted by priority."""
    links = []
    seen = set()
    for a in soup.find_all("a", href=True):
        full_url = urljoin(base_url, a["href"]).split("#")[0]
        if full_url in seen:
            continue
        link_text = a.get_text(strip=True)
        if _is_relevant_link(full_url, link_text, base_url):
            seen.add(full_url)
            links.append((full_url, link_text))
    links.sort(key=lambda x: _link_priority(x[0], x[1]))
    return links


_js_rendered_domains: set[str] = set()
_js_rendered_lock = threading.Lock()

# Domains that always require a real browser (SSL issues, aggressive bot blocking, etc.)
_BROWSER_REQUIRED_DOMAINS = frozenset({
    "hydroquebec.com", "www.hydroquebec.com",
})


def _fetch_and_parse(url: str) -> RatePage | None:
    """Fetch an HTML page and return a RatePage with extracted text.
    Falls back to Playwright for JS-rendered sites, or browser agent for 403s."""
    domain = urlparse(url).netloc
    bare_domain = domain.replace("www.", "")
    _throttle_domain(bare_domain)

    # If we already know this domain needs a browser, go straight to Playwright
    with _js_rendered_lock:
        needs_browser = domain in _js_rendered_domains
    if needs_browser or bare_domain in _BROWSER_REQUIRED_DOMAINS:
        return _fetch_and_parse_js(url)

    content, ctype, status = fetch_page(url)
    if status == 403:
        log.info(f"    Got 403 on {url[:70]}, trying browser agent...")
        pages = _try_browser_agent_fallback(url)
        return pages[0] if pages else None
    if status == 0:
        log.info(f"    Connection failed for {url[:70]}, trying Playwright...")
        with _js_rendered_lock:
            _js_rendered_domains.add(domain)
        return _fetch_and_parse_js(url)
    if status != 200:
        return None
    soup = BeautifulSoup(content, "lxml")
    title = soup.title.string.strip() if soup.title and soup.title.string else ""
    text = _extract_text(soup)

    if len(text.strip()) < 200:
        log.info(f"    Thin content from httpx ({len(text.strip())} chars), trying Playwright...")
        with _js_rendered_lock:
            _js_rendered_domains.add(domain)
        return _fetch_and_parse_js(url)

    return RatePage(
        url=url,
        title=title,
        page_type="html",
        content=text,
        content_hash=hashlib.sha256(content.encode()).hexdigest(),
    )


def _fetch_and_parse_js(url: str) -> RatePage | None:
    """Fetch a page using headless Playwright and extract text."""
    html, title = fetch_page_js(url)
    if not html:
        return None
    soup = BeautifulSoup(html, "lxml")
    if not title:
        title = soup.title.string.strip() if soup.title and soup.title.string else ""
    text = _extract_text(soup)
    if len(text.strip()) < 50:
        return None
    return RatePage(
        url=url,
        title=title,
        page_type="html",
        content=text,
        content_hash=hashlib.sha256(html.encode()).hexdigest(),
    )


def _try_browser_agent_fallback(rate_page_url: str) -> list[RatePage]:
    """Use the browser interaction agent as a fallback for pages that block
    simple HTTP requests (403) or require JS interaction."""
    try:
        from scripts.browser_interaction import BrowserAgent
    except ImportError:
        log.warning("  Browser agent not available")
        return []

    log.info(f"  Phase 2: Trying browser interaction agent for {rate_page_url[:70]}")
    try:
        with BrowserAgent(headless=True) as agent:
            snapshots = agent.scrape_interactive_rate_page(rate_page_url)

        pages = []
        for snap in snapshots:
            if snap.text and len(snap.text.strip()) > 100:
                pages.append(RatePage(
                    url=snap.url,
                    title=snap.title,
                    page_type="html",
                    content=snap.text,
                    content_hash=hashlib.sha256(snap.text.encode()).hexdigest(),
                ))
        log.info(f"  Phase 2: Browser agent captured {len(pages)} content snapshots")
        return pages
    except Exception as e:
        log.warning(f"  Browser agent fallback failed: {e}")
        return []


def phase2_discover_tariff_pages(rate_page_url: str) -> list[RatePage]:
    """Crawl the main rate page and one level of sub-pages to find
    residential and small-commercial rate detail pages."""
    log.info(f"  Phase 2: Crawling {rate_page_url}")

    # Level 0: fetch the main rates page
    domain = urlparse(rate_page_url).netloc
    bare_domain = domain.replace("www.", "")

    # If this domain is known to need a browser, skip httpx entirely
    if bare_domain in _BROWSER_REQUIRED_DOMAINS or domain in _js_rendered_domains:
        log.info(f"  Phase 2: Browser-required domain, using Playwright directly...")
        html_js, title_js = fetch_page_js(rate_page_url)
        if not html_js:
            log.warning(f"  Phase 2: Playwright also failed for {rate_page_url[:60]}")
            return []
        content = html_js
        status = 200
    else:
        content, ctype, status = fetch_page(rate_page_url)

    if status != 200:
        if status == 403:
            log.info(f"  Phase 2: Got 403 — falling back to browser agent")
            return _try_browser_agent_fallback(rate_page_url)
        if status == 0:
            log.info(f"  Phase 2: httpx connection failed — trying Playwright...")
            html_js, title_js = fetch_page_js(rate_page_url)
            if html_js and len(html_js.strip()) > 200:
                content = html_js
                _js_rendered_domains.add(domain)
            else:
                log.warning(f"  Phase 2: Failed to fetch rate page (status={status})")
                return []
        else:
            log.warning(f"  Phase 2: Failed to fetch rate page (status={status})")
            return []

    soup = BeautifulSoup(content, "lxml")
    text = _extract_text(BeautifulSoup(content, "lxml"))

    # If httpx returned thin content OR domain is known to need JS, use Playwright
    needs_playwright = len(text.strip()) < 200 or domain in _js_rendered_domains
    if needs_playwright:
        if domain not in _js_rendered_domains:
            log.info(f"  Phase 2: Thin httpx content, retrying main page with Playwright...")
        else:
            log.info(f"  Phase 2: Known JS-rendered domain, using Playwright...")
        _js_rendered_domains.add(domain)
        html_js, title_js = fetch_page_js(rate_page_url)
        if html_js:
            content = html_js
            soup = BeautifulSoup(content, "lxml")
            text = _extract_text(BeautifulSoup(content, "lxml"))

    main_page = RatePage(
        url=rate_page_url,
        title=soup.title.string.strip() if soup.title and soup.title.string else "",
        page_type="html",
        content=text,
        content_hash=hashlib.sha256(content.encode()).hexdigest(),
    )

    pages: list[RatePage] = [main_page]
    seen_urls = {rate_page_url}

    # Extract links from a FRESH soup — _extract_text destroys the soup in-place
    link_soup = BeautifulSoup(content, "lxml")
    level1_links = _extract_links(link_soup, rate_page_url)
    log.info(f"  Phase 2: Found {len(level1_links)} relevant links on main page")

    MAX_LEVEL1 = 15
    if len(level1_links) > MAX_LEVEL1:
        log.info(f"  Phase 2: Capping level 1 links to {MAX_LEVEL1} (from {len(level1_links)})")
        level1_links = level1_links[:MAX_LEVEL1]

    level2_candidates: list[tuple[str, str]] = []

    for url, link_text in level1_links:
        if url in seen_urls:
            continue
        seen_urls.add(url)

        if url.lower().endswith(".pdf"):
            log.info(f"    Extracting PDF: {url[:70]}")
            raw_bytes = _download_pdf(url)
            pdf_text = ""
            if raw_bytes:
                pdf_text = _extract_pdf_pdfplumber(raw_bytes)
                if len(pdf_text.strip()) < 200:
                    pdf_text = _extract_pdf_ocr(raw_bytes) or ""
            pages.append(RatePage(
                url=url, title=link_text, page_type="pdf",
                content=pdf_text,
                content_hash=hashlib.sha256(pdf_text.encode()).hexdigest() if pdf_text else "",
                pdf_bytes=raw_bytes,
            ))
            time.sleep(0.3)
            continue

        page = _fetch_and_parse(url)
        if not page:
            continue
        if not page.title:
            page.title = link_text
        pages.append(page)

        # Level 2: discover deeper links from this sub-page
        sub_soup = BeautifulSoup(
            page.content, "lxml"
        ) if "<" in page.content[:50] else None
        if not sub_soup:
            link_domain = urlparse(url).netloc
            link_bare = link_domain.replace("www.", "")
            if link_bare in _BROWSER_REQUIRED_DOMAINS or link_domain in _js_rendered_domains:
                sub_html, _ = fetch_page_js(url)
                if sub_html:
                    sub_soup = BeautifulSoup(sub_html, "lxml")
            else:
                sub_content, _, sub_status = fetch_page(url)
                if sub_status == 200:
                    sub_soup = BeautifulSoup(sub_content, "lxml")
        if sub_soup:
            for sub_url, sub_text in _extract_links(sub_soup, url):
                if sub_url not in seen_urls:
                    level2_candidates.append((sub_url, sub_text))

        time.sleep(0.3)

    # Level 2: fetch the deeper pages (capped to avoid runaway crawling)
    MAX_LEVEL2 = 10
    fetched_l2 = 0
    for url, link_text in level2_candidates:
        if url in seen_urls:
            continue
        if fetched_l2 >= MAX_LEVEL2:
            break
        seen_urls.add(url)

        if url.lower().endswith(".pdf"):
            log.info(f"    Extracting PDF (L2): {url[:70]}")
            raw_bytes = _download_pdf(url)
            pdf_text = ""
            if raw_bytes:
                pdf_text = _extract_pdf_pdfplumber(raw_bytes)
                if len(pdf_text.strip()) < 200:
                    pdf_text = _extract_pdf_ocr(raw_bytes) or ""
            pages.append(RatePage(
                url=url, title=link_text, page_type="pdf",
                content=pdf_text,
                content_hash=hashlib.sha256(pdf_text.encode()).hexdigest() if pdf_text else "",
                pdf_bytes=raw_bytes,
            ))
            fetched_l2 += 1
            time.sleep(0.3)
            continue

        page = _fetch_and_parse(url)
        if page:
            if not page.title:
                page.title = link_text
            pages.append(page)
            fetched_l2 += 1
        time.sleep(0.3)

    html_with_content = sum(1 for p in pages if p.page_type == "html" and p.content)
    log.info(
        f"  Phase 2: {len(pages)} total pages "
        f"({html_with_content} HTML with content, "
        f"{sum(1 for p in pages if p.page_type == 'pdf')} PDFs)"
    )
    return pages


def _extract_text(soup: BeautifulSoup) -> str:
    for tag in soup(["script", "style", "nav", "footer", "header", "noscript", "iframe"]):
        tag.decompose()
    for tag in soup.find_all(True, class_=re.compile(
        r"menu|mega-?nav|side-?bar|side-?nav|breadcrumb|skip-link", re.IGNORECASE
    )):
        tag.decompose()
    for tag in soup.find_all(True, id=re.compile(
        r"menu|mega-?nav|side-?bar|side-?nav|navigation", re.IGNORECASE
    )):
        tag.decompose()

    main = soup.find("main") or soup.find("article") or soup.find("div", {"role": "main"})
    if not main:
        main = soup.find("div", class_=re.compile(r"content|main|body", re.IGNORECASE))

    # Fallback: find the largest ContentBlock-style div with substantial text
    if not main or len(main.get_text(strip=True)) < 200:
        best_div = None
        best_len = 0
        for div in soup.find_all("div", class_=True):
            cls = " ".join(div.get("class", []))
            if re.search(r"content", cls, re.IGNORECASE):
                text_len = len(div.get_text(strip=True))
                if 200 < text_len < 10000 and text_len > best_len:
                    best_div = div
                    best_len = text_len
        if best_div:
            main = best_div

    el = main if main else soup
    return el.get_text(separator="\n", strip=True)[:15000]


RATE_CONTENT_SIGNALS = re.compile(
    r"\$/kwh|cents/kwh|per kwh|\bkwh\b.*\d|"
    r"\$/kw[^h]|\$/month|\bcharge\b.*\$|\$.*\bcharge\b|"
    r"rate.*schedule|schedule.*rate|"
    r"energy charge|demand charge|service charge|"
    r"basic charge|customer charge|delivery charge|"
    r"residential.*rate|commercial.*rate|general.*service|"
    r"tier\s*[12]|step\s*[12]|block\s*[12]|"
    r"on.peak|off.peak|shoulder|"
    r"summer.*rate|winter.*rate|seasonal",
    re.IGNORECASE,
)


_RESIDENTIAL_SIGNAL = re.compile(
    r"\bdomestic\b|residential\s+(?:service|rate|customer)|"
    r"rate\s+no\.\s*1|schedule\s+(?:r|rs|d|ds)\b|"
    r"\b[12]\.\d\s+domestic\b",
    re.IGNORECASE,
)


def _compress_whitespace(text: str) -> str:
    """Collapse runs of blank lines and excess whitespace to reduce token count."""
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{4,}", "  ", text)
    text = re.sub(r"(\n\s*){3,}", "\n\n", text)
    return text.strip()


def _select_rate_content(text: str, max_chars: int = 20000) -> str:
    """For long documents (especially PDFs), extract a contiguous window
    of text starting from the first residential/domestic rate section."""
    text = _compress_whitespace(text)
    if len(text) <= max_chars:
        return text

    # Strategy 1: find the first residential/domestic/general-service section
    # that contains actual rate values (skip TOC entries and wholesale sections)
    _per_unit = re.compile(r"per\s+(?:month|kWh|kW)", re.IGNORECASE)
    best_start = None
    for m in _RESIDENTIAL_SIGNAL.finditer(text):
        nearby = text[m.start():min(len(text), m.start() + 1500)]
        if _RATE_AMOUNT_RE.search(nearby) and _per_unit.search(nearby):
            best_start = max(0, m.start() - 200)
            break

    if best_start is not None:
        start = best_start
        end = start + max_chars
        if end > len(text):
            end = len(text)
            start = max(0, end - max_chars)
        selected = text[start:end]
        if start > 0:
            selected = "[...document truncated...]\n" + selected
        if end < len(text):
            selected += "\n[...document truncated...]"
        return selected

    # Strategy 2 (fallback): sliding window maximizing rate signal density
    block = 500
    n_blocks = (len(text) + block - 1) // block
    scores = []
    for i in range(n_blocks):
        chunk = text[i * block:(i + 1) * block]
        s = 0
        if _RATE_AMOUNT_RE.search(chunk):
            s += 3
        if _RATE_KEYWORD_RE.search(chunk):
            s += 2
        if _RATE_SCHEDULE_RE.search(chunk):
            s += 2
        scores.append(s)

    window_blocks = max_chars // block
    if window_blocks >= n_blocks:
        return text

    best_start = 0
    best_score = sum(scores[:window_blocks])
    current_score = best_score
    for start in range(1, n_blocks - window_blocks + 1):
        current_score += scores[start + window_blocks - 1] - scores[start - 1]
        if current_score > best_score:
            best_score = current_score
            best_start = start

    start_char = best_start * block
    end_char = start_char + max_chars
    selected = text[start_char:end_char]
    if start_char > 0:
        selected = "[...document truncated...]\n" + selected
    if end_char < len(text):
        selected += "\n[...document truncated...]"
    return selected


def _page_has_rate_content(text: str) -> bool:
    """Quick heuristic: does this page text look like it has rate data?"""
    return bool(RATE_CONTENT_SIGNALS.search(text))


# ---------------------------------------------------------------------------
# Phase 3: LLM extraction of tariff data
# ---------------------------------------------------------------------------

EXTRACTION_PROMPT = """Extract ONLY residential and small/general commercial electricity tariffs from this page.

INCLUDE: residential rates, small business / general service rates, default service rates
SKIP: industrial, large commercial/power, irrigation, fleet, street lighting, transmission, wholesale, interruptible, standby

For each tariff, provide:
- name: Official schedule name (e.g. "Residential Service", "Schedule GS-1")
- code: Schedule code if shown (e.g. "RS", "GS-1")
- customer_class: "residential" or "commercial"
- rate_type: One of: flat, tiered, tou, demand, seasonal, tou_tiered, seasonal_tou, seasonal_tiered, demand_tou, complex
- description: One-sentence description of who this applies to
- effective_date: YYYY-MM-DD if shown, otherwise ""
- confidence: 0.0-1.0 how confident you are the extracted rate values are correct
- components: Array of rate components. For EACH tier/period/season, include:
    - component_type: "energy" | "demand" | "fixed" | "minimum" | "adjustment"
    - unit: "$/kWh", "$/kW", "$/month", "cents/kWh" etc.
    - rate_value: Exact numeric value from the page (e.g. 0.0956). Convert cents to dollars.
    - tier_min_kwh / tier_max_kwh: For tiered rates (null otherwise)
    - tier_label: e.g. "Step 1", "First 1000 kWh"
    - period_label: For TOU, e.g. "On-Peak", "Off-Peak" (null otherwise)
    - season: "Summer" / "Winter" etc. (null if not seasonal)

Rules:
- Include ALL tiers, periods, and seasonal variations as separate component entries
- Use exact numbers from the page — do NOT estimate or round
- ONLY include tariffs that have actual numeric rate values ($/kWh, cents/kWh, $/month, $/kW etc.)
- Skip table-of-contents entries, index listings, or schedule names that lack rate values
- If the document has a table of contents AND detailed rate schedules, extract from the DETAILED sections
- If no relevant tariffs with rate values on this page, call the tool with an empty tariffs array
- Convert all rates to $/kWh (divide cents by 100)
- Set confidence to 0.9+ if values are clearly readable, 0.5-0.8 if some ambiguity, below 0.5 if guessing

EXAMPLES:

Example 1 — Simple flat rate:
Input: "Residential Service (RS): Customer charge $12.50/month. Energy charge 9.56 cents/kWh. Effective Jan 1, 2025."
Output: one tariff named "Residential Service", code "RS", class "residential", type "flat", confidence 0.95, with a fixed component ($12.50/month) and an energy component ($0.0956/kWh).

Example 2 — Tiered rate:
Input: "Schedule R: Basic charge $8.00/mo. First 500 kWh: $0.085/kWh. Over 500 kWh: $0.105/kWh."
Output: one tariff type "tiered", confidence 0.9, with a fixed component ($8.00), energy tier 1 (0-500 kWh at $0.085), energy tier 2 (500+ kWh at $0.105).

Example 3 — Seasonal TOU:
Input: "Rate TOU-D: Summer On-Peak (2pm-8pm) $0.35/kWh, Off-Peak $0.12/kWh. Winter On-Peak $0.22/kWh, Off-Peak $0.10/kWh. Service charge $10/mo."
Output: one tariff type "seasonal_tou", confidence 0.9, with fixed ($10), and 4 energy components with season+period_label combinations.

Use the store_tariffs tool to return your results.

Page URL: {url}
Page title: {title}

Content:
{content}"""


def _merge_prefix_duplicates(tariffs: list[ExtractedTariff]) -> list[ExtractedTariff]:
    """Merge tariffs where one name is a prefix of another (same customer class).

    E.g. "Domestic Service" (1 adjustment) and "Domestic Service Tariff"
    (3 full components) are the same plan at different detail levels —
    keep the one with the most components.
    """
    if len(tariffs) <= 1:
        return tariffs

    groups: dict[str, list[ExtractedTariff]] = {}
    for t in tariffs:
        groups.setdefault(t.customer_class, []).append(t)

    merged: list[ExtractedTariff] = []
    for cc, group in groups.items():
        norms = [(_normalize_tariff_name(t.name), t) for t in group]
        absorbed: set[int] = set()

        for i, (norm_i, t_i) in enumerate(norms):
            if i in absorbed:
                continue
            for j, (norm_j, t_j) in enumerate(norms):
                if j <= i or j in absorbed:
                    continue
                if norm_i == norm_j:
                    # Exact normalized match — keep the one with more components
                    if len(t_i.components) >= len(t_j.components):
                        absorbed.add(j)
                    else:
                        absorbed.add(i)
                    continue
                is_prefix = norm_i.startswith(norm_j) or norm_j.startswith(norm_i)
                if not is_prefix:
                    continue
                # One is a prefix of the other — keep the richer one
                if len(t_i.components) >= len(t_j.components):
                    absorbed.add(j)
                    log.info(f"    Merged duplicate: '{t_j.name}' ({len(t_j.components)} comp) "
                             f"absorbed by '{t_i.name}' ({len(t_i.components)} comp)")
                else:
                    absorbed.add(i)
                    log.info(f"    Merged duplicate: '{t_i.name}' ({len(t_i.components)} comp) "
                             f"absorbed by '{t_j.name}' ({len(t_j.components)} comp)")
                    break  # t_i is absorbed, stop comparing it

        for k, (_, t) in enumerate(norms):
            if k not in absorbed:
                merged.append(t)

    if len(merged) < len(tariffs):
        log.info(f"    Fuzzy dedup: {len(tariffs)} -> {len(merged)} tariffs "
                 f"({len(tariffs) - len(merged)} duplicates merged)")
    return merged


PDF_VISION_EXTRACTION_PROMPT = """Extract all residential and commercial electricity tariffs visible in these PDF page images.

For each tariff provide: name, code, customer_class ("residential"/"commercial"), rate_type, description, effective_date, confidence (0-1), and components array.
Each component needs: component_type ("energy"/"demand"/"fixed"/"minimum"/"adjustment"), unit, rate_value, and optional tier_min_kwh, tier_max_kwh, tier_label, period_label, season.

Rules:
- Read numbers exactly as shown — do NOT estimate
- Convert cents to dollars (divide by 100)
- Include ALL tiers, periods, seasonal variations
- Skip industrial/lighting/irrigation/wholesale tariffs
- Set confidence 0.9+ if values clearly readable, 0.5-0.8 if some ambiguity

Use the store_tariffs tool to return results."""

MAX_PDF_VISION_PAGES = 15


def _extract_pdf_vision(pdf_bytes: bytes, page_url: str) -> tuple[list[ExtractedTariff], int]:
    """Send PDF pages as images to Claude vision for extraction.

    Returns (tariffs, llm_call_count). Falls back to empty list if conversion fails.
    """
    try:
        from pdf2image import convert_from_bytes
    except ImportError:
        log.warning("    pdf2image not available for vision extraction")
        return [], 0

    try:
        images = convert_from_bytes(pdf_bytes, first_page=1, last_page=MAX_PDF_VISION_PAGES, dpi=150)
    except Exception as e:
        log.warning(f"    PDF to image conversion failed: {e}")
        return [], 0

    if not images:
        return [], 0

    log.info(f"    PDF vision: sending {len(images)} page images to Claude")

    import base64
    import io

    content_blocks = [{"type": "text", "text": PDF_VISION_EXTRACTION_PROMPT}]
    for i, img in enumerate(images):
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=85)
        b64 = base64.standard_b64encode(buf.getvalue()).decode("ascii")
        content_blocks.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": b64,
            },
        })

    client = _get_anthropic_client()
    try:
        resp = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=8192,
            messages=[{"role": "user", "content": content_blocks}],
            tools=[TARIFF_EXTRACTION_TOOL],
            tool_choice={"type": "tool", "name": "store_tariffs"},
        )
    except Exception as e:
        log.error(f"    PDF vision API call failed: {e}")
        return [], 1

    for block in resp.content:
        if block.type == "tool_use" and block.name == "store_tariffs":
            raw = block.input.get("tariffs", [])
            return _parse_extraction_response(raw, page_url), 1

    log.warning("    No tool_use block in PDF vision response")
    return [], 1


_COMPLEXITY_SIGNALS = re.compile(
    r"tier|step|block|on.peak|off.peak|shoulder|summer|winter|"
    r"seasonal|schedule\s+[a-z]|rate\s+[a-z]",
    re.IGNORECASE,
)

TWOPASS_IDENTIFY_PROMPT = """List all residential and commercial electricity rate plans/tariffs in this document.

For each tariff, provide ONLY:
- name: The official name or schedule code
- customer_class: "residential" or "commercial"
- location_hint: A short phrase (5-10 words) from the text near where the rate details appear

SKIP: industrial, large commercial, lighting, irrigation, wholesale, interruptible

Return a JSON array of objects with keys: name, customer_class, location_hint
If no relevant tariffs, return [].
Return ONLY valid JSON. No markdown, no explanation.

Content:
{content}"""

TWOPASS_EXTRACT_PROMPT = """Extract the complete rate details for the tariff named "{tariff_name}" ({customer_class}) from the following content.

Provide:
- name, code, customer_class, rate_type, description, effective_date, confidence
- components: ALL tiers, periods, seasons as separate entries with component_type, unit, rate_value, tier_min_kwh, tier_max_kwh, tier_label, period_label, season

Rules:
- Use exact numbers — do NOT estimate or round
- Convert cents to dollars (divide by 100)
- confidence: 0.9+ if clear, 0.5-0.8 if ambiguous, <0.5 if guessing

Use the store_tariffs tool to return your result (array with one tariff).

Content:
{content}"""


def _is_complex_page(content: str) -> bool:
    """Detect pages likely to benefit from two-pass extraction."""
    if len(content) < 8000:
        return False
    signals = len(_COMPLEXITY_SIGNALS.findall(content))
    return signals >= 5


def _extract_two_pass(page: RatePage, utility_name: str) -> tuple[list[ExtractedTariff], int]:
    """Two-pass extraction: identify tariffs first, then extract each individually.

    Returns (tariffs, llm_call_count).
    """
    content_for_llm = _select_rate_content(page.content, max_chars=25000)
    llm_calls = 0

    # Pass 1: Identify all tariff names
    identify_prompt = TWOPASS_IDENTIFY_PROMPT.format(content=content_for_llm[:15000])
    try:
        raw_text = _call_claude(identify_prompt)
        llm_calls += 1
        identified = _parse_text_response(raw_text)
    except Exception as e:
        log.error(f"    Two-pass identification failed: {e}")
        return [], llm_calls

    if not identified:
        return [], llm_calls

    relevant = [
        t for t in identified
        if isinstance(t, dict) and t.get("customer_class") in VALID_CLASSES
    ]
    log.info(f"    Two-pass: identified {len(relevant)} relevant tariffs")

    # Pass 2: Extract each tariff individually
    all_tariffs: list[ExtractedTariff] = []
    for item in relevant[:8]:
        name = item.get("name", "Unknown")
        cc = item.get("customer_class", "residential")
        hint = item.get("location_hint", "")

        # Find the relevant section around the location hint
        section = content_for_llm
        if hint and len(content_for_llm) > 4000:
            hint_lower = hint.lower()
            idx = content_for_llm.lower().find(hint_lower[:30])
            if idx >= 0:
                start = max(0, idx - 1500)
                end = min(len(content_for_llm), idx + 3000)
                section = content_for_llm[start:end]

        extract_prompt = TWOPASS_EXTRACT_PROMPT.format(
            tariff_name=name,
            customer_class=cc,
            content=section,
        )

        try:
            raw_tariffs = _call_claude_tool(extract_prompt)
            tariffs = _parse_extraction_response(raw_tariffs, page.url)
            llm_calls += 1
            for t in tariffs:
                t.source_url = page.url
                all_tariffs.append(t)
        except Exception as e:
            log.warning(f"    Two-pass extraction failed for '{name}': {e}")
            llm_calls += 1

        time.sleep(0.5)

    return all_tariffs, llm_calls


def phase3_extract_tariffs(pages: list[RatePage], utility_name: str) -> list[ExtractedTariff]:
    """Use Claude to extract structured tariff data from each rate page.

    Detail pages are processed first so they win dedup over overview pages
    that only list tariff names without rate values.

    Complex pages (long content with many rate signals) use a two-pass
    approach: identify tariffs first, then extract each individually.
    """
    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    def _depth(p: RatePage) -> int:
        return -urlparse(p.url).path.count("/")

    sorted_pages = sorted(pages, key=_depth)

    MAX_LLM_CALLS = 12
    all_tariffs: dict[str, ExtractedTariff] = {}
    llm_calls = 0

    for page in sorted_pages:
        if not page.content or len(page.content.strip()) < 100:
            log.info(f"    Skipping {page.url[:60]} (no/little content)")
            continue
        if SKIP_KEYWORDS.search(f"{page.url} {page.title}"):
            log.info(f"    Skipping {page.title or page.url[:60]} (irrelevant category)")
            continue
        if not _page_has_rate_content(page.content):
            log.info(f"    Skipping {page.title or page.url[:60]} (no rate content signals)")
            continue
        if llm_calls >= MAX_LLM_CALLS:
            log.info(f"    Stopping: reached {MAX_LLM_CALLS} LLM call limit")
            break

        log.info(f"  Phase 3: Extracting from {page.url[:80]}")

        # PDF vision: send pages as images for better table/layout preservation
        if page.page_type == "pdf" and page.pdf_bytes and len(page.pdf_bytes) > 100:
            log.info(f"    Using PDF vision extraction ({len(page.pdf_bytes)} bytes)")
            tariffs, calls = _extract_pdf_vision(page.pdf_bytes, page.url)
            llm_calls += calls
            accepted = 0
            for t in tariffs:
                if SKIP_KEYWORDS.search(t.name):
                    continue
                if t.customer_class not in VALID_CLASSES:
                    continue
                key = f"{t.name}|{t.customer_class}"
                t.source_url = page.url
                existing = all_tariffs.get(key)
                if existing and len(existing.components) >= len(t.components):
                    continue
                all_tariffs[key] = t
                accepted += 1
            if accepted > 0:
                log.info(f"    Extracted {accepted} tariffs via PDF vision from {page.url[:60]}")
                time.sleep(1)
                continue
            elif page.content:
                log.info(f"    Vision returned no usable tariffs, falling back to text extraction")
            else:
                log.info(f"    Vision returned no usable tariffs and no text content available")
                continue

        # Use two-pass for complex pages (long PDFs with many rate structures)
        if _is_complex_page(page.content):
            log.info(f"    Using two-pass extraction (complex page, {len(page.content)} chars)")
            tariffs, calls = _extract_two_pass(page, utility_name)
            llm_calls += calls
        else:
            content_for_llm = _select_rate_content(page.content, max_chars=20000)
            prompt = EXTRACTION_PROMPT.format(
                url=page.url,
                title=page.title,
                content=content_for_llm,
            )
            try:
                raw_tariffs, model_used = _extract_with_model_routing(prompt, page)
                tariffs = _parse_extraction_response(raw_tariffs, page.url)
                log.info(f"    Model used: {model_used}")
            except Exception as e:
                log.error(f"    Extraction failed: {e}")
                continue
            llm_calls += 1

        accepted = 0
        for t in tariffs:
            if SKIP_KEYWORDS.search(t.name):
                log.info(f"      Filtered out: {t.name} (irrelevant tariff)")
                continue
            if t.customer_class not in VALID_CLASSES:
                log.info(f"      Filtered out: {t.name} (class={t.customer_class}, not residential/commercial)")
                continue

            key = f"{t.name}|{t.customer_class}"
            t.source_url = page.url
            existing = all_tariffs.get(key)
            if existing and len(existing.components) >= len(t.components):
                continue
            all_tariffs[key] = t
            accepted += 1

        log.info(f"    Extracted {accepted} tariffs from {page.url[:60]}")
        time.sleep(1)

    # Drop tariffs with 0 components — they're just index entries
    # from overview pages with no actual rate data
    result = [t for t in all_tariffs.values() if t.components]
    dropped = len(all_tariffs) - len(result)
    if dropped:
        log.info(f"    Dropped {dropped} tariffs with no rate components (overview-only entries)")

    # Fuzzy name merge: if one tariff name is a prefix of another
    # (e.g. "Domestic Service" vs "Domestic Service Tariff"),
    # keep only the one with the most components.
    result = _merge_prefix_duplicates(result)

    return result


TARIFF_EXTRACTION_TOOL = {
    "name": "store_tariffs",
    "description": "Store extracted electricity tariff data from the page content.",
    "input_schema": {
        "type": "object",
        "properties": {
            "tariffs": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Official schedule name"},
                        "code": {"type": "string", "description": "Schedule code if shown (e.g. RS, GS-1)"},
                        "customer_class": {"type": "string", "enum": ["residential", "commercial"]},
                        "rate_type": {
                            "type": "string",
                            "enum": [
                                "flat", "tiered", "tou", "demand", "seasonal",
                                "tou_tiered", "seasonal_tou", "seasonal_tiered",
                                "demand_tou", "complex",
                            ],
                        },
                        "description": {"type": "string", "description": "One-sentence description"},
                        "effective_date": {"type": "string", "description": "YYYY-MM-DD or empty string"},
                        "confidence": {
                            "type": "number",
                            "description": "0.0-1.0 confidence that the extracted rate values are correct",
                        },
                        "components": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "component_type": {
                                        "type": "string",
                                        "enum": ["energy", "demand", "fixed", "minimum", "adjustment"],
                                    },
                                    "unit": {"type": "string"},
                                    "rate_value": {"type": "number"},
                                    "tier_min_kwh": {"type": ["number", "null"]},
                                    "tier_max_kwh": {"type": ["number", "null"]},
                                    "tier_label": {"type": ["string", "null"]},
                                    "period_label": {"type": ["string", "null"]},
                                    "season": {"type": ["string", "null"]},
                                },
                                "required": ["component_type", "unit", "rate_value"],
                            },
                        },
                    },
                    "required": ["name", "customer_class", "rate_type", "components", "confidence"],
                },
            },
        },
        "required": ["tariffs"],
    },
}


def _get_anthropic_client():
    """Lazy-init Anthropic client."""
    import anthropic
    client = getattr(_thread_local, "anthropic_client", None)
    if client is None:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        _thread_local.anthropic_client = client
    return client


def _call_claude(prompt: str) -> str:
    """Legacy text-only Claude call (used by two-pass identification step)."""
    client = _get_anthropic_client()
    resp = client.messages.create(
        model=HAIKU_MODEL,
        max_tokens=8192,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.content[0].text


_CACHED_SYSTEM_PROMPT = EXTRACTION_PROMPT.split("Page URL:")[0].strip()


def _call_claude_tool(prompt: str) -> list[dict]:
    """Call Claude with tool use for structured tariff extraction.

    Uses Anthropic prompt caching: the static system prompt and tool schema
    are marked ephemeral so repeated calls within a session pay only 10%
    of normal input cost for the cached portion.

    Returns the parsed tariff dicts directly from the tool call input,
    guaranteed to match the schema. Falls back to text parsing on error.
    """
    client = _get_anthropic_client()
    resp = client.messages.create(
        model=HAIKU_MODEL,
        max_tokens=8192,
        system=[
            {
                "type": "text",
                "text": _CACHED_SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": prompt}],
        tools=[
            {
                **TARIFF_EXTRACTION_TOOL,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        tool_choice={"type": "tool", "name": "store_tariffs"},
    )
    for block in resp.content:
        if block.type == "tool_use" and block.name == "store_tariffs":
            return block.input.get("tariffs", [])

    log.warning("    No tool_use block in Claude response, falling back to text parse")
    for block in resp.content:
        if hasattr(block, "text"):
            return _parse_text_response(block.text)
    return []


def _parse_text_response(text: str) -> list[dict]:
    """Fallback parser for raw text responses."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```\w*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    try:
        raw = json.loads(text)
    except json.JSONDecodeError:
        log.warning(f"    Failed to parse JSON from LLM response: {text[:200]}")
        return []
    if not isinstance(raw, list):
        raw = [raw]
    return [item for item in raw if isinstance(item, dict)]


def _get_gemini_client():
    """Lazy-init Google GenAI client (new SDK) with a 30s HTTP timeout."""
    client = getattr(_thread_local, "gemini_client", None)
    if client is None:
        from google import genai
        client = genai.Client(
            api_key=GOOGLE_AI_API_KEY,
            http_options={"timeout": 30_000},
        )
        _thread_local.gemini_client = client
    return client


# Circuit breaker: after N consecutive Gemini failures in a single process,
# stop trying Gemini and go straight to Haiku for the rest of the run.
_GEMINI_MAX_CONSECUTIVE_FAILURES = 3
_gemini_consecutive_failures = 0
_gemini_circuit_lock = threading.Lock()


def _gemini_circuit_open() -> bool:
    """Returns True if Gemini has failed too many times and should be skipped."""
    with _gemini_circuit_lock:
        return _gemini_consecutive_failures >= _GEMINI_MAX_CONSECUTIVE_FAILURES


def _gemini_record_success():
    global _gemini_consecutive_failures
    with _gemini_circuit_lock:
        _gemini_consecutive_failures = 0


def _gemini_record_failure():
    global _gemini_consecutive_failures
    with _gemini_circuit_lock:
        _gemini_consecutive_failures += 1
        if _gemini_consecutive_failures == _GEMINI_MAX_CONSECUTIVE_FAILURES:
            log.warning(
                f"    Gemini circuit breaker OPEN after {_GEMINI_MAX_CONSECUTIVE_FAILURES} "
                "consecutive failures — falling back to Haiku for remaining pages"
            )


def _call_gemini(prompt: str) -> list[dict]:
    """Call Gemini Flash for structured tariff extraction.
    Uses the google-genai SDK with JSON schema enforcement.
    Has a 30s timeout and feeds into a circuit breaker on repeated failures.
    Returns list of tariff dicts matching the same schema as Claude tool use."""
    from google.genai import types

    client = _get_gemini_client()
    try:
        resp = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                temperature=0.0,
                max_output_tokens=8192,
            ),
        )
        raw = json.loads(resp.text)
        tariffs = raw.get("tariffs", []) if isinstance(raw, dict) else []
        if tariffs:
            _gemini_record_success()
        return tariffs
    except Exception as e:
        _gemini_record_failure()
        log.warning(f"    Gemini extraction failed: {e}")
        return []


def _select_model(page: "RatePage") -> str:
    """Choose which LLM to use based on page characteristics.
    Returns 'gemini' or 'haiku'. Respects the circuit breaker."""
    if not GOOGLE_AI_API_KEY:
        return "haiku"

    if _gemini_circuit_open():
        return "haiku"

    if page.page_type == "pdf" and page.pdf_bytes:
        return "haiku"

    if page.content and _is_complex_page(page.content):
        return "haiku"

    return "gemini"


def _call_opus_tool(prompt: str) -> list[dict]:
    """Call Claude Opus 4.6 for structured tariff extraction.

    Only used as the last-resort third tier when both Gemini 3 Flash
    and Haiku fail to extract any tariffs.  More expensive but
    significantly better at complex rate structures, PDFs, and edge cases.
    """
    client = _get_anthropic_client()
    try:
        resp = client.messages.create(
            model=OPUS_MODEL,
            max_tokens=8192,
            system=[
                {
                    "type": "text",
                    "text": _CACHED_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": prompt}],
            tools=[
                {
                    **TARIFF_EXTRACTION_TOOL,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            tool_choice={"type": "tool", "name": "store_tariffs"},
        )
        for block in resp.content:
            if block.type == "tool_use" and block.name == "store_tariffs":
                return block.input.get("tariffs", [])
        log.warning("    No tool_use block in Opus response, falling back to text parse")
        for block in resp.content:
            if hasattr(block, "text"):
                return _parse_text_response(block.text)
        return []
    except Exception as e:
        log.warning(f"    Opus extraction failed: {e}")
        return []


def _extract_with_model_routing(prompt: str, page: "RatePage") -> tuple[list[dict], str]:
    """Extract tariffs using a 3-tier model strategy.

    Tier 1: Gemini 3 Flash  (fast, cheap, good for most pages)
    Tier 2: Claude Haiku    (better at complex HTML, tool use)
    Tier 3: Claude Opus 4.6 (last resort for pages both cheaper models fail on)

    Returns (tariff_dicts, model_used). Uses LLM extraction cache to avoid
    redundant API calls.
    """
    model = _select_model(page)

    if page.content_hash:
        cached = _get_llm_cache(page.content_hash, model)
        if cached is not None:
            log.info(f"    Using cached {model} extraction ({len(cached)} tariffs)")
            return cached, model

    if model == "gemini":
        result = _call_gemini(prompt)
        if result:
            if page.content_hash:
                _set_llm_cache(page.content_hash, "gemini", result)
            return result, "gemini"
        log.info("    Gemini returned no tariffs, escalating to Haiku")

    result = _call_claude_tool(prompt)
    if result:
        if page.content_hash:
            _set_llm_cache(page.content_hash, "haiku", result)
        return result, "haiku"

    log.info("    Haiku returned no tariffs, escalating to Opus 4.6")
    result = _call_opus_tool(prompt)
    if result:
        if page.content_hash:
            _set_llm_cache(page.content_hash, "opus", result)
    return result, "opus"


def _parse_extraction_response(items: list[dict], source_url: str) -> list[ExtractedTariff]:
    """Convert raw dicts (from tool use or text parse) into ExtractedTariff objects."""
    tariffs = []
    for item in items:
        tariffs.append(ExtractedTariff(
            name=item.get("name", "Unknown"),
            code=item.get("code", ""),
            customer_class=item.get("customer_class", ""),
            rate_type=item.get("rate_type", ""),
            description=item.get("description", ""),
            source_url=source_url,
            effective_date=item.get("effective_date", ""),
            components=item.get("components", []),
            confidence=float(item.get("confidence", 0.0)),
        ))
    return tariffs


# ---------------------------------------------------------------------------
# Content identity verification — detects cross-contamination
# ---------------------------------------------------------------------------

def verify_content_identity(
    pages: list[RatePage], utility_name: str, state: str,
    utility_domain: str | None = None,
) -> tuple[bool, str]:
    """Check whether fetched page content plausibly belongs to the target utility.

    Returns (is_ok, reason). Checks:
    1. Utility name words in page content
    2. State name in page content
    3. Page domains match utility's known domain (if provided)

    If the pages clearly belong to a different organization, returns (False, explanation).
    """
    if not pages:
        return True, ""

    name_words = _utility_name_words(utility_name)
    if not name_words:
        return True, ""

    all_text = " ".join(p.content[:5000].lower() for p in pages if p.content)
    if not all_text:
        return True, ""

    matches = sum(1 for w in name_words if w in all_text)
    ratio = matches / len(name_words) if name_words else 0

    # Domain check: if we know the utility's domain, verify at least some
    # pages come from it or a related domain
    domain_ok = True
    if utility_domain:
        clean_utility_domain = utility_domain.replace("www.", "").lower()
        page_domains = set()
        for p in pages:
            d = urlparse(p.url).netloc.replace("www.", "").lower()
            if d:
                page_domains.add(d)

        if page_domains:
            # Check if any page comes from the utility's domain or a subdomain
            domain_ok = any(
                d == clean_utility_domain or d.endswith("." + clean_utility_domain)
                for d in page_domains
            )
            if not domain_ok:
                # Also accept if the page domains share the base domain
                # (e.g., utility domain is "duke-energy.com" and page is from "duke-energy.com/rates")
                utility_base = ".".join(clean_utility_domain.split(".")[-2:])
                domain_ok = any(
                    ".".join(d.split(".")[-2:]) == utility_base
                    for d in page_domains
                )

    if ratio >= 0.3 or matches >= 2:
        if not domain_ok:
            log.info("  Identity check: name matches but domain differs — proceeding with caution")
        return True, ""

    state_lower = state.lower()
    state_names = {
        "TX": "texas", "CA": "california", "NY": "new york", "FL": "florida",
        "PA": "pennsylvania", "IL": "illinois", "OH": "ohio", "GA": "georgia",
        "NC": "north carolina", "MI": "michigan", "NJ": "new jersey",
        "VA": "virginia", "WA": "washington", "AZ": "arizona", "MA": "massachusetts",
        "TN": "tennessee", "IN": "indiana", "MO": "missouri", "MD": "maryland",
        "WI": "wisconsin", "CO": "colorado", "MN": "minnesota", "SC": "south carolina",
        "AL": "alabama", "LA": "louisiana", "KY": "kentucky", "OR": "oregon",
        "OK": "oklahoma", "CT": "connecticut", "UT": "utah", "NV": "nevada",
        "AR": "arkansas", "MS": "mississippi", "KS": "kansas", "NM": "new mexico",
        "NE": "nebraska", "ID": "idaho", "WV": "west virginia", "HI": "hawaii",
        "NH": "new hampshire", "ME": "maine", "MT": "montana", "RI": "rhode island",
        "DE": "delaware", "SD": "south dakota", "ND": "north dakota", "AK": "alaska",
        "VT": "vermont", "WY": "wyoming", "DC": "district of columbia",
        "ON": "ontario", "QC": "quebec", "BC": "british columbia", "AB": "alberta",
        "SK": "saskatchewan", "MB": "manitoba", "NS": "nova scotia",
        "NB": "new brunswick", "NL": "newfoundland", "PE": "prince edward island",
    }
    state_full = state_names.get(state.upper(), state_lower)
    has_state = state_lower in all_text or state_full in all_text

    # Check for OTHER states being mentioned more prominently than the target
    # state — a signal that we're looking at the wrong utility's page.
    if has_state and matches == 0:
        other_state_hits = 0
        for abbr, full_name in state_names.items():
            if abbr.upper() == state.upper():
                continue
            # Only count full state names to avoid false positives from
            # short abbreviations appearing in words
            if full_name in all_text:
                other_state_hits += 1
        if other_state_hits >= 3 and not domain_ok:
            reason = (
                f"Page mentions {other_state_hits} other states more than "
                f"target state ({state}) and domain doesn't match — "
                f"likely cross-contamination."
            )
            log.warning(f"  IDENTITY CHECK FAILED: {reason}")
            return False, reason

    if matches == 0 and not has_state and not domain_ok:
        reason = (
            f"Page content does not mention the utility name ({utility_name}), "
            f"state ({state}), and pages are from a different domain. "
            f"Likely cross-contamination."
        )
        log.warning(f"  IDENTITY CHECK FAILED: {reason}")
        return False, reason

    if matches == 0 and not has_state:
        reason = (
            f"Page content does not mention the utility name ({utility_name}) "
            f"or state ({state}). Possible cross-contamination."
        )
        log.warning(f"  IDENTITY CHECK FAILED: {reason}")
        return False, reason

    if matches == 0 and has_state:
        if not domain_ok:
            reason = (
                f"Page mentions target state ({state}) but utility name "
                f"({utility_name}) not found and domain doesn't match — "
                f"possible cross-contamination."
            )
            log.warning(f"  IDENTITY CHECK FAILED: {reason}")
            return False, reason
        else:
            log.info("  Identity check: utility name not found but state matches and domain OK — proceeding with caution")

    return True, ""


# ---------------------------------------------------------------------------
# Phase 4: Validation
# ---------------------------------------------------------------------------

VALID_CLASSES = {"residential", "commercial"}
VALID_RATE_TYPES = {
    "flat", "tiered", "tou", "demand", "seasonal",
    "tou_tiered", "seasonal_tou", "seasonal_tiered", "demand_tou",
    "tiered_demand", "demand_tiered", "tiered_demand_seasonal",
    "seasonal_demand", "demand_seasonal", "complex",
}
VALID_COMPONENT_TYPES = {"energy", "demand", "fixed", "minimum", "adjustment"}


# State-level rate bounds (95th percentile = soft flag, 99th = hard reject).
# Built from EIA average residential rates + margin. Keyed by state abbreviation.
# Values: (p95_energy, p99_energy, p95_fixed, p99_fixed, p95_demand, p99_demand)
# Defaults are used for states without specific data.
_DEFAULT_BOUNDS = (0.35, 0.60, 50.0, 150.0, 30.0, 80.0)
_STATE_RATE_BOUNDS: dict[str, tuple[float, float, float, float, float, float]] = {
    # High-cost states
    "HI": (0.50, 0.80, 30.0, 100.0, 40.0, 100.0),
    "CT": (0.40, 0.65, 30.0, 100.0, 30.0, 80.0),
    "MA": (0.40, 0.65, 20.0, 80.0, 30.0, 80.0),
    "RI": (0.40, 0.65, 20.0, 80.0, 30.0, 80.0),
    "NH": (0.35, 0.55, 25.0, 80.0, 30.0, 80.0),
    "CA": (0.55, 0.80, 20.0, 80.0, 30.0, 80.0),
    "AK": (0.40, 0.65, 30.0, 100.0, 30.0, 80.0),
    "NY": (0.35, 0.55, 30.0, 100.0, 30.0, 80.0),
    "NJ": (0.30, 0.50, 20.0, 80.0, 30.0, 80.0),
    # Mid-cost states (default covers most)
    "TX": (0.25, 0.45, 20.0, 80.0, 25.0, 70.0),
    "FL": (0.25, 0.40, 20.0, 80.0, 25.0, 70.0),
    "IL": (0.25, 0.45, 20.0, 80.0, 25.0, 70.0),
    # Low-cost states
    "WA": (0.20, 0.35, 25.0, 80.0, 20.0, 60.0),
    "OR": (0.20, 0.35, 20.0, 80.0, 20.0, 60.0),
    "ID": (0.18, 0.30, 20.0, 80.0, 20.0, 60.0),
    "LA": (0.20, 0.35, 20.0, 80.0, 20.0, 60.0),
    "AR": (0.20, 0.35, 20.0, 80.0, 20.0, 60.0),
    "WY": (0.20, 0.35, 20.0, 80.0, 20.0, 60.0),
    "UT": (0.20, 0.35, 15.0, 60.0, 20.0, 60.0),
    # Canadian provinces
    "ON": (0.25, 0.40, 40.0, 120.0, 20.0, 60.0),
    "BC": (0.20, 0.35, 20.0, 80.0, 15.0, 50.0),
    "AB": (0.30, 0.50, 30.0, 100.0, 20.0, 60.0),
    "QC": (0.15, 0.25, 25.0, 80.0, 15.0, 50.0),
}


def _get_rate_bounds(state: str) -> tuple[float, float, float, float, float, float]:
    return _STATE_RATE_BOUNDS.get(state.upper(), _DEFAULT_BOUNDS)


def phase4_validate(
    tariffs: list[ExtractedTariff], utility_name: str, state: str = ""
) -> tuple[dict, list[ExtractedTariff]]:
    """Validate extracted tariffs. Returns (report_dict, valid_tariffs_list).

    Uses state-level percentile bounds for rate validation:
    - Above 99th percentile: hard reject
    - Above 95th percentile: accepted but flagged as needs_review
    """
    bounds = _get_rate_bounds(state)
    p95_energy, p99_energy, p95_fixed, p99_fixed, p95_demand, p99_demand = bounds

    issues = []
    valid_tariffs = []
    flagged_tariffs = []

    for t in tariffs:
        tariff_issues = []
        needs_review = False

        if not t.name or t.name == "Unknown":
            tariff_issues.append("missing name")
        if t.customer_class not in VALID_CLASSES:
            tariff_issues.append(f"invalid customer_class '{t.customer_class}'")
        if t.rate_type not in VALID_RATE_TYPES:
            tariff_issues.append(f"invalid rate_type '{t.rate_type}'")

        comp_types = {comp.get("component_type") for comp in t.components}
        has_core_component = bool(comp_types & {"energy", "fixed", "demand"})
        if not has_core_component:
            tariff_issues.append("no energy/fixed/demand component (rate rider only)")

        for comp in t.components:
            if comp.get("component_type") not in VALID_COMPONENT_TYPES:
                tariff_issues.append(f"invalid component_type '{comp.get('component_type')}'")
            rate_val = comp.get("rate_value")
            if rate_val is not None:
                try:
                    rv = float(rate_val)
                    ctype = comp.get("component_type")
                    if rv < 0 and ctype not in ("adjustment", "energy"):
                        tariff_issues.append(f"negative rate_value {rv}")
                    if ctype == "energy" and rv > 0 and rv < 0.01:
                        tariff_issues.append(
                            f"energy rate {rv} $/kWh suspiciously low "
                            f"(likely cents parsed as dollars)"
                        )
                    if ctype == "energy":
                        if rv > p99_energy:
                            tariff_issues.append(
                                f"energy rate {rv} $/kWh exceeds 99th percentile "
                                f"for {state or 'US'} ({p99_energy})"
                            )
                        elif rv > p95_energy:
                            needs_review = True
                    elif ctype == "fixed":
                        if rv > p99_fixed:
                            tariff_issues.append(
                                f"fixed charge ${rv}/month exceeds 99th percentile "
                                f"for {state or 'US'} ({p99_fixed})"
                            )
                        elif rv > p95_fixed:
                            needs_review = True
                    elif ctype == "demand":
                        if rv > p99_demand:
                            tariff_issues.append(
                                f"demand charge ${rv}/kW exceeds 99th percentile "
                                f"for {state or 'US'} ({p99_demand})"
                            )
                        elif rv > p95_demand:
                            needs_review = True
                except (ValueError, TypeError):
                    tariff_issues.append(f"non-numeric rate_value '{rate_val}'")

        if tariff_issues:
            issues.append({"tariff": t.name, "issues": tariff_issues})
        else:
            valid_tariffs.append(t)
            if needs_review:
                flagged_tariffs.append(t.name)

    report = {
        "total_extracted": len(tariffs),
        "valid": len(valid_tariffs),
        "invalid": len(tariffs) - len(valid_tariffs),
        "issues": issues,
        "flagged_needs_review": flagged_tariffs,
        "has_residential": any(t.customer_class == "residential" for t in valid_tariffs),
        "has_commercial": any(t.customer_class == "commercial" for t in valid_tariffs),
    }

    log.info(f"  Phase 4: {report['valid']} valid, {report['invalid']} invalid tariffs")
    if flagged_tariffs:
        log.info(f"    Flagged for review (above 95th pctl): {flagged_tariffs}")
    if issues:
        for i in issues:
            log.warning(f"    {i['tariff']}: {', '.join(i['issues'])}")

    return report, valid_tariffs


# ---------------------------------------------------------------------------
# Store results
# ---------------------------------------------------------------------------

def _calculate_confidence(
    et: ExtractedTariff, utility_name: str, state: str, utility_domain: str | None,
) -> tuple[float, dict]:
    """Calculate a composite confidence score for a tariff from multiple signals.

    Returns (score, factors_dict) where score is 0.0-1.0.
    """
    factors: dict[str, float] = {}

    # Signal 1: Source URL domain matches utility's known domain (+0.25)
    if et.source_url and utility_domain:
        src_domain = urlparse(et.source_url).netloc.replace("www.", "")
        if utility_domain.replace("www.", "") == src_domain:
            factors["domain_match"] = 0.25
        else:
            factors["domain_match"] = 0.0

    # Signal 2: LLM self-reported confidence (+0.20)
    llm_conf = max(0.0, min(1.0, et.confidence))
    factors["llm_confidence"] = round(llm_conf * 0.20, 3)

    # Signal 3: Rate values within normal range for state (+0.15)
    bounds = _get_rate_bounds(state)
    p95_energy = bounds[0]
    all_rates_normal = True
    for comp in et.components:
        ctype = comp.get("component_type")
        rv = comp.get("rate_value")
        if rv is not None and ctype == "energy":
            try:
                if float(rv) > p95_energy:
                    all_rates_normal = False
            except (ValueError, TypeError):
                all_rates_normal = False
    factors["rates_normal"] = 0.15 if all_rates_normal else 0.0

    # Signal 4: Multiple well-structured components (+0.10)
    n_comp = len(et.components)
    if n_comp >= 2:
        factors["component_richness"] = 0.10
    elif n_comp == 1:
        factors["component_richness"] = 0.05
    else:
        factors["component_richness"] = 0.0

    # Signal 5: Has energy component (basic sanity) (+0.10)
    has_energy = any(c.get("component_type") == "energy" for c in et.components)
    factors["has_energy"] = 0.10 if has_energy else 0.0

    # Signal 6: Utility name words found in source URL or tariff name (+0.20)
    name_words = {w.lower() for w in utility_name.split() if len(w) > 2}
    url_lower = (et.source_url or "").lower()
    name_lower = et.name.lower()
    matched = sum(1 for w in name_words if w in url_lower or w in name_lower)
    if name_words:
        name_ratio = matched / len(name_words)
        factors["name_match"] = round(min(0.20, name_ratio * 0.20), 3)
    else:
        factors["name_match"] = 0.0

    score = min(1.0, sum(factors.values()))
    return round(score, 3), factors


def store_tariffs(utility_id: int, tariffs: list[ExtractedTariff], dry_run: bool) -> int:
    """Store validated tariffs in the database via direct DB connection."""
    if dry_run:
        log.info(f"  DRY RUN: Would store {len(tariffs)} tariffs for utility {utility_id}")
        return len(tariffs)

    from sqlalchemy import select
    from sqlalchemy.orm import Session
    from app.db.session import get_sync_engine
    from app.models import Tariff, RateComponent, CustomerClass, RateType, ComponentType

    CLASS_MAP = {
        "residential": CustomerClass.RESIDENTIAL,
        "commercial": CustomerClass.COMMERCIAL,
    }
    TYPE_MAP = {
        "flat": RateType.FLAT, "tiered": RateType.TIERED, "tou": RateType.TOU,
        "demand": RateType.DEMAND, "seasonal": RateType.SEASONAL,
        "tou_tiered": RateType.TOU_TIERED, "seasonal_tou": RateType.SEASONAL_TOU,
        "seasonal_tiered": RateType.SEASONAL_TIERED, "demand_tou": RateType.DEMAND_TOU,
        "complex": RateType.COMPLEX,
        "tiered_demand": RateType.COMPLEX, "demand_tiered": RateType.COMPLEX,
        "tiered_demand_seasonal": RateType.COMPLEX,
        "seasonal_demand": RateType.COMPLEX, "demand_seasonal": RateType.COMPLEX,
        "seasonal_tiered_demand": RateType.COMPLEX, "seasonal_tou_tiered": RateType.COMPLEX,
        "seasonal_tou_demand": RateType.COMPLEX, "tou_demand": RateType.COMPLEX,
    }
    COMP_MAP = {
        "energy": ComponentType.ENERGY, "demand": ComponentType.DEMAND,
        "fixed": ComponentType.FIXED, "minimum": ComponentType.MINIMUM,
        "adjustment": ComponentType.ADJUSTMENT,
    }

    engine = get_sync_engine()
    stored = 0

    # Load utility info for confidence scoring
    info = get_utility_info(utility_id)
    u_name = info.get("name", "")
    u_state = info.get("state", "")
    u_website = info.get("website_url", "")
    u_domain = urlparse(u_website).netloc if u_website else None

    with Session(engine) as session:
        for et in tariffs:
            cc = CLASS_MAP.get(et.customer_class)
            rt = TYPE_MAP.get(et.rate_type)
            if not cc or not rt:
                continue

            eff_date = None
            if et.effective_date:
                try:
                    from datetime import date
                    eff_date = date.fromisoformat(et.effective_date)
                except ValueError:
                    pass

            conf_score, conf_factors = _calculate_confidence(
                et, u_name, u_state, u_domain,
            )

            existing = session.execute(
                select(Tariff).where(
                    Tariff.utility_id == utility_id,
                    Tariff.name == et.name,
                    Tariff.customer_class == cc,
                )
            ).scalar_one_or_none()

            if existing:
                existing.rate_type = rt
                existing.description = et.description or existing.description
                existing.source_url = et.source_url or existing.source_url
                existing.effective_date = eff_date or existing.effective_date
                existing.code = et.code or existing.code
                existing.last_verified_at = datetime.now(timezone.utc)
                existing.confidence_score = conf_score
                existing.confidence_factors = conf_factors
                tariff_obj = existing
            else:
                tariff_obj = Tariff(
                    utility_id=utility_id,
                    name=et.name,
                    code=et.code,
                    customer_class=cc,
                    rate_type=rt,
                    description=et.description,
                    source_url=et.source_url,
                    effective_date=eff_date,
                    last_verified_at=datetime.now(timezone.utc),
                    approved=False,
                    confidence_score=conf_score,
                    confidence_factors=conf_factors,
                )
                session.add(tariff_obj)

            new_components = []
            for comp in et.components:
                ct = COMP_MAP.get(comp.get("component_type"))
                if not ct:
                    continue
                try:
                    rv = float(comp.get("rate_value", 0))
                except (ValueError, TypeError):
                    log.warning(f"    Skipping component with unparseable rate_value: {comp.get('rate_value')}")
                    continue
                new_components.append(RateComponent(
                    component_type=ct,
                    unit=comp.get("unit", "$/kWh"),
                    rate_value=rv,
                    tier_min_kwh=comp.get("tier_min_kwh"),
                    tier_max_kwh=comp.get("tier_max_kwh"),
                    tier_label=comp.get("tier_label"),
                    period_label=comp.get("period_label"),
                    season=comp.get("season"),
                ))

            if not new_components:
                log.warning(f"    Skipping tariff '{et.name}' — 0 valid components")
                continue

            if existing:
                tariff_obj.rate_components.clear()
            for rc in new_components:
                tariff_obj.rate_components.append(rc)

            stored += 1

        # Reconcile: remove tariffs for this utility that were not in the
        # current extraction, with guards to prevent destroying valid data.
        if stored >= 1:
            stored_names = {(et.name, CLASS_MAP.get(et.customer_class)) for et in tariffs if CLASS_MAP.get(et.customer_class)}
            has_residential = any(cc == CustomerClass.RESIDENTIAL for _, cc in stored_names)

            if has_residential:
                all_existing = session.execute(
                    select(Tariff).where(Tariff.utility_id == utility_id)
                ).scalars().all()

                existing_count = len(all_existing)

                # Count-based guard: if new extraction found significantly fewer
                # tariffs than already exist, skip reconciliation to avoid
                # destroying valid data from a partial extraction.
                if stored < existing_count * 0.75 and existing_count >= 3:
                    log.warning(
                        f"  Reconciliation SKIPPED: new extraction found {stored} tariffs "
                        f"vs {existing_count} existing (< 75%) — possible partial extraction"
                    )
                else:
                    # Source-aware reconciliation: only consider deleting tariffs
                    # whose source_url domain matches a page we processed in this run.
                    fetched_domains = set()
                    for et in tariffs:
                        if et.source_url:
                            d = urlparse(et.source_url).netloc.replace("www.", "")
                            if d:
                                fetched_domains.add(d)

                    if not fetched_domains:
                        log.warning(
                            "  Reconciliation SKIPPED: no source domains in "
                            "current extraction — cannot safely determine stale tariffs"
                        )
                        stale = []
                    else:
                        stale = []
                        for t in all_existing:
                            if (t.name, t.customer_class) in stored_names:
                                continue
                            if t.source_url:
                                t_domain = urlparse(t.source_url).netloc.replace("www.", "")
                                if t_domain and t_domain not in fetched_domains:
                                    continue
                            else:
                                # No source_url on existing tariff — don't delete
                                # without domain evidence
                                continue
                            stale.append(t)

                    if stale:
                        stale_ids = [t.id for t in stale]
                        from sqlalchemy import delete as sa_delete
                        session.execute(
                            sa_delete(RateComponent).where(RateComponent.tariff_id.in_(stale_ids))
                        )
                        session.execute(
                            sa_delete(Tariff).where(Tariff.id.in_(stale_ids))
                        )
                        log.info(
                            f"  Reconciled: removed {len(stale)} stale tariffs "
                            f"not found in current extraction"
                        )
                        for t in stale:
                            log.info(f"    Removed: {t.name} ({t.customer_class.value})")

        session.commit()

    log.info(f"  Stored {stored} tariffs for utility {utility_id}")
    return stored


def update_monitoring_source(utility_id: int, rate_page_url: str, dry_run: bool):
    """Update or create a monitoring source for the discovered rate page.
    Uses direct SQLAlchemy access instead of HTTP API calls."""
    if dry_run:
        log.info(f"  DRY RUN: Would update monitoring source for utility {utility_id}")
        return

    from sqlalchemy import select
    from sqlalchemy.orm import Session
    from app.db.session import get_sync_engine
    from app.models.monitoring import MonitoringSource, MonitoringStatus

    engine = get_sync_engine()
    with Session(engine) as session:
        stmt = (
            select(MonitoringSource)
            .where(MonitoringSource.utility_id == utility_id)
            .where(MonitoringSource.status == MonitoringStatus.ERROR)
            .limit(1)
        )
        source = session.execute(stmt).scalar_one_or_none()
        if source:
            source.url = rate_page_url
            session.commit()
            log.info(f"  Updated monitoring source {source.id} → {rate_page_url[:80]}")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def get_utility_info(utility_id: int) -> dict:
    """Get utility details from the database."""
    from sqlalchemy import select
    from sqlalchemy.orm import Session
    from app.db.session import get_sync_engine
    from app.models import Utility

    engine = get_sync_engine()
    with Session(engine) as session:
        u = session.execute(
            select(Utility).where(Utility.id == utility_id)
        ).scalar_one_or_none()
        if not u:
            return {"id": utility_id, "name": f"Utility #{utility_id}"}
        return {
            "id": u.id,
            "name": u.name,
            "state": u.state_province,
            "country": u.country.value if u.country else "",
            "website_url": u.website_url,
            "rate_page_url_override": getattr(u, "rate_page_url_override", None),
        }


CENTRALIZED_PROVINCES = {"ON"}


def _try_centralized_regulator(
    utility_id: int, state: str, country: str, dry_run: bool
) -> PipelineResult | None:
    """For provinces with centralized rate regulators, use the regulator's
    data instead of scraping individual utility websites."""
    if country != "CA" or state not in CENTRALIZED_PROVINCES:
        return None

    if state == "ON":
        try:
            from scripts.scrape_oeb_rates import (
                fetch_oeb_page, parse_oeb_rates,
                build_tariff_entries, store_oeb_tariffs,
            )
            html = fetch_oeb_page()
            rates = parse_oeb_rates(html)
            if not rates.tou and not rates.tiered:
                log.warning("  OEB scraper returned no rates — falling back to standard pipeline")
                return None

            res_tariffs = build_tariff_entries(rates, "residential")
            com_tariffs = build_tariff_entries(rates, "commercial")
            all_tariffs = res_tariffs + com_tariffs

            if not dry_run:
                count = store_oeb_tariffs(utility_id, all_tariffs, dry_run)
                log.info(f"  Stored {count} OEB tariffs for utility {utility_id}")
            else:
                count = len(all_tariffs)
                log.info(f"  DRY RUN: Would store {count} OEB tariffs for utility {utility_id}")

            result = PipelineResult(
                utility_id=utility_id,
                utility_name="",
                country=country,
                state=state,
                phase1_rate_page_url=f"https://www.oeb.ca (centralized regulator)",
            )
            result.phase4_validation = {"valid": count, "source": "OEB centralized"}
            return result
        except Exception as e:
            log.warning(f"  OEB centralized scraper failed: {e} — falling back to standard pipeline")
            return None

    return None


ADDITIONAL_SEARCH_QUERIES = [
    "{name} EV electric vehicle charging rate {state}",
    "{name} heat pump rate schedule {state}",
    "{name} time of use rate schedule {state}",
    "{name} dynamic pricing rate {state}",
    "{name} net metering solar rate {state}",
    "{name} all rate schedules tariff {state}",
]


def run_additional_tariff_search(
    utility_id: int,
    *,
    dry_run: bool = False,
) -> PipelineResult:
    """Search for additional tariff plans (EV, heat pump, TOU, etc.) that
    the primary search may have missed. Only adds NEW tariffs that don't
    duplicate existing ones."""

    info = get_utility_info(utility_id)
    utility_name = info.get("name", "")
    state = info.get("state", "")
    country = info.get("country", "")
    website_url = info.get("website_url")

    result = PipelineResult(
        utility_id=utility_id,
        utility_name=utility_name,
        country=country,
        state=state,
    )

    log.info(f"=== Additional Tariff Search: {utility_name} (id={utility_id}) ===")

    # Get existing tariff names for this utility to avoid duplicates
    from sqlalchemy import select
    from sqlalchemy.orm import Session
    from app.db.session import get_sync_engine
    from app.models import Tariff
    engine = get_sync_engine()
    with Session(engine) as s:
        existing_tariffs = s.execute(
            select(Tariff.name).where(Tariff.utility_id == utility_id)
        ).scalars().all()
    existing_names = {_normalize_tariff_name(n) for n in existing_tariffs}
    log.info(f"  Existing tariffs: {len(existing_names)}")

    clean_name = _clean_utility_name(utility_name)
    utility_domain = urlparse(website_url).netloc if website_url else None

    all_new_tariffs: list[ExtractedTariff] = []
    seen_urls: set[str] = set()

    for query_tmpl in ADDITIONAL_SEARCH_QUERIES:
        query = query_tmpl.format(name=clean_name, state=state)
        log.info(f"  Searching: [{query}]")

        results = brave_search(query, count=5)
        if not results:
            continue

        for r in results:
            url = r["url"]
            if url in seen_urls:
                continue
            seen_urls.add(url)

            score = score_search_result(r, utility_name, utility_domain)
            if score < 20:
                continue

            page = _fetch_and_parse(url)
            if not page or len(page.content.strip()) < 200:
                continue

            tariffs = phase3_extract_tariffs([page], utility_name)
            for t in tariffs:
                norm_name = _normalize_tariff_name(t.name)
                if _is_prefix_duplicate(norm_name, existing_names):
                    log.debug(f"    Skipping duplicate (prefix match): {t.name}")
                    continue
                all_new_tariffs.append(t)
                existing_names.add(norm_name)
                log.info(f"    NEW tariff found: {t.name}")

    if not all_new_tariffs:
        log.info(f"  No additional tariffs found for {utility_name}")
        return result

    validation, valid_tariffs = phase4_validate(all_new_tariffs, utility_name, state)
    result.phase4_validation = validation

    if valid_tariffs and not dry_run:
        stored = store_tariffs(utility_id, valid_tariffs, dry_run)
    elif dry_run:
        log.info(f"  DRY RUN: {len(valid_tariffs)} new tariffs ready to store")
    result.phase3_tariffs = [asdict(t) for t in valid_tariffs]

    log.info(f"=== Done additional search: {utility_name} — {len(valid_tariffs)} new tariffs ===\n")
    return result


def _normalize_tariff_name(name: str) -> str:
    """Normalize a tariff name for dedup comparison."""
    n = name.lower().strip()
    n = re.sub(r"[^a-z0-9\s]", "", n)
    n = " ".join(n.split())
    return n


def _is_prefix_duplicate(norm_name: str, existing_names: set[str]) -> bool:
    """Check if norm_name is a prefix duplicate of any existing name, or vice versa."""
    if norm_name in existing_names:
        return True
    for existing in existing_names:
        if norm_name.startswith(existing) or existing.startswith(norm_name):
            return True
    return False


def _check_fingerprints(utility_id: int, pages: list[RatePage]) -> bool:
    """Check if all crawled pages have unchanged content since last extraction.
    Returns True if ALL pages match stored fingerprints (safe to skip LLM)."""
    if not pages:
        return False
    from sqlalchemy.orm import Session
    from app.db.session import get_sync_engine
    from app.models.fingerprint import RatePageFingerprint

    engine = get_sync_engine()
    with Session(engine) as session:
        for page in pages:
            if not page.content_hash:
                return False
            fp = session.get(RatePageFingerprint, (utility_id, page.url))
            if not fp or fp.content_hash != page.content_hash:
                return False
    log.info("  All page fingerprints match — content unchanged since last extraction")
    return True


def _store_fingerprints(utility_id: int, pages: list[RatePage]):
    """Store/update fingerprints for all crawled pages after successful extraction."""
    if not pages:
        return
    from sqlalchemy.orm import Session
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from app.db.session import get_sync_engine
    from app.models.fingerprint import RatePageFingerprint

    now = datetime.now(timezone.utc)
    engine = get_sync_engine()
    with Session(engine) as session:
        for page in pages:
            if not page.content_hash:
                continue
            stmt = pg_insert(RatePageFingerprint).values(
                utility_id=utility_id,
                url=page.url,
                content_hash=page.content_hash,
                checked_at=now,
                changed_at=now,
            ).on_conflict_do_update(
                index_elements=["utility_id", "url"],
                set_={
                    "content_hash": page.content_hash,
                    "checked_at": now,
                    "changed_at": now,
                },
            )
            session.execute(stmt)
        session.commit()
    log.info(f"  Updated fingerprints for {len(pages)} pages")


def _touch_fingerprints(utility_id: int, pages: list[RatePage]):
    """Update checked_at without changing changed_at (content was unchanged)."""
    if not pages:
        return
    from sqlalchemy.orm import Session
    from app.db.session import get_sync_engine
    from app.models.fingerprint import RatePageFingerprint

    now = datetime.now(timezone.utc)
    engine = get_sync_engine()
    with Session(engine) as session:
        for page in pages:
            fp = session.get(RatePageFingerprint, (utility_id, page.url))
            if fp:
                fp.checked_at = now
        session.commit()


def _touch_tariff_verified(utility_id: int):
    """Update last_verified_at on existing tariffs to mark them as still current."""
    from sqlalchemy.orm import Session
    from sqlalchemy import update
    from app.db.session import get_sync_engine
    from app.models.tariff import Tariff

    now = datetime.now(timezone.utc)
    engine = get_sync_engine()
    with Session(engine) as session:
        session.execute(
            update(Tariff)
            .where(Tariff.utility_id == utility_id)
            .values(last_verified_at=now)
        )
        session.commit()
    log.info(f"  Refreshed last_verified_at for utility {utility_id} (content unchanged)")


def run_pipeline(
    utility_id: int,
    *,
    name_override: str | None = None,
    state_override: str | None = None,
    country_override: str | None = None,
    website_url_override: str | None = None,
    skip_search: bool = False,
    existing_rate_url: str = "",
    dry_run: bool = False,
    comprehensive: bool = False,
    force_extract: bool = False,
) -> PipelineResult:
    info = get_utility_info(utility_id)
    if not info or info.get("name", "").startswith("Utility #"):
        raise ValueError(f"Utility {utility_id} not found in database")

    utility_name = name_override or info["name"]
    state = state_override or info.get("state", "")
    country = country_override or info.get("country", "")
    website_url = website_url_override or info.get("website_url")

    result = PipelineResult(
        utility_id=utility_id,
        utility_name=utility_name,
        country=country,
        state=state,
    )

    log.info(f"=== Pipeline: {utility_name} (id={utility_id}, {state}, {country}) ===")

    # Check if this utility is in a province with a centralized regulator
    centralized = _try_centralized_regulator(utility_id, state, country, dry_run)
    if centralized:
        centralized.utility_name = utility_name
        additional_count = 0
        if comprehensive:
            log.info(f"  Comprehensive mode: searching for specialty tariffs...")
            try:
                additional = run_additional_tariff_search(
                    utility_id,
                    dry_run=dry_run,
                )
                additional_count = additional.phase4_validation.get("valid", 0) if additional.phase4_validation else 0
            except Exception as e:
                log.warning(f"  Additional tariff search failed: {e}")
        log.info(f"=== Done (centralized{' + specialty' if additional_count else ''}): {utility_name} ===\n")
        return centralized

    # Phase 1 — check for manual override first
    rate_page_url_override = info.get("rate_page_url_override")
    rate_page_url = existing_rate_url or rate_page_url_override or ""
    alt_urls: list[str] = []
    if rate_page_url_override:
        log.info(f"  Using manual rate page override: {rate_page_url_override}")
    if not skip_search and not rate_page_url:
        try:
            rate_page_url, num_results, alt_urls = phase1_find_rate_page(
                utility_name, state, website_url
            )
            result.phase1_rate_page_url = rate_page_url
            result.phase1_search_results = num_results
        except Exception as e:
            result.errors.append(f"Phase 1 error: {e}")
            log.error(f"  Phase 1 failed: {e}")
            return result
    else:
        result.phase1_rate_page_url = rate_page_url

    if not rate_page_url:
        result.errors.append("No rate page found")
        log.warning("  No rate page found — stopping")
        return result

    # Phases 2+3 with automatic retry: if first URL yields 0 tariffs,
    # pick alternates from a DIFFERENT section of the site.
    MAX_ATTEMPTS = 4
    tariffs: list[ExtractedTariff] = []
    pages: list[RatePage] = []
    tried_prefixes: set[str] = set()

    def _path_prefix(u: str) -> str:
        parts = urlparse(u).path.strip("/").split("/")
        return "/".join(parts[:2]) if len(parts) >= 2 else parts[0] if parts else ""

    remaining_alts = list(alt_urls)
    current_url = rate_page_url
    attempts = 0

    def _pick_next_alt() -> str | None:
        for alt in remaining_alts:
            alt_prefix = _path_prefix(alt)
            if alt_prefix not in tried_prefixes:
                log.warning(f"  Retrying with different site section: {alt[:70]}")
                remaining_alts.remove(alt)
                return alt
        return None

    while current_url and attempts < MAX_ATTEMPTS:
        attempts += 1
        prefix = _path_prefix(current_url)
        tried_prefixes.add(prefix)

        # Phase 2
        try:
            pages = phase2_discover_tariff_pages(current_url)
            result.phase2_sub_pages = [
                {"url": p.url, "title": p.title, "type": p.page_type, "has_content": bool(p.content)}
                for p in pages
            ]
        except Exception as e:
            result.errors.append(f"Phase 2 error on {current_url[:60]}: {e}")
            log.error(f"  Phase 2 failed: {e}")
            current_url = _pick_next_alt()
            continue

        if not pages:
            current_url = _pick_next_alt()
            continue

        # Incremental check: skip LLM extraction if page content unchanged
        if not force_extract and not dry_run and _check_fingerprints(utility_id, pages):
            _touch_fingerprints(utility_id, pages)
            _touch_tariff_verified(utility_id)
            result.phase3_tariffs = []
            log.info("  Skipping Phase 3 (content unchanged) — existing tariffs still valid")
            return result

        # Phase 3
        try:
            tariffs = phase3_extract_tariffs(pages, utility_name)
            result.phase3_tariffs = [asdict(t) for t in tariffs]
        except Exception as e:
            result.errors.append(f"Phase 3 error on {current_url[:60]}: {e}")
            log.error(f"  Phase 3 failed: {e}")
            current_url = _pick_next_alt()
            continue

        if tariffs:
            result.phase1_rate_page_url = current_url
            break

        current_url = _pick_next_alt()

    if not tariffs:
        result.errors.append("No tariffs extracted from any candidate page")
        log.warning("  No tariffs extracted from any candidate page")

    # Content identity check — reject if pages clearly belong to a different utility
    utility_domain = urlparse(website_url).netloc if website_url else None
    if tariffs and pages:
        identity_ok, identity_reason = verify_content_identity(
            pages, utility_name, state, utility_domain,
        )
        if not identity_ok:
            log.warning(f"  REJECTING {len(tariffs)} tariffs: {identity_reason}")
            result.errors.append(f"Content identity check failed: {identity_reason}")
            tariffs = []

    # Phase 4 — validation now returns (report, valid_list)
    validation, valid_tariffs = phase4_validate(tariffs, utility_name, state)
    result.phase4_validation = validation

    successful_url = result.phase1_rate_page_url or rate_page_url

    if valid_tariffs and not dry_run:
        stored = store_tariffs(utility_id, valid_tariffs, dry_run)
        update_monitoring_source(utility_id, successful_url, dry_run)
        _store_fingerprints(utility_id, pages)
    elif dry_run:
        log.info(f"  DRY RUN: {len(valid_tariffs)} tariffs ready to store")
    else:
        log.warning("  No valid tariffs to store")

    # Comprehensive mode: run additional specialty searches after base tariffs
    additional_count = 0
    if comprehensive and valid_tariffs:
        log.info(f"  Comprehensive mode: searching for specialty tariffs (EV, heat pump, TOU, solar)...")
        try:
            additional = run_additional_tariff_search(
                utility_id,
                dry_run=dry_run,
            )
            additional_count = additional.phase4_validation.get("valid", 0) if additional.phase4_validation else 0
        except Exception as e:
            log.warning(f"  Additional tariff search failed: {e}")

    total = len(valid_tariffs) + additional_count
    log.info(f"=== Done: {utility_name} — {len(valid_tariffs)} base + {additional_count} specialty = {total} tariffs ===\n")
    return result


def cleanup_between_utilities():
    """Release Playwright browser and force GC between utilities in batch mode.
    This prevents Chromium processes from accumulating and causing OOM kills."""
    import gc
    _get_pw_mgr().shutdown()
    gc.collect()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _query_utility_ids_missing_tariffs(limit: int, country: str | None = None, province: str | None = None) -> list[int]:
    """Return IDs of utilities that have zero tariffs."""
    from sqlalchemy import select, func as sa_func
    from sqlalchemy.orm import Session
    from app.db.session import get_sync_engine
    from app.models import Utility, Tariff

    engine = get_sync_engine()
    with Session(engine) as session:
        subq = (
            select(Tariff.utility_id, sa_func.count().label("cnt"))
            .group_by(Tariff.utility_id)
            .subquery()
        )
        stmt = (
            select(Utility.id)
            .outerjoin(subq, Utility.id == subq.c.utility_id)
            .where(sa_func.coalesce(subq.c.cnt, 0) == 0)
            .where(Utility.is_active.is_(True))
        )
        if country:
            stmt = stmt.where(Utility.country == country)
        if province:
            stmt = stmt.where(Utility.state_province == province)
        stmt = stmt.limit(limit)
        return list(session.execute(stmt).scalars().all())


def _query_utility_ids_with_tariffs(limit: int, country: str | None = None, province: str | None = None) -> list[int]:
    """Return IDs of utilities that already have tariffs (for additional tariff search)."""
    from sqlalchemy import select, func as sa_func
    from sqlalchemy.orm import Session
    from app.db.session import get_sync_engine
    from app.models import Utility, Tariff

    engine = get_sync_engine()
    with Session(engine) as session:
        subq = (
            select(Tariff.utility_id, sa_func.count().label("cnt"))
            .group_by(Tariff.utility_id)
            .subquery()
        )
        stmt = (
            select(Utility.id)
            .join(subq, Utility.id == subq.c.utility_id)
            .where(subq.c.cnt > 0)
            .where(Utility.is_active.is_(True))
        )
        if country:
            stmt = stmt.where(Utility.country == country)
        if province:
            stmt = stmt.where(Utility.state_province == province)
        stmt = stmt.limit(limit)
        return list(session.execute(stmt).scalars().all())


def _query_utility_ids_error_sources(limit: int) -> list[int]:
    """Return IDs of utilities whose monitoring source is in error state."""
    from sqlalchemy import select
    from sqlalchemy.orm import Session
    from app.db.session import get_sync_engine
    from app.models.monitoring import MonitoringSource, MonitoringStatus

    engine = get_sync_engine()
    with Session(engine) as session:
        stmt = (
            select(MonitoringSource.utility_id)
            .where(MonitoringSource.status == MonitoringStatus.ERROR)
            .distinct()
            .limit(limit)
        )
        return list(session.execute(stmt).scalars().all())


def _run_for_ids(ids: list[int], args) -> list[dict]:
    results = []
    comprehensive = getattr(args, "comprehensive", False)
    for uid in ids:
        try:
            result = run_pipeline(
                uid,
                skip_search=args.skip_search,
                dry_run=args.dry_run,
                comprehensive=comprehensive,
            )
            results.append(asdict(result))
        except Exception as e:
            log.error(f"Pipeline crashed for utility {uid}: {e}")
            results.append({
                "utility_id": uid,
                "utility_name": str(uid),
                "errors": [f"Unhandled crash: {e}"],
                "phase4_validation": {},
            })
        finally:
            if len(ids) > 1:
                cleanup_between_utilities()
    return results


def main():
    parser = argparse.ArgumentParser(description="Tariff discovery and extraction pipeline")
    parser.add_argument("--utility-id", type=int, help="Process a single utility")
    parser.add_argument("--utility-ids", type=str, help="Comma-separated list of utility IDs")
    parser.add_argument("--missing-tariffs", action="store_true", help="Process utilities with no tariffs")
    parser.add_argument("--error-sources", action="store_true", help="Process utilities with errored monitoring sources")
    parser.add_argument("--additional-tariffs", action="store_true", help="Search for additional tariff plans (EV, heat pump, etc.) for existing utilities")
    parser.add_argument("--comprehensive", action="store_true", help="After finding base tariffs, also search for specialty tariffs (EV, heat pump, TOU, solar) in the same run")
    parser.add_argument("--country", type=str, default=None, help="Filter by country code (e.g. CA, US)")
    parser.add_argument("--province", type=str, default=None, help="Filter by province/state code (e.g. ON, QC)")
    parser.add_argument("--limit", type=int, default=100, help="Max utilities to process")
    parser.add_argument("--skip-search", action="store_true", help="Skip Phase 1 (use existing monitoring source URL)")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to database")
    parser.add_argument("--output", type=str, help="Write results JSON to file")
    args = parser.parse_args()

    results: list[dict] = []

    if args.utility_id:
        results = _run_for_ids([args.utility_id], args)

    elif args.utility_ids:
        ids = [int(x.strip()) for x in args.utility_ids.split(",")]
        results = _run_for_ids(ids[:args.limit], args)

    elif args.missing_tariffs:
        ids = _query_utility_ids_missing_tariffs(args.limit, args.country, args.province)
        log.info(f"Found {len(ids)} utilities with no tariffs")
        results = _run_for_ids(ids, args)

    elif args.error_sources:
        ids = _query_utility_ids_error_sources(args.limit)
        log.info(f"Found {len(ids)} utilities with errored monitoring sources")
        results = _run_for_ids(ids, args)

    elif args.additional_tariffs:
        ids = _query_utility_ids_with_tariffs(args.limit, args.country, args.province)
        log.info(f"Found {len(ids)} utilities to search for additional tariffs")
        for uid in ids:
            try:
                result = run_additional_tariff_search(
                    uid,
                    dry_run=args.dry_run,
                )
                results.append(asdict(result))
            except Exception as e:
                log.error(f"Additional tariff search crashed for utility {uid}: {e}")
            finally:
                if len(ids) > 1:
                    cleanup_between_utilities()

    else:
        log.warning("No utility selection flag provided. Use --utility-id, --utility-ids, --missing-tariffs, --error-sources, or --additional-tariffs.")

    if args.output and results:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2, default=str)
        log.info(f"Results written to {args.output}")

    # Print summary
    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)
    for r in results:
        tariff_count = r.get("phase4_validation", {}).get("valid", 0)
        errors = r.get("errors", [])
        status = f"{tariff_count} tariffs" if not errors else f"FAILED: {errors[0]}"
        name = r.get("utility_name", "Unknown")[:40]
        print(f"  {name:40s} {status}")


if __name__ == "__main__":
    main()
