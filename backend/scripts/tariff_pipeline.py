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
from urllib.parse import unquote, urljoin, urlparse

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
HAIKU_MODEL = os.environ.get("HAIKU_MODEL", "claude-haiku-4-5-20251001")
OPUS_MODEL = os.environ.get("OPUS_MODEL", "claude-opus-4-7")
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3-flash-preview")
GEMINI_TIMEOUT_MS = int(os.environ.get("GEMINI_TIMEOUT_MS", "60000"))

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
    "power2switch.com", "homeotter.com", "nyenergyratings.com",
    "texaschoicepower.com", "compareelectricity.com",
    "electricalratesgeorgia.com", "electricityrate.com",
    "gotelectric.com", "ratetruth.com",
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


# Sentinel returned by fetch_page_js when the URL triggered a browser download
# instead of navigation (e.g. DocumentCenter/Download endpoints). Callers that
# see this should try _download_pdf_playwright() on the URL instead.
FETCH_JS_DOWNLOAD_SENTINEL = "__PW_DOWNLOAD__"


def _is_download_error(exc: BaseException | str) -> bool:
    """Is this Playwright exception caused by the URL triggering a download?

    Playwright's page.goto() raises specific error messages when the server
    responds with Content-Disposition: attachment — it refuses to navigate
    since there's no page to render. Detect those so callers can fall back
    to download-handling logic.
    """
    msg = str(exc).lower()
    return (
        "download is starting" in msg
        or "net::err_aborted" in msg
        or "net::err_invalid_response" in msg and "download" in msg
    )


def fetch_page_js(
    url: str,
    wait_ms: int = 2000,
    ignore_https_errors: bool = False,
) -> tuple[str, str]:
    """Fetch a page using the shared Playwright browser for JS-rendered content.
    Returns (html_content, page_title).

    If the URL triggers a file download instead of rendering a page
    (Content-Disposition: attachment), returns (FETCH_JS_DOWNLOAD_SENTINEL, "")
    so the caller can fall back to _download_pdf_playwright().

    Args:
        wait_ms: Extra milliseconds to wait after networkidle for AJAX content.
                 Default 2000ms. Use 5000+ for sites with heavy dynamic loading.
        ignore_https_errors: If True, accept self-signed or mismatched certs.
                 Useful for municipal utilities with expired/misconfigured SSL.
    """
    if not _get_pw_mgr().is_available:
        log.warning("  Playwright not installed — cannot render JS pages")
        return "", ""
    context = None
    try:
        context = _get_pw_mgr().new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            ignore_https_errors=ignore_https_errors,
        )
        page = context.new_page()
        page.goto(url, wait_until="networkidle", timeout=20000)
        page.wait_for_timeout(wait_ms)
        title = page.title()
        html = page.content()
        return html, title
    except Exception as e:
        if _is_download_error(e):
            log.info(f"  Playwright: URL triggered download, signaling PDF fallback for {url[:60]}")
            return FETCH_JS_DOWNLOAD_SENTINEL, ""
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


US_STATE_CODES = {
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id",
    "il", "in", "ia", "ks", "ky", "la", "me", "md", "ma", "mi", "mn", "ms",
    "mo", "mt", "ne", "nv", "nh", "nj", "nm", "ny", "nc", "nd", "oh", "ok",
    "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv",
    "wi", "wy",
}

US_STATE_FULL = {
    "al": "alabama", "ak": "alaska", "az": "arizona", "ar": "arkansas",
    "ca": "california", "co": "colorado", "ct": "connecticut", "de": "delaware",
    "fl": "florida", "ga": "georgia", "hi": "hawaii", "id": "idaho",
    "il": "illinois", "in": "indiana", "ia": "iowa", "ks": "kansas",
    "ky": "kentucky", "la": "louisiana", "me": "maine", "md": "maryland",
    "ma": "massachusetts", "mi": "michigan", "mn": "minnesota", "ms": "mississippi",
    "mo": "missouri", "mt": "montana", "ne": "nebraska", "nv": "nevada",
    "nh": "new-hampshire", "nj": "new-jersey", "nm": "new-mexico", "ny": "new-york",
    "nc": "north-carolina", "nd": "north-dakota", "oh": "ohio", "ok": "oklahoma",
    "or": "oregon", "pa": "pennsylvania", "ri": "rhode-island",
    "sc": "south-carolina", "sd": "south-dakota", "tn": "tennessee", "tx": "texas",
    "ut": "utah", "vt": "vermont", "va": "virginia", "wa": "washington",
    "wv": "west-virginia", "wi": "wisconsin", "wy": "wyoming",
}


def _url_mentions_wrong_state(url: str, utility_state: str) -> bool:
    """Return True if the URL clearly points at a state *other* than the utility's.

    Uses path segments and subdomain parts so we don't false-match on
    substrings like 'la' inside 'relay' or 'class'. Intentionally cautious:
    false positives would demote valid results.
    """
    if not utility_state:
        return False
    target = utility_state.lower().strip()
    if len(target) == 2:
        target_code = target
    else:
        target_code = next(
            (code for code, full in US_STATE_FULL.items() if full.replace("-", "") == target.replace(" ", "").replace("-", "")),
            "",
        )
    if not target_code:
        return False

    parsed = urlparse(url)
    # Split into tokens that could carry a state indicator
    path_segments = [seg for seg in parsed.path.lower().split("/") if seg]
    subdomain_parts = [
        part for part in parsed.netloc.lower().replace("www.", "").split(".") if part
    ][:-1]  # drop TLD segment

    tokens = set(path_segments) | set(subdomain_parts)
    # Look for other-state codes as whole path/subdomain tokens
    other_codes = US_STATE_CODES - {target_code}
    for tok in tokens:
        if tok in other_codes:
            return True

    # Look for full state names (with hyphens) elsewhere in URL
    url_lc = url.lower()
    target_full = US_STATE_FULL.get(target_code, "")
    for code, full in US_STATE_FULL.items():
        if code == target_code:
            continue
        # Require a word boundary on both sides: slash, dash, or dot
        pattern = rf"(?:^|[/\-_.]){re.escape(full)}(?:$|[/\-_.])"
        if re.search(pattern, url_lc):
            # Don't false-match if target state name also appears in URL
            if target_full and re.search(rf"(?:^|[/\-_.]){re.escape(target_full)}(?:$|[/\-_.])", url_lc):
                return False
            return True
    return False


def score_search_result(
    result: dict,
    utility_name: str,
    utility_domain: str | None,
    utility_state: str | None = None,
) -> float:
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

    # Wrong-state penalty: demote URLs whose path/subdomain clearly names
    # a different US state (e.g. /al/ when utility is in NY).
    if utility_state and _url_mentions_wrong_state(url, utility_state):
        score -= 40

    # Cancelled/superseded/historical tariff penalty: regulator archives
    # often serve both current and outdated PDFs. Prefer current ones.
    if _is_superseded_url(url, f"{title} {description}"):
        score -= 50

    url_domain = urlparse(url).netloc.replace("www.", "")
    if any(url_domain == d or url_domain.endswith(f".{d}") for d in THIRD_PARTY_DOMAINS):
        return -999  # Hard block — never use aggregator/comparison sites

    return score


# URLs/titles matching these patterns are regulator archives of cancelled or
# superseded tariffs — always demote them when a current alternative exists.
# Handles slash-separated (/cancelled/), underscore (cancelled_tariff),
# URL-encoded spaces (/cancelled%20tariff/), and plain-word occurrences.
_SUPERSEDED_TERMS = (
    r"cancell?ed|superse[dt]ed|historic(?:al)?|archive[ds]?|"
    r"withdrawn|expired|obsolete|rescinded"
)
_URL_SUPERSEDED_RE = re.compile(
    rf"(?:[/_\-\s]|%20)(?:{_SUPERSEDED_TERMS})(?:[/_\-\s]|%20|$)|"
    rf"\b(?:{_SUPERSEDED_TERMS})\b",
    re.IGNORECASE,
)


def _is_superseded_url(url: str, title: str = "") -> bool:
    """Does this URL/title look like a cancelled or superseded tariff?"""
    # Decode URL-encoded characters so /cancelled%20tariff%20pages/ matches.
    try:
        url_decoded = unquote(url)
    except Exception:
        url_decoded = url
    return bool(
        _URL_SUPERSEDED_RE.search(url_decoded)
        or (title and _URL_SUPERSEDED_RE.search(title))
    )


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
            s = score_search_result(r, utility_name, utility_domain, state)
            scored.append((s, r))
        scored.sort(key=lambda x: -x[0])

    if not scored or scored[0][0] < 10:
        log.info("  Phase 1: Brave results insufficient, trying Google Custom Search...")
        google_results = google_search(query, count=10)
        if google_results:
            for r in google_results:
                s = score_search_result(r, utility_name, utility_domain, state)
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

        # If httpx can't connect or gets blocked (403), try Playwright
        if status in (0, 403) and best_score >= 50:
            log.info(f"  Phase 1: httpx returned {status} — trying Playwright for {best_url[:60]}")
            html_js, title_js = fetch_page_js(best_url)
            if html_js == FETCH_JS_DOWNLOAD_SENTINEL:
                # URL is a PDF attachment — accept as rate page and let Phase 2 handle
                log.info(f"  Phase 1: Rate page is a PDF download: {best_url[:60]}")
                return best_url, len(results), all_alt_urls
            if html_js and len(html_js.strip()) > 200:
                log.info(f"  Phase 1: Playwright succeeded for {best_url[:60]}")
                with _js_rendered_lock:
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


def _find_relevant_links(html: str, base_url: str) -> list[tuple[str, str]]:
    """Extract relevant rate-page links from raw HTML content."""
    soup = BeautifulSoup(html, "html.parser")
    return _extract_links(soup, base_url)


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
        log.info(f"    Got 403 on {url[:70]}, trying Playwright...")
        with _js_rendered_lock:
            _js_rendered_domains.add(domain)
        return _fetch_and_parse_js(url)
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

    # If URL/title suggest rate content but body has no rate signals ($/kWh etc.),
    # the data is likely loaded via JavaScript — retry with Playwright.
    if (RATE_TITLE_KEYWORDS.search(f"{title} {url}")
            and not RATE_CONTENT_SIGNALS.search(text)):
        log.info(f"    Title/URL suggests rates but no rate signals in text — trying Playwright...")
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


def _fetch_as_pdf_via_download(url: str) -> RatePage | None:
    """Treat a URL as a PDF download: fetch bytes via Playwright's download
    handler and extract text with pdfplumber/OCR.

    Returns a PDF-typed RatePage on success or None if the URL doesn't
    actually resolve to a PDF or extraction yields no content.
    """
    pdf_bytes = _download_pdf_playwright(url)
    if not pdf_bytes or len(pdf_bytes) < 200:
        return None
    # Sanity: PDFs start with '%PDF-'. If not, this was probably a different
    # kind of download (image, zip, etc.) and we shouldn't treat it as text.
    if not pdf_bytes[:5].startswith(b"%PDF-"):
        log.info(f"    Download at {url[:60]} was not a PDF, skipping")
        return None
    text = _extract_pdf_pdfplumber(pdf_bytes)
    if len(text.strip()) < 200:
        text = _extract_pdf_ocr(pdf_bytes) or text
    if not text or len(text.strip()) < 50:
        return None
    # Use URL's last path segment as a working title
    try:
        title_hint = urlparse(url).path.rsplit("/", 1)[-1] or "PDF"
    except Exception:
        title_hint = "PDF"
    return RatePage(
        url=url,
        title=title_hint,
        page_type="pdf",
        content=text,
        content_hash=hashlib.sha256(text.encode()).hexdigest(),
        pdf_bytes=pdf_bytes,
    )


def _fetch_and_parse_js(url: str) -> RatePage | None:
    """Fetch a page using headless Playwright and extract text.

    If the URL triggers a browser download instead of rendering a page
    (common with municipal DocumentCenter/Download endpoints), falls back
    to downloading as a PDF and returning a PDF-typed RatePage.
    """
    html, title = fetch_page_js(url)
    # Download fallback: the server sent Content-Disposition: attachment
    # so there's no page to render — grab the bytes and treat as a PDF.
    if html == FETCH_JS_DOWNLOAD_SENTINEL:
        log.info(f"    Trying PDF download for {url[:70]}")
        return _fetch_as_pdf_via_download(url)
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
        if html_js == FETCH_JS_DOWNLOAD_SENTINEL:
            pdf_page = _fetch_as_pdf_via_download(rate_page_url)
            return [pdf_page] if pdf_page else []
        if not html_js:
            log.warning(f"  Phase 2: Playwright also failed for {rate_page_url[:60]}")
            return []
        content = html_js
        status = 200
    else:
        content, ctype, status = fetch_page(rate_page_url)

    if status != 200:
        if status == 403:
            log.info(f"  Phase 2: Got 403 — trying Playwright...")
            html_js, title_js = fetch_page_js(rate_page_url)
            if html_js == FETCH_JS_DOWNLOAD_SENTINEL:
                pdf_page = _fetch_as_pdf_via_download(rate_page_url)
                return [pdf_page] if pdf_page else []
            if html_js and len(html_js.strip()) > 200:
                content = html_js
                with _js_rendered_lock:
                    _js_rendered_domains.add(domain)
            else:
                log.warning(f"  Phase 2: Playwright also blocked for {rate_page_url[:60]}")
                return []
        elif status == 0:
            log.info(f"  Phase 2: httpx connection failed — trying Playwright...")
            html_js, title_js = fetch_page_js(rate_page_url)
            if html_js == FETCH_JS_DOWNLOAD_SENTINEL:
                pdf_page = _fetch_as_pdf_via_download(rate_page_url)
                return [pdf_page] if pdf_page else []
            if html_js and len(html_js.strip()) > 200:
                content = html_js
                with _js_rendered_lock:
                    _js_rendered_domains.add(domain)
            else:
                log.warning(f"  Phase 2: Failed to fetch rate page (status={status})")
                return []
        else:
            log.warning(f"  Phase 2: Failed to fetch rate page (status={status})")
            return []

    soup = BeautifulSoup(content, "lxml")
    text = _extract_text(BeautifulSoup(content, "lxml"))
    title = soup.title.string.strip() if soup.title and soup.title.string else ""

    # If httpx returned thin content, domain needs JS, or title suggests rates
    # but body has no rate signals (JS-loaded data), use Playwright.
    title_hints_rates = (
        RATE_TITLE_KEYWORDS.search(f"{title} {rate_page_url}")
        and not RATE_CONTENT_SIGNALS.search(text)
    )
    needs_playwright = (
        len(text.strip()) < 200
        or domain in _js_rendered_domains
        or title_hints_rates
    )
    if needs_playwright:
        if domain not in _js_rendered_domains:
            log.info(f"  Phase 2: Thin httpx content, retrying main page with Playwright...")
        else:
            log.info(f"  Phase 2: Known JS-rendered domain, using Playwright...")
        _js_rendered_domains.add(domain)
        html_js, title_js = fetch_page_js(rate_page_url)
        if html_js == FETCH_JS_DOWNLOAD_SENTINEL:
            pdf_page = _fetch_as_pdf_via_download(rate_page_url)
            return [pdf_page] if pdf_page else []
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

    # If the main page is clearly rate-themed, *also* pick up all PDF links
    # regardless of anchor text. Utilities often link to tariff/rate PDFs
    # with generic text like "Download" or "View" that _is_relevant_link
    # would otherwise filter out.
    page_is_rate_themed = bool(
        RATE_TITLE_KEYWORDS.search(f"{main_page.title} {rate_page_url}")
        or RATE_CONTENT_SIGNALS.search(text)
    )
    if page_is_rate_themed:
        existing_urls = {u for u, _ in level1_links}
        extra_pdfs: list[tuple[str, str]] = []
        for a in link_soup.find_all("a", href=True):
            full_url = urljoin(rate_page_url, a["href"]).split("#")[0]
            if not full_url.lower().endswith(".pdf"):
                continue
            if full_url in existing_urls:
                continue
            # Stay on same domain to avoid off-site PDFs
            if not is_same_domain(full_url, rate_page_url):
                continue
            link_text = a.get_text(strip=True) or "PDF"
            extra_pdfs.append((full_url, link_text))
            existing_urls.add(full_url)
        if extra_pdfs:
            log.info(
                f"  Phase 2: Rate-themed page — found {len(extra_pdfs)} "
                f"additional PDF links beyond filtered set"
            )
            level1_links.extend(extra_pdfs)

    log.info(f"  Phase 2: Found {len(level1_links)} relevant links on main page")

    # Demote cancelled/superseded URLs to the bottom so that when we hit the
    # MAX_LEVEL1 cap we keep current tariffs. Regulator archives (psc.ky.gov,
    # etc.) often list both current and cancelled PDFs; we want current first.
    if any(_is_superseded_url(u, t) for u, t in level1_links):
        before_count = sum(1 for u, t in level1_links if _is_superseded_url(u, t))
        level1_links = sorted(
            level1_links,
            key=lambda ut: 1 if _is_superseded_url(ut[0], ut[1]) else 0,
        )
        log.info(
            f"  Phase 2: Demoted {before_count} cancelled/superseded links "
            f"to end of queue"
        )

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
                if sub_html and sub_html != FETCH_JS_DOWNLOAD_SENTINEL:
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
    # Capture full-page text BEFORE stripping, in case structured extraction
    # accidentally removes rate data hidden in non-standard elements.
    full_raw_text = soup.get_text(separator="\n", strip=True)

    for tag in soup(["script", "style", "noscript", "iframe"]):
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
    text = el.get_text(separator="\n", strip=True)[:15000]

    # If structured extraction missed rate-bearing content (common with JS
    # frameworks, Angular, React apps using custom components), fall back to
    # the pre-decomposition full-page text.
    if len(text) < 500 or (
        not RATE_CONTENT_SIGNALS.search(text) and RATE_CONTENT_SIGNALS.search(full_raw_text)
    ):
        text = _compress_whitespace(full_raw_text)[:15000]

    return text


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


RATE_TITLE_KEYWORDS = re.compile(
    r"\brate[s]?\b|\btariff|\bpricing|\bbilling.*rate|\bschedule\b|\belectric.*charge|"
    r"\bresidential.*service|\bgeneral.*service|\bcost.*electric|\belectricity.*cost",
    re.IGNORECASE,
)


def _page_has_rate_content(text: str, title: str = "", url: str = "") -> bool:
    """Check if a page likely contains rate data.

    Uses two tiers:
    - Strong signal: body text matches rate content regex ($/kWh, etc.)
    - Weak signal: title or URL mentions rates/tariffs/pricing — even if the
      body is sparse (e.g. JS-rendered pages where the text hasn't loaded).
      This lets the LLM see the page instead of blindly skipping it.
    """
    if RATE_CONTENT_SIGNALS.search(text):
        return True
    if title and RATE_TITLE_KEYWORDS.search(title):
        return True
    if url and RATE_TITLE_KEYWORDS.search(url):
        return True
    return False


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


PAGE_SCREENSHOT_EXTRACTION_PROMPT = """Extract all residential and commercial electricity tariffs visible in this web page screenshot.

The page may display rate information as images, charts, infographics, or styled tables that don't appear in the raw HTML text.

For each tariff provide: name, code, customer_class ("residential"/"commercial"), rate_type, description, effective_date, confidence (0-1), and components array.
Each component needs: component_type ("energy"/"demand"/"fixed"/"minimum"/"adjustment"), unit, rate_value, and optional tier_min_kwh, tier_max_kwh, tier_label, period_label, season.

Rules:
- Read numbers exactly as shown — do NOT estimate
- Convert cents to dollars (divide by 100)
- Include ALL tiers, periods, seasonal variations visible
- Skip industrial/lighting/irrigation/wholesale tariffs
- If you cannot see any clear residential or commercial electricity rates in the image, return an empty tariffs array
- Set confidence 0.9+ if values clearly readable, 0.5-0.8 if some ambiguity

Use the store_tariffs tool to return results."""


# Max viewport height for the full-page screenshot. Most rate pages fit in
# one or two screens; clamping avoids Vision token blowups on very long pages.
MAX_SCREENSHOT_HEIGHT_PX = 6000


def _fetch_full_page_screenshot(url: str, wait_ms: int = 3000) -> bytes | None:
    """Render a page with Playwright and capture a full-page JPEG screenshot.

    Used as a fallback for rate-themed pages whose rate data is embedded as
    images/graphics (C2 pattern) — Fix 12. Returns None on any failure.
    """
    if not _get_pw_mgr().is_available:
        return None
    context = None
    try:
        context = _get_pw_mgr().new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            ignore_https_errors=True,
        )
        page = context.new_page()
        page.set_viewport_size({"width": 1440, "height": 2200})
        page.goto(url, wait_until="networkidle", timeout=25000)
        page.wait_for_timeout(wait_ms)
        png = page.screenshot(
            full_page=True,
            type="jpeg",
            quality=80,
            clip=None,
        )
        return png
    except Exception as e:
        log.info(f"    Screenshot capture failed for {url[:60]}: {e}")
        return None
    finally:
        if context:
            try:
                context.close()
            except Exception:
                pass


def _extract_page_screenshot_vision(url: str) -> tuple[list[ExtractedTariff], int]:
    """Capture a full-page screenshot of a rate-themed page and ask Claude
    Vision to extract tariffs visible in the rendered image.

    This is Fix 12: targets pages where rate data is displayed as images,
    infographics, or canvas-rendered content that text extraction misses.
    Returns (tariffs, llm_call_count).
    """
    img_bytes = _fetch_full_page_screenshot(url)
    if not img_bytes:
        return [], 0

    import base64

    b64 = base64.standard_b64encode(img_bytes).decode("ascii")
    log.info(f"    Page screenshot vision: sending image ({len(img_bytes)} bytes) to Claude")

    content_blocks = [
        {"type": "text", "text": PAGE_SCREENSHOT_EXTRACTION_PROMPT},
        {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": b64,
            },
        },
    ]

    client = _get_anthropic_client()
    try:
        resp = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=4096,
            messages=[{"role": "user", "content": content_blocks}],
            tools=[TARIFF_EXTRACTION_TOOL],
            tool_choice={"type": "tool", "name": "store_tariffs"},
        )
    except Exception as e:
        log.error(f"    Page screenshot vision API call failed: {e}")
        return [], 1

    for block in resp.content:
        if block.type == "tool_use" and block.name == "store_tariffs":
            raw = block.input.get("tariffs", [])
            return _parse_extraction_response(raw, url), 1

    return [], 1


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


MAX_CONSECUTIVE_LLM_ZEROS = 5


def phase3_extract_tariffs(
    pages: list[RatePage],
    utility_name: str,
    stats: dict | None = None,
) -> list[ExtractedTariff]:
    """Use Claude to extract structured tariff data from each rate page.

    Detail pages are processed first so they win dedup over overview pages
    that only list tariff names without rate values.

    Complex pages (long content with many rate signals) use a two-pass
    approach: identify tariffs first, then extract each individually.

    Args:
        pages: Candidate pages to process.
        utility_name: Target utility name.
        stats: Optional dict the caller can pass in to receive counts:
               {pages_total, pages_skipped_thin, pages_skipped_irrelevant,
                pages_skipped_no_signal, pages_sent_to_llm, llm_zero_results,
                llm_errors, early_abort}. Mutated in place.
    """
    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    def _depth(p: RatePage) -> int:
        return -urlparse(p.url).path.count("/")

    sorted_pages = sorted(pages, key=_depth)

    MAX_LLM_CALLS = 12
    all_tariffs: dict[str, ExtractedTariff] = {}
    llm_calls = 0
    consecutive_zeros = 0

    # Tracking for structured error messages
    if stats is None:
        stats = {}
    stats.setdefault("pages_total", len(pages))
    stats.setdefault("pages_skipped_thin", 0)
    stats.setdefault("pages_skipped_irrelevant", 0)
    stats.setdefault("pages_skipped_no_signal", 0)
    stats.setdefault("pages_sent_to_llm", 0)
    stats.setdefault("llm_zero_results", 0)
    stats.setdefault("llm_errors", 0)
    stats.setdefault("early_abort", False)

    for page in sorted_pages:
        page_domain = urlparse(page.url).netloc.replace("www.", "").lower()
        if any(
            page_domain == d or page_domain.endswith(f".{d}")
            for d in THIRD_PARTY_DOMAINS
        ):
            log.info(f"    Skipping {page.url[:70]} (third-party aggregator)")
            stats["pages_skipped_irrelevant"] += 1
            continue
        if not page.content or len(page.content.strip()) < 100:
            log.info(f"    Skipping {page.url[:60]} (no/little content)")
            stats["pages_skipped_thin"] += 1
            continue
        if SKIP_KEYWORDS.search(f"{page.url} {page.title}"):
            log.info(f"    Skipping {page.title or page.url[:60]} (irrelevant category)")
            stats["pages_skipped_irrelevant"] += 1
            continue
        if not _page_has_rate_content(page.content, title=page.title, url=page.url):
            log.info(f"    Skipping {page.title or page.url[:60]} (no rate content signals)")
            stats["pages_skipped_no_signal"] += 1
            continue
        if llm_calls >= MAX_LLM_CALLS:
            log.info(f"    Stopping: reached {MAX_LLM_CALLS} LLM call limit")
            break
        if consecutive_zeros >= MAX_CONSECUTIVE_LLM_ZEROS:
            log.info(
                f"    Early abort: {consecutive_zeros} consecutive 0-tariff "
                f"LLM extractions on this URL tree — likely wrong site"
            )
            stats["early_abort"] = True
            break

        log.info(f"  Phase 3: Extracting from {page.url[:80]}")
        stats["pages_sent_to_llm"] += 1

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
                consecutive_zeros = 0
                time.sleep(1)
                continue
            elif page.content:
                log.info(f"    Vision returned no usable tariffs, falling back to text extraction")
            else:
                log.info(f"    Vision returned no usable tariffs and no text content available")
                stats["llm_zero_results"] += 1
                consecutive_zeros += 1
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
                stats["llm_errors"] += 1
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
        if accepted > 0:
            consecutive_zeros = 0
        else:
            # Fix 12: if text extraction returned 0 on a rate-themed HTML
            # page whose body has no numeric rate signals, the rates may be
            # embedded as images/graphics. Try a full-page screenshot + Vision
            # before giving up on this page.
            if (
                page.page_type != "pdf"
                and is_rate_relevant_url(page.url, page.title or "")
                and not _page_has_numeric_rates(page.content)
            ):
                log.info(
                    "    Text extraction returned 0 on a rate-themed page "
                    "with no numeric signals — trying page-screenshot vision"
                )
                try:
                    vision_tariffs, vision_calls = _extract_page_screenshot_vision(page.url)
                    llm_calls += vision_calls
                    vision_accepted = 0
                    for t in vision_tariffs:
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
                        vision_accepted += 1
                    if vision_accepted > 0:
                        log.info(
                            f"    Recovered {vision_accepted} tariffs via page-screenshot "
                            f"vision from {page.url[:60]}"
                        )
                        consecutive_zeros = 0
                        time.sleep(1)
                        continue
                except Exception as e:
                    log.warning(f"    Page-screenshot vision failed: {e}")
            consecutive_zeros += 1
            stats["llm_zero_results"] += 1
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
    """Lazy-init Google GenAI client (new SDK) with configurable HTTP timeout.

    Default is 60s — Gemini 3 Flash Preview occasionally takes 30-50s on
    large pages and was hitting DEADLINE_EXCEEDED on a shorter timeout.
    """
    client = getattr(_thread_local, "gemini_client", None)
    if client is None:
        from google import genai
        client = genai.Client(
            api_key=GOOGLE_AI_API_KEY,
            http_options={"timeout": GEMINI_TIMEOUT_MS},
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


# Cache for whether google-genai SDK is installed. Checked once so we
# don't spam import errors in hot paths and so _select_model can skip
# Gemini entirely when the SDK is missing.
_GEMINI_SDK_AVAILABLE: bool | None = None


def _gemini_sdk_available() -> bool:
    """Return True iff the google-genai package can be imported.

    Cached after first call. If False, _select_model will skip Gemini
    entirely and route directly to Haiku.
    """
    global _GEMINI_SDK_AVAILABLE
    if _GEMINI_SDK_AVAILABLE is not None:
        return _GEMINI_SDK_AVAILABLE
    try:
        import google.genai  # noqa: F401
        _GEMINI_SDK_AVAILABLE = True
    except ImportError:
        log.warning(
            "google-genai SDK is not installed — Gemini tier disabled, "
            "routing all requests to Haiku/Opus"
        )
        _GEMINI_SDK_AVAILABLE = False
    return _GEMINI_SDK_AVAILABLE


def _call_gemini(prompt: str) -> list[dict]:
    """Call Gemini Flash for structured tariff extraction.
    Uses the google-genai SDK with JSON schema enforcement.
    Feeds into a circuit breaker on repeated failures.
    Returns list of tariff dicts matching the same schema as Claude tool use.

    Returns [] and records a circuit-breaker failure on any error
    (including ImportError when the SDK isn't installed), so the
    caller can fall back to Haiku without crashing the page.
    """
    try:
        from google.genai import types  # type: ignore
    except ImportError as e:
        _gemini_record_failure()
        log.warning(f"    Gemini SDK unavailable: {e}")
        return []

    try:
        client = _get_gemini_client()
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
    if not GOOGLE_AI_API_KEY or not _gemini_sdk_available():
        return "haiku"

    if _gemini_circuit_open():
        return "haiku"

    if page.page_type == "pdf" and page.pdf_bytes:
        return "haiku"

    if page.content and _is_complex_page(page.content):
        return "haiku"

    return "gemini"


def _call_opus_tool(prompt: str) -> list[dict]:
    """Call Claude Opus 4.7 for structured tariff extraction.

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


_NUMERIC_RATE_SIGNAL = re.compile(
    r"\$\s*\d+\.\d{2,4}|"                     # $0.0956, $12.50
    r"\d+\.\d+\s*(?:cents|¢)|"                # 9.56 cents
    r"\d+\.\d+\s*/\s*k[wW]h|"                 # 0.0956/kWh
    r"\d+\.\d+\s*/\s*k[wW]\b|"                # $25.00/kW
    r"\$\d+\s*/\s*month",                     # $12/month
    re.IGNORECASE,
)


def _page_has_numeric_rates(content: str) -> bool:
    """Does the page contain at least one rate-amount-shaped number?

    Pages where both Haiku and Gemini returned 0 AND which contain no
    numeric rate values cannot produce extractable tariffs no matter
    which LLM we use — so we skip escalating to the expensive Opus tier.
    """
    if not content:
        return False
    return bool(_NUMERIC_RATE_SIGNAL.search(content))


def _extract_with_model_routing(prompt: str, page: "RatePage") -> tuple[list[dict], str]:
    """Extract tariffs using a 3-tier model strategy.

    Tier 1: Gemini 3 Flash  (fast, cheap, good for most pages)
    Tier 2: Claude Haiku    (better at complex HTML, tool use)
    Tier 3: Claude Opus 4.7 (last resort — only invoked if the page has
                             at least one rate-amount-shaped number so we
                             don't burn Opus tokens on pages without data)

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

    # Only escalate to Opus if the page actually contains numeric rate data.
    # Pages with rate-themed titles but no numbers (e.g. marketing pages
    # that link to PDFs) can't yield tariffs from any LLM, so escalating
    # to the expensive Opus tier wastes tokens.
    if not _page_has_numeric_rates(page.content):
        log.info(
            "    Haiku returned 0; skipping Opus escalation "
            "(page has no numeric rate signals)"
        )
        return [], "haiku"

    log.info("    Haiku returned no tariffs, escalating to Opus 4.7")
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

    # Also check URL, domain and page title for utility name words.
    # Pages can have sparse body text (SPAs, PDF landing pages, JS-loaded
    # content) but still clearly belong to the target utility based on
    # their URL path and title.
    url_title_text = " ".join(
        f"{urlparse(p.url).netloc} {urlparse(p.url).path} {p.title or ''}".lower()
        for p in pages
    )
    url_title_matches = sum(1 for w in name_words if w in url_title_text)

    # If URL/domain/title strongly identifies the utility, accept even
    # when body text is sparse.
    if url_title_matches >= 2 or (
        url_title_matches >= 1 and len(name_words) <= 2
    ):
        matches = max(matches, url_title_matches)
        ratio = max(ratio, url_title_matches / max(len(name_words), 1))

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

            score = score_search_result(r, utility_name, utility_domain, state)
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


NAVIGATE_PROMPT = """You are navigating a utility company's website to find their electricity rate information.

Utility: {utility_name}
State: {state}
Current page: {current_url}
Page title: {page_title}

Here are ALL the links on this page:
{link_list}

Which links are most likely to lead to electricity rate/tariff/pricing information?
Think step-by-step: rates might be under "Residential", "Services", "Billing", "Customer Service", "Electric", or similar sections.

Return a JSON array of the link URLs (max 5) most likely to contain or lead to rate information, ordered by likelihood.
Return ONLY the JSON array, no explanation.
"""


def _extract_all_links(html: str, base_url: str) -> list[tuple[str, str]]:
    """Extract ALL links from an HTML page (not just rate-relevant ones)."""
    soup = BeautifulSoup(html, "html.parser")
    links = []
    seen = set()
    base_domain = urlparse(base_url).netloc
    for a in soup.find_all("a", href=True):
        full_url = urljoin(base_url, a["href"]).split("#")[0].split("?")[0]
        if full_url in seen:
            continue
        link_domain = urlparse(full_url).netloc
        if link_domain and link_domain != base_domain:
            continue
        link_text = a.get_text(strip=True)[:80]
        if not link_text or len(link_text) < 2:
            continue
        if full_url.endswith(('.jpg', '.png', '.gif', '.css', '.js', '.ico')):
            continue
        seen.add(full_url)
        links.append((full_url, link_text))
    return links


def _pw_fetch_as_page(
    url: str,
    wait_ms: int = 5000,
    ignore_https_errors: bool = False,
) -> "RatePage | None":
    """Fetch a URL with Playwright and return as a RatePage.
    Falls back to PDF download if the URL triggers a file attachment."""
    try:
        html_js, title_js = fetch_page_js(
            url, wait_ms=wait_ms, ignore_https_errors=ignore_https_errors
        )
        if html_js == FETCH_JS_DOWNLOAD_SENTINEL:
            return _fetch_as_pdf_via_download(url)
        if not html_js or len(html_js.strip()) < 200:
            return None
        text = _compress_whitespace(
            BeautifulSoup(html_js, "html.parser").get_text(" ", strip=True)
        )
        if len(text.strip()) < 50:
            return None
        return RatePage(
            url=url,
            title=title_js or "",
            page_type="html",
            content=text,
            content_hash=hashlib.sha256(text.encode()).hexdigest(),
        )
    except Exception:
        return None


def _build_extraction_failure_reason(stats: dict, rate_page_url: str | None) -> str:
    """Build a diagnostic failure message from pipeline stats.

    Shows *where* extraction broke down so operators can triage faster
    rather than seeing the generic 'No tariffs extracted from any candidate
    page' message for every failure mode.
    """
    pages_total = int(stats.get("pages_total", 0))
    llm_sent = int(stats.get("pages_sent_to_llm", 0))
    llm_zero = int(stats.get("llm_zero_results", 0))
    llm_err = int(stats.get("llm_errors", 0))
    skip_thin = int(stats.get("pages_skipped_thin", 0))
    skip_irrel = int(stats.get("pages_skipped_irrelevant", 0))
    skip_nosig = int(stats.get("pages_skipped_no_signal", 0))
    early_abort = bool(stats.get("early_abort", False))

    # No pages found at all
    if pages_total == 0:
        if rate_page_url:
            return "No candidate pages found after visiting rate page"
        return "No candidate pages found — search returned no usable URLs"

    # Pages found but all filtered before reaching the LLM
    if llm_sent == 0:
        parts = []
        if skip_nosig:
            parts.append(f"{skip_nosig} pages had no rate content signals")
        if skip_irrel:
            parts.append(f"{skip_irrel} pages were off-topic")
        if skip_thin:
            parts.append(f"{skip_thin} pages had too little content")
        detail = "; ".join(parts) if parts else f"{pages_total} pages did not pass filters"
        return f"No pages reached the LLM: {detail}"

    # LLM was called but returned no usable tariffs
    if llm_zero and llm_zero == llm_sent:
        note = " (aborted early after 5 consecutive 0-extractions)" if early_abort else ""
        return f"LLM returned 0 tariffs on all {llm_sent} pages{note}"
    if llm_err and llm_err >= llm_sent:
        return f"LLM errored on all {llm_sent} attempts — possible API issue"

    # Mixed — some extracted but none passed validation
    return (
        f"Extracted tariffs did not pass validation "
        f"({pages_total} pages total, {llm_sent} reached LLM, {llm_zero} returned 0)"
    )


def _phase5_smart_retry(
    utility_name: str,
    state: str,
    website_url: str | None,
    existing_pages: list["RatePage"],
) -> tuple[list["ExtractedTariff"], list["RatePage"], dict]:
    """Phase 5: AI-guided website navigation when the normal pipeline fails.

    Works like a human would: loads the utility's homepage, shows the LLM
    all the navigation links, and asks it which ones likely lead to rate info.
    Then follows those links and extracts tariffs.

    Two-level deep: homepage → LLM picks links → follow links → if needed,
    LLM picks sub-links → follow those too.

    Returns (tariffs, pages, stats_dict).
    """
    stats = {
        "pages_total": 0,
        "pages_sent_to_llm": 0,
        "llm_zero_results": 0,
        "llm_errors": 0,
        "phase5_homepage_failed": False,
        "phase5_no_links": False,
        "phase5_ai_picked": 0,
    }

    if not ANTHROPIC_API_KEY or not website_url:
        return [], [], stats

    log.info("  Phase 5: AI-guided website navigation...")

    # Step 1: Load homepage with Playwright (longer wait for JS).
    # Use SSL-tolerant fetch so sites with mismatched/expired certs still work.
    html_js, title_js = fetch_page_js(website_url, wait_ms=5000, ignore_https_errors=True)
    if html_js == FETCH_JS_DOWNLOAD_SENTINEL:
        log.info("  Phase 5: Homepage URL triggered a download, treating as PDF")
        pdf_page = _fetch_as_pdf_via_download(website_url)
        if pdf_page:
            return [pdf_page], [], stats
        stats["phase5_homepage_failed"] = True
        return [], [], stats
    if not html_js or len(html_js.strip()) < 200:
        log.info("  Phase 5: Could not load homepage")
        stats["phase5_homepage_failed"] = True
        return [], [], stats

    all_links = _extract_all_links(html_js, website_url)
    if not all_links:
        log.info("  Phase 5: No links found on homepage")
        stats["phase5_no_links"] = True
        return [], [], stats

    link_list = "\n".join(f"- {text}: {url}" for url, text in all_links[:50])

    # Step 2: Ask LLM which links to follow
    prompt = NAVIGATE_PROMPT.format(
        utility_name=utility_name,
        state=state,
        current_url=website_url,
        page_title=title_js or "Unknown",
        link_list=link_list,
    )

    try:
        client = _get_anthropic_client()
        resp = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```\w*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)
        nav_urls = json.loads(text)
        if not isinstance(nav_urls, list):
            nav_urls = [nav_urls]
    except Exception as e:
        log.warning(f"  Phase 5: Navigation AI failed: {e}")
        return [], [], stats

    log.info(f"  Phase 5: AI chose {len(nav_urls)} links to follow")
    stats["phase5_ai_picked"] = len(nav_urls)

    # Step 3: Fetch each suggested page with Playwright
    all_pages: list[RatePage] = []
    for url in nav_urls[:5]:
        if not isinstance(url, str) or not url.startswith("http"):
            continue
        nav_domain = urlparse(url).netloc.replace("www.", "").lower()
        if any(
            nav_domain == d or nav_domain.endswith(f".{d}")
            for d in THIRD_PARTY_DOMAINS
        ):
            log.info(f"  Phase 5: Skipping third-party aggregator: {url[:70]}")
            continue
        log.info(f"  Phase 5: Following {url[:80]}")
        page = _pw_fetch_as_page(url, ignore_https_errors=True)
        if page:
            all_pages.append(page)

            # Check sub-page links too (one level deeper)
            sub_html, _ = fetch_page_js(url, wait_ms=3000, ignore_https_errors=True)
            if sub_html and sub_html != FETCH_JS_DOWNLOAD_SENTINEL:
                sub_links = _extract_all_links(sub_html, url)
                rate_sub = [
                    (u, t) for u, t in sub_links
                    if RATE_TITLE_KEYWORDS.search(f"{u} {t}")
                ]
                for sub_url, sub_text in rate_sub[:3]:
                    sub_page = _pw_fetch_as_page(sub_url, wait_ms=3000, ignore_https_errors=True)
                    if sub_page:
                        all_pages.append(sub_page)

    if not all_pages:
        return [], [], stats

    log.info(f"  Phase 5: Collected {len(all_pages)} pages, sending to LLM")

    try:
        phase3_stats: dict = {}
        tariffs = phase3_extract_tariffs(all_pages, utility_name, stats=phase3_stats)
        # Merge Phase 3 stats into Phase 5 stats
        for k, v in phase3_stats.items():
            if k in stats and isinstance(stats[k], (int, float)):
                stats[k] += int(v)
            else:
                stats[k] = v
        return tariffs, all_pages, stats
    except Exception as e:
        log.warning(f"  Phase 5: Extraction failed: {e}")
        return [], [], stats


# ---------------------------------------------------------------------------
# Phase 6: Deep Research fallback (Gemini Interactions API)
# ---------------------------------------------------------------------------
#
# Last-resort recovery tier for utilities that fail Phases 1-5. The Gemini
# Deep Research agent performs a long-horizon, multi-source web investigation
# on our behalf and returns a cited report. Typical cost per call is on the
# order of $1.50-$2.50 and latency is 5-15 minutes, so we only run this on
# the ~5% long tail where all faster tiers struck out.
#
# Cost/safety guardrails:
#   - Gated behind env var PHASE6_ENABLED (default off)
#   - Client-side wall-clock cap via PHASE6_MAX_WAIT_SEC (default 1200s);
#     the Interactions cancel() API does NOT actually abort a running task,
#     so we simply stop polling and let the server-side work time out.
#   - Skips instantly if google-genai SDK isn't installed or no API key.
#   - Never raises — returns ([], stats_with_error) so the pipeline continues.

PHASE6_AGENT_DEFAULT = "deep-research-preview-04-2026"
PHASE6_MAX_WAIT_SEC_DEFAULT = 1200
PHASE6_POLL_INTERVAL_SEC = 20
# Per-call token ceiling. Deep Research calls normally use 800k–3M tokens;
# anything above ~3M indicates the agent is spinning on a pathological target
# (our 16-utility retest had one utility balloon to 9.27M tokens ≈ $13 for an
# empty report). We cancel client-side once we observe usage exceeding this.
PHASE6_MAX_TOKENS_DEFAULT = 3_000_000


def _phase6_enabled() -> bool:
    return os.environ.get("PHASE6_ENABLED", "").strip().lower() in ("1", "true", "yes", "on")


def _phase6_prompt(utility_name: str, state: str, attempted_urls: list[str] | None) -> str:
    """Build the Deep Research prompt.

    We stuff URLs we already tried into the prompt so the agent doesn't waste
    tokens rediscovering pages we've already confirmed fail (Cloudflare blocks,
    Angular SPA shells, image-only rate pages, etc.).
    """
    tried_clause = ""
    if attempted_urls:
        # Keep this tight — just the first 10 unique URLs
        seen: set[str] = set()
        trimmed: list[str] = []
        for u in attempted_urls:
            if u and u not in seen:
                seen.add(u)
                trimmed.append(u)
            if len(trimmed) >= 10:
                break
        if trimmed:
            tried_clause = (
                "\n\nOur existing scraper already tried these URLs and could "
                "not extract tariffs from them — either the data is rendered "
                "client-side, the site blocked automated access, or the rates "
                "are shown as images/graphics. You can still consult them, but "
                "prioritize OTHER authoritative sources such as state PUC/PSC "
                "filings, cached versions, or linked tariff PDFs:\n"
                + "\n".join(f"  - {u}" for u in trimmed)
            )

    return f"""Research task (scope-bounded, 10 minutes maximum):

Find the current published residential and commercial electricity tariffs for {utility_name} in {state}, USA.

Authoritative sources ONLY:
- The utility's own corporate website (tariff/rates pages, tariff PDFs).
- Filings on the relevant state Public Utility / Service Commission.
- Directly-linked utility tariff PDFs or regulatory orders.

Do NOT use third-party comparison or aggregator sites (energybot.com, electricrate.com, choose-energy.com, power2switch.com, nyenergyratings.com, saveonenergy.com, chooseenergy.com, findenergy.com, etc.). Do NOT use wholesale/generation-company sources.{tried_clause}

Scope limits:
- Only RESIDENTIAL and COMMERCIAL schedules. Skip industrial, lighting, irrigation, wholesale, standby, cogen, interruptible, fleet EV charging, and street light.
- Only CURRENT tariffs. Skip anything marked cancelled, superseded, historic, withdrawn, or obsolete.
- Stop once you have a reasonable set for both classes — do not exhaustively catalog every rider or adjustment.

Return your findings as a report ending with a fenced JSON block like this:

```json
[
  {{
    "name": "official tariff name",
    "code": "schedule code",
    "customer_class": "residential" or "commercial",
    "rate_type": "flat" | "tiered" | "tou" | "demand" | "seasonal",
    "effective_date": "YYYY-MM-DD" or null,
    "source_url": "URL you used",
    "confidence": 0.0-1.0,
    "components": [
      {{
        "component_type": "energy" | "fixed" | "demand" | "minimum" | "adjustment",
        "unit": "$/kWh" | "$/kW" | "$/month" | "cents/kWh",
        "rate_value": <number>,
        "tier_min_kwh": <number or null>,
        "tier_max_kwh": <number or null>,
        "tier_label": <string or null>,
        "period_label": "on-peak" | "off-peak" | "shoulder" | null,
        "season": "summer" | "winter" | null
      }}
    ]
  }}
]
```

Rules for the JSON:
- Include each schedule ONCE. Break tiers, seasons, and time-of-use periods out as separate components.
- Read numbers EXACTLY as printed in the source — do not estimate or round.
- If you use "$/kWh" as the unit, convert cents to dollars in rate_value. Otherwise use "cents/kWh" and leave rate_value as-is.
- If you cannot find the utility's current residential/commercial electric tariffs at all from authoritative sources, return an empty array [].
"""


# Regex to extract a fenced JSON code block from the Deep Research report.
# Uses a non-greedy array match that handles nested objects.
_PHASE6_JSON_RE = re.compile(
    r"```\s*json\s*(\[[\s\S]*?\])\s*```",
    re.IGNORECASE,
)


def _phase6_parse_tariffs(report_text: str, fallback_source: str) -> list[ExtractedTariff]:
    """Find and parse the JSON block from a Deep Research report.

    Returns validated ExtractedTariff objects with the same customer-class
    and skip-keyword filtering we apply to Phase 3 LLM output.
    """
    m = _PHASE6_JSON_RE.search(report_text)
    if not m:
        log.warning("  Phase 6: report did not contain a ```json ... ``` block")
        return []
    js_text = m.group(1)
    try:
        raw = json.loads(js_text)
    except json.JSONDecodeError as e:
        log.warning(f"  Phase 6: JSON parse failed ({e})")
        return []
    if not isinstance(raw, list):
        log.warning("  Phase 6: JSON block was not an array")
        return []

    tariffs: list[ExtractedTariff] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        if SKIP_KEYWORDS.search(name):
            log.info(f"    Phase 6: filtered out '{name}' (SKIP_KEYWORDS)")
            continue
        cclass = str(item.get("customer_class") or "").strip().lower()
        if cclass not in VALID_CLASSES:
            log.info(f"    Phase 6: filtered out '{name}' (class={cclass})")
            continue

        components_raw = item.get("components") or []
        components: list[dict] = []
        for c in components_raw:
            if not isinstance(c, dict):
                continue
            try:
                rv = c.get("rate_value")
                rv_float = float(rv) if rv is not None else None
            except (TypeError, ValueError):
                continue
            if rv_float is None:
                continue
            components.append({
                "component_type": str(c.get("component_type") or "energy").strip().lower(),
                "unit": str(c.get("unit") or "$/kWh"),
                "rate_value": rv_float,
                "tier_min_kwh": c.get("tier_min_kwh"),
                "tier_max_kwh": c.get("tier_max_kwh"),
                "tier_label": c.get("tier_label"),
                "period_label": c.get("period_label"),
                "season": c.get("season"),
            })
        if not components:
            log.info(f"    Phase 6: dropping '{name}' (no numeric components)")
            continue

        try:
            confidence = float(item.get("confidence") or 0.7)
        except (TypeError, ValueError):
            confidence = 0.7

        tariffs.append(ExtractedTariff(
            name=name,
            code=str(item.get("code") or "").strip(),
            customer_class=cclass,
            rate_type=str(item.get("rate_type") or "").strip().lower(),
            description=f"Phase 6 Deep Research (item {idx})",
            source_url=str(item.get("source_url") or fallback_source or "").strip(),
            effective_date=str(item.get("effective_date") or "").strip() or "",
            components=components,
            confidence=max(0.0, min(1.0, confidence)),
        ))

    return tariffs


def phase6_deep_research(
    utility_name: str,
    state: str,
    attempted_urls: list[str] | None = None,
    *,
    agent: str | None = None,
    max_wait_sec: int | None = None,
) -> tuple[list[ExtractedTariff], dict]:
    """Gemini Deep Research fallback for utilities that failed Phases 1-5.

    Returns (tariffs, stats). Never raises; all errors surface via
    `stats["phase6_error"]` and the returned tariff list is empty.
    """
    stats: dict = {
        "phase6_attempted": True,
        "phase6_enabled": _phase6_enabled(),
        "phase6_status": None,
        "phase6_elapsed_sec": 0.0,
        "phase6_interaction_id": None,
        "phase6_total_tokens": 0,
        "phase6_input_tokens": 0,
        "phase6_output_tokens": 0,
        "phase6_raw_tariffs_returned": 0,
        "phase6_accepted_tariffs": 0,
        "phase6_error": None,
    }

    if not _phase6_enabled():
        stats["phase6_status"] = "disabled"
        log.info("  Phase 6: skipped (PHASE6_ENABLED is not set)")
        return [], stats
    if not _gemini_sdk_available():
        stats["phase6_status"] = "sdk_missing"
        stats["phase6_error"] = "google-genai SDK not available"
        log.warning("  Phase 6: skipped — google-genai SDK not available")
        return [], stats
    if not GOOGLE_AI_API_KEY:
        stats["phase6_status"] = "no_api_key"
        stats["phase6_error"] = "GOOGLE_AI_API_KEY not set"
        log.warning("  Phase 6: skipped — GOOGLE_AI_API_KEY not set")
        return [], stats

    agent_id = agent or os.environ.get("PHASE6_AGENT", PHASE6_AGENT_DEFAULT)
    try:
        wait_cap = int(max_wait_sec if max_wait_sec is not None
                       else os.environ.get("PHASE6_MAX_WAIT_SEC", PHASE6_MAX_WAIT_SEC_DEFAULT))
    except ValueError:
        wait_cap = PHASE6_MAX_WAIT_SEC_DEFAULT
    try:
        token_cap = int(os.environ.get(
            "PHASE6_MAX_TOKENS", PHASE6_MAX_TOKENS_DEFAULT
        ))
    except ValueError:
        token_cap = PHASE6_MAX_TOKENS_DEFAULT

    log.info(
        f"  Phase 6: Deep Research fallback — agent={agent_id} "
        f"wait_cap={wait_cap}s token_cap={token_cap}"
    )

    prompt = _phase6_prompt(utility_name, state, attempted_urls)

    try:
        from google import genai  # type: ignore
    except ImportError as e:
        stats["phase6_status"] = "sdk_import_error"
        stats["phase6_error"] = str(e)
        return [], stats

    try:
        client = genai.Client(api_key=GOOGLE_AI_API_KEY)
    except Exception as e:
        stats["phase6_status"] = "client_init_error"
        stats["phase6_error"] = str(e)
        log.warning(f"  Phase 6: client init failed ({e})")
        return [], stats

    t0 = time.time()
    try:
        interaction = client.interactions.create(
            input=prompt,
            agent=agent_id,
            background=True,
        )
    except Exception as e:
        stats["phase6_elapsed_sec"] = round(time.time() - t0, 1)
        stats["phase6_status"] = "create_failed"
        stats["phase6_error"] = str(e)
        log.warning(f"  Phase 6: create failed ({e})")
        return [], stats

    iid = getattr(interaction, "id", None)
    stats["phase6_interaction_id"] = iid
    log.info(f"  Phase 6: submitted interaction id={iid}")

    last_status: str | None = None
    last_log_t = 0.0
    final_interaction = interaction
    while True:
        elapsed = time.time() - t0
        if elapsed > wait_cap:
            stats["phase6_status"] = "client_timeout"
            stats["phase6_error"] = f"exceeded client wall-clock cap of {wait_cap}s"
            stats["phase6_elapsed_sec"] = round(elapsed, 1)
            log.warning(
                f"  Phase 6: aborting after {elapsed:.0f}s — agent still "
                f"in_progress (cancel API is non-functional; task will be "
                f"abandoned server-side)"
            )
            try:
                client.interactions.cancel(iid)
            except Exception:
                pass
            return [], stats

        try:
            final_interaction = client.interactions.get(iid)
        except Exception as e:
            stats["phase6_status"] = "poll_error"
            stats["phase6_error"] = str(e)
            stats["phase6_elapsed_sec"] = round(elapsed, 1)
            log.warning(f"  Phase 6: poll failed ({e})")
            return [], stats

        # Mid-flight token ceiling: usage grows as the agent runs, so we can
        # abort a runaway call long before wait_cap expires. Guards us against
        # the occasional pathological target that otherwise burns $10+ before
        # returning an empty report.
        mid_usage = getattr(final_interaction, "usage", None)
        if mid_usage is not None:
            try:
                so_far = int(getattr(mid_usage, "total_tokens", 0) or 0)
            except (TypeError, ValueError):
                so_far = 0
            if so_far and so_far > token_cap:
                stats["phase6_status"] = "token_cap"
                stats["phase6_error"] = (
                    f"exceeded client token cap of {token_cap} "
                    f"(observed {so_far})"
                )
                stats["phase6_elapsed_sec"] = round(elapsed, 1)
                stats["phase6_total_tokens"] = so_far
                log.warning(
                    f"  Phase 6: aborting after {elapsed:.0f}s — "
                    f"token usage {so_far} exceeded cap {token_cap}"
                )
                try:
                    client.interactions.cancel(iid)
                except Exception:
                    pass
                return [], stats

        st = getattr(final_interaction, "status", None)
        if st != last_status or (time.time() - last_log_t) > 120:
            log.info(f"  Phase 6: t={elapsed:.0f}s status={st}")
            last_status = st
            last_log_t = time.time()
        if st in ("completed", "failed", "cancelled"):
            break
        time.sleep(PHASE6_POLL_INTERVAL_SEC)

    elapsed = time.time() - t0
    stats["phase6_elapsed_sec"] = round(elapsed, 1)
    stats["phase6_status"] = getattr(final_interaction, "status", "unknown")

    if stats["phase6_status"] != "completed":
        err = getattr(final_interaction, "error", None)
        stats["phase6_error"] = str(err) if err else "non-completed status"
        log.warning(
            f"  Phase 6: finished with status={stats['phase6_status']} in "
            f"{elapsed:.0f}s — {stats['phase6_error']}"
        )
        return [], stats

    # Collect usage stats if available
    usage = getattr(final_interaction, "usage", None)
    if usage is not None:
        for attr, key in (
            ("total_tokens", "phase6_total_tokens"),
            ("total_input_tokens", "phase6_input_tokens"),
            ("total_output_tokens", "phase6_output_tokens"),
        ):
            try:
                stats[key] = int(getattr(usage, attr, 0) or 0)
            except (TypeError, ValueError):
                pass

    # Concatenate ALL text outputs — Deep Research returns multiple (exec
    # summary, analysis+JSON, citations, ...) and the JSON block is usually
    # NOT in the last one.
    outputs = getattr(final_interaction, "outputs", None) or []
    combined_text = "\n\n".join(
        (getattr(o, "text", "") or "") for o in outputs
    )
    log.info(
        f"  Phase 6: completed in {elapsed:.0f}s — "
        f"{len(outputs)} outputs, {len(combined_text)} chars, "
        f"{stats['phase6_total_tokens']} tokens"
    )

    tariffs = _phase6_parse_tariffs(
        combined_text,
        fallback_source=f"gemini-deep-research://{iid}",
    )
    stats["phase6_raw_tariffs_returned"] = len(tariffs)
    # We still need to dedupe inside the caller, but count what we accepted
    stats["phase6_accepted_tariffs"] = len(tariffs)

    if tariffs:
        log.info(
            f"  Phase 6: recovered {len(tariffs)} tariffs from Deep Research"
        )
    else:
        log.warning(
            "  Phase 6: Deep Research returned no usable tariffs"
        )

    return tariffs, stats


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
        log.warning("  No rate page found — trying AI-guided navigation")
        if website_url:
            smart_tariffs, smart_pages, _ = _phase5_smart_retry(
                utility_name, state, website_url, []
            )
            if smart_tariffs:
                tariffs = smart_tariffs
                pages = smart_pages or []
                # Skip to Phase 4 validation
                validation, valid_tariffs = phase4_validate(tariffs, utility_name, state)
                result.phase4_validation = validation
                if valid_tariffs and not dry_run:
                    store_tariffs(utility_id, valid_tariffs, dry_run)
                    update_monitoring_source(utility_id, smart_pages[0].url if smart_pages else "", dry_run)
                total = len(valid_tariffs)
                log.info(f"=== Done (via Phase 5): {utility_name} — {total} tariffs ===\n")
                return result
        # Phase 6 fallback for the "Phase 1 found nothing" case. Without this,
        # utilities whose corporate site simply isn't in Brave's top results
        # (small munis, obscure co-ops) never get a chance at Deep Research.
        if _phase6_enabled():
            dr_tariffs, phase6_stats = phase6_deep_research(
                utility_name, state, [website_url] if website_url else [],
            )
            if dr_tariffs:
                tariffs = dr_tariffs
                validation, valid_tariffs = phase4_validate(
                    tariffs, utility_name, state
                )
                result.phase4_validation = validation
                if valid_tariffs and not dry_run:
                    store_tariffs(utility_id, valid_tariffs, dry_run)
                    src = (
                        valid_tariffs[0].source_url
                        if valid_tariffs and getattr(
                            valid_tariffs[0], "source_url", None
                        )
                        else ""
                    )
                    if src:
                        update_monitoring_source(utility_id, src, dry_run)
                total = len(valid_tariffs)
                log.info(
                    f"=== Done (via Phase 6): {utility_name} — {total} tariffs ===\n"
                )
                return result
        result.errors.append("No rate page found")
        log.warning("  No rate page found — stopping")
        return result

    # Phases 2+3 with automatic retry: if first URL yields 0 tariffs,
    # pick alternates from a DIFFERENT section of the site.
    # Cap scales with the number of alternates so we always get a chance to
    # try every distinct candidate (prevents "Retrying with <url>..." messages
    # that never actually fetch that URL when MAX_ATTEMPTS is too low).
    # Hard ceiling of 10 to avoid runaway on rare pathological cases.
    MAX_ATTEMPTS = min(10, max(6, 1 + len(alt_urls)))
    tariffs: list[ExtractedTariff] = []
    pages: list[RatePage] = []
    tried_prefixes: set[str] = set()
    tried_domains: set[str] = set()

    def _path_prefix(u: str) -> str:
        parts = urlparse(u).path.strip("/").split("/")
        return "/".join(parts[:2]) if len(parts) >= 2 else parts[0] if parts else ""

    def _alt_domain(u: str) -> str:
        return urlparse(u).netloc.replace("www.", "").lower()

    remaining_alts = list(alt_urls)
    current_url = rate_page_url
    attempts = 0
    # Aggregate stats across all attempts for diagnostic error reporting
    combined_stats = {
        "pages_total": 0,
        "pages_skipped_thin": 0,
        "pages_skipped_irrelevant": 0,
        "pages_skipped_no_signal": 0,
        "pages_sent_to_llm": 0,
        "llm_zero_results": 0,
        "llm_errors": 0,
        "early_abort": False,
    }

    def _pick_next_alt() -> str | None:
        # Prefer alternates on a DIFFERENT domain than any we've already tried.
        # When our initial pick was on a wrong-utility look-alike domain (e.g.
        # lynchesriver.com when the real coop is at lreci.coop), jumping
        # straight to a fresh domain is the fastest path to success.
        for alt in remaining_alts:
            alt_prefix = _path_prefix(alt)
            alt_dom = _alt_domain(alt)
            if alt_prefix not in tried_prefixes and alt_dom not in tried_domains:
                log.warning(
                    f"  Retrying with different-domain URL: {alt[:70]}"
                )
                remaining_alts.remove(alt)
                return alt
        # Fall back to same-domain different-section alternates.
        for alt in remaining_alts:
            alt_prefix = _path_prefix(alt)
            if alt_prefix not in tried_prefixes:
                log.warning(f"  Retrying with different site section: {alt[:70]}")
                remaining_alts.remove(alt)
                return alt
        return None

    def _merge_stats(src: dict):
        for k, v in src.items():
            if v is None:
                continue
            if isinstance(v, bool) or k == "early_abort":
                combined_stats[k] = combined_stats.get(k, False) or bool(v)
            elif isinstance(v, (int, float)):
                existing = combined_stats.get(k, 0)
                if isinstance(existing, (int, float)):
                    combined_stats[k] = existing + v
                else:
                    combined_stats[k] = v
            else:
                # Non-numeric strings (status codes, interaction IDs, errors)
                # — keep the most recent value instead of trying to sum.
                combined_stats[k] = v

    while current_url and attempts < MAX_ATTEMPTS:
        attempts += 1
        prefix = _path_prefix(current_url)
        tried_prefixes.add(prefix)
        tried_domains.add(_alt_domain(current_url))

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
        phase3_stats: dict = {}
        try:
            tariffs = phase3_extract_tariffs(pages, utility_name, stats=phase3_stats)
            result.phase3_tariffs = [asdict(t) for t in tariffs]
        except Exception as e:
            result.errors.append(f"Phase 3 error on {current_url[:60]}: {e}")
            log.error(f"  Phase 3 failed: {e}")
            current_url = _pick_next_alt()
            continue
        finally:
            _merge_stats(phase3_stats)

        # Only count tariffs with real components (energy/fixed/demand),
        # not rate riders or surcharges that Phase 4 would reject.
        base_types = {"energy", "fixed", "demand"}
        has_base_tariff = any(
            any(
                (c.get("component_type") if isinstance(c, dict) else getattr(c, "type", ""))
                in base_types
                for c in t.components
            )
            for t in tariffs
        )
        if has_base_tariff:
            result.phase1_rate_page_url = current_url
            break
        elif tariffs:
            log.info(f"  Found {len(tariffs)} tariffs but all are rate riders/surcharges, trying alternates...")

        current_url = _pick_next_alt()

    if not tariffs:
        log.warning("  No tariffs extracted from any candidate page — trying smart retry")
        # Derive website URL from the rate page we found if we don't have one
        phase5_website = website_url
        if not phase5_website and rate_page_url:
            parsed = urlparse(rate_page_url)
            phase5_website = f"{parsed.scheme}://{parsed.netloc}"
        smart_tariffs, smart_pages, phase5_stats = _phase5_smart_retry(
            utility_name, state, phase5_website, pages
        )
        _merge_stats(phase5_stats)
        if smart_tariffs:
            tariffs = smart_tariffs
            pages = smart_pages or pages
            log.info(f"  Phase 5 smart retry found {len(tariffs)} tariffs")
        else:
            log.warning("  Smart retry also found no tariffs")
            # Phase 6 — Gemini Deep Research as a last-resort fallback.
            # Gated behind PHASE6_ENABLED env var because each call costs
            # ~$1-2 and takes 5-15 minutes. Only worth it for the long tail
            # of utilities that fail every faster tier.
            if _phase6_enabled():
                attempted: list[str] = []
                if rate_page_url:
                    attempted.append(rate_page_url)
                attempted.extend(alt_urls or [])
                for p in (pages or []):
                    if getattr(p, "url", None):
                        attempted.append(p.url)
                dr_tariffs, phase6_stats = phase6_deep_research(
                    utility_name, state, attempted,
                )
                _merge_stats(phase6_stats)
                if dr_tariffs:
                    tariffs = dr_tariffs
                    log.info(
                        f"  Phase 6 Deep Research found {len(tariffs)} tariffs"
                    )
                else:
                    result.errors.append(
                        _build_extraction_failure_reason(combined_stats, rate_page_url)
                    )
            else:
                result.errors.append(
                    _build_extraction_failure_reason(combined_stats, rate_page_url)
                )

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
