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
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

try:
    from scripts import llm_cost
except ImportError:
    import llm_cost

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
# Anthropic-only extraction stack (Gemini removed 2026-10). Exact model ids —
# override via env without code change. Role map:
#   HAIKU_MODEL  — tier-1 cheap first pass (was Gemini Flash)
#   SONNET_MODEL — tier-2 main extract + vision/nav/Track B/browser/two-pass
#                  (was Haiku 4.5)
#   OPUS_MODEL   — tier-3 escalation + long-doc identify (was Opus 5)
HAIKU_MODEL = os.environ.get("HAIKU_MODEL", "claude-haiku-5-5")
SONNET_MODEL = os.environ.get("SONNET_MODEL", "claude-sonnet-5-5")
OPUS_MODEL = os.environ.get("OPUS_MODEL", "claude-opus-5-5")

# Hard cap on how many times a single utility may escalate to the expensive
# Opus tier within one pipeline run. Opus escalations hit on only ~8% of
# pages (measured), so a pathological utility firing 5+ escalations burns
# Opus tokens for near-zero yield. Cap keeps the worst cases bounded.
OPUS_MAX_PER_UTILITY = int(os.environ.get("OPUS_MAX_PER_UTILITY", "2"))

# Per-utility Opus escalation budget (reset at the top of each run_pipeline).
# Celery prefork runs one pipeline per process at a time, so a module global
# is safe here.
_opus_escalations_this_util = 0


def _reset_opus_budget() -> None:
    global _opus_escalations_this_util
    _opus_escalations_this_util = 0

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
    r"high.voltage|primary.*service|subtransmission|"
    r"government\s+(?:department|diesel|building)|"
    r"gov(?:ernment)?\.?\s*dept",
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
# extracts tariffs from them. The set lives with the source classifier so a
# tariff sourced from any of them is labelled ``third_party``.
from app.services.source_type import (  # noqa: E402
    THIRD_PARTY_DOMAINS,
    is_generic_host,
    normalize_host,
    registrable_domain,
)


def _is_third_party_domain(url: str) -> bool:
    """True if the URL's domain is a hard-blocked aggregator/data site.
    Used to keep the crawler off these domains entirely (not just to skip
    extraction after we've already paid to fetch their pages/PDFs)."""
    try:
        dom = urlparse(url).netloc.replace("www.", "").lower()
    except Exception:
        return False
    return any(dom == d or dom.endswith(f".{d}") for d in THIRD_PARTY_DOMAINS)


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
    # Set by Phase 4 when a value passed validation but sits in a
    # suspicious band (above p95, or unit auto-corrected), or by the model
    # via store_tariffs.needs_review. Persisted in confidence_factors.
    needs_review: bool = False
    # Structured TOU/seasonal completeness gap reasons from Phase 4
    # (e.g. tou_missing_clock_windows). Persisted under confidence_factors.
    completeness_reasons: list[str] = field(default_factory=list)
    # Computable-contract reasons (app.services.computable) for TOU/seasonal
    # rate types that cannot price every interval, e.g. tou_gap:weekend.
    computable_reasons: list[str] = field(default_factory=list)
    # Which extraction tier produced it (haiku / sonnet / opus / twopass /
    # vision), for accepted-after-validation yield.
    extraction_tier: str = ""
    # Model-reported gaps (Opus 5.5 prompt review schema). Persisted under
    # confidence_factors; riders_referenced_not_shown also feeds rider fetch.
    missing_fields: list[str] = field(default_factory=list)
    riders_referenced_not_shown: list[str] = field(default_factory=list)
    energy_scope: str = ""
    closed_to_new: bool = False
    energy_includes_riders: bool | None = None
    empty_reason: str = ""
    linked_document_hint: str = ""
    # Non-review notes merged into confidence_factors at store time
    # (e.g. sch102_credit_first_2000_kwh_only). Never alone sets needs_review.
    confidence_notes: dict = field(default_factory=dict)


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
    # True when extraction was skipped because page fingerprints matched —
    # a cheap SUCCESS (content re-verified), not a failure.
    skipped_unchanged: bool = False
    # Per-phase / per-model LLM cost for this utility (see scripts.llm_cost).
    cost: dict = field(default_factory=dict)


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
            # 12s — long enough for legitimate utility PDF downloads,
            # short enough that traversing a regulator index of 30+
            # broken/redirected docket links (which used to trap ConEd
            # for 30s × 30 links = 15 minutes) finishes quickly.
            with page.expect_download(timeout=12000) as dl_info:
                try:
                    page.goto(url, timeout=10000, wait_until="commit")
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


def _extract_pdf_pdfplumber(pdf_bytes: bytes, max_pages: int = 120) -> str:
    """Try extracting text from a PDF using pdfplumber (works for text-based PDFs).

    Page cap is 120 — a balance between coverage and worker memory:
    - Hydro-Québec's `electricity-rates.pdf` is 160 pages but Rate D/DP/DM/
      DT/Flex D detail (pages 12–31) and Off-Grid Systems chapter (pages
      119–127) both fit. Pages 128+ are appendices/glossary we don't need.
    - Manitoba Hydro / BC Hydro rate books follow similar structures.

    Memory protection:
    - For PDFs over ~4MB we skip `extract_tables()` (which is the most
      memory-heavy pdfplumber operation; on a 6.5MB Newfoundland Power
      rate book it ballooned to >1.5GB and OOM-killed the worker).
      `extract_text()` alone usually captures all rate values when the
      PDF uses real text (not images) for tables.
    - We call `page.flush_cache()` after each page to release the
      parsed objects and chars that pdfplumber otherwise retains for
      the lifetime of the `with` block.
    - PDFs over 18MB are refused outright — they're virtually always
      multi-document PDF bundles or scanned books that pdfplumber will
      either OOM on or produce useless OCR-grade output for.
    """
    if len(pdf_bytes) > 18_000_000:
        log.warning(
            f"  PDF too large ({len(pdf_bytes)/1e6:.1f}MB), skipping pdfplumber"
        )
        return ""
    skip_tables = len(pdf_bytes) > 4_000_000
    if skip_tables:
        log.info(
            f"  PDF is {len(pdf_bytes)/1e6:.1f}MB — extracting text only "
            f"(table extraction is OOM risk above 4MB)"
        )
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
            try:
                page_text = page.extract_text() or ""
                if not skip_tables:
                    tables = page.extract_tables()
                    for table in tables:
                        for row in table:
                            cells = [str(c).strip() if c else "" for c in row]
                            page_text += "\n" + " | ".join(cells)
                text_parts.append(page_text)
            finally:
                # Release per-page cached chars/lines/edges so memory
                # doesn't grow linearly with page count.
                try:
                    page.flush_cache()
                    page.get_textmap.cache_clear()
                except Exception:
                    pass
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
_LLM_PROMPT_VERSION = "v10"


# Opt-in, for one transition run only: also read entries written under the
# pre-2026-09 key (tier name without model id). Those may replay another
# model's output, which is exactly what the model-aware key prevents.
_LLM_CACHE_LEGACY_READ = os.environ.get("LLM_CACHE_LEGACY_READ", "0") == "1"


def _cache_model_id(tier: str) -> str:
    """Concrete model ids behind a cache tier. Part of the cache key so an
    env-only model swap never silently replays another model's output."""
    return {
        "haiku": HAIKU_MODEL,
        "sonnet": SONNET_MODEL,
        "opus": OPUS_MODEL,
        "vision": SONNET_MODEL,
        "twopass": f"{SONNET_MODEL}+{OPUS_MODEL}",
    }.get(tier, tier)


def _llm_cache_path(content_hash: str, tier: str, *, legacy: bool = False) -> str:
    # Include YYYY-MM so "current column" answers don't replay forever after
    # a future dated column becomes current (prompt review item 2 / §5.1).
    month_key = date.today().strftime("%Y-%m")
    material = (
        f"{content_hash}:{tier}:{_LLM_PROMPT_VERSION}:{month_key}"
        if legacy
        else f"{content_hash}:{tier}:{_cache_model_id(tier)}:{_LLM_PROMPT_VERSION}:{month_key}"
    )
    return os.path.join(LLM_CACHE_DIR, f"{hashlib.sha256(material.encode()).hexdigest()}.json")


def _get_llm_cache(content_hash: str, model: str) -> list[dict] | None:
    """Return cached extraction result for a content hash + tier/model, or None."""
    paths = [_llm_cache_path(content_hash, model)]
    if _LLM_CACHE_LEGACY_READ:
        paths.append(_llm_cache_path(content_hash, model, legacy=True))
    for cache_path in paths:
        if os.path.isfile(cache_path):
            try:
                with open(cache_path, "r") as f:
                    return json.load(f)
            except (OSError, json.JSONDecodeError):
                return None
    return None


def _set_llm_cache(content_hash: str, model: str, tariffs: list[dict]) -> None:
    """Store LLM extraction result keyed by content hash + tier + model id."""
    os.makedirs(LLM_CACHE_DIR, exist_ok=True)
    cache_path = _llm_cache_path(content_hash, model)
    try:
        with open(cache_path, "w") as f:
            json.dump(tariffs, f)
    except OSError as e:
        log.warning(f"    Failed to write PDF cache: {e}")


_PDF_MODIFIED: dict[str, tuple[int, int]] = {}
_PDF_MODDATE_RE = re.compile(rb"/ModDate\s*\(\s*D:(\d{4})(\d{2})")
_PDF_XMP_MOD_RE = re.compile(rb"<xmp:ModifyDate>\s*(\d{4})-(\d{2})|xmp:ModifyDate=\"(\d{4})-(\d{2})")


def pdf_modified_vintage(pdf_bytes: bytes) -> tuple[int, int] | None:
    """(year, month) of the PDF's last-modified metadata (Info /ModDate or
    XMP ModifyDate); newest wins. None when absent or implausible."""
    found = []
    head = pdf_bytes or b""
    for m in _PDF_MODDATE_RE.finditer(head):
        found.append((int(m.group(1)), int(m.group(2))))
    for m in _PDF_XMP_MOD_RE.finditer(head):
        y, mo = (m.group(1), m.group(2)) if m.group(1) else (m.group(3), m.group(4))
        found.append((int(y), int(mo)))
    found = [v for v in found if 1995 <= v[0] <= date.today().year + 1 and 1 <= v[1] <= 12]
    return max(found) if found else None


def fetch_pdf_text(url: str) -> str:
    """Download a PDF and extract text. Tries pdfplumber first, falls back to OCR.
    OCR results are cached by content hash so the same PDF is never re-OCR'd."""
    pdf_bytes = _download_pdf(url)
    if not pdf_bytes:
        return ""
    md = pdf_modified_vintage(pdf_bytes)
    if md:
        _PDF_MODIFIED[url] = md  # R21 fix 6: document age evidence

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
        # Many smaller utilities publish rates ONLY as a PDF rate book on
        # their own site — a blanket -30 buried those behind off-domain
        # HTML aggregators. Keep the penalty for off-domain or unnamed
        # PDFs (often regulator filings) but go light when the utility's
        # own domain serves a rate-named PDF.
        on_domain = utility_domain and is_same_domain(url, f"https://{utility_domain}")
        rate_named = any(kw in path for kw in ["rate", "tariff", "pricing"])
        if on_domain and rate_named:
            score -= 5
        else:
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

    # Prefer utility-published regulatory tariff books (NS Power style
    # tariff-book-YYYY.pdf). Newer year wins; older books demoted.
    book_year = _tariff_book_year(url)
    if book_year:
        score += 40 + max(0, book_year - 2020)  # e.g. 2026 >> 2024
        if utility_domain and is_same_domain(url, f"https://{utility_domain}"):
            score += 20

    url_domain = urlparse(url).netloc.replace("www.", "")
    if any(url_domain == d or url_domain.endswith(f".{d}") for d in THIRD_PARTY_DOMAINS):
        return -999  # Hard block — never use aggregator/comparison sites

    # Hard-block utility-published "average rate comparison" brochures.
    # These print one representative price per region (not real tariffs);
    # the LLM cleanly extracts that single number as a phantom FLAT
    # tariff, e.g. HQ's `comparison-electricity-prices-2019.pdf` produced
    # a fake "Residential Service $0.07299/kWh". See quality_cleanup.py
    # category A and the contamination cleanup commit.
    if _is_comparison_brochure_url(url, title):
        return -999

    # Demote (but don't outright block) customer-facing explainer pages.
    # Sometimes a utility links its actual rate page from a "how-your-bill-
    # works" page, so we may still want to crawl in for links — just give
    # it lower priority than dedicated rate URLs.
    if _is_explainer_url(url, title):
        score -= 30

    return score


# Patterns marking a URL as a comparison/marketing brochure rather than
# a real tariff schedule. Match in both URL paths and titles.
_COMPARISON_BROCHURE_PATTERNS = (
    r"comparison[-_]of[-_]electricity",
    r"comparison[-_]electricity[-_]prices",
    r"comparing[-_]electricity",
    r"electricity[-_]rate[s]?[-_]comparison",
    r"rate[-_]comparison",
    r"average[-_]electricity[-_]rates",
    # Annual "comparison-electricity-prices-YYYY.pdf" naming used by
    # several utilities (HQ, Manitoba Hydro, etc).
    r"electricity[-_]prices[-_]\d{4}",
)
_COMPARISON_BROCHURE_RE = re.compile(
    "|".join(_COMPARISON_BROCHURE_PATTERNS), re.IGNORECASE
)


def _is_comparison_brochure_url(url: str, title: str = "") -> bool:
    """True if this URL/title looks like a marketing rate-comparison
    brochure. These docs are not authoritative tariff schedules."""
    try:
        url_decoded = unquote(url)
    except Exception:
        url_decoded = url
    return bool(
        _COMPARISON_BROCHURE_RE.search(url_decoded)
        or (title and _COMPARISON_BROCHURE_RE.search(title))
    )


# Patterns that mark a URL as a customer-facing explainer/conceptual
# page rather than a rate publication. These pages talk about how
# billing works, what a kilowatt-hour is, etc. The LLM (especially
# screenshot vision) tends to hallucinate "tariffs" from this kind of
# conceptual prose — for example, "Detail de la consommation",
# "Tarification pour une résidence", "Domestic Rate Schedule -
# Bill consumption" — none of which are real published rates.
_EXPLAINER_URL_PATTERNS = (
    r"understanding[-_]",
    r"how[-_]it[-_]works",
    r"how[-_]is[-_]",
    r"how[-_]your[-_]bill",
    r"comment[-_](?:fonctionne|calculer|lire)",
    r"learn[-_]about",
    r"about[-_]your[-_](?:bill|rates?|electricity)",
    r"glossary",
    r"explanation",
    r"explained\.",
    r"demystify",
    r"comprendre",
    r"deposit[-_]payment",
    r"estimate[-_]electric",
    r"power[-_]demand",
    # Utility-owned news/blog/storytelling subdomains and paths.
    # Added 2026-05-12 after Chunk 1 caught the pipeline extracting
    # "Proposed Rate Structure" tariffs from SCE's news blog at
    # energized.edison.com/stories/sce-proposal-would-lower... These
    # are news articles announcing proposed rate cases, NOT actual
    # tariff publications. Generalising the pattern: any path
    # containing /stories/, /news/, /press-release(s)/, /blog/, or
    # /newsroom/ is a news article rather than a tariff filing.
    r"//energized\.edison\.com/",
    r"/stories/",
    r"/newsroom/",
    r"/news[-_]?releases?/",
    r"/press[-_]?releases?/",
    r"/media[-_]?releases?/",
    r"/blog/",
    r"/blogs/",
)
_EXPLAINER_URL_RE = re.compile("|".join(_EXPLAINER_URL_PATTERNS), re.IGNORECASE)


def _is_explainer_url(url: str, title: str = "") -> bool:
    """True if the URL/title looks like a customer-education page
    rather than a rate publication. Used to demote scoring and to
    skip screenshot-vision fallback (which is the main hallucination
    surface on these pages)."""
    try:
        url_decoded = unquote(url)
    except Exception:
        url_decoded = url
    return bool(
        _EXPLAINER_URL_RE.search(url_decoded)
        or (title and _EXPLAINER_URL_RE.search(title))
    )


# State public-utility-commission "rate case / docket / proceeding"
# subdomains and paths. These contain regulatory filings (testimony,
# orders, staff memos) — *not* customer-facing tariff publications.
# When a utility's main rate page links out to these (e.g. ConEd's site
# linking to ru.dps.ny.gov rate-case summaries for *all* NY utilities),
# the L2 crawler used to follow them all and hang for 30s each on the
# Playwright download timeout.
#
# CAUTION: We don't block whole regulator domains because some utilities
# (especially Canadian ones — BCUC, OEB, NEB, Régie de l'énergie) use
# the regulator as their *primary* tariff publisher. Match on filing-
# specific path/subdomain patterns instead.
_REGULATOR_FILING_PATTERNS = (
    r"://[a-z]{2,4}\.dps\.ny\.gov",
    r"/rate[-_]case[-_]",
    r"/rate[-_]case/",
    r"[-_]rate[-_]case[-_]filing",
    r"[-_]rate[-_]case[-_]summary",
    r"[-_]rate[-_]case[-_]staff",
    r"[-_]filing[-_]memo",
    r"[-_]staff[-_]broadcast[-_]memo",
    r"/dockets?/",
    r"/proceedings?/",
    r"docket[-_]?no[-_\.]?\d",
    # State PUC e-doc portals / rate-case accession numbers (PGE UE 394).
    r"://edocs\.puc\.",
    r"://[a-z0-9.-]*puc\.state\.",
    r"/efdocs/",
    r"\bue[\s_-]?\d{2,4}\b",
)
_REGULATOR_FILING_RE = re.compile(
    "|".join(_REGULATOR_FILING_PATTERNS), re.IGNORECASE
)


def _is_regulator_filing_url(url: str, title: str = "") -> bool:
    """True if the URL looks like a regulator rate-case docket or
    filing memo rather than a tariff publication."""
    try:
        url_decoded = unquote(url)
    except Exception:
        url_decoded = url
    return bool(
        _REGULATOR_FILING_RE.search(url_decoded)
        or (title and _REGULATOR_FILING_RE.search(title))
    )


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


# Canonical regulatory tariff-book PDFs for utilities whose marketing
# "rates" hubs historically diverted discovery away from the authoritative
# schedule. Prefer these over residential marketing pages when refreshing.
_TARIFF_BOOK_YEAR_RE = re.compile(
    r"tariff[-_]?book[-_]?(\d{4})\.pdf", re.IGNORECASE
)

# Nova Scotia Power — Tariffs May 2026 (NSUARB / NSEB approved).
NS_POWER_TARIFF_BOOK_2026_URL = (
    "https://www.nspower.ca/docs/default-source/regulatory/tariff-book-2026.pdf"
)

# Known preferred extraction sources by utility-name substring (lower).
PREFERRED_RATE_PAGE_URLS: dict[str, str] = {
    "nova scotia power": NS_POWER_TARIFF_BOOK_2026_URL,
}


def _tariff_book_year(url: str) -> int | None:
    """Return YYYY from ``.../tariff-book-2026.pdf`` paths, else None."""
    try:
        path = unquote(urlparse(url).path)
    except Exception:
        path = url
    m = _TARIFF_BOOK_YEAR_RE.search(path)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def preferred_rate_page_url(utility_name: str | None) -> str | None:
    """Return a hard-coded preferred rate/tariff-book URL when known."""
    if not utility_name:
        return None
    key = utility_name.strip().lower()
    for needle, url in PREFERRED_RATE_PAGE_URLS.items():
        if needle in key:
            return url
    return None


def _looks_like_stale_ns_source(url: str) -> bool:
    """True when a known NS Power URL should yield to the May 2026 book."""
    if not url:
        return True
    u = url.lower()
    year = _tariff_book_year(url)
    if year is not None and year < 2026:
        return True
    # Explicit Mar-2025 book still live in prod (utility 1739).
    if "tariff-book-20250326" in u or "tariff-book-2025" in u:
        return True
    # Marketing / explainer hubs — Phase 2 *can* find the PDF, but refreshes
    # that stop at HTML often miss the Board’s Order column + riders.
    marketing_bits = (
        "/your-home/residential-rates",
        "/about-us/electricity/rates-tariffs",
        "/about-us/producing/rate-options",
        "rate-options",
        "understanding",
    )
    if any(b in u for b in marketing_bits) and "tariff-book-2026" not in u:
        return True
    return False


def resolve_preferred_rate_page(
    utility_name: str,
    existing_url: str = "",
    override_url: str = "",
) -> tuple[str, list[str]]:
    """Pick primary rate URL, injecting known tariff books when appropriate.

    Returns ``(primary_url, alt_urls_to_prepend)``. Does not override an
    explicit ``rate_page_url_override`` set by an operator.
    """
    preferred = preferred_rate_page_url(utility_name)
    if override_url:
        alts: list[str] = []
        if preferred and preferred.rstrip("/") != override_url.split("?")[0].rstrip("/"):
            alts.append(preferred)
        return override_url, alts
    if not preferred:
        return existing_url or "", []
    if not existing_url or _looks_like_stale_ns_source(existing_url):
        alts = [existing_url] if existing_url and existing_url != preferred else []
        return preferred, alts
    # Existing looks fine (already a current tariff book) — keep it, but
    # still offer the preferred URL as an alternate.
    if preferred != existing_url:
        return existing_url, [preferred]
    return existing_url, []


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


_R20_NON_RESIDENTIAL_DOC_RE = re.compile(
    r"purpa|cogenerat|qualifying[-_ ]?facilit|avoided[-_ ]?cost|buy[-_ ]?back|\bpep[-_ ]", re.I,
)


def order_links_newest_first(links, *, today: date | None = None):
    """Stable re-order of (url, text) crawl links: current-dated first,
    old-dated and cogeneration/PURPA sheets last; others keep their order."""
    today = today or date.today()

    def _key(ut):
        u = ut[0]
        if _R20_NON_RESIDENTIAL_DOC_RE.search(urlparse(u).path):
            return (2, 0)
        v = url_document_vintage(u, today=today)
        if v is None:
            return (1, 0)
        if v[0] >= today.year - 1:
            return (0, -(v[0] * 12 + v[1]))
        if v[0] <= today.year - 2:
            return (2, -(v[0] * 12 + v[1]))
        return (1, 0)

    return sorted(links, key=_key)


@llm_cost.with_phase("phase1")
def phase1_find_rate_page(utility_name: str, state: str, website_url: str | None) -> tuple[str, int, list[str]]:
    """Search for the utility's rate page. Returns (best_url, num_results, alt_urls)."""
    utility_domain = urlparse(website_url).netloc if website_url else None
    clean_name = _clean_utility_name(utility_name)
    total_searches = 0

    # Known regulatory tariff books beat marketing hubs — return early when
    # we have a hard-coded current book (NS Power May 2026, etc.).
    preferred = preferred_rate_page_url(utility_name)
    if preferred:
        log.info(f"  Phase 1: Using preferred tariff book URL: {preferred[:90]}")
        return preferred, 1, []

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

    # R20: a supply / price-to-compare sheet (PSE&G BGS PTC) has no delivery
    # charges. Prefer the utility's own tariff / rates page when one is a
    # candidate; the sheet stays as an alternate.
    if is_supply_sheet_url(scored[0][1]["url"]):
        top_dom = urlparse(scored[0][1]["url"]).netloc.replace("www.", "")
        for i, (sc, r) in enumerate(scored[1:], start=1):
            u = r["url"]
            if (sc > 0 and urlparse(u).netloc.replace("www.", "") == top_dom
                    and not is_supply_sheet_url(u)
                    and re.search(r"tariff|rate", u, re.I)
                    and not re.search(r"news|press|blog", u, re.I)):
                log.info(f"  Phase 1: Top hit is a supply price sheet — preferring {u[:80]}")
                top = scored.pop(0)
                pick = scored.pop(i - 1)
                scored = [(max(pick[0], top[0]), pick[1]), top, *scored]
                break

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
        # 403 = bot-blocked, usually fine in a browser (SRP srpnet.com scored
        # 18 and was dropped once the aggregators above it were blocked).
        if (status == 403 and best_score >= 10) or (status == 0 and best_score >= 50):
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

        for score, r in scored[1:4]:
            alt_url = r["url"]
            _, _, alt_status = fetch_page(alt_url)
            if alt_status == 200:
                best_url = alt_url
                break
            # Bot-blocked official pages (Pedernales mypec.com → 403 to
            # httpx, fine in a browser) used to end the run with "No rate
            # page found". Phase 2 already retries 403s in Playwright.
            if alt_status == 403 and score >= 10:
                html_js, _title = fetch_page_js(alt_url)
                if html_js == FETCH_JS_DOWNLOAD_SENTINEL or (html_js and len(html_js.strip()) > 200):
                    log.info(f"  Phase 1: Playwright reached alternate {alt_url[:60]}")
                    with _js_rendered_lock:
                        _js_rendered_domains.add(urlparse(alt_url).netloc.replace("www.", ""))
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
    links = _drop_translation_duplicates(links)
    links.sort(key=lambda x: _link_priority(x[0], x[1]))
    return links


# Language-switcher prefixes seen on US/CA utility sites (PG&E links every
# page in ~12 languages). Only used to collapse translation sets, never to
# drop a lone link, so state-code paths like /ar/ are safe.
_LANG_PREFIX_RE = re.compile(
    r"^/(en|es|zh|zh-hans|zh-hant|zh-cn|zh-tw|ko|tl|ja|hmn|ar|fa|hi|km|vi|ru|pa|"
    r"hy|th|pt|so|am|uk|pl|ht|ne|ur|bn|lo|my|mn|ta|te|gu|pa)(?=/)",
    re.IGNORECASE,
)


def _drop_translation_duplicates(links: list[tuple[str, str]]) -> list[tuple[str, str]]:
    """Collapse the same page linked under several language prefixes.

    When a page is linked in English (``/en/...`` or unprefixed) AND under at
    least two other language prefixes, keep only the English link. A run on
    PG&E spent 11 of its 20 LLM calls on /zh/, /ko/, /tl/ ... copies of one
    SmartRate page and never reached the base residential plans.
    """
    groups: dict[tuple[str, str], list[tuple[str, str, str]]] = {}
    for url, text in links:
        parsed = urlparse(url)
        m = _LANG_PREFIX_RE.match(parsed.path)
        lang = m.group(1).lower() if m else ""
        rest = parsed.path[m.end():] if m else parsed.path
        groups.setdefault((parsed.netloc.lower(), rest), []).append((lang, url, text))
    drop: set[str] = set()
    for members in groups.values():
        english = [u for lang, u, _ in members if lang in ("", "en")]
        foreign = [u for lang, u, _ in members if lang not in ("", "en")]
        if english and len(foreign) >= 2:
            drop.update(foreign)
    if drop:
        log.info(f"  Phase 2: skipped {len(drop)} translated duplicate link(s)")
    return [(u, t) for u, t in links if u not in drop]


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
    # Capture hrefs BEFORE _extract_text mutates the soup — rider one-hop
    # (NSP FAM) needs the raw links; plain text has none (R8).
    page_links = _collect_page_hrefs(content, url)
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
        links=page_links,
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
    page_links = _collect_page_hrefs(html, url)
    text = _extract_text(soup)
    if len(text.strip()) < 50:
        return None
    return RatePage(
        url=url,
        title=title,
        page_type="html",
        content=text,
        content_hash=hashlib.sha256(html.encode()).hexdigest(),
        links=page_links,
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


@llm_cost.with_phase("phase2")
def phase2_discover_tariff_pages(rate_page_url: str) -> list[RatePage]:
    """Crawl the main rate page and one level of sub-pages to find
    residential and small-commercial rate detail pages."""
    # Never crawl a hard-blocked aggregator/data domain. These can slip in
    # as an OpenEI seed URL or a search alternate; crawling them wastes
    # fetches (e.g. eia.gov's decades-deep archive of state rate PDFs) and
    # can only ever yield mis-attributed multi-utility data.
    if _is_third_party_domain(rate_page_url):
        log.info(f"  Phase 2: Skipping third-party/aggregator domain: {rate_page_url[:70]}")
        return []

    log.info(f"  Phase 2: Crawling {rate_page_url}")

    # Direct PDF URL — skip HTML crawl (httpx returns raw bytes that
    # BeautifulSoup cannot parse; e.g. SCE residential fact-sheet PDF).
    if rate_page_url.lower().split("?")[0].endswith(".pdf"):
        pdf_page = _fetch_as_pdf_via_download(rate_page_url)
        if pdf_page:
            log.info(
                f"  Phase 2: Direct PDF URL, extracted {len(pdf_page.content)} chars"
            )
            return [pdf_page]
        log.warning("  Phase 2: Failed to fetch direct PDF URL")
        return []

    # Level 0: fetch the main rates page
    domain = urlparse(rate_page_url).netloc
    bare_domain = domain.replace("www.", "")
    # Always bind before the Playwright/httpx branch. The browser path has
    # no Content-Type header; referencing an unbound `ctype` below raises
    # UnboundLocalError (campaign overnight failures on JS-rendered domains
    # that Phase 1 already marked in `_js_rendered_domains`).
    content, ctype, status = "", "", 0

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

    if status == 200 and "pdf" in (ctype or "").lower():
        pdf_page = _fetch_as_pdf_via_download(rate_page_url)
        if pdf_page:
            log.info(
                f"  Phase 2: Response is PDF ({ctype}), extracted "
                f"{len(pdf_page.content)} chars"
            )
            return [pdf_page]
        log.warning("  Phase 2: PDF content-type but extraction failed")
        return []

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
                # Last resort for hard-blocked pages: the interactive
                # browser agent (handles cookie walls, JS challenges).
                return _try_browser_agent_fallback(rate_page_url)
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

        # Playwright still couldn't surface content (interactive widget,
        # accordion, rate calculator). Try the interactive browser agent
        # before giving up on the page.
        if len(text.strip()) < 200:
            agent_pages = _try_browser_agent_fallback(rate_page_url)
            if agent_pages:
                return agent_pages

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

    # Drop links that point at hard-blocked aggregator/data domains so we
    # never follow a rate page out to e.g. eia.gov's archive.
    before_tp = len(level1_links)
    level1_links = [(u, t) for u, t in level1_links if not _is_third_party_domain(u)]
    if len(level1_links) < before_tp:
        log.info(
            f"  Phase 2: Dropped {before_tp - len(level1_links)} link(s) to "
            f"third-party/aggregator domains"
        )

    log.info(f"  Phase 2: Found {len(level1_links)} relevant links on main page")

    # Drop regulator rate-case / docket / proceeding links — they're filings
    # about *changes* to rates, not the rates themselves, and Playwright
    # PDF-download timeouts on these (30s each) trapped ConEd's run.
    before_filing = len(level1_links)
    level1_links = [
        (u, t) for u, t in level1_links if not _is_regulator_filing_url(u, t)
    ]
    if len(level1_links) < before_filing:
        log.info(
            f"  Phase 2: Dropped {before_filing - len(level1_links)} regulator "
            f"rate-case / docket links"
        )

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

    # R20: newest-document rule for the crawl queue. Links dated this year or
    # last (URL date, e.g. "effective-20261001") go first; links dated two or
    # more years back and cogeneration / PURPA buy-back sheets go last, so the
    # level-1 cap keeps the current tariff book (PSE&G listed 15 PURPA sheets
    # ahead of its current tariff).
    level1_links = order_links_newest_first(level1_links)

    MAX_LEVEL1 = 15
    # Rate-book hubs: a rate-themed page linking to many PDFs is a tariff
    # library (each schedule its own PDF). Capping those at 15 silently
    # dropped half the catalog — raise the budget for that shape of page.
    if page_is_rate_themed:
        pdf_link_count = sum(
            1 for u, _ in level1_links if u.lower().split("?")[0].endswith(".pdf")
        )
        if pdf_link_count >= 10:
            MAX_LEVEL1 = 30
            log.info(
                f"  Phase 2: Rate-book hub detected ({pdf_link_count} PDF links) "
                f"— raising level-1 cap to {MAX_LEVEL1}"
            )
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

    # Drop regulator rate-case / docket / proceeding links from L2 too.
    # The L2 enumeration is where ConEd's run got trapped — its main page
    # linked to ru.dps.ny.gov which itself linked to PDF rate-case
    # summaries for every NY utility (NYSEG, RG&E, Central Hudson, etc.).
    before_filing_l2 = len(level2_candidates)
    level2_candidates = [
        (u, t) for u, t in level2_candidates
        if not _is_regulator_filing_url(u, t) and not _is_third_party_domain(u)
    ]
    if len(level2_candidates) < before_filing_l2:
        log.info(
            f"  Phase 2: Dropped {before_filing_l2 - len(level2_candidates)} "
            f"regulator rate-case / aggregator links from L2"
        )

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
    """Select the most rate-relevant portion(s) of a document.

    For long documents, find ALL residential/general-service rate
    sections (not just the first one) and concatenate windows around
    each within the char budget. This handles consolidated rate-book
    PDFs — like Hydro-Québec's `electricity-rates.pdf` (333K chars,
    160 pages) — where Rate D, Rate DP, Rate DM, Rate DT, Rate Flex D
    each occupy only a few pages and are spread across the first
    ~12% of the document. Anchoring on the first match alone
    truncates well before the second rate.

    Long-document budget bump: very large PDFs (>100K chars) get up to
    3x the requested budget. The marginal LLM cost is worth it for
    consolidated rate books.
    """
    text = _compress_whitespace(text)
    if len(text) <= max_chars:
        return text

    # Adaptive budget: triple the budget for long docs (consolidated
    # rate books). Threshold lowered to 50k chars because pdfplumber
    # text extraction is denser per page than pdfminer — a 50-page
    # rate-book PDF often produces only 90–110k chars (where pdfminer
    # would produce 200k+). Caller's `max_chars` defines the floor.
    effective_max = max_chars * 3 if len(text) > 50_000 else max_chars

    # Strategy 1: gather windows around EVERY residential/domestic rate
    # section that has actual rate values nearby (skip TOC entries and
    # wholesale sections).
    _per_unit = re.compile(
        r"per\s+(?:month|kWh|kW)|\$/\s*(?:kWh|kW|day|month)|¢/\s*kWh",
        re.IGNORECASE,
    )
    anchors: list[int] = []
    for m in _RESIDENTIAL_SIGNAL.finditer(text):
        nearby = text[m.start():min(len(text), m.start() + 2500)]
        if _RATE_AMOUNT_RE.search(nearby) and _per_unit.search(nearby):
            anchors.append(m.start())

    if anchors:
        first_anchor = anchors[0]
        last_anchor = anchors[-1]
        anchor_span = last_anchor - first_anchor

        # A consolidated rate-book PDF (HQ, Manitoba Hydro, BC Hydro, etc.)
        # has its residential sections spread across many tens of
        # thousands of characters — Rate D / DP / DM / DT / Flex D for
        # Hydro-Québec span offsets 25k–70k. Tiny per-anchor windows
        # leave gaps. When anchors are spread out (>10k between first
        # and last) OR there's plenty of budget vs. single-anchor
        # window size, use ONE big contiguous window instead.
        if anchor_span > 10_000 or effective_max > 30_000:
            # Pad before the first anchor so the section heading and
            # any preceding rate detail (commonly a lead-in section
            # like "Section 2 – Rate D") makes it into the window.
            lead_in = 10_000
            start = max(0, first_anchor - lead_in)
            end = min(len(text), start + effective_max)
            selected = text[start:end]
            if start > 0:
                selected = "[...document truncated...]\n" + selected
            if end < len(text):
                selected += "\n[...document truncated...]"
            return selected

        # Otherwise: small/medium docs with tightly-clustered anchors.
        # Build a window around each anchor and merge overlaps.
        windows = [(max(0, a - 1000), min(len(text), a + 6000)) for a in anchors]
        windows.sort()
        merged: list[list[int]] = [list(windows[0])]
        for s_, e_ in windows[1:]:
            if s_ <= merged[-1][1] + 500:
                merged[-1][1] = max(merged[-1][1], e_)
            else:
                merged.append([s_, e_])

        sep = "\n\n[...document truncated...]\n\n"
        used = 0
        parts: list[str] = []
        for s_, e_ in merged:
            chunk = text[s_:e_]
            sep_cost = len(sep) if parts else 0
            if used + sep_cost + len(chunk) > effective_max:
                remaining = effective_max - used - sep_cost
                if remaining > 500:
                    parts.append(chunk[:remaining])
                break
            parts.append(chunk)
            used += sep_cost + len(chunk)

        if parts:
            prefix = "[...document truncated...]\n" if merged[0][0] > 0 else ""
            suffix = "\n[...document truncated...]" if merged[-1][1] < len(text) else ""
            return prefix + sep.join(parts) + suffix

    # Strategy 2 (fallback): sliding window maximizing rate signal density.
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

    window_blocks = effective_max // block
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
    end_char = start_char + effective_max
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

# Shared by every Phase 3 prompt (text, two-pass, vision). No braces —
# TODAY is injected into user messages only so the cached system prompt
# stays byte-identical across days. Substituted into outer prompts via
# .replace before any str.format that needs other placeholders.
_STRUCTURED_RULES = """1. SOURCE ONLY. Every number, clock time, day type and date must come from the content you were given. Never copy a number from the examples (examples are format illustrations ONLY). If something is not shown, leave that field null (or leave the row out) and name the gap in missing_fields. Set needs_review=true ONLY for Mysa-critical gaps: missing/uncertain ENERGY price, missing TOU clock / day_type / season dates, unresolved per-kWh riders, or a validation conflict. Do NOT set needs_review for a missing effective_date alone (leave effective_date ""). A blank field is better than a guessed one.
   Allowed derivations (these are NOT guessing — do these when the source supports them):
   a) adding printed numbers to build the full per-kWh price (rule 4);
   b) "all other hours" / "all remaining hours" = exactly the hours not covered by the stated windows;
   c) a month range means whole months: "June–September" → 6/1–9/30, "Dec–Apr" → 12/1–4/30;
   d) converting a 12-hour clock to 24-hour: "7 a.m.–11 a.m." → 07:00–11:00; an end time of "9:59 p.m." → 22:00;
   e) relative seasonal riders (Rate #1.1S / #1.2DS): when the rider says energy = another schedule's rate ± a printed seasonal premium/credit, and that base schedule's ENERGY is printed in the SAME document (even in another section), add the printed base ± the printed seasonal amount to emit all-in ENERGY per season. Pulling a printed base from the same official document is an allowed calculation, not a guess.
   Nothing else. Never get hours from a label like "On-Peak" alone or dates from a word like "Summer" alone.

2. RESIDENTIAL ONLY, decided by who the rate serves: homes, dwellings, domestic customers, farm-and-home, or residential single-phase service. A schedule named "General Service", "Farm & Home", "Single-Phase Service", or similar counts when its text says it applies to residences / dwellings / domestic use. Skip rates that serve only businesses, industry, lighting, irrigation, wholesale, or government departments / government buildings (e.g. "Government Diesel", "Government Departments") — those are non-residential even when the name contains "Domestic".

3. WHICH PRICE. The user message states TODAY's date. Use the price in effect on that date. When a table has several dated columns, use the column with the latest effective date on or before TODAY. Skip proposed, pending, cancelled or withdrawn rates. If a schedule shows BOTH a temporary/interim price billed today AND a full TOU/seasonal structure that starts on a future date, extract BOTH: (i) today's billed price as one tariff with its current effective_date, and (ii) the coming TOU/seasonal structure as a separate tariff with that future effective_date (it must not be treated as current until that date). Schedules closed to new customers: still extract them and set closed_to_new=true.

4. FULL PRICE (residential ENERGY). The ENERGY rate is the total per-kWh price every customer on this plan pays:
   IN: the base energy charge plus every mandatory per-kWh charge on the same bill — fuel/purchased-power/PCA, riders and adjustments (FAM, DSM/efficiency, storm, cost recovery, interim), per-kWh delivery/transmission/distribution/regulatory charges (including Ontario delivery and regulatory adders), and — when the utility page is delivery-only — the published standard-offer / default-supply / POLR / RPP commodity price so ENERGY is a full billable ¢/kWh.
   OUT (never add these into ENERGY): optional programs a customer must sign up for (SmartRate, peak-time rebates / PTR, Green Power, Green Future, EV programs); charges or credits that apply only to some kWh or some customers (community solar, net-metering / export / surplus credits); event or critical-peak adders; taxes, franchise fees, percent-of-bill items and multipliers (loss factors); fixed monthly charges (they stay FIXED — and optional fixed add-ons like Green Power $5/month stay as separate optional FIXED rows, never the standard plan's only fixed charge).
   Match each rider to the row it applies to. If a rider differs by season, TOU period or tier, add the matching amount to each row.
   For every amount you add, ALSO emit it as its own ADJUSTMENT row with included_in_energy=true. Emit excluded or unsure items as ADJUSTMENT rows with included_in_energy=false. Never leave included_in_energy empty on a per-kWh ADJUSTMENT row. Put "all-in" in the ENERGY tier_label when riders were folded in.
   If the plan says riders apply but their amounts are not in the content, keep the base ENERGY, list the riders in riders_referenced_not_shown, and set needs_review=true.
   energy_scope: "bundled" when supply+delivery are one price; "delivery_plus_default_supply" when you combined delivery with a published default/standard-offer supply price (label the breakdown in tier_label / description); "delivery_only" or "supply_only" when the other half is not shown (also set needs_review=true).

5. UNITS. Copy each number and unit exactly as printed ("9.56 ¢/kWh", "0.0956 $/kWh", "2.5 mills/kWh"); the pipeline converts units. When adding amounts in different units, convert them to the base ENERGY unit and print the conversion in tier_label (e.g. "all-in: 9.56 ¢ + 0.0021 $ = 9.77 ¢").
   Read numbers carefully: "(0.250)" in parentheses means -0.250; a decimal comma is a decimal point ("6,509 ¢" = 6.509); a trailing footnote digit or symbol (9.56¹, 9.56*) is not part of the number.

6. TOU CLOCKS AND DAY TYPES. Every ENERGY row of a TOU-family tariff needs period_start_time, period_end_time and day_type taken from the source. One row per continuous window. Weekends or holidays priced differently get their own rows, with their own windows if stated ("weekends 07:00–23:00 off-peak, 23:00–07:00 ultra-low") or 00:00–00:00 if one price applies all day. If you emit holiday rows in one season, emit them in every season that has TOU windows. For every season and day type, the windows cover 24 hours exactly once.

7. SEASONS. Every ENERGY row of a seasonal-family tariff needs season_start/end month/day. Seasons cover the year exactly once. If one season is two separate date ranges, emit that season's rows once for each range. If the source says seasons follow billing months rather than calendar dates, still use the month range and note "billing months" in season.

8. TIERS. tier_min_kwh/tier_max_kwh exactly as printed; the next tier starts where the previous one ends (0–500, 500–1000, 1000+ with an open top). Put what the limit is measured over (per month, per day, per billing period) in tier_basis when stated. Tiered plus TOU: each row carries both the tier bounds and the clock window (rate_type tou_tiered).

9. One tariff per product: keep all seasons, periods, day types and tiers of a schedule in one tariff, named as printed. A plan priced differently by zone or area gets one tariff per zone, with the zone in the name. Residential demand (¢/kW or $/kW) stays DEMAND. Net-metering / export credits are ADJUSTMENT with included_in_energy=false, never ENERGY.

10. RELATIVE SEASONAL RIDERS (e.g. Rate #1.1S / #1.2DS): when energy equals another schedule's rate ± a seasonal premium/credit, emit all-in ENERGY per season with season_start/end month/day — never leave a season as ADJUSTMENT-only. If the base schedule's ENERGY is printed elsewhere in the SAME document, use that printed base (allowed derivation e). Do NOT invent hours or season dates from labels alone (MYSA fields: period_start_time, period_end_time, day_type, season_start/end)."""

# Static half of the main extraction prompt — sent once as the Anthropic
# system message (prompt-cached). Dynamic per-call fields live in
# EXTRACTION_USER_PROMPT only so rules are not duplicated in the user message.
EXTRACTION_SYSTEM_PROMPT = """Extract ONLY residential electricity tariffs from this page (who the rate serves — see shared rules).

INCLUDE: schedules that serve homes / dwellings / domestic / farm-and-home / residential single-phase customers (even if named "General Service" or "Farm & Home" when the text says so)
SKIP / IGNORE: rates that serve only businesses, industry, lighting, irrigation, fleet, street lighting, transmission, wholesale, interruptible, standby, government departments / government buildings (e.g. NL Rate 1.2G Government Diesel)

ATTRIBUTION CHECK (applies to every page you extract from):
- A target utility is provided below. Only return rates that the page explicitly attributes to the target utility or one of its named operating subsidiaries.
- PROVINCE-WIDE REGULATED PRICES: If the page is published by a provincial or state energy regulator (e.g. Ontario Energy Board / OEB) and lists Regulated Price Plan (RPP) or other commodity prices that apply province-wide / jurisdiction-wide to local distribution companies in that jurisdiction, treat those rates as attributable to the target utility when the target operates in that same province or state. Extract them. Do NOT invent utility-specific delivery or distribution charges that are not printed on the page.
- If the page is a comparison / aggregator page that lists rates for multiple utilities side-by-side, only include rows unambiguously labeled for the target utility. If you cannot tell, return an empty tariffs array.
- If a section, table, or rate sheet is labeled with a different utility's name (a neighboring IOU, a sister utility in another state, a competitive REP/marketer), DO NOT include those rates.
- If the document covers several states or provinces, extract only rates for the target utility's state/province given in the user message.
- If the page contains no rates clearly attributable to the target utility (and the regulator exception above does not apply), return an empty tariffs array — do NOT guess. Set empty_reason appropriately.

For each tariff, provide fields matching the store_tariffs tool schema. Key points:
- customer_class: always "residential"
- rate_type: flat (one price) | tiered (price by kWh block) | seasonal (price by season) | seasonal_tiered (blocks that differ by season) | tou (price by time of day, same all year) | seasonal_tou (time of day AND season) | tou_tiered (time of day AND blocks) | demand / demand_tou (has a $/kW charge) | complex (anything else). Pick the most specific that fits.
- effective_date: the date the extracted prices take effect ("effective for service on and after …"), YYYY-MM-DD. Not the issued, filed, approved or printed date. Leave "" if no full date is shown.
- confidence: 0.9+ if every value is clearly readable; 0.5–0.8 if some cell was hard to read. If you would be guessing, leave the value out and list it in missing_fields instead.
- needs_review, missing_fields, riders_referenced_not_shown, energy_scope, closed_to_new, energy_includes_riders as defined in the tool schema.
- components: use printed units; never invent clocks or season dates.

Rules:
- Include ALL tiers, periods, and seasonal variations as separate component entries
{structured_rules}
- Use exact numbers from the page — do NOT estimate or round
- ONLY include tariffs that have actual numeric rate values ($/kWh, cents/kWh, mills/kWh, $/month, $/kW etc.)
- Skip table-of-contents entries, index listings, or schedule names that lack rate values
- If the document has a table of contents AND detailed rate schedules, extract from the DETAILED sections
- If no relevant residential tariffs with rate values on this page, call the tool with an empty tariffs array and set empty_reason
- If a minimum monthly charge equals the basic/customer charge for the same amp tier, emit one fixed row — not both fixed and minimum duplicates
- RELATIVE SEASONAL RIDERS (e.g. Rate #1.1S): When energy charges equal another schedule's energy rate ± a seasonal premium/credit, emit one ENERGY component per season at the all-in rate (base ± adjustment). Fill season_start/end month/day when months are stated. Do NOT leave a season as ADJUSTMENT-only.
- RIDER-ONLY DOCUMENTS: If the content only lists riders/adjustments (no base plan), return one tariff per rider, named as printed, customer_class "residential", each per-kWh amount as an ADJUSTMENT row. If the rider lists separate amounts per rate class, use the residential amount only.

EXAMPLES (format illustrations ONLY — never copy their numbers into your output; take every figure from the provided page content):

Example 1 — Simple flat rate:
Input: "Residential Service (RS): Customer charge $15.00/month. Energy charge 10.000 cents/kWh. Effective 2026-03-01."
Output: one tariff named "Residential Service", code "RS", class "residential", type "flat", confidence 0.95, effective_date 2026-03-01, fixed 15.00 "$/month", energy 10.000 "¢/kWh".

Example 2 — Tiered rate:
Input: "Schedule R: Basic charge $10.00/mo. First 500 kWh: $0.100/kWh. Over 500 kWh: $0.120/kWh."
Output: one tariff type "tiered", fixed 10.00 "$/month", energy tier 1 (0–500 at 0.100 "$/kWh"), energy tier 2 (tier_min_kwh 500, open top, 0.120 "$/kWh").

Example 3 — Seasonal TOU with complement windows:
Input: "Rate TOU-D (every day): Summer (Jun 1–Sep 30) On-Peak 2pm–8pm $0.300/kWh, Off-Peak all other hours $0.100/kWh. Winter (Oct 1–May 31) On-Peak 2pm–8pm $0.200/kWh, Off-Peak all other hours $0.080/kWh. Service $12/mo."
Output: seasonal_tou; fixed 12 "$/month"; 4 ENERGY rows with season dates 6/1–9/30 or 10/1–5/31, day_type "all": On-Peak 14:00–20:00 and Off-Peak 20:00–14:00 each season.

Example 4 — Relative seasonal rider (synthetic):
Input: "Rate Domestic Seasonal: Energy Charges from Rate Domestic (10.000¢/kWh) apply, subject to Winter Premium Dec–Apr +1.000¢/kWh; Non-Winter Credit May–Nov (0.500)¢/kWh."
Output: seasonal; TWO all-in ENERGY rows: Winter 11.000 "¢/kWh" (12/1–4/30); Non-Winter 9.500 "¢/kWh" (5/1–11/30).

Example 5 — Dated columns + riders (TODAY = 2026-10-07):
Input: "Residential Service: Customer $15.00 (eff 2026-03-01) / $16.00 (eff 2027-01-01). Energy 10.000 ¢/kWh (eff 2026-03-01) / 10.500 (eff 2027-01-01). Fuel Rider 0.500 ¢/kWh and Efficiency Rider 0.250 ¢/kWh apply to all kWh. Optional Green Power: add 1.000 ¢/kWh."
Output: flat; fixed 15.00 "$/month"; ENERGY 10.750 "¢/kWh" tier_label "all-in: base 10.000 + fuel 0.500 + efficiency 0.250"; ADJUSTMENT Fuel 0.500 included_in_energy=true; ADJUSTMENT Efficiency 0.250 included_in_energy=true; ADJUSTMENT Green Power 1.000 included_in_energy=false; effective_date 2026-03-01; energy_includes_riders=true. (2027 column not yet in effect; Green Power is optional.)

Example 6 — Interim today + future TOU (TODAY = 2026-10-07):
Input: "Domestic TOU (code 80): Customer $15.00. INTERIM ENERGY CHARGE while meters unavailable: 10.000 ¢/kWh all hours (in effect today). ENERGY CHARGE TOU starts 2026-11-01: Non-winter Apr 1–Oct 31 all hours 10.000 ¢; Winter Nov 1–Mar 31 weekdays on-peak 7–11am/5–9pm 20.000 ¢, off-peak other weekday hours 10.000 ¢; winter weekends and holidays all-day off-peak. Fuel rider 0.500 ¢ applies to all."
Output: TWO tariffs. (1) flat code 80 interim: fixed 15.00; ENERGY 10.500 all-in; ADJUSTMENT fuel 0.500 included_in_energy=true; effective_date = today's in-effect date. (2) seasonal_tou code 80 with effective_date 2026-11-01: Non-winter all-hours 10.500; Winter weekday on-peak 07:00–11:00 and 17:00–21:00 at 20.500; Winter weekday off-peak complements at 10.500; Winter weekend and holiday 00:00–00:00 at 10.500 — each with fuel folded and ADJUSTMENT rows. Do not flatten the future TOU into a single interim row as the only extract.

Example 7 — Weekday TOU with weekend/holiday all-day price:
Input: "Plan TOU-R: Weekdays: On-peak 4pm–9pm 30.0¢/kWh; Off-peak all other hours 10.0¢/kWh. Weekends and holidays: 10.0¢/kWh all day. Year-round."
Output: tou; ENERGY On-peak 16:00–21:00 weekday 30.0; Off-peak 21:00–16:00 weekday 10.0; Off-peak 00:00–00:00 weekend 10.0; Off-peak 00:00–00:00 holiday 10.0.

Use the store_tariffs tool to return your results.""".replace(
    "{structured_rules}", _STRUCTURED_RULES
)

# Per-call user message only (no rules). Kept separate from
# EXTRACTION_SYSTEM_PROMPT so Anthropic prompt caching is not defeated by
# re-sending the static rules in every user message. {today} is filled at
# call time so the cached system prompt stays byte-identical across days.
EXTRACTION_USER_PROMPT = """TODAY: {today}
TARGET UTILITY: {utility_name} ({state})
Page URL: {url}
Page title: {title}

Content:
{content}"""

def _today_iso() -> str:
    """Calendar date stamped into extraction user messages (not the system prompt)."""
    return date.today().isoformat()


def format_extraction_user(
    *,
    utility_name: str,
    state: str,
    url: str = "",
    title: str = "",
    content: str = "",
    today: str | None = None,
) -> str:
    """Build the dynamic user message for main-path extraction tool calls."""
    return EXTRACTION_USER_PROMPT.format(
        today=today or _today_iso(),
        utility_name=utility_name,
        state=state,
        url=url,
        title=title,
        content=content,
    )


# Full template kept for tests / dry-run inspection (system + user joined).
# Uses a placeholder TODAY so .format still works in tests.
EXTRACTION_PROMPT = (
    EXTRACTION_SYSTEM_PROMPT
    + "\n\n"
    + EXTRACTION_USER_PROMPT.replace("{today}", "YYYY-MM-DD")
)

# Slim system prompt for Haiku (tier-1): shared rules + three short format
# examples. Complex pages already skip Haiku, so it never needs the long
# interim / relative-seasonal / NSP-style examples.
HAIKU_EXTRACTION_SYSTEM_PROMPT = """Extract ONLY residential electricity tariffs (who the rate serves — homes/dwellings/domestic/farm-and-home; keep "General Service" / "Farm & Home" when the text says they apply to residences).

ATTRIBUTION: only rates for the TARGET UTILITY in the user message (or province-wide regulator commodity prices for its jurisdiction). Otherwise empty tariffs + empty_reason.

Use the store_tariffs tool. Fields: customer_class always "residential"; rate_type as defined in the tool; effective_date = effective-for-service date; confidence 0.9+ if clear, else 0.5–0.8 or leave gaps in missing_fields.

Rules:
{structured_rules}

Examples (format ONLY — never copy these numbers):
1) Flat: Customer $15/mo + Energy 10.000 ¢/kWh → fixed 15 $/month; energy 10.000 ¢/kWh.
2) Tiered: First 500 kWh $0.100, over 500 $0.120 → two ENERGY rows with tier bounds.
3) Weekday TOU: On-peak 4–9pm 30¢, off-peak other hours 10¢; weekends/holidays 10¢ all day → weekday 16:00–21:00 + 21:00–16:00; weekend and holiday 00:00–00:00.

Use the store_tariffs tool.""".replace("{structured_rules}", _STRUCTURED_RULES)


_GENERIC_NAME_TAIL_WORDS = {
    "tariff", "service", "services", "rate", "rates", "schedule", "plan",
    "residential", "domestic", "standard", "basic", "electric", "electricity",
    "option", "the", "of", "for", "and", "customers", "customer", "pricing",
}


def _energy_values(t: ExtractedTariff) -> set[float]:
    out: set[float] = set()
    for c in t.components or []:
        if isinstance(c, dict) and c.get("component_type") == "energy":
            try:
                out.add(round(float(c.get("rate_value")), 4))
            except (TypeError, ValueError):
                continue
    return out


def _distinct_variant(tail: str, a: ExtractedTariff, b: ExtractedTariff) -> bool:
    """True when two prefix-related names are separate products.

    A tail made only of generic words ("Domestic Service" vs "Domestic
    Service Tariff") is the same plan. Otherwise, when both sides carry
    energy prices and neither price set contains the other, they are
    distinct products (HQ "Rate D" vs "Rate D T").
    """
    words = [w for w in re.split(r"[^a-z0-9]+", tail.lower()) if w]
    if not words or all(w in _GENERIC_NAME_TAIL_WORDS for w in words):
        return False
    ea, eb = _energy_values(a), _energy_values(b)
    if not ea or not eb:
        return False
    return not (ea <= eb or eb <= ea)


def _tariff_has_energy(t: ExtractedTariff) -> bool:
    return bool(_energy_values(t))


def _tariff_keep_score(t: ExtractedTariff) -> tuple:
    """Richer keeper for prefix/full-bill duplicate collapse.

    Never prefer a rider-only extract over one with ENERGY (BC Hydro R5).
    Among ENERGY plans, prefer higher all-in energy (full-bill sibling) and
    more folded/stacking riders over a bare component count.
    """
    energy_vals = _energy_values(t)
    has_energy = 1 if energy_vals else 0
    max_energy = max(energy_vals) if energy_vals else 0.0
    folded = sum(
        1
        for c in (t.components or [])
        if isinstance(c, dict)
        and str(c.get("component_type") or "").lower() == "adjustment"
        and c.get("included_in_energy")
    )
    stacking = 0
    for c in (t.components or []):
        if not isinstance(c, dict):
            continue
        # Inline check — _is_universal_stacking_rider is defined later.
        if str(c.get("component_type") or "").lower() != "adjustment":
            continue
        stacking += 1
    return (has_energy, max_energy, folded, stacking, len(t.components or []))


def _merge_prefix_duplicates(tariffs: list[ExtractedTariff]) -> list[ExtractedTariff]:
    """Merge tariffs where one name is a prefix of another (same customer class).

    E.g. "Domestic Service" (1 adjustment) and "Domestic Service Tariff"
    (3 full components) are the same plan at different detail levels —
    keep the richer one. A tariff with no ENERGY never absorbs one that has
    ENERGY (BC Hydro "flat rate with TOD" rider-only vs Residential Flat Rate).
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
                    if _tariff_keep_score(t_i) >= _tariff_keep_score(t_j):
                        absorbed.add(j)
                    else:
                        absorbed.add(i)
                    continue
                # Token-aware prefix check. The naive substring prefix test
                # incorrectly merged products like "Rate D" vs "Rate DP",
                # because "rate dp".startswith("rate d") is True. Require
                # the shorter name to end on a token boundary in the longer
                # name — i.e. the longer name's next character must be a
                # space — so "Rate D" / "Rate DP" no longer collide while
                # "Rate D" / "Rate D Service" still merges as expected.
                shorter, longer = (norm_i, norm_j) if len(norm_i) <= len(norm_j) else (norm_j, norm_i)
                is_prefix = (
                    longer.startswith(shorter)
                    and (len(longer) == len(shorter) or longer[len(shorter)] == " ")
                )
                if not is_prefix:
                    continue
                if len(longer) > len(shorter) and _distinct_variant(
                    longer[len(shorter):], t_i, t_j
                ):
                    # e.g. "Rate D" vs "Rate D T" (HQ dual-energy) or
                    # "Rate G" vs "Rate G Short-Term Contract": a separate
                    # product with its own prices, not a detail-level dupe.
                    continue
                # Hard rule: never let a no-ENERGY tariff absorb one with ENERGY.
                ei, ej = _tariff_has_energy(t_i), _tariff_has_energy(t_j)
                if ei and not ej:
                    absorbed.add(j)
                    log.info(
                        f"    Merged duplicate: '{t_j.name}' (no ENERGY) "
                        f"absorbed by '{t_i.name}' (has ENERGY)"
                    )
                    continue
                if ej and not ei:
                    absorbed.add(i)
                    log.info(
                        f"    Merged duplicate: '{t_i.name}' (no ENERGY) "
                        f"absorbed by '{t_j.name}' (has ENERGY)"
                    )
                    break
                if _tariff_keep_score(t_i) >= _tariff_keep_score(t_j):
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


def _rate_type_family(rt: str) -> str:
    r = str(rt or "").strip().lower()
    if r in ("tou", "tou_tiered", "seasonal_tou", "demand_tou"):
        return "tou"
    if r in ("seasonal", "seasonal_tiered"):
        return "seasonal"
    if r in ("tiered",):
        return "tiered"
    return r or "flat"


def _name_token_set(name: str) -> set[str]:
    stop = {
        "rate", "tariff", "schedule", "service", "the", "and", "for", "of",
        "no", "number", "num", "residential", "domestic", "electric",
        "energy", "plan", "option",
    }
    return {
        w for w in re.split(r"[^a-z0-9]+", str(name or "").lower())
        if w and w not in stop and not w.isdigit()
    }


# Max gap between a base-only sibling and its full-bill counterpart.
# Values may still be in ¢/kWh (pre-Phase-4) or $/kWh (post-normalize).
_FULL_BILL_MAX_ENERGY_DELTA_DOLLARS = 0.08  # $0.08/kWh ≈ 8¢
_FULL_BILL_MAX_ENERGY_DELTA_CENTS = 8.0


def _full_bill_energy_delta_cap(values: list[float] | set[float]) -> float:
    """Pick ¢ vs $ cap from magnitude (¢ rates are typically > 1.0)."""
    if not values:
        return _FULL_BILL_MAX_ENERGY_DELTA_DOLLARS
    return (
        _FULL_BILL_MAX_ENERGY_DELTA_CENTS
        if max(values) > 1.0
        else _FULL_BILL_MAX_ENERGY_DELTA_DOLLARS
    )


def _full_bill_product_match(a: ExtractedTariff, b: ExtractedTariff) -> bool:
    """True when two extracts are the same product at different detail.

    Never merge different names/codes (Pedernales flat → Community Solar,
    PGE EV → TOU Portfolio). Shared parent codes like Schedule 7 are not
    enough when option names differ — require identical normalized names
    (codes may still match). Anonymous base-only TOU beside a coded full
    bill is handled separately by ``_is_base_only_duplicate_of``.
    """
    if str(a.customer_class or "").lower() != str(b.customer_class or "").lower():
        return False
    if _rate_type_family(a.rate_type) != _rate_type_family(b.rate_type):
        return False
    na, nb = _normalize_tariff_name(a.name), _normalize_tariff_name(b.name)
    if not na or not nb or na != nb:
        return False
    ca = str(a.code or "").strip().lower()
    cb = str(b.code or "").strip().lower()
    if ca and cb and ca != cb:
        return False
    return True


_GENERIC_TOU_NAME_RE = re.compile(
    r"^(?:time[\s-]*of[\s-]*use(?:\s+rate)?|tou(?:\s+rate)?)$",
    re.IGNORECASE,
)


def _is_base_only_duplicate_of(thin: ExtractedTariff, full: ExtractedTariff) -> bool:
    """True when ``thin`` is an anonymous/base-only TOU of the fuller plan.

    Pedernales web 'Time-of-Use Rate' at 4.35¢ beside coded 500.2.5 all-in.
    Requires energy compatibility and that thin lacks a schedule code (or
    has a generic TOU name) while full carries delivery/TCOS riders.
    """
    if str(thin.customer_class or "").lower() != str(full.customer_class or "").lower():
        return False
    if _rate_type_family(thin.rate_type) != _rate_type_family(full.rate_type):
        return False
    if not _base_energy_compatible_for_full_bill(thin, full):
        return False
    # Only anonymous/generic "Time-of-Use Rate" extracts — never absorb an
    # EV option into a differently named TOU Portfolio (or vice versa).
    if not _GENERIC_TOU_NAME_RE.match(str(thin.name or "").strip()):
        return False
    thin_code = str(thin.code or "").strip()
    full_code = str(full.code or "").strip()
    if thin_code:
        return False  # coded schedules are real products
    if not full_code and not any(
        isinstance(c, dict)
        and str(c.get("component_type") or "").lower() == "adjustment"
        and _is_energy_unit(c.get("unit"))
        for c in (full.components or [])
    ):
        return False
    # Prefer dropping the thin side only when full looks fuller.
    return _tariff_keep_score(full) > _tariff_keep_score(thin)


def _base_energy_compatible_for_full_bill(thin: ExtractedTariff, full: ExtractedTariff) -> bool:
    """Thin plan's ENERGY values look like the full plan before riders."""
    ea = sorted(_energy_values(thin))
    eb = sorted(_energy_values(full))
    if not ea or not eb:
        return False
    # Cap how far the *corresponding* prices can diverge (guards 15¢
    # domestic vs 100¢ government). Use max-to-max / pairwise — not
    # max(full)-min(thin), which false-rejects wide TOU spreads.
    cap = _full_bill_energy_delta_cap([*ea, *eb])
    if max(eb) - max(ea) > cap + 1e-9:
        return False
    # Same number of ENERGY price points (TOU periods / tiers), each thin
    # value ≤ corresponding full value (full-bill = base + riders).
    if len(ea) == len(eb):
        if not all(b - a <= cap + 1e-9 for a, b in zip(ea, eb)):
            return False
        return all(a <= b + 1e-9 for a, b in zip(ea, eb)) and max(eb) > max(ea) + 1e-6
    # Or thin is a single base that appears among full's values / below max.
    if len(ea) == 1:
        return ea[0] <= max(eb) + 1e-9 and max(eb) > ea[0] + 1e-6 and (
            max(eb) - ea[0] <= cap + 1e-9
        )
    return False


def _collapse_full_bill_siblings(tariffs: list[ExtractedTariff]) -> list[ExtractedTariff]:
    """Prefer the full-bill sibling when duplicates differ only by missing riders.

    Pedernales web-page TOU (base-only 4.35¢) beside tariff 500.2.5 (8.67¢
    all-in) collapses to the fuller extract. Never absorbs across ENERGY
    presence (handled by prefix merge); here both sides have ENERGY.
    """
    if len(tariffs) <= 1:
        return tariffs
    groups: dict[str, list[ExtractedTariff]] = {}
    for t in tariffs:
        groups.setdefault(str(t.customer_class or ""), []).append(t)

    merged: list[ExtractedTariff] = []
    for _cc, group in groups.items():
        absorbed: set[int] = set()
        for i, t_i in enumerate(group):
            if i in absorbed or not _tariff_has_energy(t_i):
                continue
            for j, t_j in enumerate(group):
                if j <= i or j in absorbed or not _tariff_has_energy(t_j):
                    continue
                product = _full_bill_product_match(t_i, t_j)
                base_dup = (
                    _is_base_only_duplicate_of(t_i, t_j)
                    or _is_base_only_duplicate_of(t_j, t_i)
                )
                if not product and not base_dup:
                    continue
                # Require one side to look base-only relative to the other.
                i_fits_j = _base_energy_compatible_for_full_bill(t_i, t_j)
                j_fits_i = _base_energy_compatible_for_full_bill(t_j, t_i)
                if not i_fits_j and not j_fits_i:
                    # Also collapse when ENERGY sets match but one has more
                    # stacking/folded riders (same printed base, fuller bill).
                    if _energy_values(t_i) != _energy_values(t_j):
                        continue
                if _tariff_keep_score(t_i) >= _tariff_keep_score(t_j):
                    if i_fits_j and not j_fits_i:
                        # t_i is thinner — keep t_j
                        absorbed.add(i)
                        log.info(
                            f"    Full-bill sibling: '{t_i.name}' absorbed by "
                            f"richer '{t_j.name}'"
                        )
                        break
                    absorbed.add(j)
                    log.info(
                        f"    Full-bill sibling: '{t_j.name}' absorbed by "
                        f"richer '{t_i.name}'"
                    )
                else:
                    if j_fits_i and not i_fits_j:
                        absorbed.add(j)
                        continue
                    absorbed.add(i)
                    log.info(
                        f"    Full-bill sibling: '{t_i.name}' absorbed by "
                        f"richer '{t_j.name}'"
                    )
                    break
        for k, t in enumerate(group):
            if k not in absorbed:
                merged.append(t)

    if len(merged) < len(tariffs):
        log.info(
            f"    Full-bill dedup: {len(tariffs)} -> {len(merged)} tariffs "
            f"({len(tariffs) - len(merged)} base-only siblings dropped)"
        )
    return merged


def drop_superseded_same_family_adjustments(components: list[dict]) -> list[dict]:
    """Drop older duplicate values of the same charge family.

    Pedernales sample bills sometimes keep stale and current TCOS as two
    'tiers' (ADJUSTMENT or ENERGY from vision). Prefer a non-superseded
    label; otherwise keep the single remaining / highest value for that
    family (unseasoned energy-unit only).
    """
    if not components:
        return components

    # family -> list of (index, comp) for ADJUSTMENT and charge-like ENERGY
    by_fam: dict[str, list[tuple[int, dict]]] = {}
    for i, c in enumerate(components):
        if not isinstance(c, dict):
            continue
        ctype = str(c.get("component_type") or "").lower()
        if ctype not in ("adjustment", "energy"):
            continue
        if not _is_energy_unit(c.get("unit")):
            continue
        if _season_key(c.get("season")):
            continue
        # ENERGY rows only enter when their label names a rider/charge family
        # (TCOS, delivery, …) — never collapse genuine TOU/tier ENERGY.
        # PGE Sch 7 tiers label "all-in: … transmission + distribution …" —
        # those must NOT be treated as duplicate transmission charges (R8).
        fam = _rider_family_key(c)
        if ctype == "energy":
            if (
                c.get("tier_min_kwh") is not None
                or c.get("tier_max_kwh") is not None
                or c.get("period_start_time")
                or c.get("period_end_time")
                or c.get("period_label")
            ):
                continue  # structured ENERGY — keep every tier/period
            label = " ".join(
                str(c.get(k) or "") for k in ("tier_label", "period_label")
            )
            if not label.strip() or fam.startswith("val:"):
                continue
            # "all-in" ENERGY that merely *mentions* transmission in a
            # breakdown is still ENERGY, not a T&D ADJUSTMENT twin.
            if _ALL_IN_LABEL_RE.search(label):
                continue
            if fam not in {
                "tcos", "delivery", "transmission", "distribution",
                "fam", "dcrr", "scrr", "pca", "bac", "cost_recovery",
            }:
                continue
        by_fam.setdefault(fam, []).append((i, c))

    drop: set[int] = set()
    for fam, rows in by_fam.items():
        if len(rows) < 2:
            continue
        values = set()
        for _i, c in rows:
            try:
                values.add(round(float(c.get("rate_value") or 0), 6))
            except (TypeError, ValueError):
                values.add(0.0)
        if len(values) < 2:
            continue  # exact dupes left to dedupe_rate_components
        remaining = []
        for i, c in rows:
            blob = _adjustment_label_blob(c)
            if _SUPERSEDED_CHARGE_LABEL_RE.search(blob):
                drop.add(i)
            else:
                remaining.append((i, c))
        if len(remaining) <= 1:
            continue

        def _val(pair: tuple[int, dict]) -> float:
            try:
                return float(pair[1].get("rate_value") or 0)
            except (TypeError, ValueError):
                return 0.0

        remaining.sort(key=_val, reverse=True)
        for i, _c in remaining[1:]:
            drop.add(i)

    if not drop:
        return components
    return [c for i, c in enumerate(components) if i not in drop]


def drop_superseded_flat_energy_vintages(
    components: list[dict],
    *,
    rate_type: str = "",
) -> list[dict]:
    """Drop stale all-in ENERGY vintages on flat sample-bill extracts.

    Pedernales vision sometimes emits two unseasoned ENERGY values (old
    TCOS-inclusive and new) with no tier/TOU structure. Keep the higher.
    """
    rt = str(rate_type or "").strip().lower()
    if rt and rt not in ("flat",):
        return components
    energy_idxs: list[int] = []
    for i, c in enumerate(components or []):
        if not isinstance(c, dict):
            continue
        if str(c.get("component_type") or "").lower() != "energy":
            continue
        if not _is_energy_unit(c.get("unit")):
            continue
        if _season_key(c.get("season")):
            return components
        if c.get("period_start_time") or c.get("period_end_time") or c.get("period_label"):
            return components
        if c.get("tier_min_kwh") is not None or c.get("tier_max_kwh") is not None:
            return components
        energy_idxs.append(i)
    if len(energy_idxs) < 2:
        return components
    vals: list[tuple[int, float]] = []
    for i in energy_idxs:
        try:
            vals.append((i, float(components[i].get("rate_value") or 0)))
        except (TypeError, ValueError):
            return components
    # Only collapse when values differ by a rider-sized gap (≤8¢ / $0.08),
    # not genuine multi-tier flats that somehow lost bounds.
    vals.sort(key=lambda x: x[1], reverse=True)
    cap = _full_bill_energy_delta_cap([v for _i, v in vals])
    if vals[0][1] - vals[-1][1] > cap + 1e-9:
        return components
    if vals[0][1] - vals[-1][1] < 1e-9:
        return components
    keep_i = vals[0][0]
    drop = {i for i, _v in vals[1:]}
    # Prefer dropping rows labeled prior/old when present.
    for i in energy_idxs:
        blob = _adjustment_label_blob(components[i])
        if _SUPERSEDED_CHARGE_LABEL_RE.search(blob) and i != keep_i:
            drop.add(i)
    return [c for i, c in enumerate(components) if i not in drop]


# Optional *add-on programmes* (drop the whole extract / strip the charge).
# Bare "Optional" on a rate schedule name (NL 1.2DS, NSP TOD) is NOT enough.
_OPTIONAL_PROGRAMME_NAME_RE = re.compile(
    r"\b(?:green\s+future|green\s+power|green\s+energy|net[\s-]*meter|"
    r"option\s+[ivx]+\b|peak[\s-]*time\s+rebate|\bptr\b|smartrate|"
    r"smart[\s-]*rate|avenir\s+vert|renewable\s+choice|"
    r"community[\s-]*solar|tou\s+portfolio|portfolio\s+manager)\b",
    re.IGNORECASE,
)

# Charge-level optional add-ons (NSP $5 Green Power inside Domestic).
_OPTIONAL_PROGRAMME_CHARGE_RE = re.compile(
    r"\b(?:green\s+future|green\s+power|green\s+energy|community[\s-]*solar|"
    r"peak[\s-]*time\s+rebate|\bptr\b|smartrate|smart[\s-]*rate|"
    r"net[\s-]*meter|avenir\s+vert|renewable\s+choice|"
    r"optional\s+(?:green|credit|rebate|program|rider|add[\s-]*on))\b",
    re.IGNORECASE,
)


_DISTINCT_SCHEDULE_CODE_RE = re.compile(
    # NL 1.1S / 1.2DS, PG&E E-TOU-C, NSP 05 — not bare "7" / "Schedule 7".
    r"^(?:[A-Z]{1,4}[-_]?)?(?:\d+\.\d+[A-Z]{0,3}|\d{2,}[A-Z]{0,3}|[A-Z]+-\d+[A-Z]{0,3})$"
    r"|^(?:[A-Z]{1,6}[-_][A-Z0-9]+)$",
    re.IGNORECASE,
)


def _has_distinct_schedule_code(t: ExtractedTariff) -> bool:
    """True when the extract carries its own rate-schedule code (not a parent)."""
    code = str(t.code or "").strip()
    if not code:
        return False
    # Strip a leading "Schedule " / "Rate " / "Rate No." wrapper.
    code = re.sub(r"^(?:schedule|rate(?:\s*no\.?)?)\s*", "", code, flags=re.I).strip()
    if re.fullmatch(r"\d{1,3}", code):
        return False  # bare Sch 7 / rate 7 — shared parent
    return bool(_DISTINCT_SCHEDULE_CODE_RE.match(code.replace(" ", "")))


def _is_optional_program_tariff(t: ExtractedTariff) -> bool:
    """True when the whole extract is an optional add-on, not a base plan.

    Keep standalone optional *rate schedules* (NL 1.2DS '- Optional', NSP
    Domestic TOD '(Optional)') — they have their own ENERGY / clocks / code.
    Drop only named add-on programmes (Green Power, Community Solar, PTR,
    net-metering Option I, TOU Portfolio, …).
    """
    name = str(t.name or "")
    # Explicit programme / portfolio names always drop, even with ENERGY.
    if _OPTIONAL_PROGRAMME_NAME_RE.search(name):
        return True
    # A schedule with its own rate code is never an add-on programme (NL 1.1S
    # / 1.2DS), even before base-price salvage injects ENERGY.
    if _has_distinct_schedule_code(t):
        return False
    blob = f"{name} {t.description or ''} {t.code or ''}"
    if not _OPTIONAL_OR_SCOPED_RIDER_RE.search(blob):
        return False
    # Bare "(Optional)" / "- Optional" on a schedule with ENERGY → keep.
    if _tariff_has_energy(t):
        return False
    # No ENERGY and only generic "optional" wording → treat as add-on.
    return True


def strip_optional_program_components(t: ExtractedTariff) -> int:
    """Keep optional programmes out of the full billable ENERGY/FIXED price.

    Match on **component labels** (and programme-charge names like Green
    Power even without the word 'optional'). Applies to ALL charge types
    (FIXED, ADJUSTMENT $/month, ENERGY) — NSP's $5 Green Power is an
    ADJUSTMENT, not FIXED. Per-kWh optional credits are kept as
    non-included ADJUSTMENT for audit. Whole-tariff optional extracts are
    dropped by ``_is_optional_program_tariff``.
    """
    comps = list(t.components or [])
    if not comps:
        return 0
    kept: list[dict] = []
    removed = 0
    for c in comps:
        if not isinstance(c, dict):
            kept.append(c)
            continue
        ctype = str(c.get("component_type") or "").lower()
        label = " ".join(str(c.get(k) or "") for k in ("tier_label", "period_label", "season"))
        is_programme = bool(_OPTIONAL_PROGRAMME_CHARGE_RE.search(label))
        is_optional_wording = bool(_OPTIONAL_OR_SCOPED_RIDER_RE.search(label))
        if is_programme:
            if ctype == "adjustment" and _is_energy_unit(c.get("unit")):
                row = dict(c)
                row["included_in_energy"] = False
                kept.append(row)
                continue
            if ctype in ("fixed", "adjustment", "minimum"):
                # NSP $5 Green Power is $/month ADJUSTMENT — drop all non-kWh
                # optional programme charges (R8).
                removed += 1
                continue
            if ctype == "energy":
                row = dict(c)
                row["component_type"] = "adjustment"
                row["included_in_energy"] = False
                kept.append(row)
                removed += 1
                continue
        elif is_optional_wording and ctype == "adjustment" and _is_energy_unit(c.get("unit")):
            row = dict(c)
            row["included_in_energy"] = False
            kept.append(row)
            continue
        kept.append(c)
    t.components = kept
    return removed


def drop_optional_program_tariffs(
    tariffs: list[ExtractedTariff],
) -> list[ExtractedTariff]:
    """Remove optional add-on extracts before full-bill sibling merge (R7)."""
    kept: list[ExtractedTariff] = []
    for t in tariffs:
        if _is_optional_program_tariff(t):
            log.info(f"    Dropped optional programme extract '{t.name}' (pre-merge)")
            continue
        kept.append(t)
    return kept


_SAMPLE_BILL_HINT_RE = re.compile(
    r"\b(?:sample\s*bill|example\s*bill|bill\s*sample|illustrative\s*bill|"
    r"your\s*bill|monthly\s*bill\s*example)\b",
    re.IGNORECASE,
)


def _looks_like_sample_bill_flat(t: ExtractedTariff) -> bool:
    """Heuristic: flat extract that came from a sample bill, not a schedule."""
    if str(t.rate_type or "").lower() not in ("flat", ""):
        return False
    blob = f"{t.name or ''} {t.description or ''} {t.source_url or ''}"
    if _SAMPLE_BILL_HINT_RE.search(blob):
        return True
    missing = " ".join(str(m) for m in (t.missing_fields or []))
    # Official schedule missing + single all-in ENERGY often = sample bill.
    if re.search(r"official\s+schedule|schedule\s+name", missing, re.I):
        return True
    # Short generic residential name with no code, while a coded/official
    # sibling exists, is handled by the drop helper comparing keep scores.
    if not str(t.code or "").strip() and re.search(
        r"farm\s*/\s*ranch|farm\s+and\s+ranch", str(t.name or ""), re.I
    ):
        # Pedernales sample-bill title variant.
        return True
    return False


def drop_sample_bill_duplicate_flats(
    tariffs: list[ExtractedTariff],
) -> list[ExtractedTariff]:
    """Drop sample-bill flats when an official flat of the same class exists.

    Pedernales: sample-bill 10.438¢ beside official Flat Base Power 10.913¢.
    Keep the higher-scoring / coded official plan; never drop the only flat.
    """
    flats = [
        t for t in tariffs
        if str(t.customer_class or "").lower() == "residential"
        and str(t.rate_type or "").lower() == "flat"
        and _tariff_has_energy(t)
        and not _is_optional_program_tariff(t)
    ]
    if len(flats) < 2:
        return tariffs
    official = [
        t for t in flats
        if not _looks_like_sample_bill_flat(t)
        or str(t.code or "").strip()
    ]
    if not official:
        # Keep the highest keep-score flat as the official stand-in.
        official = [max(flats, key=_tariff_keep_score)]
    drop_ids = set()
    for t in flats:
        if t in official:
            continue
        if not _looks_like_sample_bill_flat(t):
            # Two non-sample flats with different prices — keep both unless
            # one is clearly a lower sample-like all-in of the other.
            continue
        # Drop sample bill when any official flat survives.
        drop_ids.add(id(t))
        log.info(
            f"    Dropped sample-bill flat '{t.name}' beside official "
            f"'{official[0].name}'"
        )
    if not drop_ids:
        return tariffs
    return [t for t in tariffs if id(t) not in drop_ids]


# User message only — full rules/examples come from the cached system prompt.
PAGE_SCREENSHOT_EXTRACTION_PROMPT_BASE = """TODAY: {today}
TARGET UTILITY: {utility_name} ({state})
These are screenshot tile(s) of a web page. Extract residential tariffs following the system rules (SOURCE ONLY, FULL PRICE, residential-by-who-served).

The page may display rates as images, charts, infographics, or styled tables.
Ignore sample bills, savings claims, "as low as" / "starting at" figures and charts without labelled values.
ATTRIBUTION: only the target utility (or province-wide regulator commodity prices for its jurisdiction). Set empty_reason if nothing attributable is visible.
If you cannot see clear residential electricity rates, return an empty tariffs array and set empty_reason.

Use the store_tariffs tool."""


# Max viewport height for the full-page screenshot. Most rate pages fit in
# one or two screens; clamping avoids Vision token blowups on very long pages.
MAX_SCREENSHOT_HEIGHT_PX = 6000
# Claude vision shrinks the long edge (~1568 px). Tall full-page JPEGs become
# unreadably narrow; send ~1400 px tiles instead.
SCREENSHOT_TILE_HEIGHT_PX = 1400


def _tile_screenshot_jpeg(img_bytes: bytes, tile_height: int = SCREENSHOT_TILE_HEIGHT_PX) -> list[bytes]:
    """Split a tall JPEG into top-to-bottom tiles for vision readability."""
    try:
        from PIL import Image
        import io
    except ImportError:
        return [img_bytes]
    try:
        img = Image.open(io.BytesIO(img_bytes))
    except Exception:
        return [img_bytes]
    w, h = img.size
    if h <= tile_height + 200:
        return [img_bytes]
    tiles: list[bytes] = []
    for top in range(0, h, tile_height):
        box = (0, top, w, min(h, top + tile_height))
        tile = img.crop(box)
        buf = io.BytesIO()
        tile.convert("RGB").save(buf, format="JPEG", quality=80)
        tiles.append(buf.getvalue())
    return tiles or [img_bytes]


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


def _extract_page_screenshot_vision(
    url: str,
    utility_name: str = "",
    state: str = "",
) -> tuple[list[ExtractedTariff], int]:
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

    tiles = _tile_screenshot_jpeg(img_bytes)
    log.info(
        f"    Page screenshot vision: sending {len(tiles)} tile(s) "
        f"({len(img_bytes)} bytes source) to Claude"
    )

    prompt_text = PAGE_SCREENSHOT_EXTRACTION_PROMPT_BASE.format(
        today=_today_iso(),
        utility_name=utility_name or "unknown",
        state=state or "",
    )
    content_blocks: list[dict] = [{"type": "text", "text": prompt_text}]
    for tile in tiles[:8]:
        content_blocks.append({
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": "image/jpeg",
                "data": base64.standard_b64encode(tile).decode("ascii"),
            },
        })

    client = _get_anthropic_client()
    try:
        resp = client.messages.create(
            model=SONNET_MODEL,
            max_tokens=8192,
            system=[
                {
                    "type": "text",
                    "text": _CACHED_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": content_blocks}],
            tools=[TARIFF_EXTRACTION_TOOL],
            tool_choice={"type": "tool", "name": "store_tariffs"},
            output_config={"effort": "medium"},
        )
    except Exception as e:
        log.error(f"    Page screenshot vision API call failed: {e}")
        return [], 1

    raw = _tool_input_tariffs(resp)
    if raw or last_tool_meta().get("empty_reason"):
        return _parse_extraction_response(
            raw, url,
            empty_reason=last_tool_meta().get("empty_reason", ""),
            linked_document_hint=last_tool_meta().get("linked_document_hint", ""),
        ), 1
    return [], 1


# User message only — full rules/examples come from the cached system prompt.
PDF_VISION_EXTRACTION_PROMPT_BASE = """TODAY: {today}
TARGET UTILITY: {utility_name} ({state})
These are page images of a PDF. Extract residential tariffs following the system rules (SOURCE ONLY, FULL PRICE, residential-by-who-served).

ATTRIBUTION: only the target utility (or province-wide regulator commodity prices for its jurisdiction). Set empty_reason if nothing attributable is visible.
Scanned pages: check each price's decimal point is really printed (956 ¢/kWh is almost certainly 9.56); read each price across from its own row label across page breaks; ignore superscript footnotes. If a value is hard to read, leave it out and list it in missing_fields.

Use the store_tariffs tool."""

# Vision page budget. Bumped from 15 because consolidated rate-book PDFs
# (HQ's electricity-rates.pdf is 160 pages) bury tariff detail past page
# 15. We pay roughly 25/15 = 1.67x more vision tokens but actually see
# the rates we need to extract.
MAX_PDF_VISION_PAGES = 25


def _extract_pdf_vision(
    pdf_bytes: bytes,
    page_url: str,
    utility_name: str = "",
    state: str = "",
) -> tuple[list[ExtractedTariff], int]:
    """Send PDF pages as images to Claude vision for extraction.

    Returns (tariffs, llm_call_count). Falls back to empty list if conversion fails.

    Cached by PDF byte hash: vision calls are the priciest per-page tier
    and scanned rate books rarely change between retries.
    """
    pdf_hash = hashlib.sha256(pdf_bytes).hexdigest()
    cached = _get_llm_cache(pdf_hash, "vision")
    if cached is not None:
        log.info(f"    Using cached vision extraction ({len(cached)} tariffs)")
        return _parse_extraction_response(cached, page_url), 0

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

    prompt_text = PDF_VISION_EXTRACTION_PROMPT_BASE.format(
        today=_today_iso(),
        utility_name=utility_name or "unknown",
        state=state or "",
    )
    content_blocks = [{"type": "text", "text": prompt_text}]
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
            model=SONNET_MODEL,
            max_tokens=8192,
            system=[
                {
                    "type": "text",
                    "text": _CACHED_SYSTEM_PROMPT,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=[{"role": "user", "content": content_blocks}],
            tools=[TARIFF_EXTRACTION_TOOL],
            tool_choice={"type": "tool", "name": "store_tariffs"},
            output_config={"effort": "medium"},
        )
    except Exception as e:
        log.error(f"    PDF vision API call failed: {e}")
        return [], 1

    raw = _tool_input_tariffs(resp)
    if raw or last_tool_meta().get("empty_reason"):
        if raw:
            _set_llm_cache(pdf_hash, "vision", raw)
        return _parse_extraction_response(
            raw, page_url,
            empty_reason=last_tool_meta().get("empty_reason", ""),
            linked_document_hint=last_tool_meta().get("linked_document_hint", ""),
        ), 1

    log.warning("    No tool_use block in PDF vision response")
    return [], 1


_COMPLEXITY_SIGNALS = re.compile(
    r"tier|step|block|on.peak|off.peak|shoulder|summer|winter|"
    r"seasonal|schedule\s+[a-z]|rate\s+[a-z]",
    re.IGNORECASE,
)

def _content_looks_like_rider_schedule(content: str) -> bool:
    """True when page text looks like a rider/adjustment schedule (no base plan).

    Used when two-pass identify returns empty so we still extract per-kWh
    ADJUSTMENT amounts from long rider PDFs.
    """
    if not content or len(content.strip()) < 80:
        return False
    if not _RIDER_DOC_HINT_RE.search(content):
        return False
    # Prefer pages that talk about add-on ¢/kWh charges.
    return bool(
        re.search(
            r"(?:¢|cents?)\s*/\s*kwh|\$\s*/\s*kwh|per[\s-]*kwh|"
            r"in\s+addition\s+to\s+the\s+energy|applies?\s+to\s+all\s+"
            r"(?:kwh|energy|customers?)",
            content,
            re.IGNORECASE,
        )
    )


TWOPASS_IDENTIFY_PROMPT = """List EVERY DISTINCT residential electricity rate/tariff/schedule named in this document for the TARGET UTILITY.

TODAY: {today}
TARGET UTILITY: {utility_name} ({state})

Residential is decided by who the rate serves (homes, dwellings, domestic, farm-and-home, residential single-phase) — not by the schedule name alone. Include "General Service" / "Farm & Home" / "Single-Phase" when the text says they apply to residences.

Be exhaustive. Include EVERY named residential rate — base rates AND variants/options — for example:
- Rate D, Rate DP, Rate DM, Rate DT, Rate Flex D
- Schedule R, Schedule RS, Schedule R-TOU, Schedule R-EV
- Plan A, Plan B, Standard Plan, TOU Plan, Critical Peak Plan
- Residential Service, Domestic Service, Time-of-Use Service, Optional Plans
A "rate" is anything the document treats as a separately-priced residential product, even if it's only a paragraph long. DO NOT collapse variants into the parent rate; list each one.

ATTRIBUTION RULE: Only list rates the document explicitly attributes to the target utility. PROVINCE-WIDE REGULATED PRICES: regulator pages (e.g. OEB RPP) with jurisdiction-wide commodity prices for LDCs in the target's province/state count as attributable. If the document is a comparison/aggregator and lists rates for several utilities, exclude rates not labeled for the target. If you cannot tell (and the regulator exception does not apply), return empty plans.

SKIP / IGNORE: rates that serve only businesses, industry, lighting, irrigation, wholesale, or government departments / government buildings (e.g. "Government Diesel"). Do NOT list per-kWh riders/adjustments (FAM, DSM, fuel, power-cost, Schedule 1xx, etc.) as separate plans when base residential schedules are also in this document — those riders are folded into ENERGY later. If this document contains ONLY rider/adjustment schedules (no base plans), still list each named rider/adjustment schedule with customer_class "residential" so their ¢/kWh amounts can be extracted as ADJUSTMENT components.

Return a JSON object:
{{
  "plans": [ {{"name": "...", "customer_class": "residential",
              "location_hint": "<3–8 words copied EXACTLY, same spelling and punctuation, from the heading or first line of THIS plan's price table — not from the table of contents>"}} ],
  "shared_sections": [ {{"kind": "tou_hours" | "seasons" | "riders" | "general_terms",
                        "location_hint": "<3–8 words copied EXACTLY from the start of that section>"}} ]
}}
"shared_sections" are parts of the document that apply to several residential plans: definitions of on-peak/off-peak hours, season dates, rider or adjustment tables, "applies to all residential schedules" notes. List each once. Use [] if there are none.
If no relevant residential tariffs, return {{"plans": [], "shared_sections": []}}.
Return ONLY valid JSON. No markdown, no explanation.

Content:
{content}"""

TWOPASS_EXTRACT_PROMPT = """TODAY: {today}
TARGET UTILITY: {utility_name} ({state})
Extract the residential tariff named "{tariff_name}" from the content below. Follow the system rules (SOURCE ONLY, FULL PRICE, residential-by-who-served).
The content has two parts: PLAN SECTION (this plan's prices) and SHARED SECTIONS (hours, seasons, riders and terms that may apply to several plans). Use a shared section only where it says it applies to this plan or to all residential plans.
If this plan's text names riders or schedules whose amounts are not in the content, list them in riders_referenced_not_shown and set needs_review=true.
Return one tariff (or none if the named plan is not here or not attributed to the target utility).

PLAN SECTION:
{section}

SHARED SECTIONS:
{shared}"""

# Cap each shared-section slice appended to a per-plan extract call.
TWOPASS_SHARED_SECTION_CHARS = 4000
# empty_reason values that mean "don't bother escalating to Opus".
_EMPTY_REASON_SKIP_OPUS = frozenset({
    "no_residential_rates",
    "wrong_utility",
    "prices_in_linked_document",
})


TWOPASS_WINDOW_BEFORE = 1500
TWOPASS_WINDOW_AFTER = 6000
# A section "has prices" when it carries at least this many rate-looking
# numbers. Table-of-contents hits score ~0 (page numbers, rate codes).
TWOPASS_MIN_SECTION_SCORE = 3
TWOPASS_MAX_HITS = 6
TWOPASS_MAX_SECTION_CHARS = 60000
_DECIMAL_RATE_RE = re.compile(r"(?<![\d.])\d+\.\d{3,}(?![\d.])")


def _rate_number_score(text: str) -> int:
    return len(_RATE_AMOUNT_RE.findall(text)) + len(_DECIMAL_RATE_RE.findall(text))


def _twopass_section(content: str, name: str, hint: str) -> str:
    """Pick the slice(s) of ``content`` most likely to hold ``name``'s prices.

    The identify ``location_hint`` (or, failing that, the tariff name) often
    appears several times: in the table of contents, in cross-references and
    at the tariff's own page. Only looking at the first hit sent table-of-
    contents windows with no prices to the extract call. Instead, merge a
    window around every hit (up to ``TWOPASS_MAX_HITS``) so the real section
    is always included; fall back to the full content when no window has
    rate-looking numbers.
    """
    if len(content) <= 4000:
        return content
    lower = content.lower()
    for needle in ((hint or "").lower()[:30].strip(), (name or "").lower()[:30].strip()):
        if not needle:
            continue
        hits = [m.start() for m in re.finditer(re.escape(needle), lower)][:TWOPASS_MAX_HITS]
        if not hits:
            continue
        spans: list[list[int]] = []
        for h in hits:
            start = max(0, h - TWOPASS_WINDOW_BEFORE)
            end = min(len(content), h + TWOPASS_WINDOW_AFTER)
            if spans and start <= spans[-1][1]:
                spans[-1][1] = max(spans[-1][1], end)
            else:
                spans.append([start, end])
        section = "\n...\n".join(content[a:b] for a, b in spans)[:TWOPASS_MAX_SECTION_CHARS]
        if _rate_number_score(section) >= TWOPASS_MIN_SECTION_SCORE:
            return section
    return content


def _parse_twopass_identify(raw_text: str) -> tuple[list[dict], list[dict]]:
    """Parse identify JSON into (plans, shared_sections).

    Accepts the v9 object shape ``{plans, shared_sections}`` and the legacy
    bare array of plan dicts (so cached / retry replies still work).
    """
    text = (raw_text or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```\w*\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    try:
        raw = json.loads(text)
    except json.JSONDecodeError:
        log.warning(f"    Failed to parse identify JSON: {text[:200]}")
        return [], []
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)], []
    if isinstance(raw, dict):
        plans = raw.get("plans") or []
        shared = raw.get("shared_sections") or []
        return (
            [x for x in plans if isinstance(x, dict)],
            [x for x in shared if isinstance(x, dict)],
        )
    return [], []


def _twopass_shared_blob(content: str, shared_sections: list[dict]) -> str:
    """Concatenate shared-section slices for every per-plan extract call."""
    if not shared_sections or not content:
        return "(none)"
    chunks: list[str] = []
    for sec in shared_sections[:8]:
        kind = str(sec.get("kind") or "shared")
        hint = str(sec.get("location_hint") or "")
        slice_ = _twopass_section(content, kind, hint)[:TWOPASS_SHARED_SECTION_CHARS]
        if not slice_ or slice_ is content:
            # Fall back to a hint-anchored window even if score is low.
            lower = content.lower()
            needle = hint.lower()[:30].strip()
            if needle and needle in lower:
                h = lower.find(needle)
                slice_ = content[max(0, h - 500): h + TWOPASS_SHARED_SECTION_CHARS]
            else:
                continue
        chunks.append(f"### {kind}\n{slice_}")
    return "\n\n".join(chunks) if chunks else "(none)"


def _parse_tool_tariffs(raw: list[dict], source_url: str) -> list[ExtractedTariff]:
    """Parse tool output, carrying empty_reason / linked_document_hint from meta."""
    meta = last_tool_meta()
    return _parse_extraction_response(
        raw,
        source_url,
        empty_reason=meta.get("empty_reason", ""),
        linked_document_hint=meta.get("linked_document_hint", ""),
    )


# Two-pass is expensive; skip it when the residential slice of a long
# commercial rate book is thin (SRP compare / multi-schedule PDFs).
_TWOPASS_MIN_RESIDENTIAL_CHARS = 4000


def _residential_content_span(content: str) -> int:
    """Approx. size of the residential-relevant slice used for extraction."""
    if not content:
        return 0
    selected = _select_rate_content(content, max_chars=25000)
    return len(selected or "")


def _is_complex_page(content: str) -> bool:
    """Detect pages likely to benefit from two-pass extraction.

    Long books with sparse residential sections fall back to single-pass
    (the residential window is too short to amortize identify+N extracts).
    """
    if len(content) < 8000:
        return False
    signals = len(_COMPLEXITY_SIGNALS.findall(content))
    if signals < 5:
        return False
    if _residential_content_span(content) < _TWOPASS_MIN_RESIDENTIAL_CHARS:
        return False
    return True


def _extract_two_pass(
    page: RatePage,
    utility_name: str,
    state: str = "",
    stats: dict | None = None,
) -> tuple[list[ExtractedTariff], int]:
    """Two-pass extraction: identify tariffs first, then extract each individually.

    Returns (tariffs, llm_call_count). When `stats` is provided, records
    `twopass_truncated` if more tariffs were identified than the per-page
    extraction cap allows (so the loss is visible, not silent).

    Results are cached by content hash — two-pass is the most expensive
    text tier (1 identify + up to 20 extract calls), and unchanged rate
    books used to re-pay that on every retry/campaign pass.
    """
    if page.content_hash:
        cached = _get_llm_cache(page.content_hash, "twopass")
        if cached is not None:
            log.info(f"    Using cached two-pass extraction ({len(cached)} tariffs)")
            try:
                return [ExtractedTariff(**d) for d in cached], 0
            except TypeError:
                pass  # stale cache schema — re-extract

    content_for_llm = _select_rate_content(page.content, max_chars=25000)
    llm_calls = 0
    today = _today_iso()

    # Pass 1: Identify all tariff names. Pass the full pre-selected
    # content (which may already be a multi-window concatenation for
    # long rate-book PDFs) so we don't miss tariffs in later sections.
    # Cap at 60k chars defensively to control LLM input size.
    identify_prompt = TWOPASS_IDENTIFY_PROMPT.format(
        content=content_for_llm[:60000],
        utility_name=utility_name,
        state=state,
        today=today,
    )
    # Long-document identify needs higher recall than Haiku reliably
    # delivers. Opus is one call per utility, so the cost delta is
    # bounded; the win is enumerating every named rate in consolidated
    # rate-book PDFs (HQ, Manitoba Hydro, BC Hydro, etc.).
    use_opus_for_identify = len(content_for_llm) >= 30_000
    shared_sections: list[dict] = []
    try:
        # Own phase tag: identify spend is not an extraction-tier outcome and
        # must not be counted as "wasted" escalation spend.
        with llm_cost.phase("phase3_identify"):
            raw_text = _call_claude(
                identify_prompt,
                model=OPUS_MODEL if use_opus_for_identify else None,
            )
        llm_calls += 1
        identified, shared_sections = _parse_twopass_identify(raw_text)
    except Exception as e:
        log.error(f"    Two-pass identification failed: {e}")
        # Retry with Sonnet (default tier) if Opus failed (rate-limit, transient error)
        if use_opus_for_identify:
            try:
                raw_text = _call_claude(identify_prompt)
                llm_calls += 1
                identified, shared_sections = _parse_twopass_identify(raw_text)
            except Exception as e2:
                log.error(f"    Identify retry with Sonnet also failed: {e2}")
                return [], llm_calls
        else:
            return [], llm_calls

    if not identified:
        # Rider-only documents (NSP FAM pages, PGE Schedule 1xx) used to
        # return [] here because identify skipped "riders that aren't
        # standalone rates". Fall back to single-pass extraction so
        # per-kWh ADJUSTMENT amounts still reach Phase 4.
        if _content_looks_like_rider_schedule(content_for_llm):
            log.info(
                "    Two-pass: identify empty on rider-like content — "
                "falling back to single-pass extraction"
            )
            try:
                prompt = format_extraction_user(
                    url=page.url,
                    title=page.title or "",
                    content=content_for_llm[:60000],
                    utility_name=utility_name,
                    state=state,
                )
                raw = _call_claude_tool(prompt)
                llm_calls += 1
                tariffs = _parse_tool_tariffs(raw, page.url)
                for t in tariffs:
                    t.source_url = page.url
                if tariffs and page.content_hash:
                    _set_llm_cache(
                        page.content_hash, "twopass",
                        [asdict(t) for t in tariffs],
                    )
                return tariffs, llm_calls
            except Exception as e:
                log.warning(f"    Rider-schedule single-pass fallback failed: {e}")
                return [], llm_calls
        return [], llm_calls

    relevant = [
        t for t in identified
        if isinstance(t, dict) and t.get("customer_class") in EXTRACT_CLASSES
    ]
    shared_blob = _twopass_shared_blob(content_for_llm, shared_sections)
    log.info(
        f"    Two-pass: identified {len(relevant)} relevant tariffs "
        f"(+{len(shared_sections)} shared sections; "
        f"model={'opus' if use_opus_for_identify else 'sonnet'})"
    )

    # Pass 2: Extract each tariff individually. Cap at 20 to bound LLM
    # cost — consolidated rate-book PDFs (HQ, Manitoba Hydro, etc.) can
    # legitimately name 15+ residential variants plus commercial. When the
    # cap truncates, record it so the run report shows the loss.
    TWOPASS_EXTRACT_CAP = 20
    if len(relevant) > TWOPASS_EXTRACT_CAP:
        dropped = len(relevant) - TWOPASS_EXTRACT_CAP
        log.warning(
            f"    Two-pass: {len(relevant)} tariffs identified but cap is "
            f"{TWOPASS_EXTRACT_CAP} — dropping {dropped}"
        )
        if stats is not None:
            stats["twopass_truncated"] = stats.get("twopass_truncated", 0) + dropped
    all_tariffs: list[ExtractedTariff] = []
    for item in relevant[:TWOPASS_EXTRACT_CAP]:
        name = item.get("name", "Unknown")
        hint = item.get("location_hint", "")

        # Find the section that actually carries this tariff's prices. The
        # identify hint often also appears in the document's table of
        # contents; a narrow window there has no numbers and the extract
        # call returns 0 (NS Power CPP/TOU, HQ Rate D — 2026-10-07).
        section = _twopass_section(content_for_llm, name, hint)

        extract_prompt = TWOPASS_EXTRACT_PROMPT.format(
            today=today,
            tariff_name=name,
            section=section,
            shared=shared_blob,
            utility_name=utility_name,
            state=state,
        )

        try:
            raw_tariffs = _call_claude_tool(extract_prompt)
            tariffs = _parse_tool_tariffs(raw_tariffs, page.url)
            llm_calls += 1
            if not tariffs and section is not content_for_llm:
                # Window missed the prices — one retry on the full selected
                # content (same bound as the identify pass).
                log.info(f"    Two-pass: 0 from section for '{name}' — retrying on full content")
                raw_tariffs = _call_claude_tool(
                    TWOPASS_EXTRACT_PROMPT.format(
                        today=today,
                        tariff_name=name,
                        section=content_for_llm[:60000],
                        shared=shared_blob,
                        utility_name=utility_name,
                        state=state,
                    )
                )
                tariffs = _parse_tool_tariffs(raw_tariffs, page.url)
                llm_calls += 1
            for t in tariffs:
                t.source_url = page.url
                all_tariffs.append(t)
        except Exception as e:
            log.warning(f"    Two-pass extraction failed for '{name}': {e}")
            llm_calls += 1

        time.sleep(0.5)

    if all_tariffs and page.content_hash:
        _set_llm_cache(
            page.content_hash, "twopass", [asdict(t) for t in all_tariffs]
        )
    return all_tariffs, llm_calls


MAX_CONSECUTIVE_LLM_ZEROS = 5


# Names that frequently appear in deregulated-state aggregator pages
# attached to a SPECIFIC utility's section. If we see one of these in
# an extracted tariff name and the target utility's name doesn't share
# a common token with the matched name, the LLM almost certainly grabbed
# a rate from another utility's row in the same multi-utility page.
#
# This list is intentionally narrow — the prompt-level attribution check
# is the primary defense. This is a defense-in-depth log-and-reject that
# catches the most common mis-attribution patterns we've observed.
_OTHER_UTILITY_TOKENS = (
    "penelec", "met-ed", "met ed", "metropolitan edison",
    "duquesne light", "west penn power", "ppl electric",
    "aep ohio", "aep texas", "appalachian power",
    "first energy", "firstenergy",
    "ohio edison", "the illuminating company", "toledo edison",
    "national grid", "eversource",
    "pseg", "pse&g", "jersey central power",
    "atlantic city electric", "delmarva power",
    "pepco", "bge", "baltimore gas",
    "consolidated edison", "con edison", "con ed",
    "central hudson", "orange and rockland",
    "centerpoint energy", "oncor", "aep central",
    "tnmp", "txu energy", "reliant energy",
    "georgia power", "alabama power", "mississippi power",
    "duke energy", "progress energy",
    "dominion energy", "dominion virginia",
    "pacific gas", "pg&e", "pge", "southern california edison", "sce",
    "san diego gas", "sdg&e",
    "xcel energy", "northern states power",
)


def _attribution_violates(
    tariff: ExtractedTariff,
    utility_name: str,
) -> str | None:
    """Return a short reason string if the tariff name appears to belong to
    a different utility than `utility_name`, otherwise None.

    Heuristic: scan the tariff `name` (only — not description) for known
    multi-utility marker phrases. If a marker matches AND the marker
    phrase is NOT itself a substring of the target utility's name,
    flag as a likely mis-attribution.

    Why name-only: the original PECO/Penelec contamination always had
    the wrong utility's name in the tariff name itself (e.g. "Penelec
    Default Service"). Description fields, by contrast, frequently
    contain a utility's own brand acronym as background context (e.g.
    "Gas Distribution Rate" with description "...applies to BGE
    customers..."), which produced false-positive rejections in
    RefreshRun #5.

    Substring match (not token overlap) keeps false positives low when
    a single common word like "light" or "energy" coincidentally
    appears in both lists.
    """
    if not utility_name:
        return None

    name = (tariff.name or "").lower()
    target = utility_name.lower()

    for marker in _OTHER_UTILITY_TOKENS:
        if marker not in name:
            continue
        if marker in target:
            return None  # marker phrase is itself part of the target utility name
        return f"name mentions '{marker}' but target is '{utility_name}'"

    return None


@llm_cost.with_phase("phase3")
def phase3_extract_tariffs(
    pages: list[RatePage],
    utility_name: str,
    stats: dict | None = None,
    state: str = "",
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
    # Prefer fresher individual schedule PDFs over older combined books
    # (R12: PGE Sched_007 Jul 2026 over all_tariffs_56_ Jan 2020).
    # API key is required only when an LLM path is taken — deterministic
    # Sched_007 / Sch 1xx extracts must work without Anthropic (R13 e2e).
    pages = _drop_stale_combined_pages_when_fresher_schedule_exists(list(pages))
    sorted_pages = sorted(pages, key=_phase3_page_rank_key)

    # Upper bound on LLM calls per utility per run. 20 lets a consolidated
    # rate-book PDF identify + extract ~10 distinct rates while leaving
    # budget for HTML page extraction and screenshot vision fallbacks.
    # Below 20, large utilities like Hydro-Québec (6 residential rates +
    # commercial) run out of budget mid-extraction.
    MAX_LLM_CALLS = 20
    all_tariffs: dict[str, ExtractedTariff] = {}
    llm_calls = 0
    consecutive_zeros = 0
    # Same rate-book under two URLs (SRP) must not pay for two-pass twice.
    seen_content_hashes: set[str] = set()
    hash_to_keys: dict[str, list[str]] = {}

    # Tracking for structured error messages
    if stats is None:
        stats = {}
    stats.setdefault("pages_total", len(pages))
    stats.setdefault("pages_skipped_thin", 0)
    stats.setdefault("pages_skipped_irrelevant", 0)
    stats.setdefault("pages_skipped_no_signal", 0)
    stats.setdefault("pages_sent_to_llm", 0)
    stats.setdefault("pages_skipped_dup_hash", 0)
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
        # Skip documents already extracted in this run (identical content
        # under a second URL — common for large rate-book mirrors).
        ch = (page.content_hash or "").strip()
        if ch and ch in seen_content_hashes:
            stats["pages_skipped_dup_hash"] += 1
            # Re-attach prior extracts to this URL when the keeper has none.
            for key in hash_to_keys.get(ch, []):
                existing = all_tariffs.get(key)
                if existing and not existing.source_url:
                    existing.source_url = page.url
            log.info(
                f"    Skipping {page.url[:70]} (duplicate content hash — "
                f"already extracted this run)"
            )
            continue
        if llm_calls >= MAX_LLM_CALLS:
            log.info(f"    Stopping: reached {MAX_LLM_CALLS} LLM call limit")
            stats["llm_call_cap_hit"] = True
            break
        if consecutive_zeros >= MAX_CONSECUTIVE_LLM_ZEROS:
            log.info(
                f"    Early abort: {consecutive_zeros} consecutive 0-tariff "
                f"LLM extractions on this URL tree — likely wrong site"
            )
            stats["early_abort"] = True
            break

        log.info(f"  Phase 3: Extracting from {page.url[:80]}")
        if ch:
            seen_content_hashes.add(ch)

        # R12: deterministic PGE Sched_007 (Default + TOD) — skip LLM.
        det_sch7 = _try_deterministic_pge_sch7_extract(page)
        if det_sch7:
            accepted = 0
            for t in det_sch7:
                if t.customer_class not in EXTRACT_CLASSES:
                    continue
                key = f"{t.name}|{t.customer_class}"
                t.source_url = page.url
                existing = all_tariffs.get(key)
                if existing and not _prefer_extract_over_existing(existing, t):
                    continue
                all_tariffs[key] = t
                if ch:
                    hash_to_keys.setdefault(ch, []).append(key)
                accepted += 1
            if accepted:
                log.info(
                    f"    Deterministic Sched_007: {accepted} tariff(s) "
                    f"from {page.url[:60]}"
                )
                consecutive_zeros = 0
                stats["pages_deterministic"] = (
                    stats.get("pages_deterministic", 0) + 1
                )
                continue

        stats["pages_sent_to_llm"] += 1
        if not ANTHROPIC_API_KEY:
            raise RuntimeError("ANTHROPIC_API_KEY not set")

        # PDF dispatch:
        #   - Rich-text PDFs (>10k chars extractable) -> fall through to
        #     text-based two-pass extraction. The vision page cap (25 pages)
        #     can't see past the first ~50k chars of a long rate book, so
        #     for HQ-style consolidated PDFs (160 pages) text extraction
        #     with the multi-section selector covers more ground.
        #   - Sparse-text PDFs (image-heavy / scanned) -> vision.
        is_rich_text_pdf = (
            page.page_type == "pdf"
            and page.pdf_bytes
            and len(page.pdf_bytes) > 100
            and page.content
            and len(page.content) >= 10_000
        )
        if (
            page.page_type == "pdf"
            and page.pdf_bytes
            and len(page.pdf_bytes) > 100
            and not is_rich_text_pdf
        ):
            log.info(f"    Using PDF vision extraction ({len(page.pdf_bytes)} bytes)")
            tariffs, calls = _extract_pdf_vision(
                page.pdf_bytes, page.url, utility_name=utility_name, state=state,
            )
            for t in tariffs:
                t.extraction_tier = "vision"
            llm_calls += calls
            accepted = 0
            for t in tariffs:
                if SKIP_KEYWORDS.search(t.name):
                    continue
                if t.customer_class not in EXTRACT_CLASSES:
                    continue
                violation = _attribution_violates(t, utility_name)
                if violation:
                    log.warning(f"      Rejected (attribution): {t.name} — {violation}")
                    stats["attribution_rejects"] = stats.get("attribution_rejects", 0) + 1
                    continue
                key = f"{t.name}|{t.customer_class}"
                t.source_url = page.url
                existing = all_tariffs.get(key)
                if existing and not _prefer_extract_over_existing(existing, t):
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
            tariffs, calls = _extract_two_pass(page, utility_name, state=state, stats=stats)
            for t in tariffs:
                t.extraction_tier = "twopass"
            llm_calls += calls
        else:
            content_for_llm = _select_rate_content(page.content, max_chars=20000)
            prompt = format_extraction_user(
                url=page.url,
                title=page.title or "",
                content=content_for_llm,
                utility_name=utility_name,
                state=state,
            )
            try:
                raw_tariffs, model_used = _extract_with_model_routing(prompt, page)
                tariffs = _parse_tool_tariffs(raw_tariffs, page.url)
                for t in tariffs:
                    t.extraction_tier = model_used
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
            if t.customer_class not in EXTRACT_CLASSES:
                log.info(f"      Filtered out: {t.name} (class={t.customer_class}, not residential)")
                continue
            violation = _attribution_violates(t, utility_name)
            if violation:
                log.warning(f"      Rejected (attribution): {t.name} — {violation}")
                stats["attribution_rejects"] = stats.get("attribution_rejects", 0) + 1
                continue

            key = f"{t.name}|{t.customer_class}"
            t.source_url = page.url
            existing = all_tariffs.get(key)
            if existing and not _prefer_extract_over_existing(existing, t):
                continue
            all_tariffs[key] = t
            if ch:
                hash_to_keys.setdefault(ch, []).append(key)
            accepted += 1

        log.info(f"    Extracted {accepted} tariffs from {page.url[:60]}")
        if accepted > 0:
            consecutive_zeros = 0
        else:
            # Fix 12: if text extraction returned 0 on a rate-themed HTML
            # page whose body has no numeric rate signals, the rates may be
            # embedded as images/graphics. Try a full-page screenshot + Vision
            # before giving up on this page.
            #
            # IMPORTANT: skip explainer/conceptual pages (e.g. "understanding-
            # power-demand.html"). Screenshot-vision on those pages
            # hallucinates "tariffs" out of conceptual prose ("Detail de la
            # consommation", "Domestic Rate Schedule – Bill consumption"
            # etc.) which then pollutes the residential view.
            if (
                page.page_type != "pdf"
                and is_rate_relevant_url(page.url, page.title or "")
                and not _page_has_numeric_rates(page.content)
                and not _is_explainer_url(page.url, page.title or "")
            ):
                log.info(
                    "    Text extraction returned 0 on a rate-themed page "
                    "with no numeric signals — trying page-screenshot vision"
                )
                try:
                    vision_tariffs, vision_calls = _extract_page_screenshot_vision(
                        page.url, utility_name=utility_name, state=state,
                    )
                    for t in vision_tariffs:
                        t.extraction_tier = "vision"
                    llm_calls += vision_calls
                    vision_accepted = 0
                    for t in vision_tariffs:
                        if SKIP_KEYWORDS.search(t.name):
                            continue
                        if t.customer_class not in EXTRACT_CLASSES:
                            continue
                        violation = _attribution_violates(t, utility_name)
                        if violation:
                            log.warning(f"      Rejected (attribution): {t.name} — {violation}")
                            stats["attribution_rejects"] = stats.get("attribution_rejects", 0) + 1
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
    # keep the richer one (never let no-ENERGY absorb ENERGY).
    result = _merge_prefix_duplicates(result)
    # R7: drop optional add-on programmes BEFORE full-bill sibling merge
    # so Community Solar / TOU Portfolio cannot absorb real plans.
    result = drop_optional_program_tariffs(result)
    # Prefer full-bill siblings over base-only duplicates of the same plan
    # (Pedernales web TOU 4.35¢ beside 500.2.5 at 8.67¢).
    result = _collapse_full_bill_siblings(result)

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
                        "code": {"type": "string", "description": "Schedule code if shown (e.g. RS, R)"},
                        "customer_class": {
                            "type": "string",
                            "enum": ["residential", "commercial"],
                            "description": "Always residential for new scrapes; commercial is ignored by the pipeline",
                        },
                        "rate_type": {
                            "type": "string",
                            "enum": [
                                "flat", "tiered", "tou", "demand", "seasonal",
                                "tou_tiered", "seasonal_tou", "seasonal_tiered",
                                "demand_tou", "complex",
                            ],
                        },
                        "description": {"type": "string", "description": "One-sentence description"},
                        "effective_date": {
                            "type": "string",
                            "description": "Effective-for-service date YYYY-MM-DD, or empty string",
                        },
                        "confidence": {
                            "type": "number",
                            "description": "0.0-1.0 confidence that the extracted rate values are correct",
                        },
                        "needs_review": {
                            "type": "boolean",
                            "description": "true when any value, clock, day type, season date or rider was unclear or missing",
                        },
                        "missing_fields": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "What the source did not show, e.g. winter off-peak hours",
                        },
                        "riders_referenced_not_shown": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Riders/schedules the plan says apply but whose amounts are not in the content",
                        },
                        "energy_scope": {
                            "type": "string",
                            "enum": [
                                "bundled",
                                "delivery_only",
                                "supply_only",
                                "delivery_plus_default_supply",
                            ],
                            "description": "bundled | delivery_only | supply_only | delivery_plus_default_supply",
                        },
                        "closed_to_new": {
                            "type": "boolean",
                            "description": "true when the schedule is closed to new customers but still active for existing ones",
                        },
                        "energy_includes_riders": {
                            "type": "boolean",
                            "description": "true when ENERGY is the full-bill all-in sum of base + mandatory riders",
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
                                    "unit": {
                                        "type": "string",
                                        "description": "Unit as printed, e.g. ¢/kWh, $/kWh, mills/kWh, $/month, ¢/day",
                                    },
                                    "rate_value": {
                                        "type": "number",
                                        "description": "Number as printed in that unit (no cents-to-dollars conversion)",
                                    },
                                    "tier_min_kwh": {"type": ["number", "null"]},
                                    "tier_max_kwh": {"type": ["number", "null"]},
                                    "tier_label": {"type": ["string", "null"]},
                                    "tier_basis": {
                                        "type": ["string", "null"],
                                        "enum": ["month", "day", "billing_period", None],
                                        "description": "What tier_max_kwh is measured over, when stated",
                                    },
                                    "period_label": {"type": ["string", "null"]},
                                    "period_start_time": {
                                        "type": ["string", "null"],
                                        "description": "HH:MM 24h clock start; null if not stated (do not invent)",
                                    },
                                    "period_end_time": {
                                        "type": ["string", "null"],
                                        "description": "HH:MM 24h clock end; overnight wrap OK; 24:00 = end of day",
                                    },
                                    "day_type": {
                                        "type": ["string", "null"],
                                        "description": "weekday | weekend | holiday | all; required on TOU ENERGY rows",
                                    },
                                    "season": {"type": ["string", "null"]},
                                    "season_start_month": {
                                        "type": ["integer", "null"],
                                        "description": "1-12 inclusive season start month; null if not stated",
                                    },
                                    "season_start_day": {"type": ["integer", "null"]},
                                    "season_end_month": {"type": ["integer", "null"]},
                                    "season_end_day": {"type": ["integer", "null"]},
                                    "included_in_energy": {
                                        "type": ["boolean", "null"],
                                        "description": "Required on per-kWh ADJUSTMENT: true if already in ENERGY all-in, false if excluded",
                                    },
                                    "rider_scope": {
                                        "type": ["string", "null"],
                                        "enum": [
                                            "all_customers",
                                            "optional_program",
                                            "some_kwh_or_customers",
                                            "tou_overlay",
                                            "event",
                                            None,
                                        ],
                                    },
                                },
                                "required": ["component_type", "unit", "rate_value"],
                            },
                        },
                    },
                    "required": ["name", "customer_class", "rate_type", "components", "confidence"],
                },
            },
            "empty_reason": {
                "type": ["string", "null"],
                "enum": [
                    "no_residential_rates",
                    "wrong_utility",
                    "prices_in_linked_document",
                    "unreadable",
                    "other",
                    None,
                ],
                "description": "Set when tariffs is empty: why nothing was extracted",
            },
            "linked_document_hint": {
                "type": ["string", "null"],
                "description": "Link text/title of the document that holds the prices, when empty_reason is prices_in_linked_document",
            },
        },
        "required": ["tariffs"],
    },
}


class _AnthropicMessagesProxy:
    """Wraps client.messages so every create() records token cost."""

    def __init__(self, messages):
        self._m = messages

    def create(self, **kwargs):
        from app.services import anthropic_compat

        resp = anthropic_compat.create(self._m, **kwargs)
        llm_cost.record_anthropic(kwargs.get("model", ""), getattr(resp, "usage", None))
        return resp

    def __getattr__(self, name):
        return getattr(self._m, name)


class _AnthropicCostProxy:
    """Transparent proxy over an Anthropic client that meters .messages.create."""

    def __init__(self, client):
        self._c = client

    @property
    def messages(self):
        return _AnthropicMessagesProxy(self._c.messages)

    def __getattr__(self, name):
        return getattr(self._c, name)


def _get_anthropic_client():
    """Lazy-init Anthropic client (wrapped to meter per-call token cost)."""
    import anthropic
    client = getattr(_thread_local, "anthropic_client", None)
    if client is None:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        _thread_local.anthropic_client = client
    return _AnthropicCostProxy(client)


def _call_claude(prompt: str, model: str | None = None) -> str:
    """Text-only Claude call (used by two-pass identification step).

    Defaults to Sonnet (main extract tier). Pass `model=OPUS_MODEL` from
    callers that need higher recall (e.g. enumerating all named rates in a
    consolidated rate-book PDF — mid-tier models are stochastic at scale
    and will silently drop tariffs from the list).
    """
    from app.services.anthropic_compat import response_text

    client = _get_anthropic_client()
    resp = client.messages.create(
        model=model or SONNET_MODEL,
        max_tokens=8192,
        messages=[{"role": "user", "content": prompt}],
    )
    return response_text(resp.content)


# Static rules + examples only. Callers must pass EXTRACTION_USER_PROMPT
# (dynamic TARGET UTILITY / URL / content) as the user message — not the
# full EXTRACTION_PROMPT — otherwise the ~14k-char rules are sent twice.
# History: Gemini took one user blob (`contents=prompt`); when Anthropic
# caching was added, the static half was copied into `system` but the
# full prompt was still passed as `user`. Gemini is gone; keep rules in
# system only so they cache once.
_CACHED_SYSTEM_PROMPT = EXTRACTION_SYSTEM_PROMPT


def _remember_tool_meta(meta: dict) -> None:
    _thread_local.last_tool_meta = dict(meta or {})


def last_tool_meta() -> dict:
    """Metadata from the most recent ``_call_claude_tool`` (empty_reason, trunc)."""
    return dict(getattr(_thread_local, "last_tool_meta", None) or {})


def _tool_input_tariffs(resp) -> list[dict]:
    """Pull tariffs (+ top-level empty_reason) from a store_tariffs tool call."""
    for block in getattr(resp, "content", None) or []:
        if getattr(block, "type", None) == "tool_use" and getattr(block, "name", None) == "store_tariffs":
            payload = block.input or {}
            _remember_tool_meta({
                "empty_reason": payload.get("empty_reason") or "",
                "linked_document_hint": payload.get("linked_document_hint") or "",
                "truncated": getattr(resp, "stop_reason", None) == "max_tokens",
            })
            raw = payload.get("tariffs", [])
            return raw if isinstance(raw, list) else []
    return []


def _call_claude_tool(
    prompt: str,
    model: str | None = None,
    *,
    max_tokens: int = 8192,
    system: str | None = None,
    effort: str | None = None,
) -> list[dict]:
    """Call Claude with tool use for structured tariff extraction.

    ``model`` defaults to ``SONNET_MODEL`` (tier-2). Pass ``HAIKU_MODEL`` for
    the cheap tier-1 pass. Uses Anthropic prompt caching: the static system
    prompt and tool schema are marked ephemeral so repeated calls within a
    session pay only 10% of normal input cost for the cached portion.

    ``system`` defaults to the full cached extraction prompt; Haiku passes the
    slim ``HAIKU_EXTRACTION_SYSTEM_PROMPT``. ``effort`` sets per-call
    ``output_config.effort`` (overrides ``ANTHROPIC_EFFORT``).

    If the reply is cut off at the token limit (``stop_reason == max_tokens``),
    retries once on the same model with a larger limit. Returns the parsed
    tariff dicts from the tool call input. Falls back to text parsing on error.
    """
    client = _get_anthropic_client()
    _remember_tool_meta({})
    use_model = model or SONNET_MODEL
    system_text = system
    if system_text is None:
        system_text = (
            HAIKU_EXTRACTION_SYSTEM_PROMPT
            if use_model == HAIKU_MODEL
            else _CACHED_SYSTEM_PROMPT
        )
    if effort is None:
        if use_model == HAIKU_MODEL:
            effort = "low"
        elif use_model == OPUS_MODEL:
            effort = "high"
        else:
            effort = "medium"
    kwargs = {
        "model": use_model,
        "max_tokens": max_tokens,
        "system": [
            {
                "type": "text",
                "text": system_text,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        "messages": [{"role": "user", "content": prompt}],
        "tools": [
            {
                **TARIFF_EXTRACTION_TOOL,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        "tool_choice": {"type": "tool", "name": "store_tariffs"},
        "output_config": {"effort": effort},
    }
    resp = client.messages.create(**kwargs)
    tariffs = _tool_input_tariffs(resp)
    if getattr(resp, "stop_reason", None) == "max_tokens":
        log.warning(
            f"    Extraction truncated (max_tokens={kwargs['max_tokens']}) — "
            "retrying once with a larger limit"
        )
        kwargs["max_tokens"] = max(max_tokens * 2, 32000)
        resp = client.messages.create(**kwargs)
        tariffs = _tool_input_tariffs(resp)
        meta = last_tool_meta()
        meta["truncated_retried"] = True
        _remember_tool_meta(meta)
    if tariffs:
        return tariffs

    if last_tool_meta().get("empty_reason"):
        # Model explicitly reported no tariffs — don't text-parse.
        return []

    log.warning("    No tool_use block in Claude response, falling back to text parse")
    for block in getattr(resp, "content", None) or []:
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


def _select_model(page: "RatePage") -> str:
    """Choose which Anthropic tier to start with based on page characteristics.

    Returns ``haiku`` (cheap first pass) or ``sonnet`` (skip straight to the
    main extract tier for PDFs / complex HTML — same heuristic that used to
    skip Gemini and start at Haiku 4.5).
    """
    if page.page_type == "pdf" and page.pdf_bytes:
        return "sonnet"

    if page.content and _is_complex_page(page.content):
        return "sonnet"

    return "haiku"


def _call_opus_tool(prompt: str) -> list[dict]:
    """Call Claude Opus (see OPUS_MODEL) for structured tariff extraction.

    Last-resort third tier when Haiku 5.5 and Sonnet 5.5 both return nothing.
    More expensive but better at complex rate structures, PDFs, and edge cases.
    Reuses ``_call_claude_tool`` so truncation retry and empty_reason meta apply.
    """
    try:
        return _call_claude_tool(prompt, model=OPUS_MODEL)
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
# OEB-style tables print bare decimals (9.8, 15.7) under a "(¢/kWh)" heading
# with no "$" or "cents" adjacent to the number.
_CENTS_KWH_HEADING = re.compile(r"(?:¢|cents)\s*/\s*k[wW]h", re.IGNORECASE)
_BARE_RATE_DECIMAL = re.compile(r"\b\d{1,3}\.\d{1,4}\b")


def _page_has_numeric_rates(content: str) -> bool:
    """Does the page contain at least one rate-amount-shaped number?

    Pages where the cheaper tiers returned 0 AND which contain no numeric
    rate values cannot produce extractable tariffs no matter which LLM we
    use — so we skip escalating to the expensive Opus tier.
    """
    if not content:
        return False
    if _NUMERIC_RATE_SIGNAL.search(content):
        return True
    # Regulator-style tables: ¢/kWh (or cents/kWh) heading + bare decimals.
    if _CENTS_KWH_HEADING.search(content) and _BARE_RATE_DECIMAL.search(content):
        return True
    return False


_TOU_FAMILY_TYPES = frozenset({
    "tou", "tou_tiered", "demand_tou", "seasonal_tou",
})
_SEASONAL_FAMILY_TYPES = frozenset({
    "seasonal", "seasonal_tiered", "seasonal_tou",
})


def _haiku_result_needs_escalate(tariffs: list[dict]) -> bool:
    """True when a non-empty Haiku answer should be re-run on Sonnet.

    Cheap checks only: TOU/seasonal rows missing required structured fields,
    or riders named but not shown. Avoids caching a broken first pass forever.
    """
    for t in tariffs:
        if not isinstance(t, dict):
            continue
        if t.get("riders_referenced_not_shown"):
            return True
        missing = t.get("missing_fields") or []
        if missing and t.get("needs_review"):
            return True
        rt = str(t.get("rate_type") or "").lower()
        comps = [c for c in (t.get("components") or []) if isinstance(c, dict)]
        energy = [c for c in comps if c.get("component_type") == "energy"]
        if not energy:
            continue
        if rt in _TOU_FAMILY_TYPES:
            if any(
                not c.get("period_start_time") or not c.get("period_end_time")
                for c in energy
            ):
                return True
        if rt in _SEASONAL_FAMILY_TYPES:
            if any(not c.get("season_start_month") for c in energy):
                return True
    return False


def _extract_with_model_routing(prompt: str, page: "RatePage") -> tuple[list[dict], str]:
    """Extract tariffs using a 3-tier Anthropic model strategy.

    Tier 1: Claude Haiku 5.5  (cheap first pass — was Gemini Flash)
    Tier 2: Claude Sonnet 5.5 (main extract — was Haiku 4.5)
    Tier 3: Claude Opus 5.5   (last resort — only invoked if the page has at
                              least one rate-amount-shaped number AND the
                              per-utility Opus budget isn't exhausted)

    Returns (tariff_dicts, model_used). Uses LLM extraction cache to avoid
    redundant API calls. Non-empty Haiku answers that fail cheap quality
    checks escalate to Sonnet (review item 7).
    """
    model = _select_model(page)

    if page.content_hash:
        # Check ALL tiers, not just the selected model: if a previous run
        # escalated to Opus for this exact content, re-running Haiku and
        # Sonnet first just burns two calls to rediscover the same answer.
        seen_models = []
        for m in (model, "haiku", "sonnet", "opus"):
            if m in seen_models:
                continue
            seen_models.append(m)
            cached = _get_llm_cache(page.content_hash, m)
            if cached is not None:
                log.info(f"    Using cached {m} extraction ({len(cached)} tariffs)")
                return cached, m

    last_tier = "haiku"
    if model == "haiku":
        result = _call_claude_tool(prompt, model=HAIKU_MODEL, effort="low")
        llm_cost.record_extraction_outcome("haiku", bool(result))
        if result and not _haiku_result_needs_escalate(result):
            if page.content_hash:
                _set_llm_cache(page.content_hash, "haiku", result)
            return result, "haiku"
        if result:
            log.info(
                "    Haiku returned tariffs but failed quality checks — "
                "escalating to Sonnet"
            )
        else:
            log.info("    Haiku returned no tariffs, escalating to Sonnet")

    last_tier = "sonnet"
    result = _call_claude_tool(prompt, model=SONNET_MODEL, effort="medium")
    llm_cost.record_extraction_outcome("sonnet", bool(result))
    if result:
        if page.content_hash:
            _set_llm_cache(page.content_hash, "sonnet", result)
        return result, "sonnet"

    empty_reason = last_tool_meta().get("empty_reason") or ""
    if empty_reason in _EMPTY_REASON_SKIP_OPUS:
        log.info(
            f"    Sonnet returned 0 with empty_reason={empty_reason!r}; "
            "skipping Opus escalation"
        )
        return [], last_tier

    # Only escalate to Opus if the page actually contains numeric rate data.
    # Pages with rate-themed titles but no numbers (e.g. marketing pages
    # that link to PDFs) can't yield tariffs from any LLM, so escalating
    # to the expensive Opus tier wastes tokens.
    if not _page_has_numeric_rates(page.content):
        log.info(
            f"    {last_tier.capitalize()} returned 0; skipping Opus escalation "
            "(page has no numeric rate signals)"
        )
        return [], last_tier

    # Per-utility Opus budget: Opus escalations hit on only ~8% of pages, so
    # cap how many times one utility may fire the expensive tier per run.
    global _opus_escalations_this_util
    if _opus_escalations_this_util >= OPUS_MAX_PER_UTILITY:
        log.info(
            f"    {last_tier.capitalize()} returned 0; skipping Opus escalation "
            f"(per-utility cap of {OPUS_MAX_PER_UTILITY} reached)"
        )
        return [], last_tier
    _opus_escalations_this_util += 1

    log.info(
        f"    {last_tier.capitalize()} returned no tariffs, escalating to Opus "
        f"({_opus_escalations_this_util}/{OPUS_MAX_PER_UTILITY})"
        + (f" empty_reason={empty_reason!r}" if empty_reason else "")
    )
    result = _call_opus_tool(prompt)
    # Telemetry: track whether the expensive Opus escalation actually pays
    # off (returns tariffs) or burns tokens for nothing. Rolled into the
    # run summary as tier_outcomes so we can see Opus's hit rate per run.
    llm_cost.record_extraction_outcome("opus", bool(result))
    if result:
        if page.content_hash:
            _set_llm_cache(page.content_hash, "opus", result)
    return result, "opus"


def _as_str_list(raw) -> list[str]:
    if not raw:
        return []
    if isinstance(raw, str):
        return [raw] if raw.strip() else []
    if isinstance(raw, (list, tuple)):
        return [str(x).strip() for x in raw if str(x).strip()]
    return []


def _parse_extraction_response(
    items: list[dict],
    source_url: str,
    *,
    empty_reason: str = "",
    linked_document_hint: str = "",
) -> list[ExtractedTariff]:
    """Convert raw dicts (from tool use or text parse) into ExtractedTariff objects."""
    tariffs = []
    for item in items:
        if not isinstance(item, dict):
            continue
        needs = bool(item.get("needs_review"))
        missing = _as_str_list(item.get("missing_fields"))
        riders_missing = _as_str_list(item.get("riders_referenced_not_shown"))
        if missing or riders_missing:
            needs = True
        energy_includes = item.get("energy_includes_riders")
        if energy_includes is not None:
            energy_includes = bool(energy_includes)
        tariffs.append(ExtractedTariff(
            name=item.get("name", "Unknown"),
            code=item.get("code", ""),
            customer_class=item.get("customer_class", ""),
            rate_type=item.get("rate_type", ""),
            description=item.get("description", ""),
            source_url=source_url,
            effective_date=item.get("effective_date", ""),
            components=item.get("components", []),
            confidence=float(item.get("confidence", 0.0) or 0.0),
            needs_review=needs,
            missing_fields=missing,
            riders_referenced_not_shown=riders_missing,
            energy_scope=str(item.get("energy_scope") or ""),
            closed_to_new=bool(item.get("closed_to_new")),
            energy_includes_riders=energy_includes,
            empty_reason=str(item.get("empty_reason") or empty_reason or ""),
            linked_document_hint=str(
                item.get("linked_document_hint") or linked_document_hint or ""
            ),
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

# Schema / historical still know commercial; new scrapes keep residential only.
VALID_CLASSES = {"residential", "commercial"}
EXTRACT_CLASSES = {"residential"}
VALID_RATE_TYPES = {
    "flat", "tiered", "tou", "demand", "seasonal",
    "tou_tiered", "seasonal_tou", "seasonal_tiered", "demand_tou",
    "tiered_demand", "demand_tiered", "tiered_demand_seasonal",
    "seasonal_demand", "demand_seasonal", "complex",
    # Aliases the DB layer normalizes to COMPLEX. Listed here so the
    # validator doesn't reject tariffs whose rate_type is recoverable.
    "seasonal_tiered_demand", "seasonal_tou_tiered",
    "seasonal_tou_demand", "tou_demand",
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


_CENTS_UNIT_RE = re.compile(r"(?:¢|\bcents?\b|(?<![a-z])c/)", re.IGNORECASE)
_MILLS_UNIT_RE = re.compile(r"\bmills?\b", re.IGNORECASE)

# Canonical dollar units per component type after normalization, used only
# when a cents unit names no denominator of its own.
_DOLLAR_UNIT = {
    "energy": "$/kWh",
    "demand": "$/kW",
    "fixed": "$/month",
    "minimum": "$/month",
    "adjustment": "$/kWh",
}

# Denominator in a cents unit string → dollar unit. Order matters: kWh
# before kW, kVA before kW.
_CENTS_DENOMINATORS = (
    (re.compile(r"kwh", re.IGNORECASE), "$/kWh"),
    (re.compile(r"kva", re.IGNORECASE), "$/kVA"),
    (re.compile(r"kw", re.IGNORECASE), "$/kW"),
    (re.compile(r"\b(?:day|daily|d)\b", re.IGNORECASE), "$/day"),
    (re.compile(r"\b(?:month|monthly|mo)\b", re.IGNORECASE), "$/month"),
    (re.compile(r"\b(?:bill|billing\s*period)\b", re.IGNORECASE), "$/bill"),
    (re.compile(r"\b(?:year|yr|annual)\b", re.IGNORECASE), "$/year"),
)

# Monthly equivalents for bounds checks on periodic charges.
_PERIODIC_TO_MONTH = {"$/day": 30.4, "$/year": 1 / 12}


def _dollar_unit_for_cents(ctype: str, unit: str) -> str:
    """Dollar unit keeping the cents unit's own billing period.

    "45.5 ¢/day" is a daily charge: relabelling it "$/month" understated it
    ~30x (audit F6b).
    """
    for pattern, dollar_unit in _CENTS_DENOMINATORS:
        if pattern.search(unit):
            return dollar_unit
    return _DOLLAR_UNIT.get(ctype, "$/kWh")


_CRITICAL_PEAK_RE = re.compile(
    r"critical[\s-]*peak|\bcpp\b|peak[\s-]*time[\s-]*rebate|\bptr\b|"
    r"\bevent[\s-]*(?:price|rate|period|hour|energy)|"
    r"peak[\s-]*day[\s-]*pricing|critical[\s-]*period|"
    r"non[\s-]*critical",
    re.IGNORECASE,
)


def _is_critical_peak_price(
    comp: dict,
    tariff: "ExtractedTariff | None" = None,
) -> bool:
    """True when a component/tariff is labelled critical-peak / CPP / event.

    Nova Scotia Power CPP event energy (~182¢/kWh) is a legitimate outlier
    and must not be hard-rejected or mistreated as cents-mislabeled dollars.
    """
    parts = [
        str(comp.get("tier_label") or ""),
        str(comp.get("period_label") or ""),
        str(comp.get("season") or ""),
    ]
    if tariff is not None:
        parts.extend(
            [
                str(tariff.name or ""),
                str(tariff.code or ""),
                str(tariff.rate_type or ""),
                str(tariff.description or ""),
            ]
        )
    return bool(_CRITICAL_PEAK_RE.search(" ".join(parts)))


def _normalize_component_units(t: ExtractedTariff, p99_energy: float) -> list[str]:
    """Normalize cents- and mills-denominated components to dollars, in place.

    Passes:
      1. Deterministic: unit string says cents (¢/kWh, cents/kWh, c/kWh)
         -> divide by 100 and rewrite the unit.
      2. Deterministic: unit string says mills (mills/kWh) -> divide by 1000
         and rewrite to the matching dollar unit (1 mill = $0.001).
      3. Heuristic rescue: unit claims dollars but an energy value is so
         large it would be hard-rejected (>3x p99) while value/100 lands
         in the plausible band — almost certainly cents mislabeled as
         dollars. Convert instead of discarding. Legit critical-peak rates
         (labelled CPP / event / critical-peak) are never touched, even
         above the hard-reject line.

    Returns a list of notes (empty when nothing changed).
    """
    notes: list[str] = []
    for comp in t.components:
        ctype = comp.get("component_type")
        unit = str(comp.get("unit") or "")
        try:
            rv = float(comp.get("rate_value"))
        except (ValueError, TypeError):
            continue

        if _MILLS_UNIT_RE.search(unit):
            new_rv = rv / 1000.0
            new_unit = _dollar_unit_for_cents(ctype, unit)
            comp["rate_value"] = new_rv
            comp["unit"] = new_unit
            notes.append(f"{ctype} {rv} {unit!r} -> {new_rv} {new_unit}")
        elif _CENTS_UNIT_RE.search(unit):
            new_rv = rv / 100.0
            new_unit = _dollar_unit_for_cents(ctype, unit)
            comp["rate_value"] = new_rv
            comp["unit"] = new_unit
            notes.append(f"{ctype} {rv} {unit!r} -> {new_rv} {new_unit}")
        elif (
            ctype == "energy"
            and rv > p99_energy * 3
            and 0.01 <= rv / 100.0 <= p99_energy
            and not _is_critical_peak_price(comp, t)
        ):
            new_rv = rv / 100.0
            comp["rate_value"] = new_rv
            comp["unit"] = "$/kWh"
            notes.append(
                f"energy {rv} looks like cents mislabeled as dollars -> {new_rv} $/kWh"
            )
    return notes



def _normalize_component_label(comp: dict) -> str:
    """Normalize tier/period label for fixed/minimum duplicate detection.

    Amp-tier wording varies ("0-10 Amp", "0 – 10 amps", "Basic Customer
    Charge (0-10A)") — collapse to a stable token string so equal charges
    for the same amp band match.
    """
    raw = (
        comp.get("tier_label")
        or comp.get("period_label")
        or comp.get("label")
        or ""
    )
    n = str(raw).lower()
    n = re.sub(r"[–—−]", "-", n)
    n = re.sub(r"[^a-z0-9\s\-]+", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    # Drop boilerplate words that differ between basic vs minimum rows.
    drop = {
        "basic", "customer", "charge", "charges", "minimum", "monthly",
        "service", "the", "a", "an", "of", "for", "per", "month", "mo",
    }
    toks = [t for t in n.split() if t not in drop]
    return " ".join(toks)


def _is_energy_unit(unit: str | None) -> bool:
    """True when a component unit is energy ($/kWh or cents/kWh)."""
    u = str(unit or "").strip().lower().replace(" ", "")
    return "kwh" in u


def _season_key(season: str | None) -> str:
    """Normalize season labels for matching (ignore month-range suffixes)."""
    s = str(season or "").strip().lower()
    if not s:
        return ""
    # "Winter (Dec–Apr)" / "Non-Winter (May–Nov)" → leading token family
    s = re.split(r"[(/]", s, maxsplit=1)[0].strip()
    s = re.sub(r"[^a-z0-9\s\-]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def count_energy_seasons(components: list[dict]) -> int:
    """Count distinct non-empty ENERGY season labels on a component list."""
    seasons: set[str] = set()
    for comp in components or []:
        if not isinstance(comp, dict):
            continue
        if str(comp.get("component_type") or "").strip().lower() != "energy":
            continue
        key = _season_key(comp.get("season"))
        if key:
            seasons.add(key)
    return len(seasons)


_SEASON_DATE_KEYS = ("season_start_month", "season_start_day", "season_end_month", "season_end_day")


def expand_relative_seasonal_energy(
    components: list[dict],
    *,
    keep_adjustments: bool = True,
) -> list[dict]:
    """Turn base ENERGY ± seasonal ADJUSTMENT riders into all-in ENERGY seasons.

    Schedules like Newfoundland Power Rate #1.1S say energy charges from a
    base rate apply subject to a winter premium and non-winter credit. LLMs
    often emit one unseasoned (or mis-seasoned) ENERGY plus ADJUSTMENT rows;
    Flux / Lookup only group ENERGY by season, so Non-Winter never appears.

    When ≥2 distinct seasonal energy-unit ADJUSTMENT rows are present and a
    single base ENERGY $/kWh can be inferred, emit ENERGY = base + adj for
    each adjustment season. Fixed/demand/minimum rows are untouched.
    ADJUSTMENT rows are kept by default for audit, flagged
    ``included_in_energy`` so cost consumers do not add them twice.
    """
    if not components:
        return components

    energy_rows: list[dict] = []
    seasonal_adjs: list[dict] = []
    other: list[dict] = []

    for comp in components:
        if not isinstance(comp, dict):
            other.append(comp)
            continue
        ctype = str(comp.get("component_type") or "").strip().lower()
        if ctype == "energy" and _is_energy_unit(comp.get("unit")):
            energy_rows.append(comp)
        elif ctype == "adjustment" and _is_energy_unit(comp.get("unit")):
            if _season_key(comp.get("season")):
                seasonal_adjs.append(comp)
            else:
                other.append(comp)
        else:
            other.append(comp)

    # Need at least two seasons of relative adjustments to expand.
    adj_by_season: dict[str, dict] = {}
    for adj in seasonal_adjs:
        key = _season_key(adj.get("season"))
        if key and key not in adj_by_season:
            adj_by_season[key] = adj
    if len(adj_by_season) < 2:
        return components

    # Infer a single base energy rate.
    base: float | None = None
    unseasoned = [
        e for e in energy_rows if not _season_key(e.get("season"))
    ]
    try:
        if len(unseasoned) == 1:
            base = float(unseasoned[0].get("rate_value"))
        elif unseasoned:
            vals = {round(float(e.get("rate_value") or 0), 6) for e in unseasoned}
            if len(vals) == 1:
                base = next(iter(vals))
        if base is None and energy_rows:
            # Mislabeled case: ENERGY tagged Winter but equal to the base,
            # with ADJUSTMENTs carrying the true differentials.
            vals = {round(float(e.get("rate_value") or 0), 6) for e in energy_rows}
            if len(vals) == 1:
                base = next(iter(vals))
    except (TypeError, ValueError):
        return components

    if base is None:
        return components

    # Existing ENERGY by season key → prefer already-correct all-in rows.
    energy_by_season: dict[str, dict] = {}
    for e in energy_rows:
        key = _season_key(e.get("season"))
        if key and key not in energy_by_season:
            energy_by_season[key] = e

    new_energy: list[dict] = []
    used_season_keys: set[str] = set()
    for key, adj in adj_by_season.items():
        try:
            adj_val = float(adj.get("rate_value"))
        except (TypeError, ValueError):
            continue
        all_in = round(base + adj_val, 6)
        existing = energy_by_season.get(key)
        season_label = str(adj.get("season") or "").strip() or key.title()
        if existing is not None:
            try:
                existing_val = round(float(existing.get("rate_value") or 0), 6)
            except (TypeError, ValueError):
                existing_val = None
            if existing_val is not None and abs(existing_val - all_in) < 1e-9:
                # Already all-in for this season — keep as-is.
                new_energy.append(existing)
            else:
                # Replace mislabeled base-as-season ENERGY with all-in.
                row = dict(existing)
                row["rate_value"] = all_in
                row["unit"] = "$/kWh"
                row["season"] = season_label or row.get("season")
                if not row.get("tier_label"):
                    row["tier_label"] = season_label
                for k in _SEASON_DATE_KEYS:
                    if row.get(k) is None and adj.get(k) is not None:
                        row[k] = adj.get(k)
                new_energy.append(row)
        else:
            new_energy.append({
                "component_type": "energy",
                "unit": "$/kWh",
                "rate_value": all_in,
                "tier_min_kwh": None,
                "tier_max_kwh": None,
                "tier_label": season_label,
                "period_label": None,
                "season": season_label,
                **{k: adj.get(k) for k in _SEASON_DATE_KEYS},
            })
        used_season_keys.add(key)

    if not new_energy:
        return components

    # Keep any ENERGY seasons that were not relative-adjustment seasons
    # (e.g. absolute Summer/Winter already present alongside riders).
    for key, e in energy_by_season.items():
        if key not in used_season_keys:
            new_energy.append(e)

    out: list[dict] = []
    out.extend(other)
    if keep_adjustments:
        out.extend({**a, "included_in_energy": True} for a in seasonal_adjs)
    out.extend(new_energy)
    return out


_STACKING_RIDER_LABEL_RE = re.compile(
    r"\b(?:fam|dsm|dcrr|scrr|storm|fuel(?:\s*(?:adjust(?:ment)?|cost|efficiency))?|"
    r"efficiency|cost\s*recovery|actual\s*adjustment|balance\s*adjustment|"
    r"aa/?ba|power\s*cost|\bpca\b|\bbac\b|rate\s*rider|energy\s*rider|"
    r"interim\s*adjust|purchased\s*power|resource\s*adequacy|"
    r"deferred\s*accounting|transition\s*adjust|supply\s*cost|"
    r"transmission|distribution|delivery|tcos|t\s*&\s*d|"
    r"schedule\s*1\d{2}|sch(?:edule)?\s*1\d{2})\b",
    re.IGNORECASE,
)

_RIDER_DONOR_NAME_RE = re.compile(
    r"\b(?:rider|adjustment|surcharge|fuel\s*cost|fuel\s*adjust|"
    r"power\s*cost|cost\s*recovery|fam|dsm|scrr|dcrr|pca|bac|"
    r"efficiency|interim\s*adjust)\b",
    re.IGNORECASE,
)

# Optional-enrollment / source-specific charges must NOT fold into every
# plan's ENERGY (PG&E SmartRate credits, Pedernales community-solar, …).
_OPTIONAL_OR_SCOPED_RIDER_RE = re.compile(
    r"\b(?:optional|opt[\s-]*in|enrollment|participat(?:ion|e|ing)?|"
    r"program|credit|rebate|smartrate|smart[\s-]*rate|"
    r"peak[\s-]*time[\s-]*rebate|\bptr\b|peak[\s-]*time[\s-]*reward|"
    r"community[\s-]*solar|solar[\s-]*received|solar[\s-]*kwh|"
    r"green[\s-]*power|green[\s-]*energy|green[\s-]*future|"
    r"avenir[\s-]*vert|renewable[\s-]*choice|"
    r"voluntary|subscriber|subscription|"
    r"net[\s-]*meter(?:ing)?|export[\s-]*credit|surplus[\s-]*credit|"
    r"option\s+[ivx]+\b|self[\s-]*generation)\b",
    re.IGNORECASE,
)

# Time-of-day surcharge/discount overlays (BC Hydro ±5¢) belong on TOU
# plans only — never copy onto flat/tiered ENERGY.
_TOD_OVERLAY_RIDER_RE = re.compile(
    r"\b(?:time[\s-]*of[\s-]*day|\btod\b|"
    r"tou[\s-]*(?:surcharge|discount|adjust|rider|premium|credit)|"
    r"(?:on|off|mid)[\s-]*peak[\s-]*(?:surcharge|discount|adjust|rider|premium|credit)|"
    r"surcharge[\s/\-]*discount|discount[\s/\-]*surcharge)\b",
    re.IGNORECASE,
)

_ALL_IN_LABEL_RE = re.compile(r"all[\s-]*in", re.IGNORECASE)
_ALL_IN_DELIVERY_RE = re.compile(
    r"transmission|distribution|delivery|t\s*&\s*d|energy\s*\+",
    re.IGNORECASE,
)
# Abbreviated delivery breakdown: "all-in: 0.397 + 7.601 + 7.051" (tx+dist+energy).
_ALL_IN_ABBREV_DELIVERY_RE = re.compile(
    r"all[\s-]*in\s*:?\s*\(?\s*\d+(?:\.\d+)?\s*\+",
    re.IGNORECASE,
)
_ALL_IN_RIDERS_RE = re.compile(
    r"\briders?\b|fam|dsm|fuel|pca|storm|dcrr|scrr",
    re.IGNORECASE,
)

# Cap on official rider/adjustment docs fetched when a residential extract
# references schedules that aren't in the current page batch (NSP FAM pages,
# PGE Schedule 1xx PDFs). Index/seed pages do NOT count against the Sch 1xx
# hop budget — that budget must cover Sch 100's full applicable list (≥26).
MAX_RIDER_DOCS_FETCH = 6
MAX_PGE_SCH1XX_FETCH = 28

# Sanity: FAM/DCRR-like ¢/kWh riders above this are almost certainly
# misparsed base energy (NSP Domestic 15.411¢ grabbed as FAM). Reject.
RIDER_FAM_SANITY_MAX_CENTS = 2.0

_RIDER_DOC_HINT_RE = re.compile(
    r"\b(?:fam|dsm|dcrr|scrr|storm\s*rider|fuel\s*adjust(?:ment)?|"
    r"fuel\s*cost|power\s*cost|schedule\s*1\d{2}|adjustment\s+schedule|"
    r"cost\s*recovery|rate\s*rider|"
    r"not\s+shown\s+on\s+this\s+page|see\s+(?:schedule|rider|appendix)|"
    r"subject\s+to\s+.{0,60}(?:rider|adjust)|"
    r"in\s+addition\s+to\s+the\s+energy)\b",
    re.IGNORECASE,
)

# Base delivery/T&D of another numbered rate schedule (PGE Sch 32) must never
# become a stacking rider on residential plans. True adjustment schedules
# (PGE 1xx) and named riders (FAM/DCRR) are exempt via donor-name checks.
_BASE_DELIVERY_CHARGE_RE = re.compile(
    r"\b(?:transmission|distribution|delivery|wheeling|daily[\s-]*price|"
    r"t\s*&\s*d)\b",
    re.IGNORECASE,
)
_SCHEDULE_NUMBER_RE = re.compile(
    r"\bschedule\s*(?:no\.?\s*)?(\d{1,3})[a-z]?\b",
    re.IGNORECASE,
)
# Rider-doc staleness: reject filings whose year is clearly older than this.
RIDER_DOC_MAX_AGE_YEARS = 2
_RIDER_DOC_YEAR_RE = re.compile(r"(?<![A-Za-z0-9])((?:19|20)\d{2})(?![A-Za-z0-9])")

# Class / schedule tokens used when sharing class-specific rider amounts
# (NSP Domestic vs Small General vs General DCRR).
_RESIDENTIAL_CLASS_TOKEN_RE = re.compile(
    r"\b(?:residential|domestic|homeowner|homes?|dwellings?|"
    r"farm[\s-]*and[\s-]*home|single[\s-]*family)\b",
    re.IGNORECASE,
)
_COMMERCIAL_CLASS_TOKEN_RE = re.compile(
    r"\b(?:commercial|industrial|business|small\s+general|medium\s+general|"
    r"large\s+general|general\s+service)\b",
    re.IGNORECASE,
)
_RIDER_FAMILY_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("fam", re.compile(r"\bfam\b|fuel\s*adjust|fuel\s*cost", re.I)),
    ("dcrr", re.compile(r"\bdcrr\b|demand[\s-]*side|\bdsm\b|efficiency", re.I)),
    ("scrr", re.compile(r"\bscrr\b|storm", re.I)),
    ("pca", re.compile(r"\bpca\b|power\s*cost|purchased\s*power", re.I)),
    ("bac", re.compile(r"\bbac\b|balance\s*adjust|actual\s*adjust|aa/?ba", re.I)),
    ("tcos", re.compile(r"\btcos\b", re.I)),
    ("delivery", re.compile(r"\bdelivery\b", re.I)),
    ("transmission", re.compile(r"\btransmission\b", re.I)),
    ("distribution", re.compile(r"\bdistribution\b", re.I)),
    ("wheeling", re.compile(r"\bwheeling\b|daily[\s-]*price", re.I)),
    ("cost_recovery", re.compile(r"cost\s*recovery|rate\s*rider|energy\s*rider", re.I)),
)
_SUPERSEDED_CHARGE_LABEL_RE = re.compile(
    r"\b(?:old|prior|previous|former|superseded|outdated|expired|replaced|"
    r"legacy|historic(?:al)?)\b",
    re.IGNORECASE,
)


def _adjustment_label_blob(comp: dict, *, tariff_name: str = "") -> str:
    return " ".join(
        [
            tariff_name,
            str(comp.get("tier_label") or ""),
            str(comp.get("period_label") or ""),
            str(comp.get("season") or ""),
        ]
    )


def _schedule_number(name: str) -> int | None:
    m = _SCHEDULE_NUMBER_RE.search(str(name or ""))
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def _is_adjustment_schedule_name(name: str) -> bool:
    """True for rider/adjustment schedules (FAM, DCRR, PGE Schedule 1xx)."""
    n = str(name or "")
    if _RIDER_DONOR_NAME_RE.search(n):
        return True
    num = _schedule_number(n)
    if num is not None and 100 <= num <= 199:
        return True
    return False


def _looks_like_base_rate_schedule(name: str) -> bool:
    """True for base rate schedules (PGE Sch 7 / 32) — not adjustment docs."""
    n = str(name or "")
    if _is_adjustment_schedule_name(n):
        return False
    num = _schedule_number(n)
    return num is not None


def _is_base_schedule_delivery_charge(comp: dict, *, tariff_name: str = "") -> bool:
    """Base T&D/delivery of another rate schedule must not become a rider."""
    if not _looks_like_base_rate_schedule(tariff_name):
        return False
    blob = _adjustment_label_blob(comp, tariff_name=tariff_name)
    return bool(_BASE_DELIVERY_CHARGE_RE.search(blob))


def _rider_family_key(comp: dict, *, tariff_name: str = "") -> str:
    """Normalize FAM / DCRR / TCOS / … so one value per family is shared.

    Prefer the component's own labels so a combined "FAM and DSM Rider"
    donor page still yields two families. Fall back to ``tariff_name`` only
    when the row itself is unlabeled.
    """
    label_blob = " ".join(
        str(comp.get(k) or "") for k in ("tier_label", "period_label", "season")
    ).lower()
    blob = label_blob.strip() or str(tariff_name or "").lower()
    for fam, pat in _RIDER_FAMILY_PATTERNS:
        if pat.search(blob):
            return fam
    label = re.sub(r"[^a-z0-9]+", " ", blob).strip()
    if label:
        return " ".join(label.split()[:4])
    try:
        return f"val:{round(float(comp.get('rate_value') or 0), 6)}"
    except (TypeError, ValueError):
        return "val:unknown"


def _class_tokens_from_text(text: str) -> set[str]:
    """Named customer-class tokens in a rider/donor/recipient label."""
    blob = str(text or "").lower()
    tokens: set[str] = set()
    if re.search(r"\bdomestic\b", blob):
        tokens.add("domestic")
    if re.search(r"\bmurb\b|multi[\s-]*unit\s+residential|residential\s+building", blob):
        tokens.add("murb")
        tokens.add("residential")
    if re.search(r"\bresidential\b", blob):
        tokens.add("residential")
    if re.search(r"\bsmall\s+general\b", blob):
        tokens.add("small_general")
    elif re.search(r"\bgeneral\s+service\b", blob) or (
        re.search(r"\bgeneral\b", blob)
        and "residential" not in blob
        and "domestic" not in blob
        and "murb" not in blob
    ):
        tokens.add("general")
    if re.search(r"\bcommercial\b", blob):
        tokens.add("commercial")
    if re.search(r"\bindustrial\b", blob):
        tokens.add("industrial")
    if _RESIDENTIAL_CLASS_TOKEN_RE.search(blob) and "residential" not in tokens:
        tokens.add("residential")
    return tokens


def _recipient_class_tokens(t: ExtractedTariff) -> set[str]:
    blob = f"{t.customer_class or ''} {t.name or ''} {t.code or ''} {t.description or ''}"
    tokens = _class_tokens_from_text(blob)
    if str(t.customer_class or "").lower() == "residential":
        tokens.update({"residential", "domestic"})
    # MURB plans must not fall through to the Domestic FAM amount (R10).
    if "murb" in tokens:
        tokens.discard("domestic")
    return tokens


def _rider_named_classes(comp: dict, *, tariff_name: str = "") -> set[str]:
    return _class_tokens_from_text(_adjustment_label_blob(comp, tariff_name=tariff_name))


def _is_sch125_default_flat_rider(comp: dict, *, tariff_name: str = "") -> bool:
    """True for PGE Sch 125's default-plan flat ¢/kWh (not 7-TOD periods)."""
    blob = _adjustment_label_blob(comp, tariff_name=tariff_name)
    if not re.search(r"schedule\s*125|\bsch(?:edule)?\s*125\b|\b125\b", blob, re.I):
        return False
    if re.search(
        r"\b(?:tod|7[\s-]*tod|(?:on|mid|off)[\s-]*peak)\b",
        blob,
        re.I,
    ):
        return False
    return True


def _is_sch125_tod_period_rider(comp: dict, *, tariff_name: str = "") -> bool:
    """True for PGE Sch 125 7-TOD on/mid/off period adjustment rows."""
    blob = _adjustment_label_blob(comp, tariff_name=tariff_name)
    if not re.search(r"schedule\s*125|\bsch(?:edule)?\s*125\b|\b125\b", blob, re.I):
        return False
    return bool(
        re.search(
            r"\b(?:tod|7[\s-]*tod|(?:on|mid|off)[\s-]*peak)\b",
            blob,
            re.I,
        )
    )


def _rider_applies_to_recipient(
    adj: dict,
    donor: ExtractedTariff,
    recipient: ExtractedTariff,
) -> bool:
    """True when a donor ADJUSTMENT may be shared onto ``recipient``.

    Residential plans only receive residential (or class-matched) riders.
    Class-specific amounts (NSP Domestic/Small General/General DCRR) must
    name the recipient's class — never stack every class onto every plan.
    """
    donor_cc = str(donor.customer_class or "").strip().lower()
    recip_cc = str(recipient.customer_class or "").strip().lower()
    recip_rt = str(recipient.rate_type or "").strip().lower()
    # Sch 125 default flat → flat/tiered Sch 7 only; TOD/EV use 7-TOD rows.
    if _is_sch125_default_flat_rider(adj, tariff_name=str(donor.name or "")):
        if recip_rt in ("tou", "tou_tiered", "seasonal_tou", "demand_tou"):
            return False
    # Sch 125 TOD period rows are applied only by apply_tod, never shared.
    if _is_sch125_tod_period_rider(adj, tariff_name=str(donor.name or "")):
        return False
    if recip_cc == "residential" and donor_cc and donor_cc not in ("", "residential"):
        # Commercial/industrial donor extracts never share onto residential
        # unless the rider label explicitly names residential/domestic.
        named = _rider_named_classes(adj, tariff_name=str(donor.name or ""))
        if not (named & {"residential", "domestic", "murb"}):
            return False

    named = _rider_named_classes(adj, tariff_name=str(donor.name or ""))
    recip_tokens = _recipient_class_tokens(recipient)
    if named:
        commercial = named & {"small_general", "general", "commercial", "industrial"}
        residential = named & {"residential", "domestic", "murb"}
        # MURB ↔ Domestic FAM amounts differ (0.207 vs 0.156) — never cross.
        if "murb" in named and "murb" not in recip_tokens:
            return False
        if "murb" in recip_tokens and "murb" not in named and "domestic" in named:
            return False
        if "murb" in named and "murb" in recip_tokens:
            return True
        if commercial and not residential and recip_cc == "residential":
            return False
        if residential and recip_cc == "residential":
            return True
        return bool(named & recip_tokens)

    # No class in the rider label — donor must itself be residential (or
    # match the recipient) and must not look commercial-only by name.
    donor_name = str(donor.name or "")
    if _COMMERCIAL_CLASS_TOKEN_RE.search(donor_name) and not _RESIDENTIAL_CLASS_TOKEN_RE.search(
        donor_name
    ):
        return False
    if donor_cc in ("", "residential") and recip_cc == "residential":
        return True
    return bool(donor_cc) and donor_cc == recip_cc


def _is_universal_stacking_rider(
    comp: dict,
    *,
    tariff_name: str = "",
    allow_tod: bool = False,
    allow_already_included: bool = False,
) -> bool:
    """True when a per-kWh ADJUSTMENT applies to every customer on the plan.

    Excludes optional-enrollment credits (SmartRate), source-specific charges
    (community solar), time-of-day overlays unless ``allow_tod``, and base
    transmission/distribution charges copied from another rate schedule.
    Rider-only donors often mark DCRR/FAM as ``included_in_energy`` to mean
    "fold into ENERGY" even though the donor itself has no ENERGY — pass
    ``allow_already_included`` when collecting from those donors (R8).
    """
    if not isinstance(comp, dict):
        return False
    if str(comp.get("component_type") or "").strip().lower() != "adjustment":
        return False
    if not _is_energy_unit(comp.get("unit")):
        return False
    if comp.get("included_in_energy") and not allow_already_included:
        return False
    if _season_key(comp.get("season")):
        return False
    blob = _adjustment_label_blob(comp, tariff_name=tariff_name)
    if _OPTIONAL_OR_SCOPED_RIDER_RE.search(blob):
        return False
    # Real TOD *period* overlays (PGE Sch 125 "7-TOD On-Peak") never share
    # as flat stacking riders — only ``apply_tod_schedule_riders`` may apply
    # them. NSP DCRR labels list "Time-of-Day" as class applicability (no
    # on/mid/off period) and still stack via the identity exemption (R11).
    _STACKING_IDENTITY = re.compile(
        r"\b(?:fam|dsm|dcrr|scrr|storm\s*cost|fuel\s*adjust|power\s*cost|\bpca\b|\bbac\b)\b",
        re.IGNORECASE,
    )
    _TOD_PERIOD_ROW = re.compile(
        r"\b(?:on|mid|off)[\s-]*peak\b|\b7[\s-]*tod\b|"
        r"tod\s+adjustment|time[\s-]*of[\s-]*day\s+period",
        re.IGNORECASE,
    )
    if not allow_tod and _TOD_PERIOD_ROW.search(blob):
        return False
    if (
        not allow_tod
        and _TOD_OVERLAY_RIDER_RE.search(blob)
        and not _STACKING_IDENTITY.search(blob)
    ):
        return False
    if _is_base_schedule_delivery_charge(comp, tariff_name=tariff_name):
        return False
    # Sch 125 default-plan flat row must not stack onto TOD/EV plans (R11).
    # Those plans take only the 7-TOD period amounts via apply_tod.
    if _is_sch125_default_flat_rider(comp, tariff_name=tariff_name):
        # Still a valid stacking rider for flat/tiered recipients; caller
        # filters by recipient rate_type in ``_rider_applies_to_recipient``.
        pass
    # Tier-block differentials ("First 1,000 kWh block adjustment") are not
    # universal riders — never share them onto another plan (R8 PGE Sch 7).
    # Exception: PGE Sched_1xx First/Over *rate blocks* (Sch 102) ARE the
    # priced adjustment schedule itself (R8b).
    if re.search(
        r"\b(?:first|next|over|above|block)\b.*\b(?:kwh|tier|block)\b|"
        r"\btier\s*(?:1|2|3|differential|block)\b",
        blob,
        re.I,
    ):
        if not re.search(r"schedule\s*1\d{2}|sch(?:edule)?\s*1\d{2}", blob, re.I):
            return False
    label = " ".join(
        str(comp.get(k) or "") for k in ("tier_label", "period_label", "season")
    )
    try:
        rv = abs(float(comp.get("rate_value") or 0))
    except (TypeError, ValueError):
        return False
    # Sch 1xx First/Over block zeros (Sch 102 Over 0.000) must still share
    # so higher tiers do not inherit the First credit (R10).
    is_sch1xx_block = bool(
        re.search(r"schedule\s*1\d{2}|sch(?:edule)?\s*1\d{2}", label, re.I)
        and re.search(r"\b(?:first|over)\b", label, re.I)
    )
    if abs(rv) < 1e-12 and not is_sch1xx_block:
        return False
    if label.strip() and not _STACKING_RIDER_LABEL_RE.search(label):
        if rv > 0.12:
            return False
    elif not label.strip() and rv > 0.12:
        return False
    return True


def _energy_already_includes_stacking_riders(energy_row: dict) -> bool:
    """Trust an all-in ENERGY label only when it claims riders were folded.

    PGE often labels energy+transmission+distribution as "all-in" while
    still omitting power-cost riders — those must still fold. Abbreviated
    delivery breakdowns (``all-in: 0.397 + 7.601 + 7.051``) are the same
    delivery-only claim without the words transmission/distribution.

    ``all-in +Sch125`` (from apply_tod) means only Sch 125 TOD is folded —
    flat Sched_1xx must still stack (R11).
    """
    label = " ".join(
        str(energy_row.get(k) or "") for k in ("tier_label", "period_label")
    )
    if not _ALL_IN_LABEL_RE.search(label):
        return False
    # Sch 125 TOD-only annotation — not a claim that all riders folded.
    if re.search(r"all[\s-]*in\s*\+\s*sch\s*125", label, re.I):
        return False
    if _ALL_IN_RIDERS_RE.search(label):
        return True
    if _ALL_IN_DELIVERY_RE.search(label):
        return False
    if _ALL_IN_ABBREV_DELIVERY_RE.search(label):
        return False
    # Bare "all-in" with no delivery/riders claim is ambiguous (PGE often
    # means energy+T&D only). Do not skip Sch 1xx / FAM stacking (R12).
    return False


def _adjustment_rate_cents(comp: dict) -> float | None:
    """Return an ADJUSTMENT's magnitude in ¢/kWh, or None if not energy-unit."""
    if not isinstance(comp, dict) or not _is_energy_unit(comp.get("unit")):
        return None
    try:
        v = float(comp.get("rate_value") or 0)
    except (TypeError, ValueError):
        return None
    unit = str(comp.get("unit") or "").strip().lower().replace(" ", "")
    if unit.startswith("$"):
        return v * 100.0
    return v


def _rider_fails_sanity(
    adj: dict,
    *,
    tariff_name: str = "",
    base_energy_cents: float | None = None,
) -> str | None:
    """Reject misparsed / absurd per-kWh riders before they fold into ENERGY.

    - FAM / fuel-adjustment above ``RIDER_FAM_SANITY_MAX_CENTS`` (≈2¢) is
      almost certainly a base energy rate grabbed as FAM (R10).
    - Any rider larger than the recipient's base ENERGY rate is absurd and
      must not silently inflate the bill.
    """
    cents = _adjustment_rate_cents(adj)
    if cents is None:
        return None
    mag = abs(cents)
    blob = _adjustment_label_blob(adj, tariff_name=tariff_name).lower()
    is_fam = bool(re.search(r"\bfam\b|fuel\s*adjust|aa/?ba", blob, re.I))
    if is_fam and mag > RIDER_FAM_SANITY_MAX_CENTS + 1e-9:
        return f"fam_exceeds_sanity_cap:{mag:.3f}c"
    if base_energy_cents is not None and mag > abs(base_energy_cents) + 1e-9:
        return f"rider_exceeds_base_energy:{mag:.3f}c>{abs(base_energy_cents):.3f}c"
    return None


def _base_energy_cents_for_sanity(t: ExtractedTariff) -> float | None:
    """Largest |ENERGY| rate in ¢/kWh on ``t`` (for rider-vs-base sanity)."""
    best = None
    for c in t.components or []:
        if not isinstance(c, dict):
            continue
        if str(c.get("component_type") or "").lower() != "energy":
            continue
        cents = _adjustment_rate_cents(c)  # same unit logic
        if cents is None:
            continue
        mag = abs(cents)
        if best is None or mag > best:
            best = mag
    return best


def expand_stacking_energy_riders(
    components: list[dict],
    *,
    keep_adjustments: bool = True,
    rate_type: str = "",
) -> list[dict]:
    """Fold flat (unseasoned) ¢/kWh ADJUSTMENT riders into all-in ENERGY.

    Nova Scotia Power (and similar) publish base ENERGY plus FAM / DSM /
    Storm riders that apply "in addition to the energy charge". Flux and
    Lookup only render ENERGY, so base-only ENERGY understates the bill.

    When ≥1 unseasoned energy-unit ADJUSTMENT is present alongside ENERGY
    rows, add the sum of those adjustments to **every** ENERGY
    ``rate_value`` (preserving tier bounds / TOU clocks — R7 PGE Sch 7).
    Seasonal ADJUSTMENTs are left for ``expand_relative_seasonal_energy``.
    Optional / source-specific / TOD-overlay rows are never folded into
    flat or tiered ENERGY. Retained ADJUSTMENT rows are flagged
    ``included_in_energy``. Idempotent: already-flagged riders are not
    folded again.
    """
    if not components:
        return components

    rt = str(rate_type or "").strip().lower()
    allow_tod = rt in ("tou", "tou_tiered", "seasonal_tou", "demand_tou")

    energy_rows: list[dict] = []
    stacking_adjs: list[dict] = []
    other: list[dict] = []

    for comp in components:
        if not isinstance(comp, dict):
            other.append(comp)
            continue
        ctype = str(comp.get("component_type") or "").strip().lower()
        if ctype == "energy" and _is_energy_unit(comp.get("unit")):
            energy_rows.append(comp)
        elif ctype == "adjustment" and _is_energy_unit(comp.get("unit")):
            # Sch 125 TOD period rows are folded only by apply_tod; the
            # default-plan flat row must never fold onto TOU/EV (R11).
            if allow_tod and (
                _is_sch125_tod_period_rider(comp)
                or _is_sch125_default_flat_rider(comp)
            ):
                other.append(comp)
            elif _is_universal_stacking_rider(comp, allow_tod=allow_tod):
                stacking_adjs.append(comp)
            else:
                other.append(comp)
        else:
            other.append(comp)

    if not energy_rows or not stacking_adjs:
        return components

    # Split First/Over block riders (PGE Sch 102) from flat stacking riders.
    # Map First → lowest ENERGY tier / lowest rate, Over → remaining tiers so
    # Sch 7's 19.55 / 20.67 shape is preserved.
    first_over: list[dict] = []
    flat_adjs: list[dict] = []
    for a in stacking_adjs:
        lab = str(a.get("tier_label") or "").lower()
        if re.search(r"\bfirst\b", lab) or re.search(r"\bover\b", lab):
            first_over.append(a)
        else:
            flat_adjs.append(a)

    # Sanity: drop absurd FAM / oversized riders before they fold (R10).
    # Compare against the largest ENERGY magnitude on this tariff.
    base_cents = None
    for e in energy_rows:
        c = _adjustment_rate_cents(e)
        if c is None:
            continue
        mag = abs(c)
        if base_cents is None or mag > base_cents:
            base_cents = mag
    sane_flat: list[dict] = []
    sane_first_over: list[dict] = []
    for a in flat_adjs:
        if _rider_fails_sanity(a, base_energy_cents=base_cents):
            continue
        sane_flat.append(a)
    for a in first_over:
        if _rider_fails_sanity(a, base_energy_cents=base_cents):
            continue
        sane_first_over.append(a)
    # Keep rejected rows in `other` (unfolder) so audit trail remains; they
    # are not marked included_in_energy.
    rejected = [a for a in stacking_adjs if a not in sane_flat and a not in sane_first_over]
    flat_adjs = sane_flat
    first_over = sane_first_over
    stacking_adjs = flat_adjs + first_over

    try:
        flat_sum = sum(float(a.get("rate_value") or 0) for a in flat_adjs)
    except (TypeError, ValueError):
        return components

    first_val = None
    over_val = None
    first_bound_kwh: float | None = None
    for a in first_over:
        lab = str(a.get("tier_label") or "")
        lab_l = lab.lower()
        try:
            v = float(a.get("rate_value") or 0)
        except (TypeError, ValueError):
            continue
        if re.search(r"\bfirst\b", lab_l):
            first_val = v
            bm = re.search(r"first\s+([\d,]+)\s*kwh", lab_l)
            if bm:
                try:
                    first_bound_kwh = float(bm.group(1).replace(",", ""))
                except ValueError:
                    first_bound_kwh = None
        elif re.search(r"\bover\b", lab_l):
            over_val = v
            if first_bound_kwh is None:
                bm = re.search(r"over\s+([\d,]+)\s*kwh", lab_l)
                if bm:
                    try:
                        first_bound_kwh = float(bm.group(1).replace(",", ""))
                    except ValueError:
                        first_bound_kwh = None
    if abs(flat_sum) < 1e-12 and first_val is None and over_val is None:
        if rejected:
            # Still return components with rejected riders left unfolded.
            return components
        return components

    has_tiers = any(
        e.get("tier_min_kwh") is not None or e.get("tier_max_kwh") is not None
        for e in energy_rows
    )

    def _split_energy_at_rider_bound(
        rows: list[dict], bound: float,
    ) -> list[dict]:
        """Split ENERGY tiers that cross Sch 102's own kWh break (R11).

        Sch 7 may break at 1,000 kWh while Sch 102 credits the first 2,000 —
        a single "Over 1,000" row must become 1,000–2,000 (credit) + 2,000+
        (no credit).
        """
        out_rows: list[dict] = []
        for e in rows:
            try:
                tmin = e.get("tier_min_kwh")
                tmax = e.get("tier_max_kwh")
                tmin_f = 0.0 if tmin is None or tmin == "" else float(tmin)
                tmax_f = None if tmax is None or tmax == "" else float(tmax)
            except (TypeError, ValueError):
                out_rows.append(e)
                continue
            # Crosses the rider bound: tmin < bound < tmax (or open top).
            crosses = tmin_f < bound - 1e-9 and (
                tmax_f is None or tmax_f > bound + 1e-9
            )
            if not crosses:
                out_rows.append(e)
                continue
            low = dict(e)
            low["tier_min_kwh"] = tmin_f
            low["tier_max_kwh"] = bound
            note = (low.get("tier_label") or "").strip()
            low["tier_label"] = (
                f"{note} (to {int(bound):,} kWh)".strip()
                if note else f"First {int(bound):,} kWh"
            )
            high = dict(e)
            high["tier_min_kwh"] = bound
            high["tier_max_kwh"] = tmax_f
            high["tier_label"] = (
                f"{note} (over {int(bound):,} kWh)".strip()
                if note else f"Over {int(bound):,} kWh"
            )
            out_rows.append(low)
            out_rows.append(high)
        return out_rows

    work_energy = list(energy_rows)
    # Split at Sch 102's kWh break when First/Over riders are present —
    # including a flat (unbound) Default that must become First/Over tiers
    # (R12: current Sch 7 is flat 11.289¢; Sch 102 still credits first 2,000).
    if first_bound_kwh is not None and (
        first_val is not None or over_val is not None
    ):
        if has_tiers:
            work_energy = _split_energy_at_rider_bound(work_energy, first_bound_kwh)
        elif len(work_energy) == 1 and not allow_tod:
            # Single unbound ENERGY (flat Default) → First/Over at bound.
            seed = dict(work_energy[0])
            seed["tier_min_kwh"] = 0.0
            seed["tier_max_kwh"] = None
            work_energy = _split_energy_at_rider_bound([seed], first_bound_kwh)
            has_tiers = True

    new_energy: list[dict] = []
    for e in work_energy:
        row = dict(e)
        # Always preserve tier bounds / period clocks when folding riders.
        for k in (
            "tier_min_kwh", "tier_max_kwh", "tier_label",
            "period_start_time", "period_end_time", "period_label",
            "day_type", "season",
            "season_start_month", "season_start_day",
            "season_end_month", "season_end_day",
        ):
            if k in e:
                row[k] = e.get(k)
        if _energy_already_includes_stacking_riders(e):
            new_energy.append(row)
            continue
        try:
            base = float(e.get("rate_value") or 0)
        except (TypeError, ValueError):
            new_energy.append(row)
            continue
        add = flat_sum
        if first_val is not None or over_val is not None:
            if has_tiers and first_bound_kwh is not None:
                # Apply First credit only while tier_max <= bound (or tier
                # ends at the bound). Over-bound tiers get over_val (often 0).
                try:
                    tmax = e.get("tier_max_kwh")
                    tmin = e.get("tier_min_kwh")
                    tmax_f = None if tmax is None or tmax == "" else float(tmax)
                    tmin_f = 0.0 if tmin is None or tmin == "" else float(tmin)
                except (TypeError, ValueError):
                    tmax_f, tmin_f = None, 0.0
                if tmax_f is not None and tmax_f <= first_bound_kwh + 1e-9:
                    add += float(first_val or 0.0)
                elif tmin_f >= first_bound_kwh - 1e-9:
                    add += float(over_val or 0.0)
                else:
                    # Unsplit residual — prefer first if mostly below bound.
                    add += float(first_val or 0.0)
            elif has_tiers:
                # No explicit bound — bottom tier gets First (legacy).
                try:
                    tmin = e.get("tier_min_kwh")
                    tmin_f = 0.0 if tmin is None or tmin == "" else float(tmin)
                except (TypeError, ValueError):
                    tmin_f = 0.0
                mins = []
                for ee in work_energy:
                    try:
                        tm = ee.get("tier_min_kwh")
                        mins.append(0.0 if tm is None or tm == "" else float(tm))
                    except (TypeError, ValueError):
                        mins.append(0.0)
                bottom = min(mins) if mins else 0.0
                if abs(tmin_f - bottom) < 1e-9:
                    add += float(first_val or 0.0)
                else:
                    add += float(over_val or 0.0)
            else:
                # Non-tiered TOU/EV: first-block amount on every period
                # (Sch 102 credit applies to the whole premise TOU usage).
                add += float(first_val or 0.0)
        row["rate_value"] = round(base + add, 6)
        # Annotate without erasing the tier identity (first-1,000 kWh, …).
        # Delivery-only "all-in: … = 11.289" must not keep the stale total
        # after riders fold (R14).
        note = (row.get("tier_label") or "").strip()
        if note and "all-in" not in note.lower():
            row["tier_label"] = f"{note} (all-in +riders)"
        elif not note:
            row["tier_label"] = "All-in (base + riders)"
        elif re.search(
            r"(?:transmission|distribution).*energy|\ball-in\b.*=\s*\d",
            note,
            re.I,
        ):
            # Drop stale "= 11.289" even when split appended "(to 2,000 kWh)".
            cleaned = re.sub(r"\s*=\s*\d+(?:\.\d+)?", "", note).strip()
            cleaned = re.sub(r"\s{2,}", " ", cleaned)
            if "riders" not in cleaned.lower():
                cleaned = f"{cleaned} +riders"
            row["tier_label"] = cleaned
        elif "+riders" not in note.lower() and "+sch" not in note.lower():
            row["tier_label"] = f"{note} +riders"
        new_energy.append(row)

    out: list[dict] = []
    out.extend(other)
    # Sanity-rejected: keep unfolded for audit; never mark included_in_energy.
    out.extend({**a, "sanity_rejected": True} for a in rejected)
    if keep_adjustments:
        out.extend({**a, "included_in_energy": True} for a in stacking_adjs)
    out.extend(new_energy)
    return out


def _component_dedupe_key(comp: dict) -> tuple:
    """Match key for collapsing near-duplicate rate components.

    Structural identity (clock window, day type, season dates, tier bounds)
    is part of the key: equal-priced windows such as off-peak night and
    weekend all-day are distinct rows, and collapsing them leaves TOU gaps
    that make the tariff non-computable.
    """
    try:
        rv = round(float(comp.get("rate_value", 0) or 0), 6)
    except (TypeError, ValueError):
        rv = 0.0
    unit = str(comp.get("unit") or "").strip().lower()
    season = str(comp.get("season") or "").strip().lower()
    label = _normalize_component_label(comp)
    s = _structured_component_fields(comp)

    def _t(v):
        return v.strftime("%H:%M") if v is not None else None

    def _n(v):
        try:
            return None if v is None or v == "" else round(float(v), 3)
        except (TypeError, ValueError):
            return None

    return (
        label, unit, rv, season,
        _t(s["period_start_time"]), _t(s["period_end_time"]), s["day_type"],
        s["season_start_month"], s["season_start_day"],
        s["season_end_month"], s["season_end_day"],
        _n(comp.get("tier_min_kwh")), _n(comp.get("tier_max_kwh")),
    )


def dedupe_rate_components(components: list[dict]) -> list[dict]:
    """Collapse duplicate fixed/minimum rows and exact same-type duplicates.

    Newfoundland Power Rate #1.1 publishes amp-tier basic customer charges
    that equal the "minimum monthly charge" for the same amp band. LLMs
    often emit both as separate components; Flux then renders fixed||minimum
    with no UI dedupe. Prefer ``fixed`` when a minimum equals the basic
    charge for the same amp tier. Exact same-type duplicates are also
    collapsed. Energy/demand/adjustment rows are only collapsed when the
    full match key AND component_type are identical.
    """
    if not components:
        return components

    kept: list[dict] = []
    # key -> index in kept for fixed/minimum cross-type collapse
    fixed_min_index: dict[tuple, int] = {}
    # (type, key) -> index for exact same-type collapse
    exact_index: dict[tuple, int] = {}

    for comp in components:
        if not isinstance(comp, dict):
            kept.append(comp)
            continue
        ctype = str(comp.get("component_type") or "").strip().lower()
        key = _component_dedupe_key(comp)

        if ctype in ("fixed", "minimum"):
            prev_i = fixed_min_index.get(key)
            if prev_i is not None:
                prev = kept[prev_i]
                prev_type = str(prev.get("component_type") or "").lower()
                # Prefer fixed over minimum when values/labels match.
                if prev_type == "minimum" and ctype == "fixed":
                    kept[prev_i] = comp
                # else keep existing (already fixed, or same type)
                continue
            fixed_min_index[key] = len(kept)
            exact_index[(ctype, key)] = len(kept)
            kept.append(comp)
            continue

        exact_k = (ctype, key)
        if exact_k in exact_index:
            continue
        exact_index[exact_k] = len(kept)
        kept.append(comp)

    return kept


# TOU / seasonal shapes the computable contract can price. tou_tiered and
# demand_tou are non-computable by design, so they are not checked here.
_TOU_OR_SEASONAL_TYPES = frozenset({"tou", "seasonal_tou", "seasonal", "seasonal_tiered"})


def _tariff_comp_types(t: ExtractedTariff) -> set[str]:
    return {
        str(c.get("component_type") or "").strip().lower()
        for c in (t.components or [])
        if isinstance(c, dict)
    }


def _is_rider_only_tariff(t: ExtractedTariff) -> bool:
    """True when a tariff has only ADJUSTMENT(/minimum) rows — no core rate."""
    types = _tariff_comp_types(t)
    return bool(types) and not (types & {"energy", "fixed", "demand"})


def _energy_unit_adjustments(
    t: ExtractedTariff,
    *,
    seasonal: bool | None = None,
    include_already_folded: bool = False,
) -> list[dict]:
    out: list[dict] = []
    for c in t.components or []:
        if not isinstance(c, dict):
            continue
        if str(c.get("component_type") or "").strip().lower() != "adjustment":
            continue
        if not _is_energy_unit(c.get("unit")):
            continue
        if c.get("included_in_energy") and not include_already_folded:
            continue
        has_season = bool(_season_key(c.get("season")))
        if seasonal is True and not has_season:
            continue
        if seasonal is False and has_season:
            continue
        out.append(c)
    return out


def _rider_fingerprint(comp: dict, *, tariff_name: str = "") -> tuple:
    """Identity for deduping shared riders (value + unit + season [+ Sch 1xx]).

    Pedernales donated community-solar charges twice under slightly different
    labels; ignoring the decorative label collapses those duplicates. Distinct
    PGE Sched_1xx riders that happen to share a ¢/kWh (e.g. 122 and 126 both
    0.214) must remain distinct — key those by schedule number only (R8b).
    """
    try:
        rv = round(float(comp.get("rate_value") or 0), 6)
    except (TypeError, ValueError):
        rv = 0.0
    unit = str(comp.get("unit") or "").strip().lower().replace(" ", "")
    blob = _adjustment_label_blob(comp, tariff_name=tariff_name)
    sch = re.search(r"schedule\s*(1\d{2})", blob, re.I)
    sch_key = sch.group(1) if sch else ""
    return (rv, unit, _season_key(comp.get("season")), sch_key)


def _seasonal_sibling_base_code(code: str) -> str:
    """Map a seasonal/optional variant code onto its base schedule code.

    ``1.2DS`` → ``1.2D`` (strip trailing seasonal ``S`` only — not product
    letters). ``1.1S`` → ``1.1``. Empty when no seasonal suffix.
    """
    c = str(code or "").strip().lower()
    if not c:
        return ""
    m = re.match(r"^(.+?)s$", c)
    return m.group(1) if m else ""


def _first_block_energy_rows(components: list[dict]) -> list[dict]:
    """ENERGY rows at the open bottom tier (tier_min 0/None), if tiered.

    Prefer rows whose label says "first block" / "tier 1" — NL 1.2D prints
    first+second blocks both without tier_min, so a bare min==0 filter
    returns two different prices and blocks 1.2DS salvage (R8).
    """
    energy = [
        c
        for c in (components or [])
        if isinstance(c, dict)
        and str(c.get("component_type") or "").lower() == "energy"
        and _is_energy_unit(c.get("unit"))
    ]
    if not energy:
        return []

    labeled = [
        c for c in energy
        if re.search(
            r"\bfirst\s*(?:block|tier|kwh)\b|\btier\s*1\b|\bblock\s*1\b",
            " ".join(str(c.get(k) or "") for k in ("tier_label", "period_label")),
            re.I,
        )
    ]
    if labeled:
        return labeled

    def _tier_min(c: dict) -> float:
        try:
            v = c.get("tier_min_kwh")
            return 0.0 if v is None or v == "" else float(v)
        except (TypeError, ValueError):
            return 0.0

    mins = [_tier_min(c) for c in energy]
    bottom = min(mins) if mins else 0.0
    first = [c for c in energy if abs(_tier_min(c) - bottom) < 1e-9]
    # When several bottom-tier rows differ in price and lack "first" labels,
    # keep only the lowest rate (typical first-block).
    if len(first) > 1:
        try:
            vals = sorted({round(float(c.get("rate_value") or 0), 6) for c in first})
        except (TypeError, ValueError):
            vals = []
        if len(vals) > 1:
            lowest = vals[0]
            first = [
                c for c in first
                if abs(round(float(c.get("rate_value") or 0), 6) - lowest) < 1e-9
            ]
    return first or energy


def _pick_base_energy_from_sibling(other: ExtractedTariff) -> dict | None:
    """Choose a single base ENERGY rate from a sibling schedule.

    Prefer unseasoned first-block ENERGY. If every first-block seasonal row
    shares one value (NL 1.2D pattern), use that. Differing seasonal bases
    mean relative-adj salvage would misprice — return None.
    """
    first = _first_block_energy_rows(list(other.components or []))
    if not first:
        return None
    unseasoned = [e for e in first if not _season_key(e.get("season"))]
    pool = unseasoned or first
    try:
        vals = {round(float(e.get("rate_value") or 0), 6) for e in pool}
    except (TypeError, ValueError):
        return None
    if len(vals) != 1:
        return None
    base = dict(pool[0])
    base["season"] = None
    for k in _SEASON_DATE_KEYS:
        base[k] = None
    return base


def _find_sibling_base_energy(
    rider: ExtractedTariff,
    batch: list[ExtractedTariff],
) -> dict | None:
    """Locate a base ENERGY row for a relative seasonal rider-only extract.

    Requires the same rate-family code: ``1.2DS`` pairs with ``1.2D``, never
    ``1.2G``. Accepts seasonal or first-block ENERGY on that sibling.
    """
    rider_code = str(rider.code or "").strip().lower()
    rider_name = str(rider.name or "").strip().lower()
    rider_class = str(rider.customer_class or "").strip().lower()
    want_code = _seasonal_sibling_base_code(rider_code)

    candidates: list[tuple[int, dict, str]] = []
    for other in batch:
        if other is rider:
            continue
        if _is_rider_only_tariff(other):
            continue
        other_class = str(other.customer_class or "").strip().lower()
        if rider_class and other_class and rider_class != other_class:
            continue
        other_code = str(other.code or "").strip().lower()
        other_name = str(other.name or "").strip().lower()
        score = 0
        if want_code and other_code == want_code:
            # Exact family match: 1.2DS → 1.2D, 1.1S → 1.1.
            score += 5
        elif not want_code and rider_code and other_code and rider_code.startswith(other_code):
            # No seasonal suffix to strip — allow careful prefix match only.
            score += 2
        if score == 0:
            continue
        if want_code and other_code != want_code:
            # Never pair 1.2DS with 1.2G (shared "1.2" stem is not enough).
            continue
        base = _pick_base_energy_from_sibling(other)
        if base is None:
            continue
        candidates.append((score, base, other_code or other_name))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def salvage_relative_rider_only_tariffs(tariffs: list[ExtractedTariff]) -> int:
    """Inject sibling base ENERGY into seasonal rider-only extracts.

    Returns the number of rider-only tariffs salvaged (NL 1.1S / 1.2DS).
    """
    salvaged = 0
    for t in tariffs:
        if not _is_rider_only_tariff(t):
            continue
        seasonal_adjs = _energy_unit_adjustments(t, seasonal=True)
        seasons = {
            _season_key(a.get("season"))
            for a in seasonal_adjs
            if _season_key(a.get("season"))
        }
        if len(seasons) < 2:
            continue
        base = _find_sibling_base_energy(t, tariffs)
        if base is None:
            continue
        t.components = [base, *list(t.components)]
        if not t.rate_type or t.rate_type == "flat":
            t.rate_type = "seasonal"
        salvaged += 1
        log.info(
            f"    Salvaged rider-only '{t.name}': injected base ENERGY "
            f"{base.get('rate_value')} {base.get('unit')} from sibling"
        )
    return salvaged


def _donor_rider_candidates(tariffs: list[ExtractedTariff]) -> list[tuple[ExtractedTariff, dict]]:
    """(donor, adjustment) pairs eligible for cross-batch stacking share.

    R7: residential ENERGY plans that already carry FAM/DSM/etc. also donate
    those stacking ADJUSTMENTs so every residential plan gets them (NSP DSM
    must not stay only on a critical-peak sibling).
    """
    out: list[tuple[ExtractedTariff, dict]] = []
    for t in tariffs:
        if _is_optional_program_tariff(t):
            continue
        donor_name = str(t.name or "")
        if _OPTIONAL_PROGRAMME_NAME_RE.search(donor_name):
            continue
        if _TOD_OVERLAY_RIDER_RE.search(donor_name):
            continue
        classic = (
            _is_rider_only_tariff(t)
            or bool(_RIDER_DONOR_NAME_RE.search(donor_name))
            or _is_adjustment_schedule_name(donor_name)
        )
        # Rider-only donors mark FAM/DCRR included_in_energy to mean "fold
        # into ENERGY" even though they have no ENERGY row — still donate.
        allow_included = _is_rider_only_tariff(t)
        has_stacking = any(
            isinstance(c, dict)
            and _is_universal_stacking_rider(
                c,
                tariff_name=donor_name,
                allow_tod=False,
                allow_already_included=allow_included,
            )
            for c in (t.components or [])
        )
        residential_share = (
            str(t.customer_class or "").lower() == "residential" and has_stacking
        )
        if not classic and not residential_share:
            continue
        # Classic path: skip other schedules' base T&D (Sch 32) unless this
        # residential plan is sharing its own stacking riders.
        if classic and not residential_share and _looks_like_base_rate_schedule(donor_name):
            continue
        for adj in _energy_unit_adjustments(
            t, seasonal=False, include_already_folded=allow_included,
        ):
            if not _is_universal_stacking_rider(
                adj,
                tariff_name=donor_name,
                allow_tod=False,
                allow_already_included=allow_included,
            ):
                continue
            # Clear the flag on the copy so expand_stacking folds it into
            # the recipient's ENERGY.
            row = dict(adj)
            row["included_in_energy"] = False
            out.append((t, row))
    return out


def _pick_one_rider_per_family(
    candidates: list[tuple[ExtractedTariff, dict]],
    recipient: ExtractedTariff,
) -> list[dict]:
    """Keep at most one ADJUSTMENT per rider family for ``recipient``."""
    recip_tokens = _recipient_class_tokens(recipient)
    best: dict[str, tuple[int, ExtractedTariff, dict]] = {}
    for donor, adj in candidates:
        if not _rider_applies_to_recipient(adj, donor, recipient):
            continue
        fam = _rider_family_key(adj, tariff_name=str(donor.name or ""))
        named = _rider_named_classes(adj, tariff_name=str(donor.name or ""))
        # Prefer class-matched label, then residential donor, then any.
        score = 0
        if named & recip_tokens:
            score += 30
        if named & {"residential", "domestic"} and str(
            recipient.customer_class or ""
        ).lower() == "residential":
            score += 20
        if str(donor.customer_class or "").lower() == "residential":
            score += 10
        if _is_adjustment_schedule_name(str(donor.name or "")):
            score += 5
        prev = best.get(fam)
        if prev is None or score > prev[0]:
            best[fam] = (score, donor, adj)
    return [dict(adj) for _score, _donor, adj in best.values()]


def apply_shared_stacking_riders_across_batch(tariffs: list[ExtractedTariff]) -> int:
    """Copy universal per-kWh riders from donor extracts onto ENERGY tariffs.

    Only riders that apply to the recipient's class are shared (residential
    donors, or riders whose label names the recipient's class/schedule).
    Never stack several values of the same rider family onto one plan —
    pick the class-matched amount (NSP Domestic DCRR, not Small General).
    Optional / community-solar / TOD overlays and base T&D of another
    schedule are excluded.
    """
    candidates = _donor_rider_candidates(tariffs)
    applied = 0
    if candidates:
        for t in tariffs:
            if _is_rider_only_tariff(t):
                continue
            energy_rows = [
                c
                for c in (t.components or [])
                if isinstance(c, dict)
                and str(c.get("component_type") or "").lower() == "energy"
                and _is_energy_unit(c.get("unit"))
            ]
            if not energy_rows:
                continue
            # Only skip when EVERY ENERGY row already claims riders folded.
            # Delivery-only "all-in" (incl. abbreviated tx+dist+energy sums)
            # must still receive Sch 1xx / FAM (R10 — TOU/EV were skipped).
            if energy_rows and all(
                _energy_already_includes_stacking_riders(e) for e in energy_rows
            ):
                continue
            existing_fps = {
                _rider_fingerprint(a, tariff_name=str(t.name or ""))
                for a in _energy_unit_adjustments(t)
            }
            existing_fams = {
                _rider_family_key(a, tariff_name=str(t.name or ""))
                for a in _energy_unit_adjustments(t)
            }
            picked = _pick_one_rider_per_family(candidates, t)
            base_cents = _base_energy_cents_for_sanity(t)
            to_add: list[dict] = []
            rejected_sanity = False
            for adj in picked:
                fam = _rider_family_key(adj)
                if fam in existing_fams:
                    continue
                fp = _rider_fingerprint(adj)
                if fp in existing_fps:
                    continue
                reason = _rider_fails_sanity(
                    adj, tariff_name=str(t.name or ""), base_energy_cents=base_cents,
                )
                if reason:
                    rejected_sanity = True
                    log.warning(
                        f"    Rider sanity reject on '{t.name}': {reason} "
                        f"({_adjustment_label_blob(adj)[:60]})"
                    )
                    continue
                to_add.append(adj)
                existing_fams.add(fam)
                existing_fps.add(fp)
            if rejected_sanity:
                t.needs_review = True
                missing = list(getattr(t, "missing_fields", None) or [])
                if "rider_sanity_reject" not in missing:
                    missing.append("rider_sanity_reject")
                t.missing_fields = missing
            if not to_add:
                continue
            t.components = list(t.components) + to_add
            applied += 1
            log.info(
                f"    Shared stacking riders → '{t.name}': "
                f"added {len(to_add)} ADJUSTMENT(s) from batch donors"
            )
    applied += apply_tod_schedule_riders_across_batch(tariffs)
    return applied


def apply_tod_schedule_riders_across_batch(tariffs: list[ExtractedTariff]) -> int:
    """Apply multi-value Schedule 1xx TOD adjustments per TOU period (R8/R11).

    PGE Sch 125 publishes separate on/mid/off amounts for Schedule 7-TOD.
    Match them to the recipient's ENERGY periods by sorted rate value
    (highest rider → highest period price) so Off-Peak is not left at the
    base-only 4.128¢.

    Never applies the default-plan flat Sch 125 row. Skips recipients that
    already carry Sch 125 TOD (dedupe against existing components / all-in
    labels) so a later expand pass cannot double-count (R11).
    """
    # Collect TOD schedule adjustments from rider-only / adjustment donors.
    tod_donors: list[tuple[ExtractedTariff, list[dict]]] = []
    for t in tariffs:
        adjs = []
        for c in t.components or []:
            if not isinstance(c, dict):
                continue
            if str(c.get("component_type") or "").lower() != "adjustment":
                continue
            if not _is_energy_unit(c.get("unit")):
                continue
            if not _is_sch125_tod_period_rider(c, tariff_name=str(t.name or "")):
                # Only Sch 125 7-TOD period rows (not default flat, not other).
                blob = _adjustment_label_blob(c, tariff_name=str(t.name or ""))
                if not (
                    re.search(r"\b(?:tod|time[\s-]*of[\s-]*day)\b", blob, re.I)
                    and re.search(r"schedule\s*1\d{2}|sch\s*1\d{2}|\b1\d{2}\b", blob, re.I)
                    and re.search(r"\b(?:on|mid|off)[\s-]*peak\b|7[\s-]*tod", blob, re.I)
                ):
                    continue
            try:
                if abs(float(c.get("rate_value") or 0)) < 1e-12:
                    continue
            except (TypeError, ValueError):
                continue
            adjs.append(dict(c))
        if len(adjs) >= 2:
            tod_donors.append((t, adjs))
    if not tod_donors:
        return 0

    applied = 0
    for t in tariffs:
        rt = str(t.rate_type or "").lower()
        if rt not in ("tou", "tou_tiered", "seasonal_tou", "demand_tou"):
            continue
        if _is_rider_only_tariff(t):
            continue
        # Already has Sch 125 TOD folded — do not apply again.
        already = False
        for c in t.components or []:
            if not isinstance(c, dict):
                continue
            blob = _adjustment_label_blob(c, tariff_name=str(t.name or ""))
            if c.get("included_in_energy") and re.search(
                r"schedule\s*125.*tod|tod\s+adjustment|all-in\s*\+sch\s*125",
                blob,
                re.I,
            ):
                already = True
                break
            pl = str(c.get("period_label") or c.get("tier_label") or "")
            if re.search(r"all-in\s*\+sch\s*125", pl, re.I):
                already = True
                break
        if already:
            continue
        energy = [
            c for c in (t.components or [])
            if isinstance(c, dict)
            and str(c.get("component_type") or "").lower() == "energy"
            and _is_energy_unit(c.get("unit"))
        ]
        if len(energy) < 2:
            continue
        donor_adjs = None
        for _d, adjs in tod_donors:
            donor_adjs = adjs
            break
        if not donor_adjs:
            continue
        period_prices = sorted({
            round(float(e.get("rate_value") or 0), 6) for e in energy
        })
        rider_prices = sorted({
            round(float(a.get("rate_value") or 0), 6) for a in donor_adjs
        })
        if len(period_prices) < 2 or len(rider_prices) < 2:
            continue
        n = min(len(period_prices), len(rider_prices))
        price_to_rider = {
            period_prices[i]: rider_prices[i] for i in range(n)
        }
        if len(period_prices) > len(rider_prices):
            for i, p in enumerate(period_prices):
                idx = min(i, len(rider_prices) - 1)
                price_to_rider[p] = rider_prices[idx]
        new_energy = []
        for e in energy:
            row = dict(e)
            try:
                base = round(float(e.get("rate_value") or 0), 6)
            except (TypeError, ValueError):
                new_energy.append(row)
                continue
            rider = price_to_rider.get(base)
            if rider is None:
                new_energy.append(row)
                continue
            row["rate_value"] = round(base + rider, 6)
            note = (row.get("period_label") or row.get("tier_label") or "").strip()
            if note and "all-in" not in note.lower():
                row["period_label"] = f"{note} (all-in +Sch125)"
            elif not note:
                row["period_label"] = "all-in +Sch125"
            new_energy.append(row)
        other = [
            c for c in (t.components or [])
            if not (
                isinstance(c, dict)
                and str(c.get("component_type") or "").lower() == "energy"
                and _is_energy_unit(c.get("unit"))
            )
        ]
        # Drop any stray Sch 125 TOD rows already on the recipient (would
        # double-fold in expand_stacking when allow_tod=True).
        cleaned = []
        for c in other:
            if isinstance(c, dict) and _is_sch125_tod_period_rider(
                c, tariff_name=str(t.name or "")
            ):
                continue
            if isinstance(c, dict) and _is_sch125_default_flat_rider(
                c, tariff_name=str(t.name or "")
            ):
                continue  # default flat never belongs on TOD/EV
            cleaned.append(c)
        other = cleaned
        for rp in rider_prices:
            other.append({
                "component_type": "adjustment",
                "unit": energy[0].get("unit") or "$/kWh",
                "rate_value": rp,
                "tier_label": "Schedule 125 TOD adjustment",
                "included_in_energy": True,
            })
        t.components = other + new_energy
        applied += 1
        log.info(
            f"    TOD schedule riders → '{t.name}': "
            f"applied {len(rider_prices)} Sch 1xx period amount(s)"
        )
    return applied


def _sch7_tou_or_ev_plan(t: ExtractedTariff) -> bool:
    """True for PGE Schedule 7 TOD / EV (or similarly named) TOU plans."""
    name = str(t.name or "")
    rt = str(t.rate_type or "").lower()
    if rt not in ("tou", "tou_tiered", "seasonal_tou", "demand_tou"):
        return False
    if re.search(
        r"schedule\s*7.*(?:time[\s-]*of[\s-]*use|time[\s-]*of[\s-]*day|tod|tou)|"
        r"(?:time[\s-]*of[\s-]*use|time[\s-]*of[\s-]*day|tod|tou).*schedule\s*7|"
        r"plug[\s-]*in\s+electric\s+vehicle|ev\s+time\s+of\s+use|"
        r"7[\s-]*tod",
        name,
        re.I,
    ):
        return True
    return bool(
        re.search(r"\bschedule\s*7\b", name, re.I)
        and (rt.startswith("tou") or rt == "seasonal_tou")
    )


def _tariff_has_sch125_tod_pca(t: ExtractedTariff) -> bool:
    """True when Sch 125 TOD period amounts are folded or present on ``t``."""
    for c in t.components or []:
        if not isinstance(c, dict):
            continue
        blob = _adjustment_label_blob(c, tariff_name=str(t.name or ""))
        pl = str(c.get("period_label") or c.get("tier_label") or "")
        if re.search(r"all-in\s*\+sch\s*125", pl, re.I):
            return True
        if _is_sch125_tod_period_rider(c, tariff_name=str(t.name or "")):
            return True
        if c.get("included_in_energy") and re.search(
            r"schedule\s*125.*(?:tod|on[\s-]*peak)|tod\s+adjustment",
            blob,
            re.I,
        ):
            return True
    return False


def flag_missing_sch125_tod_pca(tariffs: list[ExtractedTariff]) -> int:
    """Flag Sch 7 TOD/EV plans that never received Sch 125 period PCA (R13).

    When the PCA silently drops, all-in TOU prices are base+1xx only. Mark
    ``needs_review`` with a clear ``sch125_tod_pca_missing`` reason.
    """
    # Only flag when a Sch 125 TOD donor existed in the batch (otherwise the
    # gap is "rider not fetched", already covered by riders_referenced).
    has_donor = False
    for t in tariffs:
        for c in t.components or []:
            if isinstance(c, dict) and _is_sch125_tod_period_rider(
                c, tariff_name=str(t.name or "")
            ):
                has_donor = True
                break
        if has_donor:
            break
        if _tariff_has_sch125_tod_pca(t) and _is_rider_only_tariff(t):
            has_donor = True
            break
    flagged = 0
    for t in tariffs:
        if _is_rider_only_tariff(t):
            continue
        if not _sch7_tou_or_ev_plan(t):
            continue
        if _tariff_has_sch125_tod_pca(t):
            continue
        # Flag when a donor was present OR the plan is clearly Sch 7 TOD
        # (PCA is always supposed to apply — missing donor is also a gap).
        t.needs_review = True
        missing = list(getattr(t, "missing_fields", None) or [])
        if "sch125_tod_pca_missing" not in missing:
            missing.append("sch125_tod_pca_missing")
        t.missing_fields = missing
        flagged += 1
        log.warning(
            f"    Sch 125 TOD PCA missing on '{t.name}' — needs_review "
            f"(donor_in_batch={has_donor})"
        )
    return flagged


# Per-kWh energy-price changers (FAM / DCRR / PCA / Sch 1xx / storm…).
# Used to decide whether a declared rider should trigger
# ``referenced_riders_missing`` and whether a folded row counts as "has riders".
_PER_KWH_ENERGY_PRICE_RIDER_RE = re.compile(
    r"\b(?:fam|dsm|dcrr|scrr|"
    r"storm(?:\s+cost)?(?:\s+recovery)?(?:\s+rider)?|"
    r"fuel\s*adjust(?:ment)?(?:\s+mechanism)?|"
    r"power\s*cost(?:\s+adjust(?:ment)?)?|\bpca\b|\bbac\b|"
    r"schedule\s*1\d{2}|sch(?:edule)?\s*1\d{2})\b",
    re.IGNORECASE,
)

# Credits / discounts / net-metering / optional programmes — do NOT raise
# ``referenced_riders_missing`` outside PGE (HQ supply credits, SRP discounts).
_NON_ENERGY_PRICE_RIDER_HINT_RE = re.compile(
    r"\b(?:credit|discount|rebate|net[\s-]*meter(?:ing)?|"
    r"customer[\s-]*owned|transformer|"
    r"optional\s+program|green\s+(?:power|energy|future)|"
    r"attribute\s+certificate|export\s+credit|"
    r"medical\s+life\s+support|economy\s+discount|"
    r"carbon\s+reduction|"
    r"adjustment\s+for\s+transformation|"
    r"credit\s+for\s+supply)\b",
    re.IGNORECASE,
)


def _component_rider_label_blob(c: dict) -> str:
    """Labels on a component only — never the plan name (avoids 'Optional')."""
    return " ".join(
        str(c.get(k) or "")
        for k in ("tier_label", "period_label", "season")
    )


def _tariff_has_sch1xx_stacking_riders(t: ExtractedTariff) -> bool:
    """True when priced Sch 1xx / stacking riders are present or folded on ``t``.

    Counts any ADJUSTMENT already folded into the price
    (``included_in_energy=True``) or named FAM/DCRR/PCA/Sch 1xx — regardless
    of plan type. Do not pass the plan name into the stacking check: NSP
    ``…Time-Of-Day Tariff (Optional)`` would match ``optional`` and hide
    real FAM/DCRR rows (R14b).
    """
    if _tariff_has_sch125_tod_pca(t):
        return True
    for c in t.components or []:
        if not isinstance(c, dict):
            continue
        ctype = str(c.get("component_type") or "").lower()
        pl = str(c.get("period_label") or c.get("tier_label") or "")
        if ctype == "energy" and re.search(r"all-in\s*\+riders|\+riders", pl, re.I):
            return True
        if ctype != "adjustment":
            continue
        if not _is_energy_unit(c.get("unit")):
            continue
        # Already folded into the all-in price — counts for any plan type.
        if c.get("included_in_energy"):
            return True
        blob = _component_rider_label_blob(c)
        if _PER_KWH_ENERGY_PRICE_RIDER_RE.search(blob):
            return True
        # Universal stacking without plan-name poison (allow TOD / included).
        if _is_universal_stacking_rider(
            c, tariff_name="", allow_tod=True, allow_already_included=True,
        ):
            return True
    return False


def _is_per_kwh_energy_price_rider_hint(hint: str) -> bool:
    """True for Sch 1xx / FAM / DCRR / PCA / storm — not credits or discounts."""
    h = str(hint or "").strip()
    if not h:
        return False
    if _NON_ENERGY_PRICE_RIDER_HINT_RE.search(h) and not _PER_KWH_ENERGY_PRICE_RIDER_RE.search(
        h
    ):
        return False
    return bool(_PER_KWH_ENERGY_PRICE_RIDER_RE.search(h))


def _declared_per_kwh_price_riders(t: ExtractedTariff) -> list[str]:
    """``riders_referenced_not_shown`` entries that change per-kWh energy price."""
    return [
        str(h)
        for h in (getattr(t, "riders_referenced_not_shown", None) or [])
        if _is_per_kwh_energy_price_rider_hint(str(h))
    ]


def _plan_declares_external_adjustments(t: ExtractedTariff) -> bool:
    """True when the extract says per-kWh energy-price adjustments apply."""
    if _declared_per_kwh_price_riders(t):
        return True
    blob = " ".join(
        [
            _tariff_text_blob(t),
            str(getattr(t, "description", "") or ""),
        ]
    )
    # Explicit price-rider language only — not bare "subject to adjustments"
    # (HQ/SRP credits/discounts must not trip this).
    if _PER_KWH_ENERGY_PRICE_RIDER_RE.search(blob):
        return True
    return bool(
        re.search(
            r"see\s+schedule\s+1\d{2}\s+for\s+applicable\s+adjustments",
            blob,
            re.I,
        )
    )


def _is_pge_sch7_residential_plan(t: ExtractedTariff) -> bool:
    """True for PGE Schedule 7 Default / TOD residential ENERGY plans."""
    if str(t.customer_class or "").lower() != "residential":
        return False
    if _is_rider_only_tariff(t):
        return False
    name = str(t.name or "")
    return bool(
        re.search(
            r"schedule\s*7\b.*(?:residential|default|time[\s-]*of[\s-]*|"
            r"tod|tou|portfolio)|"
            r"(?:residential|default|time[\s-]*of[\s-]*|tod|tou).*schedule\s*7\b|"
            r"\b7[\s-]*tod\b",
            name,
            re.I,
        )
    )


def flag_plans_missing_referenced_riders(tariffs: list[ExtractedTariff]) -> int:
    """Flag ENERGY plans that declare per-kWh riders but received none (R14/R14b).

    PGE Sch 7 always expects Sch 1xx. Outside PGE, only Sch 1xx / FAM / DCRR /
    PCA / storm (etc.) count — credits, discounts, net metering and optional
    programmes do not raise ``referenced_riders_missing``.
    """
    flagged = 0
    for t in tariffs:
        if _is_rider_only_tariff(t):
            continue
        if str(t.customer_class or "").lower() != "residential":
            continue
        has_energy = any(
            isinstance(c, dict)
            and str(c.get("component_type") or "").lower() == "energy"
            and _is_energy_unit(c.get("unit"))
            for c in (t.components or [])
        )
        if not has_energy:
            continue
        is_pge = _is_pge_sch7_residential_plan(t)
        expects = is_pge or _plan_declares_external_adjustments(t)
        if not expects:
            continue
        if _tariff_has_sch1xx_stacking_riders(t):
            continue
        t.needs_review = True
        missing = list(getattr(t, "missing_fields", None) or [])
        reason = (
            "sch1xx_riders_missing" if is_pge else "referenced_riders_missing"
        )
        if reason not in missing:
            missing.append(reason)
        t.missing_fields = missing
        flagged += 1
        log.warning(
            f"    Referenced riders missing on '{t.name}' — needs_review "
            f"({reason})"
        )
    return flagged


def apply_source_quality_rules(tariffs: list[ExtractedTariff]) -> tuple[list[ExtractedTariff], dict]:
    """R21 fix 5: official tariff over retail offers / marketing pages.

    * a competitive retailer's contract offer (fixed-term, guaranteed-rate,
      electricity+gas bundle, offers / sign-up pages) is dropped — never
      stored as the utility's tariff;
    * a plan priced from a marketing page with every per-kWh price rounded
      to 0.1 cent is kept but marked ``price_basis='marketing_rounded'``,
      flagged, and not counted Mysa-complete (the official tariff wins).
    """
    from app.services.source_quality import is_retail_offer, marketing_rounded_price

    kept: list[ExtractedTariff] = []
    dropped: list[str] = []
    marketing = 0
    for t in tariffs:
        if is_retail_offer(t.name, t.source_url or "", getattr(t, "description", "") or ""):
            dropped.append(t.name)
            log.warning(f"    '{t.name}': retailer contract offer, not the utility tariff — dropped")
            continue
        if marketing_rounded_price(t.source_url or "", t.components):
            _r18_note(t, "price_basis", "marketing_rounded")
            _r18_add_missing(t, "marketing_page_rounded_price")
            t.needs_review = True
            marketing += 1
            log.warning(f"    '{t.name}': rounded prices from a marketing page — flagged, not Mysa-complete")
        kept.append(t)
    return kept, {"retail_offers_dropped": dropped, "marketing_rounded_plans": marketing}


def flag_wrong_jurisdiction(tariffs: list[ExtractedTariff], state: str) -> int:
    """R21 fix 7: the source URL names another state/province (and not the
    utility's own) — e.g. an Iowa utility priced from "sd-electric-tariffs.pdf".
    Flagged ``wrong_jurisdiction_document``, needs_review, not Mysa-complete.
    """
    from app.services.jurisdiction import url_jurisdictions, wrong_jurisdiction

    n = 0
    for t in tariffs:
        src = t.source_url or ""
        if not wrong_jurisdiction(src, state):
            continue
        _r18_add_missing(t, "wrong_jurisdiction_document")
        _r18_note(t, "wrong_jurisdiction", {
            "utility_state": str(state).upper(),
            "document_states": sorted(url_jurisdictions(src)),
        })
        t.needs_review = True
        n += 1
        log.warning(f"    '{t.name}': source is for {sorted(url_jurisdictions(src))}, utility is {state} — not Mysa-complete")
    return n


def mark_base_only_plans(tariffs: list[ExtractedTariff]) -> int:
    """R21 safety net: a plan whose source references per-kWh fuel / cost-
    recovery riders that are NOT in its ENERGY price is "base only".

    Marked ``price_basis='base_only'`` (+ ``riders_not_added``), flagged
    (``base_only_riders_not_added``), and never counted as Mysa-complete
    (see app.services.price_basis). Prices are never changed or guessed.
    """
    from app.services.price_basis import unadded_price_riders

    n = 0
    for t in tariffs:
        if _is_rider_only_tariff(t) or str(t.customer_class or "").lower() != "residential":
            continue
        if not any(isinstance(c, dict) and str(c.get("component_type") or "").lower() == "energy"
                   for c in t.components or []):
            continue
        riders = unadded_price_riders(
            riders_referenced=getattr(t, "riders_referenced_not_shown", None),
            missing_fields=getattr(t, "missing_fields", None),
            energy_includes_riders=getattr(t, "energy_includes_riders", None),
            components=t.components,
        )
        if not riders:
            continue
        _r18_note(t, "price_basis", "base_only")
        _r18_note(t, "riders_not_added", riders[:12])
        _r18_add_missing(t, "base_only_riders_not_added")
        t.needs_review = True
        n += 1
        log.warning(f"    '{t.name}': BASE ONLY — per-kWh riders referenced but not added: {riders[:4]}")
    return n


def annotate_sch102_first_block_on_tou(tariffs: list[ExtractedTariff]) -> int:
    """Document Sch 102 first-2,000 kWh credit when folded into TOD periods.

    The First/Over break cannot be expressed per TOD period; we keep the
    First credit in every period price (correct for most homes) and record
    ``confidence_notes['sch102_credit_first_2000_kwh_only']``. Never sets
    ``needs_review`` (R14b).
    """
    annotated = 0
    for t in tariffs:
        if _is_rider_only_tariff(t):
            continue
        rt = str(t.rate_type or "").lower()
        if rt not in ("tou", "tou_tiered", "seasonal_tou", "demand_tou"):
            continue
        first_cents: float | None = None
        bound = 2000
        for c in t.components or []:
            if not isinstance(c, dict):
                continue
            if str(c.get("component_type") or "").lower() != "adjustment":
                continue
            blob = _component_rider_label_blob(c)
            if not re.search(r"(?:schedule|sch)\s*102\b", blob, re.I):
                continue
            if not re.search(r"\bfirst\b", blob, re.I):
                continue
            cents = _adjustment_rate_cents(c)
            if cents is None:
                continue
            first_cents = cents
            bm = re.search(r"first\s+([\d,]+)\s*kwh", blob, re.I)
            if bm:
                try:
                    bound = int(bm.group(1).replace(",", ""))
                except ValueError:
                    pass
            break
        if first_cents is None:
            continue
        # Credit is negative; excess kWh are |credit| higher.
        excess = abs(float(first_cents))
        notes = dict(getattr(t, "confidence_notes", None) or {})
        key = f"sch102_credit_first_{bound}_kwh_only"
        notes[key] = f"excess kWh are {excess:g}¢ higher"
        # Stable alias matching the user-facing wording.
        notes["sch102_credit_first_2000_kwh_only"] = (
            f"excess kWh are {excess:g}¢ higher"
        )
        t.confidence_notes = notes
        annotated += 1
        log.info(
            f"    Sch 102 first-{bound:,} kWh credit on TOD '{t.name}' — "
            f"noted in confidence_factors (not needs_review)"
        )
    return annotated


def _tariff_text_blob(t: ExtractedTariff) -> str:
    parts = [str(t.name or ""), str(t.description or ""), str(t.code or "")]
    for c in t.components or []:
        if not isinstance(c, dict):
            continue
        parts.extend(
            str(c.get(k) or "")
            for k in ("tier_label", "period_label", "season")
        )
    return " ".join(parts)


def _has_universal_stacking_riders(t: ExtractedTariff) -> bool:
    return any(
        _is_universal_stacking_rider(c, tariff_name=str(t.name or ""), allow_tod=False)
        for c in (t.components or [])
        if isinstance(c, dict)
    )


def _residential_needs_external_riders(t: ExtractedTariff) -> bool:
    """True when a residential ENERGY plan hints at riders not in the batch."""
    if str(t.customer_class or "").lower() != "residential":
        return False
    if getattr(t, "riders_referenced_not_shown", None):
        return True
    has_energy = any(
        isinstance(c, dict)
        and str(c.get("component_type") or "").lower() == "energy"
        and _is_energy_unit(c.get("unit"))
        for c in (t.components or [])
    )
    if not has_energy:
        return False
    if _has_universal_stacking_riders(t):
        return False
    if any(_energy_already_includes_stacking_riders(c) for c in (t.components or []) if isinstance(c, dict)):
        return False
    return bool(_RIDER_DOC_HINT_RE.search(_tariff_text_blob(t)))


_ADJUSTMENT_SECTION_RE = re.compile(
    r"(?:adjustments?|riders?|subject\s+to)\s*[:\-]?\s*"
    r"((?:schedule\s*1\d{2}[\s,;and]*)+)",
    re.IGNORECASE,
)


def _rider_search_hints(tariffs: list[ExtractedTariff]) -> list[str]:
    """Build bounded Brave queries for missing adjustment/rider schedules."""
    hints: list[str] = []
    seen: set[str] = set()

    def _add(h: str) -> None:
        key = h.strip().lower()
        if key and key not in seen:
            seen.add(key)
            hints.append(h.strip())

    for t in tariffs:
        for named in getattr(t, "riders_referenced_not_shown", None) or []:
            _add(str(named))
            # Named "Schedule 128" etc. also enqueue the numbered form.
            for m in re.finditer(r"schedule\s*(1\d{2})", str(named), re.IGNORECASE):
                _add(f"Schedule {m.group(1)}")
        blob = _tariff_text_blob(t)
        # Always harvest Sch 1xx from the adjustments section / plan text,
        # even when some riders already folded (PGE often lists many 1xx).
        for m in re.finditer(r"schedule\s*(1\d{2})", blob, re.IGNORECASE):
            _add(f"Schedule {m.group(1)}")
        for m in _ADJUSTMENT_SECTION_RE.finditer(blob):
            for sm in re.finditer(r"1\d{2}", m.group(1)):
                _add(f"Schedule {sm.group(0)}")
        if not _residential_needs_external_riders(t) and not getattr(
            t, "riders_referenced_not_shown", None
        ):
            continue
        if re.search(r"\bfam\b|fuel\s*adjust", blob, re.IGNORECASE):
            _add("Fuel Adjustment Mechanism FAM")
        if re.search(r"\bdsm\b|dcrr|efficiency", blob, re.IGNORECASE):
            _add("DSM DCRR efficiency rider")
        if re.search(r"storm|scrr", blob, re.IGNORECASE):
            _add("Storm cost recovery rider SCRR")
        if re.search(r"power\s*cost|\bpca\b", blob, re.IGNORECASE):
            _add("Power Cost Adjustment")
        if re.search(r"cost\s*recovery", blob, re.IGNORECASE):
            _add("cost recovery adjustment")
        if re.search(r"adjustments?\s+schedule|schedule\s*1\d{2}", blob, re.IGNORECASE):
            _add("residential adjustment schedules Schedule 1")
        # Generic fallback when the extract only says riders aren't on-page.
        if not hints or _RIDER_DOC_HINT_RE.search(blob):
            _add("rate rider adjustment schedule")
    return hints[:MAX_RIDER_DOCS_FETCH]


def _same_registrable_domain(url: str, utility_domain: str) -> bool:
    if not utility_domain:
        return False
    host = normalize_host(url) or ""
    base = normalize_host(utility_domain) or utility_domain.replace("www.", "").lower()
    if not host or not base:
        return False
    try:
        host_reg = registrable_domain(host)
        base_reg = registrable_domain(base)
    except Exception:
        host_reg, base_reg = host, base
    return host_reg == base_reg or host.endswith(f".{base_reg}") or base.endswith(f".{host_reg}")


def _url_in_allowed_domains(url: str, allowed: set[str]) -> bool:
    """True when url's registrable domain is in the official-domain allowlist."""
    if not allowed:
        return False
    host = normalize_host(url) or ""
    if not host:
        return False
    try:
        reg = registrable_domain(host)
    except Exception:
        reg = host
    return reg in allowed or any(
        host == d or host.endswith(f".{d}") or reg.endswith(f".{d}") for d in allowed
    )


def _official_domains_for_rider_fetch(
    website_url: str = "",
    tariffs: list[ExtractedTariff] | None = None,
    existing_pages: list[RatePage] | None = None,
) -> set[str]:
    """Official domains allowed when fetching missing rider documents.

    Includes the utility's saved ``website_url`` when it is not a generic
    file host/CDN (ctfassets, cloudfront, …), plus registrable domains from
    existing verified tariff ``source_url``s and already-fetched pages.
    Third-party aggregators and generic hosts are always excluded — so PGE
    adjustment schedules on portlandgeneral.com are kept even when the
    saved website is ``assets.ctfassets.net``.
    """
    allowed: set[str] = set()

    def _maybe_add(url_or_host: str | None) -> None:
        if not url_or_host:
            return
        if _is_third_party_domain(url_or_host if "://" in url_or_host else f"https://{url_or_host}"):
            return
        if is_generic_host(url_or_host):
            return
        host = normalize_host(url_or_host)
        if not host:
            return
        try:
            allowed.add(registrable_domain(host))
        except Exception:
            allowed.add(host)

    _maybe_add(website_url)
    for t in tariffs or []:
        _maybe_add(getattr(t, "source_url", None))
    for p in existing_pages or []:
        _maybe_add(getattr(p, "url", None))
    return allowed


def _utility_domains_for_rider_fetch(
    website_url: str = "",
    tariffs: list[ExtractedTariff] | None = None,
    existing_pages: list[RatePage] | None = None,
) -> set[str]:
    """Utility-owned domains only (excludes regulator docket hosts)."""
    domains = _official_domains_for_rider_fetch(website_url, tariffs, existing_pages)
    return {
        d for d in domains
        if not _is_regulator_filing_url(f"https://{d}/")
    }


def _rider_doc_years(url: str, title: str = "", description: str = "") -> list[int]:
    """Calendar years mentioned in a rider-doc URL/title/snippet."""
    try:
        text = f"{unquote(url)} {title} {description}"
    except Exception:
        text = f"{url} {title} {description}"
    years: list[int] = []
    today_y = date.today().year
    for m in _RIDER_DOC_YEAR_RE.finditer(text):
        try:
            y = int(m.group(1))
        except ValueError:
            continue
        if 1990 <= y <= today_y + 2:
            years.append(y)
    return years


def _is_stale_rider_document(
    url: str,
    title: str = "",
    description: str = "",
    *,
    today: date | None = None,
    max_age_years: int = RIDER_DOC_MAX_AGE_YEARS,
) -> bool:
    """True when the doc's year is clearly older than ~max_age_years.

    Current tariff books (``tariff-book-YYYY`` near today) are never stale.
    Documents with no year hint are kept (ranked lower instead).
    """
    today = today or date.today()
    book = _tariff_book_year(url)
    if book is not None and book >= today.year - 1:
        return False
    years = _rider_doc_years(url, title, description)
    if not years:
        return False
    latest = max(years)
    return latest < today.year - max_age_years


def _rider_search_result_rank_key(
    result: dict,
    *,
    allowed_domains: set[str],
    utility_domains: set[str],
) -> tuple:
    """Lower is better: utility domain > fresh > rider-ish > regulator docket."""
    url = (result.get("url") or "").strip()
    title = str(result.get("title") or "")
    desc = str(result.get("description") or "")
    blob = f"{title} {url} {desc}"
    on_utility = 0 if _url_in_allowed_domains(url, utility_domains) else 1
    on_allowed = 0 if _url_in_allowed_domains(url, allowed_domains) else 1
    regulator = 1 if _is_regulator_filing_url(url, title) else 0
    stale = 1 if _is_stale_rider_document(url, title, desc) else 0
    riderish = 0 if _RIDER_DOC_HINT_RE.search(blob) else 1
    years = _rider_doc_years(url, title, desc)
    # Prefer newer years when present (negate so lower sorts first).
    year_score = -(max(years) if years else 0)
    return (on_utility, stale, regulator, on_allowed, riderish, year_score)


RIDER_MODE_EXTRACT_PROMPT = """TODAY: {today}
TARGET UTILITY: {utility_name} ({state})
RIDER HINT: {hint}

This page was fetched specifically for the rider/adjustment named above — NOT for base rate plans.

Extract ONLY per-kWh ADJUSTMENT (rider) amounts that residential customers pay in addition to base ENERGY. For each distinct amount:
- Put the rider name in the tariff name and/or tier_label (FAM, DCRR, DSM, PCA, Schedule 1xx, …).
- Record rate_value and unit exactly as printed.
- Record which customer class or schedule the amount applies to (Domestic, Residential, Small General, General, …). When the page lists several class-specific amounts, emit one tariff per class (customer_class residential only when the class is residential/domestic; still include the class name in the tariff name / tier_label so sharing can match). Prefer customer_class "residential" for Domestic/Residential rows.
- component_type must be "adjustment" (never "energy" for these rider pages).

Do NOT extract base ENERGY, FIXED, or demand charges of rate schedules (Domestic Service, Schedule 7, Schedule 32, …). Do NOT invent amounts. If this page has no per-kWh rider/adjustment amounts for the hint, return an empty tariffs array and set empty_reason.

Use the store_tariffs tool.

Content:
{content}"""


def _extract_rider_document(
    page: RatePage,
    utility_name: str,
    state: str,
    hint: str = "",
) -> list[ExtractedTariff]:
    """Rider-mode extract: per-kWh adjustments + classes, not plan listing."""
    content = _select_rate_content(page.content or "", max_chars=20000)
    if not content or len(content.strip()) < 80:
        return []
    prompt = RIDER_MODE_EXTRACT_PROMPT.format(
        today=_today_iso(),
        utility_name=utility_name,
        state=state or "",
        hint=hint or "rate rider / adjustment",
        content=content,
    )
    try:
        raw = _call_claude_tool(prompt, model=SONNET_MODEL, effort="medium")
    except Exception as e:
        log.info(f"    Rider-mode extract failed for {page.url[:70]}: {e}")
        return []
    tariffs = _parse_tool_tariffs(raw, page.url)
    out: list[ExtractedTariff] = []
    for t in tariffs:
        t.source_url = page.url
        t.extraction_tier = "rider_doc"
        # Force adjustment-only shape when the model still emitted ENERGY.
        comps: list[dict] = []
        for c in t.components or []:
            if not isinstance(c, dict):
                continue
            ctype = str(c.get("component_type") or "").lower()
            row = dict(c)
            if ctype == "energy" and _is_energy_unit(row.get("unit")):
                row["component_type"] = "adjustment"
                if not row.get("tier_label"):
                    row["tier_label"] = hint or t.name or "rider"
            comps.append(row)
        t.components = comps
        if not t.components:
            continue
        out.append(t)
    return out


def _filter_useful_rider_extracts(extra: list[ExtractedTariff]) -> list[ExtractedTariff]:
    """Keep true rider/adjustment donors; drop other schedules' base T&D."""
    useful: list[ExtractedTariff] = []
    for t in extra:
        name = str(t.name or "")
        # Never keep another schedule's base delivery as a donor.
        if _looks_like_base_rate_schedule(name) and not _is_adjustment_schedule_name(name):
            log.info(f"    Rider-doc drop base schedule '{name}' (not an adjustment)")
            continue
        if _is_rider_only_tariff(t) or _RIDER_DONOR_NAME_RE.search(name) or _is_adjustment_schedule_name(name):
            # Strip any leaked base-delivery rows from numbered base schedules.
            # Keep Sch 125 7-TOD period rows — they are not universal flat
            # stackers (allow_tod=False excludes them) but apply_tod needs
            # them as donors (R13: PCA silently dropped on 2020-book TOU).
            comps = [
                dict(c)
                for c in (t.components or [])
                if isinstance(c, dict)
                and not _is_base_schedule_delivery_charge(c, tariff_name=name)
                and (
                    _is_universal_stacking_rider(c, tariff_name=name, allow_tod=False)
                    or _is_sch125_tod_period_rider(c, tariff_name=name)
                )
            ]
            if not comps and _is_rider_only_tariff(t):
                # Rider-only but labels didn't match stacking regex — keep
                # energy-unit adjustments that aren't base T&D.
                comps = [
                    dict(c)
                    for c in (t.components or [])
                    if isinstance(c, dict)
                    and str(c.get("component_type") or "").lower() == "adjustment"
                    and _is_energy_unit(c.get("unit"))
                    and not _is_base_schedule_delivery_charge(c, tariff_name=name)
                    and not _OPTIONAL_OR_SCOPED_RIDER_RE.search(
                        _adjustment_label_blob(c, tariff_name=name)
                    )
                ]
            if comps:
                t.components = comps
                useful.append(t)
            continue
        if _has_universal_stacking_riders(t):
            adjs = [
                dict(c)
                for c in (t.components or [])
                if isinstance(c, dict)
                and _is_universal_stacking_rider(
                    c, tariff_name=name, allow_tod=False
                )
            ]
            if adjs:
                useful.append(
                    ExtractedTariff(
                        name=f"{t.name} (riders)",
                        code=t.code,
                        customer_class=t.customer_class or "residential",
                        rate_type="flat",
                        description=t.description,
                        source_url=t.source_url,
                        components=adjs,
                        confidence=t.confidence,
                        extraction_tier=t.extraction_tier,
                    )
                )
    return useful


def _page_lacks_rider_amounts(content: str) -> bool:
    """True when page text has almost no ¢/kWh figures (link hub, not tariff)."""
    if not content or len(content.strip()) < 40:
        return True
    amounts = re.findall(
        r"(?:¢|cents?)\s*/\s*kwh|\$\s*/\s*kwh|(?<![\d.])\d+\.\d{2,4}\s*(?:¢|cents?)",
        content,
        re.IGNORECASE,
    )
    return len(amounts) < 2


def _collect_page_hrefs(html: str, base_url: str) -> list[str]:
    """Absolute hrefs from raw HTML (before text conversion strips tags)."""
    if not html:
        return []
    out: list[str] = []
    seen: set[str] = set()
    try:
        soup = BeautifulSoup(html, "html.parser")
        hrefs = [
            (a.get("href") or "").strip()
            for a in soup.find_all("a", href=True)
        ]
    except Exception:
        hrefs = [
            m.group(1)
            for m in re.finditer(r"href=[\"']([^\"']+)[\"']", html, re.I)
        ]
    for href in hrefs:
        if not href or href.startswith(("#", "mailto:", "javascript:")):
            continue
        url = urljoin(base_url or "", href)
        key = url.split("#")[0].rstrip("/").lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(url.split("#")[0])
        if len(out) >= 40:
            break
    return out


def _page_is_rider_link_hub(page: RatePage) -> bool:
    """True when an HTML rider page is mostly links to the real tariff PDF.

    NSP FAM pages often show marketing copy with a stray decimal, so the
    strict amount check alone misses them — also treat pages that point at
    same-ish-domain FAM/DSM/tariff PDFs as hubs. Prefer ``page.links``
    (captured from raw HTML) because ``content`` is plain text (R8).
    """
    content = page.content or ""
    if _page_lacks_rider_amounts(content):
        return True
    if (page.page_type or "").lower() == "pdf":
        return False
    link_pool = list(page.links or [])
    pdf_from_content = re.findall(
        r"href=[\"']([^\"']+\.pdf[^\"']*)[\"']|(https?://[^\s<>\"']+\.pdf)",
        content,
        re.IGNORECASE,
    )
    for a, b in pdf_from_content:
        link_pool.append(a or b or "")
    pdf_hits = [u for u in link_pool if ".pdf" in u.lower()]
    labels = (
        " ".join(pdf_hits).lower()
        + " " + (page.title or "").lower()
        + " " + (page.url or "").lower()
    )
    if not pdf_hits and not link_pool:
        return False
    if re.search(r"\bfam\b|fuel\s*adjust|dsm|dcrr|scrr|tariff\s*book|rate\s*tariff", labels):
        return True
    amounts = re.findall(
        r"(?:¢|cents?)\s*/\s*kwh|(?<![\d.])\d+\.\d{2,4}\s*(?:¢|cents?)",
        content,
        re.IGNORECASE,
    )
    return len(pdf_hits) >= 1 and len(amounts) < 4


def _rider_page_one_hop_links(
    page: RatePage,
    *,
    allowed_domains: set[str],
) -> list[str]:
    """Same-domain PDF/tariff links from a rider HTML page (one hop)."""
    html = page.content or ""
    base = page.url or ""
    if not base:
        return []
    candidates: list[tuple[str, str]] = []
    # Prefer links captured from raw HTML before text conversion (R8).
    for url in page.links or []:
        candidates.append((url, ""))
    if html:
        try:
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = (a.get("href") or "").strip()
                if href and not href.startswith(("#", "mailto:", "javascript:")):
                    candidates.append((href, a.get_text(" ", strip=True)))
        except Exception:
            pass
        for m in re.finditer(
            r"href=[\"']([^\"']+)[\"']|(https?://[^\s<>\"']+\.pdf)",
            html,
            re.IGNORECASE,
        ):
            href = (m.group(1) or m.group(2) or "").strip()
            if href:
                candidates.append((href, ""))

    out: list[str] = []
    seen: set[str] = set()
    for href, anchor_text in candidates:
        url = urljoin(base, href)
        if _is_third_party_domain(url) or is_generic_host(url):
            continue
        if allowed_domains and not _url_in_allowed_domains(url, allowed_domains):
            continue
        label = f"{anchor_text} {href} {url}".lower()
        path = urlparse(url).path.lower()
        is_pdf = path.endswith(".pdf")
        looks_rider = bool(
            _RIDER_DOC_HINT_RE.search(label)
            or re.search(
                r"fam|fuel\s*adjust|dsm|dcrr|scrr|pca|tariff|rider|adjust|"
                r"mechanism|board.?s?\s*order|rate\s*book",
                label,
            )
        )
        # Prefer PDFs; allow HTML tariff pages that clearly name the rider.
        if not is_pdf and not looks_rider:
            continue
        if not is_pdf and not re.search(r"fam|dsm|dcrr|scrr|pca|tariff", label):
            continue
        key = url.split("?")[0].rstrip("/").lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(url)
        if len(out) >= 6:
            break
    return out


def _harvest_sch1xx_links_from_html(
    html: str,
    *,
    base_url: str,
    allowed_domains: set[str],
) -> list[str]:
    """Pull Schedule 1xx PDF/HTML links from a utility tariff index page."""
    if not html or not base_url:
        return []
    out: list[str] = []
    seen: set[str] = set()
    try:
        soup = BeautifulSoup(html, "html.parser")
        anchors = [
            (a.get("href") or "", a.get_text(" ", strip=True))
            for a in soup.find_all("a", href=True)
        ]
    except Exception:
        anchors = []
        for m in re.finditer(
            r"href=[\"']([^\"']+)[\"'][^>]*>([^<]{0,120})",
            html,
            re.IGNORECASE,
        ):
            anchors.append((m.group(1), m.group(2)))
    for href, text in anchors:
        href = (href or "").strip()
        if not href or href.startswith(("#", "mailto:", "javascript:")):
            continue
        label = f"{text} {href}".lower()
        if not re.search(r"schedule\s*1\d{2}|\b1\d{2}\b.*(?:adjust|rider|pca|bac)", label):
            if not re.search(r"/schedule[-_]?1\d{2}\b", href.lower()):
                continue
        url = urljoin(base_url, href)
        if _is_third_party_domain(url) or is_generic_host(url):
            continue
        if allowed_domains and not _url_in_allowed_domains(url, allowed_domains):
            continue
        key = url.split("?")[0].rstrip("/").lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(url)
        if len(out) >= 12:
            break
    return out


# Known PGE (OR) tariff-index landing pages — harvested for Sch 1xx links.
# The marketing index is JS-rendered; Gatsby page-data.json and the
# price-summaries archive embed static assets.ctfassets.net Sched_1xx PDFs.
_PGE_TARIFF_INDEX_URLS = (
    "https://portlandgeneral.com/about/info/rates-and-regulatory/tariff",
    "https://portlandgeneral.com/page-data/about/info/rates-and-regulatory/tariff/page-data.json",
    "https://portlandgeneral.com/about/info/rates-and-regulatory/price-summaries-archive",
    "https://www.portlandgeneral.com/rates/electric-service-schedules",
    "https://portlandgeneral.com/about/info/pricing",
)

_PGE_CTF_SPACE = "416ywc1laqmd"
_PGE_OPUC_FALLBACK = (
    "https://edocs.puc.state.or.us/efdocs/UBA/ue452uba342429171.pdf",
)


def _harvest_pge_sch1xx_from_text(
    text: str,
    *,
    wanted: set[str] | None = None,
    limit: int = 32,
) -> list[str]:
    """Pull assets.ctfassets.net Sched_1xx.pdf URLs from HTML or page-data JSON."""
    if not text:
        return []
    wanted = {w.zfill(3) if w.isdigit() else w for w in (wanted or set())}
    out: list[str] = []
    seen: set[str] = set()
    # Protocol-relative or absolute Contentful URLs ending in Sched_NNN.pdf
    for m in re.finditer(
        r"(?:https?:)?//assets\.ctfassets\.net/"
        + re.escape(_PGE_CTF_SPACE)
        + r"/[^\s\"'\\<>]+?/Sched_(\d{3})\.pdf",
        text,
        re.IGNORECASE,
    ):
        num = m.group(1)
        # wanted has "100","125"; Sched files use the same zero-padded numbers.
        if wanted and num not in wanted:
            continue
        url = m.group(0)
        if url.startswith("//"):
            url = "https:" + url
        key = url.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(url)
        if len(out) >= limit:
            break
    return out


def _pge_sch1xx_url_map_from_text(text: str) -> dict[str, str]:
    """Map '100' → full https://assets.ctfassets.net/.../Sched_100.pdf."""
    mapping: dict[str, str] = {}
    if not text:
        return mapping
    for m in re.finditer(
        r"(?:https?:)?//assets\.ctfassets\.net/"
        + re.escape(_PGE_CTF_SPACE)
        + r"/[^\s\"'\\<>]+?/Sched_(\d{3})\.pdf",
        text,
        re.IGNORECASE,
    ):
        num = m.group(1)
        url = m.group(0)
        if url.startswith("//"):
            url = "https:" + url
        mapping.setdefault(num, url)
    return mapping


def _refetch_pge_index_raw(url: str) -> str:
    """Re-fetch raw page-data/HTML for Sch 1xx URL harvest.

    Uses this module's ``fetch_page`` (sync httpx). Must not import a
    non-existent ``app.services.monitor.fetch_page`` — that ImportError was
    previously swallowed and left the URL map truncated at Sch 128 (R10).
    """
    if not url:
        return ""
    body, _ctype, status = fetch_page(url)
    if status == 200 and body:
        return body if isinstance(body, str) else body.decode("utf-8", "replace")
    return ""


def _pge_sch1xx_wanted_from_hints(hints: list[str]) -> set[str]:
    nums: set[str] = set()
    for h in hints:
        for m in re.finditer(r"schedule\s*(1\d{2})", str(h), re.I):
            nums.add(m.group(1))
        for m in re.finditer(r"\b(1\d{2})\b", str(h)):
            nums.add(m.group(1))
    return nums


def _parse_pge_sch100_pipe_table(
    text: str,
    *,
    base_schedule: str = "7",
) -> list[str]:
    """Parse the pipe-delimited applicability copy of the Sch 100 grid.

    pdftotext of Sched_100 often collapses blank cells in the plain grid, so
    column-align misreads which ``x`` marks apply. The same extract also
    carries a pipe-separated copy (``Schs. | 102(1) | …`` / ``7 | x | …``)
    that preserves empty cells — prefer that when present (R10).
    """
    if not text or "|" not in text:
        return []
    lines = text.splitlines()
    applicable: list[str] = []
    seen: set[str] = set()
    i = 0
    while i < len(lines):
        line = lines[i]
        if not re.match(r"^[ \t]*Schs?\.\s*\|", line, re.I):
            i += 1
            continue
        hdr_cells = [c.strip() for c in line.split("|")]
        nums: list[str | None] = []
        for cell in hdr_cells[1:]:  # skip "Schs."
            m = re.search(r"\b(1\d{2})\b", cell)
            nums.append(m.group(1) if m else None)
        if not any(nums):
            i += 1
            continue
        row_line = None
        for j in range(i + 1, min(i + 40, len(lines))):
            if re.match(r"^[ \t]*Schs?\.\s*\|", lines[j], re.I):
                break
            if re.match(
                rf"^[ \t]*{re.escape(str(base_schedule))}\s*\|",
                lines[j],
            ):
                row_line = lines[j]
                break
        if row_line is None:
            i += 1
            continue
        row_cells = [c.strip() for c in row_line.split("|")]
        # row_cells[0] is the schedule number ("7"); marks align with hdr nums.
        marks = row_cells[1:]
        for idx, num in enumerate(nums):
            if not num or num in seen:
                continue
            mark = marks[idx] if idx < len(marks) else ""
            if re.search(r"x", mark, re.I):
                seen.add(num)
                applicable.append(num)
        i += 1
    return applicable


def _parse_pge_sch100_column_align(
    text: str,
    *,
    base_schedule: str = "7",
) -> list[str]:
    """Fallback: align Schs. header column positions with an ``x`` on the row."""
    if not text:
        return []
    lines = text.splitlines()
    applicable: list[str] = []
    seen: set[str] = set()
    i = 0
    while i < len(lines):
        line = lines[i]
        hdr = re.match(r"^[ \t]*Schs?\.\s+(.*)$", line, re.I)
        if not hdr:
            i += 1
            continue
        # Skip pipe tables (handled separately).
        if "|" in line:
            i += 1
            continue
        cols: list[tuple[int, str]] = []
        for m in re.finditer(r"\b(1\d{2})\b", line):
            cols.append((m.start(1), m.group(1)))
        if not cols:
            i += 1
            continue
        row_line = None
        for j in range(i + 1, min(i + 40, len(lines))):
            rm = re.match(
                rf"^[ \t]*{re.escape(str(base_schedule))}[ \t]+(.*)$",
                lines[j],
            )
            if rm:
                row_line = lines[j]
                break
            if re.match(r"^[ \t]*Schs?\.\s+", lines[j], re.I):
                break
        if row_line is None:
            i += 1
            continue
        for start, num in cols:
            lo = max(0, start - 2)
            hi = min(len(row_line), start + len(num) + 4)
            window = row_line[lo:hi]
            if re.search(r"x", window, re.I) and num not in seen:
                seen.add(num)
                applicable.append(num)
        i += 1
    return applicable


def parse_pge_sch100_applicable_schedules(
    text: str,
    *,
    base_schedule: str = "7",
) -> list[str]:
    """Parse Sch 100 applicability grid: which Sched_1xx mark apply to Sch 7.

    Returns zero-padded schedule numbers (e.g. ``['102','105','125',…]``).
    Sch 100 itself is the map, not a priced rider. Prefers the pipe-delimited
    table copy when present (layout-preserving); falls back to column-align.
    """
    if not text:
        return []
    pipe = _parse_pge_sch100_pipe_table(text, base_schedule=base_schedule)
    if pipe:
        return pipe
    return _parse_pge_sch100_column_align(text, base_schedule=base_schedule)


def _is_pge_sch100_applicability_page(
    text: str = "",
    url: str = "",
) -> bool:
    """True only for the Sch 100 applicability map — not Sched_007 footnotes.

    Sched_007 says "See Schedule 100 for applicable adjustments"; that must
    not be treated as the map itself (R14 logged ``0 Sched_1xx`` three times).
    """
    if re.search(r"Sched_0*100\b", url or "", re.I):
        return True
    head = (text or "")[:2500]
    if not re.search(r"SCHEDULE\s+100\b", head, re.I):
        return False
    # Require the map title — not a bare "applicable adjustments" footnote.
    return bool(
        re.search(r"SUMMARY\s+OF\s+APPLICABLE\s+ADJUSTMENTS", text or "", re.I)
    )


def _enqueue_from_sch100_map(
    page_body: str,
    *,
    sch_url_map: dict[str, str],
    enqueue,
    base_schedule: str = "7",
    log_once: set[str] | None = None,
) -> list[str]:
    """Parse Sch 100 applicability and enqueue priced Sched_1xx URLs.

    Returns the applicable schedule numbers. Logs once per distinct body hash
    when ``log_once`` is provided (avoids the R14 triple ``0 Sched_1xx`` spam).
    """
    nums = parse_pge_sch100_applicable_schedules(
        page_body, base_schedule=base_schedule,
    )
    for num in nums:
        url = sch_url_map.get(num)
        if url:
            enqueue(url, f"Schedule {num}")
    if log_once is not None:
        # Stable key so identical re-parses of the same map log once.
        key = f"{len(page_body)}:{len(nums)}:{','.join(nums[:5])}"
        if key not in log_once:
            log_once.add(key)
            log.info(
                f"    Sch 100 map → {len(nums)} Sched_1xx applicable to "
                f"Schedule {base_schedule}"
            )
    else:
        log.info(
            f"    Sch 100 map → {len(nums)} Sched_1xx applicable to "
            f"Schedule {base_schedule}"
        )
    return nums


def parse_pge_sch1xx_kwh_amounts_for_schedule(
    text: str,
    *,
    schedule: str = "7",
) -> list[dict]:
    """Deterministic ¢/kWh amounts for ``schedule`` from a PGE Sched_1xx PDF text.

    Handles single-rate rows (``7  1.004 ¢ per kWh``), multi-column totals,
    parenthesized credits (``(1.112)``), First/Over tier blocks, and
    ``All schedules`` / ``All other Schedules`` fallbacks. Returns adjustment
    component dicts in ¢/kWh (caller may normalize).
    """
    if not text:
        return []
    sch = str(schedule).strip()
    out: list[dict] = []

    def _add(val: float, label: str, *, keep_zero: bool = False) -> None:
        if abs(val) < 1e-12 and not keep_zero:
            return
        out.append({
            "component_type": "adjustment",
            "unit": "¢/kWh",
            "rate_value": val,
            "tier_label": label,
            "included_in_energy": False,
            "rider_scope": "all_customers",
        })

    # Tiered First/Over block (Sch 102 style). Keep the Over 0.000 row so
    # First/Over can map onto Sch 7's two ENERGY tiers.
    first_m = re.search(
        rf"(?im)^\s*First\s+([\d,]+)\s*kWh\s+\(?(-?\d+\.\d+)\)?\s+¢\s*per\s*kWh",
        text,
    )
    over_m = re.search(
        rf"(?im)^\s*Over\s+([\d,]+)\s*kWh\s+\(?(-?\d+\.\d+)\)?\s+¢\s*per\s*kWh",
        text,
    )
    if first_m and over_m:
        v1 = float(first_m.group(2))
        v2 = float(over_m.group(2))
        if re.search(rf"First\s+{re.escape(first_m.group(1))}[^\n]*\(\s*{re.escape(first_m.group(2))}\s*\)", text):
            v1 = -abs(v1)
        if re.search(rf"Over\s+{re.escape(over_m.group(1))}[^\n]*\(\s*{re.escape(over_m.group(2))}\s*\)", text):
            v2 = -abs(v2)
        _add(v1, f"Sch first {first_m.group(1)} kWh", keep_zero=True)
        _add(v2, f"Sch over {over_m.group(1)} kWh", keep_zero=True)
        return out

    # Per-schedule row: "7   0.123  0.000  0.123  ¢ per kWh" or "7  1.004 ¢ per kWh"
    row_pat = re.compile(
        rf"(?im)^\s*{re.escape(sch)}(?:/\d+)?\s+(.+?)¢\s*per\s*kWh"
    )
    for m in row_pat.finditer(text):
        body = m.group(1)
        nums = re.findall(r"\(?(-?\d+\.\d+)\)?", body)
        if not nums:
            continue
        raw = nums[-1]  # total / sole column
        val = float(raw)
        if re.search(rf"\(\s*{re.escape(raw)}\s*\)", body):
            val = -abs(val)
        _add(val, f"Schedule {sch}")
        return out

    # Fallbacks
    for pat, label in (
        (r"(?im)All schedules\s+\(?(-?\d+\.\d+)\)?\s+¢\s*per\s*kWh", "All schedules"),
        (r"(?im)All other Schedules,?\s+\(?(-?\d+\.\d+)\)?\s*¢\s*per\s*kWh", "All other Schedules"),
    ):
        m = re.search(pat, text)
        if m:
            val = float(m.group(1))
            if f"({m.group(1)})" in (m.group(0) or ""):
                val = -abs(val)
            _add(val, label)
            return out
    return out


def _nsp_fam_2026_table_text(text: str) -> str:
    """Slice the FAM Tariff section's 2026 AA/BA table only (not the whole book).

    Searching the full tariff book grabs Domestic *base energy* 15.411¢ as
    FAM (R10). Restrict to the FAM TARIFF schedule body, then the 2026
    "Effective upon the date of the Board's Order" table that ends before
    the 2027 block.
    """
    if not text:
        return ""
    # Prefer a real schedule page ("Page N of M"), not the TOC entry.
    starts = [
        m.start()
        for m in re.finditer(
            r"FUEL\s+ADJUSTMENT\s+MECHANISM\s*\(FAM\)\s*TARIFF\s+Page\b",
            text,
            re.I,
        )
    ]
    if not starts:
        m = re.search(
            r"FUEL\s+ADJUSTMENT\s+MECHANISM\s*\(FAM\)\s*TARIFF\b",
            text,
            re.I,
        )
        if not m:
            return ""
        starts = [m.start()]
    # Walk candidates until we find the "applicable charges by rate class" table.
    section = ""
    for start in starts:
        chunk = text[start:start + 25000]
        if re.search(r"applicable\s+charges\s+by\s+rate\s+class", chunk, re.I):
            section = chunk
            break
    if not section:
        section = text[starts[0]:starts[0] + 25000]
    # 2026 table: from "applicable charges" through just before 2027 rates.
    m = re.search(r"applicable\s+charges\s+by\s+rate\s+class", section, re.I)
    if not m:
        return section
    table = section[m.start():]
    end = re.search(
        r"(?:^|\n)\s*2027\b|(?:^|\n)\s*Effective\s+January\s+1,\s*2027\b",
        table,
        re.I,
    )
    if end:
        table = table[:end.start()]
    return table


def extract_nsp_fam_aa_ba_from_text(text: str) -> list[ExtractedTariff]:
    """Deterministic FAM AA/BA ¢/kWh from the main NSP tariff book's FAM section.

    The FAM marketing page has no PDF link; the rates live in the already-
    downloaded tariff book (~p.65). Parse **only** the FAM Tariff 2026
    table: Domestic 0.156¢; MURB shares the General line 0.207¢. Values
    above ``RIDER_FAM_SANITY_MAX_CENTS`` are rejected (never silently emitted).
    """
    if not text or not re.search(
        r"FUEL\s+ADJUSTMENT\s+MECHANISM\s*\(FAM\)\s*TARIFF", text, re.I
    ):
        return []
    table = _nsp_fam_2026_table_text(text)
    if not table:
        return []

    def _combined_aa_ba(block: str) -> float | None:
        nums = re.findall(r"\d+\.\d{3}", block)
        if not nums:
            return None
        # Combined AA/BA is the last (often repeated) ¢ figure in the row.
        val = float(nums[-1])
        if val > RIDER_FAM_SANITY_MAX_CENTS + 1e-9:
            return None
        return val

    domestic = None
    # Domestic Service … Critical Peak Pricing block.
    m = re.search(
        r"Domestic\s+Service[\s\S]{0,400}?(?:Critical\s+Peak\s+Pricing|Peak\s+Pricing)",
        table,
        re.I,
    )
    if m:
        domestic = _combined_aa_ba(m.group(0))
    if domestic is None:
        m = re.search(
            r"Domestic\s+Service[\s\S]{0,300}?(\d+\.\d{3})",
            table,
            re.I,
        )
        if m:
            candidate = float(m.group(1))
            if candidate <= RIDER_FAM_SANITY_MAX_CENTS + 1e-9:
                domestic = candidate
    if domestic is None:
        # Absolute fallback only inside the 2026 table slice.
        if re.search(r"Domestic\s+Service[\s\S]{0,300}?0\.156", table, re.I):
            domestic = 0.156
    if domestic is None:
        return []

    # General / Multi-Unit / MURB share one AA/BA line (0.207). Do NOT take
    # the Large General row (0.158) that follows MURB after a page break.
    general = None
    m = re.search(
        r"General,\s*General\s+Time\s+of\s+Use[\s\S]{0,350}?"
        r"(?:Multi-Unit|MURB|Residential\s+Building)",
        table,
        re.I,
    )
    if m:
        general = _combined_aa_ba(m.group(0))
    if general is None:
        m = re.search(
            r"General,\s*General\s+Time\s+of\s+Use[\s\S]{0,200}?(\d+\.\d{3})",
            table,
            re.I,
        )
        if m:
            candidate = float(m.group(1))
            if candidate <= RIDER_FAM_SANITY_MAX_CENTS + 1e-9:
                general = candidate
    murb = general  # MURB takes the General line (R10)

    comps = [{
        "component_type": "adjustment",
        "unit": "¢/kWh",
        "rate_value": domestic,
        "tier_label": "FAM AA/BA Domestic Service / TOD / TOU / CPP",
        "included_in_energy": False,
        "rider_scope": "all_customers",
    }]
    et = ExtractedTariff(
        name="Fuel Adjustment Mechanism (FAM) AA/BA - Domestic",
        customer_class="residential",
        rate_type="flat",
        components=comps,
        extraction_tier="rider_doc_book",
        energy_scope="bundled",
        description="FAM AA/BA from main tariff book FAM Tariff 2026 table (deterministic).",
    )
    out = [et]
    if murb is not None and abs(murb - domestic) > 1e-9:
        out.append(ExtractedTariff(
            name="Fuel Adjustment Mechanism (FAM) AA/BA - MURB",
            customer_class="residential",
            rate_type="flat",
            components=[{
                "component_type": "adjustment",
                "unit": "¢/kWh",
                "rate_value": murb,
                "tier_label": "FAM AA/BA Residential Building (MURB)",
                "included_in_energy": False,
                "rider_scope": "all_customers",
            }],
            extraction_tier="rider_doc_book",
            energy_scope="bundled",
            description=(
                "FAM AA/BA MURB (= General line 0.207¢) from FAM Tariff 2026 table."
            ),
        ))
    return out


def extract_nsp_dcrr_from_text(text: str) -> list[ExtractedTariff]:
    """Deterministic DCRR ¢/kWh from the NSP tariff book's DCRR Schedule A.

    Domestic / TOD / TOU / CPP → 0.648¢; General / MURB → 0.749¢. Same sanity
    cap as FAM. Replaces the LLM Domestic-only DCRR extract so MURB gets its
    own class rate (R11).
    """
    if not text or not re.search(
        r"DEMAND\s+SIDE\s+MANAGEMENT\s+COST\s+RECOVERY\s+RIDER\s*\(DCRR\)",
        text,
        re.I,
    ):
        return []
    starts = [
        m.start()
        for m in re.finditer(
            r"DEMAND\s+SIDE\s+MANAGEMENT\s+COST\s+RECOVERY\s+RIDER\s*\(DCRR\)\s+Page\b",
            text,
            re.I,
        )
    ]
    if not starts:
        m = re.search(
            r"DEMAND\s+SIDE\s+MANAGEMENT\s+COST\s+RECOVERY\s+RIDER\s*\(DCRR\)",
            text,
            re.I,
        )
        if not m:
            return []
        starts = [m.start()]
    section = ""
    for start in starts:
        chunk = text[start:start + 20000]
        if re.search(r"2026\s+DSM\s+Cost\s+Recovery\s+Rider\s+Charges|SCHEDULE\s+A", chunk, re.I):
            section = chunk
            break
    if not section:
        section = text[starts[0]:starts[0] + 20000]

    def _dcrr_val(block: str) -> float | None:
        # Row ends with PCR  BA  DCRR — take the last ¢ figure as DCRR.
        nums = re.findall(r"\(?(-?\d+\.\d{3})\)?", block)
        if not nums:
            return None
        val = float(nums[-1])
        if abs(val) > RIDER_FAM_SANITY_MAX_CENTS + 1e-9:
            return None
        return val

    domestic = None
    m = re.search(
        r"Domestic\s+Service[\s\S]{0,350}?Critical\s+Peak\s+Pricing"
        r"[\s\S]{0,80}?(\d+\.\d{3})\s+(\S+)\s+(\d+\.\d{3})",
        section,
        re.I,
    )
    if m:
        candidate = float(m.group(3))
        if abs(candidate) <= RIDER_FAM_SANITY_MAX_CENTS + 1e-9:
            domestic = candidate
    if domestic is None:
        m = re.search(
            r"Domestic\s+Service[\s\S]{0,400}?(\d+\.\d{3})\s*$",
            section,
            re.I | re.M,
        )
        if m:
            candidate = float(m.group(1))
            if abs(candidate) <= RIDER_FAM_SANITY_MAX_CENTS + 1e-9:
                domestic = candidate
    if domestic is None and re.search(
        r"Domestic\s+Service[\s\S]{0,400}?0\.648", section, re.I
    ):
        domestic = 0.648

    murb = None
    # General / MURB share one Schedule A row (0.749). Page break may put
    # "Multi-unit Residential Building…" after the numbers — take the DCRR
    # that sits with the General block, never the following Large General.
    m = re.search(
        r"General,\s*General\s+Time\s+of\s+Use[\s\S]{0,200}?"
        r"(\d+\.\d{3})\s+(\S+)\s+(\d+\.\d{3})",
        section,
        re.I,
    )
    if m:
        # Ensure this block is the General/MURB row, not Large General.
        window = section[m.start():m.end() + 80]
        if re.search(r"Multi-unit|MURB|Critical\s+Peak", window, re.I) or not re.search(
            r"Large\s+General", section[max(0, m.start() - 40):m.start()], re.I
        ):
            candidate = float(m.group(3))
            if abs(candidate) <= RIDER_FAM_SANITY_MAX_CENTS + 1e-9:
                murb = candidate
    if murb is None:
        # Explicit 0.749 near General+MURB wording.
        m = re.search(
            r"General,\s*General\s+Time\s+of\s+Use[\s\S]{0,300}?0\.749",
            section,
            re.I,
        )
        if m:
            murb = 0.749
    if murb is None and re.search(
        r"Multi-unit\s+Residential\s+Building[\s\S]{0,80}?0\.749|"
        r"0\.749[\s\S]{0,80}?Multi-unit\s+Residential\s+Building",
        section,
        re.I,
    ):
        murb = 0.749

    out: list[ExtractedTariff] = []
    if domestic is not None:
        out.append(ExtractedTariff(
            name="DSM Cost Recovery Rider (DCRR) - Domestic Service",
            customer_class="residential",
            rate_type="flat",
            components=[{
                "component_type": "adjustment",
                "unit": "¢/kWh",
                "rate_value": domestic,
                "tier_label": (
                    "DCRR Domestic Service, Time-of-Day, Time of Use, "
                    "Critical Peak Pricing"
                ),
                "included_in_energy": False,
                "rider_scope": "all_customers",
            }],
            extraction_tier="rider_doc_book",
            energy_scope="bundled",
            description="DCRR Domestic from tariff book Schedule A (deterministic).",
        ))
    if murb is not None:
        out.append(ExtractedTariff(
            name="DSM Cost Recovery Rider (DCRR) - MURB / General",
            customer_class="residential",
            rate_type="flat",
            components=[{
                "component_type": "adjustment",
                "unit": "¢/kWh",
                "rate_value": murb,
                "tier_label": (
                    "DCRR General / Multi-unit Residential Building Time-of-Use"
                ),
                "included_in_energy": False,
                "rider_scope": "all_customers",
            }],
            extraction_tier="rider_doc_book",
            energy_scope="bundled",
            description="DCRR MURB/General from tariff book Schedule A (deterministic).",
        ))
    return out


def extract_riders_from_main_tariff_book(
    pages: list[RatePage] | None,
    hints: list[str],
) -> list[ExtractedTariff]:
    """Pull rider tables (FAM, DCRR, …) from an already-fetched main rate book."""
    if not pages or not hints:
        return []
    want_fam = any(re.search(r"\bfam\b|fuel\s+adjust", h, re.I) for h in hints)
    want_dcrr = any(re.search(r"\bdsm\b|\bdcrr\b|demand[\s-]*side", h, re.I) for h in hints)
    # Always try DCRR when FAM is wanted — both live in the same book and
    # residential extracts commonly list both riders.
    if not want_fam and not want_dcrr:
        return []
    out: list[ExtractedTariff] = []
    fam_done = False
    dcrr_done = False
    for page in pages:
        content = getattr(page, "content", None) or ""
        if len(content) < 200:
            continue
        if (want_fam or want_dcrr) and not fam_done and re.search(
            r"FUEL\s+ADJUSTMENT\s+MECHANISM\s*\(FAM\)", content, re.I
        ):
            fam = extract_nsp_fam_aa_ba_from_text(content)
            for et in fam:
                et.source_url = getattr(page, "url", None) or et.source_url
            out.extend(fam)
            if fam:
                fam_done = True
                log.info(
                    f"    FAM AA/BA read from tariff book "
                    f"{(getattr(page, 'url', '') or '')[:70]} "
                    f"({len(fam)} class row(s))"
                )
        if (want_dcrr or want_fam) and not dcrr_done and re.search(
            r"DEMAND\s+SIDE\s+MANAGEMENT\s+COST\s+RECOVERY\s+RIDER", content, re.I
        ):
            dcrr = extract_nsp_dcrr_from_text(content)
            for et in dcrr:
                et.source_url = getattr(page, "url", None) or et.source_url
            out.extend(dcrr)
            if dcrr:
                dcrr_done = True
                log.info(
                    f"    DCRR read from tariff book "
                    f"{(getattr(page, 'url', '') or '')[:70]} "
                    f"({len(dcrr)} class row(s))"
                )
        if fam_done and dcrr_done:
            break
    return out


def clear_resolved_rider_hints(tariffs: list[ExtractedTariff]) -> None:
    """Drop riders_referenced_not_shown / missing_fields once riders are folded.

    Prevents needs_review from staying lit solely because the model listed
    FAM/DSM before the batch stacking step resolved them (R8b).
    """
    for t in tariffs:
        applied = " ".join(
            str(c.get(k) or "")
            for c in (t.components or [])
            if isinstance(c, dict)
            for k in ("tier_label", "period_label", "component_type")
        )
        applied_l = applied.lower()
        energy_all_in = any(
            isinstance(c, dict)
            and str(c.get("component_type") or "").lower() == "energy"
            and "all-in" in str(c.get("tier_label") or "").lower()
            for c in (t.components or [])
        )
        remaining = []
        for hint in list(getattr(t, "riders_referenced_not_shown", None) or []):
            h = str(hint).lower()
            resolved = False
            if re.search(r"\bfam\b|fuel\s+adjust", h) and (
                "fam" in applied_l or energy_all_in
            ):
                resolved = True
            if re.search(r"\bdsm\b|\bdcrr\b", h) and (
                "dsm" in applied_l or "dcrr" in applied_l or energy_all_in
            ):
                resolved = True
            if re.search(r"\bstorm\b|\bscrr\b", h) and (
                "storm" in applied_l or "scrr" in applied_l or energy_all_in
            ):
                resolved = True
            # Sch 100 is the applicability *map*, not a priced rider. A bare
            # delivery "all-in" label must NOT clear it (R14) — only a priced
            # Sch 1xx fold (or explicit +riders annotation) does.
            if re.search(r"schedule\s*100\b", h):
                if re.search(
                    r"(?:schedule|sch)\s*1(?!00)\d{2}|all-in\s*\+riders|\+riders",
                    applied_l,
                ):
                    resolved = True
            elif re.search(r"schedule\s*(1\d{2})", h):
                num = re.search(r"schedule\s*(1\d{2})", h).group(1)
                if num in applied_l or f"sch {num}" in applied_l:
                    resolved = True
                elif energy_all_in and re.search(
                    r"all-in\s*\+riders|\+riders|included",
                    applied_l,
                ):
                    resolved = True
            # Sibling base energy after NL 1.1S / 1.2DS salvage (R12).
            if re.search(
                r"rate\s*no\.?\s*1\.[12]d?\b.*(?:energy|base)|"
                r"base\s+energy\s+charge|"
                r"1\.1\s+domestic\s+energy|"
                r"1\.2d?\s+domestic",
                h,
                re.I,
            ):
                if any(
                    isinstance(c, dict)
                    and str(c.get("component_type") or "").lower() == "energy"
                    for c in (t.components or [])
                ):
                    resolved = True
            if not resolved:
                remaining.append(hint)
        t.riders_referenced_not_shown = remaining
        # Soften missing_fields that only named now-resolved rider amounts.
        mf = []
        for field in list(getattr(t, "missing_fields", None) or []):
            fl = str(field).lower()
            if re.search(r"\b(?:fam|dsm|storm)\b", fl) and not remaining:
                if re.search(r"rider\s*amounts?|amounts?", fl):
                    continue
            # Deterministic Sch 7 pending hint — clear once Sch 1xx folded (R14).
            if fl == "referenced_riders_pending" and not remaining:
                continue
            # Salvaged sibling base ENERGY is now on the tariff (R12).
            if re.search(
                r"base\s+energy|not\s+printed\s+in\s+content|"
                r"rate\s*1\.[12]d?\b.*(?:base|energy)|"
                r"basic\s+customer\s+charge\s+for\s+rate\s*1\.1",
                fl,
                re.I,
            ):
                if any(
                    isinstance(c, dict)
                    and str(c.get("component_type") or "").lower() == "energy"
                    for c in (t.components or [])
                ):
                    continue
            mf.append(field)
        t.missing_fields = mf
        # Soft-clear needs_review when the only gap was pending riders now folded.
        if (
            getattr(t, "needs_review", False)
            and not remaining
            and not mf
        ):
            t.needs_review = False


def fetch_and_extract_referenced_riders(
    tariffs: list[ExtractedTariff],
    utility_name: str,
    state: str,
    website_url: str = "",
    existing_pages: list[RatePage] | None = None,
    *,
    max_docs: int = MAX_RIDER_DOCS_FETCH,
    stats: dict | None = None,
) -> tuple[list[ExtractedTariff], list[RatePage], list[str]]:
    """Fetch official rider docs referenced by residential extracts but missing.

    Bounded (default 6 docs). Prefers the utility's own current tariff pages
    over regulator dockets; rejects documents whose year is clearly stale
    (~2+ years old). When a fetched page is mostly links (no ¢ amounts),
    follows one hop to same-domain tariff PDFs within the cap. Extracts each
    page in rider mode. Returns (extra_tariffs, pages_fetched, unresolved_hints).
    """
    if stats is None:
        stats = {}
    hints = _rider_search_hints(tariffs)
    if not hints:
        return [], [], []

    allowed_domains = _official_domains_for_rider_fetch(
        website_url, tariffs, existing_pages,
    )
    utility_domains = _utility_domains_for_rider_fetch(
        website_url, tariffs, existing_pages,
    )
    # Prefer a non-CDN website domain for site: queries; else first utility.
    site_query_domain = ""
    if website_url and not is_generic_host(website_url) and not _is_third_party_domain(website_url):
        site_query_domain = registrable_domain(normalize_host(website_url) or "")
    if not site_query_domain and utility_domains:
        site_query_domain = next(iter(sorted(utility_domains)))
    if not site_query_domain and allowed_domains:
        site_query_domain = next(iter(sorted(allowed_domains)))

    existing_urls = {
        (p.url or "").split("?")[0].rstrip("/").lower()
        for p in (existing_pages or [])
        if getattr(p, "url", None)
    }
    for t in tariffs:
        if t.source_url:
            existing_urls.add(t.source_url.split("?")[0].rstrip("/").lower())

    # (url, hint) pairs — preserve which hint drove the fetch for rider mode.
    # Reserve slots for one-hop PDF follows (NSP FAM link hubs) so search
    # results (OPUC filings) cannot fill the entire cap first.
    pge_like = (
        "portlandgeneral.com" in (site_query_domain or "")
        or "portlandgeneral.com" in (utility_domains or set())
        or re.search(r"portland\s+general|\bpge\b", utility_name or "", re.I)
    )
    # PGE: index/seed pages use the normal rider-doc cap; Sched_1xx PDFs
    # have their own budget (MAX_PGE_SCH1XX_FETCH ≥ Sch 100's 26 applicable).
    sch1xx_budget = MAX_PGE_SCH1XX_FETCH if pge_like else 0
    if pge_like and any(re.search(r"schedule\s*1\d{2}", h, re.I) for h in hints):
        # Keep room for a handful of index pages + Sch 100 itself.
        max_docs = max(max_docs, 6)
    hop_reserve = min(2, max_docs // 3) if max_docs >= 3 else 1
    if pge_like:
        hop_reserve = max(hop_reserve, 2)
    search_cap = max(1, max_docs - hop_reserve)

    candidate_urls: list[tuple[str, str]] = []
    seen_url: set[str] = set()

    # Rider tables already in the main tariff book (NSP FAM ~p.65) — no
    # separate FAM URL needed.
    book_extras = extract_riders_from_main_tariff_book(existing_pages, hints)

    # PGE: seed the tariff-index pages so Sch 1xx links are harvested even
    # when Brave only returns OPUC filings for Schedule 125.
    if pge_like and any(re.search(r"schedule\s*1\d{2}", h, re.I) for h in hints):
        for idx_url in _PGE_TARIFF_INDEX_URLS:
            key = idx_url.split("?")[0].rstrip("/").lower()
            if key not in existing_urls and key not in seen_url:
                seen_url.add(key)
                candidate_urls.append((idx_url, "Schedule 1xx tariff index"))

    for hint in hints:
        queries = []
        if site_query_domain:
            queries.append(f'site:{site_query_domain} {hint}')
            # Explicit PDF-oriented query for FAM / Sch 1xx on the utility site.
            if re.search(r"\bfam\b|schedule\s*1\d{2}|dsm|dcrr", hint, re.I):
                queries.append(f'site:{site_query_domain} {hint} filetype:pdf')
        queries.append(f'"{utility_name}" {hint} {state}'.strip())
        for q in queries:
            try:
                results = brave_search(q, count=5)
            except Exception as e:
                log.info(f"    Rider-doc search failed for {q[:60]}: {e}")
                continue
            ranked = sorted(
                results,
                key=lambda r: _rider_search_result_rank_key(
                    r,
                    allowed_domains=allowed_domains,
                    utility_domains=utility_domains,
                ),
            )
            for r in ranked:
                url = (r.get("url") or "").strip()
                title = str(r.get("title") or "")
                desc = str(r.get("description") or "")
                if not url or _is_third_party_domain(url) or is_generic_host(url):
                    continue
                if _is_stale_rider_document(url, title, desc):
                    log.info(f"    Rider-doc skip stale: {url[:70]}")
                    continue
                # Prefer utility domain; reject regulator dockets when a
                # utility domain is known (PGE UE 394 vs portlandgeneral.com).
                if utility_domains:
                    if not _url_in_allowed_domains(url, utility_domains):
                        if _is_regulator_filing_url(url, title):
                            log.info(f"    Rider-doc skip regulator docket: {url[:70]}")
                            continue
                        if allowed_domains and not _url_in_allowed_domains(url, allowed_domains):
                            continue
                elif allowed_domains:
                    if not _url_in_allowed_domains(url, allowed_domains):
                        continue
                key = url.split("?")[0].rstrip("/").lower()
                if key in existing_urls or key in seen_url:
                    continue
                seen_url.add(key)
                candidate_urls.append((url, hint))
                if len(candidate_urls) >= search_cap:
                    break
            if len(candidate_urls) >= search_cap:
                break
        if len(candidate_urls) >= search_cap:
            break

    stats["rider_docs_candidates"] = len(candidate_urls)
    if not candidate_urls and not book_extras:
        log.info(
            f"    Rider-doc fetch: {len(hints)} hint(s) unresolved "
            f"(no official URLs found)"
        )
        return [], [], hints

    def _fetch_one(url: str) -> RatePage | None:
        try:
            if url.lower().split("?")[0].endswith(".pdf"):
                page = _fetch_as_pdf_via_download(url)
            else:
                page = _fetch_and_parse(url)
                if page is None and url.lower().endswith(".pdf"):
                    page = _fetch_as_pdf_via_download(url)
            return page
        except Exception as e:
            log.info(f"    Rider-doc fetch failed {url[:70]}: {e}")
            return None

    fetched: list[tuple[RatePage, str]] = []
    for url, hint in candidate_urls[:max_docs]:
        page = _fetch_one(url)
        if page and page.content and len(page.content.strip()) > 100:
            if _is_stale_rider_document(page.url, page.title or "", ""):
                log.info(f"    Rider-doc skip stale after fetch: {page.url[:70]}")
                continue
            fetched.append((page, hint))
            log.info(f"    Rider-doc fetched: {url[:70]} ({len(page.content)} chars)")

    # One-hop + Sch 1xx harvest: follow PDF/tariff links from link-hub pages
    # (NSP FAM) and from tariff-index HTML (PGE Sch 1xx), within the doc cap.
    hop_domains = utility_domains or allowed_domains
    hop_candidates: list[tuple[str, str]] = []
    hop_seen: set[str] = set()
    # URL map built from page-data so Sch 100's applicability list can be
    # resolved to concrete Sched_NNN.pdf Contentful URLs.
    sch_url_map: dict[str, str] = {}
    sch100_map_logged: set[str] = set()

    def _is_sch1xx_pdf_url(url: str, hint: str = "") -> bool:
        if re.search(r"Sched_1\d{2}\.pdf", url or "", re.I):
            return True
        return bool(re.search(r"schedule\s*1\d{2}", hint or "", re.I))

    def _enqueue_hop(url: str, hint: str) -> None:
        key = url.split("?")[0].rstrip("/").lower()
        if key in existing_urls or key in seen_url or key in hop_seen:
            return
        is_sch = pge_like and _is_sch1xx_pdf_url(url, hint)
        if is_sch:
            n_sch = sum(
                1 for u, h in hop_candidates if _is_sch1xx_pdf_url(u, h)
            )
            if n_sch >= sch1xx_budget:
                return
        else:
            # Non-Sch-1xx hops still share the general rider-doc cap.
            if len(fetched) + len(hop_candidates) >= max_docs + sch1xx_budget:
                return
        hop_seen.add(key)
        hop_candidates.append((url, hint))

    pages_for_hop: list[tuple[RatePage, str]] = list(fetched)
    for p in existing_pages or []:
        if getattr(p, "content", None) and getattr(p, "url", None):
            pages_for_hop.append((p, "existing"))

    wanted_sch = _pge_sch1xx_wanted_from_hints(hints) if pge_like else set()
    # Always include Sch 100 (applicability map) for PGE.
    if pge_like:
        wanted_sch = set(wanted_sch) | {"100"}

    for page, hint in pages_for_hop:
        page_url = getattr(page, "url", "") or ""
        page_body = page.content or ""
        # Gatsby page-data.json / archive HTML: harvest ctfassets Sched_1xx.
        if pge_like and (
            "page-data.json" in page_url
            or "ctfassets.net" in page_body
            or "Sched_" in page_body
            or "price-summaries" in page_url
            or "rates-and-regulatory/tariff" in page_url
        ):
            # Re-fetch raw body when content was stripped to plain text.
            # Use this module's sync fetch_page — monitor.fetch_page does not
            # exist (ImportError was previously swallowed → truncated URL map).
            raw = page_body
            if "page-data.json" in page_url or (
                "Sched_" not in page_body and "ctfassets" not in page_body
            ):
                raw = _refetch_pge_index_raw(page_url) or page_body
            sch_url_map.update(_pge_sch1xx_url_map_from_text(raw))
            # Prefer Sch 100 first (applicability map), then hint-wanted.
            prefer = ["100"] + sorted(n for n in wanted_sch if n != "100")
            for sch_url in _harvest_pge_sch1xx_from_text(
                raw, wanted=set(prefer) or None, limit=MAX_PGE_SCH1XX_FETCH + 2,
            ):
                m = re.search(r"Sched_(\d{3})\.pdf", sch_url, re.I)
                hop_hint = f"Schedule {m.group(1)}" if m else "Schedule 1xx"
                _enqueue_hop(sch_url, hop_hint)
        # Sch 100 applicability → enqueue every priced Sch 1xx for Sch 7.
        # Only the real Sch 100 map (not Sched_007's "See Schedule 100…"
        # footnote) — R14 logged 0 applicable three times on false matches.
        if pge_like and _is_pge_sch100_applicability_page(page_body, page_url):
            _enqueue_from_sch100_map(
                page_body,
                sch_url_map=sch_url_map,
                enqueue=_enqueue_hop,
                base_schedule="7",
                log_once=sch100_map_logged,
            )
        if (getattr(page, "page_type", "") or "").lower() == "pdf":
            # PDF content may still be Sch 100 text from pdftotext-style extract.
            if pge_like and _is_pge_sch100_applicability_page(page_body, page_url):
                _enqueue_from_sch100_map(
                    page_body,
                    sch_url_map=sch_url_map,
                    enqueue=_enqueue_hop,
                    base_schedule="7",
                    log_once=sch100_map_logged,
                )
            continue
        # Always harvest numbered Schedule 1xx links from index-like HTML.
        for sch_url in _harvest_sch1xx_links_from_html(
            page.content or "",
            base_url=page.url or "",
            allowed_domains=hop_domains | {"ctfassets.net"} if pge_like else hop_domains,
        ):
            hop_hint = "Schedule 1xx"
            m = re.search(r"schedule[-_\s]?1(\d{2})", sch_url, re.I)
            if m:
                hop_hint = f"Schedule 1{m.group(1)}"
            _enqueue_hop(sch_url, hop_hint)
        # FAM / rider link hubs: follow same-domain tariff PDFs (use page.links).
        if _page_is_rider_link_hub(page):
            for hop_url in _rider_page_one_hop_links(
                page, allowed_domains=hop_domains,
            ):
                _enqueue_hop(hop_url, hint)

    # If Sch 100 URL is known but not yet fetched, enqueue it first.
    if pge_like and "100" in sch_url_map:
        _enqueue_hop(sch_url_map["100"], "Schedule 100")

    # PGE fallback: OPUC filings only when the current Sched_125 PDF is
    # missing from the URL map — otherwise a live run extracts Sch 125 twice
    # (OPUC + current PDF) and wastes an LLM call (R11).
    if pge_like and wanted_sch and "125" not in sch_url_map and len(
        [1 for u, h in hop_candidates if _is_sch1xx_pdf_url(u, h)]
    ) < 2:
        _enqueue_hop(
            "https://edocs.puc.state.or.us/efdocs/UBA/ue452uba342429171.pdf",
            "Schedule 125",
        )

    sch1xx_fetched = 0
    for url, hint in hop_candidates:
        is_sch = pge_like and _is_sch1xx_pdf_url(url, hint)
        if is_sch:
            if sch1xx_fetched >= sch1xx_budget:
                continue
        elif len(fetched) >= max_docs + sch1xx_fetched:
            # General (non-Sch-1xx) hops exhausted their share.
            if not is_sch:
                continue
        page = _fetch_one(url)
        if page and page.content and len(page.content.strip()) > 100:
            if _is_stale_rider_document(page.url, page.title or "", ""):
                continue
            key = (page.url or url).split("?")[0].rstrip("/").lower()
            seen_url.add(key)
            fetched.append((page, hint))
            if is_sch:
                sch1xx_fetched += 1
            log.info(f"    Rider-doc one-hop fetched: {url[:70]} ({len(page.content)} chars)")
            # After fetching Sch 100, expand hops from its applicability map.
            if pge_like and _is_pge_sch100_applicability_page(
                page.content or "", page.url or url
            ):
                _enqueue_from_sch100_map(
                    page.content or "",
                    sch_url_map=sch_url_map,
                    enqueue=_enqueue_hop,
                    base_schedule="7",
                    log_once=sch100_map_logged,
                )

    stats["rider_docs_fetched"] = len(fetched)
    if not fetched and not book_extras:
        return [], [], hints

    extra: list[ExtractedTariff] = list(book_extras)
    for page, hint in fetched:
        try:
            # Index / page-data hubs have no prices — never send to LLM (R12).
            if _is_pge_tariff_index_page(page, hint=hint):
                log.info(
                    f"    Skipping rider LLM for index page {page.url[:70]}"
                )
                continue
            # Prefer deterministic PGE Sched_1xx table parse (no LLM $).
            # None → not a PGE schedule page (fall through);
            # list (empty or not) → recognized: never LLM (R12 — zero / % /
            # per-bill / Sch 100 / Sch 125 TOD all covered).
            det = _try_deterministic_pge_sch1xx_extract(page, hint=hint)
            if det is not None:
                if det:
                    extra.extend(det)
                else:
                    log.info(
                        f"    Deterministic Sch 1xx: no ¢/kWh rows for "
                        f"{page.url[:70]} — skipping LLM"
                    )
                continue
            if re.search(r"FUEL\s+ADJUSTMENT\s+MECHANISM\s*\(FAM\)\s*TARIFF", page.content or "", re.I):
                fam = extract_nsp_fam_aa_ba_from_text(page.content or "")
                for et in fam:
                    et.source_url = page.url
                if fam:
                    extra.extend(fam)
                    continue
            extra.extend(
                _extract_rider_document(page, utility_name, state, hint=hint)
            )
        except Exception as e:
            log.warning(f"    Rider-doc extraction failed for {page.url[:70]}: {e}")

    useful = _filter_useful_rider_extracts(extra)
    # Book extras are already adjustment-shaped; keep them even if filter is strict.
    if book_extras:
        for et in book_extras:
            if et not in useful:
                useful.append(et)
    unresolved = hints if not useful else []
    stats["rider_docs_extracted"] = len(useful)
    log.info(
        f"    Rider-doc fetch: {len(fetched)} page(s), "
        f"{len(useful)} rider extract(s)"
    )
    return useful, [p for p, _h in fetched], unresolved


_DOC_EFFECTIVE_RE = re.compile(
    r"Effective\s+for\s+service\b.{0,120}?(?:on\s+and\s+after|after)\s+"
    r"([A-Za-z]+\s+\d{1,2},\s+\d{4})",
    re.I | re.S,
)
_COMBINED_TARIFF_BOOK_RE = re.compile(
    r"all[_-]?tariffs|compiled\s+tariff|complete\s+tariff\s+book|"
    r"p\.?u\.?c\.?\s+oregon\s+no\.?\s*e-1[0-8]\b",
    re.I,
)


def _document_effective_date(text: str = "", url: str = "") -> date | None:
    """Best-effort document 'effective for service' date from text or URL."""
    blob = f"{text or ''}\n{url or ''}"
    dates: list[date] = []
    for m in _DOC_EFFECTIVE_RE.finditer(blob):
        d = _parse_effective_date_str(m.group(1))
        if d:
            dates.append(d)
    # Advice / schedule PDFs sometimes put YYYYMMDD in the URL path.
    for m in re.finditer(r"(20\d{2})[-_/]?(\d{2})[-_/]?(\d{2})", url or ""):
        try:
            dates.append(date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
        except ValueError:
            continue
    return max(dates) if dates else None


def _is_combined_tariff_book(url: str = "", content: str = "") -> bool:
    """True for compiled multi-schedule books (e.g. PGE all_tariffs_56_.pdf)."""
    blob = f"{url or ''}\n{(content or '')[:4000]}"
    if _COMBINED_TARIFF_BOOK_RE.search(blob):
        return True
    if re.search(r"all_tariffs", url or "", re.I):
        return True
    return False


def _is_individual_pge_schedule_pdf(url: str = "", content: str = "") -> bool:
    """True for a single PGE Sched_NNN.pdf (not the compiled book)."""
    if re.search(r"Sched_0*\d{1,3}\.pdf", url or "", re.I):
        return True
    if re.search(r"SCHEDULE\s+7\b", content or "", re.I) and re.search(
        r"RESIDENTIAL\s+SERVICE", content or "", re.I
    ):
        # Individual Sch 7 sheets are short; consolidated books are huge.
        if content and len(content) < 80_000 and not _is_combined_tariff_book(url, content):
            return True
    return False


def _phase3_page_rank_key(page: RatePage) -> tuple:
    """Lower is better: fresher individual schedule PDFs beat old combined books.

    R12: PGE's current Sched_007 (Jul 2026) must outrank all_tariffs_56_.pdf
    (Jan 2020 E-18 book) when both are in the candidate set.
    """
    url = page.url or ""
    content = page.content or ""
    eff = _document_effective_date(content[:8000], url)
    # Negate date ordinal so newer sorts first; missing → 0 (after dated docs).
    date_score = -(eff.toordinal()) if eff else 0
    combined = 1 if _is_combined_tariff_book(url, content) else 0
    individual = 0 if _is_individual_pge_schedule_pdf(url, content) else 1
    depth = -urlparse(url).path.count("/")
    return (combined, individual, date_score, depth)


def _prefer_extract_over_existing(
    existing: ExtractedTariff,
    new: ExtractedTariff,
) -> bool:
    """True when ``new`` should replace ``existing`` for the same dedupe key.

    Prefer a newer document effective_date even when the older extract has
    more components (R12: current flat Sch 7 beats the 2020 tiered book).
    """
    new_eff = _parse_effective_date(getattr(new, "effective_date", None))
    old_eff = _parse_effective_date(getattr(existing, "effective_date", None))
    if new_eff and old_eff and new_eff > old_eff:
        return True
    if new_eff and not old_eff:
        return True
    if old_eff and new_eff and new_eff < old_eff:
        return False
    # Same / undated: keep richer extract.
    return len(new.components or []) > len(existing.components or [])


def _drop_stale_combined_pages_when_fresher_schedule_exists(
    pages: list[RatePage],
) -> list[RatePage]:
    """Drop older combined tariff books when a fresher Sched_007 is present.

    Never use a document with an older effective date when a newer one for
    the same schedule is available (R12).
    """
    sch7_dates: list[date] = []
    for p in pages:
        if not _is_individual_pge_schedule_pdf(p.url or "", p.content or ""):
            continue
        if not re.search(r"Sched_0*7\b|SCHEDULE\s+7\b", f"{p.url}\n{(p.content or '')[:2000]}", re.I):
            continue
        d = _document_effective_date((p.content or "")[:8000], p.url or "")
        if d:
            sch7_dates.append(d)
    if not sch7_dates:
        return pages
    newest = max(sch7_dates)
    kept: list[RatePage] = []
    for p in pages:
        if not _is_combined_tariff_book(p.url or "", p.content or ""):
            kept.append(p)
            continue
        book_eff = _document_effective_date((p.content or "")[:8000], p.url or "")
        if book_eff and book_eff >= newest:
            kept.append(p)
            continue
        # Undated or older combined book — skip when a fresher Sch 7 exists.
        log.info(
            f"    Skipping stale combined tariff book {p.url[:70]} "
            f"(eff={book_eff}; fresher Sched_007 eff={newest})"
        )
    return kept


def _is_pge_tariff_index_page(page: RatePage, hint: str = "") -> bool:
    """True for PGE schedule-index / page-data hubs (no prices — skip LLM)."""
    url = (page.url or "").lower()
    h = (hint or "").lower()
    if "page-data.json" in url:
        return True
    if "tariff index" in h or "schedule 1xx tariff index" in h:
        return True
    if any(idx.lower().rstrip("/") == url.rstrip("/") for idx in _PGE_TARIFF_INDEX_URLS):
        return True
    if re.search(r"/tariff/?$", url) and "sched_" not in url:
        return True
    # page-data / archive HTML that only lists PDF links.
    content = page.content or ""
    if "page-data" in url or "price-summaries-archive" in url:
        return True
    if content.lstrip().startswith("{") and "ctfassets.net" in content and "Sched_" in content:
        return True
    return False


def _utility_looks_like_pge(utility_name: str = "", website_url: str = "") -> bool:
    blob = f"{utility_name or ''} {website_url or ''}".lower()
    return bool(
        "portlandgeneral.com" in blob
        or re.search(r"portland\s+general|\bpge\b", blob)
    )


def fetch_pge_schedule_url_map(
    *,
    index_raw: str | None = None,
) -> dict[str, str]:
    """Build Sched_NNN → PDF URL map from PGE tariff index page-data/HTML."""
    mapping: dict[str, str] = {}
    texts: list[str] = []
    if index_raw:
        texts.append(index_raw)
    else:
        for idx_url in _PGE_TARIFF_INDEX_URLS:
            raw = _refetch_pge_index_raw(idx_url)
            if raw:
                texts.append(raw)
    for raw in texts:
        mapping.update(_pge_sch1xx_url_map_from_text(raw))
    return mapping


def fetch_pge_current_sch7_page(
    *,
    url_map: dict[str, str] | None = None,
    fetch_pdf=_fetch_as_pdf_via_download,
) -> RatePage | None:
    """Download the current Sched_007.pdf from the schedule index URL map."""
    umap = url_map if url_map is not None else fetch_pge_schedule_url_map()
    url = umap.get("007") or umap.get("7")
    if not url:
        return None
    page = fetch_pdf(url)
    if page and page.content and len(page.content.strip()) > 200:
        page.title = page.title or "Schedule 7 Residential Service"
        return page
    return None


def prefer_current_individual_schedule_pages(
    utility_name: str,
    pages: list[RatePage],
    *,
    website_url: str = "",
    url_map: dict[str, str] | None = None,
    fetch_pdf=_fetch_as_pdf_via_download,
) -> list[RatePage]:
    """Prefer a current individual residential schedule over an older book.

    R13: Phase 1 often lands on PGE ``all_tariffs_56_.pdf`` (2020). Phase 2
    then only downloads that PDF, so the R12 fresher-doc preference never
    sees Sched_007. Pull Sched_007 from the schedule index and put it first;
    drop older combined books once the current sheet is present.
    """
    if not _utility_looks_like_pge(utility_name, website_url):
        return list(pages or [])
    pages = list(pages or [])
    already = any(
        _is_individual_pge_schedule_pdf(p.url or "", p.content or "")
        and re.search(r"Sched_0*7\b|SCHEDULE\s+7\b", f"{p.url}\n{(p.content or '')[:1500]}", re.I)
        for p in pages
    )
    if not already:
        sch7 = fetch_pge_current_sch7_page(url_map=url_map, fetch_pdf=fetch_pdf)
        if sch7:
            log.info(
                f"  Preferring current individual Sched_007 over combined book: "
                f"{sch7.url[:80]}"
            )
            pages = [sch7] + pages
        else:
            log.info("  PGE schedule index: Sched_007 URL not found / fetch failed")
    pages = _drop_stale_combined_pages_when_fresher_schedule_exists(pages)
    return pages


def resolve_pge_primary_rate_url(
    utility_name: str,
    existing_url: str = "",
    *,
    website_url: str = "",
    url_map: dict[str, str] | None = None,
) -> tuple[str, list[str]]:
    """Return (primary, alts) preferring current Sched_007 over a combined book.

    Used when Phase 1 / search returns an older ``all_tariffs_*.pdf``. The
    individual schedule becomes primary; the combined book is demoted to an
    alternate (and later dropped once Sched_007 is fetched).
    """
    if not _utility_looks_like_pge(utility_name, website_url):
        return existing_url or "", []
    umap = url_map if url_map is not None else fetch_pge_schedule_url_map()
    sch7_url = umap.get("007") or umap.get("7") or ""
    if not sch7_url:
        return existing_url or "", []
    if not existing_url:
        return sch7_url, []
    if _is_combined_tariff_book(existing_url, ""):
        log.info(
            f"  PGE: demoting combined tariff book to alternate; "
            f"primary → Sched_007"
        )
        alts = [existing_url] if existing_url != sch7_url else []
        return sch7_url, alts
    if re.search(r"Sched_0*7\.pdf", existing_url, re.I):
        return existing_url, []
    # Existing is some other page — keep it, but offer Sched_007 first as alt.
    return existing_url, [sch7_url]


def parse_pge_sch125_adjustment_rates(text: str) -> list[dict]:
    """Parse Sch 125 ADJUSTMENT RATES table (flat Schedule 7 + 7-TOD periods).

    Current sheet (Advice 26-24): Schedule 7 5.619; 7-TOD on/mid/off
    12.868 / 5.555 / 3.416. Returns adjustment component dicts in ¢/kWh.
    """
    if not text or not re.search(r"SCHEDULE\s+125\b", text, re.I):
        return []
    # Restrict to the ADJUSTMENT RATES block when present.
    m = re.search(r"ADJUSTMENT\s+RATES\b", text, re.I)
    block = text[m.start(): m.start() + 2500] if m else text
    out: list[dict] = []

    def _add(val: float, label: str, period: str = "") -> None:
        row = {
            "component_type": "adjustment",
            "unit": "¢/kWh",
            "rate_value": val,
            "tier_label": label,
            "included_in_energy": False,
            "rider_scope": "all_customers",
        }
        if period:
            row["period_label"] = period
        out.append(row)

    # Flat Schedule 7 row: "7 5.619" (not 7-TOD).
    flat = re.search(
        r"(?im)^\s*7(?!\s*-?\s*TOD)\s+(\d+\.\d+)\b",
        block,
    )
    if flat:
        _add(
            float(flat.group(1)),
            "Schedule 7 (Residential) Schedule 125 adjustment",
        )

    # 7-TOD multi-period block.
    tod = re.search(
        r"7\s*-?\s*TOD\s+On[\s-]*Peak\s+Period\s+(\d+\.\d+)\s*"
        r"Mid[\s-]*Peak\s+Period\s+(\d+\.\d+)\s*"
        r"Off[\s-]*Peak\s+Period\s+(\d+\.\d+)",
        block,
        re.I | re.S,
    )
    if tod:
        _add(float(tod.group(1)), "Schedule 7-TOD Schedule 125 adjustment",
             "7-TOD On-Peak Period")
        _add(float(tod.group(2)), "Schedule 7-TOD Schedule 125 adjustment",
             "7-TOD Mid-Peak Period")
        _add(float(tod.group(3)), "Schedule 7-TOD Schedule 125 adjustment",
             "7-TOD Off-Peak Period")
    return out


def _parse_pge_sch7_charge_total(section: str) -> float | None:
    """Sum Transmission + Distribution + Energy ¢/kWh lines, or a single Charge."""
    # Prefer an explicit "X Charge Y.YYY ¢ per kWh" total line (TOD periods).
    m = re.search(
        r"(?:On|Mid|Off)[\s-]*Peak\s+Charge\s+(\d+\.\d+)\s*¢\s*per\s*kWh",
        section,
        re.I,
    )
    if m:
        return float(m.group(1))
    parts = []
    for label in (
        r"Transmission\s+and\s+Related\s+Services\s+Charge",
        r"Distribution\s+Charge",
        r"Energy\s+Charge",
    ):
        m = re.search(
            rf"{label}\s+(\d+\.\d+)\s*¢\s*per\s*kWh",
            section,
            re.I,
        )
        if m:
            parts.append(float(m.group(1)))
    if len(parts) >= 3:
        return round(sum(parts), 3)
    return None


def _parse_pge_sch7_rider_references(text: str) -> list[str]:
    """Parse Sched_007 footnotes like 'See Schedule 100 for applicable adjustments'.

    Returns e.g. ``["Schedule 100"]``. Falls back to Schedule 100 when the
    sheet is clearly Sch 7 residential (the current tariff always points
    there) so the rider-fetch step cannot be skipped (R14).
    """
    out: list[str] = []
    seen: set[str] = set()
    for m in re.finditer(
        r"see\s+schedule\s+(\d+)\s+for\s+applicable\s+adjustments",
        text or "",
        re.I,
    ):
        hint = f"Schedule {m.group(1)}"
        key = hint.lower()
        if key not in seen:
            seen.add(key)
            out.append(hint)
    if not out and re.search(r"SCHEDULE\s+7\b", text or "", re.I):
        out = ["Schedule 100"]
    return out


def extract_pge_sch7_from_text(
    text: str,
    *,
    source_url: str = "",
) -> list[ExtractedTariff]:
    """Deterministic PGE Schedule 7 Default + TOD (no separate EV plan).

    Current E-19 / Advice 26-24 sheet: flat Default 11.289¢; TOD on/mid/off
    30.263 / 11.143 / 5.514 with on-peak 5–9 pm weekdays. Whole-premise and
    EV-only share the same TOD price — do not fabricate a separate EV plan.

    Both plans carry ``riders_referenced_not_shown`` from the sheet's
    "See Schedule 100 for applicable adjustments" footnote so the rider
    fetch step runs (R14).
    """
    if not text or not re.search(r"SCHEDULE\s+7\b", text, re.I):
        return []
    if not re.search(r"RESIDENTIAL\s+SERVICE", text, re.I):
        return []
    # Skip adjustment schedules that only mention Schedule 7 as applicable.
    if re.search(r"SCHEDULE\s+1\d{2}\b", text[:800], re.I) and not re.search(
        r"ENERGY\s+PRICE\s+PLANS|RESIDENTIAL\s+SERVICE\s+PRICE\s+PLAN",
        text,
        re.I,
    ):
        return []

    eff = _document_effective_date(text[:6000], source_url)
    eff_s = eff.isoformat() if eff else ""
    rider_hints = _parse_pge_sch7_rider_references(text)
    sch7_desc_suffix = (
        f" {rider_hints[0]} for applicable adjustments."
        if rider_hints
        else ""
    )

    # Fixed charge (single-family basic).
    fixed = None
    m = re.search(
        r"Single[\s-]*Family\s+Home\s+\$?\s*(\d+(?:\.\d+)?)",
        text,
        re.I,
    )
    if m:
        fixed = float(m.group(1))

    out: list[ExtractedTariff] = []

    # --- Default (flat) plan ---
    # Stop at the TOD portfolio heading — NOT at "SCHEDULE 7 (Continued)",
    # which appears on every subsequent sheet header.
    default_m = re.search(
        r"RESIDENTIAL\s+SERVICE\s+PRICE\s+PLAN\s*\(DEFAULT\s+PLAN\)(.*?)"
        r"TIME[\s-]*OF[\s-]*DAY\s*\(TOD\)\s+PORTFOLIO\s+OPTION",
        text,
        re.I | re.S,
    )
    default_section = default_m.group(1) if default_m else text[:3500]
    # Flat sheet lists T&D + Energy separately (no single "Peak Charge").
    parts = []
    for lab in (
        r"Transmission\s+and\s+Related\s+Services\s+Charge",
        r"Distribution\s+Charge",
        r"Energy\s+Charge",
    ):
        mm = re.search(rf"{lab}\s+(\d+\.\d+)\s*¢\s*per\s*kWh", default_section, re.I)
        if mm:
            parts.append(float(mm.group(1)))
    default_cents = round(sum(parts), 3) if len(parts) >= 3 else None
    if default_cents is not None:
        comps: list[dict] = []
        if fixed is not None:
            comps.append({
                "component_type": "fixed",
                "unit": "$/month",
                "rate_value": fixed,
                "tier_label": "Basic Charge Single-Family",
            })
        comps.append({
            "component_type": "energy",
            "unit": "¢/kWh",
            "rate_value": default_cents,
            "tier_label": (
                f"all-in: transmission + distribution + energy = {default_cents}"
            ),
        })
        out.append(ExtractedTariff(
            name="Schedule 7 Residential Service Price Plan (Default Plan)",
            customer_class="residential",
            rate_type="flat",
            code="7",
            effective_date=eff_s,
            source_url=source_url,
            components=comps,
            extraction_tier="deterministic",
            energy_scope="bundled",
            description=(
                "Deterministic parse of PGE Schedule 7 Default Plan."
                + sch7_desc_suffix
            ),
            riders_referenced_not_shown=list(rider_hints),
            needs_review=bool(rider_hints),
            missing_fields=(
                ["referenced_riders_pending"] if rider_hints else []
            ),
        ))

    # --- TOD portfolio (whole premise or EV — same price; one plan only) ---
    tod_m = re.search(
        r"TIME[\s-]*OF[\s-]*DAY\s*\(TOD\)\s+PORTFOLIO\s+OPTION(.*?)(?:SPECIAL\s+CONDITIONS|Advice\s+No\.|$)",
        text,
        re.I | re.S,
    )
    if tod_m:
        tod_section = tod_m.group(1)
        on_c = re.search(
            r"On[\s-]*Peak\s+Charge\s+(\d+\.\d+)\s*¢\s*per\s*kWh",
            tod_section,
            re.I,
        )
        mid_c = re.search(
            r"Mid[\s-]*Peak\s+Charge\s+(\d+\.\d+)\s*¢\s*per\s*kWh",
            tod_section,
            re.I,
        )
        off_c = re.search(
            r"Off[\s-]*Peak\s+Charge\s+(\d+\.\d+)\s*¢\s*per\s*kWh",
            tod_section,
            re.I,
        )
        if on_c and mid_c and off_c:
            on_v, mid_v, off_v = (
                float(on_c.group(1)),
                float(mid_c.group(1)),
                float(off_c.group(1)),
            )
            # Hours from the On- and Off-Peak Hours section (current: 5–9 pm).
            # Fall back to 17:00–21:00 / 07:00–17:00 / 21:00–07:00 weekdays.
            on_start, on_end = "17:00", "21:00"
            mid_start, mid_end = "07:00", "17:00"
            off_start, off_end = "21:00", "07:00"
            hm = re.search(
                r"On[\s-]*Peak\s+(\d{1,2}):(\d{2})\s*([ap])\.?m\.?\s+to\s+"
                r"(\d{1,2}):(\d{2})\s*([ap])\.?m\.?",
                tod_section,
                re.I,
            )

            def _to_24(h: int, minute: int, ampm: str) -> str:
                h = h % 12
                if ampm.lower().startswith("p"):
                    h += 12
                return f"{h:02d}:{minute:02d}"

            if hm:
                on_start = _to_24(int(hm.group(1)), int(hm.group(2)), hm.group(3))
                on_end = _to_24(int(hm.group(4)), int(hm.group(5)), hm.group(6))
            hm = re.search(
                r"Mid[\s-]*Peak\s+(\d{1,2}):(\d{2})\s*([ap])\.?m\.?\s+to\s+"
                r"(\d{1,2}):(\d{2})\s*([ap])\.?m\.?",
                tod_section,
                re.I,
            )
            if hm:
                mid_start = _to_24(int(hm.group(1)), int(hm.group(2)), hm.group(3))
                mid_end = _to_24(int(hm.group(4)), int(hm.group(5)), hm.group(6))
            hm = re.search(
                r"Off[\s-]*Peak\s+(\d{1,2}):(\d{2})\s*([ap])\.?m\.?\s+to\s+"
                r"(\d{1,2}):(\d{2})\s*([ap])\.?m\.?",
                tod_section,
                re.I,
            )
            if hm:
                off_start = _to_24(int(hm.group(1)), int(hm.group(2)), hm.group(3))
                off_end = _to_24(int(hm.group(4)), int(hm.group(5)), hm.group(6))

            comps = []
            if fixed is not None:
                comps.append({
                    "component_type": "fixed",
                    "unit": "$/month",
                    "rate_value": fixed,
                    "tier_label": "Basic Charge Single-Family",
                })

            def _energy(val: float, label: str, start: str, end: str, day: str) -> dict:
                return {
                    "component_type": "energy",
                    "unit": "¢/kWh",
                    "rate_value": val,
                    "period_label": (
                        f"{label} (all-in: transmission + distribution + energy)"
                    ),
                    "period_start_time": start,
                    "period_end_time": end,
                    "day_type": day,
                }

            comps.extend([
                _energy(on_v, "On-Peak", on_start, on_end, "weekday"),
                _energy(mid_v, "Mid-Peak", mid_start, mid_end, "weekday"),
                _energy(off_v, "Off-Peak", off_start, off_end, "weekday"),
                # Weekends + holidays are all Off-Peak (sheet wording).
                _energy(off_v, "Off-Peak", "00:00", "00:00", "weekend"),
                _energy(off_v, "Off-Peak", "00:00", "00:00", "holiday"),
            ])
            out.append(ExtractedTariff(
                name="Schedule 7 Time-of-Use Portfolio Option (Whole Premises)",
                customer_class="residential",
                rate_type="tou",
                code="7-TOD",
                effective_date=eff_s,
                source_url=source_url,
                components=comps,
                extraction_tier="deterministic",
                energy_scope="bundled",
                description=(
                    "Deterministic parse of PGE Schedule 7 TOD. "
                    "EV-only charging uses the same prices — no separate plan."
                    + sch7_desc_suffix
                ),
                riders_referenced_not_shown=list(rider_hints),
                needs_review=bool(rider_hints),
                missing_fields=(
                    ["referenced_riders_pending"] if rider_hints else []
                ),
            ))
    return out


def _try_deterministic_pge_sch7_extract(page: RatePage) -> list[ExtractedTariff] | None:
    """Parse current PGE Sched_007 without an LLM when the sheet is clear."""
    content = page.content or ""
    url = page.url or ""
    if not (
        re.search(r"Sched_0*7\b", url, re.I)
        or (
            re.search(r"SCHEDULE\s+7\b", content[:1500], re.I)
            and re.search(r"RESIDENTIAL\s+SERVICE\s+PRICE\s+PLAN", content, re.I)
        )
    ):
        return None
    if _is_combined_tariff_book(url, content) and len(content) > 80_000:
        # Huge compiled books are not the current individual schedule.
        return None
    plans = extract_pge_sch7_from_text(content, source_url=url)
    return plans if plans else None


def _try_deterministic_pge_sch1xx_extract(
    page: RatePage,
    *,
    hint: str = "",
) -> list[ExtractedTariff] | None:
    """Parse PGE Sched_1xx ¢/kWh tables without an LLM when the text is clear.

    Returns None when the page is not a recognizable PGE adjustment schedule
    (caller falls through to LLM rider mode). Recognized pages always return
    a list (possibly empty) so zero / percentage / per-bill schedules skip
    the LLM (R12). Sch 100 (applicability only) yields []. Sch 125 returns
    flat + 7-TOD period rows when present.
    """
    content = page.content or ""
    if not re.search(r"SCHEDULE\s+1\d{2}\b", content, re.I) and not re.search(
        r"Sched_1\d{2}", (page.url or ""), re.I
    ):
        return None
    m = re.search(r"SCHEDULE\s+(1\d{2})\b", content, re.I) or re.search(
        r"Sched_(1\d{2})", (page.url or ""), re.I
    )
    if not m:
        return None
    num = m.group(1)
    if num == "100" or re.search(r"SUMMARY\s+OF\s+APPLICABLE\s+ADJUSTMENTS", content, re.I):
        return []  # map only — no prices

    # Sch 125: flat Schedule 7 + 7-TOD multi-period (R12 deterministic).
    if num == "125":
        amounts = parse_pge_sch125_adjustment_rates(content)
        if amounts:
            return [ExtractedTariff(
                name="Schedule 125 Net Variable Power Cost Adjustment - Schedule 7 Residential",
                customer_class="residential",
                rate_type="flat",
                code="125",
                source_url=page.url,
                components=amounts,
                extraction_tier="rider_doc",
                energy_scope="bundled",
                description="Deterministic parse of PGE Schedule 125 for Schedule 7.",
            )]
        # Recognized as Sch 125 but no ADJUSTMENT RATES table parsed —
        # fall through to LLM (None). Do NOT return []: that would skip
        # Sonnet on short/synthetic rider pages that still have a ¢/kWh
        # amount outside the standard table (wave6 CDN fetch regression).
        return None

    amounts = parse_pge_sch1xx_kwh_amounts_for_schedule(content, schedule="7")
    if not amounts:
        # Zero / % / per-bill schedules (103, 106, 108, …) — skip LLM (R12).
        return []
    # Drop pure-zero rows (no price impact), but keep First/Over block zeros
    # so Sch 102's Over 0.000 still marks higher tiers as uncushioned (R10).
    kept = []
    for a in amounts:
        lab = str(a.get("tier_label") or "").lower()
        is_block = bool(re.search(r"\b(?:first|over)\b", lab))
        try:
            rv = float(a.get("rate_value") or 0)
        except (TypeError, ValueError):
            continue
        if abs(rv) < 1e-12 and not is_block:
            continue
        kept.append(a)
    amounts = kept
    if not amounts:
        return []
    # If the only rows are zero Over without a First, nothing to price.
    if all(abs(float(a.get("rate_value") or 0)) < 1e-12 for a in amounts):
        return []
    for a in amounts:
        a["tier_label"] = f"Schedule {num} {a.get('tier_label') or ''}".strip()
    return [ExtractedTariff(
        name=f"Schedule {num} Adjustment - Schedule 7 Residential",
        customer_class="residential",
        rate_type="flat",
        code=num,
        source_url=page.url,
        components=amounts,
        extraction_tier="rider_doc",
        energy_scope="bundled",
        description=f"Deterministic parse of PGE Schedule {num} for Schedule 7.",
    )]


_INFORMATIONAL_MISSING_FIELDS = frozenset({
    "effective_date", "effective date", "fixed", "fixed_charge",
    "customer_charge", "minimum", "description", "code",
    "base_services_charge", "base_services_charge_amount",
    "monthly_service_charge", "service_charge", "basic_charge",
})


def _is_informational_missing_field(field: str) -> bool:
    """True for gaps that are not Mysa-critical by themselves."""
    raw = str(field or "").strip().lower()
    f = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
    if f in _INFORMATIONAL_MISSING_FIELDS:
        return True
    if "effective" in f and "date" in f:
        return True
    # Name/code gaps alone are not Mysa-critical (Pedernales flat, R12).
    if re.search(r"official\s+schedule\s+name|schedule\s+name\s+and\s+code|code\s+not\s+shown", raw):
        return True
    # A flat plan noting that TOU prices live on another page is not critical
    # for that flat plan itself (Pedernales, R12).
    if re.search(
        r"time[\s-]*of[\s-]*use.*(?:not\s+shown|varies|refer\s+to)|"
        r"tou\s+(?:base\s+)?(?:power\s+)?rate.*(?:not\s+shown|varies)",
        raw,
    ):
        return True
    # Fixed / customer / base services charge gaps are not Mysa-critical
    # (Mysa prices kWh; missing FIXED alone must not flag needs_review).
    if any(
        k in f
        for k in (
            "fixed_charge", "customer_charge", "service_charge",
            "base_services", "basic_charge", "monthly_charge",
            "minimum_charge",
        )
    ):
        return True
    return False


def _is_mysa_critical_review_reason(
    *,
    missing_fields: list[str] | None = None,
    riders_missing: list[str] | None = None,
    completeness_reasons: list[str] | None = None,
    computable_reasons: list[str] | None = None,
    energy_scope: str = "",
) -> bool:
    """needs_review only for Mysa-critical problems (R7).

    Critical: missing energy price, missing/broken TOU clock, missing season
    dates for seasonal plans, unresolved per-kWh rider, delivery/supply-only
    scope. NOT critical alone: effective_date, holiday wording, baseline kWh,
    fixed charges.
    """
    informational_extra = {
        "holiday", "holiday_calendar", "holiday_rows_require_calendar",
        "baseline", "baseline_kwh", "fixed", "fixed_charge", "customer_charge",
        "minimum", "description", "code",
    }
    for m in missing_fields or []:
        if _is_informational_missing_field(m):
            continue
        key = re.sub(r"[^a-z0-9]+", "_", str(m).strip().lower()).strip("_")
        if key in informational_extra or "holiday" in key or "baseline" in key:
            continue
        return True
    if riders_missing:
        return True
    critical_comp_prefixes = (
        "missing_energy",
        "tou_missing_clock",
        "tou_missing_day",
        "tou_gap",
        "tou_overlap",
        "tou_clock",
        "seasonal_missing_calendar",
        "energy_rate_not_numeric",
    )
    for r in list(completeness_reasons or []) + list(computable_reasons or []):
        s = str(r).lower()
        if "holiday" in s and "calendar" in s:
            continue  # holiday wording alone is not Mysa-critical
        if any(s.startswith(p) or f":{p}" in f":{s}" for p in critical_comp_prefixes):
            return True
        base = s.split(":", 1)[0]
        if base in {
            "missing_energy_rates",
            "tou_missing_clock_windows",
            "tou_missing_day_type",
            "tou_gap",
            "tou_overlap",
            "seasonal_missing_calendar_dates",
            "energy_rate_not_numeric",
        }:
            return True
    if energy_scope in ("delivery_only", "supply_only"):
        return True
    return False


def _hhmm_to_minutes(value) -> int | None:
    """Parse HH:MM / time / '7:00 a.m.' into minutes-from-midnight."""
    if value is None or value == "":
        return None
    if hasattr(value, "hour"):
        return int(value.hour) * 60 + int(value.minute)
    s = str(value).strip()
    m = re.match(r"^(\d{1,2}):(\d{2})", s)
    if m:
        return int(m.group(1)) * 60 + int(m.group(2))
    return None


def _minutes_to_hhmm(mins: int) -> str:
    mins = mins % (24 * 60)
    return f"{mins // 60:02d}:{mins % 60:02d}"


def _period_family(label: str) -> str:
    """Normalize a TOU period label to off/mid/partial/on/peak/other."""
    s = str(label or "").lower()
    if re.search(r"partial|mid[\s-]*peak|shoulder", s):
        return "partial"
    if re.search(r"off[\s-]*peak|overnight|super[\s-]*off", s):
        return "off"
    if re.search(r"on[\s-]*peak|peak", s) and "off" not in s and "partial" not in s:
        return "on"
    return "other"


def repair_one_hour_tou_gaps(
    components: list[dict],
    *,
    missing_fields: list[str] | None = None,
) -> tuple[list[dict], list[str]]:
    """Fill a single 60-minute TOU gap only from a *stated* period (R8).

    Prefer inserting/extending the period the document names for that hour
    (e.g. missing_fields "3-4 p.m. partial peak"). Never invent prices and
    never stretch a neighboring window of a different period family — that
    wrongly priced PG&E E-ELEC/EV2-A 3–4 pm as off-peak. Unknown gaps are
    left for the computable check to flag.
    """
    if not components:
        return components, []
    stated_partial = False
    for m in missing_fields or []:
        if re.search(r"3\s*[-–]\s*4|15:00|3\s*p\.?m", str(m), re.I) and re.search(
            r"partial", str(m), re.I
        ):
            stated_partial = True
            break

    groups: dict[tuple, list[int]] = {}
    for i, c in enumerate(components):
        if not isinstance(c, dict):
            continue
        if str(c.get("component_type") or "").lower() != "energy":
            continue
        start = _hhmm_to_minutes(c.get("period_start_time"))
        end = _hhmm_to_minutes(c.get("period_end_time"))
        if start is None or end is None:
            continue
        if start == end == 0:
            continue  # all-day sentinel
        day = str(c.get("day_type") or "all").lower()
        season = (
            c.get("season_start_month"), c.get("season_start_day"),
            c.get("season_end_month"), c.get("season_end_day"),
            str(c.get("season") or "").lower(),
        )
        groups.setdefault((day, season), []).append(i)

    notes: list[str] = []
    out = [dict(c) if isinstance(c, dict) else c for c in components]
    for (day, season), idxs in groups.items():
        intervals: list[tuple[int, int, int]] = []
        for i in idxs:
            s = _hhmm_to_minutes(out[i].get("period_start_time"))
            e = _hhmm_to_minutes(out[i].get("period_end_time"))
            if s is None or e is None:
                continue
            if e == 0 and s != 0:
                e = 24 * 60
            if e <= s:
                e += 24 * 60
            intervals.append((s, e, i))
        if len(intervals) < 1:
            continue
        intervals.sort()
        covered = list(intervals)
        for a, b in zip(covered, covered[1:]):
            gap = b[0] - a[1]
            if gap != 60:
                continue
            gap_start, gap_end = a[1], b[0]
            # Only fill when the document states which period owns the gap.
            donor_idx = None
            if stated_partial and gap_start % (24 * 60) == 15 * 60:
                # Prefer a printed partial-peak row in the same season/day.
                for i in idxs:
                    if _period_family(out[i].get("period_label") or "") == "partial":
                        donor_idx = i
                        break
            if donor_idx is None:
                # Same-family neighbor only (never stretch off into a
                # partial/on gap).
                fam_a = _period_family(out[a[2]].get("period_label") or "")
                fam_b = _period_family(out[b[2]].get("period_label") or "")
                if fam_a == fam_b and fam_a != "other":
                    out[a[2]]["period_end_time"] = _minutes_to_hhmm(gap_end)
                    notes.append(
                        f"tou_clock_gap_repaired:{day}:"
                        f"{_minutes_to_hhmm(gap_start)}-{_minutes_to_hhmm(gap_end)}"
                    )
                # else: leave the gap — computable check will flag.
                continue
            # Insert a one-hour window cloned from the stated period's price.
            donor = dict(out[donor_idx])
            donor["period_start_time"] = _minutes_to_hhmm(gap_start)
            donor["period_end_time"] = _minutes_to_hhmm(gap_end)
            if not donor.get("period_label"):
                donor["period_label"] = "Partial peak"
            out.append(donor)
            notes.append(
                f"tou_clock_gap_filled_from_stated:{day}:"
                f"{_minutes_to_hhmm(gap_start)}-{_minutes_to_hhmm(gap_end)}"
            )
    return out, notes


def flag_unresolved_external_riders(
    tariffs: list[ExtractedTariff],
    unresolved_hints: list[str],
) -> int:
    """Mark residential plans that still lack referenced per-kWh riders.

    Only runs when the bounded rider-doc fetch could not resolve the hints —
    we do not guess rider amounts. Flags needs_review only when the hint
    looks like a per-kWh price changer (FAM/DCRR/Sch 1xx), not generic noise.
    """
    if not unresolved_hints:
        return 0
    price_hints = [
        h for h in unresolved_hints
        if _RIDER_DOC_HINT_RE.search(h)
        or re.search(r"schedule\s*1\d{2}|fam|dsm|dcrr|pca|fuel|storm", h, re.I)
    ]
    if not price_hints:
        return 0
    flagged = 0
    for t in tariffs:
        if not _residential_needs_external_riders(t):
            continue
        t.needs_review = True
        flagged += 1
        log.info(
            f"    Unresolved external riders on '{t.name}' — needs_review "
            f"(hints: {', '.join(price_hints[:3])})"
        )
    return flagged


def enrich_tariffs_with_referenced_rider_docs(
    tariffs: list[ExtractedTariff],
    utility_name: str,
    state: str,
    website_url: str = "",
    pages: list[RatePage] | None = None,
    stats: dict | None = None,
) -> tuple[list[ExtractedTariff], list[RatePage]]:
    """Bounded official rider-doc fetch + merge before Phase 4."""
    if not tariffs:
        return tariffs, list(pages or [])
    extra, fetched, unresolved = fetch_and_extract_referenced_riders(
        tariffs,
        utility_name,
        state,
        website_url=website_url,
        existing_pages=pages,
        stats=stats,
    )
    merged = list(tariffs) + list(extra)
    flag_unresolved_external_riders(merged, unresolved)
    return merged, list(pages or []) + list(fetched)


# ---------------------------------------------------------------------------
# R18: one-utility plan reconcile (runs after every plan is all-in)
#   * optional add-on variants (… with Renewable Energy Rider) repaired from
#     their base plan when the extract under-folded a shared adder;
#   * guard: an optional variant priced below its base with no credit;
#   * same plan extracted twice (tariff book + marketing page, or a
#     temporary-price book + standard book) collapsed to ONE live copy;
#   * rider extracts naming a customer group with no extracted plan are
#     reported (NSP MURB) instead of vanishing silently.
# ---------------------------------------------------------------------------

_R18_PRICE_TOL = 0.0002  # $/kWh — 0.02¢ rounding slack

# Tail words that make "<base name> <tail>" a separate optional product.
_R18_VARIANT_TAIL_RE = re.compile(
    r"\b(?:with|rider|renewable|green|optional|option|solar|wind|"
    r"environmental|eco)\b",
    re.I,
)
# Tail words that make two prefix-related names DIFFERENT plans for the
# duplicate collapse (interim vs final, EV vs base, export, pilot, …).
_R18_DISTINCT_TAIL_RE = re.compile(
    r"\b(?:with|rider|renewable|green|optional|option|solar|wind|interim|"
    r"proposed|future|pilot|ev|vehicle|export|net|critical|cpp|demand|"
    r"time of use|time of day|tou|tod|super|lifeline|low income|senior|"
    r"employee|seasonal|heat|heating|water|controlled|interruptible)\b",
    re.I,
)
_R18_GENERIC_TOKENS = frozenset({
    "rate", "rates", "plan", "plans", "price", "pricing", "service",
    "residential", "standard", "tariff", "schedule", "domestic",
    "electric", "electricity", "for", "the", "and", "of", "no", "a",
})
_R18_MONTHS = (
    r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\.?"
)
_R18_WINDOW_RE = re.compile(
    rf"\b({_R18_MONTHS})\s*(\d{{4}})?\s*(?:-|–|—|to|through|thru)\s*"
    rf"({_R18_MONTHS})\s*,?\s*(\d{{4}})",
    re.I,
)
_R18_MONTH_NUM = {
    m: i for i, m in enumerate(
        ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep",
         "oct", "nov", "dec"), start=1,
    )
}


def _r18_norm(name: str) -> str:
    """Lowercase, punctuation → space, collapse whitespace."""
    n = re.sub(r"[^a-z0-9]+", " ", str(name or "").lower())
    return " ".join(n.split())


def _r18_utility_tokens(utility_name: str) -> set[str]:
    words = [w for w in _r18_norm(utility_name).split() if w]
    toks = {w for w in words if len(w) >= 3}
    initials = "".join(w[0] for w in words if w not in ("of", "the", "and"))
    if len(initials) >= 2:
        toks.add(initials)
    return toks


def _r18_strip_utility(norm: str, util_toks: set[str]) -> str:
    return " ".join(w for w in norm.split() if w not in util_toks)


def _r18_contains(longer: str, shorter: str) -> tuple[bool, str, str]:
    """Token-boundary containment; returns (hit, text before, text after)."""
    if not shorter or not longer:
        return False, "", ""
    hay = f" {longer} "
    needle = f" {shorter} "
    idx = hay.find(needle)
    if idx < 0:
        return False, "", ""
    before = " ".join(hay[:idx].split())
    after = " ".join(hay[idx + len(needle):].split())
    return True, before, after


def _r18_is_strong_alias(alias: str) -> bool:
    toks = alias.split()
    if len(toks) < 2:
        return False
    return any(t not in _R18_GENERIC_TOKENS for t in toks)


def _r18_aliases(t: ExtractedTariff, util_toks: set[str]) -> tuple[str, list[str]]:
    """(full normalized name w/o utility words, explicit parenthetical aliases)."""
    raw = str(t.name or "")
    full = _r18_strip_utility(_r18_norm(raw), util_toks)
    parens = [
        _r18_strip_utility(_r18_norm(p), util_toks)
        for p in re.findall(r"\(([^()]{3,})\)", raw)
    ]
    return full, [p for p in parens if p]


def _r18_same_plan_name(
    a: ExtractedTariff, b: ExtractedTariff, util_toks: set[str],
) -> bool:
    fa, pa = _r18_aliases(a, util_toks)
    fb, pb = _r18_aliases(b, util_toks)
    if not fa or not fb:
        return False
    if fa == fb:
        return True
    # Explicit marketed alias in parentheses ("E-23 … (SRP Basic Price Plan)")
    for alias, other_full, other_par in (
        *((p, fb, pb) for p in pa), *((p, fa, pa) for p in pb),
    ):
        if not _r18_is_strong_alias(alias):
            continue
        if alias == other_full or alias in other_par:
            return True
        hit, _, _ = _r18_contains(other_full, alias)
        if hit:
            return True
    # Plain containment: shorter full name inside the longer one, with a
    # tail that carries no product discriminator.
    shorter, longer = (fa, fb) if len(fa) <= len(fb) else (fb, fa)
    if not _r18_is_strong_alias(shorter):
        return False
    hit, before, after = _r18_contains(longer, shorter)
    if not hit:
        return False
    if _R18_DISTINCT_TAIL_RE.search(f"{before} {after}"):
        return False
    # A trailing number / letter-number ("Rate D" vs "Rate D 2") is a
    # different schedule; a LEADING code ("E-24 M-Power …") is not.
    return not re.search(r"\b[a-z]?\d+[a-z]?\b", after)


def _r18_energy_rows(t: ExtractedTariff) -> list[dict]:
    out = []
    for c in t.components or []:
        if not isinstance(c, dict):
            continue
        if str(c.get("component_type") or "").lower() != "energy":
            continue
        unit = str(c.get("unit") or "$/kWh").strip().lower()
        if unit not in ("$/kwh", "kwh", ""):
            continue
        try:
            float(c.get("rate_value"))
        except (TypeError, ValueError):
            continue
        out.append(c)
    return out


def _r18_day(c: dict) -> str:
    d = str(c.get("day_type") or "").strip().lower()
    return d if d not in ("", "none", "all days", "everyday", "every day") else "all"


def _r18_fine_key(c: dict) -> tuple:
    return (
        c.get("season_start_month"), c.get("season_end_month"), _r18_day(c),
        c.get("period_start_time"), c.get("period_end_time"),
        c.get("tier_min_kwh"), c.get("tier_max_kwh"),
    )


def _r18_coarse_key(c: dict) -> tuple:
    return (
        c.get("season_start_month"), c.get("season_end_month"),
        c.get("period_start_time"), c.get("period_end_time"),
        c.get("tier_min_kwh"), c.get("tier_max_kwh"),
    )


def _r18_fine_map(t: ExtractedTariff) -> dict | None:
    """fine key → $/kWh; None when one key carries two different prices."""
    out: dict = {}
    for c in _r18_energy_rows(t):
        k = _r18_fine_key(c)
        v = float(c.get("rate_value"))
        if k in out and abs(out[k] - v) > _R18_PRICE_TOL:
            return None
        out[k] = v
    return out


def _r18_coarse_map(t: ExtractedTariff) -> dict:
    out: dict = {}
    for c in _r18_energy_rows(t):
        out.setdefault(_r18_coarse_key(c), set()).add(round(float(c.get("rate_value")), 5))
    return out


def _r18_included_adders(t: ExtractedTariff) -> list[float]:
    vals = []
    for c in t.components or []:
        if not isinstance(c, dict):
            continue
        if str(c.get("component_type") or "").lower() != "adjustment":
            continue
        if not c.get("included_in_energy"):
            continue
        unit = str(c.get("unit") or "").strip().lower()
        if unit not in ("$/kwh", "kwh"):
            continue
        try:
            vals.append(round(float(c.get("rate_value")), 6))
        except (TypeError, ValueError):
            continue
    return sorted(vals)


def _r18_has_kwh_credit(t: ExtractedTariff) -> bool:
    for c in t.components or []:
        if not isinstance(c, dict):
            continue
        ctype = str(c.get("component_type") or "").lower()
        unit = str(c.get("unit") or "").strip().lower()
        if ctype not in ("adjustment", "credit") or unit not in ("$/kwh", "kwh"):
            continue
        try:
            if float(c.get("rate_value")) < 0:
                return True
        except (TypeError, ValueError):
            continue
        if re.search(r"credit|discount|rebate", str(c.get("tier_label") or c.get("period_label") or ""), re.I):
            return True
    return False


def _r18_add_missing(t: ExtractedTariff, reason: str) -> None:
    missing = list(getattr(t, "missing_fields", None) or [])
    if reason not in missing:
        missing.append(reason)
    t.missing_fields = missing
    t.needs_review = True


def _r18_note(t: ExtractedTariff, key: str, value) -> None:
    notes = dict(getattr(t, "confidence_notes", None) or {})
    notes[key] = value
    t.confidence_notes = notes


def _r18_variant_pairs(tariffs: list[ExtractedTariff]) -> list[tuple[ExtractedTariff, ExtractedTariff]]:
    """(base, variant) where variant name = base name + optional-product tail."""
    pairs = []
    norms = [(_r18_norm(t.name), t) for t in tariffs]
    for nb, base in norms:
        if not nb:
            continue
        for nv, var in norms:
            if var is base or len(nv) <= len(nb):
                continue
            if base.customer_class != var.customer_class:
                continue
            if not nv.startswith(nb + " "):
                continue
            tail = nv[len(nb):]
            if _R18_VARIANT_TAIL_RE.search(tail):
                pairs.append((base, var))
    return pairs


def reconcile_optional_variant_prices(tariffs: list[ExtractedTariff]) -> int:
    """Repair an optional rider variant priced below base + its own adders.

    Pedernales R17: the "… with Renewable Energy Rider" TOU extract listed
    the right adders (delivery, TCOS, REC) but folded TCOS as 0.000688
    instead of 0.020688 into off-/mid-peak, leaving those periods 2.0¢ under
    the base plan. When the variant carries every base adder plus extra
    positive adders, has the exact same period grid, and at least one
    period already equals base + extras (proving the shared base), every
    period priced BELOW base + extras is reset to that value. Periods priced
    above it are left alone (flagged by the guard instead). Returns the
    number of variants repaired.
    """
    repaired = 0
    for base, var in _r18_variant_pairs(tariffs):
        if (base.source_url or "") != (var.source_url or "") or not base.source_url:
            continue
        bmap, vmap = _r18_fine_map(base), _r18_fine_map(var)
        if not bmap or not vmap or set(bmap) != set(vmap):
            continue
        b_add, v_add = _r18_included_adders(base), _r18_included_adders(var)
        extra = list(v_add)
        ok = True
        for a in b_add:
            if a in extra:
                extra.remove(a)
            else:
                ok = False
                break
        if not ok or not extra or any(x <= 0 or x > 0.05 for x in extra):
            continue
        add = round(sum(extra), 6)
        expected = {k: round(bmap[k] + add, 6) for k in bmap}
        anchors = [k for k in vmap if abs(vmap[k] - expected[k]) <= _R18_PRICE_TOL]
        low = [k for k in vmap if vmap[k] < expected[k] - 0.0005]
        if not anchors or not low:
            continue
        old = {}
        for c in _r18_energy_rows(var):
            k = _r18_fine_key(c)
            if k in low:
                old_v = float(c.get("rate_value"))
                old[str(round(old_v, 6))] = expected[k]
                c["rate_value"] = expected[k]
        _r18_note(var, "variant_price_repaired", {
            "base_plan": base.name,
            "extra_adders_per_kwh": add,
            "periods_fixed": len(low),
            "old_to_new": old,
        })
        log.warning(
            f"    Variant reconcile on '{var.name}': {len(low)} period(s) "
            f"below base '{base.name}' + {add:.6f} $/kWh adders — reset "
            f"({old})"
        )
        repaired += 1
    return repaired


def flag_optional_priced_below_base(tariffs: list[ExtractedTariff]) -> int:
    """needs_review when an optional variant is cheaper than its base plan
    in any matching period and carries no per-kWh credit explaining it."""
    flagged = 0
    for base, var in _r18_variant_pairs(tariffs):
        bmap, vmap = _r18_fine_map(base), _r18_fine_map(var)
        if not bmap or not vmap:
            continue
        common = set(bmap) & set(vmap)
        below = [k for k in common if vmap[k] < bmap[k] - _R18_PRICE_TOL]
        if not below or _r18_has_kwh_credit(var):
            continue
        _r18_add_missing(var, "optional_plan_below_base_unexplained")
        _r18_note(var, "optional_plan_below_base", {
            "base_plan": base.name, "periods_below": len(below),
        })
        log.warning(
            f"    Optional '{var.name}' priced below base '{base.name}' in "
            f"{len(below)} period(s) with no credit — needs_review"
        )
        flagged += 1
    return flagged


def _r18_price_vintage(t: ExtractedTariff) -> str | None:
    """'temporary', 'standard', or None (unknown)."""
    name = str(t.name or "").lower()
    url = str(t.source_url or "").lower()
    desc = str(t.description or "").lower()
    if re.search(r"temporar", url) or re.search(r"\btemporar", name):
        return "temporary"
    if re.search(
        r"\b(?:exclud\w*|without|not\s+includ\w*|before|after|apply\s+from)"
        r"\b[^.;]{0,40}\btemporar", desc,
    ):
        return "standard"
    if re.search(
        r"\b(?:includ\w*|reflect\w*|with)\b[^.;]{0,60}\btemporar", desc
    ) or re.search(
        r"\btemporar\w*\s+(?:price|rate|fuel|decrease|reduction|discount|"
        r"credit|increase|surcharge)s?\b[^.;]{0,30}\b(?:in\s+effect|applied|"
        r"included)", desc,
    ):
        return "temporary"
    return None


def _r18_temporary_window(t: ExtractedTariff, siblings: list[ExtractedTariff]) -> dict | None:
    """Temporary price window stated near 'temporary' in this plan's text or
    a plan from the same document. None when the dates are not clear."""
    texts = [t] + [
        s for s in siblings
        if s is not t and s.source_url and s.source_url == t.source_url
    ]
    for s in texts:
        blob = " ".join(
            [str(s.description or "")]
            + [
                str(c.get("period_label") or c.get("tier_label") or "")
                for c in (s.components or []) if isinstance(c, dict)
            ]
        )
        for m in re.finditer(r"temporar", blob, re.I):
            seg = blob[max(0, m.start() - 80): m.end() + 120]
            w = _R18_WINDOW_RE.search(seg)
            if not w:
                continue
            m_end = _R18_MONTH_NUM.get(w.group(3)[:3].lower())
            year = w.group(4)
            if not m_end or not year:
                continue
            return {
                "window": w.group(0).strip(),
                "until": f"{year}-{m_end:02d}",
            }
    return None


def _r18_keep_score(t: ExtractedTariff) -> tuple:
    url = str(t.source_url or "").lower()
    official = 1 if (
        url.endswith(".pdf") or ".pdf?" in url
        or re.search(r"tariff|ratebook|rate-book|schedule|rider", url)
    ) else 0
    n_days = len({_r18_day(c) for c in _r18_energy_rows(t)})
    return (
        official,
        0 if t.needs_review else 1,
        n_days,
        len(_r18_energy_rows(t)),
        len(t.components or []),
        1 if t.code else 0,
    )


def _r18_same_structure(a: ExtractedTariff, b: ExtractedTariff) -> tuple[bool, list]:
    ca, cb = _r18_coarse_map(a), _r18_coarse_map(b)
    if not ca or not cb:
        return False, []
    sa = {(k[0], k[1]) for k in ca}
    sb = {(k[0], k[1]) for k in cb}
    if sa != sb:
        return False, []
    common = set(ca) & set(cb)
    small = min(len(ca), len(cb))
    if not common or len(common) / small < 0.75:
        return False, []
    return True, sorted(common, key=str)


def _r18_prices_equal(a: ExtractedTariff, b: ExtractedTariff, common: list) -> bool:
    ca, cb = _r18_coarse_map(a), _r18_coarse_map(b)
    for k in common:
        va, vb = sorted(ca[k]), sorted(cb[k])
        if len(va) != len(vb) or any(abs(x - y) > _R18_PRICE_TOL for x, y in zip(va, vb)):
            return False
    return True


def _r18_energy_snapshot(t: ExtractedTariff, limit: int = 48) -> list[dict]:
    out = []
    for c in _r18_energy_rows(t)[:limit]:
        out.append({
            "season": c.get("season"),
            "months": [c.get("season_start_month"), c.get("season_end_month")],
            "day_type": c.get("day_type"),
            "hours": [c.get("period_start_time"), c.get("period_end_time")],
            "label": c.get("period_label") or c.get("tier_label"),
            "rate_value": c.get("rate_value"),
        })
    return out


def _r18_restamp_with_standard(tmp: ExtractedTariff, std: ExtractedTariff) -> bool:
    """Rewrite tmp's energy rows with std prices (per season value map).

    Used when the temporary copy has the richer clock/day grid (SRP E-28
    weekday/weekend rows) and the standard copy has the right prices. Each
    season's temp value must map to exactly one standard value through the
    shared clock windows, and every temp row must be covered.
    """
    std_c = _r18_coarse_map(std)
    vmap: dict = {}
    for c in _r18_energy_rows(tmp):
        k = _r18_coarse_key(c)
        if k not in std_c or len(std_c[k]) != 1:
            continue
        season = (k[0], k[1])
        tv = round(float(c.get("rate_value")), 5)
        sv = next(iter(std_c[k]))
        prev = vmap.get((season, tv))
        if prev is not None and abs(prev - sv) > _R18_PRICE_TOL:
            return False
        vmap[(season, tv)] = sv
    rows = _r18_energy_rows(tmp)
    for c in rows:
        k = _r18_coarse_key(c)
        if ((k[0], k[1]), round(float(c.get("rate_value")), 5)) not in vmap:
            return False
    for c in rows:
        k = _r18_coarse_key(c)
        c["rate_value"] = vmap[((k[0], k[1]), round(float(c.get("rate_value")), 5))]
    return True


def dedupe_same_plan_variants(
    tariffs: list[ExtractedTariff], utility_name: str = "",
) -> tuple[list[ExtractedTariff], list[dict]]:
    """Keep ONE live copy of a plan extracted more than once in a run.

    Same plan = same customer class, matching name (shared marketed alias or
    containment without a product discriminator), same season calendar and
    mostly the same clock windows. Identical prices → keep the richer
    official copy. Conflicting prices → prefer the standard price over a
    temporary/expiring one; the temporary prices are stored as a note with
    their window when the dates are clear, otherwise the kept plan is
    flagged. Conflicts with no temporary/standard signal keep one copy and
    flag it. Returns (kept, actions).
    """
    util_toks = _r18_utility_tokens(utility_name)
    alive = list(tariffs)
    actions: list[dict] = []
    changed = True
    while changed:
        changed = False
        for i in range(len(alive)):
            for j in range(i + 1, len(alive)):
                a, b = alive[i], alive[j]
                if a.customer_class != b.customer_class:
                    continue
                if not _r18_same_plan_name(a, b, util_toks):
                    continue
                same, common = _r18_same_structure(a, b)
                if not same:
                    continue
                ea = _parse_effective_date(getattr(a, "effective_date", None))
                eb = _parse_effective_date(getattr(b, "effective_date", None))
                if ea and eb and abs((ea - eb).days) > 31 and (
                    ea > date.today() or eb > date.today()
                ):
                    # Succession (current vs future-dated) — both are valid.
                    continue
                keep, drop, action = _r18_resolve_pair(a, b, common, alive)
                actions.append(action)
                log.info(
                    f"    Same-plan dedupe: kept '{keep.name}' "
                    f"({keep.source_url}), dropped '{drop.name}' "
                    f"({drop.source_url}) — {action['outcome']}"
                )
                alive = [t for t in alive if t is not drop]
                changed = True
                break
            if changed:
                break
    return alive, actions


def _r18_resolve_pair(a, b, common, siblings):
    action = {"plans": [a.name, b.name]}
    merged_from = lambda t: {"name": t.name, "source_url": t.source_url}
    if _r18_prices_equal(a, b, common):
        keep, drop = (a, b) if _r18_keep_score(a) >= _r18_keep_score(b) else (b, a)
        if not keep.code and drop.code:
            keep.code = drop.code
        prev = list((keep.confidence_notes or {}).get("duplicate_copies_merged") or [])
        _r18_note(keep, "duplicate_copies_merged", prev + [merged_from(drop)])
        action.update(outcome="identical prices — kept richer copy", kept=keep.name)
        return keep, drop, action

    va, vb = _r18_price_vintage(a), _r18_price_vintage(b)
    tmp = std = None
    if va == "temporary" and vb != "temporary":
        tmp, std = a, b
    elif vb == "temporary" and va != "temporary":
        tmp, std = b, a
    if tmp is None:
        da = _parse_effective_date(getattr(a, "effective_date", None))
        db = _parse_effective_date(getattr(b, "effective_date", None))
        if da and db and abs((da - db).days) >= 28:
            # R19: newest official effective date wins; older kept as a note.
            keep, drop = (a, b) if da > db else (b, a)
            _r18_note(keep, "older_copy_prices", {
                **merged_from(drop), "effective_date": str(drop.effective_date),
                "energy": _r18_energy_snapshot(drop),
            })
            action.update(outcome="price conflict — kept newest effective date", kept=keep.name)
            return keep, drop, action
        keep, drop = (a, b) if _r18_keep_score(a) >= _r18_keep_score(b) else (b, a)
        _r18_add_missing(keep, "duplicate_plan_price_conflict")
        _r18_note(keep, "duplicate_plan_other_prices", {
            **merged_from(drop), "energy": _r18_energy_snapshot(drop),
        })
        action.update(outcome="price conflict, vintage unknown — kept one, flagged", kept=keep.name)
        return keep, drop, action

    window = _r18_temporary_window(tmp, siblings)
    tmp_snapshot = {
        **merged_from(tmp), "energy": _r18_energy_snapshot(tmp),
        **(window or {}),
    }
    # Prefer the richer clock/day grid when it can carry standard prices.
    keep = std
    restamped = False
    if _r18_keep_score(tmp) > _r18_keep_score(std):
        before = [dict(c) for c in tmp.components or []]
        if _r18_restamp_with_standard(tmp, std):
            keep, restamped = tmp, True
            _r18_note(keep, "standard_prices_source_url", std.source_url)
            keep.description = (
                (str(keep.description or "").strip() + " ").lstrip()
                + "Energy prices are the standard (non-temporary) prices from "
                f"{std.source_url}; temporary prices are kept in notes."
            )
        else:
            tmp.components = before
    drop = std if keep is tmp else tmp
    _r18_note(keep, "temporary_prices", tmp_snapshot)
    if window:
        _r18_note(keep, "temporary_price_until", window["until"])
        outcome = f"price conflict — kept standard price; temporary stored (until {window['until']})"
    else:
        _r18_add_missing(keep, "temporary_price_dates_unclear")
        outcome = "price conflict — kept standard price; temporary dates unclear, flagged"
    if restamped:
        outcome += "; clock grid from the richer temporary copy"
    action.update(outcome=outcome, kept=keep.name)
    return keep, drop, action


def mark_temporary_only_plans(tariffs: list[ExtractedTariff]) -> int:
    """A surviving plan whose ONLY copy carries temporary prices: note the
    expiry when the dates are clear, otherwise flag it."""
    n = 0
    for t in tariffs:
        if _r18_price_vintage(t) != "temporary":
            continue
        if (t.confidence_notes or {}).get("standard_prices_source_url"):
            continue  # already restamped to standard prices
        window = _r18_temporary_window(t, tariffs)
        if window:
            _r18_note(t, "temporary_price_until", window["until"])
            _r18_note(t, "temporary_price_window", window["window"])
        else:
            _r18_add_missing(t, "temporary_price_dates_unclear")
        n += 1
    return n


def find_plans_possibly_not_extracted(
    all_in: list[ExtractedTariff], kept: list[ExtractedTariff],
) -> list[str]:
    """Customer groups named by rider extracts with no extracted plan.

    NSP R17: "FAM … - MURB" and "DCRR … - MURB / General" were absorbed but
    no MURB plan was extracted, so the MURB TOU plan vanished silently.
    """
    kept_names = " ".join(_r18_norm(t.name) for t in kept)
    generic = {"domestic", "domestic service", "residential", "general",
               "residential service", "all", "standard"}
    out: list[str] = []
    kept_ids = {id(t) for t in kept}
    for t in all_in:
        if id(t) in kept_ids or not _is_rider_only_tariff(t):
            continue
        name = str(t.name or "")
        if " - " not in name:
            continue
        suffix = name.rsplit(" - ", 1)[1]
        # "Domestic Service Tariff - DSM Rider" names a rider, not a group.
        if re.search(
            r"\b(?:rider|adjustment|charge|mechanism|credit|surcharge|"
            r"program|programme|recovery|fee)s?\b", suffix, re.I,
        ):
            continue
        for part in re.split(r"[/,&]", suffix):
            p = _r18_norm(part)
            if not p or p in generic:
                continue
            if f" {p} " in f" {kept_names} ":
                continue
            label = p.upper() if len(p) <= 5 else p
            if label not in out:
                out.append(label)
    return out


# ---------------------------------------------------------------------------
# R19: newest official document + optional bill credits
#   * document vintage from URL ("2025-Ratebook-with-2026-TCA") and from a
#     PDF's cover text ("Prices effective with the November 2023 Billing
#     Cycle");
#   * known-URL fallback picks the newest dated official rate document
#     (SRP: 2025/2026 ratebook, not the Nov 2023 ratebook.pdf);
#   * plans built from a document clearly older than another official one
#     known to the run are flagged (older_rate_document);
#   * negative "fixed" monthly credits (autopay / paperless) move to notes
#     instead of getting the whole plan rejected (Pedernales flat).
# ---------------------------------------------------------------------------

_R19_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7,
    "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
_R19_NOT_CURRENT_URL_RE = re.compile(
    r"propos|draft|filing|application|testimony|exhibit|docket|notice|"
    r"redline|archive|historic|superseded|sample[-_ ]?bill",
    re.I,
)
_R19_RATE_DOC_URL_RE = re.compile(
    r"\.pdf(?:$|\?)|rate[-_ ]?book|tariff|price[-_ ]?plan|rate[-_ ]?schedule|"
    r"schedule[-_ ]?of[-_ ]?rates",
    re.I,
)
# Months a plan's document must trail a newer official one before flagging.
_R19_OLDER_DOC_MONTHS = 18

# Per-run context set by run_pipeline (pages + known URLs) so phase 4 can
# compare document vintages. Reset at the start of every run.
_RUN_DOC_CONTEXT: dict = {}


def _r19_months(v: tuple[int, int] | None) -> int | None:
    return None if v is None else v[0] * 12 + (v[1] - 1)


def url_document_vintage(url: str, *, today: date | None = None) -> tuple[int, int] | None:
    """(year, month) from a document URL / filename, else None.

    Takes the newest plausible year in the path ("2025-Ratebook-with-2026-
    TCA" → 2026). A month name or ``_-_11_`` next to that year sets the
    month; otherwise January (conservative: never newer than stated).
    Future years and non-current documents (proposed / filing / sample bill)
    return None.
    """
    today = today or date.today()
    path = urlparse(str(url or "")).path
    path = re.sub(r"%20|[_\-+.]", " ", path).lower()
    best: tuple[int, int] | None = None
    for m in re.finditer(r"(?<!\d)(20\d{2})(?!\d)", path):
        y = int(m.group(1))
        if y < 2000 or y > today.year:
            continue
        month = 1
        around = path[max(0, m.start() - 12): m.end() + 12]
        mm = re.search(
            r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*", around,
        )
        if mm:
            month = _R19_MONTHS[mm.group(1)[:3]]
        else:
            num = re.search(rf"{y}\s+(0?[1-9]|1[0-2])\b", around)
            if num:
                month = int(num.group(1))
        cand = (y, month)
        if best is None or cand > best:
            best = cand
    # R20: compact dates — "effective-20261001" (YYYYMMDD) and
    # "effective-09012023" (MMDDYYYY).
    for m in re.finditer(r"(?<!\d)(20\d{2})(0[1-9]|1[0-2])([0-3]\d)(?!\d)", path):
        y, mo = int(m.group(1)), int(m.group(2))
        if 2000 <= y <= today.year and (best is None or (y, mo) > best):
            best = (y, mo)
    for m in re.finditer(r"(?<!\d)(0[1-9]|1[0-2])([0-3]\d)(20\d{2})(?!\d)", path):
        y, mo = int(m.group(3)), int(m.group(1))
        if 2000 <= y <= today.year and (best is None or (y, mo) > best):
            best = (y, mo)
    if best is None:
        # "April2015" / "Nov2012" glued forms
        for m in re.finditer(
            r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s?(20\d{2})", path,
        ):
            y = int(m.group(2))
            if 2000 <= y <= today.year:
                cand = (y, _R19_MONTHS[m.group(1)[:3]])
                if best is None or cand > best:
                    best = cand
    if best and best > (today.year, today.month):
        return None
    return best


_R19_TEXT_DATE_RE = re.compile(
    r"\beffective\b[^.]{0,40}?\b("
    r"jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\.?\s+"
    r"(?:(\d{1,2}),?\s+)?(20\d{2})",
    re.I,
)


def text_document_vintage(text: str, *, head_chars: int = 4000,
                          today: date | None = None) -> tuple[int, int] | None:
    """(year, month) of the newest 'effective <Month> [day,] <Year>' on the
    document's first page(s), else None. Future dates are ignored."""
    today = today or date.today()
    best = None
    for m in _R19_TEXT_DATE_RE.finditer(str(text or "")[:head_chars]):
        y = int(m.group(3))
        cand = (y, _R19_MONTHS[m.group(1)[:3].lower()])
        if cand > (today.year, today.month) or y < 2000:
            continue
        if best is None or cand > best:
            best = cand
    return best


def _r19_is_rate_doc_url(url: str) -> bool:
    u = str(url or "")
    return bool(_R19_RATE_DOC_URL_RE.search(u)) and not _R19_NOT_CURRENT_URL_RE.search(u)


def newest_dated_rate_document(urls, *, today: date | None = None,
                               min_year: int | None = None) -> tuple[str, tuple[int, int]] | None:
    """Newest URL-dated current rate document among ``urls`` (or None)."""
    today = today or date.today()
    best = None
    for u in urls or []:
        if not u or not _r19_is_rate_doc_url(u):
            continue
        v = url_document_vintage(u, today=today)
        if v is None or (min_year is not None and v[0] < min_year):
            continue
        if best is None or v > best[1]:
            best = (u, v)
    return best


def set_run_document_context(pages=None, known_urls=None, *, ctx=None) -> None:
    """Record this run's document vintages for phase-4 comparison."""
    from app.services.source_type import THIRD_PARTY, classify_source

    page_v: dict[str, tuple[int, int]] = {}
    for p in pages or []:
        url = getattr(p, "url", "") or ""
        if not url:
            continue
        if ctx is not None and classify_source(url, ctx).source_type == THIRD_PARTY:
            continue
        v = None
        if str(getattr(p, "page_type", "")).lower() == "pdf" or url.lower().split("?")[0].endswith(".pdf"):
            v = text_document_vintage(getattr(p, "content", "") or "")
        page_v[url] = v or url_document_vintage(url)
    known = []
    for u in known_urls or []:
        if not u:
            continue
        if ctx is not None and classify_source(u, ctx).source_type == THIRD_PARTY:
            continue
        known.append(u)
    pdf_mod = {u: _PDF_MODIFIED[u] for u in page_v if u in _PDF_MODIFIED}
    _RUN_DOC_CONTEXT.clear()
    _RUN_DOC_CONTEXT.update({"page_vintages": page_v, "known_urls": known, "pdf_modified": pdf_mod})


_R19_BOOK_URL_RE = re.compile(
    r"rate\s*book|tariff\s*book|price\s*plans?\b|schedule\s*of\s*rates|"
    r"rates\s*(?:and|&)\s*regulations|rates\s*rules|tariff\s*and\s*business",
    re.I,
)


def _r19_is_book_url(url: str) -> bool:
    path = re.sub(r"%20|[_\-+.]", " ", urlparse(str(url or "")).path)
    return bool(_R19_BOOK_URL_RE.search(path)) and not _R19_NOT_CURRENT_URL_RE.search(str(url or ""))


_R21_STALE_FLAG_MONTHS = 24       # "about 2 years": flag for review
_R21_STALE_INCOMPLETE_MONTHS = 36  # clearly outdated document: not Mysa-complete


def flag_stale_source_documents(
    tariffs: list[ExtractedTariff], doc_context: dict | None = None,
    *, today: date | None = None,
) -> int:
    """R21 fix 6: absolute document age.

    A plan's document date is the NEWEST of: the PDF's modified date, the
    date printed on its cover, the newest effective date printed for plans
    taken from it, and a date in its URL (a 2024 re-issue of a 2010 sheet is
    current). Older than ~2 years → ``stale_rate_document`` (needs_review).
    Older than 3 years on document-level evidence (PDF modified / cover /
    URL date) → also ``price_basis='stale_document'``: not Mysa-complete.
    Never changes prices.
    """
    ctxd = doc_context if doc_context is not None else _RUN_DOC_CONTEXT
    today = today or date.today()
    page_v = dict((ctxd or {}).get("page_vintages") or {})
    pdf_mod = dict((ctxd or {}).get("pdf_modified") or {})
    plan_dates: dict[str, list[tuple[int, int]]] = {}
    for t in tariffs:
        d = _parse_effective_date(getattr(t, "effective_date", None), today=today)
        if d and d <= today:
            plan_dates.setdefault(t.source_url or "", []).append((d.year, d.month))
    now_m = _r19_months((today.year, today.month))
    n = 0
    for t in tariffs:
        if _is_rider_only_tariff(t):
            continue
        src = t.source_url or ""
        doc_ev = [v for v in (pdf_mod.get(src), page_v.get(src), url_document_vintage(src, today=today)) if v]
        ev = doc_ev + plan_dates.get(src, [])
        if not ev:
            continue
        newest = max(ev)
        age = now_m - _r19_months(newest)
        if age <= _R21_STALE_FLAG_MONTHS:
            continue
        basis = "pdf_modified" if pdf_mod.get(src) == newest else (
            "document_date" if newest in doc_ev else "printed_effective_date")
        _r18_add_missing(t, "stale_rate_document")
        _r18_note(t, "stale_rate_document", {
            "plan_document": src,
            "document_date": f"{newest[0]}-{newest[1]:02d}",
            "age_months": age,
            "evidence": basis,
        })
        t.needs_review = True
        if age > _R21_STALE_INCOMPLETE_MONTHS and doc_ev and max(doc_ev) == newest:
            if (t.confidence_notes or {}).get("price_basis") in (None, "full"):
                _r18_note(t, "price_basis", "stale_document")
        log.warning(
            f"    '{t.name}': source document dated {newest[0]}-{newest[1]:02d} "
            f"({age} months old, {basis}) — stale, needs_review"
        )
        n += 1
    return n


def flag_older_source_documents(
    tariffs: list[ExtractedTariff], doc_context: dict | None = None,
    *, today: date | None = None,
) -> int:
    """needs_review when a plan's source document trails another official
    rate document available to this run by >= 18 months
    (``older_rate_document``).

    A document's date: its PDF cover text ("effective with the November
    2023 billing cycle"), else the newest effective_date of plans taken
    from it, else a date in its URL. Compared against: the other documents
    plans came from in this run, plus the newest dated rate BOOK among the
    utility's known URLs (rider / news / proposed documents never count).
    """
    ctxd = doc_context if doc_context is not None else _RUN_DOC_CONTEXT
    if not ctxd or not tariffs:
        return 0
    today = today or date.today()
    page_v = dict(ctxd.get("page_vintages") or {})

    by_src: dict[str, list[date]] = {}
    for t in tariffs:
        d = _parse_effective_date(getattr(t, "effective_date", None), today=today)
        if d:
            by_src.setdefault(t.source_url or "", []).append(d)

    def _doc_v(src: str):
        v = page_v.get(src)
        if v is None and by_src.get(src):
            d = max(by_src[src])
            v = (d.year, d.month)
        if v is None:
            v = url_document_vintage(src, today=today)
        return v

    src_v = {}
    for t in tariffs:
        src = t.source_url or ""
        if src and src not in src_v:
            src_v[src] = _doc_v(src)
    candidates = [(u, v) for u, v in src_v.items() if v]
    books = [u for u in (ctxd.get("known_urls") or []) if _r19_is_book_url(u)]
    nd = newest_dated_rate_document(books, today=today)
    if nd:
        candidates.append(nd)
    if not candidates:
        return 0
    newest_url, newest_v = max(candidates, key=lambda x: x[1])

    flagged = 0
    for t in tariffs:
        src = t.source_url or ""
        if src == newest_url:
            continue
        # Document date first (a 2026 book may restate a 2019 plan date).
        v = src_v.get(src)
        if v is None:
            d = _parse_effective_date(getattr(t, "effective_date", None), today=today)
            v = (d.year, d.month) if d else None
        if v is None:
            continue
        gap = _r19_months(newest_v) - _r19_months(v)
        if gap < _R19_OLDER_DOC_MONTHS:
            continue
        _r18_add_missing(t, "older_rate_document")
        _r18_note(t, "older_rate_document", {
            "plan_document": src,
            "plan_document_date": f"{v[0]}-{v[1]:02d}",
            "newer_official_document": newest_url,
            "newer_document_date": f"{newest_v[0]}-{newest_v[1]:02d}",
        })
        log.warning(
            f"    '{t.name}' comes from a document dated {v[0]}-{v[1]:02d}; a "
            f"newer official document ({newest_v[0]}-{newest_v[1]:02d}) is "
            f"available: {newest_url[:90]} — needs_review"
        )
        flagged += 1
    return flagged


_R19_CREDIT_LABEL_RE = re.compile(
    r"credit|discount|rebate|auto\s*-?pay|bank\s*draft|paperless|e-?bill|"
    r"optional|waiver",
    re.I,
)


def move_negative_fixed_credits_to_notes(t: ExtractedTariff) -> int:
    """Negative fixed / minimum monthly amounts are bill credits, not
    charges (Pedernales autopay −$1.50 / paperless −$1.00). Move them to
    ``confidence_notes['bill_credits']`` so the plan is not rejected for a
    negative fixed charge. Returns the number moved."""
    keep, moved = [], []
    for c in t.components or []:
        if not isinstance(c, dict):
            keep.append(c)
            continue
        ctype = str(c.get("component_type") or "").lower()
        try:
            rv = float(c.get("rate_value"))
        except (TypeError, ValueError):
            keep.append(c)
            continue
        if ctype in ("fixed", "minimum") and rv < 0:
            label = str(c.get("tier_label") or c.get("period_label") or "")
            moved.append({
                "label": label or "credit",
                "amount": rv,
                "unit": c.get("unit") or "$/month",
                "optional": bool(_R19_CREDIT_LABEL_RE.search(label)),
            })
            continue
        keep.append(c)
    if moved:
        t.components = keep
        notes = dict(getattr(t, "confidence_notes", None) or {})
        notes["bill_credits"] = list(notes.get("bill_credits") or []) + moved
        t.confidence_notes = notes
        log.info(
            f"    Bill credit(s) on '{t.name}' kept as notes, not charges: "
            f"{[m['label'][:40] for m in moved]}"
        )
    return len(moved)



# ---------------------------------------------------------------------------
# R20: supply / default-service price sheets (PSE&G BGS "price to compare").
#   * A supply-only plan (energy supply prices, no delivery charges) is never
#     stored on its own. When the same run has the matching delivery plan,
#     the two are combined into one plan labelled "delivery + default
#     supply"; otherwise the supply-only plan is dropped (reported).
#   * A supply sheet must be current: one dated more than 12 months ago, or
#     6+ months older than another official document in the run, is flagged
#     (supply_sheet_not_current).
# ---------------------------------------------------------------------------
_R20_SUPPLY_RE = re.compile(
    r"\bptc\b|price[-_ ]?to[-_ ]?compare|\bbgs\b|basic\s+generation|"
    r"default\s+(?:electric\s+)?(?:service|supply)|standard\s+offer|"
    r"\bsupply\s+(?:rate|price|charge)s?\b|electric\s+supply|"
    r"\bgeneration\s+(?:rate|charge|service)s?\b|\bsos\b|provider\s+of\s+last\s+resort",
    re.I,
)
_R20_DELIVERY_LABEL_RE = re.compile(
    r"distribution|delivery|customer\s+charge|service\s+charge|basic\s+(?:service\s+)?charge|"
    r"meter(?:ing)?\s+charge|monthly\s+charge|system\s+benefit|societal\s+benefit|"
    r"\bsbc\b|\bdsic\b|\bwires\b",
    re.I,
)
_R20_MISSING_DELIVERY_RE = re.compile(r"deliver|distribution", re.I)
_R20_MISSING_SUPPLY_RE = re.compile(
    r"supply|generation|\bbgs\b|commodity|energy\s+charge|\bgsc\b|price\s+to\s+compare|\bptc\b|"
    r"basic\s+service|default\s+service|standard\s+offer",
    re.I,
)
_R20_SUPPLY_STALE_MONTHS = 12
_R20_SUPPLY_TRAIL_MONTHS = 6


def is_supply_sheet_url(url: str) -> bool:
    """File name looks like a supply / price-to-compare sheet (not a full
    tariff book that merely mentions BGS)."""
    name = urlparse(str(url or "")).path.rstrip("/").rsplit("/", 1)[-1]
    name = re.sub(r"%20|[_\-+.]", " ", name)
    if re.search(r"tariff|rate\s*book|rates?\s+and\s+(?:rules|regulations)", name, re.I):
        return False
    return bool(_R20_SUPPLY_RE.search(name))


def _r20_labels(t: ExtractedTariff) -> str:
    out = []
    for c in t.components or []:
        if isinstance(c, dict):
            out.append(str(c.get("tier_label") or ""))
            out.append(str(c.get("period_label") or ""))
    return " ".join(out)


def _r20_has_delivery(t: ExtractedTariff) -> bool:
    for c in t.components or []:
        if not isinstance(c, dict):
            continue
        ctype = str(c.get("component_type") or "").lower()
        try:
            rv = float(c.get("rate_value") or 0)
        except (TypeError, ValueError):
            rv = 0.0
        label = f"{c.get('tier_label') or ''} {c.get('period_label') or ''}"
        if ctype in ("fixed", "minimum") and rv > 0:
            return True
        if _R20_DELIVERY_LABEL_RE.search(label):
            return True
    return False


def _r20_has_energy(t: ExtractedTariff) -> bool:
    return any(
        isinstance(c, dict) and str(c.get("component_type") or "").lower() == "energy"
        for c in t.components or []
    )


def is_supply_only_plan(t: ExtractedTariff) -> bool:
    """Energy supply prices with no delivery charges at all."""
    if not _r20_has_energy(t):
        return False
    # R21: the model's own energy_scope is authoritative ("Transmission
    # Service Charge" must not count as delivery — PPL GSC-1).
    if str(getattr(t, "energy_scope", "") or "") == "supply_only":
        return True
    if _r20_has_delivery(t):
        return False
    missing = " ".join(str(m) for m in (getattr(t, "missing_fields", None) or []))
    signal = " ".join([t.name or "", t.description or "", _r20_labels(t)])
    return bool(
        _R20_SUPPLY_RE.search(signal)
        or is_supply_sheet_url(t.source_url)
        or _R20_MISSING_DELIVERY_RE.search(missing)
    )


def is_delivery_only_plan(t: ExtractedTariff) -> bool:
    """Delivery charges present but the energy supply price is missing."""
    if str(getattr(t, "energy_scope", "") or "") == "delivery_only":
        return True
    if not _r20_has_delivery(t):
        return False
    missing = " ".join(str(m) for m in (getattr(t, "missing_fields", None) or []))
    energy = [c for c in t.components or [] if isinstance(c, dict)
              and str(c.get("component_type") or "").lower() == "energy"]
    if energy and all(
        _R20_DELIVERY_LABEL_RE.search(f"{c.get('tier_label') or ''} {c.get('period_label') or ''}")
        for c in energy
    ):
        return True
    if not energy:
        return True
    return bool(_R20_MISSING_SUPPLY_RE.search(missing)) and not _R20_SUPPLY_RE.search(_r20_labels(t))


def _r20_schedule_key(t: ExtractedTariff) -> set[str]:
    codes = extract_ratebook_codes(t.name, t.code)
    lead = re.match(r"\s*([A-Za-z]{1,4}(?:-?\d+(?:\.\d+)?)?)\s*(?:[-–—:(]|$)", t.name or "")
    if lead:
        codes.add(lead.group(1).lower())
    return {c for c in codes if c and c not in ("rate", "the")}


def _r20_pair(supply: ExtractedTariff, deliveries: list[ExtractedTariff]):
    sk = _r20_schedule_key(supply)
    if sk:
        hits = [d for d in deliveries if _r20_schedule_key(d) & sk]
        if len(hits) == 1:
            return hits[0]
        if hits:
            return None
    q = vintage_qualifiers(supply.name, supply.code) - {"bgs", "rscp", "supply", "default", "generation"}
    hits = [d for d in deliveries if q and q <= vintage_qualifiers(d.name, d.code)]
    return hits[0] if len(hits) == 1 else None


_R21_OPTIONAL_SUPPLY_RE = re.compile(
    r"time[-\s]*of[-\s]*(?:use|day)|\btou\b|\btod\b|optional|program|pilot|\bev\b|"
    r"electric\s+vehicle|green|renewable|\d+[-\s]*(?:year|yr)s?\b",
    re.I,
)


def _r21_energy_rows(t: ExtractedTariff) -> list[dict]:
    return [c for c in t.components or [] if isinstance(c, dict)
            and str(c.get("component_type") or "").lower() == "energy"]


def _r21_per_kwh(c: dict) -> bool:
    return str(c.get("unit") or "$/kWh").replace(" ", "").lower() in ("$/kwh", "")


def default_supply_plan(supply: list[ExtractedTariff]) -> ExtractedTariff | None:
    """The one standard-offer / default supply price in a run, if exactly one.

    Optional supply products (TOU programs, EV, green, multi-year fixed
    terms) are not the default. Returns None when it is ambiguous.
    """
    std = [t for t in supply if not _R21_OPTIONAL_SUPPLY_RE.search(f"{t.name} {t.description or ''}")]
    flat = [t for t in std if len({round(float(c.get("rate_value") or 0), 6) for c in _r21_energy_rows(t)}) <= 2]
    return flat[0] if len(flat) == 1 else None


def _r21_fold(delivery: ExtractedTariff, supply: ExtractedTariff) -> list[dict] | None:
    """Components of one all-in plan: delivery + default supply.

    The shape (periods / tiers / seasons) comes from whichever side has more
    than one per-kWh price; the other side must be a single per-kWh price.
    Each stored ENERGY price = supply + delivery per-kWh (full price), with
    both halves kept as ADJUSTMENT audit rows (included_in_energy=true).
    Returns None when both sides vary (cannot be combined safely).
    """
    de, se = _r21_energy_rows(delivery), _r21_energy_rows(supply)
    if not se or not all(_r21_per_kwh(c) for c in de + se):
        return None
    dv = {round(float(c.get("rate_value") or 0), 6) for c in de}
    sv = {round(float(c.get("rate_value") or 0), 6) for c in se}
    if len(dv) > 1 and len(sv) > 1:
        return None
    d_add = next(iter(dv)) if len(dv) == 1 else None
    s_add = next(iter(sv)) if len(sv) == 1 else None
    out: list[dict] = []
    shape, add, add_label, base_label = (se, d_add or 0.0, "delivery", "supply") if d_add is not None or not de else (de, s_add, "supply", "delivery")
    for c in shape:
        c = dict(c)
        base = float(c.get("rate_value") or 0)
        c["rate_value"] = round(base + add, 6)
        lab = c.get("tier_label") or ""
        c["tier_label"] = f"all-in: {base_label} {base:.5f} + {add_label} {add:.5f}" + (f" ({lab})" if lab and not lab.startswith("all-in") else "")
        out.append(c)
    if de:
        out.append({"component_type": "adjustment", "unit": "$/kWh",
                    "rate_value": d_add if d_add is not None else 0.0,
                    "tier_label": "Delivery per-kWh (folded into energy)" if d_add is not None else "Delivery per-kWh varies by period (folded into energy)",
                    "included_in_energy": True})
    out.append({"component_type": "adjustment", "unit": "$/kWh",
                "rate_value": s_add if s_add is not None else 0.0,
                "tier_label": f"Default supply: {supply.name}"[:250],
                "included_in_energy": True})
    for c in delivery.components or []:
        if isinstance(c, dict) and str(c.get("component_type") or "").lower() not in ("energy",):
            out.append(dict(c))
    for c in supply.components or []:
        if not isinstance(c, dict):
            continue
        ct = str(c.get("component_type") or "").lower()
        if ct in ("energy",):
            continue
        if ct == "adjustment" and c.get("included_in_energy"):
            continue  # already inside the supply energy price
        c = dict(c)
        c["tier_label"] = f"Supply: {c.get('tier_label') or c.get('period_label') or ''}".strip()
        out.append(c)
    return out


def combine_supply_with_delivery(valid: list[ExtractedTariff]) -> tuple[list[ExtractedTariff], dict]:
    """Joshua default 2: delivery + standard-offer supply, combined and
    labelled. Never store half-plans.

    * A supply-only plan is paired with its delivery plan by schedule code /
      qualifiers; unpaired delivery plans get the run's single default
      supply price (R21), if there is exactly one.
    * The combined plan's ENERGY is the full price (supply + delivery per
      kWh), named "... (delivery + default supply)".
    * Unpaired supply-only plans and model-declared delivery-only plans are
      dropped and reported (``supply_only_dropped`` / ``delivery_only_dropped``).
    """
    supply = [t for t in valid if is_supply_only_plan(t)]
    # Only the model's own "delivery_only" is trusted on its own; the label
    # heuristic is used only when the run also has a supply price to pair
    # (bundled plans with "Distribution charge" labels are not half-plans).
    declared = [t for t in valid if t not in supply
                and str(getattr(t, "energy_scope", "") or "") == "delivery_only"]
    heuristic = [t for t in valid if supply and t not in supply and t not in declared
                 and str(getattr(t, "energy_scope", "") or "") not in ("bundled", "delivery_plus_default_supply")
                 and is_delivery_only_plan(t)]
    deliveries = declared + heuristic
    if not supply and not deliveries:
        return valid, {}
    combined, dropped, d_dropped = [], [], []
    pairs: list[tuple] = []
    used: set[int] = set()
    paired_supply: set[int] = set()
    for s_t in supply:
        d = _r20_pair(s_t, [x for x in deliveries if id(x) not in used])
        if d is not None:
            used.add(id(d))
            paired_supply.add(id(s_t))
            pairs.append((d, s_t))
    default = default_supply_plan(supply)
    if default is not None:
        for d in deliveries:
            if id(d) not in used:
                used.add(id(d))
                paired_supply.add(id(default))
                pairs.append((d, default))
    out = [t for t in valid if t not in supply and t not in deliveries]
    for d, s_t in pairs:
        comps = _r21_fold(d, s_t)
        if comps is None:
            d_dropped.append(d.name)
            log.warning(f"    '{d.name}': delivery and supply both vary by period — cannot combine; not stored")
            continue
        d.components = comps
        if _rate_type_family(d.rate_type) == "flat" and _rate_type_family(s_t.rate_type) != "flat":
            d.rate_type = s_t.rate_type
        # No parentheses: a shared "(…)" would read as a marketed alias and
        # merge different schedules (PPL RS vs RTS (R)) in same-plan dedupe.
        d.name = f"{d.name} — delivery + default supply"
        d.energy_scope = "delivery_plus_default_supply"
        if s_t.effective_date and (not d.effective_date or str(s_t.effective_date) > str(d.effective_date)):
            d.effective_date = s_t.effective_date
        d.missing_fields = [
            m for m in (list(d.missing_fields or []) + list(s_t.missing_fields or []))
            if not _R20_MISSING_DELIVERY_RE.search(str(m)) and not _R20_MISSING_SUPPLY_RE.search(str(m))
        ]
        d.needs_review = bool(d.needs_review or s_t.needs_review)
        _r18_note(d, "combined_supply", {
            "supply_plan": s_t.name,
            "supply_source": s_t.source_url,
            "delivery_source": d.source_url,
            "label": "delivery + default supply",
        })
        out.append(d)
        combined.append(d.name)
        log.info(f"    Combined delivery + default supply → '{d.name}'")
    for s_t in supply:
        if id(s_t) not in paired_supply:
            dropped.append(s_t.name)
            log.warning(f"    Supply-only plan '{s_t.name}' has no matching delivery plan in this run — not stored (never supply-only)")
    for d in deliveries:
        if id(d) not in used and d not in declared:
            out.append(d)  # heuristic only: left as extracted (old behaviour)
        elif id(d) not in used:
            d_dropped.append(d.name)
            log.warning(f"    Delivery-only plan '{d.name}' has no default supply price in this run — not stored (never half-plans)")
    info = {}
    if combined:
        info["supply_combined"] = combined
    if dropped:
        info["supply_only_dropped"] = dropped
    if d_dropped:
        info["delivery_only_dropped"] = d_dropped
    return out, info


def flag_supply_sheet_currency(tariffs: list[ExtractedTariff], doc_context: dict | None = None,
                               *, today: date | None = None) -> int:
    """Flag plans priced from a supply sheet that is not current."""
    ctxd = doc_context if doc_context is not None else _RUN_DOC_CONTEXT
    today = today or date.today()
    page_v = dict((ctxd or {}).get("page_vintages") or {})
    newest = None
    for u, v in page_v.items():
        if v and (newest is None or v > newest[1]):
            newest = (u, v)
    nd = newest_dated_rate_document((ctxd or {}).get("known_urls") or [], today=today)
    if nd and (newest is None or nd[1] > newest[1]):
        newest = nd
    flagged = 0
    for t in tariffs:
        srcs = [t.source_url or ""]
        cs = (getattr(t, "confidence_notes", None) or {}).get("combined_supply")
        if isinstance(cs, dict) and cs.get("supply_source"):
            srcs.append(cs["supply_source"])
        for src in srcs:
            if not src or not (is_supply_sheet_url(src) or (src == t.source_url and _R20_SUPPLY_RE.search(t.name or ""))):
                continue
            v = page_v.get(src) or url_document_vintage(src, today=today)
            if v is None:
                d = _parse_effective_date(getattr(t, "effective_date", None), today=today)
                v = (d.year, d.month) if d else None
            if v is None:
                continue
            age = _r19_months((today.year, today.month)) - _r19_months(v)
            trail = (_r19_months(newest[1]) - _r19_months(v)) if newest and newest[0] != src else 0
            if age > _R20_SUPPLY_STALE_MONTHS or trail >= _R20_SUPPLY_TRAIL_MONTHS:
                _r18_add_missing(t, "supply_sheet_not_current")
                _r18_note(t, "supply_sheet_not_current", {
                    "supply_document": src,
                    "supply_document_date": f"{v[0]}-{v[1]:02d}",
                    "newer_official_document": newest[0] if newest and trail > 0 else None,
                })
                log.warning(f"    '{t.name}' uses a supply price sheet dated {v[0]}-{v[1]:02d} — not current, needs_review")
                flagged += 1
                break
    return flagged


def reconcile_same_utility_plans(
    valid: list[ExtractedTariff],
    utility_name: str = "",
    all_in: list[ExtractedTariff] | None = None,
) -> tuple[list[ExtractedTariff], dict]:
    """R18 post-validation pass over one utility's accepted plans."""
    info: dict = {}
    valid, sup_info = combine_supply_with_delivery(valid)
    info.update(sup_info)
    n_rep = reconcile_optional_variant_prices(valid)
    kept, actions = dedupe_same_plan_variants(valid, utility_name)
    n_tmp = mark_temporary_only_plans(kept)
    n_guard = flag_optional_priced_below_base(kept)
    n_old = flag_older_source_documents(kept)
    n_stale = flag_stale_source_documents(kept)
    if n_stale:
        info["stale_documents"] = n_stale
    n_sup = flag_supply_sheet_currency(kept)
    if n_sup:
        info["supply_sheet_not_current"] = n_sup
    missing = find_plans_possibly_not_extracted(all_in or [], kept)
    if n_rep:
        info["variant_prices_repaired"] = n_rep
    if actions:
        info["same_plan_dedupe"] = actions
    if n_tmp:
        info["temporary_price_plans"] = n_tmp
    if n_guard:
        info["optional_below_base_flagged"] = n_guard
    if n_old:
        info["older_rate_document_flagged"] = n_old
    if missing:
        info["plans_possibly_not_extracted"] = missing
        log.warning(
            f"    Source names customer group(s) with no extracted plan: "
            f"{missing} — plan(s) may be missing from this run"
        )
    return kept, info



def phase4_validate(
    tariffs: list[ExtractedTariff], utility_name: str, state: str = ""
) -> tuple[dict, list[ExtractedTariff]]:
    """Validate extracted tariffs. Returns (report_dict, valid_tariffs_list).

    Uses state-level percentile bounds for rate validation:
    - Above 99th percentile: hard reject
    - Above 95th percentile: accepted but flagged as needs_review
    - Critical-peak / CPP / event ENERGY above 3× p99: keep + needs_review

    Units are normalized first (cents -> dollars) so magnitude checks run
    against comparable values. Before per-tariff checks, the batch salvages
    relative seasonal rider-only extracts (NL 1.1S) and applies shared
    stacking per-kWh riders from separate rider pages onto ENERGY tariffs
    (FULL-BILL product rule). Optional / source-specific / TOD overlays are
    never shared onto flat plans.
    """
    bounds = _get_rate_bounds(state)
    p95_energy, p99_energy, p95_fixed, p99_fixed, p95_demand, p99_demand = bounds

    issues = []
    valid_tariffs = []
    flagged_tariffs = []
    absorbed_rider_only = 0

    # Pass 0: structured normalize + unit normalize on every tariff so
    # batch rider salvage compares $/kWh values. Unit auto-correction alone
    # is not Mysa-critical — do not set needs_review for it.
    for t in tariffs:
        t.components = normalize_structured_components(t.components)
        unit_notes = _normalize_component_units(t, p99_energy)
        if unit_notes:
            log.info(f"    Unit normalization on '{t.name}': {'; '.join(unit_notes)}")
        strip_optional_program_components(t)
        move_negative_fixed_credits_to_notes(t)
        # Soften model needs_review when the only gap is effective_date /
        # or when the model set the flag with no Mysa-critical reason.
        missing = list(getattr(t, "missing_fields", None) or [])
        if getattr(t, "needs_review", False):
            critical_missing = [m for m in missing if not _is_informational_missing_field(m)]
            if not critical_missing and not getattr(t, "riders_referenced_not_shown", None):
                if not _is_mysa_critical_review_reason(
                    missing_fields=missing,
                    riders_missing=list(getattr(t, "riders_referenced_not_shown", None) or []),
                    energy_scope=str(getattr(t, "energy_scope", "") or ""),
                ):
                    t.needs_review = False

    # R8: run base-price derivation (NL 1.1S / 1.2DS allowed derivation e)
    # BEFORE the optional-programme drop — those extracts hold only seasonal
    # ± adjustments until salvage injects the sibling base ENERGY.
    n_salvaged = salvage_relative_rider_only_tariffs(tariffs)
    n_shared = apply_shared_stacking_riders_across_batch(tariffs)
    # After riders are on the ENERGY plans, drop resolved FAM/DSM hints so
    # needs_review is not kept solely for already-folded riders (R8b).
    clear_resolved_rider_hints(tariffs)
    flag_missing_sch125_tod_pca(tariffs)
    flag_plans_missing_referenced_riders(tariffs)
    # Sch 102 First adj is already on TOD recipients after apply_shared —
    # record the first-2,000 kWh limitation (no needs_review).
    annotate_sch102_first_block_on_tou(tariffs)
    if n_salvaged or n_shared:
        log.info(
            f"    Batch rider salvage: {n_salvaged} relative rider-only, "
            f"{n_shared} tariffs received shared stacking riders"
        )

    # Drop optional add-on programmes BEFORE full-bill sibling merge so
    # Community Solar / TOU Portfolio cannot absorb real residential plans.
    before_opt = len(tariffs)
    tariffs[:] = drop_optional_program_tariffs(tariffs)
    absorbed_rider_only += before_opt - len(tariffs)
    # Sample-bill flats beside an official flat of the same product.
    tariffs[:] = drop_sample_bill_duplicate_flats(tariffs)

    # After sharing + optional drop, prefer full-bill siblings over base-only
    # duplicates (also covers EV TOU once Sch 1xx riders land on the base row).
    collapsed = _collapse_full_bill_siblings(tariffs)
    if len(collapsed) < len(tariffs):
        tariffs[:] = collapsed

    for t in tariffs:
        tariff_issues = []
        # Recompute after optional strip / informational soft-clear.
        needs_review = bool(t.needs_review)
        # Soft-clear model needs_review unless Mysa-critical reasons remain.
        if needs_review and not _is_mysa_critical_review_reason(
            missing_fields=list(getattr(t, "missing_fields", None) or []),
            riders_missing=list(getattr(t, "riders_referenced_not_shown", None) or []),
            energy_scope=str(getattr(t, "energy_scope", "") or ""),
        ):
            needs_review = False

        # Optional programmes already removed pre-merge; keep a safety net.
        if _is_optional_program_tariff(t):
            absorbed_rider_only += 1
            log.info(f"    Dropped optional programme extract '{t.name}'")
            continue
        if SKIP_KEYWORDS.search(str(t.name or "")):
            tariff_issues.append(f"non-residential name ({t.name!r})")
            issues.append({"tariff": t.name, "issues": tariff_issues})
            continue

        # Rider-only extracts that donated stacking riders (and were not
        # salvaged into a seasonal schedule) are absorbed — not rejected —
        # when an ENERGY sibling in the batch could receive them.
        if _is_rider_only_tariff(t):
            seasonal_adjs = _energy_unit_adjustments(t, seasonal=True)
            seasons = {
                _season_key(a.get("season"))
                for a in seasonal_adjs
                if _season_key(a.get("season"))
            }
            has_energy_sibling = any(
                o is not t
                and not _is_rider_only_tariff(o)
                and any(
                    isinstance(c, dict)
                    and str(c.get("component_type") or "").lower() == "energy"
                    for c in (o.components or [])
                )
                for o in tariffs
            )
            if len(seasons) < 2 and has_energy_sibling:
                absorbed_rider_only += 1
                log.info(
                    f"    Absorbed rider-only '{t.name}' after sharing "
                    f"adjustments with ENERGY tariffs"
                )
                continue

        # Relative seasonal riders: expand base ENERGY ± seasonal ADJUSTMENT
        # into all-in ENERGY per season (NF Rate #1.1S pattern) before dedupe.
        before_seasonal = list(t.components)
        t.components = expand_relative_seasonal_energy(t.components)
        if t.components != before_seasonal:
            log.info(
                f"    Seasonal rider expand on '{t.name}': "
                f"{len(before_seasonal)} → {len(t.components)} comps, "
                f"{count_energy_seasons(t.components)} ENERGY seasons"
            )

        # Flat stacking riders (FAM/DSM/Storm/fuel): fold unseasoned
        # ADJUSTMENT ¢/kWh into all-in ENERGY so Flux/Lookup show FULL-BILL.
        # TOD overlays only fold onto TOU-family rate types.
        before_stack = list(t.components)
        t.components = expand_stacking_energy_riders(
            t.components, rate_type=str(t.rate_type or ""),
        )
        if t.components != before_stack:
            log.info(
                f"    Stacking rider expand on '{t.name}': "
                f"{len(before_stack)} → {len(t.components)} comps"
            )
        if any(
            isinstance(c, dict) and c.get("sanity_rejected")
            for c in (t.components or [])
        ):
            needs_review = True
            t.needs_review = True
            missing = list(getattr(t, "missing_fields", None) or [])
            if "rider_sanity_reject" not in missing:
                missing.append("rider_sanity_reject")
            t.missing_fields = missing
        clear_resolved_rider_hints([t])

        # Drop stale duplicate values of the same ADJUSTMENT family (e.g.
        # sample-bill TCOS old + new kept as two "tiers") before exact dedupe.
        before_super = len(t.components)
        t.components = drop_superseded_same_family_adjustments(t.components)
        t.components = drop_superseded_flat_energy_vintages(
            t.components, rate_type=str(t.rate_type or ""),
        )
        if len(t.components) < before_super:
            log.info(
                f"    Superseded-charge drop on '{t.name}': "
                f"{before_super} → {len(t.components)}"
            )

        # Collapse fixed/minimum twins and exact same-type duplicates before
        # bounds checks and persistence (NF Rate #1.1 amp-tier basic charge
        # was stored twice — once as fixed, once as equal minimum).
        before_n = len(t.components)
        t.components = dedupe_rate_components(t.components)
        if len(t.components) < before_n:
            log.info(
                f"    Component dedupe on '{t.name}': "
                f"{before_n} → {len(t.components)}"
            )

        if not t.name or t.name == "Unknown":
            tariff_issues.append("missing name")
        if t.customer_class not in VALID_CLASSES:
            tariff_issues.append(f"invalid customer_class '{t.customer_class}'")
        elif t.customer_class not in EXTRACT_CLASSES:
            # New scrapes are residential-only. Existing commercial DB rows
            # are untouched (reconcile is per-class and only sees classes
            # present in this extraction).
            log.info(
                f"    Dropping non-residential '{t.name}' "
                f"(class={t.customer_class}) from scrape"
            )
            continue
        if t.rate_type not in VALID_RATE_TYPES:
            tariff_issues.append(f"invalid rate_type '{t.rate_type}'")

        # Reject "proposed" / "pending" rates -- these come from rate-case
        # news articles describing what a utility *wants* to charge, not
        # the rate it actually charges today. Added 2026-05-12 after
        # Chunk 1 caught SCE's news-blog "Proposed Rate Structure"
        # tariffs being stored as if they were current filings.
        if t.name:
            _lower = t.name.lower()
            if any(token in _lower for token in (
                "proposed rate", "(proposed)", "pending approval",
                "subject to approval", "preliminary rate",
            )):
                tariff_issues.append(
                    f"rate marked as proposed/pending in name "
                    f"({t.name!r}) -- not a current tariff"
                )

        comp_types = {comp.get("component_type") for comp in t.components}
        has_core_component = bool(comp_types & {"energy", "fixed", "demand"})
        if not has_core_component:
            tariff_issues.append("no energy/fixed/demand component (rate rider only)")

        rt_l = str(t.rate_type or "").lower()

        # Far-future effective dates (>12 months) — keep review flag (R6).
        eff = _parse_effective_date(getattr(t, "effective_date", None))
        if eff and (eff - date.today()).days > 365:
            needs_review = True
            log.info(
                f"    Far-future effective_date {eff.isoformat()} on "
                f"'{t.name}' — needs_review"
            )

        # Structured TOU/seasonal completeness (clock windows + season calendar).
        # Prefer structured columns; do NOT invent times/dates from labels.
        # Incomplete shapes are flagged needs_review only when Mysa-critical
        # (missing clocks / season dates) — not holiday wording alone.
        try:
            from app.services.tou_seasonal_completeness import (
                evaluate_tariff_completeness,
            )

            completeness = evaluate_tariff_completeness(rt_l, t.components)
            t.completeness_reasons = list(completeness.reasons)
            if not completeness.complete and (
                rt_l in ("tou", "tou_tiered", "demand_tou", "seasonal_tou",
                         "seasonal", "seasonal_tiered")
            ):
                if _is_mysa_critical_review_reason(
                    completeness_reasons=list(completeness.reasons),
                ):
                    needs_review = True
                    log.info(
                        f"    Incomplete TOU/seasonal shape on '{t.name}': "
                        f"{', '.join(completeness.reasons)}"
                    )
        except Exception as e:
            log.warning(f"    Completeness check failed on '{t.name}': {e}")

        # Tier bounds + TOU clocks on the same ENERGY rows → tou_tiered
        # (PG&E E-TOU-C baseline tiers misread as overlapping periods).
        # Also detect "above/below baseline" labels without numeric bounds.
        energy_rows = [
            c for c in (t.components or [])
            if isinstance(c, dict)
            and str(c.get("component_type") or "").lower() == "energy"
        ]
        has_tier_bounds = any(
            c.get("tier_min_kwh") is not None or c.get("tier_max_kwh") is not None
            for c in energy_rows
        )
        has_baseline_labels = sum(
            1 for c in energy_rows
            if re.search(
                r"\b(?:above|below|over|under)\s+baseline\b|\bbaseline\b",
                " ".join(str(c.get(k) or "") for k in ("tier_label", "period_label")),
                re.I,
            )
        ) >= 2
        if rt_l in ("tou", "seasonal_tou") and (has_tier_bounds or has_baseline_labels):
            t.rate_type = "tou_tiered"
            rt_l = "tou_tiered"
            log.info(
                f"    Reclassified '{t.name}' as tou_tiered "
                f"({'baseline labels' if has_baseline_labels else 'tier bounds'}"
                f" + TOU clocks on ENERGY rows)"
            )
        # R13: Sch 102 2,000 kWh split turns a flat Default into two ENERGY
        # tiers — label it tiered (not flat).
        if rt_l == "flat" and has_tier_bounds and len(energy_rows) >= 2:
            t.rate_type = "tiered"
            rt_l = "tiered"
            log.info(
                f"    Reclassified '{t.name}' as tiered "
                f"(ENERGY rows carry tier_min/max after rider split)"
            )

        # Repair a 1-hour TOU gap only from a stated period (never guess).
        if rt_l in ("tou", "seasonal_tou", "demand_tou", "tou_tiered"):
            repaired, repair_notes = repair_one_hour_tou_gaps(
                t.components,
                missing_fields=list(getattr(t, "missing_fields", None) or []),
            )
            if repair_notes:
                t.components = repaired
                # Drop the now-resolved "3-4 pm partial peak" gap notes.
                if any("filled_from_stated" in n for n in repair_notes):
                    t.missing_fields = [
                        m for m in (t.missing_fields or [])
                        if not re.search(
                            r"3\s*[-–]\s*4|partial\s*peak\s*price", str(m), re.I
                        )
                    ]
                log.info(
                    f"    TOU clock repair on '{t.name}': {', '.join(repair_notes)}"
                )

        if rt_l in _TOU_OR_SEASONAL_TYPES or rt_l == "tou_tiered":
            from app.services.computable import evaluate_computable

            verdict = evaluate_computable(rt_l, t.components, name=t.name)
            t.computable_reasons = list(verdict.reasons)
            clock_broken = [
                r for r in verdict.reasons
                if str(r).startswith(("tou_gap", "tou_overlap", "tou_zero_length"))
            ]
            if clock_broken:
                # R7: KEEP the plan with prices; flag needs_review — never
                # drop a residential plan solely for imperfect clocks.
                needs_review = True
                t.computable_reasons = list(verdict.reasons) + [
                    f"tou_clock_{'gap' if any(str(r).startswith('tou_gap') for r in clock_broken) else 'overlap'}"
                ]
                log.info(
                    f"    Keeping '{t.name}' with needs_review — broken TOU "
                    f"clocks: {', '.join(clock_broken[:4])}"
                )
            elif not verdict.computable:
                if _is_mysa_critical_review_reason(
                    computable_reasons=list(verdict.reasons),
                ):
                    needs_review = True
                    log.info(
                        f"    Not computable '{t.name}': "
                        f"{', '.join(verdict.reasons[:6])}"
                    )

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
                    # Tiered validation: only HARD-reject when a value is so
                    # far out of bounds that it's almost certainly a parser
                    # or LLM hallucination (>3x the 99th percentile). Within
                    # 1x-3x of the 99th percentile we keep the tariff but
                    # flag it for human review — these are legitimate peak,
                    # dynamic-pricing, or critical-period rates that can
                    # legitimately exceed the typical bound (e.g. HQ's Rate
                    # DT dual-fuel pricing, ConEd's critical peak, etc.).
                    # Labelled CPP / critical-peak / event ENERGY above
                    # 3× p99 is kept with needs_review (NS Power ~182¢).
                    if ctype == "energy":
                        if rv > p99_energy * 3:
                            if _is_critical_peak_price(comp, t):
                                # CPP event prices are expected outliers —
                                # keep without a Mysa-critical needs_review
                                # unless clocks/seasons are also broken.
                                log.info(
                                    f"    CPP/event energy {rv} $/kWh on "
                                    f"'{t.name}' exceeds 3x p99 "
                                    f"({p99_energy}) — kept"
                                )
                            else:
                                tariff_issues.append(
                                    f"energy rate {rv} $/kWh > 3x 99th percentile "
                                    f"for {state or 'US'} ({p99_energy}) — likely hallucination"
                                )
                        # p95 alone is NOT Mysa-critical — do not flag.
                    elif ctype == "fixed":
                        rv *= _PERIODIC_TO_MONTH.get(str(comp.get("unit") or "").strip(), 1.0)
                        if rv > p99_fixed * 3:
                            tariff_issues.append(
                                f"fixed charge ${rv}/month > 3x 99th percentile "
                                f"for {state or 'US'} ({p99_fixed}) — likely hallucination"
                            )
                        # p95 fixed alone is not Mysa-critical.
                    elif ctype == "demand":
                        if rv > p99_demand * 3:
                            tariff_issues.append(
                                f"demand charge ${rv}/kW > 3x 99th percentile "
                                f"for {state or 'US'} ({p99_demand}) — likely hallucination"
                            )
                except (ValueError, TypeError):
                    tariff_issues.append(f"non-numeric rate_value '{rate_val}'")

        if tariff_issues:
            issues.append({"tariff": t.name, "issues": tariff_issues})
        else:
            # Final gate: every needs_review flag must carry a Mysa-critical
            # reason (missing energy / broken TOU clock / season dates /
            # unresolved rider). No reason → clear the flag (R8).
            if needs_review and not _is_mysa_critical_review_reason(
                missing_fields=list(getattr(t, "missing_fields", None) or []),
                riders_missing=list(getattr(t, "riders_referenced_not_shown", None) or []),
                completeness_reasons=list(getattr(t, "completeness_reasons", None) or []),
                computable_reasons=list(getattr(t, "computable_reasons", None) or []),
                energy_scope=str(getattr(t, "energy_scope", "") or ""),
            ):
                needs_review = False
            t.needs_review = needs_review
            valid_tariffs.append(t)
            if needs_review:
                flagged_tariffs.append(t.name)

    # R18: one copy per plan, optional-variant reconcile + below-base guard,
    # temporary-price handling, and missing-plan hints (all-in values now).
    valid_tariffs, sq_info = apply_source_quality_rules(valid_tariffs)
    n_before_r18 = len(valid_tariffs)
    valid_tariffs, r18_info = reconcile_same_utility_plans(
        valid_tariffs, utility_name, all_in=tariffs,
    )
    duplicates_dropped = n_before_r18 - len(valid_tariffs)
    # R21: after every rider fold — plans still missing referenced per-kWh
    # riders are "base only" (flagged, not Mysa-complete).
    n_base_only = mark_base_only_plans(valid_tariffs)
    n_wrong_state = flag_wrong_jurisdiction(valid_tariffs, state)
    if n_wrong_state:
        r18_info = {**(r18_info or {}), "wrong_jurisdiction_plans": n_wrong_state}
    if n_base_only:
        r18_info = {**(r18_info or {}), "base_only_plans": n_base_only}
    if sq_info["retail_offers_dropped"] or sq_info["marketing_rounded_plans"]:
        r18_info = {**(r18_info or {}), **sq_info}
    flagged_tariffs = [t.name for t in valid_tariffs if t.needs_review]

    llm_cost.record_tier_acceptance(
        [getattr(t, "extraction_tier", "") for t in tariffs],
        [getattr(t, "extraction_tier", "") for t in valid_tariffs],
    )

    # Absorbed rider-only extracts are neither valid nor invalid — they
    # donated adjustments to ENERGY tariffs and should not inflate "invalid".
    invalid_count = (
        len(tariffs) - len(valid_tariffs) - absorbed_rider_only
        - duplicates_dropped
    )
    report = {
        "total_extracted": len(tariffs),
        "valid": len(valid_tariffs),
        "invalid": max(0, invalid_count),
        "absorbed_rider_only": absorbed_rider_only,
        "duplicates_dropped": duplicates_dropped,
        "issues": issues,
        "flagged_needs_review": flagged_tariffs,
        "has_residential": any(t.customer_class == "residential" for t in valid_tariffs),
        "has_commercial": any(t.customer_class == "commercial" for t in valid_tariffs),
    }
    if r18_info:
        report.update(r18_info)

    log.info(f"  Phase 4: {report['valid']} valid, {report['invalid']} invalid tariffs")
    if absorbed_rider_only:
        log.info(f"    Absorbed {absorbed_rider_only} rider-only extract(s) into ENERGY")
    if flagged_tariffs:
        log.info(f"    Flagged for review (Mysa-critical): {flagged_tariffs}")
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

    # Signal 4: Multiple well-structured components (+0.10).
    # Count after fixed/minimum dedupe so near-duplicate basic+minimum
    # twins do not inflate richness.
    n_comp = len(dedupe_rate_components(et.components))
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


# Column lengths for rate_components string fields (see models/tariff.py).
# LLM extraction from dense tariff books (NS Power May 2026) routinely
# emits season/period labels longer than the original VARCHAR(50/100),
# which crashed process_utility for utility 1739 on 2026-09-01 with
# psycopg2.errors.StringDataRightTruncation.
_RC_UNIT_MAX = 50
_RC_TIER_LABEL_MAX = 100
_RC_PERIOD_LABEL_MAX = 100
_RC_SEASON_MAX = 50
_TARIFF_CODE_MAX = 100


def _clip_str(value: Any, max_len: int) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if len(s) <= max_len:
        return s
    return s[: max_len - 1].rstrip() + "…"


def _clip_component_strings(
    comp: dict, *, tariff_name: str = ""
) -> tuple[str, str | None, str | None, str | None]:
    """Return (unit, tier_label, period_label, season) clipped to column limits."""
    unit_raw = comp.get("unit") or "$/kWh"
    unit = _clip_str(unit_raw, _RC_UNIT_MAX) or "$/kWh"
    tier_label = _clip_str(comp.get("tier_label"), _RC_TIER_LABEL_MAX)
    period_label = _clip_str(comp.get("period_label"), _RC_PERIOD_LABEL_MAX)
    season = _clip_str(comp.get("season"), _RC_SEASON_MAX)
    clipped = []
    if comp.get("season") and season != str(comp.get("season") or "").strip():
        clipped.append("season")
    if comp.get("period_label") and period_label != str(comp.get("period_label") or "").strip():
        clipped.append("period_label")
    if comp.get("tier_label") and tier_label != str(comp.get("tier_label") or "").strip():
        clipped.append("tier_label")
    if clipped:
        log.warning(
            f"    Clipped {', '.join(clipped)} on '{tariff_name or '?'}' "
            f"to fit VARCHAR limits (prevents StringDataRightTruncation)"
        )
    return unit, tier_label, period_label, season


_DAY_TYPE_ALLOWED = frozenset({"weekday", "weekend", "holiday", "all"})


def _parse_period_time(value: Any) -> Any:
    """Parse HH:MM / HH:MM:SS / 24:00 into datetime.time. Never invents.

    Returns None when absent or unparseable. ``24:00`` maps to ``00:00``
    (end-of-day convention; see RateComponent docs).
    """
    from datetime import time as _time

    if value is None or value == "":
        return None
    if isinstance(value, _time):
        return value
    s = str(value).strip().lower()
    if not s or s in ("null", "none", "n/a"):
        return None
    s = s.replace("a.m.", "am").replace("p.m.", "pm").replace("a.m", "am").replace("p.m", "pm")
    if "midnight" in s:
        return _time(0, 0, 0)
    if "noon" in s:
        return _time(12, 0, 0)
    # Strip common am/pm if LLM emits "7:00 am"
    meridiem = None
    if s.endswith("am") or s.endswith("pm"):
        meridiem = s[-2:]
        s = s[:-2].strip()
    s = s.replace(".", ":")
    if s in ("24:00", "24:00:00"):
        return _time(0, 0, 0)
    parts = s.split(":")
    try:
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        sec = int(float(parts[2])) if len(parts) > 2 else 0
    except (ValueError, IndexError):
        return None
    if meridiem == "pm" and h < 12:
        h += 12
    elif meridiem == "am" and h == 12:
        h = 0
    if h == 24 and m == 0 and sec == 0:
        return _time(0, 0, 0)
    if not (0 <= h <= 23 and 0 <= m <= 59 and 0 <= sec <= 59):
        return None
    return _time(h, m, sec)


def _parse_day_type(value: Any) -> str | None:
    if value is None or value == "":
        return None
    s = str(value).strip().lower()
    if s in _DAY_TYPE_ALLOWED:
        return s
    # Mild aliases — still source-derived, not invented windows
    if s in ("weekdays", "wd"):
        return "weekday"
    if s in ("weekends", "we", "saturday/sunday", "sat/sun"):
        return "weekend"
    if s in ("holidays",):
        return "holiday"
    if s in ("all days", "every day", "everyday", "daily"):
        return "all"
    return None


def _parse_season_int(value: Any, *, lo: int, hi: int) -> int | None:
    if value is None or value == "":
        return None
    try:
        n = int(value)
    except (TypeError, ValueError):
        return None
    if lo <= n <= hi:
        return n
    return None


_MONTHS = {
    m: i for i, names in enumerate((
        (), ("jan", "january"), ("feb", "february"), ("mar", "march"), ("apr", "april"),
        ("may",), ("jun", "june"), ("jul", "july"), ("aug", "august"),
        ("sep", "sept", "september"), ("oct", "october"), ("nov", "november"),
        ("dec", "december"),
    )) for m in names
}
_MONTH_DAY_RE = re.compile(r"^\s*([a-z]+)\.?\s+(\d{1,2})(?:st|nd|rd|th)?\s*$", re.I)
_NUMERIC_MONTH_DAY_RE = re.compile(r"^\s*(\d{1,2})\s*[-/]\s*(\d{1,2})\s*$")

_WEEKDAY_ALIASES = re.compile(
    r"^(?:weekdays?|mon(?:day)?\s*(?:-|–|—|to|through|thru)\s*fri(?:day)?|business days?)"
    r"(?:\s*\(?(?:excluding|except|excl\.?)\s+(?:statutory\s+|public\s+)?holidays?\)?)?$"
)
_WEEKEND_ALIASES = re.compile(
    r"^(?:weekends?|sat(?:urday)?\s*(?:-|–|—|to|and|&|/)\s*sun(?:day)?)$"
)
_ALL_DAY_ALIASES = re.compile(r"^(?:all|all days|every day|everyday|daily|7 days(?: a week)?|all week)$")
_HOLIDAY_ALIASES = re.compile(r"^(?:(?:statutory|public|stat|nerc)\s+)?holidays?$")


def _day_types(value: Any) -> list[str]:
    """Source day-type phrase → one or more ``day_type`` values ([] if unknown).

    Compound phrases ("weekends and holidays") map to several values so the
    window can be stored once per day type — the source states each one.
    """
    if value is None or value == "":
        return []
    if isinstance(value, (list, tuple, set)):
        out: list[str] = []
        for v in value:
            for d in _day_types(v):
                if d not in out:
                    out.append(d)
        return out
    s = re.sub(r"\s+", " ", str(value).strip().lower())
    single = _parse_day_type(s)
    if single:
        return [single]
    for pattern, day in (
        (_WEEKDAY_ALIASES, "weekday"), (_WEEKEND_ALIASES, "weekend"),
        (_ALL_DAY_ALIASES, "all"), (_HOLIDAY_ALIASES, "holiday"),
    ):
        if pattern.match(s):
            return [day]
    parts = [p for p in re.split(r"\s*(?:,|/|&|\+|\band\b|\bor\b)\s*", s) if p]
    if len(parts) > 1 and not _WEEKEND_ALIASES.match(s):
        out = []
        for p in parts:
            got = _day_types(p)
            if not got:
                return []
            out.extend(d for d in got if d not in out)
        return out
    return []


def _parse_month(value: Any) -> int | None:
    if isinstance(value, str) and not value.strip().isdigit():
        return _MONTHS.get(value.strip().lower().rstrip("."))
    return _parse_season_int(value, lo=1, hi=12)


def _parse_month_day(value: Any) -> tuple[int, int] | None:
    """'Nov 1' / 'November 1st' / '11-01' / '11/1' → (11, 1)."""
    if not isinstance(value, str):
        return None
    m = _MONTH_DAY_RE.match(value)
    if m:
        month = _MONTHS.get(m.group(1).lower())
        day = int(m.group(2))
    else:
        m = _NUMERIC_MONTH_DAY_RE.match(value)
        if not m:
            return None
        month, day = int(m.group(1)), int(m.group(2))
    if month and 1 <= month <= 12 and 1 <= day <= 31:
        return month, day
    return None


_STRUCTURED_KEY_ALIASES = {
    "period_start_time": ("start_time", "period_start", "window_start", "time_start"),
    "period_end_time": ("end_time", "period_end", "window_end", "time_end"),
    "day_type": ("day_types", "days", "applies_on", "day"),
}


def normalize_structured_components(components: list[dict]) -> list[dict]:
    """Map the forms models actually emit onto the structured columns.

    Only rewrites what the model returned (aliases, "7:00 a.m.", "noon",
    month names, "Nov 1" season bounds, inclusive ":59" window ends) and
    splits compound day types into one row per day type. Never fills a
    field the model left empty.
    """
    out: list[dict] = []
    for comp in components or []:
        if not isinstance(comp, dict):
            out.append(comp)
            continue
        c = dict(comp)
        for key, aliases in _STRUCTURED_KEY_ALIASES.items():
            if c.get(key) in (None, ""):
                for alias in aliases:
                    if c.get(alias) not in (None, ""):
                        c[key] = c.pop(alias)
                        break

        for key in ("period_start_time", "period_end_time"):
            if c.get(key) in (None, ""):
                continue
            t = _parse_period_time(c[key])
            if t is None:
                c[key] = None
                continue
            if key == "period_end_time" and t.minute == 59:
                # Books print inclusive ends ("to 10:59 a.m."); the window
                # is half-open, ending at the next minute.
                t = (datetime.combine(datetime(2000, 1, 1), t) + timedelta(minutes=1)).time()
            c[key] = t.strftime("%H:%M")

        for edge in ("start", "end"):
            mk, dk = f"season_{edge}_month", f"season_{edge}_day"
            combined = c.get(f"season_{edge}") or c.get(f"season_{edge}_date")
            if c.get(mk) in (None, "") and combined:
                md = _parse_month_day(combined)
                if md:
                    c[mk], c[dk] = md
            if c.get(mk) not in (None, ""):
                c[mk] = _parse_month(c[mk])

        days = _day_types(c.get("day_type"))
        if len(days) > 1:
            out.extend({**c, "day_type": d} for d in days)
            continue
        c["day_type"] = days[0] if days else None
        out.append(c)
    return out


def _structured_component_fields(comp: dict) -> dict:
    """Extract validated structured TOU/season fields from an LLM component dict."""
    return {
        "period_start_time": _parse_period_time(comp.get("period_start_time")),
        "period_end_time": _parse_period_time(comp.get("period_end_time")),
        "day_type": _parse_day_type(comp.get("day_type")),
        "season_start_month": _parse_season_int(
            comp.get("season_start_month"), lo=1, hi=12
        ),
        "season_start_day": _parse_season_int(
            comp.get("season_start_day"), lo=1, hi=31
        ),
        "season_end_month": _parse_season_int(
            comp.get("season_end_month"), lo=1, hi=12
        ),
        "season_end_day": _parse_season_int(
            comp.get("season_end_day"), lo=1, hi=31
        ),
    }


_STORE_CLASS_MAP_KEYS = ("residential", "commercial")
_STORE_TYPE_MAP_RAW = {
    "flat": "flat", "tiered": "tiered", "tou": "tou",
    "demand": "demand", "seasonal": "seasonal",
    "tou_tiered": "tou_tiered", "seasonal_tou": "seasonal_tou",
    "seasonal_tiered": "seasonal_tiered", "demand_tou": "demand_tou",
    "complex": "complex",
    "tiered_demand": "complex", "demand_tiered": "complex",
    "tiered_demand_seasonal": "complex",
    "seasonal_demand": "complex", "demand_seasonal": "complex",
    "seasonal_tiered_demand": "complex", "seasonal_tou_tiered": "complex",
    "seasonal_tou_demand": "complex", "tou_demand": "complex",
}

# Reconciliation retires live rows missing from an extraction only when the
# extraction covers at least this share of the class's live rows; below it
# the extraction is treated as partial and nothing is retired.
RECONCILE_MIN_COVERAGE = 0.75


def _build_rate_components(et: ExtractedTariff) -> list:
    from app.models import RateComponent, ComponentType

    comp_map = {c.value: c for c in ComponentType}
    out = []
    for comp in et.components:
        ct = comp_map.get(comp.get("component_type"))
        if not ct:
            continue
        try:
            rv = float(comp.get("rate_value", 0))
        except (ValueError, TypeError):
            log.warning(f"    Skipping component with unparseable rate_value: {comp.get('rate_value')}")
            continue
        # Clip VARCHAR fields — NS Power (and other rate books) emit
        # long season/period labels that previously crashed refresh
        # with StringDataRightTruncation (utility 1739, 2026-09-01).
        unit, tier_label, period_label, season = _clip_component_strings(
            comp, tariff_name=et.name
        )
        structured = _structured_component_fields(comp)
        out.append(RateComponent(
            component_type=ct,
            unit=unit,
            rate_value=rv,
            tier_min_kwh=comp.get("tier_min_kwh"),
            tier_max_kwh=comp.get("tier_max_kwh"),
            tier_label=tier_label,
            period_label=period_label,
            period_start_time=structured["period_start_time"],
            period_end_time=structured["period_end_time"],
            day_type=structured["day_type"],
            season=season,
            season_start_month=structured["season_start_month"],
            season_start_day=structured["season_start_day"],
            season_end_month=structured["season_end_month"],
            season_end_day=structured["season_end_day"],
            included_in_energy=bool(comp.get("included_in_energy")),
        ))
    return out


def _pick_live_row(rows: list):
    """One live row per (utility, name, class) is the invariant; if history
    left several, act on the newest and leave the rest to vintage/dup
    cleanup instead of raising MultipleResultsFound."""
    if not rows:
        return None
    if len(rows) > 1:
        log.warning(
            f"    {len(rows)} live rows share the name '{rows[0].name}' "
            f"(ids {[r.id for r in rows]}); using the newest"
        )
    return choose_vintage_keeper(rows)


_EFFECTIVE_DATE_MIN = date(1990, 1, 1)
# Rate books announce the next edition ahead of time. Allow up to ~3 years
# so far-future rows can be stored and flagged for review (>12 months) rather
# than silently dropped; anything beyond that is almost certainly a misread.
_EFFECTIVE_DATE_MAX_AHEAD_DAYS = 1100


def _parse_effective_date(raw, *, today: date | None = None) -> date | None:
    """A full calendar date from an extracted ``effective_date``, else None.

    Accepts ISO dates (optionally with a time suffix), ``YYYY/MM/DD``,
    month-name forms ("January 1, 2026", "1 Jan 2026") and ``M/D/YYYY`` only
    when day and month cannot be swapped. Partial dates (year or month only)
    and implausible years return None: a day is never invented.
    """
    if raw is None or raw == "":
        return None
    if isinstance(raw, datetime):
        d = raw.date()
    elif isinstance(raw, date):
        d = raw
    else:
        d = _parse_effective_date_str(str(raw).strip())
    if d is None:
        return None
    today = today or date.today()
    if d < _EFFECTIVE_DATE_MIN or (d - today).days > _EFFECTIVE_DATE_MAX_AHEAD_DAYS:
        return None
    return d


def _parse_effective_date_str(s: str) -> date | None:
    def _mk(y, m, d):
        try:
            return date(int(y), int(m), int(d))
        except (TypeError, ValueError):
            return None

    m = re.match(r"^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})(?:$|[T\s])", s)
    if m:
        return _mk(*m.groups())
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        a, b, y = (int(x) for x in m.groups())
        if a == b or b > 12:
            return _mk(y, a, b)
        if a > 12:
            return _mk(y, b, a)
        return None
    cleaned = re.sub(r"[,.]", " ", s.lower())
    cleaned = re.sub(r"(\d)(st|nd|rd|th)\b", r"\1", cleaned).split()
    if len(cleaned) == 3:
        mon = _MONTHS.get(cleaned[0])
        if mon and cleaned[1].isdigit() and re.fullmatch(r"\d{4}", cleaned[2]):
            return _mk(cleaned[2], mon, cleaned[1])
        mon = _MONTHS.get(cleaned[1])
        if mon and cleaned[0].isdigit() and re.fullmatch(r"\d{4}", cleaned[2]):
            return _mk(cleaned[2], mon, cleaned[0])
    return None


def _is_newer_effective_date(existing_date, extracted_date) -> bool:
    return (
        existing_date is not None
        and extracted_date is not None
        and extracted_date > existing_date
    )


def _should_fill_effective_date(existing_date, extracted_date) -> bool:
    return existing_date is None and extracted_date is not None


def _content_matches(existing, rate_type, eff_date, new_components) -> bool:
    """Same rates as the live row, for re-verify purposes.

    The effective date only breaks a match when the extract carries a newer
    date (a new rate-book edition → soft-supersede). An undated extract, an
    older date, or a date for a row that has none all re-verify; the
    blank-date case is then filled in place by ``store_tariffs``.
    """
    from app.services.tariff_history import component_signature

    if existing.rate_type != rate_type:
        return False
    if _is_newer_effective_date(existing.effective_date, eff_date):
        return False
    return component_signature(existing.rate_components) == component_signature(new_components)


def _computable_regression(existing, rate_type, new_components) -> list[str] | None:
    """Reasons the new rows are not computable when the live row is; else None.

    A re-extraction that lost clocks, day types or seasons must not retire a
    live row that prices every interval.
    """
    from app.services.computable import evaluate_computable

    new_rt = getattr(rate_type, "value", rate_type)
    new = evaluate_computable(new_rt, new_components)
    if new.computable or any(r.split(":")[0].endswith("_unsupported") for r in new.reasons):
        return None
    old = evaluate_computable(existing.rate_type, list(existing.rate_components or []), name=existing.name)
    return list(new.reasons) if old.computable else None


def store_tariffs(
    utility_id: int,
    tariffs: list[ExtractedTariff],
    dry_run: bool,
    *,
    actor_type: str = "pipeline",
    actor_id: str | None = None,
    source_hashes: dict[str, str] | None = None,
) -> int:
    """Persist validated tariffs — soft-supersede only, never in place.

    Each extracted tariff is matched to the *live* row with the same
    (utility, name, customer_class):

    - no live row → insert a new row;
    - identical content (rate_type, component signature, and no newer
      effective_date) → re-verify: touch ``last_verified_at``. A blank
      ``effective_date`` is filled from the extract (event ``metadata`` /
      ``effective_date_fill``; a ``hold`` on protected rows); an undated or
      older-dated extract never changes the stored date;
    - identical rates with a newer effective_date count as changed content;
    - changed content on an unprotected row → insert a new row and
      soft-supersede the old one (reason ``refresh``). The prior components
      stay on the superseded row;
    - changed content on a protected row (approved / repair / manual) →
      hold: the protected row stays live and untouched, and the proposal
      is logged as a ``hold`` change event. If the row is pinned and the
      extract comes from a different document than the pinned one, a
      verification is opened for the automated verifier.

    ``source_hashes`` maps source_url → stable document hash
    (``monitor.stable_text_hash``) and is persisted as
    ``source_document_hash``.

    Returns the number of tariffs inserted, revised or re-verified.
    """
    if dry_run:
        log.info(f"  DRY RUN: Would store {len(tariffs)} tariffs for utility {utility_id}")
        return len(tariffs)

    from sqlalchemy import select
    from sqlalchemy.orm import Session
    from app.db.session import get_sync_engine
    from app.models import Tariff, CustomerClass, RateType
    from app.services.pins import active_pin_for, propose_verification
    from app.services.tariff_history import (
        is_protected,
        merge_confidence_factors,
        record_event,
        serialize_components,
        supersede_tariff,
    )

    source_hashes = source_hashes or {}
    class_map = {k: CustomerClass(k) for k in _STORE_CLASS_MAP_KEYS}
    type_map = {k: RateType(v) for k, v in _STORE_TYPE_MAP_RAW.items()}

    engine = get_sync_engine()
    stored = 0
    held = 0
    now = datetime.now(timezone.utc)

    info = get_utility_info(utility_id)
    u_name = info.get("name", "")
    u_state = info.get("state", "")
    u_website = info.get("website_url", "")
    u_domain = urlparse(u_website).netloc if u_website else None
    event_kw = {"actor_type": actor_type, "actor_id": actor_id}
    from app.services.source_type import OFFICIAL, THIRD_PARTY, classify_source

    source_ctx = _source_context(info)

    def _source_type(url: str | None) -> str:
        return classify_source(url, source_ctx).source_type

    with Session(engine) as session:
        # (name, class) -> the live row that now represents that product
        # (inserted, revised, re-verified or held). Used by the OpenEI
        # matcher and to keep reconciliation away from these rows.
        fresh_by_key: dict = {}
        for et in tariffs:
            cc = class_map.get(et.customer_class)
            rt = type_map.get(et.rate_type)
            if not cc or not rt:
                continue

            eff_date = _parse_effective_date(et.effective_date)
            if et.effective_date and eff_date is None:
                log.info(
                    f"    Ignoring unparseable/implausible effective_date "
                    f"{et.effective_date!r} for '{et.name}'"
                )

            conf_score, conf_factors = _calculate_confidence(
                et, u_name, u_state, u_domain,
            )
            # Durable review flag (Phase 4 p95 band / unit auto-correction)
            # so suspicious rows are queryable, not just logged.
            missing = list(getattr(et, "missing_fields", None) or [])
            riders_missing = list(getattr(et, "riders_referenced_not_shown", None) or [])
            scope = str(getattr(et, "energy_scope", "") or "")
            reasons = getattr(et, "completeness_reasons", None) or []
            not_computable = list(getattr(et, "computable_reasons", None) or [])
            # needs_review only for Mysa-critical problems — not bare
            # effective_date / informational missing_fields.
            review = bool(getattr(et, "needs_review", False)) and _is_mysa_critical_review_reason(
                missing_fields=missing,
                riders_missing=riders_missing,
                completeness_reasons=list(reasons),
                computable_reasons=not_computable,
                energy_scope=scope,
            )
            if not review:
                review = _is_mysa_critical_review_reason(
                    missing_fields=missing,
                    riders_missing=riders_missing,
                    completeness_reasons=list(reasons),
                    computable_reasons=not_computable,
                    energy_scope=scope,
                )
            if review:
                conf_factors = {**conf_factors, "needs_review": True}
            if missing:
                conf_factors = {**conf_factors, "missing_fields": missing}
            if riders_missing:
                conf_factors = {
                    **conf_factors,
                    "riders_referenced_not_shown": riders_missing,
                }
            if scope:
                conf_factors = {**conf_factors, "energy_scope": scope}
            if getattr(et, "closed_to_new", False):
                conf_factors = {**conf_factors, "closed_to_new": True}
            if getattr(et, "energy_includes_riders", None) is not None:
                conf_factors = {
                    **conf_factors,
                    "energy_includes_riders": bool(et.energy_includes_riders),
                }
            # Non-review documentation (Sch 102 first-2,000 on TOD, …).
            notes = getattr(et, "confidence_notes", None) or {}
            if notes:
                conf_factors = {**conf_factors, **dict(notes)}
            # Future-dated extracts stay in the DB but are not served until
            # their effective_date. Near-term future (≤12 months) is normal
            # (interim + coming TOU); farther out is recorded as informational
            # only — far-future alone is not Mysa-critical (R8.11).
            if eff_date and eff_date > date.today():
                days_ahead = (eff_date - date.today()).days
                conf_factors = {**conf_factors, "not_yet_effective": True}
                if days_ahead > 365:
                    conf_factors = {
                        **conf_factors,
                        "effective_date_far_future": True,
                    }
            if reasons:
                conf_factors = {
                    **conf_factors,
                    "tou_seasonal_incomplete": True,
                    "tou_seasonal_incomplete_reasons": list(reasons),
                }
            if not_computable:
                conf_factors = {**conf_factors, "extract_not_computable": not_computable}

            code_clipped = _clip_str(et.code, _TARIFF_CODE_MAX) if et.code else et.code
            doc_hash = source_hashes.get(et.source_url) if et.source_url else None

            # Safety net: phase4 already dedupes, but callers may invoke
            # store_tariffs directly (browser_interaction, OEB, repair).
            et.components = dedupe_rate_components(et.components)
            new_components = _build_rate_components(et)
            if not new_components:
                log.warning(f"    Skipping tariff '{et.name}' — 0 valid components")
                continue

            existing = _pick_live_row(session.execute(
                select(Tariff).where(
                    Tariff.utility_id == utility_id,
                    Tariff.name == et.name,
                    Tariff.customer_class == cc,
                    Tariff.superseded_by_tariff_id.is_(None),
                    Tariff.supersede_reason.is_(None),
                )
            ).scalars().all())

            new_source_type = _source_type(et.source_url)
            old_source_type = _source_type(existing.source_url) if existing is not None else None
            # Same rates, but now read from the utility's own document while
            # the live row still cites a third party: revise (new row +
            # soft-supersede) so the provenance change is auditable.
            content_same = existing is not None and _content_matches(
                existing, rt, eff_date, new_components
            )
            source_upgrade = (
                content_same
                and not is_protected(existing)
                and old_source_type == THIRD_PARTY
                and new_source_type == OFFICIAL
            )

            if content_same and not source_upgrade:
                existing.last_verified_at = now
                if not is_protected(existing):
                    existing.confidence_score = conf_score
                    existing.confidence_factors = merge_confidence_factors(
                        existing.confidence_factors, conf_factors
                    )
                    existing.description = existing.description or et.description
                    existing.source_url = existing.source_url or et.source_url
                    existing.code = existing.code or code_clipped
                    if doc_hash and existing.source_url == et.source_url:
                        existing.source_document_hash = doc_hash
                if _should_fill_effective_date(existing.effective_date, eff_date):
                    date_event = {
                        "session": session,
                        "utility_id": utility_id,
                        "before_tariff_id": existing.id,
                        "source_url": et.source_url,
                        "source_document_hash": doc_hash,
                        **event_kw,
                    }
                    if is_protected(existing):
                        record_event(
                            decision="hold",
                            reason="protected_row",
                            payload={"proposed": {"effective_date": eff_date.isoformat()}},
                            **date_event,
                        )
                    else:
                        existing.effective_date = eff_date
                        record_event(
                            decision="metadata",
                            reason="effective_date_fill",
                            after_tariff_id=existing.id,
                            payload={"effective_date": {"from": None, "to": eff_date.isoformat()}},
                            **date_event,
                        )
                        log.info(
                            f"    Filled blank effective_date on '{et.name}' "
                            f"({existing.id}) → {eff_date.isoformat()}"
                        )
                stored += 1
                fresh_by_key[(et.name, cc)] = existing
                continue

            if existing is not None and is_protected(existing):
                proposal = {
                    "name": et.name,
                    "code": code_clipped or None,
                    "customer_class": cc.value,
                    "rate_type": rt.value,
                    "effective_date": eff_date.isoformat() if eff_date else None,
                    "components": serialize_components(new_components),
                }
                record_event(
                    session,
                    decision="hold",
                    reason="protected_row",
                    utility_id=utility_id,
                    before_tariff_id=existing.id,
                    source_url=et.source_url,
                    source_document_hash=doc_hash,
                    payload={"proposed": proposal},
                    **event_kw,
                )
                # Same pinned document: an extraction disagreement, kept as
                # evidence on the hold event. A different document may be a
                # newer rate book the pin would otherwise never see.
                pin = active_pin_for(session, existing.id)
                if pin is not None and et.source_url and et.source_url != pin.pinned_source_url:
                    propose_verification(
                        session, pin,
                        trigger="new_document",
                        new_source_url=et.source_url,
                        proposed=proposal,
                    )
                log.warning(
                    f"    HOLD '{et.name}': live row {existing.id} is protected "
                    f"(approved/repair/manual) and the extraction differs — "
                    f"proposal logged, live row unchanged"
                )
                held += 1
                fresh_by_key[(et.name, cc)] = existing
                continue

            if old_source_type == OFFICIAL and new_source_type == THIRD_PARTY:
                record_event(
                    session,
                    decision="hold",
                    reason="source_downgrade",
                    utility_id=utility_id,
                    before_tariff_id=existing.id,
                    source_url=et.source_url,
                    source_document_hash=doc_hash,
                    payload={
                        "proposed": {
                            "name": et.name,
                            "rate_type": rt.value,
                            "effective_date": eff_date.isoformat() if eff_date else None,
                            "components": serialize_components(new_components),
                        },
                        "live_source_url": existing.source_url,
                    },
                    **event_kw,
                )
                log.warning(
                    f"    HOLD '{et.name}': live row {existing.id} cites the utility's own "
                    f"document; a third-party extraction ({et.source_url[:60]}) may not replace it"
                )
                held += 1
                fresh_by_key[(et.name, cc)] = existing
                continue

            regression = (
                _computable_regression(existing, rt, new_components)
                if existing is not None else None
            )
            if regression:
                record_event(
                    session,
                    decision="hold",
                    reason="computable_regression",
                    utility_id=utility_id,
                    before_tariff_id=existing.id,
                    source_url=et.source_url,
                    source_document_hash=doc_hash,
                    payload={
                        "proposed": {
                            "name": et.name,
                            "rate_type": rt.value,
                            "effective_date": eff_date.isoformat() if eff_date else None,
                            "components": serialize_components(new_components),
                        },
                        "computable_reasons": regression,
                    },
                    **event_kw,
                )
                log.warning(
                    f"    HOLD '{et.name}': live row {existing.id} is computable and the "
                    f"extraction is not ({', '.join(regression[:4])}) — live row unchanged"
                )
                held += 1
                fresh_by_key[(et.name, cc)] = existing
                continue

            tariff_obj = Tariff(
                utility_id=utility_id,
                name=et.name,
                code=code_clipped or (existing.code if existing else None),
                customer_class=cc,
                rate_type=rt,
                is_default=existing.is_default if existing else False,
                description=et.description or (existing.description if existing else None),
                source_url=et.source_url or (existing.source_url if existing else None),
                source_document_hash=doc_hash,
                effective_date=eff_date or (existing.effective_date if existing else None),
                last_verified_at=now,
                approved=False,
                confidence_score=conf_score,
                confidence_factors=conf_factors,
            )
            tariff_obj.rate_components.extend(new_components)
            session.add(tariff_obj)
            session.flush()
            if existing is None:
                record_event(
                    session,
                    decision="insert",
                    utility_id=utility_id,
                    after_tariff_id=tariff_obj.id,
                    source_url=et.source_url,
                    **event_kw,
                )
            else:
                supersede_tariff(
                    session, existing,
                    successor=tariff_obj,
                    reason="source_upgrade" if source_upgrade else "refresh",
                    source_url=et.source_url,
                    **event_kw,
                )
                log.info(
                    f"    Revised '{et.name}': {existing.id} → {tariff_obj.id} "
                    + (
                        "(same rates, third-party → official source)"
                        if source_upgrade
                        else "(soft supersede, prior components retained)"
                    )
                )
            stored += 1
            fresh_by_key[(et.name, cc)] = tariff_obj

        # Targeted OpenEI supersede: when a freshly-extracted tariff
        # plausibly names the same product as a 2017-era OpenEI import
        # for the same utility+class, soft-supersede the OpenEI row
        # (supersede_reason='matcher') so the API hides it while keeping
        # the audit trail. Runs BEFORE reconciliation so it works even on
        # partial extractions. See tariffs_likely_same() for the matcher.
        if fresh_by_key:
            session.flush()
            fresh_ids = {obj.id for obj in fresh_by_key.values()}
            openei_siblings = session.execute(
                select(Tariff).where(
                    Tariff.utility_id == utility_id,
                    Tariff.openei_id.is_not(None),
                    Tariff.superseded_by_tariff_id.is_(None),
                    Tariff.supersede_reason.is_(None),
                )
            ).scalars().all()
            absorbed = 0
            for ot in openei_siblings:
                if ot.id in fresh_ids or is_protected(ot):
                    continue  # re-verified this run, or curated
                for (fresh_name, fresh_class), fresh_obj in fresh_by_key.items():
                    if ot.customer_class != fresh_class:
                        continue
                    if tariffs_likely_same(ot.name, fresh_name):
                        supersede_tariff(
                            session, ot, successor=fresh_obj, reason="matcher", **event_kw
                        )
                        absorbed += 1
                        break
            if absorbed:
                log.info(
                    f"  Superseded: marked {absorbed} stale OpenEI siblings "
                    f"as absorbed by fresh extractions (soft supersede)"
                )

        # Vintage soft-supersede: among live same-utility + same customer
        # class tariffs, collapse rate-book editions of the same product
        # (shared #1.1 / Rate #1.1, or stem match ignoring optional "Flat").
        # Keep newest effective_date; losers get supersede_reason='vintage'.
        if stored >= 1:
            session.flush()
            vintage_absorbed = supersede_older_vintages(
                session, utility_id, actor_type=actor_type, actor_id=actor_id
            )
            if vintage_absorbed:
                log.info(
                    f"  Vintage supersede: retired {vintage_absorbed} older "
                    f"rate-book siblings (soft supersede)"
                )

        # R21: retire old copies that a row written this run clearly
        # replaces — also when the coverage-gated reconcile below is skipped.
        if fresh_by_key:
            session.flush()
            replaced = supersede_clear_replacements(
                session, utility_id, {obj.id for obj in fresh_by_key.values()},
                utility_name=u_name, actor_type=actor_type, actor_id=actor_id,
            )
            if replaced:
                log.info(f"  Clear replacement: retired {replaced} old copies (soft supersede)")

        if stored >= 1:
            session.flush()
            _reconcile_missing_tariffs(
                session, utility_id, tariffs, fresh_by_key,
                actor_type=actor_type, actor_id=actor_id,
            )

        session.commit()

    log.info(
        f"  Stored {stored} tariffs for utility {utility_id}"
        + (f" ({held} held: protected live rows)" if held else "")
    )
    return stored


def _reconcile_missing_tariffs(
    session,
    utility_id: int,
    tariffs: list[ExtractedTariff],
    fresh_by_key: dict,
    *,
    actor_type: str,
    actor_id: str | None,
) -> int:
    """Soft-retire live rows that a (near-)complete extraction no longer lists.

    Scoped per customer class present in the extraction and to rows whose
    source_url domain was fetched this run. Skipped for a class when the
    extraction covers < RECONCILE_MIN_COVERAGE of its live rows (partial
    extraction). OpenEI seeds, absorb targets and protected rows are never
    retired here; a protected row gets a ``hold`` event instead. Nothing is
    ever deleted. Returns the number of rows retired.
    """
    from sqlalchemy import select
    from app.models import Tariff, CustomerClass
    from app.services.tariff_history import is_live, is_protected, record_event, supersede_tariff

    class_map = {k: CustomerClass(k) for k in _STORE_CLASS_MAP_KEYS}
    names_by_class: dict = {}
    for et in tariffs:
        cc = class_map.get(et.customer_class)
        if cc:
            names_by_class.setdefault(cc, set()).add(et.name)

    fetched_domains = {
        urlparse(et.source_url).netloc.replace("www.", "")
        for et in tariffs if et.source_url
    } - {""}
    if not fetched_domains:
        log.warning(
            "  Reconciliation SKIPPED: no source domains in current "
            "extraction — cannot safely determine stale tariffs"
        )
        return 0

    all_rows = session.execute(
        select(Tariff).where(Tariff.utility_id == utility_id)
    ).scalars().all()
    successor_ids = {t.superseded_by_tariff_id for t in all_rows if t.superseded_by_tariff_id}
    keep_ids = {obj.id for obj in fresh_by_key.values()}

    retired = 0
    for cc, names in names_by_class.items():
        live = [t for t in all_rows if t.customer_class == cc and is_live(t)]
        present = sum(1 for (_, c) in fresh_by_key if c == cc)
        if present < len(live) * RECONCILE_MIN_COVERAGE:
            log.warning(
                f"  Reconciliation SKIPPED for {cc.value}: extraction covers "
                f"{present} of {len(live)} live tariffs "
                f"(< {RECONCILE_MIN_COVERAGE:.0%}) — possible partial extraction"
            )
            continue
        for t in live:
            if t.name in names or t.id in keep_ids:
                continue
            if t.openei_id is not None or t.id in successor_ids or not t.source_url:
                continue
            t_domain = urlparse(t.source_url).netloc.replace("www.", "")
            if not t_domain or t_domain not in fetched_domains:
                continue
            if is_protected(t):
                record_event(
                    session,
                    decision="hold",
                    reason="missing_from_extraction",
                    utility_id=utility_id,
                    before_tariff_id=t.id,
                    actor_type=actor_type,
                    actor_id=actor_id,
                )
                log.info(f"    Kept protected '{t.name}' (absent from extraction)")
                continue
            supersede_tariff(
                session, t,
                reason="reconcile_missing",
                actor_type=actor_type,
                actor_id=actor_id,
            )
            retired += 1
            log.info(f"    Retired (soft): {t.name} ({t.customer_class.value})")
    if retired:
        log.info(
            f"  Reconciled: soft-retired {retired} tariffs not found in "
            f"current extraction (components retained)"
        )
    return retired


def update_monitoring_source(utility_id: int, rate_page_url: str, dry_run: bool):
    """Update or create a monitoring source for the discovered rate page.
    Uses direct SQLAlchemy access instead of HTTP API calls."""
    if dry_run:
        log.info(f"  DRY RUN: Would update monitoring source for utility {utility_id}")
        return
    if not rate_page_url:
        return

    from sqlalchemy import select
    from sqlalchemy.orm import Session
    from app.db.session import get_sync_engine
    from app.models.monitoring import MonitoringSource, MonitoringStatus
    from app.services.source_type import classify_source, source_rank

    ctx = _source_context(get_utility_info(utility_id))
    new_rank = source_rank(classify_source(rate_page_url, ctx).source_type)
    engine = get_sync_engine()
    with Session(engine) as session:
        stmt = (
            select(MonitoringSource)
            .where(MonitoringSource.utility_id == utility_id)
            .where(MonitoringSource.status == MonitoringStatus.ERROR)
            .limit(1)
        )
        source = session.execute(stmt).scalar_one_or_none()
        if source and new_rank > source_rank(classify_source(source.url, ctx).source_type):
            log.info(
                f"  Kept monitoring source {source.id} on {source.url[:80]} "
                f"(more official than {rate_page_url[:80]})"
            )
        elif source:
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
    from app.models import MonitoringSource, MonitoringStatus, Utility

    engine = get_sync_engine()
    with Session(engine) as session:
        u = session.execute(
            select(Utility).where(Utility.id == utility_id)
        ).scalar_one_or_none()
        if not u:
            return {"id": utility_id, "name": f"Utility #{utility_id}"}
        monitored = session.execute(
            select(MonitoringSource.url)
            .where(MonitoringSource.utility_id == utility_id)
            .where(MonitoringSource.status != MonitoringStatus.ERROR)
            .order_by(MonitoringSource.id)
        ).scalars().all()
        return {
            "id": u.id,
            "name": u.name,
            "state": u.state_province,
            "country": u.country.value if u.country else "",
            "website_url": u.website_url,
            "rate_page_url_override": getattr(u, "rate_page_url_override", None),
            "tariff_page_urls": u.tariff_page_urls,
            "monitoring_urls": [m for m in monitored if m],
        }


def _source_context(info: dict):
    """Classifier context (official hosts) for a ``get_utility_info`` dict."""
    from app.services.source_type import UtilitySourceContext, configured_urls

    return UtilitySourceContext(
        website_url=info.get("website_url"),
        official_urls=configured_urls(info.get("tariff_page_urls"), info.get("rate_page_url_override")),
        country=info.get("country"),
        state_province=info.get("state"),
    )


def _known_rate_urls(info: dict) -> list[str]:
    """Rate URLs already on file for the utility: configured, then monitored."""
    from app.services.source_type import configured_urls

    return list(dict.fromkeys([
        *configured_urls(info.get("tariff_page_urls")),
        *(info.get("monitoring_urls") or []),
    ]))


def prefer_official_targets(
    primary: str,
    alts: list[str],
    known_urls: list[str],
    ctx,
    *,
    locked: bool = False,
) -> tuple[str, list[str]]:
    """Order fetch targets official → unknown → third-party.

    When the primary is not official (or missing) and an official candidate
    is known, the official one becomes primary and the old primary is
    demoted into the alternates. An operator override / preferred tariff
    book (``locked``) stays primary. Nothing is dropped: a third-party URL
    that is the only candidate is still tried.
    """
    from app.services.source_type import OFFICIAL, THIRD_PARTY, classify_source, rank_urls

    pool = rank_urls([u for u in [*alts, *known_urls] if u and u != primary], ctx)
    if locked:
        return primary, pool

    def _is_official(u: str) -> bool:
        return classify_source(u, ctx).source_type == OFFICIAL

    if pool and _is_official(pool[0]) and (not primary or not _is_official(primary)):
        new_primary = pool.pop(0)
        if primary:
            log.info(f"  Preferring official URL {new_primary[:80]} over {primary[:80]}")
            pool = rank_urls([primary, *pool], ctx)
        return new_primary, pool
    usable = [u for u in pool if classify_source(u, ctx).source_type != THIRD_PARTY]
    if not primary and usable:
        # Search found nothing reachable, but a rate URL is already on file
        # (configured / monitored). Try it rather than stopping with
        # "No rate page found" — utilities with no website_url on record
        # (SRP, PG&E, Pedernales) never classify their own URLs as official.
        new_primary = usable[0]
        # R19: prefer the newest dated official rate document on file (SRP
        # 2025 ratebook with 2026 TCA over the undated Nov-2023 ratebook.pdf).
        newest = newest_dated_rate_document(usable, min_year=date.today().year - 1)
        if newest and newest[0] != new_primary:
            cur_v = url_document_vintage(new_primary)
            if cur_v is None or cur_v < newest[1]:
                log.info(
                    f"  Preferring newest dated rate document {newest[0][:80]} "
                    f"({newest[1][0]}-{newest[1][1]:02d}) over {new_primary[:80]}"
                )
                new_primary = newest[0]
        pool.remove(new_primary)
        log.info(f"  No search hit — falling back to known rate URL {new_primary[:80]}")
        return new_primary, pool
    return primary, pool


def _third_party_upgrade_pending(utility_id: int, pages: list, ctx) -> bool:
    """Live tariffs still sourced from a third party while this run reached an
    official page — worth extracting even if the page fingerprint is unchanged."""
    from sqlalchemy import select
    from sqlalchemy.orm import Session
    from app.db.session import get_sync_engine
    from app.models import Tariff
    from app.services.source_type import OFFICIAL, THIRD_PARTY, classify_source

    if not any(classify_source(p.url, ctx).source_type == OFFICIAL for p in pages or []):
        return False
    with Session(get_sync_engine()) as session:
        urls = session.execute(
            select(Tariff.source_url).where(
                Tariff.utility_id == utility_id,
                Tariff.superseded_by_tariff_id.is_(None),
                Tariff.supersede_reason.is_(None),
            )
        ).scalars().all()
    return any(classify_source(u, ctx).source_type == THIRD_PARTY for u in urls)


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
                fetch_billdata_xml, parse_billdata_xml, match_ldc_delivery,
                build_tariff_entries, store_oeb_tariffs, get_ontario_utilities,
                BILLDATA_URL,
            )
            html = fetch_oeb_page()
            rates = parse_oeb_rates(html)
            if not rates.tou and not rates.tiered:
                log.warning("  OEB scraper returned no rates — falling back to standard pipeline")
                return None

            # Resolve the utility name for BillData matching.
            util_name = ""
            try:
                for u in get_ontario_utilities():
                    if u["id"] == utility_id:
                        util_name = u["name"]
                        break
            except Exception:
                util_name = ""

            ldc = None
            try:
                ldc = match_ldc_delivery(util_name, parse_billdata_xml(fetch_billdata_xml()))
            except Exception as be:
                log.warning(f"  BillData.xml unavailable ({be}) — commodity-only RPP")

            # R21: residential only — no Small Business (commercial) rows.
            all_tariffs = build_tariff_entries(rates, "residential", ldc=ldc)

            if not dry_run:
                count = store_oeb_tariffs(utility_id, all_tariffs, dry_run)
                log.info(
                    f"  Stored {count} OEB tariffs for utility {utility_id}"
                    + (f" (+BillData {ldc.distributor})" if ldc else "")
                )
            else:
                count = len(all_tariffs)
                log.info(
                    f"  DRY RUN: Would store {count} OEB tariffs for utility {utility_id}"
                    + (f" (+BillData {ldc.distributor})" if ldc else "")
                )

            result = PipelineResult(
                utility_id=utility_id,
                utility_name=util_name,
                country=country,
                state=state,
                phase1_rate_page_url=(
                    f"{BILLDATA_URL} (OEB BillData + RPP)"
                    if ldc else "https://www.oeb.ca (centralized regulator)"
                ),
            )
            result.phase4_validation = {
                "valid": count,
                "source": "OEB+BillData" if ldc else "OEB centralized",
                "ldc_delivery": bool(ldc),
            }
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

            tariffs = phase3_extract_tariffs([page], utility_name, state=state)
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


# ---------------------------------------------------------------------------
# Stale-tariff supersede helpers (used both at write-time in Phase 4 and by
# the offline `quality_cleanup.py` script). Kept here so a single matcher
# governs both code paths.
# ---------------------------------------------------------------------------

# Tokens that distinguish otherwise-similar tariff names. If both sides
# carry conflicting discriminators, they are NOT the same product.
_TARIFF_DISCRIMINATORS: frozenset[str] = frozenset({
    # phase / voltage
    "single", "multi", "multiphase", "polyphase", "three", "threephase",
    "1ph", "3ph", "primary", "secondary", "transmission", "subtransmission",
    # size class
    "small", "medium", "large",
    # season
    "summer", "winter", "seasonal",
    # rate-shape
    "tou", "flat", "tiered", "demand",
    "flex", "flexible", "block", "declining",
    # program type
    "optional", "experimental", "pilot", "trial",
    # load-shape / DR
    "interruptible", "curtailable", "controlled", "controllable",
    "load", "management",
    # special-purpose
    "lighting", "heating", "heater", "irrigation", "agricultural", "farm",
    "rural", "urban", "inside", "outside",
    "second", "water",
    "additional", "auxiliary", "supplemental",
    # time-encoded forms (post-normalization)
    "timeofuse", "timeofday", "tod",
    # DG / DERs
    "ev", "solar", "wind", "renewable", "metering", "net",
    "plugin", "vehicle", "charging", "pev", "evse",
    # equity / income
    "lifeline", "subsidized", "low", "income", "senior",
    # fuel mix
    "dual", "fuel",
    # usage qualifiers
    "limited", "without",
    # negation prefixes — meaningful when paired with a discriminator,
    # e.g. "Demand Billing" vs "Non-Demand Billing"
    "no", "non",
})

_TARIFF_NAME_STOPWORDS: frozenset[str] = frozenset({
    "and", "or", "for", "the", "of", "in", "to", "a", "an", "is",
    "by", "as", "on", "at", "with",
    "rate", "plan", "schedule", "service",
})

_BLURB_HINTS: frozenset[str] = frozenset({
    "for", "with", "applies", "applicable", "whose", "when",
    "that", "including",
})


def _norm_tariff_name_for_match(name: str) -> str:
    """Normalize a tariff name for cross-source matching.

    Critically, hyphens / dashes / slashes are turned into spaces BEFORE
    stripping the rest of the punctuation, so 'Time-of-Use' tokenizes as
    {time, of, use} rather than collapsing into the compound 'timeofuse'.
    """
    if not name:
        return ""
    n = name.lower().strip()
    n = re.sub(r"[\-–—/_]+", " ", n)
    n = re.sub(r"[^a-z0-9\s]", "", n)
    return " ".join(n.split())


def _strip_tariff_blurb(name: str) -> str:
    """Drop trailing '- <descriptive blurb>' that OpenEI is fond of, but
    preserve trailing '- <real qualifier>' like '- Single Phase'."""
    for sep in (" - ", " — "):
        if sep in name:
            head, _, tail = name.partition(sep)
            tail_toks = [t.lower() for t in tail.split()]
            if len(tail_toks) > 6 or any(t in _BLURB_HINTS for t in tail_toks):
                return head.strip()
    return name.strip()


def _extract_rate_code_tokens(name: str) -> set[str]:
    """Return rate-code-shaped tokens (e.g. 'sc1c', 'e1', 'd1', 'rsm',
    '1c', 'tou-c') from a tariff name. A rate-code token is short
    (2-6 chars), contains at least one digit AND one letter, and is
    not a common English word.

    Run on the raw name (not the heavily-normalized version) so the
    embedded parens-codes like '(E-1)' or '(RSM)' survive.
    """
    if not name:
        return set()
    n = name.lower()
    out: set[str] = set()

    # Recognise common verbose forms of rate-code references and add the
    # compact equivalents. NY utilities label residential and general
    # service classes as "Service Classification No. 1", "No. 1C",
    # "No. 2" etc. — but the URDB seed names abbreviate to "SC1",
    # "SC1C", "SC2". Normalise both directions so they pair.
    #   "Service Classification No. 1C"  -> sc1c
    #   "Schedule D-1"                    -> d1
    #   "Rate Schedule E-TOU-C"           -> etouc
    for m in re.finditer(r"service classification\s*(?:no\.?|number)?\s*([0-9]+[a-z]?)",
                         n):
        out.add("sc" + m.group(1))
    for m in re.finditer(r"\b(?:rate\s+)?schedule\s+([a-z]+[\-\.]?[0-9]+[a-z]?)",
                         n):
        compact = re.sub(r"[\-.]+", "", m.group(1))
        if 2 <= len(compact) <= 6:
            out.add(compact)

    # Tokens that *look* like rate codes but aren't. Time-of-day stamps
    # (4pm, 7pm, 9am), voltage labels (12kv, 480v, 240v), and a handful
    # of unit / phase markers slip through the short-token heuristic.
    # Excluding them here prevents false-positive matches like
    # "Time of Use 4pm-7pm Weekdays" <-> "Time Advantage 7pm to noon".
    def _is_real_rate_code(tok: str) -> bool:
        if re.fullmatch(r"\d{1,2}(am|pm|a|p)", tok):
            return False
        if re.fullmatch(r"\d{1,4}(kv|v|kw|mw|hz|w|kva|mva)", tok):
            return False
        if re.fullmatch(r"\d{1,2}(st|nd|rd|th)", tok):
            return False
        return True

    # Tokenize on spaces, parens, slashes, colons, commas
    raw = re.split(r"[\s\(\)\[\]\{\}/:,;]+", n)
    for tok in raw:
        # Remove leading/trailing dashes/dots but keep internal ones
        t = tok.strip("-.")
        if 2 <= len(t) <= 6 and any(c.isdigit() for c in t) and any(c.isalpha() for c in t):
            if not _is_real_rate_code(t):
                continue
            out.add(t)
            # Also add the version with internal punctuation removed
            stripped = re.sub(r"[\-.]+", "", t)
            if 2 <= len(stripped) <= 6 and _is_real_rate_code(stripped):
                out.add(stripped)
    return out


def _rate_codes_match(codes_a: set[str], codes_b: set[str]) -> bool:
    """True if a rate code on one side names the SAME code as the other.

    Exact equality always matches. The only partial match allowed is a
    pure letter PREFIX difference: '1c' matches 'sc1c' (the URDB 'SC'
    classification prefix), because the trailing code body is identical.

    A general substring rule used to live here and caused false merges:
    'sc1' is a substring of 'sc1c' but SC-1 and SC-1C are DIFFERENT
    products (same for 'd1' vs 'd1a', 'e1' vs 'e16'). Trailing-character
    differences always mean a different rate, so they never match now.
    """
    for a in codes_a:
        for b in codes_b:
            if a == b:
                return True
            shorter, longer = (a, b) if len(a) < len(b) else (b, a)
            if len(shorter) < 2 or not any(c.isdigit() for c in shorter):
                continue
            if longer.endswith(shorter):
                prefix = longer[: -len(shorter)]
                if prefix.isalpha() and len(prefix) <= 3:
                    return True
    return False


def tariffs_likely_same(name_a: str, name_b: str) -> bool:
    """True iff the two tariff names plausibly describe the same product.

    This is the matcher used both at extraction-write time (to supersede
    OpenEI siblings) and offline by `quality_cleanup.py`. Conservative:
    favors false negatives over false positives.
    """
    a_full = _norm_tariff_name_for_match(name_a)
    b_full = _norm_tariff_name_for_match(name_b)
    if not a_full or not b_full:
        return False
    if a_full == b_full:
        return True
    a_head = _norm_tariff_name_for_match(_strip_tariff_blurb(name_a))
    b_head = _norm_tariff_name_for_match(_strip_tariff_blurb(name_b))
    if a_head and b_head and a_head == b_head:
        return True

    ta = set((a_head or a_full).split())
    tb = set((b_head or b_full).split())

    # Rate-code fast path: when both names encode the SAME rate code
    # (exact, or letter-prefix variant like '1c' vs 'sc1c'), treat them
    # as the same product — unless both names carry discriminator words
    # and they disagree ('SC1 TOU' vs 'SC1 Flat' are different products
    # even though the code matches). One-sided discriminators are fine:
    # URDB names often omit the rate shape the marketing name spells out.
    rc_a = _extract_rate_code_tokens(name_a)
    rc_b = _extract_rate_code_tokens(name_b)
    if rc_a and rc_b and _rate_codes_match(rc_a, rc_b):
        a_disc_fast = ta & _TARIFF_DISCRIMINATORS
        b_disc_fast = tb & _TARIFF_DISCRIMINATORS
        if not a_disc_fast or not b_disc_fast or a_disc_fast == b_disc_fast:
            return True

    if not ta or not tb:
        return False
    overlap = len(ta & tb) / min(len(ta), len(tb))
    if overlap < 0.6:
        return False
    a_disc = ta & _TARIFF_DISCRIMINATORS
    b_disc = tb & _TARIFF_DISCRIMINATORS
    if a_disc != b_disc:
        return False
    def _codes(toks: set[str]) -> set[str]:
        return {
            t for t in toks
            if 1 <= len(t) <= 4
            and any(c.isalpha() for c in t)
            and t not in _TARIFF_DISCRIMINATORS
            and t not in _TARIFF_NAME_STOPWORDS
        }
    ca, cb = _codes(ta), _codes(tb)
    if (ca or cb) and not (ca & cb):
        return False
    return True


# ---------------------------------------------------------------------------
# Vintage (rate-book roll) soft-supersede helpers
# ---------------------------------------------------------------------------
# Distinct from tariffs_likely_same(): that matcher must keep TOU vs Flat as
# different products. Vintage matching is for successive rate-book editions
# of the SAME product (e.g. NL "Rate #1.1 Domestic Service" 2026-07-01 vs
# "Domestic Service (Flat)" 2025-07-01). Optional shape words like "Flat"
# do not differentiate vintages; shared rate codes like #1.1 bind them.

_VINTAGE_OPTIONAL_SHAPE: frozenset[str] = frozenset({"flat"})

_RATEBOOK_CODE_RE = re.compile(
    r"(?:rate\s*)?#\s*(\d+(?:\.\d+)?)"
    r"|(?:^|[^a-z0-9])rate\s+#?\s*(\d+\.\d+)(?![a-z0-9])",
    re.IGNORECASE,
)


def extract_ratebook_codes(name: str, code: str | None = None) -> set[str]:
    """Extract rate-book style codes such as #1.1 / Rate #1.1 / 1.1.

    Also accepts alphanumeric codes from the ``code`` column or name via the
    existing short-token extractor (SC1, RS, etc.).
    """
    out: set[str] = set()
    for raw in (name or "", code or ""):
        if not raw:
            continue
        for m in _RATEBOOK_CODE_RE.finditer(raw):
            token = m.group(1) or m.group(2)
            if token:
                out.add(token.lower())
        # Digits-only schedule codes stored in the code column ("1.1", "1")
        c = str(raw).strip().lower()
        if re.fullmatch(r"\d+(?:\.\d+)?", c):
            out.add(c)
        out |= {t for t in _extract_rate_code_tokens(raw)}
    return out


def _vintage_name_stem(name: str) -> str:
    """Name stem for vintage identity: drop rate codes + optional 'flat'."""
    n = _strip_tariff_blurb(name or "")
    n = _RATEBOOK_CODE_RE.sub(" ", n)
    n = _norm_tariff_name_for_match(n)
    toks = [
        t for t in n.split()
        if t not in _VINTAGE_OPTIONAL_SHAPE
        and t not in _TARIFF_NAME_STOPWORDS
        and not re.fullmatch(r"\d+(?:\.\d+)?", t)
    ]
    return " ".join(toks)


def _rate_type_family(rate_type) -> str:
    s = rate_type.value if hasattr(rate_type, "value") else str(rate_type or "")
    s = s.lower().strip()
    if "tou" in s:
        return "tou"
    if "demand" in s:
        return "demand"
    if "tier" in s:
        return "tiered"
    if s in ("", "none", "null", "flat"):
        return "flat"
    return s or "flat"


def _rate_types_compatible_for_vintage(a, b) -> bool:
    fa, fb = _rate_type_family(a), _rate_type_family(b)
    return fa == fb


def _vintage_strong_discriminators(name: str) -> set[str]:
    """Discriminators that still separate products for vintage purposes.

    ``flat`` is intentionally excluded — rate books often label the default
    domestic schedule "(Flat)" without meaning a distinct product.
    """
    stem = _vintage_name_stem(name)
    return set(stem.split()) & (_TARIFF_DISCRIMINATORS - _VINTAGE_OPTIONAL_SHAPE)


# R20: words that only describe an edition / boilerplate, never a different
# plan. Everything else left in a name after codes and stopwords is a
# qualifier ("geothermal", "full", "equipment", "3", "pm", "heating" ...).
_R20_EDITION_WORDS: frozenset[str] = frozenset({
    "standard", "price", "pricing", "rates",
    "tariff", "electric", "electricity", "customer", "customers", "general",
    "effective", "eff", "edition", "revised", "revision", "updated", "current",
    "new", "issued", "sheet", "no", "number", "flat",
    "jan", "january", "feb", "february", "mar", "march", "apr", "april", "may",
    "jun", "june", "jul", "july", "aug", "august", "sep", "sept", "september",
    "oct", "october", "nov", "november", "dec", "december",
}) - {"no"}  # "no" stays a qualifier ("No Demand")


def _r20_code_pieces(codes: set[str]) -> set[str]:
    out: set[str] = set()
    for c in codes:
        c = str(c).lower()
        out.add(c)
        out |= set(re.findall(r"[a-z]+|\d+(?:\.\d+)?", c))
        out.add(re.sub(r"[^a-z0-9]", "", c))
    return out


def vintage_qualifiers(name: str, code: str | None = None) -> set[str]:
    """Plan-identity words in a name: everything except rate codes,
    stopwords, edition / boilerplate words and years."""
    codes = extract_ratebook_codes(name, code)
    pieces = _r20_code_pieces(codes)
    raw = _strip_tariff_blurb(name or "").lower()
    toks = re.findall(r"[a-z]+|\d+(?:\.\d+)?", raw)
    out = set()
    for t in toks:
        if t in pieces or t in _TARIFF_NAME_STOPWORDS or t in _R20_EDITION_WORDS:
            continue
        if re.fullmatch(r"(19|20)\d{2}", t):
            continue
        out.add(t)
    return out


def same_vintage_product(
    name_a: str,
    name_b: str,
    *,
    code_a: str | None = None,
    code_b: str | None = None,
    rate_type_a=None,
    rate_type_b=None,
) -> bool:
    """True if two live tariffs are editions of the same plan.

    R20: strict. Rate types must be compatible, strong discriminators must
    agree, two different rate codes (D1.11 vs D1.7) never match, and the
    names' qualifier words must be identical once codes, stopwords and
    edition words (year, month, "revised", "flat" ...) are removed — so
    "Geothermal Time of Day" ≠ "Time of Day", "RS-1 EV Full Installation" ≠
    "RS-1 EV Equipment only Installation". A shared code binds only when the
    qualifiers also agree. NL "Rate #1.1 Domestic Service" still matches
    "Domestic Service (Flat)".
    """
    if not _rate_types_compatible_for_vintage(rate_type_a, rate_type_b):
        return False
    if _vintage_strong_discriminators(name_a) != _vintage_strong_discriminators(name_b):
        return False
    codes_a = extract_ratebook_codes(name_a, code_a)
    codes_b = extract_ratebook_codes(name_b, code_b)
    if codes_a and codes_b and not (codes_a & codes_b):
        return False
    qa = vintage_qualifiers(name_a, code_a)
    qb = vintage_qualifiers(name_b, code_b)
    if qa != qb:
        return False
    if codes_a & codes_b:
        return True
    return bool(qa)


def choose_vintage_keeper(candidates: list) -> object:
    """Keep newest *currently effective* date; future-dated siblings lose.

    Among candidates with effective_date <= today (or blank), prefer the
    newest effective_date; tie-break protected (approved / repair / manual)
    over scraped, then last_verified_at, then id. Future-dated coming-TOU
    rows are never chosen over a currently billed interim / current column.
    """
    from datetime import date as _date, datetime as _datetime
    from app.services.tariff_history import is_protected

    today = _date.today()

    def _key(t):
        eff = getattr(t, "effective_date", None)
        currently = 1
        if eff is None:
            eff_key = _date.min
        elif eff > today:
            currently = 0
            eff_key = eff
        else:
            eff_key = eff
        ver = getattr(t, "last_verified_at", None)
        if ver is None:
            ver = _datetime.min.replace(tzinfo=timezone.utc)
        elif ver.tzinfo is None:
            ver = ver.replace(tzinfo=timezone.utc)
        tid = getattr(t, "id", None) or 0
        return (currently, eff_key, is_protected(t), ver, tid)

    return max(candidates, key=_key)


def _r20_same(a, b) -> bool:
    return same_vintage_product(
        getattr(a, "name", ""), getattr(b, "name", ""),
        code_a=getattr(a, "code", None), code_b=getattr(b, "code", None),
        rate_type_a=getattr(a, "rate_type", None), rate_type_b=getattr(b, "rate_type", None),
    )


def group_live_tariffs_by_vintage(tariffs: list) -> list[list]:
    """Groups of live tariffs that are editions of one plan.

    R20: complete linkage — a row joins a group only if it matches EVERY
    member, so A~B and B~C never collapse A, B and C together.
    """
    groups: list[list] = []
    for t in tariffs:
        for g in groups:
            if all(_r20_same(t, m) for m in g):
                g.append(t)
                break
        else:
            groups.append([t])
    return [g for g in groups if len(g) > 1]


def _r20_same_document(a, b) -> bool:
    ua, ub = (getattr(a, "source_url", None) or ""), (getattr(b, "source_url", None) or "")
    ha = getattr(a, "source_document_hash", None)
    hb = getattr(b, "source_document_hash", None)
    if ha and hb:
        return ha == hb
    return bool(ua) and ua == ub


def is_newer_edition(newer, older, *, today=None) -> bool:
    """True only when ``newer`` is a later, currently effective edition.

    Same effective date → never (same edition: dedupe's job, not vintage).
    A future-dated row never retires a current one. An undated older row is
    replaced by a dated row only when they come from different documents.
    """
    from datetime import date as _date
    today = today or _date.today()
    ne = getattr(newer, "effective_date", None)
    oe = getattr(older, "effective_date", None)
    if ne is None:
        return False
    if ne > today:
        return False
    if oe is None:
        return not _r20_same_document(newer, older)
    return ne > oe


def supersede_older_vintages(
    session,
    utility_id: int,
    reason: str = "vintage",
    *,
    actor_type: str = "pipeline",
    actor_id: str | None = None,
) -> int:
    """Soft-supersede older live vintages of the same product for a utility.

    Groups live tariffs by customer_class, clusters by vintage product key,
    keeps the newest effective_date (see choose_vintage_keeper), and
    soft-supersedes the losers. A protected loser (approved / repair /
    manual) is never retired onto an unprotected keeper — both stay live
    and a ``hold`` event is logged. Never hard-deletes. Returns the number
    of rows superseded.
    """
    from collections import defaultdict
    from sqlalchemy import select
    from app.models import Tariff
    from app.services.tariff_history import is_protected, record_event, supersede_tariff

    live = session.execute(
        select(Tariff).where(
            Tariff.utility_id == utility_id,
            Tariff.superseded_by_tariff_id.is_(None),
            Tariff.supersede_reason.is_(None),
        )
    ).scalars().all()
    by_class: dict = defaultdict(list)
    for t in live:
        by_class[t.customer_class].append(t)

    absorbed = 0
    for _cc, rows in by_class.items():
        # R20: each row is compared directly with every other row (no
        # chaining). It is retired only onto a strictly newer edition of the
        # same plan; if it matches several newer rows that are not the same
        # plan as each other, it is ambiguous and stays live.
        plan = []
        for loser in rows:
            def _beats(k, _l=loser):
                if is_newer_edition(k, _l):
                    return True
                # A protected (approved / repair / manual) row absorbs a
                # scraped copy of the same plan with the same date: the
                # human-checked row wins the tie; the protected row itself
                # is never retired here.
                return (
                    getattr(k, "effective_date", None) == getattr(_l, "effective_date", None)
                    and is_protected(k) and not is_protected(_l)
                )
            newer = [k for k in rows if k is not loser and _r20_same(loser, k) and _beats(k)]
            if not newer:
                continue
            if any(not _r20_same(a, b) for i, a in enumerate(newer) for b in newer[i + 1:]):
                log.info(f"  Vintage: '{loser.name}' matches several different newer plans — kept live")
                continue
            plan.append((loser, choose_vintage_keeper(newer)))
        for loser, keeper in plan:
            if is_protected(loser) and not is_protected(keeper):
                record_event(
                    session,
                    decision="hold",
                    reason="vintage_protected",
                    utility_id=utility_id,
                    before_tariff_id=loser.id,
                    after_tariff_id=keeper.id,
                    actor_type=actor_type,
                    actor_id=actor_id,
                )
                log.info(
                    f"  Vintage HOLD: protected '{loser.name}' kept live "
                    f"alongside newer scraped '{keeper.name}'"
                )
                continue
            supersede_tariff(
                session, loser,
                successor=keeper,
                reason=reason,
                actor_type=actor_type,
                actor_id=actor_id,
            )
            absorbed += 1
            log.info(
                f"  Vintage supersede: '{loser.name}' "
                f"(eff={loser.effective_date}) → keeper "
                f"'{keeper.name}' (eff={keeper.effective_date}) "
                f"reason={reason}"
            )
    return absorbed



# ---------------------------------------------------------------------------
# R21: clear-replacement clean-up (runs even on partial extractions)
# ---------------------------------------------------------------------------
# The coverage-gated reconcile (RECONCILE_MIN_COVERAGE) skips utilities whose
# run picked up < 75% of their live plans, and the vintage step only retires
# a row onto a strictly newer *dated* edition. Old copies of a plan that this
# run just re-read therefore stayed live beside the fresh row. This pass
# retires an old row only when a row written THIS RUN is clearly the same
# plan. Keepers are always fresh rows and fresh rows are never retired here,
# so there is no chaining.

# Words that never make two plans different once the rate code and rate-type
# family already agree ("R-1B Time-of-Use Residential Service" vs
# "Time of Use R-1B (TOU) Residential").
_R21_GENERIC_WORDS: frozenset[str] = frozenset({
    "residential", "residence", "service", "services", "rate", "rates",
    "schedule", "plan", "pricing", "price", "prices", "tou", "time", "use",
    "of", "day", "timeofuse", "timeofday", "tod", "tiered", "tier", "standard",
    "customers", "customer", "option",
})

# Ontario RPP plan families written by the OEB feed (code OEB-RPP-*).
_R21_RPP_NOISE: frozenset[str] = frozenset({
    "rpp", "regulated", "price", "prices", "pricing", "plan", "residential",
    "time", "of", "use", "tou", "ulo", "ultra", "low", "overnight", "tiered",
    "tier", "rates", "rate", "electricity", "service", "the",
})
_R21_PARTIAL_SCOPES = ("delivery_only", "supply_only")


def _r21_norm_code(code) -> str:
    return re.sub(r"[^a-z0-9]", "", str(code or "").lower())


def _r21_family(rate_type) -> str:
    return _rate_type_family(rate_type)


def rpp_plan_family(name: str, code: str | None = None) -> str | None:
    """'tou' / 'ulo' / 'tiered' for an Ontario RPP plan name, else None.

    Returns None when the name carries anything beyond RPP boilerplate
    ("COVID-19 Recovery Rate for Time-of-Use Customers", "Heat Pump ..."),
    so only plain RPP copies map to a family. Tier numbers are allowed
    ("Tiered RPP - Tier 1" is a partial copy of the tiered plan).
    """
    raw = f"{name or ''} {code or ''}".lower()
    toks = re.findall(r"[a-z]+|\d+", raw)
    if any(t not in _R21_RPP_NOISE and not t.isdigit() for t in toks):
        return None
    ts = set(toks)
    if "ulo" in ts or {"ultra", "overnight"} <= ts:
        return "ulo"
    if ts & {"tier", "tiered"}:
        return "tiered"
    if "tou" in ts or {"time", "use"} <= ts:
        return "tou"
    return None


def _r21_is_oeb_feed_row(t) -> bool:
    cf = getattr(t, "confidence_factors", None) or {}
    return str(getattr(t, "code", "") or "").upper().startswith("OEB-RPP") or cf.get("origin") == "oeb_feed"


def clearly_same_plan(old, new, *, utility_name: str = "") -> bool:
    """True when ``new`` (written this run) is clearly the same plan as ``old``.

    Any one of:
    * the strict R20 edition test (``same_vintage_product``);
    * the same non-empty rate code, compatible rate-type family ("complex"
      is compatible with any family), and identical qualifier words once
      codes, generic words and the utility's own name are removed;
    * an Ontario OEB-feed row and a plain scraped copy of the same RPP
      family (TOU / ULO / Tiered).
    """
    if _r20_same(old, new):
        return True
    on, nn = getattr(old, "name", "") or "", getattr(new, "name", "") or ""
    if _r21_is_oeb_feed_row(new) and not _r21_is_oeb_feed_row(old):
        fam_new = rpp_plan_family(nn.replace("—", " "), None)
        fam_old = rpp_plan_family(on, getattr(old, "code", None))
        return fam_new is not None and fam_new == fam_old
    co, cn = _r21_norm_code(getattr(old, "code", None)), _r21_norm_code(getattr(new, "code", None))
    if not co or co != cn:
        return False
    fo, fn = _r21_family(getattr(old, "rate_type", None)), _r21_family(getattr(new, "rate_type", None))
    if fo != fn and "complex" not in (fo, fn):
        return False
    noise = set(_R21_GENERIC_WORDS)
    noise |= _r20_code_pieces({str(old.code).lower(), str(new.code).lower()})
    noise |= set(re.findall(r"[a-z]+", (utility_name or "").lower())) - _TARIFF_DISCRIMINATORS
    # Full names (no blurb stripping): "Residential Service - General Use
    # and Space Heat Two Meters" is a different plan from "Schedule R -
    # Residential General Use" even though both are code R.
    return _r21_full_qualifiers(on, old.code) - noise == _r21_full_qualifiers(nn, new.code) - noise


def _r21_full_qualifiers(name: str, code: str | None) -> set[str]:
    pieces = _r20_code_pieces(extract_ratebook_codes(name, code))
    out = set()
    for t in re.findall(r"[a-z]+|\d+(?:\.\d+)?", (name or "").lower()):
        if t in pieces or t in _TARIFF_NAME_STOPWORDS or t in _R20_EDITION_WORDS:
            continue
        if re.fullmatch(r"(19|20)\d{2}", t):
            continue
        out.add(t)
    return out


def _r21_keeper_ok(new, *, today) -> bool:
    eff = getattr(new, "effective_date", None)
    if eff is not None and eff > today:
        return False  # future-dated rows never retire a current one
    cf = getattr(new, "confidence_factors", None) or {}
    if _r21_is_oeb_feed_row(new) and not cf.get("ontario_ldc_delivery"):
        return False  # commodity-only feed row is not the full price
    return str(cf.get("energy_scope") or "") not in _R21_PARTIAL_SCOPES


def _r21_date_ok(old, new) -> bool:
    oe, ne = getattr(old, "effective_date", None), getattr(new, "effective_date", None)
    if oe is None or ne is None or ne >= oe:
        return True
    # Official source over a third-party copy of the same plan wins even if
    # the third-party row claims a later date (official sources first).
    return (getattr(old, "source_type", None) == "third_party"
            and getattr(new, "source_type", None) == "official")


def plan_clear_replacements(live_rows: list, fresh_ids: set, *, utility_name: str = "", today=None) -> list[tuple]:
    """Pure planner: [(old_row, keeper_row)] pairs to retire. See
    ``supersede_clear_replacements``. Rows must share a customer class."""
    from datetime import date as _date
    from app.services.computable import evaluate_computable

    today = today or _date.today()
    fresh = [t for t in live_rows if t.id in fresh_ids and _r21_keeper_ok(t, today=today)]
    plan = []
    if not fresh:
        return plan
    for old in live_rows:
        if old.id in fresh_ids or getattr(old, "openei_id", None) is not None:
            continue
        cands = [f for f in fresh
                 if f.customer_class == old.customer_class
                 and clearly_same_plan(old, f, utility_name=utility_name)
                 and _r21_date_ok(old, f)]
        if not cands:
            continue
        if any(not clearly_same_plan(a, b, utility_name=utility_name) and not clearly_same_plan(b, a, utility_name=utility_name)
               for i, a in enumerate(cands) for b in cands[i + 1:]):
            log.info(f"  Replace: '{old.name}' matches several different fresh plans — kept live")
            continue
        keeper = choose_vintage_keeper(cands)
        try:
            o_ok = evaluate_computable(old.rate_type, list(old.rate_components or []), name=old.name).computable
            k_ok = evaluate_computable(keeper.rate_type, list(keeper.rate_components or []), name=keeper.name).computable
        except Exception:
            o_ok, k_ok = False, True
        if o_ok and not k_ok:
            log.info(f"  Replace: '{old.name}' is Mysa-complete and fresh '{keeper.name}' is not — kept live")
            continue
        plan.append((old, keeper))
    return plan


def supersede_clear_replacements(
    session,
    utility_id: int,
    fresh_ids: set,
    *,
    utility_name: str = "",
    reason: str = "replaced",
    actor_type: str = "pipeline",
    actor_id: str | None = None,
) -> int:
    """Soft-retire live rows that a row written this run clearly replaces.

    Runs whether or not the coverage-gated reconcile runs. A protected
    (approved / repair / manual) old row is never retired onto an
    unprotected keeper: a ``hold`` event is logged instead. Never deletes.
    """
    from collections import defaultdict
    from sqlalchemy import select
    from app.models import Tariff
    from app.services.tariff_history import is_protected, record_event, supersede_tariff

    if not fresh_ids:
        return 0
    live = session.execute(
        select(Tariff).where(
            Tariff.utility_id == utility_id,
            Tariff.superseded_by_tariff_id.is_(None),
            Tariff.supersede_reason.is_(None),
        )
    ).scalars().all()
    by_class: dict = defaultdict(list)
    for t in live:
        by_class[t.customer_class].append(t)
    retired = 0
    for rows in by_class.values():
        for old, keeper in plan_clear_replacements(rows, set(fresh_ids), utility_name=utility_name):
            if is_protected(old) and not is_protected(keeper):
                record_event(
                    session, decision="hold", reason="replaced_protected",
                    utility_id=utility_id, before_tariff_id=old.id, after_tariff_id=keeper.id,
                    actor_type=actor_type, actor_id=actor_id,
                )
                log.info(f"  Replace HOLD: protected '{old.name}' kept live beside '{keeper.name}'")
                continue
            supersede_tariff(session, old, successor=keeper, reason=reason,
                             actor_type=actor_type, actor_id=actor_id)
            retired += 1
            log.info(f"  Replaced: '{old.name}' ({old.id}, eff={old.effective_date}) → "
                     f"'{keeper.name}' ({keeper.id}, eff={keeper.effective_date})")
    return retired


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
    """Update last_verified_at on pipeline-verified tariffs to mark them as
    still current.

    Only rows that were ALREADY verified by an extraction are touched: an
    unchanged rate page re-confirms what we extracted from it, nothing
    else. Blanket-touching every row used to mark 2017 OpenEI seeds (never
    re-extracted) as "verified", silently corrupting freshness metrics and
    Track B's fresh/stranded split. Superseded/retired rows are skipped
    too — they are not served, so they should not look fresh. Pinned rows
    are skipped: only a verification of their own document re-verifies them.
    """
    from sqlalchemy.orm import Session
    from sqlalchemy import select, update
    from app.db.session import get_sync_engine
    from app.models import TariffPin
    from app.models.tariff import Tariff
    from app.services.pins import OPEN_PIN_STATES

    now = datetime.now(timezone.utc)
    engine = get_sync_engine()
    with Session(engine) as session:
        result = session.execute(
            update(Tariff)
            .where(
                Tariff.utility_id == utility_id,
                Tariff.last_verified_at.is_not(None),
                Tariff.superseded_by_tariff_id.is_(None),
                Tariff.supersede_reason.is_(None),
                ~Tariff.id.in_(
                    select(TariffPin.tariff_id).where(TariffPin.state.in_(OPEN_PIN_STATES))
                ),
            )
            .values(last_verified_at=now)
        )
        session.commit()
    log.info(
        f"  Refreshed last_verified_at on {result.rowcount} verified tariffs "
        f"for utility {utility_id} (content unchanged)"
    )


NAVIGATE_PROMPT = """You are navigating a utility company's website to find their residential electricity rate information.

Utility: {utility_name}
State: {state}
Current page: {current_url}
Page title: {page_title}

Here are ALL the links on this page:
{link_list}

Pick up to 5 links from the list above that most likely lead to the residential electricity price list or tariff document.
Best: links named like "Rates", "Rate schedules", "Tariffs", "Pricing", "Residential rates", "Time-of-use", "Domestic", or links to PDFs with those words.
Next: "Residential", "Home", "Billing", "My bill explained".
Never: login, pay bill, outage, careers, news, contact, commercial-only, gas-only, industrial, lighting, wholesale pages.
Copy each URL exactly as it appears in the list; never make one up.
Return only a JSON array of URLs, best first.
"""


def _host_key(netloc: str) -> str:
    """Normalize host for same-site checks (strip www.)."""
    h = (netloc or "").lower()
    return h[4:] if h.startswith("www.") else h


_RATE_LINK_SCORE_RE = re.compile(
    r"\brates?\b|\btariffs?\b|\bpricing\b|price\s*list|rate\s*schedules?|"
    r"time[\s-]*of[\s-]*use|\btou\b|\bdomestic\b|\bresidential\b|"
    r"electric(?:ity)?\s+rates?|\bschedule\b|\.pdf\b",
    re.IGNORECASE,
)


def _rank_links_for_nav(
    links: list[tuple[str, str]], *, limit: int = 50,
) -> list[tuple[str, str]]:
    """Prefer rate/tariff/PDF links before cutting to ``limit``.

    Footer "Rates & Tariffs" often sits past the first 50 DOM links; scoring
    by rate keywords keeps those in the LLM's candidate list.
    """
    scored: list[tuple[int, int, str, str]] = []
    for i, (url, text) in enumerate(links):
        blob = f"{url} {text}"
        score = len(_RATE_LINK_SCORE_RE.findall(blob))
        if url.lower().endswith(".pdf"):
            score += 2
        scored.append((-score, i, url, text))
    scored.sort()
    return [(u, t) for _, _, u, t in scored[:limit]]


def _extract_all_links(html: str, base_url: str) -> list[tuple[str, str]]:
    """Extract ALL links from an HTML page (not just rate-relevant ones)."""
    soup = BeautifulSoup(html, "html.parser")
    links = []
    seen = set()
    base_domain = _host_key(urlparse(base_url).netloc)
    for a in soup.find_all("a", href=True):
        full_url = urljoin(base_url, a["href"]).split("#")[0].split("?")[0]
        if full_url in seen:
            continue
        link_domain = _host_key(urlparse(full_url).netloc)
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
    cap_hit = bool(stats.get("llm_call_cap_hit", False))
    twopass_dropped = int(stats.get("twopass_truncated", 0))

    suffix = ""
    if cap_hit:
        suffix += " [LLM call budget exhausted before all pages were tried]"
    if twopass_dropped:
        suffix += f" [{twopass_dropped} identified tariffs dropped by two-pass cap]"

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
        return f"LLM returned 0 tariffs on all {llm_sent} pages{note}{suffix}"
    if llm_err and llm_err >= llm_sent:
        return f"LLM errored on all {llm_sent} attempts — possible API issue{suffix}"

    # Mixed — some extracted but none passed validation
    return (
        f"Extracted tariffs did not pass validation "
        f"({pages_total} pages total, {llm_sent} reached LLM, {llm_zero} returned 0)"
        f"{suffix}"
    )


@llm_cost.with_phase("phase5")
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
            # Run the PDF through Phase 3 like any other page. (This used
            # to return the RatePage itself in the tariffs slot, which
            # poisoned downstream handling with non-tariff objects.)
            try:
                phase3_stats: dict = {}
                tariffs = phase3_extract_tariffs(
                    [pdf_page], utility_name, stats=phase3_stats, state=state
                )
                for k, v in phase3_stats.items():
                    if k in stats and isinstance(stats[k], (int, float)):
                        stats[k] += int(v)
                    else:
                        stats[k] = v
                return tariffs, [pdf_page], stats
            except Exception as e:
                log.warning(f"  Phase 5: PDF homepage extraction failed: {e}")
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

    ranked_links = _rank_links_for_nav(all_links, limit=50)
    allowed_urls = {u for u, _ in ranked_links}
    link_list = "\n".join(f"- {text}: {url}" for url, text in ranked_links)

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
            model=SONNET_MODEL,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
            output_config={"effort": "low"},
        )
        from app.services.anthropic_compat import response_text

        text = response_text(resp.content).strip()
        if text.startswith("```"):
            text = re.sub(r"^```\w*\n?", "", text)
            text = re.sub(r"\n?```$", "", text)
        nav_urls = json.loads(text)
        if not isinstance(nav_urls, list):
            nav_urls = [nav_urls]
    except Exception as e:
        log.warning(f"  Phase 5: Navigation AI failed: {e}")
        return [], [], stats

    # Drop invented URLs that were not in the ranked candidate list.
    nav_urls = [
        u for u in nav_urls
        if isinstance(u, str) and u in allowed_urls
    ]
    log.info(f"  Phase 5: AI chose {len(nav_urls)} links to follow")
    stats["phase5_ai_picked"] = len(nav_urls)

    # Step 3: Fetch each suggested page with Playwright
    all_pages: list[RatePage] = []
    for url in nav_urls[:5]:
        if not isinstance(url, str) or not url.startswith("http"):
            continue
        nav_domain = _host_key(urlparse(url).netloc)
        if any(
            nav_domain == d or nav_domain.endswith(f".{d}")
            for d in THIRD_PARTY_DOMAINS
        ):
            log.info(f"  Phase 5: Skipping third-party aggregator: {url[:70]}")
            continue
        log.info(f"  Phase 5: Following {url[:80]}")
        # Fetch ONCE with Playwright and reuse the same HTML for both the
        # page content and its links (this used to fetch every URL twice).
        try:
            sub_html, sub_title = fetch_page_js(url, wait_ms=5000, ignore_https_errors=True)
        except Exception:
            sub_html, sub_title = None, ""
        if sub_html == FETCH_JS_DOWNLOAD_SENTINEL:
            pdf_page = _fetch_as_pdf_via_download(url)
            if pdf_page:
                all_pages.append(pdf_page)
            continue
        if not sub_html or len(sub_html.strip()) < 200:
            continue
        sub_text_content = _compress_whitespace(
            BeautifulSoup(sub_html, "html.parser").get_text(" ", strip=True)
        )
        if len(sub_text_content.strip()) >= 50:
            all_pages.append(RatePage(
                url=url,
                title=sub_title or "",
                page_type="html",
                content=sub_text_content,
                content_hash=hashlib.sha256(sub_text_content.encode()).hexdigest(),
            ))

            # Check sub-page links too (one level deeper), from the HTML we
            # already have.
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
        tariffs = phase3_extract_tariffs(all_pages, utility_name, stats=phase3_stats, state=state)
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
# Phase 6 was Gemini Deep Research. The 2026-10 Anthropic-only stack drop
# removed the runtime Gemini dependency; helpers below (_phase6_prompt /
# parse) remain so Mysa Completeness rules stay unit-tested, but
# phase6_deep_research never calls an LLM. PHASE6_ENABLED is ignored.

PHASE6_AGENT_DEFAULT = "deep-research-preview-04-2026"  # legacy; unused
PHASE6_MAX_WAIT_SEC_DEFAULT = 1200
PHASE6_POLL_INTERVAL_SEC = 20
PHASE6_MAX_TOKENS_DEFAULT = 3_000_000


def _phase6_enabled() -> bool:
    """Phase 6 Gemini Deep Research was removed; always False."""
    return False



_CA_PROVINCE_CODES = {
    "ON", "BC", "AB", "QC", "MB", "SK", "NS", "NB", "NL", "PE", "YT", "NT", "NU",
}


def _phase6_prompt(utility_name: str, state: str, attempted_urls: list[str] | None) -> str:
    """Build the Deep Research prompt.

    We stuff URLs we already tried into the prompt so the agent doesn't waste
    tokens rediscovering pages we've already confirmed fail (Cloudflare blocks,
    Angular SPA shells, image-only rate pages, etc.).

    Country-aware: Canadian provinces get 'Canada' and the provincial
    regulator framing — the old hardcoded 'USA' sent the agent hunting for
    nonexistent US PUC filings for Hydro-Québec et al.
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

    is_canada = (state or "").strip().upper() in _CA_PROVINCE_CODES
    country_name = "Canada" if is_canada else "USA"
    regulator_line = (
        "- Filings on the relevant provincial energy regulator (OEB, BCUC, "
        "AUC, Régie de l'énergie, etc.)."
        if is_canada
        else "- Filings on the relevant state Public Utility / Service Commission."
    )

    # Keep Phase 6 on the same Mysa Completeness field contract as Phase 3.
    # Concatenate rather than nest braces inside the f-string body.
    structured_rules = _STRUCTURED_RULES
    return (
        f"""Research task (scope-bounded, 10 minutes maximum):

Find the current published residential electricity tariffs for {utility_name} in {state}, {country_name}.

Authoritative sources ONLY:
- The utility's own corporate website (tariff/rates pages, tariff PDFs).
{regulator_line}
- Directly-linked utility tariff PDFs or regulatory orders.

Do NOT use third-party comparison or aggregator sites (energybot.com, electricrate.com, choose-energy.com, power2switch.com, nyenergyratings.com, saveonenergy.com, chooseenergy.com, findenergy.com, etc.). Do NOT use wholesale/generation-company sources.{tried_clause}

Scope limits:
- Only schedules that serve homes / dwellings / domestic / farm-and-home / residential single-phase customers (keep "General Service" / "Farm & Home" when the text says they apply to residences). Skip rates that serve only businesses, industry, lighting, irrigation, wholesale, standby, cogen, interruptible, fleet EV, or street light.
- Use the price in effect today. Skip cancelled, superseded, historic, withdrawn, or obsolete. If interim today + future TOU both appear, extract BOTH (today's billed price and the future-dated TOU).
- Stop once you have a reasonable residential set — do not exhaustively catalog every rider or adjustment.

Return your findings as a report ending with a fenced JSON block like this:

```json
[
  {{
    "name": "official tariff name",
    "code": "schedule code",
    "customer_class": "residential",
    "rate_type": "flat" | "tiered" | "tou" | "demand" | "seasonal" | "tou_tiered" | "seasonal_tou" | "seasonal_tiered" | "demand_tou" | "complex",
    "effective_date": "YYYY-MM-DD" or null,
    "source_url": "URL you used",
    "confidence": 0.0-1.0,
    "components": [
      {{
        "component_type": "energy" | "fixed" | "demand" | "minimum" | "adjustment",
        "unit": "¢/kWh" | "$/kWh" | "$/kW" | "$/month" | "¢/day" (as printed),
        "rate_value": <number as printed in that unit — do not convert cents>,
        "tier_min_kwh": <number or null>,
        "tier_max_kwh": <number or null>,
        "tier_label": <string or null>,
        "period_label": "On-Peak" | "Off-Peak" | "Mid-Peak" | null (display only),
        "period_start_time": "HH:MM" or null,
        "period_end_time": "HH:MM" or null,
        "day_type": "weekday" | "weekend" | "holiday" | "all" | null,
        "season": "Summer" | "Winter" | null (display only),
        "season_start_month": <1-12 or null>,
        "season_start_day": <1-31 or null>,
        "season_end_month": <1-12 or null>,
        "season_end_day": <1-31 or null>
      }}
    ]
  }}
]
```

Rules for the JSON:
- Include each schedule ONCE. Break tiers, seasons, and time-of-use periods out as separate components.
- Read numbers EXACTLY as printed in the source — do not estimate or round.
"""
        + structured_rules
        + """
- Stacking ¢/kWh riders (FAM, DSM/DCRR, Storm, fuel, efficiency, power-cost) and per-kWh delivery/regulatory charges: emit FULL PRICE all-in ENERGY (base + riders); keep ADJUSTMENT audit rows with included_in_energy=true.
- Delivery-only + published default/standard-offer supply: combine into ENERGY and set energy_scope delivery_plus_default_supply.
- If you cannot find the utility's current residential electric tariffs at all from authoritative sources, return an empty array [].
"""
    )


# Regex to extract a fenced JSON code block from the Deep Research report.
# Uses a non-greedy array match that handles nested objects.
_PHASE6_JSON_RE = re.compile(
    r"```\s*json\s*(\[[\s\S]*?\])\s*```",
    re.IGNORECASE,
)

# Trailing commas before } or ] — the most common LLM JSON defect.
_TRAILING_COMMA_RE = re.compile(r",\s*([}\]])")


def _phase6_extract_json(report_text: str) -> list | None:
    """Best-effort extraction of the tariff JSON array from a DR report.

    Tries, in order:
      1. The LAST fenced ```json array (the prompt asks for the report to
         END with it; earlier blocks are often examples or partial drafts).
      2. Any fenced code block containing an array.
      3. A bare top-level array (first '[' to last ']').
    Each candidate is retried with trailing commas stripped.
    """
    candidates: list[str] = [m.group(1) for m in _PHASE6_JSON_RE.finditer(report_text)]
    candidates.reverse()  # last block first

    generic = re.findall(r"```\w*\s*(\[[\s\S]*?\])\s*```", report_text)
    candidates.extend(reversed(generic))

    first, last = report_text.find("["), report_text.rfind("]")
    if first != -1 and last > first:
        candidates.append(report_text[first:last + 1])

    for js_text in candidates:
        for attempt in (js_text, _TRAILING_COMMA_RE.sub(r"\1", js_text)):
            try:
                raw = json.loads(attempt)
            except json.JSONDecodeError:
                continue
            if isinstance(raw, dict):
                raw = [raw]
            if isinstance(raw, list):
                return raw
    return None


def _phase6_parse_tariffs(report_text: str, fallback_source: str) -> list[ExtractedTariff]:
    """Find and parse the JSON block from a Deep Research report.

    Returns validated ExtractedTariff objects with the same customer-class
    and skip-keyword filtering we apply to Phase 3 LLM output.
    """
    raw = _phase6_extract_json(report_text)
    if raw is None:
        log.warning(
            "  Phase 6: report did not contain a parseable JSON tariff array"
        )
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
        if cclass not in EXTRACT_CLASSES:
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
                "period_start_time": c.get("period_start_time"),
                "period_end_time": c.get("period_end_time"),
                "day_type": c.get("day_type"),
                "season": c.get("season"),
                "season_start_month": c.get("season_start_month"),
                "season_start_day": c.get("season_start_day"),
                "season_end_month": c.get("season_end_month"),
                "season_end_day": c.get("season_end_day"),
            })
        if not components:
            log.info(f"    Phase 6: dropping '{name}' (no numeric components)")
            continue

        try:
            confidence = float(item.get("confidence") or 0.7)
        except (TypeError, ValueError):
            confidence = 0.7

        source_url = str(item.get("source_url") or fallback_source or "").strip()
        if source_url and _is_third_party_domain(source_url):
            log.info(f"    Phase 6: dropping '{name}' (third-party source {source_url[:60]})")
            continue

        tariffs.append(ExtractedTariff(
            name=name,
            code=str(item.get("code") or "").strip(),
            customer_class=cclass,
            rate_type=str(item.get("rate_type") or "").strip().lower(),
            description=f"Phase 6 Deep Research (item {idx})",
            source_url=source_url,
            effective_date=str(item.get("effective_date") or "").strip() or "",
            components=components,
            confidence=max(0.0, min(1.0, confidence)),
            extraction_tier="gemini_dr",
        ))

    return tariffs


@llm_cost.with_phase("phase6")
def _phase6_meter(interaction, stats: dict) -> None:
    """Price Deep Research usage on every exit, not only on completion.

    Timeouts and token-cap aborts still bill; previously they were never
    recorded (audit F7). When usage has a total but no in/out split, the
    total is priced at the input rate (a lower bound).
    """
    usage = getattr(interaction, "usage", None)
    if usage is not None:
        for attr, key in (
            ("total_tokens", "phase6_total_tokens"),
            ("total_input_tokens", "phase6_input_tokens"),
            ("total_output_tokens", "phase6_output_tokens"),
        ):
            try:
                value = int(getattr(usage, attr, 0) or 0)
            except (TypeError, ValueError):
                continue
            stats[key] = max(stats.get(key) or 0, value)
    tin, tout = stats.get("phase6_input_tokens") or 0, stats.get("phase6_output_tokens") or 0
    if not (tin or tout) and stats.get("phase6_total_tokens"):
        tin = stats["phase6_total_tokens"]
    llm_cost.record_manual("gemini_dr", tin, tout)
    if stats.get("phase6_status") != "completed":
        llm_cost.record_abort("gemini_dr", priced=bool(tin or tout))


def phase6_deep_research(
    utility_name: str,
    state: str,
    attempted_urls: list[str] | None = None,
    *,
    agent: str | None = None,
    max_wait_sec: int | None = None,
) -> tuple[list[ExtractedTariff], dict]:
    """Phase 6 stub — Gemini Deep Research was removed (Anthropic-only stack).

    Always returns no tariffs. Never raises. Kept so call sites in
    ``run_pipeline`` need no structural rewrite.
    """
    del utility_name, state, attempted_urls, agent, max_wait_sec  # unused
    stats: dict = {
        "phase6_attempted": False,
        "phase6_enabled": False,
        "phase6_status": "removed",
        "phase6_elapsed_sec": 0.0,
        "phase6_interaction_id": None,
        "phase6_total_tokens": 0,
        "phase6_input_tokens": 0,
        "phase6_output_tokens": 0,
        "phase6_raw_tariffs_returned": 0,
        "phase6_accepted_tariffs": 0,
        "phase6_error": (
            "Phase 6 Gemini Deep Research removed; pipeline is Anthropic-only "
            "(HAIKU_MODEL / SONNET_MODEL / OPUS_MODEL)"
        ),
    }
    log.info("  Phase 6: skipped (Gemini Deep Research removed)")
    return [], stats


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
    llm_cost.reset()  # start a fresh per-utility cost accumulation window
    _reset_opus_budget()  # reset the per-utility Opus escalation cap
    _RUN_DOC_CONTEXT.clear()  # R19: per-run document vintages
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

    # Phase 1 — check for manual override first, then inject known
    # regulatory tariff-book URLs (e.g. NS Power May 2026 book) when the
    # existing monitoring/seed URL is a marketing hub or older book year.
    rate_page_url_override = info.get("rate_page_url_override")
    preferred_primary, preferred_alts = resolve_preferred_rate_page(
        utility_name,
        existing_url=existing_rate_url or "",
        override_url=rate_page_url_override or "",
    )
    rate_page_url = preferred_primary or existing_rate_url or rate_page_url_override or ""
    alt_urls: list[str] = list(preferred_alts)
    if rate_page_url_override:
        log.info(f"  Using manual rate page override: {rate_page_url_override}")
    elif preferred_primary and preferred_primary != (existing_rate_url or ""):
        log.info(
            f"  Using preferred regulatory tariff source: {preferred_primary[:90]}"
        )
    search_ran = False
    if not skip_search and not rate_page_url:
        search_ran = True
        try:
            rate_page_url, num_results, search_alts = phase1_find_rate_page(
                utility_name, state, website_url
            )
            # Keep preferred alts ahead of search alts.
            for u in search_alts:
                if u and u not in alt_urls and u != rate_page_url:
                    alt_urls.append(u)
            result.phase1_rate_page_url = rate_page_url
            result.phase1_search_results = num_results
        except Exception as e:
            result.errors.append(f"Phase 1 error: {e}")
            log.error(f"  Phase 1 failed: {e}")
            return result
    else:
        result.phase1_rate_page_url = rate_page_url

    source_ctx = _source_context(info)
    rate_page_url, alt_urls = prefer_official_targets(
        rate_page_url,
        alt_urls,
        _known_rate_urls(info),
        source_ctx,
        locked=bool(rate_page_url_override or preferred_primary),
    )
    # R13: PGE schedule index lists current Sched_007 — prefer it over an
    # older combined all_tariffs_*.pdf that Phase 1 site-search returns.
    if not rate_page_url_override and _utility_looks_like_pge(
        utility_name, website_url or ""
    ):
        try:
            pge_primary, pge_alts = resolve_pge_primary_rate_url(
                utility_name,
                rate_page_url or "",
                website_url=website_url or "",
            )
            if pge_primary and pge_primary != rate_page_url:
                if rate_page_url and rate_page_url not in pge_alts:
                    pge_alts = [rate_page_url, *pge_alts]
                rate_page_url = pge_primary
                for u in pge_alts:
                    if u and u not in alt_urls and u != rate_page_url:
                        alt_urls.append(u)
        except Exception as e:
            log.warning(f"  PGE Sched_007 preference failed: {e}")
    result.phase1_rate_page_url = rate_page_url or result.phase1_rate_page_url

    if not rate_page_url:
        log.warning("  No rate page found — trying AI-guided navigation")
        if website_url:
            smart_tariffs, smart_pages, _ = _phase5_smart_retry(
                utility_name, state, website_url, []
            )
            if smart_tariffs:
                tariffs = smart_tariffs
                pages = smart_pages or []
                # Skip to Phase 4 validation (with bounded rider-doc enrich)
                tariffs, pages = enrich_tariffs_with_referenced_rider_docs(
                    tariffs, utility_name, state, website_url, pages,
                )
                validation, valid_tariffs = phase4_validate(tariffs, utility_name, state)
                result.phase4_validation = validation
                if valid_tariffs and not dry_run:
                    store_tariffs(
                        utility_id, valid_tariffs, dry_run,
                        source_hashes=_page_document_hashes(smart_pages),
                    )
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
                tariffs, pages = enrich_tariffs_with_referenced_rider_docs(
                    tariffs, utility_name, state, website_url, pages,
                )
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
        from app.services.source_type import classify_source, source_rank

        # Official alternates before unknown before third-party; within a
        # class, prefer a DIFFERENT domain than any we've already tried.
        # When our initial pick was on a wrong-utility look-alike domain (e.g.
        # lynchesriver.com when the real coop is at lreci.coop), jumping
        # straight to a fresh domain is the fastest path to success.
        for rank in sorted({source_rank(classify_source(a, source_ctx).source_type) for a in remaining_alts}):
            tier = [
                a for a in remaining_alts
                if source_rank(classify_source(a, source_ctx).source_type) == rank
            ]
            for alt in tier:
                alt_prefix = _path_prefix(alt)
                alt_dom = _alt_domain(alt)
                if alt_prefix not in tried_prefixes and alt_dom not in tried_domains:
                    log.warning(
                        f"  Retrying with different-domain URL: {alt[:70]}"
                    )
                    remaining_alts.remove(alt)
                    return alt
            # Fall back to same-domain different-section alternates.
            for alt in tier:
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

    def _search_after_dead_override(dead_url: str) -> str | None:
        """A manual override that 404s/fails used to lock the run onto
        stale known URLs (PGE: dead override -> commercial Sched 489 PDF,
        2026-10-07). Run the Phase 1 search once and try its best hit next."""
        nonlocal search_ran
        if (
            search_ran
            or skip_search
            or not rate_page_url_override
            or dead_url != rate_page_url_override
        ):
            return None
        search_ran = True
        log.warning("  Manual rate page override is unreachable — falling back to search")
        try:
            best, _n, s_alts = phase1_find_rate_page(utility_name, state, website_url)
        except Exception as e:
            log.warning(f"  Fallback search failed: {e}")
            return None
        fresh = [
            u for u in [best, *s_alts]
            if u and u != dead_url and u not in remaining_alts
            and _path_prefix(u) not in tried_prefixes
        ]
        if not fresh:
            return None
        remaining_alts[:0] = fresh[1:]
        return fresh[0]

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
            current_url = _search_after_dead_override(current_url) or _pick_next_alt()
            continue

        if not pages:
            current_url = _search_after_dead_override(current_url) or _pick_next_alt()
            continue

        # R13: if Phase 2 only got a combined book (or missed Sched_007),
        # pull the current individual residential schedule from the index
        # so deterministic Sch 7 / fresher-doc preference can run.
        try:
            pages = prefer_current_individual_schedule_pages(
                utility_name,
                pages,
                website_url=website_url or "",
            )
            result.phase2_sub_pages = [
                {"url": p.url, "title": p.title, "type": p.page_type, "has_content": bool(p.content)}
                for p in pages
            ]
        except Exception as e:
            log.warning(f"  Individual schedule preference failed: {e}")

        # Incremental check: skip LLM extraction if page content unchanged
        if (
            not force_extract
            and not dry_run
            and _check_fingerprints(utility_id, pages)
            and not _third_party_upgrade_pending(utility_id, pages, source_ctx)
        ):
            _touch_fingerprints(utility_id, pages)
            _touch_tariff_verified(utility_id)
            result.phase3_tariffs = []
            result.skipped_unchanged = True
            log.info("  Skipping Phase 3 (content unchanged) — existing tariffs still valid")
            return result

        # Phase 3
        phase3_stats: dict = {}
        try:
            tariffs = phase3_extract_tariffs(pages, utility_name, stats=phase3_stats, state=state)
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
        pdf_override = rate_page_url_override or (
            rate_page_url
            if rate_page_url.lower().split("?")[0].endswith(".pdf")
            else ""
        )
        pdf_override_failed = bool(
            pdf_override and pdf_override.lower().split("?")[0].endswith(".pdf")
        )
        if pdf_override_failed:
            # The pinned PDF came up empty (moved, scanned, image-only).
            # Phase 5 is still worth running — AI navigation of the live
            # site often finds the CURRENT rate page that replaced the
            # stale PDF. Phase 6 stays skipped for these: deep-research
            # spend on known-PDF utilities historically returned nothing
            # the override didn't already cover.
            log.warning(
                "  PDF override produced no tariffs — trying Phase 5 "
                "(Phase 6 remains skipped for PDF overrides)"
            )
        else:
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
            if _phase6_enabled() and not pdf_override_failed:
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

    # Bounded fetch of official rider/adjustment docs referenced by
    # residential extracts but missing from the Phase 2/3 page batch
    # (NSP FAM pages, PGE Schedule 1xx). Cap = MAX_RIDER_DOCS_FETCH.
    if tariffs:
        rider_stats: dict = {}
        tariffs, pages = enrich_tariffs_with_referenced_rider_docs(
            tariffs, utility_name, state, website_url, pages, stats=rider_stats,
        )
        if rider_stats:
            _merge_stats(rider_stats)

    # R19: document vintages for the older-document flag in phase 4.
    try:
        set_run_document_context(
            pages, [*alt_urls, *_known_rate_urls(info)], ctx=source_ctx,
        )
    except Exception as e:
        log.warning(f"  Document vintage context failed: {e}")
    # Phase 4 — validation now returns (report, valid_list)
    validation, valid_tariffs = phase4_validate(tariffs, utility_name, state)
    result.phase4_validation = validation

    successful_url = result.phase1_rate_page_url or rate_page_url

    if valid_tariffs and not dry_run:
        stored = store_tariffs(
            utility_id, valid_tariffs, dry_run, source_hashes=_page_document_hashes(pages),
        )
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


def _page_document_hashes(pages) -> dict[str, str]:
    """source_url → stable normalized-text hash for persisted provenance."""
    from app.services.monitor import stable_text_hash

    return {p.url: stable_text_hash(p.content) for p in (pages or []) if getattr(p, "content", "")}


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
