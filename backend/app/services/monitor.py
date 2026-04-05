"""
URL monitoring service for detecting changes to utility tariff pages.

Fetches URLs, extracts text content, computes content hashes, and
detects changes for flagging human review.
"""

import asyncio
import hashlib
import re
from datetime import datetime, timezone

import httpx
from bs4 import BeautifulSoup

try:
    import pdfplumber
except ImportError:
    pdfplumber = None


async def fetch_and_hash_url(url: str, *, timeout: int = 12) -> dict:
    """Fetch a URL, extract meaningful text content, and return a content hash.

    Returns dict with keys: content_hash, content_preview, error
    """
    try:
        t = httpx.Timeout(timeout, connect=6.0)
        async with httpx.AsyncClient(
            timeout=t,
            follow_redirects=True,
            max_redirects=5,
            headers={"User-Agent": "UtilityTariffMonitor/0.1"},
        ) as client:
            resp = await asyncio.wait_for(client.get(url), timeout=timeout)
            resp.raise_for_status()

        content_type = resp.headers.get("content-type", "")

        if "pdf" in content_type or url.lower().endswith(".pdf"):
            text = _extract_pdf_text(resp.content)
        else:
            text = _extract_html_text(resp.text)

        text = _normalize_text(text)
        content_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()

        return {
            "content_hash": content_hash,
            "content_preview": text[:500],
            "error": None,
        }
    except Exception as e:
        return {
            "content_hash": None,
            "content_preview": None,
            "error": str(e),
        }


def _extract_html_text(html: str) -> str:
    """Extract meaningful text from HTML, stripping nav/footer/scripts."""
    soup = BeautifulSoup(html, "lxml")

    for tag in soup(["script", "style", "nav", "footer", "header", "noscript", "iframe"]):
        tag.decompose()

    main = soup.find("main") or soup.find("article") or soup.find("div", {"role": "main"})
    if main:
        text = main.get_text(separator="\n", strip=True)
    else:
        text = soup.get_text(separator="\n", strip=True)

    return text


def _extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes."""
    if pdfplumber is None:
        return hashlib.sha256(content).hexdigest()

    import io
    text_parts = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)

    return "\n".join(text_parts)


def _normalize_text(text: str) -> str:
    """Normalize text to reduce false-positive change detection from
    whitespace or timestamp differences."""
    text = re.sub(r'\s+', ' ', text)
    text = text.strip().lower()
    text = re.sub(r'\b\d{1,2}/\d{1,2}/\d{2,4}\b', '', text)
    text = re.sub(r'\b\d{4}-\d{2}-\d{2}\b', '', text)
    return text


def compute_diff_summary(old_preview: str | None, new_preview: str | None) -> str:
    """Generate a simple diff summary between old and new content previews."""
    if not old_preview:
        return "Initial check - no previous content to compare"
    if not new_preview:
        return "Content became unavailable"

    old_words = set(old_preview.split())
    new_words = set(new_preview.split())

    added = new_words - old_words
    removed = old_words - new_words

    parts = []
    if added:
        sample = list(added)[:10]
        parts.append(f"New terms: {', '.join(sample)}")
    if removed:
        sample = list(removed)[:10]
        parts.append(f"Removed terms: {', '.join(sample)}")

    return "; ".join(parts) if parts else "Content changed (details in full diff)"
