"""
Independent tariff data auditor using Claude Opus.

Selects a random sample of utilities, fetches their source web pages,
and asks Claude Opus to compare what's on the page against what's in
our database — acting as a reviewer, not an extractor.

Usage:
    python -m scripts.opus_audit --count 25 --output audit_report.json
    python -m scripts.opus_audit --count 10 --states TX,CA
    python -m scripts.opus_audit --utility-ids 42,99,155

Requires:
    ANTHROPIC_API_KEY  — environment variable
    SYNC_DATABASE_URL  — via app.config.settings (or DATABASE_URL)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx
from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload

from app.db.session import get_sync_engine
from app.models.tariff import (
    ComponentType,
    CustomerClass,
    RateComponent,
    RateType,
    Tariff,
)
from app.models.utility import Utility

from scripts.tariff_pipeline import (
    _fetch_and_parse as pipeline_fetch_and_parse,
    fetch_pdf_text as pipeline_fetch_pdf_text,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("opus_audit")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
OPUS_MODEL = "claude-opus-4-20250514"

MAX_PAGE_CHARS = 25_000
MAX_PAGES_PER_UTILITY = 5

_PDF_URL_RE = re.compile(r"\.pdf(\?.*)?$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Page fetching — delegates to the tariff pipeline's robust fetchers which
# handle httpx → Playwright fallback for HTML, and pdfplumber → OCR for PDFs.
# ---------------------------------------------------------------------------

def _is_pdf_url(url: str) -> bool:
    """Heuristic: does the URL look like a PDF?"""
    return bool(_PDF_URL_RE.search(urlparse(url).path))


def fetch_page_text(url: str) -> tuple[str, str]:
    """Fetch a URL and return (extracted_text, page_title).

    Dispatches to the pipeline's PDF extractor for PDF URLs, and the
    pipeline's HTML fetcher (with Playwright fallback) for everything else.
    Returns ("", "") on failure.
    """
    if _is_pdf_url(url):
        log.info("    -> PDF detected, using pdfplumber/OCR pipeline")
        text = pipeline_fetch_pdf_text(url)
        if text and len(text.strip()) > 100:
            return text[:MAX_PAGE_CHARS], "(PDF document)"
        log.warning("    PDF extraction returned thin/no content")
        return "", ""

    page = pipeline_fetch_and_parse(url)
    if page and page.content and len(page.content.strip()) > 100:
        return page.content[:MAX_PAGE_CHARS], page.title
    return "", ""


# ---------------------------------------------------------------------------
# DB data formatting
# ---------------------------------------------------------------------------

def _format_component(rc: RateComponent) -> str:
    parts = [f"{rc.component_type.value} | {rc.unit} | ${float(rc.rate_value):.6f}"]
    if rc.tier_label:
        parts.append(f"tier: {rc.tier_label}")
    if rc.tier_min_kwh is not None or rc.tier_max_kwh is not None:
        low = rc.tier_min_kwh if rc.tier_min_kwh is not None else 0
        high = rc.tier_max_kwh if rc.tier_max_kwh is not None else "∞"
        parts.append(f"range: {low}-{high} kWh")
    if rc.period_label:
        parts.append(f"period: {rc.period_label}")
    if rc.season:
        parts.append(f"season: {rc.season}")
    return " | ".join(parts)


def format_db_tariffs(tariffs: list[Tariff]) -> str:
    """Format all tariffs for a utility into a readable text block for the prompt."""
    if not tariffs:
        return "(No tariffs in database)"

    lines = []
    for i, t in enumerate(tariffs, 1):
        lines.append(f"--- DB Tariff #{i} ---")
        lines.append(f"  Name: {t.name}")
        if t.code:
            lines.append(f"  Code: {t.code}")
        lines.append(f"  Customer Class: {t.customer_class.value}")
        lines.append(f"  Rate Type: {t.rate_type.value}")
        lines.append(f"  Is Default: {t.is_default}")
        if t.description:
            lines.append(f"  Description: {t.description}")
        if t.effective_date:
            lines.append(f"  Effective Date: {t.effective_date}")
        if t.source_url:
            lines.append(f"  Source URL: {t.source_url}")
        if t.rate_components:
            lines.append(f"  Components ({len(t.rate_components)}):")
            for rc in sorted(t.rate_components, key=lambda c: (c.component_type.value, c.season or "", c.period_label or "", c.tier_min_kwh or 0)):
                lines.append(f"    - {_format_component(rc)}")
        else:
            lines.append("  Components: NONE")
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Opus audit prompt
# ---------------------------------------------------------------------------

AUDIT_PROMPT = """You are an independent auditor reviewing electricity tariff data for accuracy.

A tariff extraction system used an AI model to read utility rate pages and store structured tariff data in a database. Your job is to compare the database entries against the actual source web pages and identify any discrepancies.

## Utility Under Audit

Name: {utility_name}
State/Province: {state}
Country: {country}
Website: {website_url}

## What's Currently in the Database

{db_tariffs}

## Source Web Page Content

{page_content}

## Your Audit Task

Compare the database entries against the source page content. Evaluate:

1. **Existence**: Does each database tariff actually appear on the source page?
2. **Rate Accuracy**: Are the $/kWh, $/kW, $/month values in the database correct? Check EVERY rate component against the source page. Even small differences matter.
3. **Classification**: Is the customer_class (residential/commercial) correct? Is the rate_type (flat/tiered/tou/etc.) correct?
4. **Completeness**: Are ALL tiers, TOU periods, and seasonal variations captured? Are any rate components missing?
5. **Currency**: Does the effective_date match what's on the page? Are the rates current or stale?
6. **Missing Tariffs**: Are there residential or small commercial tariffs on the source page that are NOT in the database?
7. **Phantom Tariffs**: Are there tariffs in the database that DON'T appear anywhere on the source page?

## Grading

Assign an overall letter grade:
- A: All tariffs correct, complete, and current. No issues.
- B: Minor issues only (e.g., slightly stale effective date, missing optional description). Core rate values are correct.
- C: Some rate values are wrong OR one tariff is missing/phantom. Needs attention.
- D: Multiple rate errors or missing tariffs. Data is unreliable.
- F: Fundamentally wrong — wrong utility, most rates incorrect, or critical tariffs missing.

If the source page content is empty or doesn't contain rate information (the fetch may have failed), grade as "N/A" and note that the source page was unavailable.

## Response Format

Return ONLY a valid JSON object (no markdown, no explanation outside the JSON):

{{
  "overall_grade": "A",
  "tariffs_verified_correct": 2,
  "tariffs_with_issues": 1,
  "missing_from_db": 0,
  "phantom_in_db": 0,
  "issues": [
    {{
      "severity": "high",
      "type": "wrong_rate",
      "tariff_name": "Schedule E-1",
      "field": "energy rate tier 2",
      "db_value": "0.1234",
      "correct_value": "0.1456",
      "explanation": "Tier 2 rate on the page is $0.1456/kWh but database has $0.1234/kWh"
    }}
  ],
  "missing_tariffs": [
    {{
      "name": "Schedule E-TOU-D",
      "customer_class": "residential",
      "explanation": "Time-of-use residential plan appears on the page but is not in the database"
    }}
  ],
  "notes": "Overall good coverage. One rate is stale from a Jan 2025 update."
}}

Issue types: wrong_rate, wrong_classification, wrong_rate_type, missing_component, extra_component, stale_date, phantom_tariff, other
Severity levels: high (wrong dollar amounts, missing tariffs), medium (wrong classification, missing non-core components), low (stale dates, minor description issues)
"""


# ---------------------------------------------------------------------------
# Opus API call
# ---------------------------------------------------------------------------

def call_opus(prompt: str) -> dict | None:
    """Call Claude Opus and parse the JSON response."""
    if not ANTHROPIC_API_KEY:
        log.error("ANTHROPIC_API_KEY not set")
        return None

    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    body = {
        "model": OPUS_MODEL,
        "max_tokens": 4096,
        "messages": [{"role": "user", "content": prompt}],
    }

    try:
        resp = httpx.post(
            "https://api.anthropic.com/v1/messages",
            headers=headers,
            json=body,
            timeout=httpx.Timeout(120.0, connect=15.0),
        )
        if resp.status_code != 200:
            log.error("  Opus API error %d: %s", resp.status_code, resp.text[:500])
            return None

        data = resp.json()
        raw_text = data["content"][0]["text"]

        json_match = re.search(r"\{[\s\S]*\}", raw_text)
        if not json_match:
            log.error("  No JSON object found in Opus response")
            return None
        return json.loads(json_match.group())

    except json.JSONDecodeError as e:
        log.error("  Failed to parse Opus JSON: %s", e)
        return None
    except Exception as e:
        log.error("  Opus API call failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Single-utility audit
# ---------------------------------------------------------------------------

def audit_utility(session: Session, utility: Utility) -> dict:
    """Run the full audit for one utility. Returns the result dict."""
    log.info("Auditing: [%s] %s (id=%d)", utility.state_province, utility.name, utility.id)

    tariffs = (
        session.query(Tariff)
        .options(joinedload(Tariff.rate_components))
        .filter(Tariff.utility_id == utility.id)
        .all()
    )

    source_urls: list[str] = []
    seen_urls: set[str] = set()
    for t in tariffs:
        if t.source_url and t.source_url not in seen_urls:
            source_urls.append(t.source_url)
            seen_urls.add(t.source_url)
    if utility.website_url and utility.website_url not in seen_urls:
        source_urls.append(utility.website_url)

    source_urls = source_urls[:MAX_PAGES_PER_UTILITY]

    page_blocks = []
    urls_fetched = []
    for url in source_urls:
        log.info("  Fetching: %s", url[:100])
        text, title = fetch_page_text(url)
        if text and len(text.strip()) > 100:
            urls_fetched.append(url)
            page_blocks.append(f"--- Page: {url} ---\nTitle: {title}\n\n{text}\n")
        else:
            log.warning("  Skipped (no/thin content): %s", url[:80])

    if not page_blocks:
        log.warning("  No source page content retrieved for %s", utility.name)
        return {
            "utility_id": utility.id,
            "utility_name": utility.name,
            "state": utility.state_province,
            "source_urls_checked": source_urls,
            "overall_grade": "N/A",
            "tariffs_in_db": len(tariffs),
            "tariffs_verified_correct": 0,
            "tariffs_with_issues": 0,
            "missing_from_db": 0,
            "phantom_in_db": 0,
            "issues": [],
            "missing_tariffs": [],
            "notes": "Could not fetch any source pages for this utility.",
        }

    page_content = "\n\n".join(page_blocks)
    if len(page_content) > MAX_PAGE_CHARS * MAX_PAGES_PER_UTILITY:
        page_content = page_content[: MAX_PAGE_CHARS * MAX_PAGES_PER_UTILITY]

    db_text = format_db_tariffs(tariffs)

    prompt = AUDIT_PROMPT.format(
        utility_name=utility.name,
        state=utility.state_province,
        country=utility.country.value if utility.country else "US",
        website_url=utility.website_url or "(none)",
        db_tariffs=db_text,
        page_content=page_content,
    )

    log.info("  Calling Opus (prompt ~%d chars)...", len(prompt))
    start = time.time()
    result = call_opus(prompt)
    elapsed = time.time() - start
    log.info("  Opus responded in %.1fs", elapsed)

    if result is None:
        return {
            "utility_id": utility.id,
            "utility_name": utility.name,
            "state": utility.state_province,
            "source_urls_checked": urls_fetched,
            "overall_grade": "ERROR",
            "tariffs_in_db": len(tariffs),
            "tariffs_verified_correct": 0,
            "tariffs_with_issues": 0,
            "missing_from_db": 0,
            "phantom_in_db": 0,
            "issues": [],
            "missing_tariffs": [],
            "notes": "Opus API call failed.",
        }

    result["utility_id"] = utility.id
    result["utility_name"] = utility.name
    result["state"] = utility.state_province
    result["source_urls_checked"] = urls_fetched
    result["tariffs_in_db"] = len(tariffs)
    return result


# ---------------------------------------------------------------------------
# Sampling and orchestration
# ---------------------------------------------------------------------------

def select_utilities(
    session: Session,
    count: int,
    states: list[str] | None = None,
    utility_ids: list[int] | None = None,
) -> list[Utility]:
    """Select utilities to audit."""
    if utility_ids:
        return (
            session.query(Utility)
            .filter(Utility.id.in_(utility_ids), Utility.is_active.is_(True))
            .all()
        )

    q = (
        session.query(Utility)
        .filter(
            Utility.is_active.is_(True),
            Utility.id.in_(
                session.query(Tariff.utility_id).group_by(Tariff.utility_id)
            ),
        )
    )
    if states:
        q = q.filter(Utility.state_province.in_(states))

    q = q.order_by(func.random()).limit(count)
    return q.all()


def generate_summary(results: list[dict]) -> dict:
    """Aggregate individual results into a summary."""
    total = len(results)
    grades = {}
    severity_counts = {"high": 0, "medium": 0, "low": 0}
    type_counts: dict[str, int] = {}
    total_issues = 0
    total_missing = 0
    total_phantom = 0
    perfect = 0
    needs_attention = []

    for r in results:
        grade = r.get("overall_grade", "ERROR")
        grades[grade] = grades.get(grade, 0) + 1

        issues = r.get("issues", [])
        total_issues += len(issues)
        for iss in issues:
            sev = iss.get("severity", "medium")
            severity_counts[sev] = severity_counts.get(sev, 0) + 1
            itype = iss.get("type", "other")
            type_counts[itype] = type_counts.get(itype, 0) + 1

        missing = r.get("missing_from_db", 0)
        phantom = r.get("phantom_in_db", 0)
        if isinstance(missing, list):
            missing = len(missing)
        if isinstance(phantom, list):
            phantom = len(phantom)
        total_missing += missing
        total_phantom += phantom

        missing_tariffs_list = r.get("missing_tariffs", [])
        if isinstance(missing_tariffs_list, list) and len(missing_tariffs_list) > missing:
            total_missing += len(missing_tariffs_list) - missing

        if grade == "A":
            perfect += 1
        elif grade in ("C", "D", "F"):
            needs_attention.append({
                "utility_id": r["utility_id"],
                "utility_name": r["utility_name"],
                "state": r["state"],
                "grade": grade,
                "issue_count": len(issues),
            })

    return {
        "total_audited": total,
        "grade_distribution": dict(sorted(grades.items())),
        "perfect_score_count": perfect,
        "accuracy_rate": f"{perfect / total * 100:.1f}%" if total else "N/A",
        "total_issues_found": total_issues,
        "issues_by_severity": severity_counts,
        "issues_by_type": dict(sorted(type_counts.items(), key=lambda x: -x[1])),
        "total_missing_tariffs": total_missing,
        "total_phantom_tariffs": total_phantom,
        "utilities_needing_attention": needs_attention,
    }


def print_summary(summary: dict, results: list[dict]):
    """Print a readable summary to console."""
    print("\n" + "=" * 70)
    print("  OPUS TARIFF AUDIT REPORT")
    print("=" * 70)
    print(f"\n  Utilities audited:   {summary['total_audited']}")
    print(f"  Perfect score (A):   {summary['perfect_score_count']}")
    print(f"  Accuracy rate:       {summary['accuracy_rate']}")
    print(f"\n  Grade distribution:")
    for grade, count in summary["grade_distribution"].items():
        bar = "█" * count
        print(f"    {grade:>5s}: {count:>3d}  {bar}")

    print(f"\n  Total issues found:  {summary['total_issues_found']}")
    if summary["issues_by_severity"]:
        print(f"    High:   {summary['issues_by_severity'].get('high', 0)}")
        print(f"    Medium: {summary['issues_by_severity'].get('medium', 0)}")
        print(f"    Low:    {summary['issues_by_severity'].get('low', 0)}")

    if summary["issues_by_type"]:
        print(f"\n  Issue types:")
        for itype, count in summary["issues_by_type"].items():
            print(f"    {itype:<25s} {count:>3d}")

    if summary["total_missing_tariffs"]:
        print(f"\n  Missing tariffs (on page but not in DB): {summary['total_missing_tariffs']}")
    if summary["total_phantom_tariffs"]:
        print(f"  Phantom tariffs (in DB but not on page): {summary['total_phantom_tariffs']}")

    if summary["utilities_needing_attention"]:
        print(f"\n  Utilities needing attention:")
        for u in summary["utilities_needing_attention"]:
            print(f"    [{u['state']}] {u['utility_name'][:45]:<45s} grade={u['grade']}  issues={u['issue_count']}")

    print("\n  Per-utility results:")
    print(f"  {'State':<6s} {'Utility':<42s} {'Grade':>5s} {'DB#':>4s} {'OK':>4s} {'Iss':>4s} {'Miss':>5s}")
    print("  " + "-" * 68)
    for r in sorted(results, key=lambda x: (x.get("overall_grade", "Z"), x["state"])):
        grade = r.get("overall_grade", "?")
        ok = r.get("tariffs_verified_correct", 0)
        iss = r.get("tariffs_with_issues", 0)
        miss = r.get("missing_from_db", 0)
        if isinstance(miss, list):
            miss = len(miss)
        print(f"  {r['state']:<6s} {r['utility_name'][:40]:<42s} {grade:>5s} {r['tariffs_in_db']:>4d} {ok:>4d} {iss:>4d} {miss:>5d}")

    print("\n" + "=" * 70)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Independent tariff audit using Claude Opus")
    parser.add_argument("--count", type=int, default=25, help="Number of utilities to audit (default: 25)")
    parser.add_argument("--states", type=str, default=None, help="Comma-separated state codes to filter (e.g. TX,CA)")
    parser.add_argument("--utility-ids", type=str, default=None, help="Comma-separated utility IDs to audit")
    parser.add_argument("--output", type=str, default=None, help="Output JSON file path (default: audit_YYYYMMDD_HHMMSS.json)")
    args = parser.parse_args()

    if not ANTHROPIC_API_KEY:
        print("ERROR: ANTHROPIC_API_KEY environment variable not set", file=sys.stderr)
        sys.exit(1)

    states = [s.strip().upper() for s in args.states.split(",")] if args.states else None
    utility_ids = [int(x.strip()) for x in args.utility_ids.split(",")] if args.utility_ids else None

    output_path = args.output or f"audit_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"

    engine = get_sync_engine()
    with Session(engine) as session:
        utilities = select_utilities(session, args.count, states, utility_ids)
        log.info("Selected %d utilities for audit", len(utilities))

        if not utilities:
            print("No utilities matched the criteria.")
            sys.exit(0)

        results = []
        for i, util in enumerate(utilities, 1):
            log.info("[%d/%d] Starting audit...", i, len(utilities))
            result = audit_utility(session, util)
            results.append(result)

            grade = result.get("overall_grade", "?")
            issues = result.get("issues", [])
            log.info(
                "[%d/%d] %s — grade=%s, issues=%d",
                i, len(utilities), util.name[:40], grade, len(issues),
            )

    summary = generate_summary(results)
    report = {
        "audit_metadata": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "model": OPUS_MODEL,
            "utilities_requested": args.count,
            "utilities_audited": len(results),
            "state_filter": states,
            "utility_id_filter": utility_ids,
        },
        "summary": summary,
        "results": results,
    }

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    log.info("Report written to %s", output_path)

    print_summary(summary, results)


if __name__ == "__main__":
    main()
