# Utility Tariff Finder — Comprehensive Project Summary

*Last updated: 2026-03-31*

This document provides a complete summary of the Utility Tariff Finder project — what it does, how it works, every major decision made, every problem encountered, every fix applied, the current optimization state, and what's next. It is intended for external reviewers who need full context.

---

## Table of Contents

1. [The Problem](#1-the-problem)
2. [What We Built](#2-what-we-built)
3. [High-Level Architecture](#3-high-level-architecture)
4. [The Tariff Pipeline — How It Works](#4-the-tariff-pipeline--how-it-works)
5. [Optimization Phases Implemented](#5-optimization-phases-implemented)
6. [Monitoring & Maintenance System](#6-monitoring--maintenance-system)
7. [Independent Audit System (Opus Auditor)](#7-independent-audit-system-opus-auditor)
8. [Database State](#8-database-state)
9. [Bugs, Issues & Fixes — The Full History](#9-bugs-issues--fixes--the-full-history)
10. [Audit Results & Accuracy Over Time](#10-audit-results--accuracy-over-time)
11. [Deployment & Operations](#11-deployment--operations)
12. [Cost Profile](#12-cost-profile)
13. [Known Limitations & Open Problems](#13-known-limitations--open-problems)
14. [Key Decisions & Trade-offs](#14-key-decisions--trade-offs)
15. [File Map — Where Everything Lives](#15-file-map--where-everything-lives)

---

## 1. The Problem

### Goal

Build a system that automatically discovers, extracts, and stores structured electricity tariff data for ~1,800 US and Canadian utilities. The data powers a consumer-facing application where users enter an address and see their electricity rate plans.

### Why This Is Hard

There is **no standardized API or central database** for US/Canadian utility tariffs. Each of the ~1,800 utilities publishes rates differently:

- Some have clean HTML pages with rate tables
- Some publish dense multi-hundred-page PDFs
- Some have JavaScript-heavy single-page applications
- Some bury rates in regulatory filings
- Some are barely online at all
- Rate structures vary wildly: flat rates, tiered rates, time-of-use, seasonal, demand charges, riders, surcharges

The system must find the right page, crawl it, extract structured data from unstructured content, validate it, and keep it current — at scale, affordably, and accurately.

### Scope

- **Utilities**: 1,820 total (1,611 active) sourced from EIA Form 861 (US) and provincial data (Canada)
- **Tariff types**: Residential and commercial only (lighting/industrial excluded)
- **Data per tariff**: Plan name, customer class, rate type, effective date, source URL, and structured rate components (energy $/kWh, fixed $/month, demand $/kW, with tiers/seasons/TOU periods)

---

## 2. What We Built

### Tech Stack

| Layer | Technology |
|-------|-----------|
| **Backend** | Python 3.12, FastAPI, SQLAlchemy 2.0, Alembic |
| **Frontend** | React 18, TypeScript, Vite |
| **Database** | PostgreSQL 16 with PostGIS |
| **Task Queue** | Celery + Redis |
| **Web Server** | Caddy (auto-HTTPS via Let's Encrypt) |
| **Containers** | Docker Compose (6 services) |
| **Web Scraping** | httpx, BeautifulSoup, Playwright (headless Chromium) |
| **PDF Extraction** | pdfplumber, pdf2image + pytesseract (OCR), Claude Vision |
| **LLM Extraction** | Google Gemini 2.0 Flash (cheap/fast) + Anthropic Claude Haiku 4.5 (complex/fallback) |
| **LLM Auditing** | Anthropic Claude Opus (independent verification) |
| **Web Search** | Brave Search API, Google Custom Search (fallback) |
| **Auth** | Google OAuth 2.0 |
| **Infrastructure** | Google Cloud VM (e2-standard-2, us-central1-a) |

### Data Sources

| Source | Coverage | Use |
|--------|----------|-----|
| EIA Form 861 | ~3,300 US utilities | Utility master list |
| OpenEI URDB | 62,600+ rate records | Tariff seed data |
| HIFLD | US utility territory polygons | Address-to-utility mapping (PostGIS) |
| Provincial boards (OEB, etc.) | Canadian utilities | Manual curation |

---

## 3. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Google Cloud VM                             │
│                   (e2-standard-2, us-central1-a)                │
│                                                                  │
│  ┌──────────┐  ┌──────────┐  ┌──────────────┐  ┌──────────┐   │
│  │  Caddy   │  │ FastAPI  │  │   Celery      │  │  Celery  │   │
│  │  (HTTPS) │──│   API    │  │   Worker      │  │   Beat   │   │
│  │  :443    │  │  :8000   │  │ (concur=8)    │  │(scheduler│   │
│  └──────────┘  └────┬─────┘  └──────┬────────┘  └──────────┘   │
│                     │               │                            │
│               ┌─────┴─────┐  ┌──────┴─────┐                     │
│               │ PostgreSQL│  │   Redis     │                     │
│               │ (PostGIS) │  │   :6379     │                     │
│               │   :5432   │  │             │                     │
│               └───────────┘  └─────────────┘                     │
└─────────────────────────────────────────────────────────────────┘
        │                        │                    │
        │ Brave Search API       │ Anthropic API      │ Google AI API
        │ (web search)           │ (Claude Haiku/     │ (Gemini Flash
        │                        │  Claude Opus)      │  extraction)
        ▼                        ▼                    ▼
   ┌──────────┐          ┌──────────────┐    ┌──────────────┐
   │  Brave   │          │  Anthropic   │    │  Google AI   │
   │  Search  │          │  Claude API  │    │  Gemini API  │
   └──────────┘          └──────────────┘    └──────────────┘
```

### Docker Services

| Service | Memory Limit | Purpose |
|---------|-------------|---------|
| `db` | 1 GB | PostgreSQL with PostGIS |
| `redis` | 256 MB | Celery broker + result backend |
| `api` | 4 GB | FastAPI + pipeline scripts + Playwright |
| `celery-worker` | 4 GB | Background task execution (8 concurrent) |
| `celery-beat` | 256 MB | Periodic task scheduler |
| `web` | 128 MB | Caddy reverse proxy + static frontend |

---

## 4. The Tariff Pipeline — How It Works

The core pipeline (`backend/scripts/tariff_pipeline.py`, ~3,300 lines) processes one utility at a time through four phases:

```
run_pipeline(utility_id, utility_name, state, country, website_url, ...)
    │
    ├── Fingerprint Check (incremental)
    │       └── If page content unchanged since last run → skip LLM, touch timestamps, done
    │
    ├── Phase 1: Find rate page (Brave Search + Google CSE fallback)
    │       └── score_search_result() → ranked URL candidates
    │
    ├── Phase 2: Discover tariff sub-pages (crawl + PDF detection)
    │       └── Returns list[RatePage] (HTML pages + extracted PDFs)
    │
    ├── Phase 3: LLM extraction (Gemini Flash → Haiku fallback)
    │       └── Returns list[ExtractedTariff] with components
    │
    ├── verify_content_identity() → cross-contamination check
    │
    ├── Phase 4: Validate
    │       └── Checks classes, types, rate bounds, components
    │
    └── store_tariffs() → upsert to DB + reconcile stale tariffs + store fingerprints
```

### Phase 1: Find Rate Page

Given a utility name and state, find the URL of their rates/tariffs page.

1. If no known website URL, discover the utility's domain via Brave Search
2. Search for rate pages: `"{name} residential electric rates {state}"`
3. Score each result using `score_search_result()`:
   - **+50** if URL domain matches utility's known domain
   - **+15** each for "rate", "pricing", "electric" in URL path
   - **+10** for "residential" in path
   - **+25** if title/description mentions ≥2 utility name words
   - **-40** if URL domain is in `THIRD_PARTY_DOMAINS` blocklist (40+ domains)
   - **-30** if URL is a PDF or homepage-only path
4. If top result isn't from the utility's domain, try direct URL patterns (`/rates`, `/electric-rates`, etc.)
5. If still no match, try a site-scoped search (`site:{domain} residential rates`)
6. If Brave fails entirely, fall back to Google Custom Search API

### Phase 2: Discover Tariff Pages

Starting from the rate page URL, crawl to find all sub-pages and PDFs containing rate data.

1. Fetch main rate page (httpx first, Playwright fallback for JS-rendered sites)
2. Extract all links, filter for rate-relevant URLs using regex patterns
3. Follow up to 10 relevant links
4. For each page:
   - **HTML**: fetch and extract text via BeautifulSoup, compress whitespace for token efficiency
   - **PDF**: download bytes → pdfplumber text extraction → OCR fallback (pdf2image + pytesseract) if < 200 chars → Claude Vision fallback for scanned/image-heavy PDFs
5. Per-domain throttling (1 req/sec minimum) prevents rate limiting

**Playwright management**: A singleton `_PlaywrightManager` reuses one Chromium browser instance, auto-recycling after 25 contexts to prevent OOM. Thread-safe domain tracking remembers which domains require JavaScript rendering.

### Phase 3: LLM Extraction

Extract structured tariff data from page content using a **tiered model strategy**:

1. **Model selection** (`_select_model`):
   - Complex pages (many TOU periods, seasonal variations) → Claude Haiku 4.5
   - PDF content → Claude Haiku 4.5
   - Simple HTML pages → Gemini 2.0 Flash (cheaper)
   - If Gemini circuit breaker is open → Claude Haiku 4.5
2. **LLM cache check**: Before calling any LLM, check the file-based cache keyed on `content_hash + model + prompt_version`
3. **Extraction with fallback**: If Gemini returns 0 tariffs, automatically escalate to Haiku
4. **Claude tool use**: Haiku uses Anthropic's structured tool-use API with a JSON schema, prompt caching for the static system prompt (saves ~50% on input tokens for repeated calls)
5. **Circuit breaker**: After 3 consecutive Gemini failures, stop trying Gemini for the rest of the run and go straight to Haiku

Output: list of tariff dicts with name, code, customer_class, rate_type, components, effective_date, confidence score.

### Content Identity Verification

After extraction, `verify_content_identity()` checks that the extracted data actually belongs to the target utility:

1. Tokenize utility name into significant words
2. Check if those words appear in the page content
3. If 0 name words match and state not mentioned → **reject all tariffs**
4. Prevents cross-contamination from wrong utility pages

### Phase 4: Validation

Rule-based validation catches obviously bad data:
- Tariff must have a name (not "Unknown")
- `customer_class` must be "residential" or "commercial"
- Must have at least one core component (energy, fixed, or demand)
- Rate value bounds: energy ≤ $2.00/kWh, fixed ≤ $500/month, demand ≤ $100/kW
- Statistical validation against state-level percentiles

### Store + Reconciliation

- **Upsert**: Match on `(utility_id, name, customer_class)` — update existing or create new
- **Reconciliation**: After storing, delete any existing tariff for this utility whose `(name, customer_class)` is NOT in the current extraction AND whose source domain matches a domain we fetched from
- **Wipe protection**: If no source domains in the current extraction, skip reconciliation entirely (prevents accidental data loss)

---

## 5. Optimization Phases Implemented

We went through a structured 6-phase optimization process to improve accuracy, speed, and cost.

### Phase 1: Critical Bug Fixes

| Fix | What | Why |
|-----|------|-----|
| **1A** Reconciliation wipe protection | Skip reconciliation when `fetched_domains` is empty | Prevented accidental deletion of all tariffs when pipeline fetches fail |
| **1B** Fix success/error semantics | Changed success to `valid_count > 0` | Pipeline was reporting success even when it stored 0 valid tariffs |
| **1C** Fix benchmark match counting | Only count matches when `_rate_close` is true | Benchmark was over-counting precision/recall |
| **1D** Fix Opus audit aggregation | Correctly aggregate `total_missing_tariffs` | Audit summary was under-counting missing tariffs |
| **1E** PDF vision fallback gap | Check `accepted > 0` after PDF vision | Text extraction was being skipped even when vision returned nothing |
| **1F** Thread-safe `_js_rendered_domains` | Added threading lock | Race condition when multiple threads marked domains as JS-required |

### Phase 2: Incremental Scraping (Content Fingerprinting)

Added a `rate_page_fingerprints` table that stores content hashes, ETags, Last-Modified headers, and content length for each URL per utility. On subsequent runs:

1. Crawl pages and compute fingerprints
2. Compare against stored fingerprints
3. If ALL pages unchanged → skip expensive LLM extraction, just touch timestamps
4. If any page changed → run full extraction

This dramatically reduces costs for monthly refreshes where most pages haven't changed.

### Phase 3: Tiered Model Strategy (Gemini + Haiku)

Introduced Google Gemini 2.0 Flash as the default model for simple pages:

- **Gemini Flash**: ~10x cheaper than Haiku, good for straightforward HTML rate tables
- **Haiku 4.5**: Used for complex pages (TOU, seasonal), PDFs, and as fallback when Gemini fails
- **Circuit breaker**: After 3 consecutive Gemini failures, automatically switches to Haiku for the rest of the run (prevents cascading timeouts)
- **30-second HTTP timeout** on Gemini client prevents indefinite hangs

### Phase 4: Caching & Token Reduction

| Optimization | Impact |
|-------------|--------|
| **Anthropic prompt caching** | Static system prompt + tool schema cached with `cache_control: ephemeral`, saving ~50% on input tokens for repeated calls |
| **LLM response cache** | File-based cache keyed on `content_hash + model + prompt_version`, avoids re-calling LLMs for identical content |
| **HTML whitespace compression** | `_compress_whitespace()` collapses blank lines and excess whitespace, reducing token count by 10-30% |
| **Brave search cache** | 30-day file cache for Brave Search results, eliminates redundant API calls during retries |
| **Selective comprehensive mode** | Monthly refreshes use `comprehensive=False` (faster), quarterly recovery uses `comprehensive=True` (broader search) |

### Phase 5: Celery Parallelism & Scaling

- **Chord-based task distribution**: Parent tasks identify target utility IDs and dispatch individual `process_utility` child tasks via Celery `chord`, with a `finalize_refresh_run` callback
- **Worker concurrency**: Increased from 2 to 8 concurrent workers with `--prefetch-multiplier=1`
- **Per-utility rate limiting**: `rate_limit="8/m"` on child tasks prevents overwhelming external APIs
- **Per-domain throttling**: In-process throttle ensures minimum 1-second interval between requests to the same domain

### Phase 6: Code Quality & Configuration

- **Removed internal HTTP API calls**: `update_monitoring_source` now uses direct SQLAlchemy queries instead of `api_get`/`api_patch` HTTP calls
- **Unified configuration**: All API keys now load through `_load_setting()` which reads from `app.config.settings` first, falling back to environment variables
- **Centralized settings**: All pipeline keys added to the Pydantic `Settings` class for consistency

---

## 6. Monitoring & Maintenance System

Celery Beat schedules three recurring tasks:

| Task | Schedule | Purpose |
|------|----------|---------|
| `check_all_sources` | Every Monday 06:00 UTC | Fetch all monitoring source URLs, compare content hashes to detect changes |
| `refresh_changed_tariffs` | 1st of month 08:00 UTC | Re-run pipeline on utilities with detected changes or stale data (>90 days) |
| `recover_error_utilities` | Quarterly (Jan/Apr/Jul/Oct) | Re-try utilities stuck in ERROR state with comprehensive search |

### Weekly Change Detection

- Fetches each monitored URL (concurrency=24)
- Hashes content and compares to stored hash
- Updates source status: `ok`, `changed`, `error`
- Logs each check with diff summary

### Monthly Refresh (chord-based)

- Targets utilities with `CHANGED` monitoring status or stale tariffs (>90 days)
- Dispatches individual Celery tasks per utility
- Callback aggregates results into `refresh_runs` table
- Uses `comprehensive=False` for speed

### Quarterly Recovery

- Targets utilities in ERROR state (up to 200)
- Uses `comprehensive=True` for broader search
- Tries to rediscover tariff pages via fresh Brave Search

---

## 7. Independent Audit System (Opus Auditor)

### Purpose

An independent verification tool using Claude Opus (a more powerful, expensive LLM) to check whether stored tariff data matches what's actually on the utility's website.

### How It Works (`backend/scripts/opus_audit.py`)

1. Select utilities (random sample or specific IDs)
2. Fetch source pages using the same fetchers as the pipeline
3. Pull stored tariffs + components from the database
4. Send both page content and DB records to Claude Opus
5. Opus grades accuracy (A-F) and lists specific issues
6. Generate summary report as JSON

### Grading Scale

- **A**: Tariffs match source perfectly
- **B**: Minor discrepancies (slight rate differences, missing optional fields)
- **C**: Moderate issues (some wrong rates, missing tariffs)
- **D**: Significant problems (many wrong rates, phantom tariffs)
- **F**: Fundamentally wrong data
- **N/A**: Could not grade (source page not fetchable)

### Cost

~$0.15-0.25 per utility (Opus is expensive). A 50-utility sample costs ~$8-12.

---

## 8. Database State

### Schema Overview

**Core tables**: `utilities`, `tariffs`, `rate_components`, `service_territories`, `monitoring_sources`, `monitoring_logs`, `refresh_runs`, `rate_page_fingerprints`

### Current Statistics

| Metric | Count |
|--------|-------|
| Total utilities | 1,820 |
| Active utilities | 1,611 |
| Utilities with tariff data | 1,584 |
| Total tariffs | ~20,000 |
| Total rate components | ~65,000 |
| Coverage (active with data) | 98.3% |

### Key Models

**Tariff** (unique on `utility_id + name + customer_class`):
- name, code, customer_class, rate_type, description
- source_url, effective_date, confidence_score
- last_verified_at, approved (manual flag)

**RateComponent** (belongs to tariff):
- component_type: energy, demand, fixed, minimum, adjustment
- rate_value, unit ($/kWh, $/month, $/kW)
- tier_min_kwh, tier_max_kwh, tier_label
- season, period_label (for TOU)

**RatePageFingerprint** (for incremental scraping):
- utility_id, url (composite PK)
- content_hash, etag, last_modified, content_length
- checked_at, changed_at

---

## 9. Bugs, Issues & Fixes — The Full History

### 9.1 OOM Crashes During Batch Processing

**Symptom**: API container killed at ~utility 33/79 during Batch 1.

**Root cause**: Every Playwright call launched a new Chromium process that was never cleaned up.

**Fixes**:
- `_PlaywrightManager` singleton: reuses one browser, recycles after 25 contexts
- `cleanup_between_utilities()`: shuts down Playwright + GC between each utility
- API container memory increased from 2 GB to 4 GB

### 9.2 Cross-Contamination (Wrong Utility's Tariffs)

**Symptom**: Opus audit found utilities with tariffs from completely different companies.

**Root cause**: Phase 1 search returns irrelevant results from third-party aggregators, and the pipeline extracted tariffs from whatever page it found.

**Fixes**:
1. Expanded `THIRD_PARTY_DOMAINS` blocklist from ~18 to 40+ domains
2. Added utility name matching in search scoring (+25 points)
3. Added `verify_content_identity()` gate — rejects all tariffs if page content doesn't mention the utility

### 9.3 Phantom Tariffs (Stale Data Accumulation)

**Symptom**: Database contained tariffs that no longer exist on utility websites.

**Root cause**: Pipeline only did upserts, never deleted tariffs removed from websites.

**Fix**: Added reconciliation logic to `store_tariffs()` — deletes existing tariffs not in the current extraction, with domain-scoped safety (only deletes tariffs from domains we actually fetched) and wipe protection (skips reconciliation if no source domains).

### 9.4 Critical Script Calling Convention Bug

**Symptom**: During a targeted re-run of 27 flagged utilities, ALL 27 received identical tariffs from LG&E and KU (a Kentucky utility).

**Root cause**: `run_pipeline()` takes `utility_name`, `state`, `country` as optional parameters with empty-string defaults. The re-run script only passed `utility_id` without loading the utility's name/state. With empty name and state, Phase 1 searched for just "residential electric rates" — a generic query that consistently returned LG&E's rate page.

**Impact**: 27 utilities had their tariffs replaced with LG&E data. For 21 where the re-run succeeded, reconciliation deleted their original tariffs. For 6 where the corrected re-run got 0 tariffs, LG&E data persisted.

**Fix**: Updated re-run scripts to load utility details from the database before calling `run_pipeline()`.

### 9.5 Gemini Model Timeouts

**Symptom**: In an earlier iteration, the Gemini model would hang indefinitely, causing pipeline stalls.

**Root cause**: No HTTP timeout on the Gemini client, and no fallback mechanism.

**Fixes (three layers)**:
1. 30-second HTTP timeout on the Gemini client (`http_options={"timeout": 30_000}`)
2. Automatic fallback to Claude Haiku if Gemini fails or returns 0 tariffs
3. Circuit breaker: after 3 consecutive Gemini failures, bypass Gemini for the remainder of the run

### 9.6 Deployment Issues on GCP VM

Multiple deployment challenges were encountered and resolved:

- **Docker bind mounts**: Containers weren't seeing code changes. Fixed by adding bind mounts for `backend/app`, `backend/scripts`, `backend/alembic` in `docker-compose.yml`
- **macOS resource forks**: `._*` files from macOS were causing `SyntaxError: source code string cannot contain null bytes` in Python. Fixed by cleaning these during sync
- **File permissions**: Synced files had restrictive permissions. Fixed with `chmod -R a+rX`
- **SSH timeouts**: Long-running processes died when SSH disconnected. Fixed by wrapping commands in `tmux` sessions via `deploy/run-on-vm.sh`
- **Idempotent migrations**: Alembic migration for GiST index failed on re-run. Fixed with `if_not_exists=True`

### 9.7 Celery Misconfiguration

- Missing `include` parameter for task discovery
- Enum serialization mismatch (Python uppercase vs PostgreSQL lowercase)
- Both fixed with straightforward code changes

---

## 10. Audit Results & Accuracy Over Time

### Four Audit Rounds

**V1 — Initial (25 random, original pipeline)**
```
A=1, B=1, C=4, D=2, F=4, N/A=13
A/B rate: 8%  |  N/A rate: 52%
Top issues: phantom_tariff (25), stale_date (7), wrong_rate (6)
```

**V2 — Re-audit N/A utilities (13 utilities, improved page fetching in auditor)**
```
A=1, C=1, D=2, F=1, N/A=8
A/B rate: 7.7%  |  N/A rate: 62%
Top issues: wrong_rate (24), missing_component (2)
```

**V3 — New sample (25 random, auditor with PDF/Playwright support)**
```
A=1, B=3, C=5, D=4, F=4, N/A=8
A/B rate: 4%  |  N/A rate: 32%
Top issues: wrong_rate (38), phantom_tariff (38), stale_date (23)
```

**V4 — Post-fix (27 flagged, after pipeline fixes + re-extraction)**
```
A=5, B=7, C=3, D=2, F=10
A/B rate: 18.5%  |  N/A rate: 0%
```

For the 21 that actually re-extracted properly (excluding 6 with corrupted data from the LG&E bug):
```
A=5, B=7, C=3, D=2, F=4
A/B rate: 57%  |  A/B/C rate: 71%
```

### Accuracy Improvement Summary

| Metric | Before Fixes | After Fixes (proper extraction) |
|--------|-------------|--------------------------------------|
| A/B rate | ~17% | ~57% |
| A/B/C rate | ~50% | ~71% |
| F rate | ~33% | ~19% |

### Ground Truth Benchmark

A manually curated set of 35 utilities with verified rates was created for automated benchmarking (`backend/tests/fixtures/ground_truth.json`). The benchmark script (`backend/scripts/benchmark.py`) measures precision, recall, and rate value accuracy against this dataset.

---

## 11. Deployment & Operations

### Infrastructure

- **VM**: Google Cloud `e2-standard-2` (2 vCPU, 8 GB RAM) in `us-central1-a`
- **Instance name**: `utility-tariff-finder`
- **Access**: `gcloud compute ssh utility-tariff-finder --zone=us-central1-a`
- **HTTPS**: Caddy with auto-provisioned Let's Encrypt SSL via `.nip.io` domain

### Deployment Scripts

| Script | Purpose |
|--------|---------|
| `deploy/sync-to-vm.sh` | Syncs local code to the VM, optionally rebuilds containers or runs migrations |
| `deploy/run-on-vm.sh` | Runs commands on the VM inside `tmux` sessions (survives SSH disconnect), logs output |
| `deploy/vm-power.sh` | Start, stop, or check status of the VM |

### Deployment Workflow

1. Edit code locally
2. Run `deploy/sync-to-vm.sh` to sync files to VM
3. If dependencies changed: `deploy/sync-to-vm.sh --rebuild`
4. If migrations needed: `deploy/sync-to-vm.sh --migrate`
5. Long-running processes: use `deploy/run-on-vm.sh` which wraps in `tmux`

Code changes to Python files are picked up automatically via Docker bind mounts (no rebuild needed for code-only changes).

---

## 12. Cost Profile

### Initial Bulk Run (All ~1,600 Utilities)

| Item | Cost |
|------|------|
| Anthropic (Claude Haiku) | ~$40 |
| Brave Search API | ~$25 |
| GCP VM (processing time) | ~$15 |
| **Total** | **~$80** |

### Projected Monthly Costs (After Optimizations)

| Item | Before Optimization | After Optimization |
|------|--------------------|--------------------|
| GCP VM (24/7) | ~$50 | ~$50 |
| Monthly refresh (LLM) | ~$10 | ~$3-5 (fingerprint skip + Gemini) |
| Weekly monitoring | ~$2 | ~$2 |
| Brave Search | ~$5 | ~$2 (caching) |
| **Total** | **~$67/month** | **~$57-59/month** |

### Full Re-extraction Cost (All Utilities)

| Item | Before Optimization | After Optimization |
|------|--------------------|--------------------|
| LLM calls | ~$40 (all Haiku) | ~$15-20 (Gemini for simple, Haiku for complex) |
| Brave Search | ~$25 | ~$5-10 (cached) |
| **Total** | **~$65** | **~$20-30** |

### Per-Operation Costs

| Operation | Cost |
|-----------|------|
| Pipeline extraction (1 utility, Gemini) | ~$0.01 |
| Pipeline extraction (1 utility, Haiku) | ~$0.04 |
| Opus audit (1 utility) | ~$0.15-0.25 |
| Monitoring check (1 URL) | ~$0.001 |

---

## 13. Known Limitations & Open Problems

### 13.1 Pipeline Accuracy Ceiling

The pipeline achieves ~57% A/B grade on audited utilities. The remaining ~43% have issues:

- **LLM extraction errors on complex rate structures**: Tiered rates, TOU periods, seasonal variations in PDFs are frequently misread
- **Wrong source pages**: For smaller utilities, search doesn't always find the right page
- **Incomplete extraction**: The LLM may extract only a subset of tariffs, then reconciliation deletes the rest

### 13.2 Reconciliation Aggression

Reconciliation (delete tariffs not in current extraction) is a double-edged sword. Domain-scoped safety and wipe protection help, but if the pipeline only finds 2 of 6 tariffs from the same domain, it still deletes the other 4.

### 13.3 `run_pipeline()` API Design

The function requires callers to pass `utility_name`, `state`, `country`, `website_url`. It does NOT look these up from the database. This led to the critical LG&E bug (Section 9.4). Any new calling code must remember to load utility details first.

### 13.4 PDF Extraction Quality

Many utilities publish rates exclusively as PDFs. The extraction chain (pdfplumber → OCR → Vision) works but complex table layouts are often mangled, and the LLM then has to extract structured data from poorly-formatted text.

### 13.5 Search Dependency

The pipeline depends on Brave Search returning relevant results. For smaller utilities with minimal web presence, search often returns aggregator sites. Google Custom Search is available as a fallback but isn't always better.

### 13.6 Validation Bounds Are Generous

A rate of $0.50/kWh passes validation but is likely wrong for most utilities. Statistical validation against state-level percentiles helps but doesn't catch all outliers.

---

## 14. Key Decisions & Trade-offs

### Residential + Commercial Only
Lighting and industrial tariffs were excluded (~7,137 deleted from database). These have fundamentally different structures and the LLM extraction prompt isn't designed for them.

### Non-Retail Utilities Deactivated
Wholesale agencies, generation companies, energy marketers, military bases, water districts, and transit agencies are deactivated. They consistently fail pipeline extraction and waste processing time. ~21 deactivated across initial batches.

### Gemini Flash as Default, Haiku as Fallback
Gemini is ~10x cheaper but less capable on complex pages. The tiered strategy with automatic fallback gives the best cost/accuracy trade-off. The circuit breaker prevents cascading Gemini failures from slowing down entire runs.

### Aggressive Reconciliation with Safety Guards
We chose to delete stale tariffs rather than accumulate phantom data. The trade-off is that incomplete extractions can cause data loss. The domain-scoped safety and wipe protection mitigate but don't eliminate this risk.

### File-Based Caching Over Redis
LLM response cache and Brave search cache use the filesystem rather than Redis. This simplifies the architecture (no Redis memory pressure) and persists across container restarts via Docker volumes.

### Docker Bind Mounts for Development
Code directories are bind-mounted into containers, allowing code changes without rebuilds. This trades container isolation for development speed.

---

## 15. File Map — Where Everything Lives

### Core Pipeline & Scripts
| File | Lines | Purpose |
|------|-------|---------|
| `backend/scripts/tariff_pipeline.py` | ~3,300 | Core 4-phase extraction pipeline with all optimizations |
| `backend/scripts/opus_audit.py` | ~500 | Independent Claude Opus auditor |
| `backend/scripts/benchmark.py` | ~200 | Automated benchmarking against ground truth |
| `backend/scripts/us_batch_runner.py` | ~300 | Batch processing orchestrator |
| `backend/scripts/rerun_50_test.py` | ~100 | 50-utility test runner |
| `backend/scripts/cleanup_duplicate_tariffs.py` | ~200 | Post-processing deduplication |

### Application Backend
| File | Purpose |
|------|---------|
| `backend/app/main.py` | FastAPI application entry point |
| `backend/app/config.py` | Pydantic settings (all API keys, DB URLs, auth config) |
| `backend/app/models/` | SQLAlchemy ORM models (utility, tariff, monitoring, fingerprint) |
| `backend/app/api/routes/` | API endpoints (tariffs, utilities, monitoring, auth, lookup) |
| `backend/app/tasks/refresh.py` | Celery chord-based refresh tasks |
| `backend/app/tasks/monitoring.py` | Weekly change detection tasks |
| `backend/app/services/` | Business logic (geocoding, OAuth, territory lookup) |
| `backend/alembic/versions/` | 7 database migration scripts |

### Frontend
| File | Purpose |
|------|---------|
| `frontend/src/pages/LookupPage.tsx` | Address lookup wizard |
| `frontend/src/pages/TariffBrowserPage.tsx` | Filterable tariff list |
| `frontend/src/pages/TariffDetailPage.tsx` | Tariff detail with rate components |
| `frontend/src/pages/MonitoringPage.tsx` | Admin monitoring dashboard |
| `frontend/src/auth/AuthContext.tsx` | Google OAuth context |

### Deployment & Operations
| File | Purpose |
|------|---------|
| `docker-compose.yml` | Full stack definition (6 services with bind mounts) |
| `deploy/sync-to-vm.sh` | Code sync to GCP VM |
| `deploy/run-on-vm.sh` | tmux-wrapped remote command execution |
| `deploy/vm-power.sh` | VM start/stop/status |
| `deploy/Caddyfile` | HTTPS reverse proxy config |

### Documentation
| File | Purpose |
|------|---------|
| `README.md` | Quick start guide and API reference |
| `TECHNICAL_REVIEW.md` | Detailed technical review (pre-optimization) |
| `CONVERSATION_HANDOFF.md` | Batch processing handoff notes |
| `docs/ARCHITECTURE.md` | System architecture overview |
| `docs/GCP.md`, `docs/GCP_FIRST_TIME.md` | GCP deployment guides |

### Test Data
| File | Purpose |
|------|---------|
| `backend/tests/fixtures/ground_truth.json` | 35-utility manually verified benchmark dataset |
| `backend/test_results/` | Lookup test results (US and Canada) |

---

*This document covers the full project history through 2026-03-31. For the original pre-optimization technical review, see `TECHNICAL_REVIEW.md`. For batch processing notes, see `CONVERSATION_HANDOFF.md`.*
