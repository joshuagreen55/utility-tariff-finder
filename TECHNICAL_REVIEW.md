# Utility Tariff Finder — Technical Review Document

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Detailed Implementation](#3-detailed-implementation)
4. [Database Schema & Current State](#4-database-schema--current-state)
5. [The Tariff Pipeline — Deep Dive](#5-the-tariff-pipeline--deep-dive)
6. [Monitoring & Maintenance System](#6-monitoring--maintenance-system)
7. [Independent Audit System (Opus Auditor)](#7-independent-audit-system-opus-auditor)
8. [Issues Encountered & Fixes Applied](#8-issues-encountered--fixes-applied)
9. [Audit Results & Accuracy Assessment](#9-audit-results--accuracy-assessment)
10. [Current Open Problems](#10-current-open-problems)
11. [Cost Profile](#11-cost-profile)

---

## 1. Problem Statement

### Goal

Build a system that automatically discovers, extracts, and stores structured electricity tariff data (rate plans with their components) for ~1,800 US and Canadian utilities. The data powers a consumer-facing application that helps users understand their electricity rates.

### Scope

- **Utilities**: 1,820 total (1,611 active) sourced from EIA (US) and provincial data (Canada)
- **Tariff types**: Residential and commercial only (lighting and industrial excluded)
- **Data per tariff**: Plan name, customer class, rate type, effective date, source URL, and structured rate components (energy $/kWh, fixed $/month, demand $/kW, with tiers/seasons/TOU periods)
- **Sources**: Utility company websites, rate schedule PDFs, regulatory filings

### Core Challenge

There is no standardized API or central database for US/Canadian utility tariffs. Each utility publishes rates differently — some on clean HTML pages, some in dense PDFs, some behind JavaScript-heavy sites, some barely online at all. The system must:

1. **Find** the right rate page for each utility via web search
2. **Crawl** that page and discover sub-pages/PDFs containing actual rate data
3. **Extract** structured tariff information from unstructured HTML/PDF content using an LLM
4. **Validate** the extracted data for correctness and completeness
5. **Store** it reliably and keep it up to date

---

## 2. High-Level Architecture

### System Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    Google Cloud VM                           │
│                  (e2-standard-2, us-central1-a)             │
│                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │  Caddy   │  │ FastAPI  │  │  Celery   │  │  Celery  │   │
│  │  (HTTPS) │──│   API    │  │  Worker   │  │   Beat   │   │
│  │  :443    │  │  :8000   │  │ (concur=2)│  │(scheduler│   │
│  └──────────┘  └────┬─────┘  └─────┬─────┘  └──────────┘   │
│                     │              │                         │
│               ┌─────┴─────┐  ┌─────┴─────┐                  │
│               │ PostgreSQL│  │   Redis    │                  │
│               │ (PostGIS) │  │   :6379    │                  │
│               │   :5432   │  │            │                  │
│               └───────────┘  └────────────┘                  │
└─────────────────────────────────────────────────────────────┘
         │                        │
         │ Brave Search API       │ Anthropic API
         │ (web search)           │ (Claude Haiku for extraction)
         │                        │ (Claude Opus for auditing)
         ▼                        ▼
    ┌──────────┐           ┌──────────────┐
    │  Brave   │           │  Anthropic   │
    │  Search  │           │  Claude API  │
    └──────────┘           └──────────────┘
```

### Tech Stack

| Layer | Technology |
|-------|-----------|
| **Backend** | Python 3.12, FastAPI, SQLAlchemy 2.0, Alembic |
| **Frontend** | React 18, TypeScript, Vite |
| **Database** | PostgreSQL 16 (PostGIS) |
| **Task Queue** | Celery + Redis |
| **Web Server** | Caddy (auto-HTTPS via Let's Encrypt) |
| **Container** | Docker Compose (6 services) |
| **Web Scraping** | httpx, BeautifulSoup, Playwright (headless Chromium) |
| **PDF Extraction** | pdfplumber, pdf2image + pytesseract (OCR fallback) |
| **LLM Extraction** | Anthropic Claude Haiku (`claude-sonnet-4-20250514`) |
| **LLM Auditing** | Anthropic Claude Opus (`claude-opus-4-20250514`) |
| **Web Search** | Brave Search API |
| **Auth** | Google OAuth 2.0 |

### Docker Services

| Service | Memory Limit | Purpose |
|---------|-------------|---------|
| `db` | 1 GB | PostgreSQL with PostGIS |
| `redis` | 256 MB | Celery broker + result backend |
| `api` | 4 GB | FastAPI + pipeline scripts + Playwright |
| `celery-worker` | 4 GB | Background task execution |
| `celery-beat` | 256 MB | Periodic task scheduler |
| `web` | 128 MB | Caddy reverse proxy + static frontend |

---

## 3. Detailed Implementation

### Directory Structure

```
├── backend/
│   ├── app/
│   │   ├── api/routes/         # FastAPI endpoints (tariffs, utilities, admin)
│   │   ├── auth/               # Google OAuth
│   │   ├── db/                 # session.py (async + sync engines), base.py
│   │   ├── models/             # SQLAlchemy ORM models
│   │   │   ├── utility.py      # Utility model
│   │   │   ├── tariff.py       # Tariff + RateComponent models
│   │   │   ├── monitoring.py   # MonitoringSource + MonitoringLog
│   │   │   └── refresh_run.py  # RefreshRun tracking model
│   │   ├── schemas/            # Pydantic request/response schemas
│   │   ├── services/           # Business logic services
│   │   └── tasks/              # Celery tasks
│   │       ├── celery_app.py   # Celery configuration + beat schedule
│   │       ├── monitoring.py   # Weekly change detection tasks
│   │       └── refresh.py      # Monthly re-extraction tasks
│   ├── scripts/
│   │   ├── tariff_pipeline.py  # Core 4-phase extraction pipeline (~2,300 lines)
│   │   ├── opus_audit.py       # Independent Claude Opus auditor
│   │   ├── us_batch_runner.py  # Batch processing orchestrator
│   │   ├── cleanup_duplicate_tariffs.py
│   │   ├── batch_audit.py
│   │   └── rerun_flagged.py    # Re-run pipeline on specific utilities
│   └── alembic/                # Database migrations
├── frontend/
│   └── src/
│       ├── pages/              # LookupPage, TariffBrowserPage, TariffDetailPage
│       ├── components/         # UI components
│       └── api/                # API client
├── deploy/
│   ├── Dockerfile.web          # Caddy + built frontend
│   └── Caddyfile               # HTTPS config
├── docker-compose.yml
└── logs/                       # Mounted volume for audit reports
```

### Key API Endpoints

- `GET /api/tariffs/` — List tariffs with filters (utility, state, class)
- `GET /api/tariffs/{id}` — Tariff detail with rate components
- `GET /api/utilities/` — List utilities with filters
- `GET /api/utilities/search` — Address-based utility lookup
- `POST /api/admin/pipeline/run` — Trigger pipeline for a utility
- `GET /api/admin/monitoring/sources` — List monitoring sources

---

## 4. Database Schema & Current State

### Core Models

#### `utilities` table
```
id              INT PRIMARY KEY
name            VARCHAR(500)        -- "Pacific Gas and Electric Co"
eia_id          INT UNIQUE          -- EIA identifier (US utilities)
country         ENUM(US, CA)
state_province  VARCHAR(50)         -- "CA", "ON", etc.
utility_type    ENUM(IOU, municipal, cooperative, ...)
website_url     TEXT                -- Known website URL (nullable)
tariff_page_urls JSONB              -- Discovered rate page URLs
is_active       BOOLEAN             -- False = deactivated (not a retail provider)
created_at      TIMESTAMP
updated_at      TIMESTAMP
```

#### `tariffs` table
```
id                  INT PRIMARY KEY
utility_id          INT FK -> utilities
name                VARCHAR(500)        -- "Residential Service - Rate RS"
code                VARCHAR(100)        -- "RS", "GS-1", etc.
customer_class      ENUM(residential, commercial)
rate_type           ENUM(flat, tiered, tou, demand, seasonal, ...)
description         TEXT
source_url          TEXT                -- URL where this tariff was found
source_document_hash VARCHAR(64)
effective_date      DATE
last_verified_at    TIMESTAMP
approved            BOOLEAN             -- Manual approval flag
created_at          TIMESTAMP
updated_at          TIMESTAMP

UNIQUE(utility_id, name, customer_class)
```

#### `rate_components` table
```
id              INT PRIMARY KEY
tariff_id       INT FK -> tariffs (CASCADE DELETE)
component_type  ENUM(energy, demand, fixed, minimum, adjustment)
unit            VARCHAR(50)         -- "$/kWh", "$/month", "$/kW"
rate_value      DECIMAL             -- The actual rate number
tier_min_kwh    DECIMAL             -- Tier lower bound
tier_max_kwh    DECIMAL             -- Tier upper bound
tier_label      VARCHAR(200)        -- "First 500 kWh"
period_label    VARCHAR(200)        -- "On-Peak", "Off-Peak"
season          VARCHAR(100)        -- "Summer", "Winter"
created_at      TIMESTAMP
updated_at      TIMESTAMP
```

#### `monitoring_sources` table
```
id              INT PRIMARY KEY
utility_id      INT FK -> utilities
url             TEXT                -- Rate page URL to monitor
frequency       VARCHAR(20)         -- "weekly", "monthly"
content_hash    VARCHAR(64)         -- Hash of last-seen content
status          ENUM(ok, changed, error, pending)
last_checked_at TIMESTAMP
last_changed_at TIMESTAMP
```

#### `monitoring_logs` table
```
id              INT PRIMARY KEY
source_id       INT FK -> monitoring_sources
checked_at      TIMESTAMP
content_hash    VARCHAR(64)
changed         BOOLEAN
diff_summary    TEXT
review_status   ENUM(pending, reviewed, dismissed)
```

#### `refresh_runs` table
```
id                  INT PRIMARY KEY
refresh_type        ENUM(monthly, quarterly, manual)
started_at          TIMESTAMP
finished_at         TIMESTAMP
utilities_targeted  INT
utilities_processed INT
tariffs_added       INT
tariffs_updated     INT
tariffs_stale       INT
errors              INT
summary_json        JSONB
error_details       JSONB
```

### Current Database Statistics

| Metric | Count |
|--------|-------|
| Total utilities | 1,820 |
| Active utilities | 1,611 |
| Utilities with tariff data | 1,584 |
| Total tariffs | 20,041 |
| Total rate components | 64,950 |
| Coverage (active with data) | 98.3% |

### Tariff Distribution (Top 20 States)

| State | Utilities | Tariffs |
|-------|-----------|---------|
| TX | 160 | 2,884 |
| CA | 57 | 1,154 |
| NY | 53 | 1,104 |
| OH | 55 | 1,081 |
| NC | 63 | 955 |
| WI | 70 | 951 |
| TN | 81 | 717 |
| GA | 61 | 677 |
| MN | 45 | 559 |
| FL | 36 | 496 |
| OK | 34 | 440 |
| MO | 49 | 399 |
| IN | 45 | 394 |
| ON | 60 | 387 |
| MA | 23 | 368 |
| OR | 22 | 368 |
| NE | 24 | 366 |
| PA | 29 | 366 |
| WA | 36 | 357 |
| IL | 27 | 352 |

---

## 5. The Tariff Pipeline — Deep Dive

### Overview

The core pipeline (`backend/scripts/tariff_pipeline.py`, ~2,300 lines) processes one utility at a time through four phases. It is invoked by the batch runner, the monitoring refresh tasks, or directly via CLI.

```
run_pipeline(utility_id, utility_name, state, country, website_url, ...)
    │
    ├── Phase 1: Find rate page (Brave Search)
    │       └── score_search_result() → ranked URL candidates
    │
    ├── Phase 2: Discover tariff sub-pages (crawl + PDF detection)
    │       └── Returns list[RatePage] (HTML pages + extracted PDFs)
    │
    ├── Phase 3: LLM extraction (Claude Haiku)
    │       └── Returns list[ExtractedTariff] with components
    │
    ├── verify_content_identity() → cross-contamination check
    │
    ├── Phase 4: Validate
    │       └── Checks classes, types, rate bounds, components
    │
    └── store_tariffs() → upsert to DB + reconcile stale tariffs
```

### Phase 1: Find Rate Page

**Purpose**: Given a utility name and state, find the URL of their rates/tariffs page.

**Process**:
1. If no known website URL, discover the utility's domain via Brave Search (`"{name} electric utility official website {state}"`)
2. Search for rate pages: `"{name} residential electric rates {state}"`
3. Score each result using `score_search_result()`:
   - **+50** if URL domain matches utility's known domain
   - **+15** each for "rate", "pricing", "electric" in URL path
   - **+10** for "residential" in path
   - **+25** if search result title/description mentions ≥2 utility name words
   - **-40** if URL domain is in `THIRD_PARTY_DOMAINS` blocklist
   - **-30** if URL is a PDF or homepage-only path
   - **-15** if URL path contains regulatory/filing keywords
4. If top result isn't from the utility's domain, try direct URL patterns (`/rates`, `/electric-rates`, etc.)
5. If still no match, try a site-scoped search (`site:{domain} residential rates`)

**`THIRD_PARTY_DOMAINS` blocklist** (35 domains):
```
openei.org, wikipedia.org, yelp.com, yellowpages.com, bbb.org,
facebook.com, twitter.com, linkedin.com, utility-rates.com,
costcheckusa.com, findenergy.com, energysage.com, choosetexaspower.org,
electricityplans.com, saveonenergy.com, electricrate.com, wattbuy.com,
opb.org, prnewswire.com, businesswire.com, reddit.com, nextdoor.com,
glassdoor.com, mapquest.com, solar.com, nrgcleanpower.com,
greenridgesolar.com, koin.com, madison.com, energybot.com,
electricitylocal.com, energypal.com, electricchoice.com,
paylesspower.com, texaselectricityratings.com, powertochoose.org,
electricityrates.com, utilitygenius.com, switchwise.com,
energyrates.ca, ratehub.ca
```

**Key vulnerability**: If Brave Search returns no relevant results from the utility's own site, and direct URL patterns fail, the pipeline either gives up or latches onto an irrelevant result from a third-party site.

### Phase 2: Discover Tariff Pages

**Purpose**: Starting from the rate page URL, crawl to find all sub-pages and PDFs containing actual rate data.

**Process**:
1. Fetch the main rate page (httpx first, Playwright fallback for JS-rendered sites)
2. Extract all links, filter for rate-relevant URLs using regex patterns
3. Follow up to 10 relevant links
4. For each page:
   - HTML: fetch and extract text via BeautifulSoup
   - PDF: download bytes, extract text via pdfplumber, OCR fallback (pdf2image + pytesseract) if pdfplumber yields < 200 chars
5. Return list of `RatePage` objects

**PDF handling chain**:
```
URL → _download_pdf() [httpx] 
    → if fail: _download_pdf_playwright() [headless browser]
    → _extract_pdf_pdfplumber() [text extraction]
    → if < 200 chars: _extract_pdf_ocr() [image-based OCR]
    → cached via content hash
```

**Playwright management**: A singleton `_PlaywrightManager` reuses one Chromium browser instance, auto-recycling after 25 contexts to prevent OOM. Between utilities, `cleanup_between_utilities()` shuts down Playwright and runs GC.

### Phase 3: LLM Extraction

**Purpose**: Use Claude Haiku to extract structured tariff data from page content.

**Process**:
1. Sort pages by URL depth (detail pages first)
2. For each page with rate content signals (regex check for $/kWh, cents/kWh, etc.):
   - Trim content to 20,000 chars of rate-relevant sections
   - Send to Claude with the extraction prompt
   - Parse JSON response into `ExtractedTariff` objects
3. Deduplicate across pages — if same tariff name appears from multiple pages, keep the version with more components
4. Max 8 LLM calls per utility

**LLM model**: `claude-sonnet-4-20250514` (Claude Haiku class)

**Extraction prompt** asks Claude to return JSON array of tariffs with:
```json
{
  "name": "Rate RS - Residential Service",
  "code": "RS",
  "customer_class": "residential",
  "rate_type": "tiered",
  "description": "Standard residential service",
  "effective_date": "2024-01-01",
  "components": [
    {
      "component_type": "energy",
      "unit": "$/kWh",
      "rate_value": 0.08543,
      "tier_min_kwh": 0,
      "tier_max_kwh": 500,
      "tier_label": "First 500 kWh",
      "season": "Summer",
      "period_label": null
    }
  ]
}
```

### Content Identity Verification (Post-Phase 3)

**Purpose**: Detect cross-contamination — when the pipeline accidentally extracts tariffs from the wrong utility's website.

**Process** (`verify_content_identity()`):
1. Tokenize the utility name into significant words (dropping corporate suffixes)
2. Check if those words appear in the first 5,000 chars of each fetched page
3. If ≥30% of name words match (or ≥2 words), pass
4. If 0 name words match and the state isn't mentioned either, **reject all tariffs**
5. If 0 name words match but state is mentioned, proceed with caution

**Limitation**: If the utility name is generic or very short (e.g., "City of Newport"), this check is less effective. It also can't catch cases where the wrong utility happens to be in the same state.

### Phase 4: Validation

**Purpose**: Apply rule-based validation to catch obviously bad data.

**Checks**:
- Tariff must have a name (not "Unknown")
- `customer_class` must be "residential" or "commercial"
- `rate_type` must be in the allowed set (flat, tiered, tou, demand, seasonal, etc.)
- Must have at least one core component (energy, fixed, or demand)
- Rate value bounds:
  - Energy: ≤ $2.00/kWh
  - Fixed: ≤ $500/month
  - Demand: ≤ $100/kW
  - No negative rates (except adjustments and energy)
- Component types must be valid

### Store Tariffs + Reconciliation

**Upsert logic** (`store_tariffs()`):
- For each validated tariff, check if a tariff with the same `(utility_id, name, customer_class)` already exists
- If exists: update rate_type, description, source_url, effective_date, components
- If new: create tariff + components
- All rate components are replaced on update (delete old, insert new)

**Reconciliation** (added later to fix phantom tariff problem):
- After storing, if ≥1 residential tariff was stored:
  - Fetch all existing tariffs for this utility
  - Delete any tariff whose `(name, customer_class)` is NOT in the current extraction
  - This removes "phantom" tariffs that no longer exist on the utility's website

**Reconciliation risk**: If the pipeline only finds 2 of a utility's actual 6 tariffs, it will delete the other 4. The safeguard (requiring at least 1 residential) is minimal.

### Retry Mechanism

If Phase 2+3 yield 0 tariffs from the top URL, the pipeline tries up to 4 alternate URLs from Phase 1 results, choosing URLs with different path prefixes to explore different site sections.

---

## 6. Monitoring & Maintenance System

### Architecture

Celery Beat schedules three recurring tasks:

| Task | Schedule | Purpose |
|------|----------|---------|
| `check_all_sources` | Every Monday 06:00 UTC | Fetch all monitoring source URLs, compare content hashes to detect changes |
| `refresh_changed_tariffs` | 1st of each month 08:00 UTC | Re-run pipeline on utilities with detected changes or stale data (>90 days) |
| `recover_error_utilities` | Quarterly (Jan/Apr/Jul/Oct 1st) | Re-try utilities where all monitoring sources are in ERROR state |

### Weekly Change Detection (`check_all_sources`)

- Fetches each monitored URL (concurrency=24, per-host limit=4)
- Hashes content and compares to stored hash
- Updates source status: `ok` (unchanged), `changed` (content differs), `error` (fetch failed)
- Logs each check with diff summary

### Monthly Refresh (`refresh_changed_tariffs`)

- Targets utilities with:
  - Recent `CHANGED` monitoring status
  - Stale tariffs (no `last_verified_at` within 90 days)
- Runs the full pipeline on each
- Records results in `refresh_runs` table

---

## 7. Independent Audit System (Opus Auditor)

### Purpose

An independent verification tool that uses a different, more powerful LLM (Claude Opus) to check whether our stored tariff data matches what's actually on the utility's website. This is conceptually similar to a human manually checking each utility.

### How It Works (`backend/scripts/opus_audit.py`)

1. **Select utilities**: Random sample or specific IDs
2. **Fetch source pages**: Uses the same robust fetchers as the pipeline (pdfplumber/OCR for PDFs, Playwright for JS sites)
3. **Format DB tariffs**: Pulls all stored tariffs + rate components for the utility
4. **Construct audit prompt**: Sends both the page content and DB records to Claude Opus, asking it to grade accuracy
5. **Parse response**: Structured JSON with grade (A-F), issue list, missing/phantom tariffs
6. **Generate report**: Summary statistics saved to JSON

### Grading Scale

- **A**: Tariffs match source perfectly
- **B**: Minor discrepancies (slight rate differences, missing optional fields)
- **C**: Moderate issues (some wrong rates, missing tariffs)
- **D**: Significant problems (many wrong rates, phantom tariffs)
- **F**: Fundamentally wrong data (wrong utility's tariffs, completely inaccurate)
- **N/A**: Could not grade (source page not fetchable)

### Cost

~$0.15-0.25 per utility (Opus is expensive). A 25-utility sample costs ~$4-6.

---

## 8. Issues Encountered & Fixes Applied

### 8.1 OOM Crashes (Early Batch Processing)

**Symptom**: API container killed at ~utility 33/79 during Batch 1.

**Root cause**: Every Playwright call launched a new Chromium process that was never cleaned up.

**Fixes**:
- `_PlaywrightManager` singleton: reuses one browser, recycles after 25 contexts
- `cleanup_between_utilities()`: shuts down Playwright + GC between each utility
- API container memory increased from 2 GB to 4 GB
- Removed `--single-process` Chromium arg (caused fragility)

### 8.2 Cross-Contamination

**Symptom**: Opus audit found utilities with tariffs from completely different companies. E.g., a Tennessee co-op having rates from a Texas provider.

**Root cause**: Phase 1 search returns irrelevant results (especially from third-party aggregator sites), and the pipeline extracts tariffs from whatever page it finds — even if it belongs to a different utility.

**Fixes applied**:
1. Expanded `THIRD_PARTY_DOMAINS` blocklist from ~18 to ~35 domains
2. Added utility name matching in `score_search_result()` — results mentioning the utility name get +25 score
3. Added `verify_content_identity()` gate after extraction — rejects all tariffs if page content doesn't mention the utility name or state

**Remaining gap**: If the search finds a page from the wrong utility in the same state, the identity check still passes. The check is also weak for utilities with very generic names.

### 8.3 Phantom Tariffs

**Symptom**: Database contained tariffs that no longer exist on the utility's website — old rate plans that were superseded.

**Root cause**: The pipeline only did upserts. It never deleted tariffs that were no longer present in a new extraction. Over time, stale tariffs accumulated.

**Fix applied**: Added reconciliation logic to `store_tariffs()` — after storing new tariffs, any existing tariff for that utility not in the current extraction gets deleted (if at least 1 residential tariff was stored as a safety guard).

**Remaining gap**: The reconciliation is aggressive. If the pipeline only finds a subset of the utility's actual tariffs, it will delete the ones it missed. The "at least 1 residential" safeguard is minimal.

### 8.4 Stale/Wrong Rates

**Symptom**: Rate values in the database didn't match what's on the utility's website — could be outdated or incorrectly parsed.

**Root cause**: The LLM sometimes misreads rate values from complex PDF tables, tiered rate structures, or when multiple rate schedules are on the same page. No bounds checking caught implausible values.

**Fix applied**: Added validation bounds in Phase 4:
- Energy rate > $2.00/kWh → rejected
- Fixed charge > $500/month → rejected
- Demand charge > $100/kW → rejected

**Remaining gap**: These bounds are generous. A rate of $0.50/kWh would pass validation but is likely wrong for most utilities. The fundamental issue is LLM extraction accuracy on complex rate structures.

### 8.5 Celery Configuration Issues

**Symptom**: Celery worker started but discovered no tasks.

**Root cause**: Missing `include` parameter in Celery app initialization.

**Fix**: Added `include=["app.tasks.monitoring", "app.tasks.refresh"]` to Celery app config.

### 8.6 Enum Serialization Bug

**Symptom**: `InvalidTextRepresentation: invalid input value for enum refreshtype: "MANUAL"`

**Root cause**: Python enum names were uppercase (`MONTHLY`) but PostgreSQL enum values were lowercase (`monthly`). SQLAlchemy serialized the Python name instead of the value.

**Fix**: Changed Python enum member names to lowercase to match DB values.

### 8.7 Script Calling Convention Bug (Critical)

**Symptom**: During a targeted re-run of 27 flagged utilities, ALL 27 were given the same tariffs from LG&E and KU (a Kentucky utility).

**Root cause**: `run_pipeline()` takes `utility_name`, `state`, `country` as optional parameters with empty-string defaults. The re-run script only passed `utility_id` without loading the utility's name/state from the database. With empty name and state, Phase 1 searched for just "residential electric rates" — a generic query that consistently returned LG&E's rate page.

**Impact**: All 27 utilities had their tariffs replaced with LG&E data. For the 21 where the re-run succeeded (storing LG&E tariffs), reconciliation then deleted their original tariffs. For the 6 where the corrected re-run later got 0 tariffs, the LG&E data persists because reconciliation only triggers when new tariffs are stored.

**Fix**: Updated the re-run script to load utility details from the database before calling `run_pipeline()`.

**Lesson**: `run_pipeline()` does NOT look up the utility from the database itself — the caller must provide all context. This is an API design issue that should arguably be fixed at the function level.

---

## 9. Audit Results & Accuracy Assessment

### Four Audit Rounds

#### V1 — Initial Audit (25 random utilities, original pipeline)
```
Grade distribution: A=1, B=1, C=4, D=2, F=4, N/A=13
Accuracy rate: 4% (perfect scores)
Top issues: phantom_tariff (25), stale_date (7), wrong_rate (6)
N/A rate: 52% (auditor couldn't fetch source pages)
```

#### V2 — Re-audit of V1 N/A utilities (13 utilities, improved page fetching in auditor)
```
Grade distribution: A=1, C=1, D=2, F=1, N/A=8
Accuracy rate: 7.7%
Top issues: wrong_rate (24), missing_component (2), phantom_tariff (2)
N/A rate: 62% (still couldn't fetch many pages)
```

#### V3 — New random sample (25 utilities, auditor with PDF/Playwright support)
```
Grade distribution: A=1, B=3, C=5, D=4, F=4, N/A=8
Accuracy rate: 4%
Top issues: wrong_rate (38), phantom_tariff (38), stale_date (23)
N/A rate: 32% (improved with PDF/Playwright)
```

#### V4 — Post-fix re-audit (27 flagged utilities, after pipeline fixes + re-extraction)
```
Grade distribution: A=5, B=7, C=3, D=2, F=10
Accuracy rate: 18.5%
Top issues: phantom_tariff (25), wrong_rate (25), missing_component (12)
N/A rate: 0% (all utilities graded)
```

### V4 Context

The V4 results are complicated by the botched re-run (Section 8.7). Of the 10 F-grade utilities:
- **6 still have LG&E data** from the botched run (pipeline re-run got 0 tariffs for these, so wrong data persists)
- **4 are genuinely hard-to-scrape** sites (Alaska utilities, retail energy marketers, Canadian hydro utilities)

For the **21 utilities that actually re-extracted properly**, the grade distribution was:
```
A=5, B=7, C=3, D=2, F=4
A/B rate: 57% (12/21)
A/B/C rate: 71% (15/21)
```

### Accuracy Summary

| Metric | Before Fixes | After Fixes (proper extraction only) |
|--------|-------------|--------------------------------------|
| A/B rate | ~17% | ~57% |
| A/B/C rate | ~50% | ~71% |
| F rate | ~33% | ~19% |

The fixes clearly help, but there is still a significant tail of utilities where the pipeline produces incorrect or incomplete data.

---

## 10. Current Open Problems

### 10.1 Fundamental Pipeline Accuracy Ceiling

The pipeline's accuracy appears to plateau around 55-65% A/B grade. The remaining ~35-45% have issues that the current guardrails don't address:

- **LLM extraction errors on complex rate structures**: Tiered rates, TOU periods, seasonal variations, and demand charges in PDFs are frequently misread
- **Wrong source pages**: For smaller utilities, Brave Search doesn't always find the right page. The pipeline may extract from a state regulatory site or aggregator that has similar but different data
- **Incomplete extraction**: The LLM may only extract 2 of 5 tariffs from a page, then reconciliation deletes the other 3

### 10.2 Reconciliation Aggression

The phantom tariff fix (delete tariffs not in current extraction) is a double-edged sword:
- It correctly removes outdated tariffs
- But if the pipeline only finds a subset of tariffs (common), it also removes valid ones
- The only safeguard is "at least 1 residential tariff must be stored" — this is too permissive

### 10.3 `run_pipeline()` API Design

The function requires callers to pass `utility_name`, `state`, `country`, and `website_url`. It does NOT look these up from the database. This led to the critical bug in Section 8.7 and is a footgun for any new calling code.

### 10.4 Search Dependency on Brave

The entire pipeline depends on Brave Search API returning relevant results. For smaller utilities with minimal web presence, Brave often returns aggregator sites or unrelated results. There is no fallback to another search engine.

### 10.5 PDF Extraction Quality

Many utilities publish rates exclusively as PDFs. The pdfplumber → OCR pipeline works but:
- Complex table layouts are often mangled
- Multi-page rate schedules lose structure
- OCR is slow and error-prone on scanned documents
- The LLM then has to extract structured data from poorly-formatted text

### 10.6 No Ground Truth

There is no manually verified "ground truth" dataset to benchmark against. The Opus auditor is the closest thing, but it's also an LLM — it can make mistakes, and "N/A" grades (can't fetch source) reduce sample sizes.

### 10.7 Six Utilities with Corrupted Data

From the botched re-run, 6 utilities (IDs: 50, 768, 906, 1228, 1571, 1799) currently have LG&E and KU tariffs in the database. These need to be either re-extracted or have their tariffs manually cleaned.

---

## 11. Cost Profile

### Initial Bulk Run (All Batches)

| Item | Cost |
|------|------|
| Anthropic (Claude Haiku) | ~$40 |
| Brave Search API | ~$25 |
| GCP VM (e2-standard-2) | ~$15 |
| **Total** | **~$80** |

### Ongoing Monthly Costs

| Item | Cost/Month |
|------|-----------|
| GCP VM (running 24/7) | ~$50 |
| Weekly monitoring (Brave/httpx) | ~$2 |
| Monthly refresh (Haiku extraction) | ~$5-10 |
| Opus audit (25 utilities/month) | ~$5 |
| **Total** | **~$62-67/month** |

### Per-Utility Costs

| Operation | Cost |
|-----------|------|
| Pipeline extraction (1 utility) | ~$0.04 (Haiku + Brave) |
| Opus audit (1 utility) | ~$0.15-0.25 |
| Monitoring check (1 URL) | ~$0.001 |

---

*Document generated 2026-04-04. Data reflects database state as of this date.*
