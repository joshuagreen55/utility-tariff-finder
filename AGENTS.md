# AGENTS.md — operating manual for the Utility Tariff Finder

This file is the **system-of-record for AI agents and new engineers**. Read it
before making changes or operating the system. It is intentionally
comprehensive; when in doubt, prefer what is written here over older docs
(`PROJECT_SUMMARY.md` and `TECHNICAL_REVIEW.md` predate most of the current
refresh/quarantine/cost/model systems).

_Last updated: 2026-08-28._

---

## 1. What this system does

Utility Tariff Finder lets a user enter a US/Canadian address and see which
electric utilities serve it and their current rate tariffs (residential and
commercial), broken down into rate components (energy, demand, fixed, etc.).

The hard part is not the lookup — it is **keeping ~16,000 tariffs across
~1,425 active utilities accurate and fresh**, because utilities publish rates
on their own websites/PDFs in wildly inconsistent formats. The bulk of the
codebase is an **automated extraction + refresh pipeline** that discovers each
utility's rate page, extracts structured tariffs with LLMs, validates them,
and re-verifies them on a schedule.

### Data sources
| Source | Coverage | Use |
|--------|----------|-----|
| EIA Form 861 | ~3,300 US utilities | Utility master list |
| OpenEI URDB | 62,600+ rate records | Tariff **seed** data (much of it stale, from ~2017) |
| HIFLD | US territory polygons | Address→utility mapping |
| Provincial boards (OEB, etc.) | Canadian utilities | Manual/centralized curation |

"**Stale OpenEI seed**" = a tariff imported from OpenEI that has never been
re-verified against a live source (`last_verified_at IS NULL`). Reducing this
backlog is the main quality lever.

---

## 2. Tech stack & repo layout

- **Backend**: Python 3.12, FastAPI (async), SQLAlchemy, Pydantic. PostgreSQL 16 + PostGIS.
- **Task queue**: Celery + Redis (beat scheduler for monitoring & refresh).
- **Frontend**: React 18 + TypeScript + Vite 5, `react-router-dom`. Served by Caddy.
- **LLMs**: Anthropic Claude (Haiku, Opus) + Google Gemini (Flash + Deep Research).
- **Deploy**: Docker Compose on a single GCP VM (`utility-tariff-finder`, `us-central1-a`).

```
backend/
  app/
    api/routes/       # FastAPI routers: lookup, utilities, tariffs, monitoring, auth
    models/           # SQLAlchemy models (see §3)
    services/         # geocoder, territory_lookup, monitor, monitoring_runner, google_oauth
    tasks/            # celery_app (beat schedule), monitoring, refresh
    config.py         # Pydantic Settings (all env vars)
    db/session.py     # async + sync engine singletons
    main.py           # FastAPI entrypoint
  alembic/versions/   # DB migrations (head = b9c0d1e2f3a4)
  scripts/            # the pipeline + seeds + campaigns + audits (see §5, §6)
  tests/fixtures/     # ground_truth.json (no pytest suite; benchmark via scripts/benchmark.py)
frontend/src/         # React app (pages/, components/, api/client.ts)
deploy/               # sync-to-vm.sh, run-on-vm.sh, vm-*.sh, Caddyfile, Dockerfile.web
docs/                 # ARCHITECTURE.md, GCP*.md, DATABASE_ACCESS.md, CENTRALIZED_REGULATORS.md
docker-compose.yml    # the production stack
```

---

## 3. Data model (PostgreSQL)

Models live in `backend/app/models/`. Key tables and columns:

- **`utilities`** (`utility.py`) — `id`, `name`, `eia_id`, `country` (US/CA),
  `state_province`, `utility_type` (IOU, municipal, cooperative, …),
  `website_url`, `tariff_page_urls` (JSONB), `rate_page_url_override`,
  `is_active`. **Refresh-quarantine bookkeeping**: `refresh_fail_streak`,
  `refresh_quarantined_at`, `refresh_last_reason`, `refresh_last_attempt_at`.
- **`tariffs`** (`tariff.py`) — `id`, `utility_id`, `name`, `code`,
  `customer_class` (residential/commercial/industrial/lighting), `rate_type`,
  `effective_date`, `end_date`, `source_url`, `last_verified_at`, `approved`,
  `confidence_score`, `confidence_factors` (JSONB — includes `needs_review`),
  `openei_id`, `raw_openei_data`. **Soft-supersede** (audit-preserving
  retirement): `superseded_by_tariff_id` (self-FK) + `supersede_reason`
  (`matcher` | `llm_absorb` | `manual`). TOU schedules stored as JSONB.
- **`rate_components`** (`tariff.py`) — `tariff_id`, `component_type`
  (energy/demand/fixed/minimum/adjustment), `unit`, `rate_value`
  (`Numeric(16,6)`), tiering + TOU period fields.
- **`monitoring_sources`** (`monitoring.py`) — a URL to watch per utility;
  `status` (unchanged/changed/error/pending), `last_content_hash`,
  `last_changed_at`. **`monitoring_logs`** records each check.
- **`refresh_runs`** (`refresh_run.py`) — one row per monthly/quarterly/manual
  run: `utilities_targeted`, `utilities_processed`, `tariffs_added`,
  `tariffs_updated`, `errors`, `summary_json` (JSONB — holds
  `targeted_utility_ids`, `quarantined_skipped`, and `llm_cost`),
  `error_details`.
- **`service_territories`** (`territory.py`) — PostGIS `MULTIPOLYGON` + zip
  arrays for address→utility matching.
- **`rate_page_fingerprints`** (`fingerprint.py`) — content hashes per
  (utility, url) so unchanged pages can be skipped (a cheap re-verify).

### "Live" vs "superseded" — a critical invariant
A tariff is **live** only when `superseded_by_tariff_id IS NULL AND
supersede_reason IS NULL`. Every user-facing query, coverage count, and health
metric must filter to live tariffs. Do **not** hard-delete superseded rows —
they are the audit trail.

---

## 4. The extraction pipeline (`backend/scripts/tariff_pipeline.py`)

`run_pipeline(utility_id, ...)` runs up to six phases. It is the core engine;
one Celery `process_utility` task = one `run_pipeline` call for one utility.

| Phase | Function | What it does |
|------|----------|--------------|
| 1 | `phase1_find_rate_page` | Find the utility's rate page via Brave Search (+ domain discovery / Google CSE fallback). Hard-blocks third-party aggregators. |
| 2 | `phase2_discover_tariff_pages` | Crawl the rate page + one/two levels of sub-pages/PDFs into `RatePage` candidates. Skips aggregator/data domains (e.g. `eia.gov`) and regulator docket filings. |
| 3 | `phase3_extract_tariffs` | LLM structured extraction (tool-call), detail-page-first dedup, two-pass for complex pages. **3-tier model routing** (see below). |
| 4 | `phase4_validate` + `store_tariffs` | Validate against per-state percentile bounds (reject >p99, flag >p95 `needs_review`), normalize units, then persist + soft-supersede matching OpenEI seeds. |
| 5 | `_phase5_smart_retry` | AI-guided nav fallback: load homepage, LLM picks nav links two levels deep, re-extract. |
| 6 | `phase6_deep_research` | Gemini Deep Research (Interactions API) for the long tail. Last-resort, cost- and token-guarded. Gated by `PHASE6_ENABLED`. |

### Model-tier routing (Phase 3 extraction)
`Gemini 3.7 Flash` (tier 1, cheap) → `Claude Haiku 4.5` (tier 2) → `Claude
Opus 5` (tier 3, last resort). Opus is only invoked when a page has numeric
rate signals AND the per-utility Opus budget isn't spent.

**Key model/cost env vars** (all overridable):
- `OPUS_MODEL` (default `claude-opus-5`) — tier-3 + long-doc identify.
- `HAIKU_MODEL` (default `claude-haiku-4-5-20251001`) — tier-2.
- `GEMINI_MODEL` (default `gemini-3.7-flash`) — tier-1.
- `OPUS_MAX_PER_UTILITY` (default `2`) — cap on Opus escalations per utility
  per run. Opus historically hit on only ~8% of escalations while being ~70%
  of run cost, so this cap matters.
- `PHASE6_ENABLED` (compose default `1`), `PHASE6_MAX_WAIT_SEC`,
  `PHASE6_MAX_TOKENS`.

> **Model choices are behind env vars on purpose.** To try a model, override
> the env var in `docker-compose.yml` (or the container env) and restart the
> worker — no code change or redeploy of the image required.

---

## 5. Refresh, quarantine, health & cost systems

### Scheduled jobs (Celery beat, `app/tasks/celery_app.py`)
| Job | Task | Schedule (UTC) |
|-----|------|-----------|
| Heartbeat | `beat_heartbeat` | every 60s |
| Weekly monitoring | `check_all_sources` | Mon 06:00 |
| **Monthly refresh** | `refresh_changed_tariffs` | 1st of month 08:00 |
| **Quarterly recovery** | `recover_error_utilities` | 1st Jan/Apr/Jul/Oct 10:00 |
| Stalled-run reaper | `reap_stalled_runs` | hourly at :15 |

- **Monthly** (`refresh.py`): targets utilities whose monitoring detected a
  **change**, plus **stale** utilities (no tariff verified in 90d, ordered
  **oldest-first**). Capped at `MONTHLY_MAX_UTILITIES` (default **600**).
  Dispatches `process_utility` tasks as a Celery chord; `finalize_refresh_run`
  aggregates results (incl. per-run LLM cost) into the `RefreshRun`.
- **Quarterly**: blind re-extraction of utilities whose monitoring sources are
  all in error state.
- **Reaper**: if a chord callback never fires, reconstructs the run summary
  from the DB audit trail so the dashboard never sticks on "running".

### Sourceless quarantine (`refresh.py` + `seed_sourceless_quarantine.py`)
Utilities with no machine-readable rates on the web (small co-ops/munis) fail
every run and waste LLM spend. The scheduler tracks **consecutive structural
failures** (page reached but 0 tariffs / no rate page / no signals) on the
utility row; transient errors and crashes do **not** count. After
`QUARANTINE_STRUCTURAL_THRESHOLD` (3) consecutive structural failures a utility
is **soft-quarantined**: skipped by monthly **and** quarterly runs, re-checked
only every `QUARANTINE_RECHECK_DAYS` (120) — unless a monitoring **CHANGED**
signal overrides it. State is on the `utilities.refresh_*` columns (durable +
queryable). `seed_sourceless_quarantine.py` pre-seeds known-dead utilities.

### Health score (`scripts/health_score.py`)
Composite 0–100 score, weighted: Coverage 40% / Freshness 30% / Completeness
20% / Provenance 10%. Coverage counts a utility as covered only if it has a
**live, verified residential tariff with an energy component**. Freshness
decays as tariffs age past 90 days (that's why the score drifts down between
runs and recovers after them). Run it to get the current scorecard.

### LLM cost tracking (`scripts/llm_cost.py` + `llm_cost_report.py`)
Per-phase, per-model USD attribution from token counts. Recorded per utility,
merged into each `RefreshRun.summary_json.llm_cost`, and reported by
`llm_cost_report.py`. Also tracks **extraction-tier yield** (`tier_outcomes`:
hit/miss per model) so you can see whether Opus is earning its spend. Pricing
lives in `DEFAULT_PRICING` (override via `LLM_PRICING_JSON`); keep it in sync
with real list prices.

---

## 6. Campaign & maintenance scripts (`backend/scripts/`)
- `run_campaign.py` — unattended full stale-OpenEI cleanup in one session.
- `triage_seeds.py` — bucket stale OpenEI seeds into actionable categories.
- `supersede_via_llm.py` — "Track B": LLM 1:N absorption of stranded old
  tariffs into a fresh one (sets `supersede_reason='llm_absorb'`).
- `cleanup_duplicate_tariffs.py` / `dedup_tariffs.py` — retire duplicates via soft-supersede.
- `seed_*.py` — `seed_eia861`, `seed_canada`, `seed_openei`, `seed_territories`, `seed_monitoring_sources`.
- `opus_yield_probe.py` — live dry-run probe of extraction-tier yield for given utility IDs.
- `benchmark.py` — score pipeline output vs `tests/fixtures/ground_truth.json`.
- `run_monitoring.py` — CLI monitoring runner (concurrent).

Full inventory: run `ls backend/scripts/`. Many `*_audit.py` / `inspect_*.py`
are ad-hoc one-offs.

---

## 7. Deployment & operations runbook

**The production stack runs on a GCP VM via Docker Compose.** Local code is
bind-mounted into the `api` and `celery-worker` containers, so syncing files
makes them live instantly (no image rebuild for app/script changes).

### Deploy code changes
```bash
./deploy/sync-to-vm.sh              # sync code (live instantly via bind mounts)
./deploy/sync-to-vm.sh --migrate    # also run: alembic upgrade head
./deploy/sync-to-vm.sh --rebuild    # full image rebuild (ONLY for dependency changes)
```
> After changing anything imported at process start (models, tasks, pipeline
> constants), **restart the worker** so it reloads:
> `docker compose restart celery-worker celery-beat api`.

### Run long scripts (NEVER via `ssh --command` — SSH drops kill the process)
```bash
./deploy/run-on-vm.sh "python -m scripts.run_campaign ..." --name camp
./deploy/run-on-vm.sh --status          # list tmux sessions
./deploy/run-on-vm.sh --attach camp     # watch live
./deploy/run-on-vm.sh --logs camp       # tail the log file
./deploy/run-on-vm.sh --kill camp
```
`run-on-vm.sh` already executes **inside** the `api` container — do **not**
prefix the command with `docker compose exec`.

### VM lifecycle
- `./deploy/vm-power.sh {start|stop|status}` — stop/start to save cost.
- `./deploy/vm-resize.sh <machine-type>` — resize (preserves `pgdata`).
- Compose services: `db` (postgis), `redis`, `api`, `celery-worker`,
  `celery-beat`, `autoheal` (restarts a hung beat), `web` (Caddy: serves the
  SPA and proxies `/api/*`).

### Check system state (read-only, safe)
Run inside the api container (`docker compose exec -T api ...`) on the VM:
```bash
python /app/scripts/health_score.py                 # current health scorecard
python /app/scripts/llm_cost_report.py --runs 5     # LLM cost + Opus yield
# quarantine count:
docker compose exec -T db psql -U postgres -d utility_tariff_finder \
  -c "SELECT count(*) FILTER (WHERE refresh_quarantined_at IS NOT NULL) FROM utilities;"
# recent runs:
docker compose exec -T db psql -U postgres -d utility_tariff_finder \
  -c "SELECT id, refresh_type, started_at::date, utilities_targeted, utilities_processed, errors, tariffs_added FROM refresh_runs ORDER BY id DESC LIMIT 5;"
```
Read-only DB access from a laptop: see `docs/DATABASE_ACCESS.md` (SSH tunnel).

---

## 8. Guardrails for agents (read before acting)

1. **Never push to `main` directly. Open a pull request** and let a human
   review. `main` is what deploys.
2. **Never commit secrets.** `.env`, API keys, `*.pem` are gitignored — keep
   it that way. Secrets live in the VM's environment, not the repo.
3. **Require explicit approval before any production-changing action**:
   running a refresh/campaign, `--migrate`, `--rebuild`, resizing/stopping the
   VM, bulk supersede/delete, or deactivating utilities. Read-only checks
   (health score, cost report, status queries) are fine to run freely.
4. **Migrations are additive and reversible.** New Alembic revision →
   `down_revision` = current head (`b9c0d1e2f3a4`) → test `upgrade` and
   `downgrade`. Never edit an applied migration.
5. **Preserve the live/superseded invariant** (§3). Soft-supersede, don't
   delete. Filter to live tariffs in any user-facing/metric query.
6. **After code changes, sync to the VM and restart the worker** or the
   running process keeps the old code (see §7).
7. **Long jobs go through `run-on-vm.sh`** (tmux), never a raw SSH command.
8. **Cost awareness**: extraction spends real LLM money. Prefer dry-run
   (`opus_yield_probe.py`, `run_pipeline(dry_run=True)`) to validate before a
   full run. Opus is the priciest tier — respect `OPUS_MAX_PER_UTILITY`.

---

## 9. Local development
```bash
# Backend
cd backend && python3.12 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env            # fill DB creds + API keys
alembic upgrade head
uvicorn app.main:app --reload

# Frontend
cd frontend && npm install && npm run dev

# Whole stack via Docker (from repo root)
docker compose up -d --build
docker compose exec api alembic upgrade head
```
Seed order: `seed_eia861` → `seed_canada` → `seed_openei` → `seed_territories`
→ `seed_monitoring_sources`.

---

## 10. Key env vars (names only — values live in the VM env / `.env`)
`DATABASE_URL`, `SYNC_DATABASE_URL`, `REDIS_URL`, `ADMIN_API_KEY`,
`CORS_ORIGINS`, `OPENEI_API_KEY`, `BRAVE_API_KEY`, `ANTHROPIC_API_KEY`,
`GOOGLE_AI_API_KEY`, `GOOGLE_CSE_API_KEY`, `GOOGLE_CSE_CX`,
`GOOGLE_MAPS_API_KEY`; model/cost: `OPUS_MODEL`, `HAIKU_MODEL`, `GEMINI_MODEL`,
`OPUS_MAX_PER_UTILITY`, `PHASE6_ENABLED`, `LLM_PRICING_JSON`,
`MONTHLY_MAX_UTILITIES`, `CELERY_CONCURRENCY`, `QUARANTINE_RECHECK_DAYS`; auth:
`AUTH_ENABLED`, `GOOGLE_OAUTH_CLIENT_ID/SECRET`, `AUTH_ALLOWED_EMAIL_DOMAIN`.

---

## 11. Pointers to deeper docs
- `docs/ARCHITECTURE.md` — system overview + diagrams.
- `docs/GCP.md`, `docs/GCP_FIRST_TIME.md` — VM deployment.
- `docs/DATABASE_ACCESS.md` — read-only DB access via SSH tunnel.
- `docs/CENTRALIZED_REGULATORS.md` — jurisdictions with centralized rate-setting.
- `README.md` — quick start + API endpoint list.
- `PROJECT_SUMMARY.md`, `TECHNICAL_REVIEW.md` — **historical** (2026-03/04); superseded by this file for anything about the refresh/quarantine/cost/model systems.
