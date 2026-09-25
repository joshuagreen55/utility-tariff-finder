# AGENTS.md — operating manual for the Utility Tariff Finder

This file is the **system-of-record for AI agents and new engineers**. Read it
before making changes or operating the system. It is intentionally
comprehensive; when in doubt, prefer what is written here over older docs
(`PROJECT_SUMMARY.md` and `TECHNICAL_REVIEW.md` predate most of the current
refresh/quarantine/cost/model systems).

_Last updated: 2026-09-25 (audit remediation, issue #11)._

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
    api/routes/       # FastAPI routers: lookup, utilities, tariffs, corrections, monitoring, auth
    models/           # SQLAlchemy models (see §3)
    services/         # geocoder, territory_lookup, monitor, monitoring_runner, google_oauth
    tasks/            # celery_app (beat schedule), monitoring, refresh
    config.py         # Pydantic Settings (all env vars)
    db/session.py     # async + sync engine singletons
    main.py           # FastAPI entrypoint
  alembic/versions/   # DB migrations (head = f3a4b5c6d7e8)
  scripts/            # the pipeline + seeds + campaigns + audits (see §5, §6)
  tests/              # unittest suite (+ DB-backed regressions when TEST_DATABASE_URL is set)
  tests/fixtures/     # ground_truth.json (benchmark via scripts/benchmark.py)
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
  `is_active`, `timezone` (IANA override; single-zone states/provinces are
  derived in `app/services/timezones.py`), `holiday_calendar` (code, not yet
  populated). **Refresh-quarantine bookkeeping**: `refresh_fail_streak`,
  `refresh_quarantined_at`, `refresh_last_reason`, `refresh_last_attempt_at`.
- **`tariffs`** (`tariff.py`) — `id`, `utility_id`, `name`, `code`,
  `customer_class` (residential/commercial/industrial/lighting), `rate_type`,
  `effective_date`, `end_date`, `source_url`, `last_verified_at`, `approved`,
  `confidence_score`, `confidence_factors` (JSONB — includes `needs_review`),
  `openei_id`, `raw_openei_data`. **Soft-supersede** (audit-preserving
  retirement): `superseded_by_tariff_id` (self-FK) + `supersede_reason` +
  `superseded_at` (stamped by a DB trigger for every writer; NULL on rows
  retired before 2026-09). Reasons written today: `refresh` (re-extraction
  changed the rates), `oeb_refresh`, `matcher`, `vintage`, `llm_absorb`,
  `dup_cleanup`, `dup_exact_name`, `dup_normalized`, `no_core_components`,
  `reconcile_missing` (no successor), `out_of_scope`, `manual` /
  `manual_retire` (correction API, DELETE), `agent_verify_accept`, plus
  reasons written by `quality_cleanup.py`. `source_document_hash` = sha256 of
  the source page's normalized text (`monitor.stable_text_hash`). TOU
  schedules stored as JSONB.
- **`rate_components`** (`tariff.py`) — `tariff_id`, `component_type`
  (energy/demand/fixed/minimum/adjustment), `unit`, `rate_value`
  (`Numeric(16,6)`), tiering + TOU period fields. **Structured TOU/season
  (preferred over label parsing):** `period_start_time` / `period_end_time`
  (`TIME`), `day_type` (`weekday`|`weekend`|`holiday`|`all`), and inclusive
  season calendar `season_start_month`/`season_start_day`/
  `season_end_month`/`season_end_day`. Keep `period_label` / `season` for
  display. See `docs/TOU_SEASONAL_FIELDS.md`. Never invent clock times or
  season dates from labels alone. `included_in_energy` marks an ADJUSTMENT
  row already folded into all-in ENERGY (kept for audit; consumers skip it).
  Cents units keep their period on conversion (`¢/day` → `$/day`).
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
- **`tariff_change_events`** (`tariff_change_event.py`) — **append-only**
  audit log (UPDATE/DELETE/TRUNCATE rejected by trigger): `decision`
  (`insert` | `supersede` | `retire` | `hold` | `hard_delete`), `reason`,
  `actor_type` (`pipeline` | `oeb` | `cleanup` | `script` | …), before/after
  tariff ids, source URL/hash, `idempotency_key`, `payload` (e.g. the
  proposal a `hold` refused). Tariff ids are plain ints (no FK). Any hard
  delete of a tariff appends a `hard_delete` event with a JSON snapshot of
  the row and its components.
- **`tariff_pins`** / **`tariff_verifications`** (`pin.py`) — document-scoped
  pins on manual / agent-verified rows, and the proposed refreshes an
  automated verifier accepts or holds. See
  `docs/TARIFF_CORRECTIONS_AND_PINS.md`.

### "Live" vs "superseded" — a critical invariant
A tariff is **live** only when `superseded_by_tariff_id IS NULL AND
supersede_reason IS NULL`. Every user-facing query, coverage count, and health
metric must filter to live tariffs. Do **not** hard-delete superseded rows —
they are the audit trail.

**Rate content is never edited in place.** A change to a live tariff's rates
is a *new* row plus a soft-supersede of the old one, so the prior components
stay queryable. Use `app.services.tariff_history.supersede_tariff()` (sets
the supersede columns and writes a change event) rather than assigning the
columns by hand. Identical re-extractions only touch `last_verified_at`.

**Protected rows** (`is_protected()`: `approved=True`, or
`confidence_factors` carrying `repair` / `manual` / curated `origin`) are
never overwritten, retired or collapsed by heuristic paths (LLM
re-extraction, reconciliation, dup cleanup, vintage collapse onto a scraped
sibling). Those paths log a `hold` change event instead. The OEB feed owns
only commodity ENERGY: it revises OEB/repair rows but carries every non-ENERGY
row (e.g. Hydro One FIXED delivery) forward, and holds manual rows.

**Manual corrections** arrive already approved (Mysa owns the second
check) via `POST /api/tariff-corrections`, authenticated only by
`TARIFF_CORRECTIONS_API_KEY` (not the admin key, not a session). They
soft-supersede (`manual`), pin the row to its evidence document and log a
change event; scraper refreshes then hold. A CHANGED signal on the pinned
document, a scrape from a different document, or the yearly re-check opens
a verification that an automated verifier accepts or holds — no human queue.
`DELETE /api/tariffs/{id}` soft-retires (`manual_retire`); it never hard-deletes.

Legacy scripts that still hard-delete (`purge_aggregator_contamination.py`,
`clean_corrupted.py`, `deactivate_non_retail.py`, component deletes in
`repair_vintage_tariffs.py`) are not the fix path for bad rates —
soft-supersede instead.

---

## 4. The extraction pipeline (`backend/scripts/tariff_pipeline.py`)

`run_pipeline(utility_id, ...)` runs up to six phases. It is the core engine;
one Celery `process_utility` task = one `run_pipeline` call for one utility.

| Phase | Function | What it does |
|------|----------|--------------|
| 1 | `phase1_find_rate_page` | Find the utility's rate page via Brave Search (+ domain discovery / Google CSE fallback). Hard-blocks third-party aggregators. |
| 2 | `phase2_discover_tariff_pages` | Crawl the rate page + one/two levels of sub-pages/PDFs into `RatePage` candidates. Skips aggregator/data domains (e.g. `eia.gov`) and regulator docket filings. |
| 3 | `phase3_extract_tariffs` | LLM structured extraction (tool-call), detail-page-first dedup, two-pass for complex pages. **3-tier model routing** (see below). |
| 4 | `phase4_validate` + `store_tariffs` | Validate against hand-set per-state bounds (hard-reject >3× p99, flag >p95 `needs_review`), normalize units, then persist soft-supersede-only (see §3): new row on changed rates, re-verify on identical rates, hold on protected rows; soft-supersede matching OpenEI seeds / older vintages; retire (never delete) rows missing from a ≥75%-coverage extraction of the same customer class. |
| 5 | `_phase5_smart_retry` | AI-guided nav fallback: load homepage, LLM picks nav links two levels deep, re-extract. |
| 6 | `phase6_deep_research` | Gemini Deep Research (Interactions API) for the long tail. Last-resort, cost- and token-guarded. Gated by `PHASE6_ENABLED`. |

### Model-tier routing (Phase 3 extraction)
`Gemini 3.8 Flash` (tier 1, cheap) → `Claude Haiku 4.5` (tier 2) → `Claude
Opus 5` (tier 3, last resort). Opus is only invoked when a page has numeric
rate signals AND the per-utility Opus budget isn't spent.

**Key model/cost env vars** (all overridable):
- `OPUS_MODEL` (default `claude-opus-5`) — tier-3 + long-doc identify, and
  `opus_audit.py` unless `AUDITOR_MODEL` is set.
- `HAIKU_MODEL` (default `claude-haiku-4-5-20251001`) — tier-2, vision, nav,
  two-pass extract, Track B, browser CLI.
- `GEMINI_MODEL` (default `gemini-3.8-flash`) — tier-1.
- `OPUS_MAX_PER_UTILITY` (default `2`) — cap on Opus escalations per utility
  per run (long-doc identify is not counted). Opus reportedly hit on ~8% of
  escalations while being ~70% of run cost (claimed; "hit" meant "returned
  anything" — judge it by `tier_acceptance` now), so this cap matters.
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
| (off by default) Pin verifications | `process_pin_verifications` | commented out in `celery_app.py` |

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

### Completeness helper (`app/services/tou_seasonal_completeness.py`)
Product rules on ENERGY rows using **structured** columns (not label regex):
1. TOU-family (`tou`/`tou_tiered`/`demand_tou`/`seasonal_tou`) — every ENERGY
   row must have `period_start_time` + `period_end_time` (and a rate).
2. Seasonal-family (`seasonal`/`seasonal_tiered`/`seasonal_tou`) — every ENERGY
   row must have inclusive `season_*` month/day fields (and a rate).
3. `seasonal_tou` needs both.

### Computable contract (`app/services/computable.py`, "completeness v2")
v1 above is necessary, not sufficient. `evaluate_computable()` decides
whether the structured rows can price **every** interval of the year: per
season × day type the TOU windows partition 24 h exactly once (`day_type`
required), seasons cover the year exactly once with valid dates, tiers run
contiguously from 0 to an open top, one ENERGY price per season otherwise.
Demand, TOU+tiered, complex, critical-peak/event and dynamic pricing are
`computable=false` by design. The API exposes `computable`,
`computable_reasons`, `computable_warnings`, `needs_review` on tariff
list/detail/browse and `computable_residential_tariff_count` on lookup.
Consumer rules: `docs/MYSA_CONSUMER_CONTRACT.md`. The health score reports a
computable lens but keeps it out of the composite (history comparability).

CI: `.github/workflows/backend-tests.yml` runs
`python -m scripts.check_tou_seasonal_completeness`. Optional Celery stub
`audit_tou_seasonal_completeness` exists but is not on beat by default.
**Do not invent times/dates** in extraction; flag incomplete shapes
`needs_review` + `confidence_factors.tou_seasonal_incomplete`.

### Health score (`scripts/health_score.py`)
Composite 0–100 score, weighted: Coverage 40% / Freshness 30% / Completeness
20% / Provenance 10%. Coverage counts a utility as covered only if it has a
**live, verified residential tariff with an energy component**. Freshness
decays as tariffs age past 90 days (that's why the score drifts down between
runs and recovers after them). Run it to get the current scorecard.

### LLM cost tracking (`scripts/llm_cost.py` + `llm_cost_report.py`)
Per-phase, per-model USD attribution from token counts. Recorded per utility,
merged into each `RefreshRun.summary_json.llm_cost`, and reported by
`llm_cost_report.py`. Yield: `tier_outcomes` (hit/miss = "returned
anything") plus **`tier_acceptance`** (tariffs per tier accepted after Phase 4
validation — the honest metric). Phase 6 aborts are priced from partial usage
(`aborts`), long-doc identify is its own phase (`phase3_identify`), and script
spend outside refresh runs (Track B, campaigns, `opus_audit`) goes to
`logs/llm_cost_ledger.jsonl`, which the report includes. The LLM cache key
includes the concrete model id, so env-only model swaps never replay another
model's output. Pricing lives in `DEFAULT_PRICING` (override via
`LLM_PRICING_JSON`); keep it in sync with real list prices. How to probe a
model change: `docs/LLM_MEASUREMENT.md`.

---

## 6. Campaign & maintenance scripts (`backend/scripts/`)
- `run_campaign.py` — unattended full stale-OpenEI cleanup in one session.
- `triage_seeds.py` — bucket stale OpenEI seeds into actionable categories.
- `supersede_via_llm.py` — "Track B": LLM 1:N absorption of stranded old
  tariffs into a fresh one (sets `supersede_reason='llm_absorb'`).
- `cleanup_duplicate_tariffs.py` / `dedup_tariffs.py` — retire duplicates via soft-supersede.
  `cleanup_duplicate_tariffs` also runs automatically after every refresh
  chord for the affected states; its keeper rule is protected → verified →
  most core/total components, and it never retires a protected row.
- `repair_hydro_one_oeb_residential.py` — Hydro One RPP TOU → OEB seasonal
  clocks (`period_*` / `season_*`); optional tiered/ULO if incomplete.
- `seed_*.py` — `seed_eia861`, `seed_canada`, `seed_openei`, `seed_territories`, `seed_monitoring_sources`.
- `opus_yield_probe.py` — live dry-run probe of extraction-tier yield for given utility IDs.
- `benchmark.py` — score live tariffs vs `tests/fixtures/ground_truth.json` (flat/tiered, 15%) and
  `ground_truth_tou_seasonal.json` (TOU/seasonal gold: exact values, clocks, season dates, computable).
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
   `down_revision` = current head (`f3a4b5c6d7e8`) → test `upgrade` and
   `downgrade`. Never edit an applied migration.
5. **Preserve the live/superseded invariant** (§3). Soft-supersede, don't
   delete, don't edit rate components in place. Filter to live tariffs in any
   user-facing/metric query.
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

# Backend tests. DB-backed regressions need a throwaway PostGIS server the
# role can CREATE DATABASE on (CI uses a postgis/postgis:16-3.4 service):
python -m unittest discover -s tests -v
TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres \
  python -m unittest discover -s tests -v

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
`GOOGLE_MAPS_API_KEY`, `TARIFF_CORRECTIONS_API_KEY`; pins: `PIN_VERIFY_DAILY_MAX`,
`PIN_VERIFIER`, `PIN_ARBITER`; model/cost: `OPUS_MODEL`, `HAIKU_MODEL`, `GEMINI_MODEL`,
`AUDITOR_MODEL`, `OPUS_MAX_PER_UTILITY`, `PHASE6_ENABLED`, `LLM_PRICING_JSON`,
`LLM_CACHE_LEGACY_READ`,
`MONTHLY_MAX_UTILITIES`, `CELERY_CONCURRENCY`, `QUARANTINE_RECHECK_DAYS`; auth:
`AUTH_ENABLED`, `GOOGLE_OAUTH_CLIENT_ID/SECRET`, `AUTH_ALLOWED_EMAIL_DOMAIN`.

---

## 11. Pointers to deeper docs
- `docs/ARCHITECTURE.md` — system overview + diagrams.
- `docs/GCP.md`, `docs/GCP_FIRST_TIME.md` — VM deployment.
- `docs/DATABASE_ACCESS.md` — read-only DB access via SSH tunnel.
- `docs/FLUX_MIGRATION_DISCOVERY.md` — open questions to answer before any data-plane move to Flux (no cutover).
- `docs/CENTRALIZED_REGULATORS.md` — jurisdictions with centralized rate-setting.
- `docs/TOU_SEASONAL_FIELDS.md` — structured TOU clock + season calendar columns.
- `docs/MYSA_CONSUMER_CONTRACT.md` — how machine consumers price intervals and build TOU schedules from `computable` tariffs.
- `docs/TARIFF_CORRECTIONS_AND_PINS.md` — correction API, document pins, automated re-verification.
- `docs/LLM_MEASUREMENT.md` — measuring extraction quality / spend before any model change.
- `README.md` — quick start + API endpoint list.
- `PROJECT_SUMMARY.md`, `TECHNICAL_REVIEW.md` — **historical** (2026-03/04); superseded by this file for anything about the refresh/quarantine/cost/model systems.
