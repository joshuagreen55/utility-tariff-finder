# Flux migration — discovery checklist (no cutover)

Questions to answer with the Flux / platform owners **before** any part of
the Utility Tariff Finder data plane moves off the GCP VM. This is a
checklist, not a runbook: GCP stays authoritative until a dual-run proves
parity. Source: audit #10 §8, plus facts verified in this repo.
`empoweredhomes/mysa-flux` was not readable from the agent that wrote this,
so everything Flux-side is an open question.

**Today:** Flux serves the SPA plus `tariff_finder_proxy.py`, which calls the
UTF API at `TARIFF_FINDER_API_BASE_URL` with an optional
`TARIFF_FINDER_API_KEY`. The UTF data plane (Postgres/PostGIS, Redis,
Celery worker + beat, API, Caddy) runs via Docker Compose on one GCP VM.

**Dependency order:** data plane (Postgres/PostGIS + backups) → workers,
beat, Redis → secrets and egress → dual-run parity → flip the proxy base URL
→ decommission GCP.

## 1. Postgres + PostGIS
As built: `postgis/postgis:16-3.4` with a 1 GB container limit and data in
the `pgdata` volume. `service_territories.geometry` is a `MULTIPOLYGON` with a
GiST index, and lookup uses `ST_Contains`. Migrations are Alembic; the
initial migration assumes the `postgis` extension already exists.

- [ ] Is PostGIS 3.x available on Flux Postgres? Which Postgres major? Can we `CREATE EXTENSION postgis`?
- [ ] Connection limits: the API uses async (asyncpg) and sync (psycopg2) engines; Celery prefork children each open sync connections.
- [ ] Triggers and plpgsql allowed? The tariff history relies on them: `superseded_at` stamping, append-only `tariff_change_events`, and the hard-delete snapshot.
- [ ] Can the app role be restricted to INSERT/SELECT on `tariff_change_events`, as defence in depth next to the trigger?
- [ ] Dump/restore rehearsal including PostGIS geometry and the trigger functions.

## 2. Backups / PITR
As built: **no backup job in this repo**. Whether GCP disk snapshots exist is unknown.

- [ ] Flux backup cadence, point-in-time recovery, retention, and who owns restores.
- [ ] Restore test: restore to a scratch database, run `alembic current`, then run `health_score.py` read-only.
- [ ] Take a pre-migration backup of the GCP `pgdata` volume, owned by the overseer.

## 3. Build reproducibility
As built: `backend/Dockerfile` is `python:3.12-slim-bookworm` + `tesseract-ocr` + `poppler-utils` + Playwright Chromium (`--with-deps`). Requirements are ranges. The DB/task stack is upper-bounded since #12: SQLAlchemy `<2.1` (2.1 broke fresh migrations), psycopg2 `<3`, alembic `<2`, celery `<6`, pydantic `<3`, fastapi `<1`.

- [ ] Will Flux build from a lockfile or pinned image? Capture `pip freeze` from the running prod image (read-only) as the lockfile candidate **before** any rebuild.
- [ ] Image size and base-image policy: is Chromium plus OCR tooling allowed in the worker image?
- [ ] CI parity: `.github/workflows/backend-tests.yml` already runs a fresh install plus `alembic upgrade head` against a `postgis/postgis:16-3.4` service. Can Flux CI do the same?

## 4. Celery, Redis, beat
As built:
- **Redis 7:** `maxmemory 512mb`, `volatile-lru`, 768 MB limit.
- **Chords:** `result_chord_join_timeout` 12 h, `result_expires` 7 days.
- **Per-utility lock:** Redis key `refresh:lock:utility:{id}` with a 1,900 s TTL. It is shared with the correction API (#14).
- **Beat heartbeat:** key `beat:heartbeat`, read by the beat container healthcheck. The `autoheal` sidecar (`docker.sock`) restarts a hung beat.
- **Worker:** prefork, concurrency `${CELERY_CONCURRENCY:-3}`, `max-tasks-per-child 25`, `process_utility` `time_limit=1800`, `rate_limit="8/m"` **per worker instance**.

- [ ] Managed Redis with eviction-policy control and persistence? Latency to workers?
- [ ] A singleton scheduling primitive for **exactly one** beat, and a replacement for autoheal. Avoid "cron calls HTTP": tasks run up to 30 min and chords take hours.
- [ ] Long-running worker processes allowed? Replica count? The per-instance rate limit multiplies with replicas. Graceful shutdown vs `acks_late` + `reject_on_worker_lost`.
- [ ] Beat entries shipped **off** today: `nightly-tou-seasonal-completeness` and `daily-pin-verifications`. Keep them off until owners enable them.

## 5. Resources
As built: API and worker 4 GB limits each, beat 256 MB, db 1 GB, redis 768 MB. Chromium uses about 300–500 MB per pipeline (claimed in a Compose comment). Large PDFs are guarded (>18 MB skipped, table extraction off above 4 MB) because of past OOMs.

- [ ] Memory and CPU limits, and whether auto-increase needs approval (claimed: memory auto-increase with approval). Ephemeral disk for PDFs/OCR?

## 6. Filesystem state
As built: `./logs` is bind-mounted and holds the LLM extraction cache (keyed by model id since #15), the PDF/OCR cache, the Brave cache, `health_score_history.jsonl` and `llm_cost_ledger.jsonl`.

- [ ] Persistent volume on Flux? If dropped, expect a one-time LLM/Brave/OCR cost spike (magnitude unknown) and lost cost/health history. Copy the ledger and history files.

## 7. Scrape egress
As built: Brave / Google CSE, Anthropic, Gemini (including Deep Research), OpenEI, and hundreds of utility domains via httpx and Playwright. The monitoring user agent is `UtilityTariffMonitor/0.1`.

- [ ] Outbound to arbitrary domains permitted? Static egress IP and reputation (Cloudflare / bot walls)? Bandwidth limits?

## 8. Secrets and credential scoping
Names only (values live in the VM `.env`): `DATABASE_URL`, `SYNC_DATABASE_URL`, `REDIS_URL`, `POSTGRES_PASSWORD`, `REDIS_PASSWORD`, `ADMIN_API_KEY`, `TARIFF_CORRECTIONS_API_KEY`, `ANTHROPIC_API_KEY`, `GOOGLE_AI_API_KEY`, `BRAVE_API_KEY`, `GOOGLE_CSE_API_KEY`, `GOOGLE_CSE_CX`, `OPENEI_API_KEY`, `GOOGLE_MAPS_API_KEY`, `AUTH_*`, `GOOGLE_OAUTH_*`, model/cost vars (`OPUS_MODEL`, `HAIKU_MODEL`, `GEMINI_MODEL`, `AUDITOR_MODEL`, `OPUS_MAX_PER_UTILITY`, `PHASE6_*`, `LLM_PRICING_JSON`, `MONTHLY_MAX_UTILITIES`, `QUARANTINE_RECHECK_DAYS`, `PIN_*`).

- [ ] Flux secret store, rotation, and per-service scoping (API vs worker vs beat).
- [ ] The Flux read proxy holds `ADMIN_API_KEY`, which unlocks every `/api/*` route including admin writes. Since #14, DELETE only soft-retires and rate corrections need the separate `TARIFF_CORRECTIONS_API_KEY`. Still: give the proxy a **read-only** credential, or restrict it to GET, and check whether `tariff_finder_proxy.py` already restricts methods and paths.
- [ ] `TARIFF_CORRECTIONS_API_KEY` goes only to the Mysa cloud service that pushes approved corrections, never to the proxy.

## 9. Ops model
As built: bind-mount hot sync (`deploy/sync-to-vm.sh`), long scripts via `deploy/run-on-vm.sh` (tmux inside the api container), and read-only DB access over an SSH tunnel (`docs/DATABASE_ACCESS.md`).

- [ ] Image-only deploys? Where do long one-off jobs run (campaigns, repairs, `opus_audit`, benchmark)? What is the read-only DB access path?
- [ ] How are `alembic upgrade head` and the post-deploy worker restart sequenced?

## 10. Auth / UI
As built: Google OAuth with a redirect URI on the current host, cookie flags (`AUTH_COOKIE_SECURE`), CORS origins, and the UTF admin SPA served by Caddy.

- [ ] Does the UTF admin SPA move to Flux or stay behind the API? New OAuth redirect URIs, cookie domain and CORS.

## 11. Cost
As built: GCP VM (claimed e2-standard-2) plus LLM spend (claimed ~$2k/yr ceiling). There is no in-repo budget enforcement beyond per-utility caps, `MONTHLY_MAX_UTILITIES` and `PIN_VERIFY_DAILY_MAX`.

- [ ] Flux hosting cost model and approver for resource increases.

## 12. Dual-run and parity (gate for the proxy flip)
- [ ] Restore a GCP dump into Flux and run the same `alembic` head on both.
- [ ] Parity checks, all read-only:
  - `GET /api/lookup` on a fixed address set, comparing matched utilities and `computable_residential_tariff_count`.
  - Live tariff counts per utility.
  - `health_score.py --json` on both (including the computable lens).
  - `benchmark.py` on both fixtures.
- [ ] Decide which side runs beat during dual-run. **Never both**: two beats would double-dispatch refreshes and double-spend LLM budget.
- [ ] Rollback plan: point the proxy base URL back at GCP.

Out of scope here: Compose changes, the cutover itself, GCP teardown.
