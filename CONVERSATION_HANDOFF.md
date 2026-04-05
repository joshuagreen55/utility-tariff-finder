# Conversation Handoff Summary

## Project Overview
**Utility Tariff Finder** — A web application that discovers, extracts, and stores electricity utility tariffs for US and Canadian utilities. Deployed on a Google Cloud VM (`utility-tariff-finder`, zone `us-central1-a`) using Docker Compose.

### Tech Stack
- **Backend**: Python, FastAPI, SQLAlchemy, PostgreSQL, Alembic
- **Frontend**: React, TypeScript, Vite
- **Pipeline**: Brave Search API + Anthropic Claude Haiku (LLM extraction) + Playwright (headless browser)
- **Deployment**: Docker Compose on GCP VM, Caddy for HTTPS, Google OAuth for auth
- **Domain**: Accessed via `.nip.io` domain with auto-provisioned Let's Encrypt SSL

---

## Current State of US Batch Processing

We're processing US utilities in **15 regional batches** (~80-140 utilities each), running them sequentially with review/cleanup between each batch.

### Completed Batches

| Batch | Region | States | Utilities | Success Rate | Final Tariffs | Notes |
|-------|--------|--------|-----------|-------------|---------------|-------|
| 1 | Pacific NW + Islands | WA, OR, AK, HI | 75 | 92% | ~1,551 | Had OOM crash mid-run, fixed with Playwright browser manager |
| 2 | California + Nevada | CA, NV | 99 | 53% | ~1,267 | Low success rate due to many CA energy marketers/CCAs with no tariff pages |
| 3 | Mountain West | AZ, CO, ID, MT, NM, UT, WY | 108 | 95% | ~1,193 | Cleanest batch so far |

### Remaining Batches (4-15)

| # | Region | States | Utilities |
|---|--------|--------|-----------|
| 4 | Texas | TX | 204 |
| 5 | Northern Plains | MN, ND, SD, NE | 116 |
| 6 | Heartland | IA, KS, MO, OK | 136 |
| 7 | South Central | AR, LA, MS | 90 |
| 8 | Deep South | AL, GA | 111 |
| 9 | Southeast | FL, NC, SC | 135 |
| 10 | Upper South | TN, KY, WV | 127 |
| 11 | Great Lakes | WI, MI | 109 |
| 12 | Ohio Valley | OH, IN, IL | 138 |
| 13 | Mid-Atlantic | PA, NJ, VA, MD, DE, DC | 98 |
| 14 | New York | NY | 72 |
| 15 | New England | MA, CT, ME, VT, NH, RI | 72 |

### Process for Each Batch
1. Start in tmux on VM: `tmux new-session -d -s batchN "cd /home/josh/utility-tariff-finder && docker compose exec -T api python3 -m scripts.us_batch_runner --batch N --comprehensive 2>&1 | tee /home/josh/batchN.log"`
2. Monitor: `gcloud compute ssh utility-tariff-finder --zone=us-central1-a --command='tail -30 /home/josh/batchN.log'`
3. After completion, run audit: `docker compose exec -T api python3 -m scripts.batch_audit --states XX,YY`
4. Run cleanup (dry run first): `docker compose exec -T api python3 -m scripts.cleanup_duplicate_tariffs --country US --states XX,YY --dry-run`
5. Commit cleanup: same without `--dry-run`
6. Deactivate non-retail entities (wholesale agencies, energy marketers, etc.)
7. Review failures, decide if pipeline tweaks needed before next batch

---

## Key Decisions Made

### Only Residential + Commercial Tariffs
- **LIGHTING and INDUSTRIAL tariffs are excluded** — deleted 7,137 from the database
- Pipeline `VALID_CLASSES` = `{"residential", "commercial"}` — LLM prompt and validation both enforce this
- `SKIP_KEYWORDS` regex blocks lighting/street light/industrial/large power at link-discovery level

### Non-Retail Utilities Deactivated
We deactivate entities that aren't retail electricity providers (wholesale agencies, generation companies, energy marketers, military bases, water districts, transit agencies). These consistently fail and waste processing time. So far ~21 deactivated across batches 1-3.

### Canada is Complete
- All Canadian provinces processed and committed to DB
- Cleanup script run for all of Canada
- Hydro-Quebec required special Playwright handling due to SSL issues

---

## Key Technical Fixes Applied

### OOM Prevention (Batch 1 crashed at utility 33/79)
- **Root cause**: Every Playwright call launched a new Chromium process
- **Fix 1**: `_PlaywrightManager` singleton — reuses one browser, auto-recycles after 25 contexts, auto-restarts if browser crashes
- **Fix 2**: `cleanup_between_utilities()` — shuts down Playwright + runs GC between each utility in batch loops
- **Fix 3**: API container memory bumped from 2GB to 4GB
- **Fix 4**: `--skip N` flag on `us_batch_runner.py` to resume after crashes
- **Fix 5**: Removed `--single-process` from Chromium args (caused browser fragility)

### Data Quality Pipeline
- `cleanup_duplicate_tariffs.py` — Two-pass cleanup: (1) remove tariffs with no core components, (2) merge prefix-duplicate names keeping the richer version
- Pipeline `_merge_prefix_duplicates` in phase3 catches duplicates during extraction
- `phase4_validate` requires at least one energy/fixed/demand component
- Quebec rate codes protected from over-aggressive merging (suffix must be 4+ chars)

---

## Key Files

### Pipeline & Processing
- `backend/scripts/tariff_pipeline.py` — Core pipeline (phases 1-4 + additional tariff search)
- `backend/scripts/us_batch_runner.py` — Batch runner with `--batch N`, `--comprehensive`, `--skip N`, `--dry-run`
- `backend/scripts/cleanup_duplicate_tariffs.py` — Post-processing deduplication
- `backend/scripts/batch_audit.py` — Data quality audit per batch
- `backend/scripts/check_failures.py` — Inspect failed utilities

### Application
- `backend/app/api/routes/tariffs.py` — Tariff API endpoints
- `backend/app/models/tariff.py` — Tariff + RateComponent SQLAlchemy models
- `frontend/src/pages/LookupPage.tsx` — Address lookup wizard (Mysa-style UI)
- `frontend/src/pages/TariffBrowserPage.tsx` — Filterable tariff list with delete capability
- `frontend/src/pages/TariffDetailPage.tsx` — Tariff detail view (groups by season)

### Deployment
- `docker-compose.yml` — Full stack (db, redis, api, web)
- `deploy/Caddyfile` — HTTPS reverse proxy config
- VM: `utility-tariff-finder` in `us-central1-a`
- SSH: `gcloud compute ssh utility-tariff-finder --zone=us-central1-a`
- Deploy files: `gcloud compute scp <file> utility-tariff-finder:/home/josh/utility-tariff-finder/<file> --zone=us-central1-a`
- Rebuild API: `docker compose build --no-cache api && docker compose up -d api`
- Rebuild frontend: `docker compose build --no-cache web && docker compose up -d web`

---

## Database Current State (as of Batch 3 completion)

- **~12,000+ tariffs** total (residential + commercial only)
- **Canada**: Complete — all provinces processed
- **US Batches 1-3**: WA, OR, AK, HI, CA, NV, AZ, CO, ID, MT, NM, UT, WY
- **US Batches 4-15**: Not yet processed (many have pre-existing tariff data from EIA import but need pipeline enrichment)

---

## Immediate Next Steps

1. **Deactivate 3 non-retail entities from Batch 3**: Platte River Power Authority (CO), Upper Missouri G&T El Coop (MT), Strawberry Water Users Assn (UT)
2. **Start Batch 4 (Texas)**: 204 utilities — largest batch
3. Continue through batches 5-15, reviewing and cleaning after each
4. After all batches complete: final comprehensive data quality review

---

## Useful Commands

```bash
# SSH to VM
gcloud compute ssh utility-tariff-finder --zone=us-central1-a

# Check running tmux sessions
gcloud compute ssh utility-tariff-finder --zone=us-central1-a --command='tmux ls'

# Start a batch in tmux (example: batch 4)
gcloud compute ssh utility-tariff-finder --zone=us-central1-a --command='
tmux new-session -d -s batch4 "cd /home/josh/utility-tariff-finder && docker compose exec -T api python3 -m scripts.us_batch_runner --batch 4 --comprehensive 2>&1 | tee /home/josh/batch4.log"
'

# Monitor a running batch
gcloud compute ssh utility-tariff-finder --zone=us-central1-a --command='grep -E "^\[" /home/josh/batch4.log | tail -5; echo "---"; tail -5 /home/josh/batch4.log'

# Resume a crashed batch (skip first N completed utilities)
gcloud compute ssh utility-tariff-finder --zone=us-central1-a --command='
tmux new-session -d -s batch4 "cd /home/josh/utility-tariff-finder && docker compose exec -T api python3 -m scripts.us_batch_runner --batch 4 --comprehensive --skip 50 2>&1 | tee /home/josh/batch4_resume.log"
'

# Run audit after batch completes
gcloud compute ssh utility-tariff-finder --zone=us-central1-a --command='cd /home/josh/utility-tariff-finder && docker compose exec -T api python3 -m scripts.batch_audit --states TX 2>&1'

# Run cleanup (dry run first, then without --dry-run)
gcloud compute ssh utility-tariff-finder --zone=us-central1-a --command='cd /home/josh/utility-tariff-finder && docker compose exec -T api python3 -m scripts.cleanup_duplicate_tariffs --country US --states TX --dry-run 2>&1'

# List all batches with current status
gcloud compute ssh utility-tariff-finder --zone=us-central1-a --command='cd /home/josh/utility-tariff-finder && docker compose exec -T api python3 -m scripts.us_batch_runner --list-batches 2>&1'

# Copy updated file to VM and into running container
gcloud compute scp backend/scripts/somefile.py utility-tariff-finder:/home/josh/utility-tariff-finder/backend/scripts/somefile.py --zone=us-central1-a
gcloud compute ssh utility-tariff-finder --zone=us-central1-a --command='cd /home/josh/utility-tariff-finder && docker compose cp backend/scripts/somefile.py api:/app/scripts/somefile.py'
```
