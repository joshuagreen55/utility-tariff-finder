# Utility Tariff Finder

Lookup electricity utility providers and rate tariffs by US or Canadian address.

> **Operating this system?** Read **[`AGENTS.md`](AGENTS.md)** first — it is the
> system-of-record and runbook (data model, the extraction pipeline, refresh &
> quarantine systems, health scoring, LLM cost tracking, VM deploy/ops, and
> guardrails). This README covers overview + local setup only.

**Other docs:** [Architecture](docs/ARCHITECTURE.md) · [Next steps](docs/NEXT_STEPS.md) · [Google Cloud (first VM)](docs/GCP_FIRST_TIME.md) · [Query the database](docs/DATABASE_ACCESS.md) · [OpenClaw](deploy/OPENCLAW_VM.md)

## Architecture

- **Backend**: Python/FastAPI + PostgreSQL/PostGIS
- **Frontend**: React + Vite + TypeScript
- **Task Queue**: Celery + Redis — weekly monitoring, monthly refresh, quarterly recovery
- **Extraction pipeline**: 6-phase LLM tariff extractor (`backend/scripts/tariff_pipeline.py`), 3-tier model routing (Gemini Flash → Claude Haiku → Claude Opus) + Gemini Deep Research fallback

## Data Sources

| Source | Coverage | Use |
|--------|----------|-----|
| EIA Form 861 | ~3,300 US utilities | Utility master list |
| OpenEI URDB | 62,600+ rate records | Tariff seed data |
| HIFLD | US utility territory polygons | Address-to-utility mapping |
| Provincial boards (OEB, etc.) | Canadian utilities | Manual curation |

## Quick Start

### Prerequisites

- Python 3.12+
- PostgreSQL 15+ with PostGIS extension
- Redis (for Celery task queue)
- Node.js 20+

### Backend Setup

```bash
cd backend
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Copy and configure environment
cp .env.example .env
# Edit .env with your database credentials

# Create database with PostGIS
psql -c "CREATE DATABASE utility_tariff_finder;"
psql -d utility_tariff_finder -c "CREATE EXTENSION postgis;"

# Run migrations
alembic upgrade head

# Seed data (in order)
python -m scripts.seed_eia861          # US utilities from EIA
python -m scripts.seed_canada          # Canadian utilities
python -m scripts.seed_openei          # Rate tariffs from OpenEI
python -m scripts.seed_territories     # Service territory polygons
python -m scripts.seed_monitoring_sources  # Monitoring URLs

# Start API server
uvicorn app.main:app --reload
```

### Frontend Setup

```bash
cd frontend
npm install
npm run dev
```

### Monitoring Worker

```bash
cd backend
celery -A app.tasks.celery_app worker -l info
celery -A app.tasks.celery_app beat -l info
```

### Monitoring batch (CLI, concurrent)

For a **full baseline** without Celery (recommended on a VM):

```bash
cd backend
source venv/bin/activate
python -m scripts.run_monitoring --all --concurrency 32 --per-host 4 \
  --output-summary ./logs/monitoring-summary.json
```

Tune `--concurrency` / `--per-host` if remote sites rate-limit you.

### Docker Compose (single VM)

From the **repo root**:

```bash
cp .env.docker.example .env   # edit DATABASE_URL if needed, add API keys, set CORS_ORIGINS for prod
docker compose up -d --build
docker compose exec api alembic upgrade head
# then run seeds (see above) or restore a database dump
```

- **UI + API**: open `http://localhost` (port mapped in `docker-compose.yml`, default **80**).
- Architecture overview: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- **Google Cloud VM**: [docs/GCP.md](docs/GCP.md) — use `gcloud auth login` on **your** machine; **do not** paste secret tokens into chat.

Production: set `CORS_ORIGINS` to your HTTPS UI origin and configure TLS in `deploy/Caddyfile`.

**Security:** Set `ADMIN_API_KEY` in the API environment so `/api/admin/*` accepts header `X-Admin-Key` (or `Authorization: Bearer …`) **or** a Google session cookie when `AUTH_ENABLED=true`. For Docker, set `VITE_ADMIN_API_KEY` in `.env` to match so the Monitoring UI still works without typing the key when the admin key is required. Leave both empty for local dev only.

Optional **Google sign-in** (`AUTH_ENABLED=true`, vars in `.env.docker.example`): gates all `/api/*` routes except `/api/health` and `/api/auth/*`. Only verified `@getmysa.com` (configurable via `AUTH_ALLOWED_EMAIL_DOMAIN`) accounts can obtain a session cookie.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/auth/me` | Auth status (`auth_enabled`, session / email) |
| GET | `/api/auth/google/login` | Start Google OAuth (redirect) |
| GET | `/api/auth/google/callback` | OAuth redirect target (Google → app) |
| POST | `/api/auth/logout` | Clear session cookie |
| GET | `/api/health` | Liveness probe |
| GET | `/api/lookup?address=...` | Find utilities serving an address |
| GET | `/api/utilities` | List/search utilities |
| GET | `/api/utilities/{id}` | Utility detail |
| GET | `/api/utilities/{id}/tariffs?customer_class=residential` | List tariffs |
| GET | `/api/tariffs/filters` | Distinct filter values (country/state/utility/class) |
| GET | `/api/tariffs/browse` | Paginated tariff browser |
| GET | `/api/tariffs/{id}` | Tariff detail with rate components |
| GET | `/api/tariffs/{id}/source` | Source verification info |
| DELETE | `/api/tariffs/{id}` | Delete a tariff (admin) |
| GET | `/api/admin/monitoring/sources` | Monitoring sources |
| GET | `/api/admin/monitoring/stats` | Monitoring dashboard counts |
| GET | `/api/admin/monitoring/logs` | Change detection logs |
| GET | `/api/admin/monitoring/dead-utilities` | Utilities with all-failing sources |
| GET | `/api/admin/monitoring/error-categories` | Categorized source failures |
| PATCH | `/api/admin/monitoring/sources/{id}` | Update monitored URL (for remediation) |
| POST | `/api/admin/monitoring/sources/{id}/check` | Run one check immediately |
| POST | `/api/admin/monitoring/sources/check-ids` | Batch check; `?wait=true` returns results |
| POST | `/api/admin/monitoring/check-all` | Queue many checks (background) |
| PATCH | `/api/admin/monitoring/logs/{id}` | Update review status |
| GET | `/api/admin/monitoring/refresh-runs` | List refresh runs |
| GET | `/api/admin/analytics/coverage` | Coverage by country/state |
| GET | `/api/admin/data-quality/overview` | Data-quality dashboard aggregates |
