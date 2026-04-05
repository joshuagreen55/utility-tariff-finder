# Next steps (checklist)

You are ready to **run a full monitoring baseline**, **lock down admin APIs**, and **deploy to Google Cloud** when you choose.

## Done in-repo

- Concurrent monitoring CLI + JSON summary for agents  
- Docker Compose (PostGIS, Redis, API, Caddy + React)  
- Admin API: update URL, batch check with `wait=true`  
- **Optional `ADMIN_API_KEY`** — set this before any public deployment  

## What you should do next (in order)

1. **Generate secrets (on your machine; do not paste into chat)**  
   - Long random `ADMIN_API_KEY` (e.g. `openssl rand -hex 32`).  
   - Put it in `backend/.env` as `ADMIN_API_KEY=...`  
   - Put the same value in `frontend/.env` as `VITE_ADMIN_API_KEY=...` so the Monitoring page works.  

2. **Run a full baseline locally or on the VM**  
   ```bash
   cd backend && source venv/bin/activate
   python -m scripts.run_monitoring --all --concurrency 32 --per-host 4 \
     --output-summary ./logs/monitoring-summary.json
   ```  
   Review `error_details` in the JSON for the agent / manual curation.  

3. **Prove Docker on your laptop (optional)**  
   ```bash
   cp .env.docker.example .env
   # edit .env: DATABASE_*, ADMIN_API_KEY, VITE_* build args if using compose build
   docker compose up -d --build
   docker compose exec api alembic upgrade head
   ```  

4. **Create a GCP VM (step-by-step)** — [GCP_FIRST_TIME.md](GCP_FIRST_TIME.md) (then [GCP.md](GCP.md) for extras).  

5. **Baseline on the VM** — `deploy/run-baseline-vm.sh` → `logs/monitoring-summary.json`.  

6. **OpenClaw on the VM** — [../deploy/OPENCLAW_VM.md](../deploy/OPENCLAW_VM.md).  

For architecture diagrams, see [ARCHITECTURE.md](ARCHITECTURE.md).
