# Google Cloud VM deployment (checklist)

Goal: run **docker compose** on a **Compute Engine VM** so the app is not tied to your laptop.

## Security: credentials

**Do not paste secret tokens, API keys, or service-account JSON into AI chat or email.**

Recommended patterns:

1. **You** create the VM and log in with SSH from your machine.
2. **You** create a `.env` file **on the server** (or use **Secret Manager** later). The app reads variables from there.
3. For **Google Cloud CLI** on your laptop: use normal Google login, not copying long-lived tokens into chat.

### Install `gcloud` and log in (your laptop)

```bash
# macOS (example)
brew install google-cloud-sdk
gcloud init
gcloud auth login
gcloud auth application-default login   # optional; for tools/SDKs that use ADC
```

This opens a browser window. You sign in with your Google account. **No token needs to be handed to an assistant.**

### Create the VM (console or CLI)

**Console:** Compute Engine → **Create instance** → Ubuntu LTS or Debian → allow HTTP/HTTPS (or add firewall rules for ports 80/443).

**Sizing (starting point):** e2-standard-4 (4 vCPU, 16 GB) if you want fast monitoring baselines; e2-standard-2 is OK for lighter traffic.

**Disk:** 50–100 GB balanced SSD.

### Firewall

- Allow **SSH (22)** from **your IP** only (not `0.0.0.0/0` if you can avoid it).  
- Allow **80** (and **443** when you enable TLS).

## On the VM: Docker

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl git
# Install Docker using Docker’s official instructions for your OS
```

Clone your project (or upload a release), then:

```bash
cd /path/to/Utility-Tariff-Finder
cp .env.docker.example .env
nano .env   # set passwords, GOOGLE_MAPS_API_KEY, CORS_ORIGINS, ADMIN_API_KEY (long random)
docker compose up -d --build
docker compose exec api alembic upgrade head
```

**Seed data:** follow the main `README.md` seed order, or restore a `pg_dump` from staging to avoid re-downloading everything.

## Admin API key

Set **`ADMIN_API_KEY`** in the API container environment to a long random string. All **`/api/admin/*`** routes (monitoring, URL updates, batch checks) then require:

- Header `X-Admin-Key: <your key>`, or  
- `Authorization: Bearer <your key>`

For the built-in Monitoring page in the React app, set **`VITE_ADMIN_API_KEY`** to the **same value** before `npm run build` / Docker web image build (only suitable if the UI is not fully public; prefer VPN or IP allowlisting for serious lock-down).

Agents (OpenClaw) and `curl` should use `X-Admin-Key` with the secret from the VM `.env`, never from chat.

## TLS (HTTPS)

The included `deploy/Caddyfile` uses **HTTP on port 80** for simplicity.

For production:

1. Point a **DNS A record** at the VM’s external IP.  
2. Edit `deploy/Caddyfile` to use your domain and enable automatic HTTPS (Caddy will obtain Let’s Encrypt certificates).  
3. Set `CORS_ORIGINS` to `https://your-domain`.

## Running a full monitoring baseline

On the VM (or any host with DB access):

```bash
docker compose exec api python -m scripts.run_monitoring --all --concurrency 32 --per-host 4 \
  --output-summary /tmp/monitoring-summary.json
```

Tune `--concurrency` / `--per-host` if sites rate-limit you.

## Optional: managed database

Instead of Postgres in Docker, you can use **Cloud SQL for PostgreSQL** with the **PostGIS** flag enabled. Point `DATABASE_URL` / `SYNC_DATABASE_URL` at the Cloud SQL connection string and keep **only** the API + web containers on the VM.

## What to tell an assistant (safe)

You **can** say: region, machine type, error messages from logs, **non-secret** config (e.g. “I use `e2-standard-4` in `us-central1`”).

Avoid sharing: **`.env` contents**, **private keys**, **database passwords**, **Google OAuth refresh tokens**.
