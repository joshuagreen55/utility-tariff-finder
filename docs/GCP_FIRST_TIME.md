# First-time Google Cloud VM deploy (click-by-click)

Your order: **(1) VM + app → (2) baseline → (3) OpenClaw**. This page is **(1)** in detail.

You do **not** need to memorize terminal commands—copy each block when the step says to.

**Never used SSH?** You don’t need the `ssh` command on your Mac. Use the Console’s **SSH** button for a browser terminal, then edit `.env` with `nano` — see **[BROWSER_SSH_NO_CLI.md](BROWSER_SSH_NO_CLI.md)**.

---

## A. Create the VM (Google Cloud Console)

1. Open [Google Cloud Console](https://console.cloud.google.com/) and pick your project.
2. Left menu → **Compute Engine** → **VM instances** → **Create instance**.
3. Suggested settings:
   - **Name:** `utility-tariff-finder` (any name is fine)
   - **Region:** closest to you (e.g. `us-central1`)
   - **Machine type:** **e2-standard-4** (4 vCPU, 16 GB) — good for a fast monitoring baseline
   - **Boot disk:** Ubuntu **22.04 LTS** or **24.04 LTS**, **50–100 GB** balanced SSD
   - **Firewall:** check **Allow HTTP traffic** and **Allow HTTPS traffic** (we use port 80 first; HTTPS later with Caddy + domain)
4. Click **Create** and wait until status shows a green check.

**External IP:** On the instances list, note **External IP** (e.g. `34.x.x.x`). You will open `http://THAT_IP` in a browser after deploy.

### Fresh projects only: if the site “times out” in the browser

Some new GCP projects **do not** create `default-allow-http` / `default-allow-https` automatically. Your VM must have tags **`http-server`** and **`https-server`**, and these rules must exist (VPC network → **Firewall**):

- **tcp:80** from `0.0.0.0/0` → target tag **`http-server`**
- **tcp:443** from `0.0.0.0/0` → target tag **`https-server`**

Without them, Chrome shows **`ERR_CONNECTION_TIMED_OUT`**.

---

## B. Allow SSH from your home (recommended)

1. **VPC network** → **Firewall** → **Create firewall rule**
2. Name: `allow-ssh-from-home`
3. Targets: **All instances in the network** (or a tagged target if you use tags)
4. Source IP ranges: **your home’s public IP/32** (search “what is my IP” in a browser)
5. Protocols: **tcp:22**
6. Create

(If you skip this, you can still use **“SSH”** button in the Console—it uses Google’s browser session.)

---

## C. Put the project on the VM

Pick **one** method.

### Option 1 — Git (if the repo is on GitHub/GitLab)

On the VM (SSH session):

```bash
sudo apt-get update && sudo apt-get install -y git
git clone YOUR_REPO_URL
cd YOUR_REPO_FOLDER
```

### Option 2 — Copy from your Mac (no Git)

On your **Mac** Terminal (replace IP and path):

```bash
export COPYFILE_DISABLE=1
scp -r "/path/to/Utility Tariff Finder" YOUR_LINUX_USER@YOUR_VM_IP:~/utility-tariff-finder
```

`COPYFILE_DISABLE=1` avoids macOS `._*` junk files that can break Alembic migrations on Linux.

On the VM:

```bash
cd ~/utility-tariff-finder
```

---

## D. Install Docker on the VM

On the VM:

```bash
chmod +x deploy/vm-bootstrap.sh
./deploy/vm-bootstrap.sh
```

**Important:** Log out of SSH and connect again (or run `newgrp docker`), then verify:

```bash
docker --version
docker compose version
```

---

## E. Configure secrets and start the stack

On the VM, inside the project folder (easiest: **[open SSH in the browser](BROWSER_SSH_NO_CLI.md)** — Compute Engine → VM → **SSH**):

```bash
cp .env.docker.example .env
nano .env
```

Set at least:

| Variable | What to put |
|----------|-------------|
| `ADMIN_API_KEY` | Long random string (same idea as on your laptop) |
| `VITE_ADMIN_API_KEY` | **Same value** as `ADMIN_API_KEY` |
| `VITE_GOOGLE_MAPS_API_KEY` | Your Google Maps key (for address autocomplete) |
| `CORS_ORIGINS` | `http://YOUR_VM_EXTERNAL_IP` and later `https://your-domain.com` |

Database lines can stay as in `.env.docker.example` for a first install (postgres/postgres internally). Change passwords later for hardening.

### Google sign-in (limit to @getmysa.com)

Optional: require **Google OAuth** before the UI or JSON API are usable (except `/api/health`).

**Google does not allow raw IP addresses** (e.g. `http://34.x.x.x`) as **Authorized JavaScript origins** or **Authorized redirect URIs** for a Web client—you’ll see errors like “must end with a public top-level domain”. You need a **hostname**:

- **Recommended:** A DNS name you control, e.g. `https://utilities.getmysa.com` → A record to the VM IP, TLS in Caddy, then use that **https** URL everywhere (OAuth console, `CORS_ORIGINS`, `PUBLIC_APP_URL`, `AUTH_GOOGLE_REDIRECT_URI`, and set `AUTH_COOKIE_SECURE=true`).
- **Quick try (nip.io):** Use the ready-made snippet **[`deploy/oauth-nipio.env.snippet`](../deploy/oauth-nipio.env.snippet)** — it pins **`http://34.63.25.32.nip.io`** (change the IP in that file + Google Console if your VM address changed). In Google’s OAuth Web client, set **Authorized JavaScript origins** and **redirect URI** to the exact lines in the snippet header, merge the variables into `.env` on the VM, then `docker compose up -d --build`. Always open the app at **`http://34.63.25.32.nip.io`**, not the raw IP, so cookies and OAuth match.

1. **APIs & Services** → **OAuth consent screen** — complete the wizard (Workspace **Internal** is simplest if everyone has a getmysa.com Google account).
2. **Credentials** → **Create credentials** → **OAuth client ID** → **Web application**.
3. **Authorized JavaScript origins:** your **hostname** with scheme (e.g. `https://utilities.getmysa.com` or `http://34.63.25.32.nip.io` — **not** a bare IP).
4. **Authorized redirect URIs:** same host + `/api/auth/google/callback` (must match `AUTH_GOOGLE_REDIRECT_URI` in `.env` exactly).
5. In `.env`, set the variables documented in `.env.docker.example` (`AUTH_ENABLED=true`, `GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`, `AUTH_GOOGLE_REDIRECT_URI`, `PUBLIC_APP_URL`, `AUTH_SESSION_SECRET`, `AUTH_ALLOWED_EMAIL_DOMAIN=getmysa.com`, and `AUTH_COOKIE_SECURE=false` for HTTP or `true` for HTTPS). Set **`CORS_ORIGINS`** to the **same origin** users type in the address bar (the hostname, not a bare IP if you use OAuth).
6. Restart with rebuild: `docker compose up -d --build`

The API enforces **verified `@getmysa.com`** emails on the server (the Google `hd=` hint alone is not enough). Server-side jobs can still use **`ADMIN_API_KEY`** on `/api/admin/*` without a browser session.

Start everything:

```bash
docker compose up -d --build
docker compose exec api alembic upgrade head
```

**First-time data:** Either run the seed scripts **inside the container** (same order as `README.md`), or restore a `pg_dump` you took from your laptop.

Example seeds (slow on first OpenEI run):

```bash
docker compose exec api python -m scripts.seed_eia861
docker compose exec api python -m scripts.seed_canada
docker compose exec api python -m scripts.seed_openei
docker compose exec api python -m scripts.seed_territories
docker compose exec api python -m scripts.seed_monitoring_sources
```

Open in your browser: **`http://YOUR_VM_EXTERNAL_IP`** (port 80).

---

## F. Next steps (you already planned)

2. **Baseline on the VM:** [../deploy/run-baseline-vm.sh](../deploy/run-baseline-vm.sh) — see [OPENCLAW_VM.md](../deploy/OPENCLAW_VM.md) for the output path.  
3. **OpenClaw on the VM:** [../deploy/OPENCLAW_VM.md](../deploy/OPENCLAW_VM.md)

More reference: [GCP.md](GCP.md), [NEXT_STEPS.md](NEXT_STEPS.md)
