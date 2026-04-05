# OpenClaw on the GCP VM

**Goal:** After the monitoring baseline and deterministic remediation run, the OpenClaw AI agent researches replacement URLs for still-broken sources, patches them via the admin API, and re-checks them — all automatically.

---

## Architecture

```
weekly-monitoring.sh
  ├─ Step 1: Baseline check (docker compose exec api → run_monitoring)
  ├─ Step 2: Deterministic remediation (remediate_urls.py — HTTP/HTTPS, www, common paths)
  └─ Step 3: OpenClaw agent (LLM + web search for remaining errors)
                │
                ├─ Reads errors from admin API (/api/admin/monitoring/sources?status=error)
                ├─ Searches the web for each utility's current tariff page
                ├─ PATCHes the source URL via admin API
                ├─ Re-checks the source to confirm it loads
                └─ Logs every action to logs/agent-audit.log
```

The API is exposed on `127.0.0.1:8000` (loopback only) so OpenClaw and scripts can call it without going through Caddy. Admin calls require the `X-Admin-Key` header.

---

## Quick Start (automated)

SSH into the VM and run:

```bash
cd ~/utility-tariff-finder

# Set your keys (same ADMIN_API_KEY as in .env)
export GEMINI_API_KEY="your-gemini-api-key"
export ADMIN_API_KEY="your-admin-api-key"

# Run the setup script
bash deploy/openclaw/setup-vm.sh
```

This installs Node.js 24, OpenClaw, runs non-interactive onboarding with Gemini, deploys the tariff-remediation skill, creates the env file, configures the exec tool, and adds a weekly cron job.

---

## Manual Setup (step by step)

### 1. Install Node.js 24

```bash
curl -fsSL https://deb.nodesource.com/setup_24.x | sudo -E bash -
sudo apt-get install -y nodejs
node --version   # should be v24.x
```

### 2. Install OpenClaw

```bash
curl -fsSL https://openclaw.ai/install.sh | bash
```

### 3. Run onboarding

```bash
openclaw onboard --non-interactive \
  --mode local \
  --auth-choice gemini-api-key \
  --gemini-api-key "$GEMINI_API_KEY" \
  --gateway-port 18789 \
  --gateway-bind loopback \
  --install-daemon \
  --daemon-runtime node \
  --skip-skills
```

Verify: `openclaw gateway status` — should show the gateway listening on port 18789.

### 4. Deploy the skill

```bash
mkdir -p ~/.openclaw/skills/tariff-remediation
cp deploy/openclaw/skills/tariff-remediation/* ~/.openclaw/skills/tariff-remediation/
chmod +x ~/.openclaw/skills/tariff-remediation/*.sh
```

### 5. Create the environment file

```bash
mkdir -p ~/.config
cat > ~/.config/utility-tariff.env <<EOF
UTILITY_TARIFF_API_BASE=http://127.0.0.1:8000
UTILITY_TARIFF_ADMIN_KEY=your-admin-api-key
EOF
chmod 600 ~/.config/utility-tariff.env
```

### 6. Add the weekly cron job

```bash
openclaw cron add \
  --name "weekly-tariff-remediation" \
  --cron "0 6 * * 1" \
  --message "Run the tariff-remediation skill: fetch all errored monitoring sources and fix their URLs using web search. Process up to 50 sources. Log every action to logs/agent-audit.log." \
  --no-deliver
```

---

## Usage

### Run the full weekly pipeline

```bash
cd ~/utility-tariff-finder
./deploy/weekly-monitoring.sh
```

This runs all three steps: baseline, deterministic fixes, then OpenClaw agent.

### Run only the OpenClaw agent

```bash
openclaw agent --message "Use the tariff-remediation skill to fix 20 errored monitoring sources."
```

### Run the weekly cron job immediately

```bash
openclaw cron run --name weekly-tariff-remediation
```

### Check agent status

```bash
openclaw gateway status
openclaw dashboard     # opens the web UI
```

### View audit trail

```bash
tail -50 ~/utility-tariff-finder/logs/agent-audit.log
```

---

## Skill Files

All skill files live in `deploy/openclaw/skills/tariff-remediation/` in the repo and get copied to `~/.openclaw/skills/tariff-remediation/` on the VM.

| File | Purpose |
|------|---------|
| `SKILL.md` | Instructions the LLM reads — describes the workflow, guardrails, and API reference |
| `fetch-errors.sh` | Calls admin API to list errored sources as JSON |
| `patch-source.sh` | PATCHes a source URL and re-checks it |

The agent uses OpenClaw's built-in **web search** tool (Brave/Tavily) to find replacement URLs, and the **exec** tool to run the helper scripts.

---

## API Reference (for the agent)

Base URL: `http://127.0.0.1:8000`  
Auth: `X-Admin-Key: <your-admin-api-key>`

| Action | Method | Path |
|--------|--------|------|
| Health check | GET | `/api/health` |
| Monitoring stats | GET | `/api/admin/monitoring/stats` |
| List error sources | GET | `/api/admin/monitoring/sources?status=error&limit=50` |
| Update source URL | PATCH | `/api/admin/monitoring/sources/{id}` — body: `{"url":"..."}` |
| Re-check one source | POST | `/api/admin/monitoring/sources/{id}/check` |
| Re-check batch | POST | `/api/admin/monitoring/sources/check-ids?wait=true` — body: `{"source_ids":[...]}` |

---

## Switching LLM Providers

The setup script defaults to Gemini. To switch:

```bash
# Re-run onboarding with a different provider
openclaw onboard --non-interactive \
  --mode local \
  --auth-choice apiKey \
  --anthropic-api-key "$ANTHROPIC_API_KEY" \
  --gateway-port 18789 \
  --gateway-bind loopback \
  --install-daemon \
  --daemon-runtime node \
  --skip-skills
```

See `openclaw onboard --help` for all providers (OpenAI, Anthropic, Gemini, Ollama, custom).

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `openclaw: command not found` | Re-run install: `curl -fsSL https://openclaw.ai/install.sh \| bash` |
| Gateway not running | `openclaw gateway status` then `openclaw gateway start` |
| Agent can't reach API | Verify Docker stack is up: `docker compose ps`. Check `curl -sS http://127.0.0.1:8000/api/health` |
| Agent errors on exec | Check `~/.openclaw/openclaw.json` exec allowlist includes `curl` |
| Cron job not firing | `openclaw cron list` to verify. Gateway must be running for cron to work. |
