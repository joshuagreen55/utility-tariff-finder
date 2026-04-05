#!/usr/bin/env bash
# Install and configure OpenClaw on the GCP VM for tariff URL remediation.
#
# Prerequisites:
#   - Ubuntu/Debian VM with the Docker Compose stack already running
#   - A Google Gemini API key (https://aistudio.google.com/apikey)
#   - The ADMIN_API_KEY value from .env
#
# Usage:
#   export GEMINI_API_KEY="your-gemini-api-key"
#   export ADMIN_API_KEY="your-admin-api-key"    # same as in .env
#   bash deploy/openclaw/setup-vm.sh

set -euo pipefail

GEMINI_API_KEY="${GEMINI_API_KEY:?Set GEMINI_API_KEY before running this script}"
ADMIN_API_KEY="${ADMIN_API_KEY:?Set ADMIN_API_KEY before running this script}"

PROJECT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
SKILL_SRC="${PROJECT_DIR}/deploy/openclaw/skills/tariff-remediation"
SKILL_DST="$HOME/.openclaw/skills/tariff-remediation"
ENV_FILE="$HOME/.config/utility-tariff.env"

echo "=== Step 1: Install Node.js 24 (if needed) ==="
if command -v node &>/dev/null; then
  NODE_MAJOR=$(node --version | grep -oP '(?<=v)\d+')
  if [ "$NODE_MAJOR" -ge 22 ]; then
    echo "Node.js $(node --version) found — OK"
  else
    echo "Node.js $(node --version) is too old (need 22+). Installing Node 24..."
    curl -fsSL https://deb.nodesource.com/setup_24.x | sudo -E bash -
    sudo apt-get install -y nodejs
  fi
else
  echo "Node.js not found. Installing Node 24..."
  curl -fsSL https://deb.nodesource.com/setup_24.x | sudo -E bash -
  sudo apt-get install -y nodejs
fi
echo "Node: $(node --version)  npm: $(npm --version)"

echo ""
echo "=== Step 2: Install OpenClaw ==="
if command -v openclaw &>/dev/null; then
  echo "OpenClaw already installed: $(openclaw --version 2>/dev/null || echo 'version unknown')"
  echo "Updating to latest..."
  curl -fsSL https://openclaw.ai/install.sh | bash || npm install -g @anthropic/openclaw
else
  echo "Installing OpenClaw..."
  curl -fsSL https://openclaw.ai/install.sh | bash || npm install -g @anthropic/openclaw
fi

echo ""
echo "=== Step 3: Non-interactive onboarding (Gemini) ==="
openclaw onboard --non-interactive \
  --mode local \
  --auth-choice gemini-api-key \
  --gemini-api-key "$GEMINI_API_KEY" \
  --gateway-port 18789 \
  --gateway-bind loopback \
  --install-daemon \
  --daemon-runtime node \
  --skip-skills

echo ""
echo "=== Step 4: Verify gateway ==="
sleep 3
openclaw gateway status || echo "Gateway may still be starting — check with 'openclaw gateway status' in a minute."

echo ""
echo "=== Step 5: Deploy tariff-remediation skill ==="
mkdir -p "$SKILL_DST"
cp -v "${SKILL_SRC}/SKILL.md"        "$SKILL_DST/"
cp -v "${SKILL_SRC}/fetch-errors.sh" "$SKILL_DST/"
cp -v "${SKILL_SRC}/patch-source.sh" "$SKILL_DST/"
chmod +x "$SKILL_DST/fetch-errors.sh" "$SKILL_DST/patch-source.sh"
echo "Skill deployed to ${SKILL_DST}"

echo ""
echo "=== Step 6: Create environment file ==="
mkdir -p "$(dirname "$ENV_FILE")"
cat > "$ENV_FILE" <<ENVEOF
UTILITY_TARIFF_API_BASE=http://127.0.0.1:8000
UTILITY_TARIFF_ADMIN_KEY=${ADMIN_API_KEY}
ENVEOF
chmod 600 "$ENV_FILE"
echo "Environment file written to ${ENV_FILE}"

echo ""
echo "=== Step 7: Create logs directory ==="
mkdir -p "${PROJECT_DIR}/logs"

echo ""
echo "=== Step 8: Configure exec tool allowlist ==="
OPENCLAW_CONFIG="$HOME/.openclaw/openclaw.json"
if [ -f "$OPENCLAW_CONFIG" ]; then
  echo "openclaw.json already exists — adding exec tool config..."
  python3 -c "
import json, sys
cfg_path = '$OPENCLAW_CONFIG'
with open(cfg_path) as f:
    cfg = json.load(f)
cfg.setdefault('tools', {})
cfg['tools']['exec'] = {
    'shell': '/bin/bash',
    'allowCommands': ['curl', 'python3', 'cat', 'echo', 'date', 'head', 'tail', 'grep', 'wc', 'jq'],
    'env': {
        'UTILITY_TARIFF_API_BASE': 'http://127.0.0.1:8000',
        'UTILITY_TARIFF_ADMIN_KEY': '$ADMIN_API_KEY'
    }
}
with open(cfg_path, 'w') as f:
    json.dump(cfg, f, indent=2)
print('exec tool config updated in ' + cfg_path)
"
else
  echo "Creating openclaw.json with exec tool config..."
  mkdir -p "$(dirname "$OPENCLAW_CONFIG")"
  cat > "$OPENCLAW_CONFIG" <<CFGEOF
{
  "tools": {
    "exec": {
      "shell": "/bin/bash",
      "allowCommands": ["curl", "python3", "cat", "echo", "date", "head", "tail", "grep", "wc", "jq"],
      "env": {
        "UTILITY_TARIFF_API_BASE": "http://127.0.0.1:8000",
        "UTILITY_TARIFF_ADMIN_KEY": "${ADMIN_API_KEY}"
      }
    }
  }
}
CFGEOF
  echo "Created ${OPENCLAW_CONFIG}"
fi

echo ""
echo "=== Step 9: Add weekly cron job ==="
openclaw cron add \
  --name "weekly-tariff-remediation" \
  --cron "0 6 * * 1" \
  --message "Run the tariff-remediation skill: fetch all errored monitoring sources and fix their URLs using web search. Process up to 50 sources. Log every action to logs/agent-audit.log." \
  --no-deliver \
  2>/dev/null || echo "Cron job may already exist or cron feature not available — you can add it manually later."

echo ""
echo "============================================"
echo "  OpenClaw setup complete!"
echo "============================================"
echo ""
echo "Quick test:"
echo "  openclaw gateway status"
echo "  openclaw dashboard"
echo ""
echo "Run the agent manually:"
echo "  openclaw agent --message 'Use the tariff-remediation skill to fix 10 errored monitoring sources.'"
echo ""
echo "The weekly cron fires every Monday at 06:00 UTC."
echo "To run it now:  openclaw cron run --name weekly-tariff-remediation"
