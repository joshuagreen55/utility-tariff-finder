#!/usr/bin/env bash
# Run once on a fresh Ubuntu/Debian Google Cloud VM (as your login user, with sudo).
# Installs Docker Engine + Compose plugin so you can use docker compose from the project folder.

set -euo pipefail

if ! command -v curl >/dev/null 2>&1; then
  sudo apt-get update
  sudo apt-get install -y curl ca-certificates git
fi

echo "Installing Docker (official convenience script)..."
curl -fsSL https://get.docker.com | sudo sh

echo "Adding $(whoami) to docker group (log out and back in if 'docker ps' says permission denied)..."
sudo usermod -aG docker "$(whoami)" || true

echo ""
echo "Done. Next:"
echo "  1) Log out of SSH and reconnect (or run: newgrp docker)"
echo "  2) Clone or copy this project into the VM"
echo "  3) Follow docs/GCP_FIRST_TIME.md"
