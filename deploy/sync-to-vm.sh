#!/usr/bin/env bash
# Sync local backend code to the VM and hot-deploy into running containers.
#
# Usage (from project root):
#   ./deploy/sync-to-vm.sh                  # sync all backend code
#   ./deploy/sync-to-vm.sh --migrate        # also run alembic upgrade head
#   ./deploy/sync-to-vm.sh --restart        # also restart api + celery-worker
#   ./deploy/sync-to-vm.sh --rebuild        # full docker compose build + up
#
# Requirements:
#   - gcloud CLI authenticated and in PATH
#   - VM name and zone set below (or via env vars)

set -euo pipefail

VM_NAME="${VM_NAME:-utility-tariff-finder}"
VM_ZONE="${VM_ZONE:-us-central1-a}"
REMOTE_DIR="/home/josh/utility-tariff-finder"

DO_MIGRATE=false
DO_RESTART=false
DO_REBUILD=false

for arg in "$@"; do
  case "$arg" in
    --migrate) DO_MIGRATE=true ;;
    --restart) DO_RESTART=true ;;
    --rebuild) DO_REBUILD=true ;;
    --help|-h)
      echo "Usage: $0 [--migrate] [--restart] [--rebuild]"
      echo "  --migrate   Run alembic upgrade head after syncing"
      echo "  --restart   Restart api + celery-worker containers"
      echo "  --rebuild   Full docker compose build + up -d"
      exit 0
      ;;
    *) echo "Unknown arg: $arg"; exit 1 ;;
  esac
done

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== Sync backend code to VM ==="
echo "  Local:  $PROJECT_ROOT/backend/"
echo "  Remote: $VM_NAME:$REMOTE_DIR/backend/"

# Step 1: Sync backend source and docker-compose.yml to VM
# With bind mounts in docker-compose.yml, syncing to the VM directory
# automatically makes the code available inside running containers.
echo "  Syncing backend directories..."
for dir in app scripts alembic tests; do
  gcloud compute scp --recurse --zone="$VM_ZONE" --quiet --compress \
    "$PROJECT_ROOT/backend/$dir" \
    "$VM_NAME:$REMOTE_DIR/backend/"
done

echo "  Syncing config files..."
gcloud compute scp --zone="$VM_ZONE" --quiet \
  "$PROJECT_ROOT/backend/alembic.ini" \
  "$PROJECT_ROOT/backend/requirements.txt" \
  "$PROJECT_ROOT/docker-compose.yml" \
  "$VM_NAME:$REMOTE_DIR/"

# Move files that landed in the wrong place
gcloud compute ssh "$VM_NAME" --zone="$VM_ZONE" --quiet --command="
  cd $REMOTE_DIR && \
  mv -f alembic.ini backend/alembic.ini 2>/dev/null; \
  mv -f requirements.txt backend/requirements.txt 2>/dev/null; \
  true
"

echo "  Files synced to VM."
echo ""
echo "  With bind mounts in docker-compose.yml, code changes are"
echo "  immediately visible inside running api + celery-worker containers."
echo "  (A container restart is needed only for dependency changes.)"

# Step 3: Optional migration
if $DO_MIGRATE; then
  echo ""
  echo "=== Running alembic upgrade head ==="
  gcloud compute ssh "$VM_NAME" --zone="$VM_ZONE" --quiet --command="
    cd $REMOTE_DIR && docker compose exec -T api alembic upgrade head
  "
fi

# Step 4: Optional restart
if $DO_RESTART; then
  echo ""
  echo "=== Restarting containers ==="
  gcloud compute ssh "$VM_NAME" --zone="$VM_ZONE" --quiet --command="
    cd $REMOTE_DIR && docker compose restart api celery-worker
  "
fi

# Step 5: Optional full rebuild
if $DO_REBUILD; then
  echo ""
  echo "=== Full rebuild ==="
  gcloud compute ssh "$VM_NAME" --zone="$VM_ZONE" --quiet --command="
    cd $REMOTE_DIR && docker compose up -d --build api celery-worker
  "
fi

echo ""
echo "=== Done ==="
echo "Verify with: gcloud compute ssh $VM_NAME --zone=$VM_ZONE --command='cd $REMOTE_DIR && docker compose exec -T api python -c \"print(\\\"Code deployed OK\\\")\"'"
