#!/usr/bin/env bash
# Resize the production VM's machine type, then bring the stack back up.
#
# A machine-type change requires the instance to be TERMINATED, so this
# does: stop -> set-machine-type -> start -> wait for SSH -> docker compose
# up -d -> wait for the API healthcheck. The persistent disk (and the
# Postgres pgdata volume on it) is preserved across stop/start.
#
# Usage (from project root):
#   ./deploy/vm-resize.sh e2-standard-2      # downsize for steady-state serving
#   ./deploy/vm-resize.sh e2-standard-4      # scale up for a large backfill
#   ./deploy/vm-resize.sh --status           # show current machine type
#
# Cost reference (us-central1, on-demand, approx/mo):
#   e2-medium     2 vCPU / 4GB   ~$24    (worker must run CELERY_CONCURRENCY=1)
#   e2-standard-2 2 vCPU / 8GB   ~$49    (balanced: serving + concurrency 3)
#   e2-standard-4 4 vCPU / 16GB  ~$98    (large backfills, concurrency 4+)

set -euo pipefail

VM_NAME="${VM_NAME:-utility-tariff-finder}"
VM_ZONE="${VM_ZONE:-us-central1-a}"
REMOTE_DIR="/home/josh/utility-tariff-finder"

ssh_cmd() {
  gcloud compute ssh "$VM_NAME" --zone="$VM_ZONE" --quiet --command="$1"
}

current_type() {
  gcloud compute instances describe "$VM_NAME" --zone="$VM_ZONE" \
    --format="value(machineType.scope(machineTypes))"
}

if [[ "${1:-}" == "--status" ]]; then
  echo "Instance: $VM_NAME ($VM_ZONE)"
  echo "Machine type: $(current_type)"
  echo "Status: $(gcloud compute instances describe "$VM_NAME" --zone="$VM_ZONE" --format='value(status)')"
  exit 0
fi

TARGET_TYPE="${1:-}"
if [[ -z "$TARGET_TYPE" ]]; then
  echo "Error: machine type required (e.g. e2-standard-2). Use --status to inspect." >&2
  exit 1
fi

CURRENT=$(current_type)
echo "=== VM resize: $VM_NAME ==="
echo "  Current: $CURRENT"
echo "  Target:  $TARGET_TYPE"
if [[ "$CURRENT" == "$TARGET_TYPE" ]]; then
  echo "  Already $TARGET_TYPE — ensuring stack is up."
else
  echo ""
  echo "  Stopping instance (brief downtime)..."
  gcloud compute instances stop "$VM_NAME" --zone="$VM_ZONE" --quiet

  echo "  Setting machine type to $TARGET_TYPE..."
  gcloud compute instances set-machine-type "$VM_NAME" --zone="$VM_ZONE" \
    --machine-type="$TARGET_TYPE" --quiet

  echo "  Starting instance..."
  gcloud compute instances start "$VM_NAME" --zone="$VM_ZONE" --quiet
fi

echo "  Waiting for SSH..."
for i in $(seq 1 30); do
  if ssh_cmd "true" >/dev/null 2>&1; then break; fi
  sleep 5
done

echo "  Bringing up docker stack..."
ssh_cmd "cd $REMOTE_DIR && docker compose up -d"

echo "  Waiting for API health..."
HEALTHY=0
for i in $(seq 1 24); do
  if ssh_cmd "curl -fs http://127.0.0.1:8000/api/health >/dev/null 2>&1"; then
    HEALTHY=1
    break
  fi
  sleep 5
done

echo ""
echo "  Machine type now: $(current_type)"
if [[ "$HEALTHY" == "1" ]]; then
  echo "  API healthy. Done."
else
  echo "  WARNING: API not healthy after ~2 min. Check: ./deploy/run-on-vm.sh --status" >&2
  exit 1
fi
