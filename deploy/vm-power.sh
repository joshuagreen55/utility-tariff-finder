#!/usr/bin/env bash
# Start or stop the GCP VM to save costs between runs.
#
# Usage:
#   ./deploy/vm-power.sh start    # start the VM
#   ./deploy/vm-power.sh stop     # stop the VM (saves ~60% of compute cost)
#   ./deploy/vm-power.sh status   # check current state

set -euo pipefail

VM_NAME="${VM_NAME:-utility-tariff-finder}"
VM_ZONE="${VM_ZONE:-us-central1-a}"

case "${1:-status}" in
  start)
    echo "Starting VM $VM_NAME..."
    gcloud compute instances start "$VM_NAME" --zone="$VM_ZONE"
    echo "Waiting for SSH to become available..."
    sleep 10
    gcloud compute ssh "$VM_NAME" --zone="$VM_ZONE" --quiet --command="
      cd /home/josh/utility-tariff-finder && docker compose up -d
    "
    echo "VM started and containers running."
    ;;
  stop)
    echo "Stopping containers on $VM_NAME..."
    gcloud compute ssh "$VM_NAME" --zone="$VM_ZONE" --quiet --command="
      cd /home/josh/utility-tariff-finder && docker compose stop
    " 2>/dev/null || true
    echo "Stopping VM..."
    gcloud compute instances stop "$VM_NAME" --zone="$VM_ZONE"
    echo "VM stopped. No compute charges while stopped (disk charges still apply)."
    ;;
  status)
    gcloud compute instances describe "$VM_NAME" --zone="$VM_ZONE" \
      --format="table(name,status,networkInterfaces[0].accessConfigs[0].natIP)"
    ;;
  *)
    echo "Usage: $0 {start|stop|status}"
    exit 1
    ;;
esac
