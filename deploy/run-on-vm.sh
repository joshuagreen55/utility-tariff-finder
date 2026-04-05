#!/usr/bin/env bash
# Run a command inside the API container on the VM, safely wrapped in tmux.
# The tmux session survives SSH disconnects, and output is tee'd to a log file.
#
# Usage (from project root):
#   ./deploy/run-on-vm.sh "python -m scripts.benchmark --run-pipeline"
#   ./deploy/run-on-vm.sh "python -m scripts.rerun_50_test" --name rerun50
#   ./deploy/run-on-vm.sh "python -m scripts.opus_audit" --name opus
#   ./deploy/run-on-vm.sh --attach rerun50        # reattach to running session
#   ./deploy/run-on-vm.sh --status                 # list active tmux sessions
#   ./deploy/run-on-vm.sh --logs rerun50           # tail the log file
#   ./deploy/run-on-vm.sh --kill rerun50           # kill a session
#
# The log file is written to ~/utility-tariff-finder/logs/<name>_<timestamp>.log
# which is bind-mounted into the container at /app/logs/.

set -euo pipefail

VM_NAME="${VM_NAME:-utility-tariff-finder}"
VM_ZONE="${VM_ZONE:-us-central1-a}"
REMOTE_DIR="/home/josh/utility-tariff-finder"

SESSION_NAME=""
MODE="run"  # run | attach | status | logs | kill

# Parse arguments
COMMAND=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --name)
      SESSION_NAME="$2"
      shift 2
      ;;
    --attach)
      MODE="attach"
      SESSION_NAME="$2"
      shift 2
      ;;
    --status)
      MODE="status"
      shift
      ;;
    --logs)
      MODE="logs"
      SESSION_NAME="$2"
      shift 2
      ;;
    --kill)
      MODE="kill"
      SESSION_NAME="$2"
      shift 2
      ;;
    --help|-h)
      echo "Usage:"
      echo "  $0 \"<command>\"                Run command in tmux"
      echo "  $0 \"<command>\" --name <name>  Run with named session"
      echo "  $0 --attach <name>             Reattach to session"
      echo "  $0 --status                    List tmux sessions"
      echo "  $0 --logs <name>               Tail log file"
      echo "  $0 --kill <name>               Kill session"
      exit 0
      ;;
    *)
      COMMAND="$1"
      shift
      ;;
  esac
done

ssh_cmd() {
  gcloud compute ssh "$VM_NAME" --zone="$VM_ZONE" --quiet --command="$1"
}

ssh_interactive() {
  gcloud compute ssh "$VM_NAME" --zone="$VM_ZONE" -- "$@"
}

case "$MODE" in
  status)
    echo "=== Active tmux sessions on VM ==="
    ssh_cmd "tmux list-sessions 2>/dev/null || echo '  No active sessions'"
    echo ""
    echo "=== Recent log files ==="
    ssh_cmd "ls -lt $REMOTE_DIR/logs/*.log 2>/dev/null | head -10 || echo '  No log files'"
    exit 0
    ;;

  attach)
    if [[ -z "$SESSION_NAME" ]]; then
      echo "Error: --attach requires a session name"
      exit 1
    fi
    echo "Attaching to tmux session '$SESSION_NAME'..."
    echo "(Detach with Ctrl+B then D)"
    ssh_interactive -t "tmux attach-session -t $SESSION_NAME"
    exit 0
    ;;

  logs)
    if [[ -z "$SESSION_NAME" ]]; then
      echo "Error: --logs requires a session name"
      exit 1
    fi
    echo "=== Tailing logs for $SESSION_NAME ==="
    ssh_cmd "tail -100 $REMOTE_DIR/logs/${SESSION_NAME}_*.log 2>/dev/null || echo 'No log file found for $SESSION_NAME'"
    exit 0
    ;;

  kill)
    if [[ -z "$SESSION_NAME" ]]; then
      echo "Error: --kill requires a session name"
      exit 1
    fi
    echo "Killing tmux session '$SESSION_NAME'..."
    ssh_cmd "tmux kill-session -t $SESSION_NAME 2>/dev/null && echo 'Killed.' || echo 'Session not found.'"
    exit 0
    ;;

  run)
    if [[ -z "$COMMAND" ]]; then
      echo "Error: no command provided"
      echo "Usage: $0 \"python -m scripts.benchmark --run-pipeline\" [--name mybench]"
      exit 1
    fi

    # Generate session name from command if not provided
    if [[ -z "$SESSION_NAME" ]]; then
      SESSION_NAME="run_$(date +%H%M%S)"
    fi

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    LOG_FILE="$REMOTE_DIR/logs/${SESSION_NAME}_${TIMESTAMP}.log"

    echo "=== Running command on VM ==="
    echo "  Command:  $COMMAND"
    echo "  Session:  $SESSION_NAME"
    echo "  Log file: $LOG_FILE"
    echo ""

    # Create tmux session on the VM that runs the command inside docker
    ssh_cmd "
      mkdir -p $REMOTE_DIR/logs
      tmux new-session -d -s $SESSION_NAME \"
        cd $REMOTE_DIR && \\
        echo '=== Started: \$(date -Iseconds) ===' | tee $LOG_FILE && \\
        echo '=== Command: $COMMAND ===' | tee -a $LOG_FILE && \\
        echo '' | tee -a $LOG_FILE && \\
        docker compose exec -T api $COMMAND 2>&1 | tee -a $LOG_FILE; \\
        EXIT_CODE=\\\$?; \\
        echo '' | tee -a $LOG_FILE; \\
        echo \\\"=== Finished: \\\$(date -Iseconds) (exit code: \\\$EXIT_CODE) ===\\\" | tee -a $LOG_FILE; \\
        echo 'Press Enter to close this session...'; \\
        read
      \"
    "

    echo "  Started in tmux session '$SESSION_NAME'"
    echo ""
    echo "Useful commands:"
    echo "  ./deploy/run-on-vm.sh --attach $SESSION_NAME   # watch live"
    echo "  ./deploy/run-on-vm.sh --logs $SESSION_NAME     # tail log"
    echo "  ./deploy/run-on-vm.sh --status                 # list sessions"
    echo "  ./deploy/run-on-vm.sh --kill $SESSION_NAME     # kill it"
    ;;
esac
