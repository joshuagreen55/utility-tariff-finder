#!/usr/bin/env bash
# Sets up weekly monitoring via macOS launchd (or shows cron alternative).
#
# Usage: bash scripts/setup_monitoring_schedule.sh [install|uninstall|cron]

set -euo pipefail

BACKEND_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PLIST_NAME="com.utilitytariff.monitoring"
PLIST_PATH="$HOME/Library/LaunchAgents/${PLIST_NAME}.plist"
LOG_DIR="$BACKEND_DIR/logs"
PYTHON="$BACKEND_DIR/venv/bin/python"

case "${1:-help}" in
  install)
    mkdir -p "$LOG_DIR"
    mkdir -p "$HOME/Library/LaunchAgents"

    cat > "$PLIST_PATH" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>${PLIST_NAME}</string>
    <key>ProgramArguments</key>
    <array>
        <string>${PYTHON}</string>
        <string>-m</string>
        <string>scripts.run_monitoring</string>
        <string>--limit</string>
        <string>200</string>
    </array>
    <key>WorkingDirectory</key>
    <string>${BACKEND_DIR}</string>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Weekday</key>
        <integer>1</integer>
        <key>Hour</key>
        <integer>6</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    <key>StandardOutPath</key>
    <string>${LOG_DIR}/monitoring.log</string>
    <key>StandardErrorPath</key>
    <string>${LOG_DIR}/monitoring-error.log</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin</string>
    </dict>
</dict>
</plist>
PLIST

    launchctl load "$PLIST_PATH"
    echo "Installed and loaded: $PLIST_PATH"
    echo "Monitoring will run every Monday at 6:00 AM (200 sources per run)."
    echo "Logs: $LOG_DIR/monitoring.log"
    ;;

  uninstall)
    if [ -f "$PLIST_PATH" ]; then
      launchctl unload "$PLIST_PATH" 2>/dev/null || true
      rm "$PLIST_PATH"
      echo "Uninstalled: $PLIST_PATH"
    else
      echo "Not installed."
    fi
    ;;

  cron)
    echo "Add this to your crontab (crontab -e):"
    echo ""
    echo "# Utility Tariff Monitoring - every Monday at 6 AM"
    echo "0 6 * * 1 cd ${BACKEND_DIR} && ${PYTHON} -m scripts.run_monitoring --limit 200 >> ${LOG_DIR}/monitoring.log 2>&1"
    ;;

  *)
    echo "Usage: $0 [install|uninstall|cron]"
    echo ""
    echo "  install    - Install macOS launchd agent for weekly monitoring"
    echo "  uninstall  - Remove the launchd agent"
    echo "  cron       - Show crontab entry (for Linux or manual setup)"
    ;;
esac
