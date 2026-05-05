"""Docker healthcheck for the celery-beat container.

Reads the Redis heartbeat key written by `app.tasks.monitoring.beat_heartbeat`
(fires every 60s) and exits non-zero if the heartbeat is missing or stale
by more than the freshness window. Docker then marks the container
unhealthy and the autoheal sidecar restarts it.

This catches the failure mode we saw on 2026-05-04: the beat *process*
was alive (so `docker ps` thought it was healthy) but the scheduler had
stalled and was not dispatching any tasks. Without the watchdog, beat
sat dead silent for 23 hours.
"""
import os
import sys
import time

import redis


# Beat fires the heartbeat every 60s. Allow up to 3 minutes of drift
# (covers redis flakes, GC pauses, the autoheal restart window). If we
# go past this, the container is marked unhealthy.
FRESHNESS_WINDOW_SEC = 180

HEARTBEAT_KEY = "beat:heartbeat"


def main() -> int:
    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        print("REDIS_URL not set", file=sys.stderr)
        return 1

    try:
        r = redis.from_url(redis_url, decode_responses=True, socket_timeout=4)
        raw = r.get(HEARTBEAT_KEY)
    except Exception as e:
        print(f"redis error: {e}", file=sys.stderr)
        return 1

    if raw is None:
        print(f"{HEARTBEAT_KEY} missing — beat has not ticked recently", file=sys.stderr)
        return 1

    try:
        last = float(raw)
    except (TypeError, ValueError):
        print(f"{HEARTBEAT_KEY} not a number: {raw!r}", file=sys.stderr)
        return 1

    age = time.time() - last
    if age > FRESHNESS_WINDOW_SEC:
        print(
            f"beat heartbeat stale: {age:.0f}s old "
            f"(window {FRESHNESS_WINDOW_SEC}s)",
            file=sys.stderr,
        )
        return 1

    print(f"beat alive (heartbeat {age:.0f}s old)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
