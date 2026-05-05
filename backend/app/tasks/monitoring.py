"""
Celery tasks for weekly monitoring of utility tariff source URLs.

Also hosts the beat heartbeat task — a 1-minute self-pulse that lets us
detect a hung beat scheduler. See `beat_heartbeat` below.
"""

import asyncio
import logging
import os
import time

import redis as redis_lib
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import MonitoringSource
from app.services.monitoring_runner import check_one_source_id, run_monitoring_concurrent
from app.tasks.celery_app import celery_app

log = logging.getLogger(__name__)

# Redis key + TTL used by the beat watchdog. The Docker healthcheck on
# celery-beat reads this same key and marks the container unhealthy if
# the heartbeat is missing or older than the freshness window. autoheal
# then restarts the container. See docker-compose.yml.
BEAT_HEARTBEAT_KEY = "beat:heartbeat"
BEAT_HEARTBEAT_TTL_SEC = 300  # 5 minutes — generous, beat fires every 60s


@celery_app.task(name="app.tasks.monitoring.check_all_sources")
def check_all_sources():
    """Check all monitoring sources (concurrent batch)."""
    engine = get_sync_engine()
    with Session(engine) as session:
        ids = list(session.execute(select(MonitoringSource.id)).scalars().all())
    summary = asyncio.run(
        run_monitoring_concurrent(
            engine,
            ids,
            concurrency=24,
            per_host_limit=4,
            delay_ms=0,
        )
    )
    return summary["counts"]


@celery_app.task(name="app.tasks.monitoring.check_single_source")
def check_single_source(source_id: int):
    """Check a single monitoring source for changes."""
    return asyncio.run(_check_single_source(source_id))


async def _check_single_source(source_id: int) -> str:
    """Fetch URL, compare hash, log result."""
    res = await check_one_source_id(get_sync_engine(), source_id)
    return str(res.get("outcome") or "error")


@celery_app.task(
    name="app.tasks.monitoring.beat_heartbeat",
    time_limit=10,
    soft_time_limit=8,
)
def beat_heartbeat() -> float:
    """Tick the beat-alive heartbeat in Redis.

    Fires every 60s via the beat schedule. The Docker healthcheck on the
    celery-beat container reads the same key and marks the container
    unhealthy if it's missing or stale by more than 3 minutes; an
    autoheal sidecar then restarts beat.

    The May 4 incident (beat process alive but not ticking due to a
    corrupted celerybeat-schedule file) would have been auto-recovered
    within ~4 minutes if this watchdog had been in place.
    """
    redis_url = os.environ.get("REDIS_URL")
    if not redis_url:
        log.warning("beat_heartbeat: REDIS_URL not set, skipping")
        return 0.0
    now = time.time()
    r = redis_lib.from_url(redis_url, decode_responses=True)
    r.set(BEAT_HEARTBEAT_KEY, str(now), ex=BEAT_HEARTBEAT_TTL_SEC)
    return now
