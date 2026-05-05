from celery import Celery
from celery.schedules import crontab

from app.config import settings

celery_app = Celery(
    "utility_tariff_finder",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=[
        "app.tasks.monitoring",
        "app.tasks.refresh",
    ],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    # Refresh runs can dispatch hundreds of per-utility tasks that take many
    # hours to drain through the 8/min LLM rate limit. The Celery defaults
    # (result_expires=1d, chord_unlock max_retries=3) are not safe at that
    # scale: the chord unlock helper task can give up before the header
    # tasks finish and silently drop the callback. We bump both well above
    # the longest run we can plausibly produce. See `_reap_stalled_runs`
    # in app.tasks.refresh for the safety-net reaper that catches the
    # remaining edge cases.
    result_expires=60 * 60 * 24 * 7,            # 7 days
    result_chord_join_timeout=60 * 60 * 12,     # 12 hours
    result_chord_retry_interval=10,             # poll unlock every 10s
    task_track_started=True,
    beat_schedule={
        # Watchdog: writes a Redis heartbeat key that the celery-beat
        # Docker healthcheck reads. If beat hangs (as it did on May 4),
        # the heartbeat goes stale, the container is marked unhealthy,
        # and the autoheal sidecar restarts it within ~1 minute.
        "beat-heartbeat": {
            "task": "app.tasks.monitoring.beat_heartbeat",
            "schedule": 60.0,  # every 60 seconds
        },
        "weekly-monitoring-check": {
            "task": "app.tasks.monitoring.check_all_sources",
            "schedule": crontab(hour=6, minute=0, day_of_week=1),  # Monday 6 AM UTC
        },
        "monthly-tariff-refresh": {
            "task": "app.tasks.refresh.refresh_changed_tariffs",
            "schedule": crontab(hour=8, minute=0, day_of_month=1),  # 1st of month 8 AM UTC
        },
        "quarterly-stale-recovery": {
            "task": "app.tasks.refresh.recover_error_utilities",
            "schedule": crontab(hour=10, minute=0, day_of_month=1, month_of_year="1,4,7,10"),
        },
        # Safety net: if a chord callback fails to fire (as happened on
        # 2026-05-01), this hourly reaper detects RefreshRun records that
        # have been "running" for >6 hours and finalizes them by querying
        # the database directly. Idempotent.
        "reap-stalled-refresh-runs": {
            "task": "app.tasks.refresh.reap_stalled_runs",
            "schedule": crontab(minute=15),  # every hour at :15
        },
    },
)
