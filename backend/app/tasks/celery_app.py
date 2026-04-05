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
    beat_schedule={
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
    },
)
