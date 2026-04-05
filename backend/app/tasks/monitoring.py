"""
Celery tasks for weekly monitoring of utility tariff source URLs.
"""

import asyncio

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import MonitoringSource
from app.services.monitoring_runner import check_one_source_id, run_monitoring_concurrent
from app.tasks.celery_app import celery_app


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
