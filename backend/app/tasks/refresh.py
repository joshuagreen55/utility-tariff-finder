"""
Celery tasks for monthly tariff re-extraction and quarterly stale recovery.

Architecture:
  - Parent tasks (enqueue_monthly_refresh, enqueue_quarterly_recovery) compute
    target utility IDs, create a RefreshRun record, and dispatch individual
    process_utility tasks via Celery.
  - Child task (process_utility) runs run_pipeline for a single utility.
  - Completion task (finalize_refresh_run) aggregates results and updates the
    RefreshRun record.

Monthly: re-extracts tariffs for utilities whose monitoring sources detected
page changes, plus utilities with tariffs not verified in 90+ days.

Quarterly: blind re-extraction for utilities stuck in monitoring error state,
using Brave Search to rediscover tariff page URLs.
"""

import json
import logging
from datetime import datetime, timedelta, timezone

from celery import chord, group
from sqlalchemy import distinct, func, select
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import (
    MonitoringSource,
    MonitoringStatus,
    RefreshRun,
    RefreshType,
    Tariff,
    Utility,
)
from app.tasks.celery_app import celery_app

log = logging.getLogger(__name__)

STALE_THRESHOLD_DAYS = 90
QUARTERLY_ERROR_LIMIT = 200

LLM_RATE_LIMIT = "8/m"
DOMAIN_RATE_LIMIT = "1/s"


# ---------------------------------------------------------------------------
# Utility selection helpers
# ---------------------------------------------------------------------------

def _get_changed_utility_ids(session: Session, since_days: int = 30) -> list[int]:
    """Utilities with at least one monitoring source that changed recently."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
    stmt = (
        select(distinct(MonitoringSource.utility_id))
        .join(Utility, MonitoringSource.utility_id == Utility.id)
        .where(MonitoringSource.status == MonitoringStatus.CHANGED)
        .where(MonitoringSource.last_changed_at >= cutoff)
        .where(Utility.is_active.is_(True))
    )
    return list(session.execute(stmt).scalars().all())


def _get_stale_utility_ids(session: Session, threshold_days: int = STALE_THRESHOLD_DAYS) -> list[int]:
    """Active utilities whose tariffs haven't been verified recently."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=threshold_days)
    stmt = (
        select(Utility.id)
        .where(Utility.is_active.is_(True))
        .where(
            ~Utility.id.in_(
                select(distinct(Tariff.utility_id)).where(
                    Tariff.last_verified_at >= cutoff
                )
            )
        )
    )
    return list(session.execute(stmt).scalars().all())


def _get_error_utility_ids(session: Session, limit: int = QUARTERLY_ERROR_LIMIT) -> list[int]:
    """Active utilities where ALL monitoring sources are in error state."""
    total_per = (
        select(
            MonitoringSource.utility_id,
            func.count(MonitoringSource.id).label("total"),
        )
        .group_by(MonitoringSource.utility_id)
        .subquery()
    )
    error_per = (
        select(
            MonitoringSource.utility_id,
            func.count(MonitoringSource.id).label("err"),
        )
        .where(MonitoringSource.status == MonitoringStatus.ERROR)
        .group_by(MonitoringSource.utility_id)
        .subquery()
    )
    stmt = (
        select(Utility.id)
        .join(total_per, Utility.id == total_per.c.utility_id)
        .join(error_per, Utility.id == error_per.c.utility_id)
        .where(Utility.is_active.is_(True))
        .where(total_per.c.total == error_per.c.err)
        .limit(limit)
    )
    return list(session.execute(stmt).scalars().all())


def _count_tariffs(session: Session, utility_ids: list[int]) -> dict[int, int]:
    """Count tariffs per utility."""
    if not utility_ids:
        return {}
    rows = session.execute(
        select(Tariff.utility_id, func.count(Tariff.id))
        .where(Tariff.utility_id.in_(utility_ids))
        .group_by(Tariff.utility_id)
    ).all()
    return {uid: cnt for uid, cnt in rows}


# ---------------------------------------------------------------------------
# Child task: process a single utility
# ---------------------------------------------------------------------------

@celery_app.task(
    name="app.tasks.refresh.process_utility",
    bind=True,
    max_retries=1,
    soft_time_limit=600,
    time_limit=660,
    rate_limit=LLM_RATE_LIMIT,
    acks_late=True,
)
def process_utility(self, uid: int, comprehensive: bool = False) -> dict:
    """Run the full tariff pipeline for a single utility.
    
    This is the unit of parallelism — Celery distributes these across workers.
    """
    from scripts.tariff_pipeline import cleanup_between_utilities, run_pipeline

    log.info(f"Processing utility {uid} (comprehensive={comprehensive})")
    try:
        result = run_pipeline(uid, dry_run=False, comprehensive=comprehensive)
        valid_count = (result.phase4_validation or {}).get("valid", 0)
        return {
            "utility_id": uid,
            "utility_name": result.utility_name,
            "state": result.state,
            "tariffs_found": valid_count,
            "errors": result.errors,
            "success": valid_count > 0,
        }
    except Exception as e:
        log.error(f"Utility {uid} CRASHED: {e}")
        return {
            "utility_id": uid,
            "utility_name": str(uid),
            "state": "",
            "tariffs_found": 0,
            "errors": [f"Unhandled crash: {e}"],
            "success": False,
        }
    finally:
        cleanup_between_utilities()


# ---------------------------------------------------------------------------
# Completion callback: finalize a refresh run
# ---------------------------------------------------------------------------

@celery_app.task(name="app.tasks.refresh.finalize_refresh_run")
def finalize_refresh_run(results: list[dict], run_id: int, before_counts_json: str):
    """Aggregate child task results and update the RefreshRun record."""
    before_counts = json.loads(before_counts_json)
    engine = get_sync_engine()

    processed = sum(1 for r in results if r and r.get("success"))
    error_count = sum(1 for r in results if r and not r.get("success"))
    total_tariffs_found = sum(r.get("tariffs_found", 0) for r in results if r)

    utility_ids = [r["utility_id"] for r in results if r]
    with Session(engine) as session:
        after_counts = _count_tariffs(session, utility_ids)

    tariffs_added = 0
    tariffs_updated = 0
    for r in results:
        if not r:
            continue
        uid = r["utility_id"]
        before = before_counts.get(str(uid), 0)
        after = after_counts.get(uid, 0)
        if after > before:
            tariffs_added += after - before
        elif after == before and before > 0:
            tariffs_updated += after

    affected_states = list({r["state"] for r in results if r and r.get("state")})

    # Run duplicate cleanup
    if affected_states:
        try:
            from scripts.cleanup_duplicate_tariffs import run_cleanup
            run_cleanup(country=None, province=None, states=affected_states, dry_run=False)
        except Exception as e:
            log.warning(f"Duplicate cleanup failed: {e}")

    # Count stale tariffs
    stale_count = 0
    with Session(engine) as session:
        cutoff = datetime.now(timezone.utc) - timedelta(days=STALE_THRESHOLD_DAYS * 3)
        stale_result = session.execute(
            select(func.count(Tariff.id)).where(
                Tariff.utility_id.in_(utility_ids),
                (Tariff.last_verified_at < cutoff) | (Tariff.last_verified_at.is_(None)),
            )
        )
        stale_count = stale_result.scalar() or 0

    error_details = []
    for r in results:
        if r and not r.get("success"):
            error_details.append(
                f"{r.get('utility_name', '?')} ({r.get('state', '?')}): "
                f"{'; '.join(r.get('errors', ['unknown']))}"
            )

    summary = {
        "total_targeted": len(results),
        "processed_ok": processed,
        "errors": error_count,
        "tariffs_found_in_run": total_tariffs_found,
        "tariffs_added": tariffs_added,
        "tariffs_updated": tariffs_updated,
        "tariffs_stale": stale_count,
        "affected_states": affected_states,
    }

    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        run = session.get(RefreshRun, run_id)
        if run:
            run.finished_at = now
            run.utilities_processed = processed
            run.tariffs_added = tariffs_added
            run.tariffs_updated = tariffs_updated
            run.tariffs_stale = stale_count
            run.errors = error_count
            run.summary_json = summary
            run.error_details = "\n".join(error_details) if error_details else None
            session.commit()

    log.info(
        f"Refresh run {run_id} complete: {processed} ok, {error_count} errors, "
        f"{tariffs_added} added, {tariffs_updated} updated"
    )
    return summary


# ---------------------------------------------------------------------------
# Parent tasks: orchestrate refresh runs
# ---------------------------------------------------------------------------

@celery_app.task(
    name="app.tasks.refresh.refresh_changed_tariffs",
    time_limit=300,
    soft_time_limit=280,
)
def refresh_changed_tariffs():
    """Monthly tariff refresh: enqueue per-utility tasks for changed + stale utilities."""
    engine = get_sync_engine()

    with Session(engine) as session:
        run = RefreshRun(refresh_type=RefreshType.monthly)
        session.add(run)
        session.commit()
        run_id = run.id

    log.info("=" * 60)
    log.info("MONTHLY TARIFF REFRESH")
    log.info(f"  Started: {datetime.now(timezone.utc).isoformat()}")

    with Session(engine) as session:
        changed_ids = _get_changed_utility_ids(session)
        stale_ids = _get_stale_utility_ids(session)
        all_ids = list(set(changed_ids + stale_ids))
        before_counts = _count_tariffs(session, all_ids)

    log.info(f"  Changed (monitoring detected): {len(changed_ids)}")
    log.info(f"  Stale (not verified in {STALE_THRESHOLD_DAYS}d): {len(stale_ids)}")
    log.info(f"  Total unique targets: {len(all_ids)}")

    with Session(engine) as session:
        run = session.get(RefreshRun, run_id)
        run.utilities_targeted = len(all_ids)
        session.commit()

    if not all_ids:
        log.info("  No utilities to process")
        return {"run_id": run_id, "targeted": 0}

    before_json = json.dumps({str(k): v for k, v in before_counts.items()})

    # Dispatch per-utility tasks as a chord: all process in parallel,
    # then finalize_refresh_run runs once all are done
    task_group = group(
        process_utility.s(uid, False) for uid in all_ids
    )
    callback = finalize_refresh_run.s(run_id, before_json)
    chord(task_group)(callback)

    log.info(f"  Dispatched {len(all_ids)} tasks for run {run_id}")
    return {"run_id": run_id, "targeted": len(all_ids)}


@celery_app.task(
    name="app.tasks.refresh.recover_error_utilities",
    time_limit=300,
    soft_time_limit=280,
)
def recover_error_utilities():
    """Quarterly recovery: enqueue per-utility tasks for error-state utilities."""
    engine = get_sync_engine()

    with Session(engine) as session:
        run = RefreshRun(refresh_type=RefreshType.quarterly)
        session.add(run)
        session.commit()
        run_id = run.id

    log.info("=" * 60)
    log.info("QUARTERLY STALE RECOVERY")
    log.info(f"  Started: {datetime.now(timezone.utc).isoformat()}")

    with Session(engine) as session:
        error_ids = _get_error_utility_ids(session, limit=QUARTERLY_ERROR_LIMIT)
        before_counts = _count_tariffs(session, error_ids)

    log.info(f"  Utilities with all-error monitoring sources: {len(error_ids)}")

    with Session(engine) as session:
        run = session.get(RefreshRun, run_id)
        run.utilities_targeted = len(error_ids)
        session.commit()

    if not error_ids:
        log.info("  No error utilities to process")
        return {"run_id": run_id, "targeted": 0}

    before_json = json.dumps({str(k): v for k, v in before_counts.items()})

    task_group = group(
        process_utility.s(uid, True) for uid in error_ids
    )
    callback = finalize_refresh_run.s(run_id, before_json)
    chord(task_group)(callback)

    log.info(f"  Dispatched {len(error_ids)} tasks for run {run_id}")
    return {"run_id": run_id, "targeted": len(error_ids)}
