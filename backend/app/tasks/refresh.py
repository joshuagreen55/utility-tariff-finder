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
import os
from datetime import datetime, timedelta, timezone

from celery import chord, group
from sqlalchemy import distinct, func, select, update
from sqlalchemy.orm import Session

from app.config import settings
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

# Hard cap on a single monthly run. Without it, one bad month (monitoring
# flapping + a large stale backlog) dispatches thousands of tasks that take
# days to drain through the LLM rate limit and starve everything else.
#
# Sized to the steady-state rotation: keeping ~1,425 utilities on a 90-day
# freshness cycle needs ~475 re-verifications/month, plus changed-signal
# utilities. 400 sat just under that (freshness slowly decayed); 600 covers
# the rotation with headroom. The sourceless quarantine + aggregator-domain
# guards keep the extra slots pointed at tractable utilities, so the added
# spend is mostly cheap re-verifications rather than dead-end escalations.
MONTHLY_MAX_UTILITIES = int(os.environ.get("MONTHLY_MAX_UTILITIES", "600"))

# A utility that fails this many consecutive *structural* extractions
# (page reached but 0 tariffs, no rate page, no rate signals — i.e. it has
# no machine-readable rates on the web) is soft-quarantined. Transient
# network errors and crashes do NOT count toward the streak. State is
# persisted on the utility row (survives Redis flushes and is queryable).
QUARANTINE_STRUCTURAL_THRESHOLD = 3

# A quarantined utility is re-checked at most this often. Between rechecks
# it is skipped by both the monthly and quarterly runs — unless a monitoring
# source detects a genuinely new page (a CHANGED signal), which always
# overrides the quarantine.
QUARANTINE_RECHECK_DAYS = int(os.environ.get("QUARANTINE_RECHECK_DAYS", "120"))

# Error-string fragments that indicate a *transient* failure (worth
# retrying as-is) rather than a structural "no source" failure. Matched
# case-insensitively.
_TRANSIENT_ERROR_MARKERS = (
    "timeout", "timed out", "connection", "connreset", "reset by peer",
    "rate limit", "429", "temporarily", "503", "502", "504",
    "unhandled crash", "eof occurred", "ssl",
)

# Per-utility dispatch lock TTL. Slightly above process_utility's hard
# time_limit so a crashed worker can never leave a utility locked forever.
UTILITY_LOCK_TTL = 1900

LLM_RATE_LIMIT = "8/m"
DOMAIN_RATE_LIMIT = "1/s"

_LIVE_TARIFF = Tariff.superseded_by_tariff_id.is_(None) & Tariff.supersede_reason.is_(None)


def _get_redis():
    """Best-effort Redis client for locks/quarantine. Returns None when
    unavailable — callers must degrade gracefully (no lock beats no run)."""
    try:
        import redis
        client = redis.Redis.from_url(settings.redis_url)
        client.ping()
        return client
    except Exception as e:
        log.warning(f"Redis unavailable for refresh helpers: {e}")
        return None


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
    """Active utilities whose tariffs haven't been verified recently,
    ordered oldest-first.

    Ordering matters when the monthly cap binds: we want to spend the
    budget on the *most*-aged utilities and let the least-stale ones roll
    to next month, rather than trimming an arbitrary slice. Utilities with
    no verified tariff at all (NULL max) sort first via NULLS FIRST.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=threshold_days)
    last_verified = (
        select(
            Tariff.utility_id.label("uid"),
            func.max(Tariff.last_verified_at).label("last_verified"),
        )
        .where(_LIVE_TARIFF)
        .group_by(Tariff.utility_id)
        .subquery()
    )
    stmt = (
        select(Utility.id)
        .outerjoin(last_verified, last_verified.c.uid == Utility.id)
        .where(Utility.is_active.is_(True))
        .where(
            ~Utility.id.in_(
                select(distinct(Tariff.utility_id)).where(
                    Tariff.last_verified_at >= cutoff,
                    _LIVE_TARIFF,
                )
            )
        )
        .order_by(last_verified.c.last_verified.asc().nulls_first())
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


def _filter_quarantined(session: Session, ids: list[int]) -> tuple[list[int], list[int]]:
    """Split ids into (keep, quarantined).

    A utility is *actively* quarantined when `refresh_quarantined_at` is set
    and newer than the recheck window. Those are skipped. Utilities whose
    quarantine has aged past the window are returned in `keep` so they get
    one recheck (and re-quarantine on a fresh failure).
    """
    if not ids:
        return ids, []
    recheck_cutoff = datetime.now(timezone.utc) - timedelta(days=QUARANTINE_RECHECK_DAYS)
    rows = session.execute(
        select(Utility.id, Utility.refresh_quarantined_at).where(Utility.id.in_(ids))
    ).all()
    quarantined_set = {
        uid
        for uid, qat in rows
        if qat is not None
        and (qat if qat.tzinfo else qat.replace(tzinfo=timezone.utc)) >= recheck_cutoff
    }
    keep = [i for i in ids if i not in quarantined_set]
    quarantined = [i for i in ids if i in quarantined_set]
    return keep, quarantined


def _count_tariffs(session: Session, utility_ids: list[int]) -> dict[int, int]:
    """Count live tariffs per utility (a revision adds a row but not a plan)."""
    if not utility_ids:
        return {}
    rows = session.execute(
        select(Tariff.utility_id, func.count(Tariff.id))
        .where(Tariff.utility_id.in_(utility_ids), _LIVE_TARIFF)
        .group_by(Tariff.utility_id)
    ).all()
    return {uid: cnt for uid, cnt in rows}


# ---------------------------------------------------------------------------
# Child task: process a single utility
# ---------------------------------------------------------------------------

TRANSIENT_ERRORS = (ConnectionError, TimeoutError, OSError)


def _clear_changed_sources(uid: int):
    """After a successful extraction, mark this utility's CHANGED monitoring
    sources as UNCHANGED so next month's run doesn't re-target the same
    already-consumed change signal."""
    engine = get_sync_engine()
    with Session(engine) as session:
        session.execute(
            update(MonitoringSource)
            .where(
                MonitoringSource.utility_id == uid,
                MonitoringSource.status == MonitoringStatus.CHANGED,
            )
            .values(status=MonitoringStatus.UNCHANGED)
        )
        session.commit()


def _looks_transient(errors: list[str]) -> bool:
    """Heuristic: do these error strings look like a retryable transient
    failure (network/timeout/rate-limit/crash) rather than a structural
    'this utility has no web-readable rates' failure?"""
    blob = " ".join(errors or []).lower()
    return any(marker in blob for marker in _TRANSIENT_ERROR_MARKERS)


def _record_outcome(uid: int, success: bool, structural_failure: bool, reason: str = ""):
    """Persist the refresh outcome on the utility row.

    - success: reset the streak, lift any quarantine.
    - structural failure: increment the streak; quarantine once it crosses
      the threshold.
    - transient failure / crash: record the attempt + reason but do NOT
      advance the streak (we don't want a bad network day to quarantine a
      utility that genuinely has rates).
    """
    now = datetime.now(timezone.utc)
    reason = (reason or "")[:200]
    engine = get_sync_engine()
    try:
        with Session(engine) as session:
            u = session.get(Utility, uid)
            if u is None:
                return
            u.refresh_last_attempt_at = now
            if success:
                u.refresh_fail_streak = 0
                u.refresh_quarantined_at = None
                u.refresh_last_reason = None
            elif structural_failure:
                u.refresh_fail_streak = (u.refresh_fail_streak or 0) + 1
                u.refresh_last_reason = reason
                if u.refresh_fail_streak >= QUARANTINE_STRUCTURAL_THRESHOLD:
                    # Stamp a fresh timestamp so the recheck clock restarts
                    # each time it re-fails, keeping chronic cases quarantined.
                    u.refresh_quarantined_at = now
            else:
                # Transient: leave streak untouched, just note the reason.
                u.refresh_last_reason = reason
            session.commit()
    except Exception as e:
        log.warning(f"Failed to record refresh outcome for {uid}: {e}")


@celery_app.task(
    name="app.tasks.refresh.process_utility",
    bind=True,
    max_retries=2,
    # Budget must comfortably exceed the longest plausible Phase 6 Deep
    # Research call (~10 min) plus Phases 1-5 (~1-2 min) so the parent
    # task does not get killed mid-flight, throwing away DR compute we
    # have already paid for. The May 4 quarterly run wasted ~55 of 65
    # Phase 6 calls because the previous 660s budget was tighter than
    # one DR call. See PHASE6_MAX_WAIT_SEC=1200 in docker-compose.
    soft_time_limit=1700,
    time_limit=1800,
    rate_limit=LLM_RATE_LIMIT,
    acks_late=True,
    # Without this, acks_late re-queues a task whose worker died (OOM kill,
    # docker restart) and it can crash the next worker the same way, looping
    # forever. Reject instead: the run's finalize/reaper accounts for it.
    reject_on_worker_lost=True,
    autoretry_for=TRANSIENT_ERRORS,
    retry_backoff=60,
    retry_backoff_max=300,
    retry_jitter=True,
)
def process_utility(self, uid: int, comprehensive: bool = False) -> dict:
    """Run the full tariff pipeline for a single utility.

    This is the unit of parallelism — Celery distributes these across workers.
    Automatically retries up to 2 times with exponential backoff (60s, then
    up to 300s with jitter) on transient network/timeout errors.

    A per-utility Redis lock prevents two concurrent runs (e.g. overlapping
    monthly + quarterly dispatches, or a manual campaign) from processing
    the same utility at once — concurrent store_tariffs calls can duplicate
    rows and race the reconciliation pass.
    """
    from scripts import llm_cost
    from scripts.tariff_pipeline import cleanup_between_utilities, run_pipeline

    r = _get_redis()
    lock = None
    if r is not None:
        lock = r.lock(f"refresh:lock:utility:{uid}", timeout=UTILITY_LOCK_TTL)
        if not lock.acquire(blocking=False):
            log.info(f"Utility {uid} already being processed elsewhere — skipping")
            return {
                "utility_id": uid,
                "utility_name": str(uid),
                "state": "",
                "tariffs_found": 0,
                "errors": [],
                "success": True,
                "skipped_locked": True,
            }

    log.info(f"Processing utility {uid} (comprehensive={comprehensive}, attempt={self.request.retries + 1})")
    try:
        result = run_pipeline(uid, dry_run=False, comprehensive=comprehensive)
        valid_count = (result.phase4_validation or {}).get("valid", 0)
        # A fingerprint skip (content unchanged, existing tariffs re-verified)
        # is a cheap success, not an error — counting it as a failure used to
        # inflate error rates and trigger pointless retry campaigns.
        skipped = getattr(result, "skipped_unchanged", False)
        success = valid_count > 0 or skipped
        # A non-success with no transient markers means the pipeline reached
        # the utility but found no extractable rates — a structural failure
        # that should count toward quarantine.
        structural = not success and not _looks_transient(result.errors)
        _record_outcome(
            uid, success, structural,
            reason="; ".join(result.errors[:2]) if result.errors else "",
        )
        if success:
            try:
                _clear_changed_sources(uid)
            except Exception as e:
                log.warning(f"Could not clear CHANGED sources for {uid}: {e}")
        return {
            "utility_id": uid,
            "utility_name": result.utility_name,
            "state": result.state,
            "tariffs_found": valid_count,
            "errors": result.errors,
            "success": success,
            "skipped_unchanged": skipped,
            "cost": llm_cost.summary(),
        }
    except TRANSIENT_ERRORS:
        raise  # Let autoretry handle these
    except Exception as e:
        log.error(f"Utility {uid} CRASHED: {e}")
        # A crash is treated as transient (likely a bug or resource blip),
        # not a structural "no source" failure — don't advance the streak.
        _record_outcome(uid, False, structural_failure=False, reason=f"crash: {e}")
        return {
            "utility_id": uid,
            "utility_name": str(uid),
            "state": "",
            "tariffs_found": 0,
            "errors": [f"Unhandled crash: {e}"],
            "success": False,
            "cost": llm_cost.summary(),
        }
    finally:
        cleanup_between_utilities()
        if lock is not None:
            try:
                lock.release()
            except Exception:
                pass  # TTL expiry already released it


# ---------------------------------------------------------------------------
# Completion callback: finalize a refresh run
# ---------------------------------------------------------------------------

@celery_app.task(name="app.tasks.refresh.log_chord_failure")
def log_chord_failure(request, exc, traceback):
    """Surfaced when a chord errors out so we can see it in logs.

    Without this, chord failures are silent and the dashboard sits stuck
    on a "running" RefreshRun until the hourly reaper finalizes it.
    """
    log.error(
        f"Chord callback FAILED: task={getattr(request, 'task', '?')} "
        f"id={getattr(request, 'id', '?')}: {exc}"
    )


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
                _LIVE_TARIFF,
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

    # Roll up per-utility LLM cost into a run-level breakdown.
    from scripts import llm_cost
    cost = llm_cost.merge_summaries([r.get("cost") for r in results if r])

    summary = {
        "total_targeted": len(results),
        "processed_ok": processed,
        "errors": error_count,
        "tariffs_found_in_run": total_tariffs_found,
        "tariffs_added": tariffs_added,
        "tariffs_updated": tariffs_updated,
        "tariffs_stale": stale_count,
        "affected_states": affected_states,
        "llm_cost": cost,
    }

    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        run = session.get(RefreshRun, run_id)
        if run:
            # Preserve the dispatch-time audit scope written by the parent.
            prior = run.summary_json or {}
            for key in ("targeted_utility_ids", "quarantined_skipped"):
                if key in prior:
                    summary[key] = prior[key]
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
# Safety-net reaper: finalize stalled runs from the audit trail
# ---------------------------------------------------------------------------

# A refresh that has been "running" longer than this is considered stalled
# (its chord callback failed to fire). Set to comfortably exceed the longest
# real run we could plausibly produce: ~519 utilities at the 8/min LLM rate
# limit + per-task pipeline work tops out around 4-6 hours.
STALLED_RUN_THRESHOLD_HOURS = 6


def _finalize_run_from_audit(session: Session, run: RefreshRun) -> dict:
    """Reconstruct a refresh-run summary from the database audit trail.

    Used when the chord callback never fires (Celery result expiry, unlock
    retry exhaustion, header task killed by the time limit, etc). The
    numbers are best-effort: errors is computed conservatively as
    `targeted - utilities_processed`. Per-utility error reasons cannot be
    recovered after the fact.
    """
    started = run.started_at
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    # Bound the audit window at "now" (or 24h after start, whichever is
    # earlier — the longest a real run could plausibly take is ~6h, but
    # leave headroom for exotic cases).
    window_end = min(
        datetime.now(timezone.utc),
        started + timedelta(hours=24),
    )

    # Scope the reconstruction to the utilities this run actually
    # dispatched (recorded at dispatch time). Without this, concurrent
    # campaigns/manual scripts running in the same window get credited to
    # this run and the numbers are fiction.
    targeted_ids = (run.summary_json or {}).get("targeted_utility_ids") or []

    created_q = select(func.count(Tariff.id)).where(
        Tariff.created_at >= started,
        Tariff.created_at < window_end,
    )
    verified_q = select(func.count(Tariff.id)).where(
        Tariff.last_verified_at >= started,
        Tariff.last_verified_at < window_end,
        (Tariff.created_at < started) | (Tariff.created_at.is_(None)),
    )
    processed_q = select(func.count(func.distinct(Tariff.utility_id))).where(
        Tariff.last_verified_at >= started,
        Tariff.last_verified_at < window_end,
    )
    if targeted_ids:
        created_q = created_q.where(Tariff.utility_id.in_(targeted_ids))
        verified_q = verified_q.where(Tariff.utility_id.in_(targeted_ids))
        processed_q = processed_q.where(Tariff.utility_id.in_(targeted_ids))

    created = session.execute(created_q).scalar() or 0
    verified_only = session.execute(verified_q).scalar() or 0
    utilities_processed = session.execute(processed_q).scalar() or 0

    targeted = run.utilities_targeted or 0
    errors = max(targeted - utilities_processed, 0)

    summary = {
        "total_targeted": targeted,
        "processed_ok": utilities_processed,
        "errors": errors,
        "tariffs_added": created,
        "tariffs_updated": verified_only,
        "_note": (
            f"Reconstructed from audit trail at "
            f"{datetime.now(timezone.utc).isoformat()} because the chord "
            f"callback did not fire within {STALLED_RUN_THRESHOLD_HOURS}h."
        ),
    }

    run.finished_at = window_end
    run.utilities_processed = utilities_processed
    run.tariffs_added = created
    run.tariffs_updated = verified_only
    run.errors = errors
    run.summary_json = summary
    run.error_details = (
        "(Reconstructed post-hoc; chord callback never collected per-task "
        "results.)"
    )
    return summary


@celery_app.task(
    name="app.tasks.refresh.reap_stalled_runs",
    time_limit=120,
    soft_time_limit=110,
)
def reap_stalled_runs() -> dict:
    """Finalize any refresh run whose chord callback never fired.

    Runs hourly (see beat_schedule). Idempotent — only touches runs whose
    `finished_at` is still NULL after the staleness threshold has elapsed.
    """
    engine = get_sync_engine()
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=STALLED_RUN_THRESHOLD_HOURS)
    reaped: list[int] = []

    with Session(engine) as session:
        stalled = session.execute(
            select(RefreshRun)
            .where(RefreshRun.finished_at.is_(None))
            .where(RefreshRun.started_at < cutoff)
        ).scalars().all()

        for run in stalled:
            # Scale the threshold with run size: a 5,000-utility campaign at
            # the 8/min LLM rate limit legitimately takes >10h. Reaping it
            # at 6h would finalize a run that is still healthy.
            targeted = run.utilities_targeted or 0
            expected_hours = max(
                STALLED_RUN_THRESHOLD_HOURS,
                (targeted / 480.0) * 2 + 1,  # 8/min = 480/h, 2x headroom
            )
            started = run.started_at
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
            age_hours = (now - started).total_seconds() / 3600
            if age_hours < expected_hours:
                log.info(
                    f"Skipping RefreshRun #{run.id}: {age_hours:.1f}h old, "
                    f"expected up to {expected_hours:.1f}h for {targeted} targets"
                )
                continue
            log.warning(
                f"Reaping stalled RefreshRun #{run.id} "
                f"(started {run.started_at.isoformat()}, "
                f"type={run.refresh_type})"
            )
            try:
                summary = _finalize_run_from_audit(session, run)
                session.commit()
                reaped.append(run.id)
                log.info(
                    f"  Reaped #{run.id}: processed={summary['processed_ok']}, "
                    f"errors={summary['errors']}, added={summary['tariffs_added']}"
                )
            except Exception as e:
                log.error(f"  Failed to reap run #{run.id}: {e}")
                session.rollback()

    return {"reaped_run_ids": reaped, "count": len(reaped)}


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

    log.info(f"  Changed (monitoring detected): {len(changed_ids)}")
    log.info(f"  Stale (not verified in {STALE_THRESHOLD_DAYS}d): {len(stale_ids)}")

    # Quarantine: skip chronically sourceless utilities (structural failures
    # past the threshold) — retrying them burns LLM spend with no new data.
    # A CHANGED monitoring signal always overrides the quarantine, so a
    # utility that finally publishes a real rate page still gets picked up.
    with Session(engine) as session:
        stale_ids, quarantined = _filter_quarantined(session, stale_ids)
    changed_set = set(changed_ids)
    quarantined = sorted(uid for uid in quarantined if uid not in changed_set)
    if quarantined:
        log.info(
            f"  Quarantined (>= {QUARANTINE_STRUCTURAL_THRESHOLD} structural "
            f"failures, no change signal): {len(quarantined)}"
        )

    # Cap the run: changed utilities first (a real signal), stale fills the
    # remainder. Whatever doesn't fit is naturally still stale next month.
    all_ids = list(dict.fromkeys(changed_ids + [i for i in stale_ids if i not in set(changed_ids)]))
    if len(all_ids) > MONTHLY_MAX_UTILITIES:
        log.info(f"  Capping run at {MONTHLY_MAX_UTILITIES} of {len(all_ids)} candidates")
        all_ids = all_ids[:MONTHLY_MAX_UTILITIES]

    log.info(f"  Total unique targets: {len(all_ids)}")

    with Session(engine) as session:
        before_counts = _count_tariffs(session, all_ids)
        run = session.get(RefreshRun, run_id)
        run.utilities_targeted = len(all_ids)
        # Persist exactly which utilities this run dispatched so the audit
        # reaper can scope its reconstruction to them (instead of counting
        # every tariff touched globally during the window).
        run.summary_json = {
            "targeted_utility_ids": all_ids,
            "quarantined_skipped": len(quarantined),
        }
        session.commit()

    if not all_ids:
        log.info("  No utilities to process")
        return {"run_id": run_id, "targeted": 0}

    before_json = json.dumps({str(k): v for k, v in before_counts.items()})

    # Dispatch per-utility tasks as a chord: all process in parallel,
    # then finalize_refresh_run runs once all are done. The hourly
    # reap_stalled_runs task is the safety net if the chord callback
    # ever fails to fire.
    task_group = group(
        process_utility.s(uid, False) for uid in all_ids
    )
    callback = finalize_refresh_run.s(run_id, before_json).on_error(
        log_chord_failure.s()
    )
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
        log.info(f"  Utilities with all-error monitoring sources: {len(error_ids)}")
        # Skip chronically sourceless utilities here too. The quarterly run
        # used to be the place these got retried forever; now a quarantined
        # utility is only re-checked once its recheck window elapses.
        error_ids, quarantined = _filter_quarantined(session, error_ids)
        if quarantined:
            log.info(f"  Quarantined (skipped): {len(quarantined)}")
        before_counts = _count_tariffs(session, error_ids)

    with Session(engine) as session:
        run = session.get(RefreshRun, run_id)
        run.utilities_targeted = len(error_ids)
        run.summary_json = {
            "targeted_utility_ids": error_ids,
            "quarantined_skipped": len(quarantined),
        }
        session.commit()

    if not error_ids:
        log.info("  No error utilities to process")
        return {"run_id": run_id, "targeted": 0}

    before_json = json.dumps({str(k): v for k, v in before_counts.items()})

    task_group = group(
        process_utility.s(uid, True) for uid in error_ids
    )
    callback = finalize_refresh_run.s(run_id, before_json).on_error(
        log_chord_failure.s()
    )
    chord(task_group)(callback)

    log.info(f"  Dispatched {len(error_ids)} tasks for run {run_id}")
    return {"run_id": run_id, "targeted": len(error_ids)}


@celery_app.task(
    name="app.tasks.refresh.audit_tou_seasonal_completeness",
    time_limit=600,
    soft_time_limit=580,
)
def audit_tou_seasonal_completeness(limit: int = 5000):
    """Optional nightly stub: read-only scan of incomplete TOU/seasonal shapes.

    Not registered on beat by default (operators can add a crontab entry when
    ready). Logs counts; does not mutate rows or invent clock/season data.
    Prefer ``python -m scripts.check_tou_seasonal_completeness --audit-db``.
    """
    from app.services.tou_seasonal_completeness import (
        SEASONAL_FAMILY,
        TOU_FAMILY,
        evaluate_tariff_completeness,
    )
    from sqlalchemy.orm import selectinload

    in_scope = TOU_FAMILY | SEASONAL_FAMILY
    engine = get_sync_engine()
    total = incomplete = 0
    with Session(engine) as session:
        rows = (
            session.execute(
                select(Tariff)
                .options(selectinload(Tariff.rate_components))
                .where(Tariff.superseded_by_tariff_id.is_(None))
                .where(Tariff.supersede_reason.is_(None))
                .order_by(Tariff.id)
                .limit(limit)
            )
            .scalars()
            .all()
        )
        for t in rows:
            rt = t.rate_type.value if t.rate_type else ""
            if rt not in in_scope:
                continue
            total += 1
            if not evaluate_tariff_completeness(rt, t.rate_components or []).complete:
                incomplete += 1
    summary = {
        "in_scope": total,
        "incomplete": incomplete,
        "limit": limit,
    }
    log.info(
        "TOU/seasonal completeness audit: "
        f"in_scope={total} incomplete={incomplete} (limit={limit})"
    )
    return summary
