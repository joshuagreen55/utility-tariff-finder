import asyncio
from collections import Counter, defaultdict
from urllib.parse import urlparse

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from sqlalchemy import case, literal_column, select, desc, func, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db, get_sync_engine
from app.models import MonitoringSource, MonitoringLog, Utility, Tariff, MonitoringStatus, ReviewStatus, RefreshRun
from app.schemas.monitoring import (
    MonitoringSourceRead,
    MonitoringLogRead,
    MonitoringLogUpdate,
    MonitoringSourceUrlUpdate,
    MonitoringCheckIdsRequest,
    ErrorCategoriesResponse,
    ErrorCategoryDetail,
    ErrorCategoryStateBreakdown,
    CoverageResponse,
    CountryCoverageSummary,
    StateCoverage,
)
from app.services.monitoring_runner import check_one_source_id, run_monitoring_concurrent

router = APIRouter()


def _sync_run_concurrent(source_ids: list[int], concurrency: int, per_host: int, delay_ms: int) -> None:
    engine = get_sync_engine()
    asyncio.run(
        run_monitoring_concurrent(
            engine,
            source_ids,
            concurrency=concurrency,
            per_host_limit=per_host,
            delay_ms=delay_ms,
        )
    )


@router.get("/monitoring/sources", response_model=list[MonitoringSourceRead])
async def list_monitoring_sources(
    status: MonitoringStatus | None = None,
    limit: int = Query(default=50, le=500),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    stmt = (
        select(MonitoringSource, Utility.name.label("utility_name"))
        .join(Utility, MonitoringSource.utility_id == Utility.id)
    )
    if status:
        stmt = stmt.where(MonitoringSource.status == status)
    stmt = stmt.order_by(desc(MonitoringSource.last_changed_at)).offset(offset).limit(limit)

    result = await db.execute(stmt)
    rows = result.all()

    return [
        MonitoringSourceRead(
            id=src.id,
            utility_id=src.utility_id,
            utility_name=uname,
            url=src.url,
            check_frequency_days=src.check_frequency_days,
            last_checked_at=src.last_checked_at,
            last_changed_at=src.last_changed_at,
            status=src.status,
        )
        for src, uname in rows
    ]


@router.patch("/monitoring/sources/{source_id}", response_model=MonitoringSourceRead)
async def update_monitoring_source_url(
    source_id: int,
    update: MonitoringSourceUrlUpdate,
    db: AsyncSession = Depends(get_db),
):
    """Update the monitored URL (e.g. after an agent finds a working tariff page)."""
    parsed = urlparse(update.url.strip())
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise HTTPException(status_code=400, detail="Invalid URL; must start with http:// or https://")

    stmt = (
        select(MonitoringSource, Utility.name.label("utility_name"))
        .join(Utility, MonitoringSource.utility_id == Utility.id)
        .where(MonitoringSource.id == source_id)
    )
    result = await db.execute(stmt)
    row = result.first()
    if not row:
        raise HTTPException(status_code=404, detail="Monitoring source not found")

    src, uname = row[0], row[1]
    src.url = update.url.strip()
    await db.commit()
    await db.refresh(src)

    return MonitoringSourceRead(
        id=src.id,
        utility_id=src.utility_id,
        utility_name=uname or "",
        url=src.url,
        check_frequency_days=src.check_frequency_days,
        last_checked_at=src.last_checked_at,
        last_changed_at=src.last_changed_at,
        status=src.status,
    )


@router.get("/monitoring/logs", response_model=list[MonitoringLogRead])
async def list_monitoring_logs(
    source_id: int | None = None,
    changed_only: bool = False,
    review_status: ReviewStatus | None = None,
    limit: int = Query(default=50, le=500),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(MonitoringLog)
    if source_id:
        stmt = stmt.where(MonitoringLog.source_id == source_id)
    if changed_only:
        stmt = stmt.where(MonitoringLog.changed.is_(True))
    if review_status:
        stmt = stmt.where(MonitoringLog.review_status == review_status)
    stmt = stmt.order_by(desc(MonitoringLog.checked_at)).offset(offset).limit(limit)

    result = await db.execute(stmt)
    return result.scalars().all()


@router.patch("/monitoring/logs/{log_id}", response_model=MonitoringLogRead)
async def update_monitoring_log(
    log_id: int,
    update: MonitoringLogUpdate,
    db: AsyncSession = Depends(get_db),
):
    stmt = select(MonitoringLog).where(MonitoringLog.id == log_id)
    result = await db.execute(stmt)
    log = result.scalar_one_or_none()
    if not log:
        raise HTTPException(status_code=404, detail="Monitoring log not found")

    log.review_status = update.review_status
    await db.commit()
    await db.refresh(log)
    return log


@router.post("/monitoring/sources/{source_id}/check")
async def check_single_source_endpoint(source_id: int):
    """Run an on-demand check for a single monitoring source."""
    result = await check_one_source_id(get_sync_engine(), source_id)
    if result.get("outcome") == "not_found":
        raise HTTPException(status_code=404, detail="Monitoring source not found")
    if result.get("outcome") == "error":
        return {"status": "error", "error": result.get("error")}
    return {
        "status": result.get("outcome"),
        "content_hash": result.get("content_hash"),
    }


@router.post("/monitoring/sources/check-ids")
async def check_monitoring_source_ids(
    body: MonitoringCheckIdsRequest,
    background_tasks: BackgroundTasks,
    wait: bool = Query(default=False, description="If true, run synchronously and return full results"),
    concurrency: int = Query(default=32, ge=1, le=80),
    per_host: int = Query(default=4, ge=1, le=20),
    delay_ms: int = Query(default=0, ge=0, le=5000),
):
    """Check specific source IDs (for targeted reruns after URL fixes)."""
    ids = list(dict.fromkeys(body.source_ids))
    if len(ids) > 2000:
        raise HTTPException(status_code=400, detail="Too many source_ids (max 2000)")

    engine = get_sync_engine()
    if wait:
        return await run_monitoring_concurrent(
            engine,
            ids,
            concurrency=concurrency,
            per_host_limit=per_host,
            delay_ms=delay_ms,
        )

    background_tasks.add_task(_sync_run_concurrent, ids, concurrency, per_host, delay_ms)
    return {"message": f"Queued {len(ids)} sources for checking", "count": len(ids)}


@router.post("/monitoring/check-all")
async def check_all_sources_endpoint(
    background_tasks: BackgroundTasks,
    state_province: str | None = None,
    limit: int = Query(default=500, le=10000),
    concurrency: int = Query(default=32, ge=1, le=80),
    per_host: int = Query(default=4, ge=1, le=20),
    delay_ms: int = Query(default=0, ge=0, le=5000),
    db: AsyncSession = Depends(get_db),
):
    """Queue on-demand checks for many monitoring sources (oldest-checked first)."""
    stmt = select(MonitoringSource.id)
    if state_province:
        stmt = stmt.join(Utility, MonitoringSource.utility_id == Utility.id).where(
            Utility.state_province == state_province
        )
    stmt = stmt.order_by(MonitoringSource.last_checked_at.asc().nullsfirst()).limit(limit)
    result = await db.execute(stmt)
    source_ids = list(result.scalars().all())

    background_tasks.add_task(_sync_run_concurrent, source_ids, concurrency, per_host, delay_ms)
    return {
        "message": f"Queued {len(source_ids)} sources for checking",
        "count": len(source_ids),
    }


@router.get("/monitoring/stats")
async def monitoring_stats(db: AsyncSession = Depends(get_db)):
    """Get overall monitoring statistics."""
    total = await db.execute(select(func.count(MonitoringSource.id)))
    total_count = total.scalar()

    checked = await db.execute(
        select(func.count(MonitoringSource.id)).where(MonitoringSource.last_checked_at.isnot(None))
    )
    checked_count = checked.scalar()

    changed = await db.execute(
        select(func.count(MonitoringSource.id)).where(MonitoringSource.status == MonitoringStatus.CHANGED)
    )
    changed_count = changed.scalar()

    errors = await db.execute(
        select(func.count(MonitoringSource.id)).where(MonitoringSource.status == MonitoringStatus.ERROR)
    )
    error_count = errors.scalar()

    pending_reviews = await db.execute(
        select(func.count(MonitoringLog.id))
        .where(MonitoringLog.review_status == ReviewStatus.PENDING)
        .where(MonitoringLog.changed.is_(True))
    )
    pending_count = pending_reviews.scalar()

    return {
        "total_sources": total_count,
        "checked": checked_count,
        "unchecked": total_count - checked_count,
        "changed": changed_count,
        "errors": error_count,
        "pending_reviews": pending_count,
    }


@router.get("/monitoring/dead-utilities")
async def list_dead_utilities(
    limit: int = Query(default=50, le=500),
    db: AsyncSession = Depends(get_db),
):
    """List active utilities where ALL monitoring sources are in error status."""
    total_per_utility = (
        select(
            MonitoringSource.utility_id,
            func.count(MonitoringSource.id).label("total"),
        )
        .group_by(MonitoringSource.utility_id)
        .subquery()
    )
    errors_per_utility = (
        select(
            MonitoringSource.utility_id,
            func.count(MonitoringSource.id).label("error_count"),
        )
        .where(MonitoringSource.status == MonitoringStatus.ERROR)
        .group_by(MonitoringSource.utility_id)
        .subquery()
    )
    stmt = (
        select(
            Utility.id,
            Utility.name,
            Utility.state_province,
            Utility.country,
            Utility.website_url,
            total_per_utility.c.total.label("source_count"),
            errors_per_utility.c.error_count,
        )
        .join(total_per_utility, Utility.id == total_per_utility.c.utility_id)
        .join(errors_per_utility, Utility.id == errors_per_utility.c.utility_id)
        .where(Utility.is_active.is_(True))
        .where(total_per_utility.c.total == errors_per_utility.c.error_count)
        .order_by(desc(total_per_utility.c.total))
        .limit(limit)
    )
    result = await db.execute(stmt)
    rows = result.all()
    return [
        {
            "utility_id": r.id,
            "name": r.name,
            "state_province": r.state_province,
            "country": str(r.country.value) if r.country else None,
            "website_url": r.website_url,
            "source_count": r.source_count,
            "error_count": r.error_count,
        }
        for r in rows
    ]


ERROR_CATEGORY_DESCRIPTIONS = {
    "403_forbidden": "Server returned 403 Forbidden — site blocks automated requests or requires JavaScript rendering",
    "404_not_found": "Page not found (404) — URL has moved or been removed",
    "5xx_server_error": "Server error (500/502/503) — utility website is down or misconfigured",
    "timeout": "Request timed out — server too slow or unresponsive",
    "ssl_certificate": "SSL/TLS certificate error — expired, self-signed, or misconfigured cert",
    "dns_failed": "DNS resolution failed — domain no longer exists or is misconfigured",
    "connection_error": "Connection refused or reset — server not accepting connections",
    "empty_content": "Page loaded but contained no extractable text content (likely JavaScript-only SPA)",
    "auth_required": "Authentication required (401) — page behind a login wall",
    "redirect_error": "Too many redirects or redirect loop",
    "placeholder_url": "URL is a placeholder (not a real tariff page URL)",
    "other": "Unclassified error",
}


def _classify_error(diff_summary: str | None, url: str) -> str:
    if url and "placeholder" in url.lower():
        return "placeholder_url"
    if not diff_summary:
        return "other"
    msg = diff_summary.lower()
    if "403" in msg or "forbidden" in msg:
        return "403_forbidden"
    if "404" in msg:
        return "404_not_found"
    if any(c in msg for c in ("500", "502", "503")):
        return "5xx_server_error"
    if "timeout" in msg or "timed out" in msg:
        return "timeout"
    if "ssl" in msg or "certificate" in msg:
        return "ssl_certificate"
    if "name or service not known" in msg or "nodename" in msg or "getaddrinfo" in msg:
        return "dns_failed"
    if ("connect" in msg or "refused" in msg) and "403" not in msg:
        return "connection_error"
    if "empty content" in msg:
        return "empty_content"
    if "401" in msg or "unauthorized" in msg:
        return "auth_required"
    if "redirect" in msg:
        return "redirect_error"
    return "other"


@router.get("/monitoring/error-categories", response_model=ErrorCategoriesResponse)
async def error_categories(db: AsyncSession = Depends(get_db)):
    """Categorize all error monitoring sources by failure reason.

    For each error source, looks up the latest monitoring log to determine what
    kind of error occurred, then groups results by category with domain and
    geographic breakdowns.
    """
    latest_log = (
        select(
            MonitoringLog.source_id,
            MonitoringLog.diff_summary,
            func.row_number()
            .over(partition_by=MonitoringLog.source_id, order_by=desc(MonitoringLog.checked_at))
            .label("rn"),
        )
        .subquery()
    )

    stmt = (
        select(
            MonitoringSource.id,
            MonitoringSource.url,
            MonitoringSource.utility_id,
            Utility.name.label("utility_name"),
            Utility.country,
            Utility.state_province,
            latest_log.c.diff_summary,
        )
        .join(Utility, MonitoringSource.utility_id == Utility.id)
        .outerjoin(
            latest_log,
            (latest_log.c.source_id == MonitoringSource.id) & (latest_log.c.rn == 1),
        )
        .where(MonitoringSource.status == MonitoringStatus.ERROR)
        .where(Utility.is_active.is_(True))
    )

    result = await db.execute(stmt)
    rows = result.all()

    cat_sources: dict[str, list] = defaultdict(list)
    cat_utilities: dict[str, set] = defaultdict(set)
    cat_domains: dict[str, Counter] = defaultdict(Counter)
    cat_country: dict[str, Counter] = defaultdict(Counter)
    state_errors: Counter = Counter()
    state_utilities: dict[tuple, set] = defaultdict(set)
    all_utility_ids: set[int] = set()

    for row in rows:
        cat = _classify_error(row.diff_summary, row.url)
        cat_sources[cat].append(row.id)
        cat_utilities[cat].add(row.utility_id)
        all_utility_ids.add(row.utility_id)

        try:
            domain = urlparse(row.url).netloc
        except Exception:
            domain = "unknown"
        cat_domains[cat][domain] += 1

        country_val = row.country.value if row.country else "unknown"
        cat_country[cat][country_val] += 1

        key = (country_val, row.state_province or "unknown")
        state_errors[key] += 1
        state_utilities[key].add(row.utility_id)

    categories = []
    for cat in sorted(cat_sources, key=lambda c: -len(cat_sources[c])):
        categories.append(
            ErrorCategoryDetail(
                category=cat,
                description=ERROR_CATEGORY_DESCRIPTIONS.get(cat, ""),
                source_count=len(cat_sources[cat]),
                utility_count=len(cat_utilities[cat]),
                top_domains=[d for d, _ in cat_domains[cat].most_common(5)],
                by_country=dict(cat_country[cat].most_common()),
            )
        )

    by_state = sorted(
        [
            ErrorCategoryStateBreakdown(
                country=k[0],
                state=k[1],
                error_sources=v,
                affected_utilities=len(state_utilities[k]),
            )
            for k, v in state_errors.items()
        ],
        key=lambda s: -s.error_sources,
    )

    return ErrorCategoriesResponse(
        total_error_sources=len(rows),
        total_affected_utilities=len(all_utility_ids),
        categories=categories,
        by_state=by_state,
    )


@router.get("/analytics/coverage", response_model=CoverageResponse)
async def coverage_analysis(db: AsyncSession = Depends(get_db)):
    """Compute monitoring URL and tariff data coverage by country and state.

    For every active utility, determines whether it has at least one working
    monitoring source URL and at least one tariff record.
    """
    working_source_sub = (
        select(MonitoringSource.utility_id)
        .where(MonitoringSource.status.notin_([MonitoringStatus.ERROR, MonitoringStatus.PENDING]))
        .group_by(MonitoringSource.utility_id)
        .subquery()
    )

    tariff_sub = (
        select(Tariff.utility_id)
        .group_by(Tariff.utility_id)
        .subquery()
    )

    stmt = (
        select(
            Utility.country,
            Utility.state_province,
            func.count(Utility.id).label("total"),
            func.count(working_source_sub.c.utility_id).label("with_working_url"),
            func.count(tariff_sub.c.utility_id).label("with_tariff"),
        )
        .outerjoin(working_source_sub, Utility.id == working_source_sub.c.utility_id)
        .outerjoin(tariff_sub, Utility.id == tariff_sub.c.utility_id)
        .where(Utility.is_active.is_(True))
        .group_by(Utility.country, Utility.state_province)
        .order_by(Utility.country, Utility.state_province)
    )

    result = await db.execute(stmt)
    rows = result.all()

    country_totals: dict[str, dict] = defaultdict(lambda: {"total": 0, "url": 0, "tariff": 0})
    by_state = []

    for row in rows:
        country_val = row.country.value if row.country else "unknown"
        total = row.total
        with_url = row.with_working_url
        with_tariff = row.with_tariff

        country_totals[country_val]["total"] += total
        country_totals[country_val]["url"] += with_url
        country_totals[country_val]["tariff"] += with_tariff

        by_state.append(
            StateCoverage(
                country=country_val,
                state=row.state_province or "unknown",
                total_utilities=total,
                with_working_url=with_url,
                with_tariff_data=with_tariff,
                url_coverage_pct=round(with_url / total * 100, 1) if total else 0,
                tariff_coverage_pct=round(with_tariff / total * 100, 1) if total else 0,
            )
        )

    summary = {}
    for country_val, agg in country_totals.items():
        t = agg["total"]
        summary[country_val] = CountryCoverageSummary(
            total_utilities=t,
            with_working_url=agg["url"],
            with_tariff_data=agg["tariff"],
            url_coverage_pct=round(agg["url"] / t * 100, 1) if t else 0,
            tariff_coverage_pct=round(agg["tariff"] / t * 100, 1) if t else 0,
        )

    return CoverageResponse(summary=summary, by_state=by_state)


@router.patch("/monitoring/utilities/{utility_id}/deactivate")
async def deactivate_utility(
    utility_id: int,
    reason: str = Query(default="", description="Why this utility is being deactivated"),
    db: AsyncSession = Depends(get_db),
):
    """Mark a utility as inactive (no longer monitored)."""
    stmt = select(Utility).where(Utility.id == utility_id)
    result = await db.execute(stmt)
    utility = result.scalar_one_or_none()
    if not utility:
        raise HTTPException(status_code=404, detail="Utility not found")

    utility.is_active = False
    await db.commit()
    await db.refresh(utility)

    return {
        "utility_id": utility.id,
        "name": utility.name,
        "is_active": utility.is_active,
        "reason": reason,
    }


@router.get("/monitoring/refresh-runs")
async def list_refresh_runs(
    limit: int = Query(default=20, le=100),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """List past refresh runs with summary stats, most recent first."""
    total_result = await db.execute(select(func.count(RefreshRun.id)))
    total = total_result.scalar() or 0

    stmt = (
        select(RefreshRun)
        .order_by(desc(RefreshRun.started_at))
        .offset(offset)
        .limit(limit)
    )
    result = await db.execute(stmt)
    runs = result.scalars().all()

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "runs": [
            {
                "id": r.id,
                "refresh_type": r.refresh_type.value if r.refresh_type else None,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "finished_at": r.finished_at.isoformat() if r.finished_at else None,
                "duration_minutes": round(
                    (r.finished_at - r.started_at).total_seconds() / 60, 1
                ) if r.finished_at and r.started_at else None,
                "utilities_targeted": r.utilities_targeted,
                "utilities_processed": r.utilities_processed,
                "tariffs_added": r.tariffs_added,
                "tariffs_updated": r.tariffs_updated,
                "tariffs_stale": r.tariffs_stale,
                "errors": r.errors,
                "summary_json": r.summary_json,
            }
            for r in runs
        ],
    }


@router.get("/monitoring/refresh-runs/{run_id}")
async def get_refresh_run(
    run_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Get details of a specific refresh run including error details."""
    stmt = select(RefreshRun).where(RefreshRun.id == run_id)
    result = await db.execute(stmt)
    run = result.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Refresh run not found")

    return {
        "id": run.id,
        "refresh_type": run.refresh_type.value if run.refresh_type else None,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "duration_minutes": round(
            (run.finished_at - run.started_at).total_seconds() / 60, 1
        ) if run.finished_at and run.started_at else None,
        "utilities_targeted": run.utilities_targeted,
        "utilities_processed": run.utilities_processed,
        "tariffs_added": run.tariffs_added,
        "tariffs_updated": run.tariffs_updated,
        "tariffs_stale": run.tariffs_stale,
        "errors": run.errors,
        "summary_json": run.summary_json,
        "error_details": run.error_details,
    }
