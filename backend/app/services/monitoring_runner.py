"""
Shared monitoring check logic: single-source check and concurrent batches.

Used by the CLI (run_monitoring), FastAPI routes, and background tasks.
Each concurrent task uses its own SQLAlchemy Session (thread/async safe pattern).
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Callable, Awaitable
from urllib.parse import urlparse

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import MonitoringLog, MonitoringSource, MonitoringStatus, ReviewStatus
from app.services.monitor import compute_diff_summary, fetch_and_hash_url


def _netloc(url: str) -> str:
    p = urlparse(url)
    return (p.netloc or "unknown").lower()


def _load_source_info(engine, source_id: int) -> tuple[int | None, str | None, str | None]:
    """Sync: load utility_id, url, last_content_hash from DB."""
    with Session(engine) as session:
        source = session.get(MonitoringSource, source_id)
        if not source:
            return None, None, None
        return source.utility_id, source.url, source.last_content_hash


def _persist_result(
    engine, source_id: int, result: dict, last_hash: str | None
) -> dict[str, Any]:
    """Sync: write check outcome to DB and return result dict."""
    with Session(engine) as session:
        source = session.get(MonitoringSource, source_id)
        if not source:
            return {"source_id": source_id, "outcome": "not_found"}

        now = datetime.now(timezone.utc)

        if result["error"] or not result["content_hash"]:
            error_msg = result["error"] or "Empty content"
            source.status = MonitoringStatus.ERROR
            source.last_checked_at = now
            session.add(
                MonitoringLog(
                    source_id=source.id,
                    content_hash="error",
                    changed=False,
                    diff_summary=f"Error: {error_msg}",
                    review_status=ReviewStatus.PENDING,
                )
            )
            session.commit()
            return {
                "source_id": source_id,
                "utility_id": source.utility_id,
                "url": source.url,
                "outcome": "error",
                "error": error_msg,
            }

        content_hash = result["content_hash"]
        changed = last_hash is not None and last_hash != content_hash

        if changed:
            diff_summary = compute_diff_summary(None, result["content_preview"])
            source.status = MonitoringStatus.CHANGED
            source.last_changed_at = now
        else:
            diff_summary = None
            source.status = MonitoringStatus.UNCHANGED

        source.last_checked_at = now
        source.last_content_hash = content_hash

        session.add(
            MonitoringLog(
                source_id=source.id,
                content_hash=content_hash,
                changed=changed,
                diff_summary=diff_summary,
                review_status=ReviewStatus.PENDING if changed else ReviewStatus.DISMISSED,
            )
        )
        session.commit()

        return {
            "source_id": source_id,
            "utility_id": source.utility_id,
            "url": source.url,
            "outcome": "changed" if changed else "unchanged",
            "content_hash": content_hash,
        }


async def check_one_source_id(engine, source_id: int) -> dict[str, Any]:
    """
    Fetch URL for monitoring source, persist hash/log, update source status.
    DB operations run in a thread so they don't block the async event loop.
    """
    loop = asyncio.get_running_loop()
    utility_id, url, last_hash = await loop.run_in_executor(
        None, _load_source_info, engine, source_id
    )
    if url is None:
        return {"source_id": source_id, "outcome": "not_found"}

    result = await fetch_and_hash_url(url)

    return await loop.run_in_executor(
        None, _persist_result, engine, source_id, result, last_hash
    )


async def run_monitoring_concurrent(
    engine,
    source_ids: list[int],
    *,
    concurrency: int = 32,
    per_host_limit: int = 4,
    delay_ms: int = 0,
    id_to_url: dict[int, str] | None = None,
    progress: Callable[[int, int], Awaitable[None] | None] | None = None,
) -> dict[str, Any]:
    """
    Run checks for many source IDs with a global concurrency limit and per-host limit.

    id_to_url: optional map to avoid an extra DB read per task for host bucketing;
               if omitted, URLs are loaded in one query first.
    """
    if not source_ids:
        return {
            "counts": {"checked": 0, "unchanged": 0, "changed": 0, "errors": 0, "not_found": 0},
            "results": [],
        }

    url_map: dict[int, str] = {}
    if id_to_url is not None:
        url_map = {sid: id_to_url[sid] for sid in source_ids if sid in id_to_url}
    else:
        with Session(engine) as session:
            rows = session.execute(
                select(MonitoringSource.id, MonitoringSource.url).where(MonitoringSource.id.in_(source_ids))
            ).all()
            url_map = {int(r[0]): str(r[1]) for r in rows}

    host_semaphores: dict[str, asyncio.Semaphore] = {}

    def get_host_sem(host: str) -> asyncio.Semaphore:
        if host not in host_semaphores:
            host_semaphores[host] = asyncio.Semaphore(max(1, per_host_limit))
        return host_semaphores[host]

    results: list[dict[str, Any]] = []
    counts: dict[str, int] = defaultdict(int)

    async def run_one(idx: int, sid: int) -> None:
        url = url_map.get(sid, "")
        host = _netloc(url)
        hsem = get_host_sem(host)
        async with hsem:
            if delay_ms > 0:
                await asyncio.sleep(delay_ms / 1000)
            try:
                res = await check_one_source_id(engine, sid)
            except Exception as e:  # noqa: BLE001
                res = {
                    "source_id": sid,
                    "utility_id": None,
                    "url": url,
                    "outcome": "error",
                    "error": f"exception: {e}",
                }
            results.append(res)
            oc = str(res.get("outcome") or "")
            if oc in ("unchanged", "changed", "error", "not_found"):
                counts[oc] += 1
            else:
                counts["errors"] += 1
            if progress:
                p = progress(idx + 1, len(source_ids))
                if asyncio.iscoroutine(p):
                    await p

    # Process in bounded batches to avoid overwhelming the event loop / connection pool
    batch_size = max(1, concurrency)
    for batch_start in range(0, len(source_ids), batch_size):
        batch = source_ids[batch_start : batch_start + batch_size]
        await asyncio.gather(*(run_one(batch_start + i, sid) for i, sid in enumerate(batch)))

    err_total = int(counts["error"]) + int(counts.get("errors", 0))
    results.sort(key=lambda r: int(r.get("source_id") or 0))

    return {
        "counts": {
            "checked": len(source_ids),
            "unchanged": int(counts["unchanged"]),
            "changed": int(counts["changed"]),
            "errors": err_total,
            "not_found": int(counts["not_found"]),
        },
        "results": results,
    }
