"""
Run monitoring checks for utility tariff source URLs.

Standalone script for cron / VM — no Celery required.
Supports concurrent checks for full baseline in under an hour.

Usage:
    python -m scripts.run_monitoring --all --concurrency 32 --output-summary ./logs/summary.json
    python -m scripts.run_monitoring --limit 500 --state BC --delay-ms
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import MonitoringSource, Utility
from app.services.monitoring_runner import run_monitoring_concurrent


def _load_source_ids(
    engine,
    *,
    limit: int | None,
    state_province: str | None,
) -> list[int]:
    with Session(engine) as session:
        stmt = select(MonitoringSource.id)
        if state_province:
            stmt = stmt.join(Utility, MonitoringSource.utility_id == Utility.id).where(
                Utility.state_province == state_province
            )
        stmt = stmt.order_by(MonitoringSource.last_checked_at.asc().nullsfirst())
        if limit is not None:
            stmt = stmt.limit(limit)
        return list(session.execute(stmt).scalars().all())


async def _run(
    *,
    limit: int | None,
    state_province: str | None,
    concurrency: int,
    per_host_limit: int,
    delay_ms: int,
    output_summary: str | None,
) -> dict:
    engine = get_sync_engine()
    ids = _load_source_ids(engine, limit=limit, state_province=state_province)
    run_id = str(uuid.uuid4())
    started = datetime.now(timezone.utc)

    print(
        f"Run {run_id[:8]}… | {len(ids)} sources | "
        f"concurrency={concurrency} per_host={per_host_limit} delay_ms={delay_ms}"
        + (f" | state={state_province}" if state_province else "")
    )

    t0 = time.time()
    batch = await run_monitoring_concurrent(
        engine,
        ids,
        concurrency=concurrency,
        per_host_limit=per_host_limit,
        delay_ms=delay_ms,
    )
    elapsed = time.time() - t0
    finished = datetime.now(timezone.utc)

    c = batch["counts"]
    print(f"\nCompleted in {elapsed:.1f}s")
    print(f"  Checked:   {c['checked']}")
    print(f"  Unchanged: {c['unchanged']}")
    print(f"  Changed:   {c['changed']}")
    print(f"  Errors:    {c['errors']}")
    print(f"  Not found: {c['not_found']}")

    error_details = []
    for r in sorted(batch["results"], key=lambda x: int(x.get("source_id", 0))):
        if r.get("outcome") == "error":
            error_details.append(
                {
                    "source_id": r.get("source_id"),
                    "utility_id": r.get("utility_id"),
                    "url": r.get("url"),
                    "message": r.get("error"),
                }
            )

    summary = {
        "run_id": run_id,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "elapsed_seconds": round(elapsed, 2),
        "params": {
            "limit": limit,
            "state_province": state_province,
            "concurrency": concurrency,
            "per_host_limit": per_host_limit,
            "delay_ms": delay_ms,
        },
        "counts": c,
        "error_details": error_details,
    }

    if output_summary:
        with open(output_summary, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        print(f"\nWrote summary: {output_summary}")

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Run monitoring checks (concurrent batch)")
    parser.add_argument("--limit", type=int, default=100, help="Max sources (ignored if --all)")
    parser.add_argument("--all", action="store_true", help="Check every monitoring source")
    parser.add_argument("--state", type=str, default=None, help="Filter by state/province")
    parser.add_argument("--concurrency", type=int, default=32, help="Max parallel requests globally")
    parser.add_argument("--per-host", type=int, default=4, dest="per_host_limit", help="Max parallel requests per hostname")
    parser.add_argument("--delay-ms", type=int, default=0, help="Optional delay inside each task (politeness)")
    parser.add_argument("--output-summary", type=str, default=None, help="Write JSON summary for downstream agents")
    args = parser.parse_args()

    lim: int | None = None if args.all else args.limit

    asyncio.run(
        _run(
            limit=lim,
            state_province=args.state,
            concurrency=args.concurrency,
            per_host_limit=args.per_host_limit,
            delay_ms=args.delay_ms,
            output_summary=args.output_summary,
        )
    )


if __name__ == "__main__":
    main()
