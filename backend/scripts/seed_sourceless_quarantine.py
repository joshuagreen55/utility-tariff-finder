"""Seed the refresh quarantine with utilities that are *already* known to be
sourceless, so the mechanism takes effect immediately instead of waiting for
three more monthly runs to accumulate a failure streak.

A utility is treated as chronically sourceless when ALL of these hold:

  1. It is active.
  2. It has no live extraction to show for itself — i.e. no non-superseded
     tariff has ever been verified (every tariff is either an unverified
     OpenEI seed with a NULL last_verified_at, or superseded, or it has no
     tariffs at all).
  3. Its web presence is dead: every monitoring source is in ERROR state
     (or it has monitoring sources and none are usable).

Optionally (--from-runs N) it also folds in utilities that appear as
*structural* failures in the error_details of the last N refresh runs
(name+state match), but still requires condition (2) so we never quarantine
a utility that has a working extraction.

Matched utilities get refresh_fail_streak bumped to the quarantine threshold
and refresh_quarantined_at stamped now. A monitoring CHANGED signal or the
slow-cadence recheck (QUARANTINE_RECHECK_DAYS) will still re-attempt them.

Usage:
    python /app/scripts/seed_sourceless_quarantine.py            # dry run
    python /app/scripts/seed_sourceless_quarantine.py --apply
    python /app/scripts/seed_sourceless_quarantine.py --apply --from-runs 6
"""
from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timezone

from sqlalchemy import func, not_, select
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import (
    MonitoringSource,
    MonitoringStatus,
    RefreshRun,
    Tariff,
    Utility,
)
from app.tasks.refresh import QUARANTINE_STRUCTURAL_THRESHOLD

# Same structural markers used by the live classifier, inverted: a run
# error line is "structural" unless it looks transient.
_TRANSIENT = (
    "timeout", "timed out", "connection", "rate limit", "429",
    "temporarily", "503", "502", "504", "crash", "ssl",
)


def _has_no_live_tariff_subq():
    """Subquery of utility_ids that DO have at least one live (non-superseded,
    verified) tariff — these are safe and must be excluded."""
    return (
        select(Tariff.utility_id)
        .where(
            Tariff.superseded_by_tariff_id.is_(None),
            Tariff.supersede_reason.is_(None),
            Tariff.last_verified_at.is_not(None),
        )
        .distinct()
    )


def _all_monitoring_error_ids(session: Session) -> set[int]:
    """Utility ids where every monitoring source is in ERROR state."""
    total_per = (
        select(MonitoringSource.utility_id, func.count(MonitoringSource.id).label("total"))
        .group_by(MonitoringSource.utility_id)
        .subquery()
    )
    error_per = (
        select(MonitoringSource.utility_id, func.count(MonitoringSource.id).label("err"))
        .where(MonitoringSource.status == MonitoringStatus.ERROR)
        .group_by(MonitoringSource.utility_id)
        .subquery()
    )
    rows = session.execute(
        select(total_per.c.utility_id)
        .join(error_per, total_per.c.utility_id == error_per.c.utility_id)
        .where(total_per.c.total == error_per.c.err)
    ).scalars().all()
    return set(rows)


def _structural_failures_from_runs(session: Session, n_runs: int) -> set[tuple[str, str]]:
    """(name, state) pairs that failed structurally in the last N runs."""
    rows = (
        session.execute(
            select(RefreshRun.error_details)
            .where(RefreshRun.error_details.is_not(None))
            .order_by(RefreshRun.id.desc())
            .limit(n_runs)
        )
        .scalars()
        .all()
    )
    pairs: set[tuple[str, str]] = set()
    # Lines look like: "Utility Name (ST): reason; reason"
    pat = re.compile(r"^(.*?)\s*\(([A-Z]{2})\):\s*(.*)$")
    for blob in rows:
        for line in (blob or "").splitlines():
            m = pat.match(line.strip())
            if not m:
                continue
            name, state, reason = m.group(1), m.group(2), m.group(3).lower()
            if any(t in reason for t in _TRANSIENT):
                continue
            pairs.add((name.strip(), state.strip()))
    return pairs


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Write changes (default: dry run)")
    ap.add_argument("--from-runs", type=int, default=0,
                    help="Also fold in structural failures from last N refresh runs")
    args = ap.parse_args(argv or sys.argv[1:])

    engine = get_sync_engine()
    now = datetime.now(timezone.utc)

    with Session(engine) as session:
        has_live = _has_no_live_tariff_subq()

        # Base candidate set: active, no live verified tariff.
        no_live_active = session.execute(
            select(Utility.id, Utility.name, Utility.state_province, Utility.refresh_quarantined_at)
            .where(Utility.is_active.is_(True))
            .where(not_(Utility.id.in_(has_live)))
        ).all()
        no_live_ids = {r[0] for r in no_live_active}
        meta = {r[0]: (r[1], r[2], r[3]) for r in no_live_active}

        all_err = _all_monitoring_error_ids(session)

        # Path 1: no live tariff AND all monitoring in error.
        candidates: dict[int, str] = {}
        for uid in no_live_ids & all_err:
            candidates[uid] = "all monitoring sources in error, no live tariff"

        # Path 2 (optional): structural failure in recent runs, no live tariff.
        if args.from_runs:
            pairs = _structural_failures_from_runs(session, args.from_runs)
            if pairs:
                name_to_id = {
                    (n, s): uid for uid, (n, s, _) in meta.items()
                }
                for (name, state) in pairs:
                    uid = name_to_id.get((name, state))
                    if uid and uid not in candidates:
                        candidates[uid] = "structural failure in recent run(s), no live tariff"

        already = [uid for uid in candidates if meta.get(uid, (None, None, None))[2] is not None]

        print("=" * 64)
        print("  SOURCELESS QUARANTINE SEED")
        print("=" * 64)
        print(f"  Active utilities with NO live verified tariff : {len(no_live_ids)}")
        print(f"  Utilities with ALL monitoring sources in error : {len(all_err)}")
        print(f"  --> Quarantine candidates                      : {len(candidates)}")
        print(f"      (already quarantined                       : {len(already)})")
        print()
        by_reason: dict[str, int] = {}
        for reason in candidates.values():
            by_reason[reason] = by_reason.get(reason, 0) + 1
        for reason, cnt in sorted(by_reason.items(), key=lambda x: -x[1]):
            print(f"    {cnt:>5}  {reason}")
        print()
        print("  Sample:")
        for uid in list(candidates)[:15]:
            name, state, _ = meta.get(uid, ("?", "?", None))
            print(f"    [{uid}] {name} ({state}) — {candidates[uid]}")

        if not args.apply:
            print()
            print(f"  DRY RUN — re-run with --apply to quarantine {len(candidates)} utilities.")
            return

        applied = 0
        for uid in candidates:
            u = session.get(Utility, uid)
            if u is None:
                continue
            u.refresh_fail_streak = max(u.refresh_fail_streak or 0, QUARANTINE_STRUCTURAL_THRESHOLD)
            u.refresh_quarantined_at = now
            u.refresh_last_reason = candidates[uid][:200]
            u.refresh_last_attempt_at = u.refresh_last_attempt_at or now
            applied += 1
        session.commit()
        print()
        print(f"  APPLIED — quarantined {applied} utilities as of {now.isoformat()}.")


if __name__ == "__main__":
    main()
