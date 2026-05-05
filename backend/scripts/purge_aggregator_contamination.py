"""Purge tariffs that were extracted from third-party aggregator pages.

The pipeline maintains a `THIRD_PARTY_DOMAINS` blocklist that prevents
aggregator pages (energybot.com, comparepower.com, etc.) from being
followed during search and extraction. But that only protects *new*
pipeline runs — historical extractions performed before a given domain
was added to the blocklist live on in the database.

This script finds all tariffs whose `source_url` matches the current
blocklist and deletes them. Affected utilities' monitoring sources are
also flipped to CHANGED so the next monthly refresh picks them up
through the (now-clean) pipeline.

Dry-run by default. Pass `--apply` to actually delete.

Usage:
    python -m scripts.purge_aggregator_contamination               # dry-run
    python -m scripts.purge_aggregator_contamination --apply       # delete
"""
import argparse
import sys
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import urlparse

from sqlalchemy import select, text, update
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import (
    MonitoringSource,
    MonitoringStatus,
    Tariff,
    Utility,
)
from scripts.tariff_pipeline import THIRD_PARTY_DOMAINS


def is_blocked_domain(source_url: str | None) -> bool:
    """Same logic the pipeline applies during extraction.

    Strips a single leading `www.` and matches either the exact domain
    or any subdomain of a blocked one.
    """
    if not source_url:
        return False
    try:
        domain = urlparse(source_url).netloc.lower()
    except Exception:
        return False
    if not domain:
        return False
    domain = domain.removeprefix("www.")
    return any(
        domain == d or domain.endswith(f".{d}")
        for d in THIRD_PARTY_DOMAINS
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete (default is dry-run)",
    )
    parser.add_argument(
        "--no-flag-for-refresh",
        action="store_true",
        help="Do not mark affected utilities' monitoring sources as CHANGED",
    )
    args = parser.parse_args()

    eng = get_sync_engine()
    with Session(eng) as s:
        # Fetch every tariff with a non-null source_url so we can apply
        # the same domain logic the pipeline uses. SQL `LIKE` matching
        # would be brittle vs subdomains and edge cases.
        rows = s.execute(
            select(Tariff.id, Tariff.utility_id, Tariff.source_url, Utility.name, Utility.state_province)
            .join(Utility, Utility.id == Tariff.utility_id)
            .where(Tariff.source_url.is_not(None))
        ).all()

        contaminated_ids: list[int] = []
        per_domain: dict[str, int] = defaultdict(int)
        per_utility: dict[tuple[str, str], int] = defaultdict(int)
        utility_ids: set[int] = set()

        for tariff_id, uid, url, uname, state in rows:
            if is_blocked_domain(url):
                contaminated_ids.append(tariff_id)
                domain = urlparse(url).netloc.lower().removeprefix("www.")
                per_domain[domain] += 1
                per_utility[(state or "?", uname or "?")] += 1
                utility_ids.add(uid)

        # Pre-cleanup totals for context
        total_tariffs = s.execute(text("SELECT COUNT(*) FROM tariffs")).scalar()

        print()
        print("=" * 78)
        print(f"AGGREGATOR CONTAMINATION SCAN  ({'APPLY MODE' if args.apply else 'DRY-RUN'})")
        print("=" * 78)
        print(f"  Total tariffs in DB:           {total_tariffs:>7,d}")
        print(f"  Tariffs flagged as contaminated:{len(contaminated_ids):>6,d} "
              f"({100 * len(contaminated_ids) / total_tariffs:.2f}%)")
        print(f"  Distinct utilities affected:    {len(utility_ids):>6,d}")
        print()
        print("--- By blocked domain ---")
        for domain, n in sorted(per_domain.items(), key=lambda kv: -kv[1]):
            print(f"  {domain:<45s} {n:>6,d}")
        print()
        print("--- Top 20 affected utilities ---")
        ranked = sorted(per_utility.items(), key=lambda kv: -kv[1])[:20]
        for (state, uname), n in ranked:
            print(f"  {state:<3s} {uname[:55]:<55s} {n:>4,d}")

        if not contaminated_ids:
            print()
            print("Nothing to clean. Exiting.")
            return 0

        if not args.apply:
            print()
            print("DRY-RUN: no changes made. Re-run with --apply to delete.")
            return 0

        # ------------------------------------------------------------------
        # Apply: delete contaminated tariffs in batches. RateComponent
        # rows cascade via Tariff.rate_components (delete-orphan), and
        # the FK has ondelete=CASCADE as a belt-and-braces measure.
        # ------------------------------------------------------------------
        print()
        print(f"DELETING {len(contaminated_ids):,d} contaminated tariffs...")
        BATCH = 500
        deleted = 0
        for i in range(0, len(contaminated_ids), BATCH):
            chunk = contaminated_ids[i : i + BATCH]
            res = s.execute(
                text("DELETE FROM tariffs WHERE id = ANY(:ids)"),
                {"ids": chunk},
            )
            deleted += res.rowcount or 0
            print(f"  deleted {deleted:,d} / {len(contaminated_ids):,d}")
        s.commit()

        if not args.no_flag_for_refresh:
            print()
            print(f"Flagging {len(utility_ids)} utilities for re-extraction "
                  f"(setting monitoring_sources.status = CHANGED)...")
            now = datetime.now(timezone.utc)
            res = s.execute(
                update(MonitoringSource)
                .where(MonitoringSource.utility_id.in_(utility_ids))
                .values(
                    status=MonitoringStatus.CHANGED,
                    last_changed_at=now,
                )
            )
            s.commit()
            print(f"  flipped {res.rowcount or 0} monitoring sources to CHANGED")

        # Post-cleanup state
        new_total = s.execute(text("SELECT COUNT(*) FROM tariffs")).scalar()
        new_cov = s.execute(
            text(
                "SELECT COUNT(DISTINCT utility_id) FROM tariffs "
                "WHERE utility_id IN (SELECT id FROM utilities WHERE is_active = true)"
            )
        ).scalar()
        n_active = s.execute(
            text("SELECT COUNT(*) FROM utilities WHERE is_active = true")
        ).scalar()

        print()
        print("=" * 78)
        print("DONE")
        print("=" * 78)
        print(f"  Tariffs deleted:           {deleted:>7,d}")
        print(f"  Tariffs remaining in DB:   {new_total:>7,d}")
        print(f"  Coverage now:              {new_cov} / {n_active} "
              f"({100 * new_cov / n_active:.2f}%)")
        print(f"  Utilities flagged for refresh: {len(utility_ids)}")
        return 0


if __name__ == "__main__":
    sys.exit(main())
