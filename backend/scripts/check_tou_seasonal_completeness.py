#!/usr/bin/env python3
"""Flag incomplete live TOU / seasonal shapes (structured-column rules).

CI mode (default): runs the unit-test completeness suite — no DB required.

    cd backend && python -m scripts.check_tou_seasonal_completeness
    cd backend && python -m scripts.check_tou_seasonal_completeness --unittest

Optional DB audit (not used in CI; needs DATABASE_URL / sync engine):

    python -m scripts.check_tou_seasonal_completeness --audit-db --limit 500

Nightly Celery hook: see ``app.tasks.refresh.audit_tou_seasonal_completeness``
(stub; enable via beat when operators want a scheduled DB scan).
"""
from __future__ import annotations

import argparse
import sys
import unittest


def run_unit_suite() -> int:
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromName("tests.test_tou_seasonal_completeness")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


def audit_db(*, limit: int = 500) -> int:
    """Scan live keepers and print incomplete counts. Read-only."""
    from sqlalchemy import select
    from sqlalchemy.orm import Session, selectinload

    from app.db.session import get_sync_engine
    from app.models import Tariff
    from app.services.tou_seasonal_completeness import (
        SEASONAL_FAMILY,
        TOU_FAMILY,
        evaluate_tariff_completeness,
    )

    in_scope = TOU_FAMILY | SEASONAL_FAMILY
    engine = get_sync_engine()
    incomplete = 0
    total = 0
    samples: list[str] = []

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
            result = evaluate_tariff_completeness(rt, t.rate_components or [])
            if not result.complete:
                incomplete += 1
                if len(samples) < 20:
                    samples.append(
                        f"  tariff_id={t.id} rate_type={rt} "
                        f"reasons={list(result.reasons)}"
                    )

    print(f"In-scope live tariffs scanned: {total}")
    print(f"Incomplete (structured rules): {incomplete}")
    if samples:
        print("Sample incomplete:")
        print("\n".join(samples))
    # Audit is informational — exit 0 unless scan itself fails.
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--unittest",
        action="store_true",
        default=True,
        help="Run completeness unit tests (default; CI)",
    )
    parser.add_argument(
        "--audit-db",
        action="store_true",
        help="Read-only DB scan of live TOU/seasonal keepers",
    )
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args(argv)

    if args.audit_db:
        return audit_db(limit=args.limit)
    return run_unit_suite()


if __name__ == "__main__":
    sys.exit(main())
