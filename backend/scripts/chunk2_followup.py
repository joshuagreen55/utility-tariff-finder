"""Chunk 2 follow-up: retry Duke Florida + re-run Track B on Chunk 2 utilities."""
import json
import sys
import time
from datetime import datetime, timezone

from sqlalchemy import text, update as sa_update
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import Utility
from scripts.tariff_pipeline import run_pipeline

# Import Track B helpers from supersede_via_llm
from scripts.supersede_via_llm import CONF_RANK, HAIKU_MODEL, _pair_one_utility
import anthropic

CHUNK2_IDS = [1064, 870, 246, 819, 858, 382, 316, 850, 1020, 238]

DUKE_FL_ID = 382
DUKE_FL_URL = "https://www.duke-energy.com/home/billing/rates/index-of-rate-schedules?jur=FL01"


def retry_duke_fl(session: Session) -> dict:
    util = session.get(Utility, DUKE_FL_ID)
    if not util:
        raise SystemExit(f"utility {DUKE_FL_ID} not found")
    print(f"\n=== Duke Florida retry ===")
    print(f"  Setting rate_page_url_override -> {DUKE_FL_URL}")
    util.rate_page_url_override = DUKE_FL_URL
    session.commit()

    before = session.execute(
        text("""
          SELECT COUNT(*) FILTER (WHERE last_verified_at IS NOT NULL
                                  AND superseded_by_tariff_id IS NULL),
                 COUNT(*) FILTER (WHERE last_verified_at IS NULL
                                  AND openei_id IS NOT NULL
                                  AND superseded_by_tariff_id IS NULL)
          FROM tariffs WHERE utility_id = :uid
        """),
        {"uid": DUKE_FL_ID},
    ).first()
    print(f"  Before: fresh={before[0]}, old={before[1]}")

    t0 = time.time()
    result = run_pipeline(DUKE_FL_ID, dry_run=False, comprehensive=False)
    valid = (result.phase4_validation or {}).get("valid", 0)
    elapsed = time.time() - t0

    after = session.execute(
        text("""
          SELECT COUNT(*) FILTER (WHERE last_verified_at IS NOT NULL
                                  AND superseded_by_tariff_id IS NULL),
                 COUNT(*) FILTER (WHERE last_verified_at IS NULL
                                  AND openei_id IS NOT NULL
                                  AND superseded_by_tariff_id IS NULL)
          FROM tariffs WHERE utility_id = :uid
        """),
        {"uid": DUKE_FL_ID},
    ).first()
    print(f"  After:  fresh={after[0]}, old={after[1]}  ({valid} valid extracted, {elapsed:.0f}s)")
    if result.errors:
        print(f"  Errors: {result.errors[:3]}")
    return {
        "utility_id": DUKE_FL_ID,
        "fresh_before": before[0],
        "fresh_after": after[0],
        "old_before": before[1],
        "old_after": after[1],
        "valid_extracted": valid,
        "errors": result.errors,
    }


def track_b_chunk2(session: Session, apply: bool, min_conf: str = "high") -> dict:
    from app.models.tariff import Tariff

    min_rank = CONF_RANK[min_conf]
    client = anthropic.Anthropic()
    print(f"\n=== Track B re-run on Chunk 2 ({len(CHUNK2_IDS)} utilities, min_conf={min_conf}) ===")

    total_proposed = 0
    total_applied = 0

    for uid in CHUNK2_IDS:
        util = session.get(Utility, uid)
        if not util:
            continue

        fresh_q = session.execute(
            text("""
              SELECT id, name, customer_class, rate_type FROM tariffs
              WHERE utility_id = :uid AND last_verified_at IS NOT NULL
                AND superseded_by_tariff_id IS NULL
              ORDER BY customer_class, name
            """),
            {"uid": uid},
        ).all()
        stranded_q = session.execute(
            text("""
              SELECT id, name, customer_class, rate_type FROM tariffs
              WHERE utility_id = :uid AND last_verified_at IS NULL
                AND openei_id IS NOT NULL AND superseded_by_tariff_id IS NULL
              ORDER BY customer_class, name
            """),
            {"uid": uid},
        ).all()
        if not fresh_q or not stranded_q:
            print(f"  util_id={uid} {util.name[:30]:30s}  skip (fresh={len(fresh_q)}, old={len(stranded_q)})")
            continue

        fresh = [dict(r._mapping) for r in fresh_q]
        stranded = [dict(r._mapping) for r in stranded_q]

        try:
            pairings = _pair_one_utility(client, util.name, util.state_province, fresh, stranded)
        except Exception as e:
            print(f"  util_id={uid} FAIL: {e}")
            continue

        fresh_ids = {f["id"] for f in fresh}
        stranded_ids = {s["id"] for s in stranded}
        applied = 0
        non_null = [p for p in pairings if p.get("fresh_id") is not None]
        for p in non_null:
            if CONF_RANK.get(p.get("confidence", "low"), 0) < min_rank:
                continue
            if p["fresh_id"] not in fresh_ids or p["stranded_id"] not in stranded_ids:
                continue
            applied += 1
            if apply:
                session.execute(
                    sa_update(Tariff)
                    .where(Tariff.id == p["stranded_id"])
                    .values(superseded_by_tariff_id=p["fresh_id"], supersede_reason="llm_absorb")
                )

        total_proposed += len(non_null)
        total_applied += applied
        if apply:
            session.commit()
        print(f"  util_id={uid} {util.state_province} {util.name[:28]:28s}  "
              f"fresh={len(fresh):>3}  old={len(stranded):>3}  "
              f"proposed={len(non_null):>3}  applied={applied:>3}")

    return {"proposed": total_proposed, "applied": total_applied}


def main():
    apply = "--apply" in sys.argv
    engine = get_sync_engine()

    with Session(engine) as session:
        duke_result = retry_duke_fl(session)
        tb_result = track_b_chunk2(session, apply=apply)

    print("\n=== DONE ===")
    print(json.dumps({"duke_fl": duke_result, "track_b": tb_result, "applied": apply}, indent=2))


if __name__ == "__main__":
    main()
