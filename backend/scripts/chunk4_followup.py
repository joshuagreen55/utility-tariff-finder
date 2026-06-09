"""Chunk 4 follow-up: retry failures + Track B on winners."""
import json
import sys
import time
from datetime import datetime, timezone

from sqlalchemy import text, update as sa_update
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import Utility
from scripts.supersede_via_llm import CONF_RANK, _pair_one_utility
from scripts.tariff_pipeline import run_pipeline
import anthropic

# RefreshRun #12 reported 7 ok; these had fresh extractions or re-verification.
CHUNK4_SUCCESS_IDS = [249, 51, 344, 872, 10, 122, 1309]

RETRY_OVERRIDES = {
    1339: {
        "url": "https://www.energy.gov/sites/prod/files/2013/06/f1/SCE%26G-4-E.pdf",
        "website": "https://www.energy.gov/sepa",
    },
    304: {
        "url": (
            "https://www.michigan.gov/-/media/Project/Websites/mpsc/consumer/"
            "rate-books/electric/dte/dtee1cur.pdf"
        ),
        "website": "https://www.dteenergy.com",
    },
    872: {
        "url": (
            "https://www.pacificpower.net/content/dam/pcorp/documents/en/pacificpower/"
            "rates-regulation/oregon/tariffs/rates/"
            "004_Residential_Service_Delivery_Service.pdf"
        ),
        "website": "https://www.pacificpower.net",
    },
}


def retry_utilities(session: Session) -> list[dict]:
    results = []
    for uid, cfg in RETRY_OVERRIDES.items():
        util = session.get(Utility, uid)
        if not util:
            continue
        print(f"\n=== Retry utility {uid}: {util.name} ===")
        print(f"  Override: {cfg['url'][:80]}...")
        util.rate_page_url_override = cfg["url"]
        if cfg.get("website") and not util.website_url:
            util.website_url = cfg["website"]
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
            {"uid": uid},
        ).first()
        print(f"  Before: fresh={before[0]}, old={before[1]}")

        t0 = time.time()
        pipeline_result = run_pipeline(
            uid, dry_run=False, comprehensive=False, skip_search=True
        )
        valid = (pipeline_result.phase4_validation or {}).get("valid", 0)
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
            {"uid": uid},
        ).first()
        print(
            f"  After:  fresh={after[0]}, old={after[1]}  "
            f"({valid} valid extracted, {elapsed:.0f}s)"
        )
        if pipeline_result.errors:
            print(f"  Errors: {pipeline_result.errors[:3]}")

        results.append(
            {
                "utility_id": uid,
                "utility_name": util.name,
                "fresh_before": before[0],
                "fresh_after": after[0],
                "old_before": before[1],
                "old_after": after[1],
                "valid_extracted": valid,
                "errors": pipeline_result.errors,
            }
        )
    return results


def track_b_chunk4(session: Session, apply: bool, min_conf: str = "high") -> dict:
    from app.models.tariff import Tariff

    min_rank = CONF_RANK[min_conf]
    client = anthropic.Anthropic()
    print(
        f"\n=== Track B on Chunk 4 winners "
        f"({len(CHUNK4_SUCCESS_IDS)} utilities, min_conf={min_conf}) ==="
    )

    total_proposed = 0
    total_applied = 0

    for uid in CHUNK4_SUCCESS_IDS:
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
            print(
                f"  util_id={uid} {util.name[:30]:30s}  "
                f"skip (fresh={len(fresh_q)}, old={len(stranded_q)})"
            )
            continue

        fresh = [dict(r._mapping) for r in fresh_q]
        stranded = [dict(r._mapping) for r in stranded_q]

        try:
            pairings = _pair_one_utility(
                client, util.name, util.state_province, fresh, stranded
            )
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
                    .values(
                        superseded_by_tariff_id=p["fresh_id"],
                        supersede_reason="llm_absorb",
                    )
                )

        total_proposed += len(non_null)
        total_applied += applied
        if apply:
            session.commit()
        print(
            f"  util_id={uid} {util.state_province} {util.name[:28]:28s}  "
            f"fresh={len(fresh):>3}  old={len(stranded):>3}  "
            f"proposed={len(non_null):>3}  applied={applied:>3}"
        )

    return {"proposed": total_proposed, "applied": total_applied}


def main() -> None:
    apply = "--apply" in sys.argv
    skip_retry = "--skip-retry" in sys.argv
    skip_track_b = "--skip-track-b" in sys.argv

    out: dict = {"started_at": datetime.now(timezone.utc).isoformat()}
    engine = get_sync_engine()

    with Session(engine) as session:
        if not skip_retry:
            out["retries"] = retry_utilities(session)
        if not skip_track_b:
            out["track_b"] = track_b_chunk4(session, apply=apply)

    out["applied"] = apply
    print("\n=== DONE ===")
    print(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
