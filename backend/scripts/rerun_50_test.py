"""Re-run the new pipeline on 50 selected utilities for before/after comparison."""

import json
import logging
import sys
import time
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from scripts.tariff_pipeline import run_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("rerun_50")

TEST_IDS = [
    51, 95, 97, 110, 112, 198, 234, 238, 243, 246,
    249, 275, 283, 316, 364, 381, 419, 442, 493, 503,
    561, 630, 649, 688, 793, 818, 895, 927, 948, 979,
    1000, 1064, 1113, 1190, 1192, 1210, 1247, 1310, 1314, 1322,
    1329, 1373, 1405, 1412, 1421, 1439, 1452, 1484, 1721, 1791,
]


def main():
    engine = get_sync_engine()
    results = []
    total = len(TEST_IDS)
    overall_start = time.time()

    for i, uid in enumerate(TEST_IDS, 1):
        log.info("[%d/%d] Running pipeline for utility_id=%d", i, total, uid)
        start = time.time()
        try:
            run_pipeline(uid)
            elapsed = time.time() - start
            log.info("[%d/%d] utility_id=%d completed in %.1fs", i, total, uid, elapsed)
            results.append({"utility_id": uid, "status": "ok", "elapsed_s": round(elapsed, 1)})
        except Exception as e:
            elapsed = time.time() - start
            log.error("[%d/%d] utility_id=%d FAILED after %.1fs: %s", i, total, uid, elapsed, e)
            results.append({"utility_id": uid, "status": "error", "error": str(e), "elapsed_s": round(elapsed, 1)})

        # Save progress after each utility
        with open("/app/rerun_50_progress.json", "w") as f:
            json.dump({
                "started": overall_start,
                "completed": i,
                "total": total,
                "elapsed_min": round((time.time() - overall_start) / 60, 1),
                "results": results,
            }, f, indent=2)

    total_min = (time.time() - overall_start) / 60
    ok = sum(1 for r in results if r["status"] == "ok")
    log.info("Done. %d/%d succeeded in %.1f minutes", ok, total, total_min)


if __name__ == "__main__":
    main()
