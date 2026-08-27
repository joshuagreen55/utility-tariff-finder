"""Live probe: run the extraction pipeline (dry-run, forced extract, Phase 6
off) against a list of utilities and report the extraction-tier yield —
specifically whether the expensive Opus escalation returns any tariffs.

    python /app/scripts/opus_yield_probe.py 26 30 49 50 257 293
"""
from __future__ import annotations

import sys

from scripts import llm_cost


def main(argv: list[str]) -> None:
    ids = [int(a) for a in argv if a.isdigit()]
    if not ids:
        print("usage: opus_yield_probe.py <utility_id> [<utility_id> ...]")
        return

    from scripts.tariff_pipeline import run_pipeline

    per_util_summaries = []
    print(f"=== Opus-yield probe over {len(ids)} utilities (dry-run) ===\n")
    for uid in ids:
        try:
            result = run_pipeline(uid, dry_run=True, force_extract=True)
            valid = (result.phase4_validation or {}).get("valid", 0)
            # run_pipeline resets the accumulator at its start, so snapshot
            # the summary now — before the next iteration wipes it.
            s = llm_cost.summary()
            per_util_summaries.append(s)
            oc = s.get("tier_outcomes") or {}
            opus = oc.get("opus", {})
            print(
                f"  [{uid}] {result.utility_name[:34]:<34} "
                f"valid={valid:<3} "
                f"opus_hit={opus.get('hit', 0)} opus_miss={opus.get('miss', 0)}"
            )
        except Exception as e:  # noqa: BLE001
            print(f"  [{uid}] ERROR: {e}")

    merged = llm_cost.merge_summaries(per_util_summaries)
    print("\n--- AGGREGATE TIER YIELD ---")
    outcomes = merged.get("tier_outcomes") or {}
    by_model = merged.get("by_model") or {}
    for key in sorted(outcomes):
        rec = outcomes[key]
        hit, miss = rec.get("hit", 0), rec.get("miss", 0)
        calls = hit + miss
        rate = (100.0 * hit / calls) if calls else 0.0
        spend = by_model.get(key, 0.0)
        line = f"  {key:<8} {hit:>3} hit / {miss:>3} miss = {rate:4.0f}% hit  (${spend:.4f} spend)"
        if key == "opus" and calls:
            wasted = spend * (miss / calls)
            line += f"  ~${wasted:.4f} wasted on 0-yield"
        print(line)
    print(f"\n  Total probe LLM spend: ${merged.get('total_usd', 0.0):.4f}")


if __name__ == "__main__":
    main(sys.argv[1:])
