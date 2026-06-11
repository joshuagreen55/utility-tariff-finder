"""Report LLM cost by phase and model.

Two modes:

  Rollup (default) — aggregate the llm_cost breakdown stored in recent
  RefreshRun.summary_json records, so you can see where the monthly/quarterly
  spend actually goes:
      python /app/scripts/llm_cost_report.py
      python /app/scripts/llm_cost_report.py --runs 10
      python /app/scripts/llm_cost_report.py --json

  Probe — run the pipeline live against one utility (dry-run, no DB writes)
  and print its per-phase / per-model cost. Useful to sanity-check pricing
  and attribution before a scheduled run:
      python /app/scripts/llm_cost_report.py --probe 1064
"""
from __future__ import annotations

import argparse
import json
import sys

from sqlalchemy import desc, select
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import RefreshRun
from scripts import llm_cost


def _print_breakdown(cost: dict) -> None:
    total = cost.get("total_usd", 0.0)
    print(f"  TOTAL: ${total:.4f}")
    print()
    by_phase = cost.get("by_phase") or {}
    by_model = cost.get("by_model") or {}
    if by_phase:
        print("  By phase:")
        for ph, c in sorted(by_phase.items(), key=lambda x: -x[1]):
            pct = (100.0 * c / total) if total else 0.0
            print(f"    {ph:<10} ${c:>10.4f}  {pct:5.1f}%")
        print()
    if by_model:
        print("  By model:")
        for m, c in sorted(by_model.items(), key=lambda x: -x[1]):
            pct = (100.0 * c / total) if total else 0.0
            print(f"    {m:<10} ${c:>10.4f}  {pct:5.1f}%")
        print()
    detail = cost.get("detail") or {}
    if detail:
        print("  Detail (phase / model: calls, in/out tokens, $):")
        for ph in sorted(detail):
            for key, rec in sorted(detail[ph].items()):
                print(
                    f"    {ph:<8} {key:<8} "
                    f"calls={rec.get('calls', 0):<4} "
                    f"in={rec.get('in', 0):<9} out={rec.get('out', 0):<8} "
                    f"${rec.get('cost', 0.0):.4f}"
                )


def _print_pricing() -> None:
    print("  Assumed pricing (USD per 1M tokens; override via LLM_PRICING_JSON):")
    for key, p in llm_cost.PRICING.items():
        print(
            f"    {key:<10} in=${p.get('in', 0):<7} out=${p.get('out', 0):<7} "
            f"cache_read=${p.get('cache_read', 0)} cache_write=${p.get('cache_write', 0)}"
        )
    print()


def rollup(runs: int, as_json: bool) -> None:
    engine = get_sync_engine()
    with Session(engine) as session:
        rows = (
            session.execute(
                select(RefreshRun)
                .where(RefreshRun.finished_at.is_not(None))
                .order_by(desc(RefreshRun.id))
                .limit(runs)
            )
            .scalars()
            .all()
        )

    summaries = []
    per_run = []
    for r in rows:
        cost = (r.summary_json or {}).get("llm_cost")
        if cost:
            summaries.append(cost)
            per_run.append((r.id, r.utilities_processed, cost.get("total_usd", 0.0)))

    merged = llm_cost.merge_summaries(summaries)

    if as_json:
        print(json.dumps({"runs_analyzed": len(summaries), "merged": merged, "per_run": per_run}, indent=2))
        return

    print("=" * 60)
    print(f"  LLM COST ROLLUP — last {len(summaries)} run(s) with cost data")
    print("=" * 60)
    print()
    _print_pricing()
    if not summaries:
        print("  No runs carry llm_cost yet. Run a refresh after deploying the")
        print("  cost-tracking change, or use --probe <utility_id> for a live test.")
        return
    _print_breakdown(merged)
    if per_run:
        print("  Per run (id: utilities, $):")
        for rid, n, c in per_run:
            avg = (c / n) if n else 0.0
            print(f"    run {rid:<5} {n or 0:>4} utils  ${c:>9.4f}  (${avg:.4f}/util)")


def probe(utility_id: int) -> None:
    from scripts.tariff_pipeline import run_pipeline

    print(f"=== Live cost probe: utility {utility_id} (dry-run, forced extract) ===\n")
    _print_pricing()
    # force_extract bypasses the fingerprint-skip fast path so the probe
    # actually exercises the LLM extraction phases.
    result = run_pipeline(utility_id, dry_run=True, force_extract=True)
    cost = getattr(result, "cost", None) or llm_cost.summary()
    valid = (result.phase4_validation or {}).get("valid", 0)
    print(f"  Pipeline result: {valid} valid tariffs, "
          f"{'errors: ' + '; '.join(result.errors) if result.errors else 'no errors'}")
    print()
    _print_breakdown(cost)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="Report LLM cost by phase/model")
    p.add_argument("--runs", type=int, default=5, help="How many recent runs to roll up")
    p.add_argument("--probe", type=int, help="Run one utility live (dry-run) and print its cost")
    p.add_argument("--json", action="store_true", help="JSON output (rollup mode)")
    args = p.parse_args(argv or sys.argv[1:])

    if args.probe:
        probe(args.probe)
    else:
        rollup(args.runs, args.json)


if __name__ == "__main__":
    main()
