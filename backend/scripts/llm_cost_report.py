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

The rollup also includes spend recorded by scripts outside refresh runs
(Track B, campaign chunks, opus_audit) from logs/llm_cost_ledger.jsonl
over the last --ledger-days days.
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
        print()
    outcomes = cost.get("tier_outcomes") or {}
    if outcomes:
        print("  Extraction-tier calls (did the tier return anything? — not correctness):")
        opus_cost = _opus_escalation_cost(cost)
        for key in sorted(outcomes):
            rec = outcomes[key]
            hit, miss = rec.get("hit", 0), rec.get("miss", 0)
            calls = hit + miss
            rate = (100.0 * hit / calls) if calls else 0.0
            extra = ""
            if key == "opus" and calls:
                wasted = opus_cost * (miss / calls) if calls else 0.0
                extra = f"  (~${wasted:.2f} on 0-yield escalations; excludes identify)"
            print(f"    {key:<8} {hit:>4} hit / {miss:>4} miss  = {rate:4.0f}% hit rate{extra}")
        print()
    acceptance = cost.get("tier_acceptance") or {}
    if acceptance:
        print("  Accepted-after-validation yield (tariffs surviving Phase 4, per tier):")
        for key in sorted(acceptance):
            rec = acceptance[key]
            ret, acc = rec.get("returned", 0), rec.get("accepted", 0)
            rate = (100.0 * acc / ret) if ret else 0.0
            print(f"    {key:<9} {acc:>5} accepted / {ret:>5} returned = {rate:4.0f}%")
        print()
    aborts = cost.get("aborts") or {}
    if aborts:
        print("  Aborted calls (still billed; unpriced = usage not reported):")
        for key, rec in sorted(aborts.items()):
            print(f"    {key:<9} {rec.get('aborted', 0):>4} aborted, {rec.get('unpriced', 0):>4} unpriced")
        print()


def _opus_escalation_cost(cost: dict) -> float:
    """Opus spend on tier-3 escalations only (long-doc identify is tagged
    phase3_identify and is not an escalation outcome)."""
    detail = cost.get("detail") or {}
    if not detail:
        return (cost.get("by_model") or {}).get("opus", 0.0) or 0.0
    return sum(
        (models.get("opus") or {}).get("cost", 0.0)
        for ph, models in detail.items()
        if ph != "phase3_identify"
    )


def _print_pricing() -> None:
    print("  Assumed pricing (USD per 1M tokens; override via LLM_PRICING_JSON):")
    for key, p in llm_cost.PRICING.items():
        print(
            f"    {key:<10} in=${p.get('in', 0):<7} out=${p.get('out', 0):<7} "
            f"cache_read=${p.get('cache_read', 0)} cache_write=${p.get('cache_write', 0)}"
        )
    print()


def _print_ledger(rows: list[dict], days: int) -> None:
    if not rows:
        return
    by_source: dict[str, float] = {}
    for r in rows:
        key = str(r.get("source", "?")).split(":", 1)[0]
        by_source[key] = by_source.get(key, 0.0) + (r.get("cost") or {}).get("total_usd", 0.0)
    print(f"  Script spend outside refresh runs (last {days}d, {len(rows)} runs):")
    for src, c in sorted(by_source.items(), key=lambda x: -x[1]):
        print(f"    {src:<14} ${c:>10.4f}")
    print()


def rollup(runs: int, as_json: bool, ledger_days: int = 90) -> None:
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
    ledger = llm_cost.read_ledger(since_days=ledger_days)
    ledger_merged = llm_cost.merge_summaries([r.get("cost") for r in ledger])

    if as_json:
        print(json.dumps({
            "runs_analyzed": len(summaries),
            "merged": merged,
            "per_run": per_run,
            "script_ledger": {"days": ledger_days, "runs": len(ledger), "merged": ledger_merged},
        }, indent=2))
        return

    print("=" * 60)
    print(f"  LLM COST ROLLUP — last {len(summaries)} run(s) with cost data")
    print("=" * 60)
    print()
    _print_pricing()
    _print_ledger(ledger, ledger_days)
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
    p.add_argument("--ledger-days", type=int, default=90, help="Script-ledger window (days)")
    args = p.parse_args(argv or sys.argv[1:])

    if args.probe:
        probe(args.probe)
    else:
        rollup(args.runs, args.json, args.ledger_days)


if __name__ == "__main__":
    main()
