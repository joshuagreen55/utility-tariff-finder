"""Benchmark pipeline output against ground truth dataset.

Compares the tariffs currently in the database for each ground-truth utility
against the expected tariffs and produces precision, recall, and rate-accuracy
metrics.

Usage:
    cd backend
    python -m scripts.benchmark [--run-pipeline] [--output report.json]

Without --run-pipeline, it only compares what's already in the DB.
With --run-pipeline, it re-runs the pipeline for each ground truth utility first.
"""
import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field, asdict
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import Tariff, RateComponent

log = logging.getLogger("benchmark")
logging.basicConfig(level=logging.INFO, format="%(message)s")

GROUND_TRUTH_PATH = Path(__file__).parent.parent / "tests" / "fixtures" / "ground_truth.json"

RATE_TOLERANCE = 0.15  # 15% relative tolerance for rate value comparison
FIXED_TOLERANCE = 5.0  # $5 absolute tolerance for fixed charges


@dataclass
class TariffMatch:
    gt_name: str
    gt_class: str
    db_name: str | None = None
    db_class: str | None = None
    matched: bool = False
    component_precision: float = 0.0
    component_recall: float = 0.0
    rate_errors: list[dict] = field(default_factory=list)


@dataclass
class UtilityResult:
    utility_id: int
    name: str
    state: str
    gt_tariff_count: int = 0
    db_tariff_count: int = 0
    tariff_precision: float = 0.0
    tariff_recall: float = 0.0
    matches: list[TariffMatch] = field(default_factory=list)
    missing_tariffs: list[str] = field(default_factory=list)
    extra_tariffs: list[str] = field(default_factory=list)


def _normalize(name: str) -> str:
    import re
    n = name.lower().strip()
    n = re.sub(r"[^a-z0-9\s]", "", n)
    return " ".join(n.split())


def _rate_close(expected: float, actual: float, ctype: str) -> bool:
    if ctype == "fixed":
        return abs(expected - actual) <= FIXED_TOLERANCE
    if expected == 0:
        return actual == 0
    return abs(actual - expected) / abs(expected) <= RATE_TOLERANCE


def _find_best_match(gt_tariff: dict, db_tariffs: list[Tariff]) -> Tariff | None:
    """Find the DB tariff that best matches a ground truth tariff."""
    gt_norm = _normalize(gt_tariff["name"])
    gt_class = gt_tariff["customer_class"]

    # Exact name + class match
    for t in db_tariffs:
        if _normalize(t.name) == gt_norm and t.customer_class.value == gt_class:
            return t

    # Fuzzy: check if ground truth name is a substring or vice versa
    for t in db_tariffs:
        t_norm = _normalize(t.name)
        if t.customer_class.value != gt_class:
            continue
        if gt_norm in t_norm or t_norm in gt_norm:
            return t

    # Last resort: same customer class, look for keyword overlap
    gt_words = set(gt_norm.split())
    best, best_overlap = None, 0
    for t in db_tariffs:
        if t.customer_class.value != gt_class:
            continue
        t_words = set(_normalize(t.name).split())
        overlap = len(gt_words & t_words)
        if overlap > best_overlap:
            best, best_overlap = t, overlap
    if best_overlap >= 2:
        return best
    return None


def _compare_components(gt_components: list[dict], db_components: list[RateComponent]) -> tuple[float, float, list[dict]]:
    """Compare ground truth components against DB components.

    Returns (precision, recall, rate_errors).
    """
    if not gt_components:
        return 1.0, 1.0, []

    matched_gt = set()
    matched_db = set()
    errors = []

    for gi, gc in enumerate(gt_components):
        gc_type = gc["component_type"]
        gc_rate = gc["rate_value"]

        best_di = None
        best_diff = float("inf")

        for di, dc in enumerate(db_components):
            if di in matched_db:
                continue
            if dc.component_type.value != gc_type:
                continue
            diff = abs(float(dc.rate_value) - gc_rate)
            if diff < best_diff:
                best_diff = diff
                best_di = di

        if best_di is not None:
            dc = db_components[best_di]
            if _rate_close(gc_rate, float(dc.rate_value), gc_type):
                matched_gt.add(gi)
                matched_db.add(best_di)
            else:
                errors.append({
                    "component_type": gc_type,
                    "expected": gc_rate,
                    "actual": float(dc.rate_value),
                    "tier_label": gc.get("tier_label"),
                    "season": gc.get("season"),
                    "period_label": gc.get("period_label"),
                })
        else:
            errors.append({
                "component_type": gc_type,
                "expected": gc_rate,
                "actual": None,
                "issue": "missing_in_db",
            })

    precision = len(matched_db) / len(db_components) if db_components else 0.0
    recall = len(matched_gt) / len(gt_components) if gt_components else 0.0
    return round(precision, 3), round(recall, 3), errors


def benchmark_utility(session: Session, gt_entry: dict) -> UtilityResult:
    uid = gt_entry["utility_id"]
    result = UtilityResult(
        utility_id=uid,
        name=gt_entry["name"],
        state=gt_entry["state"],
        gt_tariff_count=len(gt_entry["tariffs"]),
    )

    db_tariffs = list(
        session.execute(
            select(Tariff).where(Tariff.utility_id == uid)
        ).scalars().all()
    )
    result.db_tariff_count = len(db_tariffs)

    matched_db_ids = set()

    for gt_t in gt_entry["tariffs"]:
        best = _find_best_match(gt_t, [t for t in db_tariffs if t.id not in matched_db_ids])
        if best:
            matched_db_ids.add(best.id)
            comps = list(best.rate_components)
            prec, rec, errs = _compare_components(gt_t["components"], comps)
            result.matches.append(TariffMatch(
                gt_name=gt_t["name"],
                gt_class=gt_t["customer_class"],
                db_name=best.name,
                db_class=best.customer_class.value,
                matched=True,
                component_precision=prec,
                component_recall=rec,
                rate_errors=errs,
            ))
        else:
            result.missing_tariffs.append(gt_t["name"])
            result.matches.append(TariffMatch(
                gt_name=gt_t["name"],
                gt_class=gt_t["customer_class"],
                matched=False,
            ))

    for t in db_tariffs:
        if t.id not in matched_db_ids and t.customer_class.value in ("residential", "commercial"):
            result.extra_tariffs.append(t.name)

    n_gt = len(gt_entry["tariffs"])
    n_matched = sum(1 for m in result.matches if m.matched)
    result.tariff_recall = round(n_matched / n_gt, 3) if n_gt else 0.0
    result.tariff_precision = round(n_matched / len(db_tariffs), 3) if db_tariffs else 0.0

    return result


def main():
    parser = argparse.ArgumentParser(description="Benchmark pipeline against ground truth")
    parser.add_argument("--run-pipeline", action="store_true", help="Re-run pipeline before benchmarking")
    parser.add_argument("--output", type=str, help="Write JSON report to file")
    args = parser.parse_args()

    with open(GROUND_TRUTH_PATH) as f:
        gt_data = json.load(f)

    gt_utilities = gt_data["utilities"]
    log.info(f"Ground truth: {len(gt_utilities)} utilities")

    if args.run_pipeline:
        from scripts.tariff_pipeline import run_pipeline, cleanup_between_utilities
        log.info("Re-running pipeline for ground truth utilities...")
        for i, gt in enumerate(gt_utilities, 1):
            uid = gt["utility_id"]
            log.info(f"  [{i}/{len(gt_utilities)}] {gt['name']} (id={uid})")
            try:
                run_pipeline(uid, dry_run=False)
            except Exception as e:
                log.error(f"    Pipeline failed: {e}")
            cleanup_between_utilities()

    engine = get_sync_engine()
    results: list[UtilityResult] = []

    with Session(engine) as session:
        for gt in gt_utilities:
            r = benchmark_utility(session, gt)
            results.append(r)

    # Summary statistics
    total_gt_tariffs = sum(r.gt_tariff_count for r in results)
    total_matched = sum(sum(1 for m in r.matches if m.matched) for r in results)
    total_missing = sum(len(r.missing_tariffs) for r in results)
    total_rate_errors = sum(
        sum(len(m.rate_errors) for m in r.matches)
        for r in results
    )

    avg_comp_precision = 0.0
    avg_comp_recall = 0.0
    matched_entries = [m for r in results for m in r.matches if m.matched]
    if matched_entries:
        avg_comp_precision = sum(m.component_precision for m in matched_entries) / len(matched_entries)
        avg_comp_recall = sum(m.component_recall for m in matched_entries) / len(matched_entries)

    utilities_with_data = sum(1 for r in results if r.db_tariff_count > 0)
    utilities_perfect = sum(
        1 for r in results
        if r.tariff_recall == 1.0 and all(not m.rate_errors for m in r.matches)
    )

    print("\n" + "=" * 70)
    print("BENCHMARK RESULTS")
    print("=" * 70)
    print(f"Utilities tested:           {len(results)}")
    print(f"Utilities with data:        {utilities_with_data}/{len(results)}")
    print(f"Utilities perfect match:    {utilities_perfect}/{len(results)}")
    print(f"")
    print(f"Tariff-level recall:        {total_matched}/{total_gt_tariffs} ({total_matched/total_gt_tariffs*100:.1f}%)")
    print(f"Missing tariffs:            {total_missing}")
    print(f"Component-level precision:  {avg_comp_precision:.1%}")
    print(f"Component-level recall:     {avg_comp_recall:.1%}")
    print(f"Rate value errors:          {total_rate_errors}")
    print(f"")

    print("Per-utility breakdown:")
    for r in results:
        status = "OK" if r.tariff_recall == 1.0 else "MISSING" if r.db_tariff_count == 0 else "PARTIAL"
        n_matched = sum(1 for m in r.matches if m.matched)
        n_errors = sum(len(m.rate_errors) for m in r.matches)
        print(f"  {r.name[:35]:35s} {r.state:3s} {status:8s} {n_matched}/{r.gt_tariff_count} tariffs  {n_errors} rate errors")

    if args.output:
        report = {
            "summary": {
                "utilities_tested": len(results),
                "utilities_with_data": utilities_with_data,
                "utilities_perfect": utilities_perfect,
                "tariff_recall": round(total_matched / total_gt_tariffs, 3) if total_gt_tariffs else 0,
                "avg_component_precision": round(avg_comp_precision, 3),
                "avg_component_recall": round(avg_comp_recall, 3),
                "total_rate_errors": total_rate_errors,
            },
            "utilities": [asdict(r) for r in results],
        }
        with open(args.output, "w") as f:
            json.dump(report, f, indent=2, default=str)
        log.info(f"\nReport written to {args.output}")


if __name__ == "__main__":
    main()
