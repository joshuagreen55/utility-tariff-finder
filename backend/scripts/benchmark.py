"""Benchmark pipeline output against ground truth dataset.

Compares the tariffs currently in the database for each ground-truth utility
against the expected tariffs and produces precision, recall, and rate-accuracy
metrics.

Usage:
    cd backend
    python -m scripts.benchmark [--run-pipeline] [--output report.json]
    python -m scripts.benchmark --fixtures tests/fixtures/ground_truth_tou_seasonal.json

Without --run-pipeline, it only compares what's already in the DB (live rows
only). With --run-pipeline, it re-runs the pipeline for each utility first.

Fixtures: ground_truth.json (flat / tiered, 15% legacy tolerance) and
ground_truth_tou_seasonal.json (TOU / seasonal, exact to 1e-5 $/kWh plus
exact clock windows and season dates). A fixture's ``_meta`` may set
``rate_abs_tolerance``, ``fixed_abs_tolerance`` and ``compare_structure``.
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

FIXTURE_DIR = Path(__file__).parent.parent / "tests" / "fixtures"
GROUND_TRUTH_PATH = FIXTURE_DIR / "ground_truth.json"
TOU_SEASONAL_GOLD_PATH = FIXTURE_DIR / "ground_truth_tou_seasonal.json"
DEFAULT_FIXTURES = (GROUND_TRUTH_PATH, TOU_SEASONAL_GOLD_PATH)

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
    expect_computable: bool | None = None
    db_computable: bool | None = None
    db_computable_reasons: list[str] = field(default_factory=list)
    failure_modes: dict = field(default_factory=dict)
    top_failure: str = ""


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


@dataclass(frozen=True)
class Tolerance:
    rate_rel: float = RATE_TOLERANCE
    rate_abs: float | None = None
    fixed_abs: float = FIXED_TOLERANCE
    structure: bool = False

    @classmethod
    def from_meta(cls, meta: dict) -> "Tolerance":
        return cls(
            rate_abs=meta.get("rate_abs_tolerance"),
            fixed_abs=meta.get("fixed_abs_tolerance", FIXED_TOLERANCE),
            structure=bool(meta.get("compare_structure")),
        )


def _rate_close(expected: float, actual: float, ctype: str, tol: Tolerance = Tolerance()) -> bool:
    if ctype == "fixed":
        return abs(expected - actual) <= tol.fixed_abs
    if tol.rate_abs is not None:
        return abs(expected - actual) <= tol.rate_abs
    if expected == 0:
        return actual == 0
    return abs(actual - expected) / abs(expected) <= tol.rate_rel


def _hhmm(v) -> str | None:
    if v is None or v == "":
        return None
    if hasattr(v, "strftime"):
        return v.strftime("%H:%M")
    s = str(v).strip()
    return "00:00" if s in ("24:00", "24:00:00") else s[:5]


def _structure_key(c) -> tuple:
    get = c.get if isinstance(c, dict) else (lambda k, d=None: getattr(c, k, d))
    return (
        _hhmm(get("period_start_time")),
        _hhmm(get("period_end_time")),
        (get("day_type") or None),
        get("season_start_month"), get("season_start_day"),
        get("season_end_month"), get("season_end_day"),
        get("tier_min_kwh"), get("tier_max_kwh"),
    )


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


def _compare_components(
    gt_components: list[dict], db_components: list[RateComponent], tol: Tolerance = Tolerance(),
) -> tuple[float, float, list[dict]]:
    """Compare ground truth components against DB components.

    With ``tol.structure`` a DB component only matches a gold component when
    its clock window, day type, season dates and tier bounds are identical.
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
            if tol.structure and _structure_key(dc) != _structure_key(gc):
                continue
            diff = abs(float(dc.rate_value) - gc_rate)
            if diff < best_diff:
                best_diff = diff
                best_di = di

        if best_di is not None:
            dc = db_components[best_di]
            if _rate_close(gc_rate, float(dc.rate_value), gc_type, tol):
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
                "issue": "missing_structure" if tol.structure else "missing_in_db",
                "structure": list(_structure_key(gc)) if tol.structure else None,
            })

    precision = len(matched_db) / len(db_components) if db_components else 0.0
    recall = len(matched_gt) / len(gt_components) if gt_components else 0.0
    return round(precision, 3), round(recall, 3), errors


FAILURE_MODES = (
    "product_match", "wrong_price", "missing_clock", "day_type",
    "missing_season", "tier_bounds", "missing_component", "extra_component",
)


def failure_taxonomy(gt_components: list[dict], db_components: list, errors: list[dict],
                     tol: Tolerance = Tolerance()) -> dict[str, int]:
    """Count why a matched tariff misses its gold, per gold component.

    ``wrong_price``: a row with the gold structure exists but its value is
    off. For a missing structure, the closest-priced DB row of the same type
    names the field that differs: clock window, day type, season dates or
    tier bounds; no same-priced row at all is ``missing_component``.
    """
    counts = {m: 0 for m in FAILURE_MODES}
    get = lambda c, k: c.get(k) if isinstance(c, dict) else getattr(c, k, None)  # noqa: E731

    def ctype(c) -> str:
        v = get(c, "component_type")
        return getattr(v, "value", v)

    for e in errors:
        if e.get("actual") is not None:
            counts["wrong_price"] += 1
            continue
        gold = next(
            (g for g in gt_components
             if g["component_type"] == e["component_type"] and g["rate_value"] == e["expected"]
             and list(_structure_key(g)) == (e.get("structure") or list(_structure_key(g)))),
            None,
        )
        same_price = [
            d for d in db_components
            if ctype(d) == e["component_type"]
            and _rate_close(e["expected"], float(get(d, "rate_value")), e["component_type"], tol)
        ]
        if gold is None or not same_price:
            counts["missing_component"] += 1
            continue
        gk = _structure_key(gold)

        def distance(d) -> int:
            dk = _structure_key(d)
            return sum(a != b for a, b in zip(gk, dk))

        dk = _structure_key(min(same_price, key=distance))
        if gk[0:2] != dk[0:2]:
            counts["missing_clock"] += 1
        elif gk[2] != dk[2]:
            counts["day_type"] += 1
        elif gk[3:7] != dk[3:7]:
            counts["missing_season"] += 1
        else:
            counts["tier_bounds"] += 1
    counts["extra_component"] = max(0, len(db_components) - len(gt_components))
    return counts


def top_failure(modes: dict[str, int], *, matched: bool) -> str:
    if not matched:
        return "product_match"
    ranked = [m for m in FAILURE_MODES if m != "extra_component" and modes.get(m)]
    if ranked:
        return max(ranked, key=lambda m: modes[m])
    return "extra_component" if modes.get("extra_component") else "none"


def resolve_utility_id(session: Session, gt_entry: dict) -> int | None:
    if gt_entry.get("utility_id"):
        return gt_entry["utility_id"]
    from app.models import Utility

    rows = session.execute(
        select(Utility.id).where(
            Utility.name == gt_entry["name"],
            Utility.state_province == gt_entry["state"],
        )
    ).scalars().all()
    return rows[0] if len(rows) == 1 else None


def benchmark_utility(session: Session, gt_entry: dict, tol: Tolerance = Tolerance()) -> UtilityResult:
    from app.services.computable import evaluate_computable

    uid = resolve_utility_id(session, gt_entry)
    result = UtilityResult(
        utility_id=uid or 0,
        name=gt_entry["name"],
        state=gt_entry["state"],
        gt_tariff_count=len(gt_entry["tariffs"]),
    )

    db_tariffs = [] if uid is None else list(
        session.execute(
            select(Tariff).where(
                Tariff.utility_id == uid,
                Tariff.superseded_by_tariff_id.is_(None),
                Tariff.supersede_reason.is_(None),
            )
        ).scalars().all()
    )
    result.db_tariff_count = len(db_tariffs)

    matched_db_ids = set()

    for gt_t in gt_entry["tariffs"]:
        best = _find_best_match(gt_t, [t for t in db_tariffs if t.id not in matched_db_ids])
        if best:
            matched_db_ids.add(best.id)
            comps = list(best.rate_components)
            prec, rec, errs = _compare_components(gt_t["components"], comps, tol)
            verdict = evaluate_computable(best.rate_type, comps, name=best.name)
            modes = failure_taxonomy(gt_t["components"], comps, errs, tol)
            result.matches.append(TariffMatch(
                gt_name=gt_t["name"],
                gt_class=gt_t["customer_class"],
                db_name=best.name,
                db_class=best.customer_class.value,
                matched=True,
                component_precision=prec,
                component_recall=rec,
                rate_errors=errs,
                expect_computable=gt_t.get("expect_computable"),
                db_computable=verdict.computable,
                db_computable_reasons=list(verdict.reasons),
                failure_modes=modes,
                top_failure=top_failure(modes, matched=True),
            ))
        else:
            result.missing_tariffs.append(gt_t["name"])
            result.matches.append(TariffMatch(
                gt_name=gt_t["name"],
                gt_class=gt_t["customer_class"],
                matched=False,
                expect_computable=gt_t.get("expect_computable"),
                top_failure="product_match",
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
    parser.add_argument(
        "--fixtures", nargs="+", default=[str(p) for p in DEFAULT_FIXTURES],
        help="Ground-truth fixture files (default: flat/tiered + TOU/seasonal gold)",
    )
    parser.add_argument(
        "--baseline", type=str,
        help="Earlier --output report; exit 1 if computable agreement drops or "
             "rate / structure errors rise against it",
    )
    args = parser.parse_args()

    gt_utilities: list[tuple[dict, Tolerance]] = []
    for path in args.fixtures:
        with open(path) as f:
            data = json.load(f)
        tol = Tolerance.from_meta(data.get("_meta") or {})
        gt_utilities.extend((u, tol) for u in data["utilities"])
    log.info(f"Ground truth: {len(gt_utilities)} utilities from {len(args.fixtures)} fixture(s)")

    if args.run_pipeline:
        from scripts.tariff_pipeline import run_pipeline, cleanup_between_utilities
        log.info("Re-running pipeline for ground truth utilities...")
        with Session(get_sync_engine()) as session:
            resolved = [(gt, resolve_utility_id(session, gt)) for gt, _tol in gt_utilities]
        for i, (gt, uid) in enumerate(resolved, 1):
            if uid is None:
                log.warning(f"  [{i}/{len(resolved)}] {gt['name']}: utility not found, skipped")
                continue
            log.info(f"  [{i}/{len(gt_utilities)}] {gt['name']} (id={uid})")
            try:
                run_pipeline(uid, dry_run=False)
            except Exception as e:
                log.error(f"    Pipeline failed: {e}")
            cleanup_between_utilities()

    engine = get_sync_engine()
    results: list[UtilityResult] = []

    with Session(engine) as session:
        for gt, tol in gt_utilities:
            r = benchmark_utility(session, gt, tol)
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

    structure_errors = sum(
        1 for r in results for m in r.matches for e in m.rate_errors
        if e.get("issue") == "missing_structure"
    )
    computable_checked = [m for m in matched_entries if m.expect_computable is not None]
    computable_agree = sum(1 for m in computable_checked if m.db_computable == m.expect_computable)

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
    print(f"  of which clock/season/tier structure missing: {structure_errors}")
    print(f"Computable agreement:       {computable_agree}/{len(computable_checked)}")
    print(f"")

    print("Per-utility breakdown:")
    for r in results:
        status = "OK" if r.tariff_recall == 1.0 else "MISSING" if r.db_tariff_count == 0 else "PARTIAL"
        n_matched = sum(1 for m in r.matches if m.matched)
        n_errors = sum(len(m.rate_errors) for m in r.matches)
        print(f"  {r.name[:35]:35s} {r.state:3s} {status:8s} {n_matched}/{r.gt_tariff_count} tariffs  {n_errors} rate errors")

    gold = [(r, m) for r in results for m in r.matches if m.expect_computable is not None]
    mode_totals = {k: 0 for k in FAILURE_MODES}
    for _r, m in gold:
        for k, v in (m.failure_modes or {}).items():
            mode_totals[k] = mode_totals.get(k, 0) + v
        if not m.matched:
            mode_totals["product_match"] += 1
    if gold:
        print("\nTOU / seasonal gold scoreboard (top failure per tariff):")
        for r, m in gold:
            verdict = "computable" if m.db_computable else "NOT computable" if m.matched else "-"
            modes = ", ".join(f"{k}={v}" for k, v in (m.failure_modes or {}).items() if v)
            print(f"  {r.name[:22]:22s} {m.gt_name[:40]:40s} {m.top_failure or 'none':17s} "
                  f"{verdict:15s} {modes} {'; '.join(m.db_computable_reasons[:2])}")
        print("  totals: " + ", ".join(f"{k}={v}" for k, v in mode_totals.items()))

    summary = {
        "utilities_tested": len(results),
        "utilities_with_data": utilities_with_data,
        "utilities_perfect": utilities_perfect,
        "tariff_recall": round(total_matched / total_gt_tariffs, 3) if total_gt_tariffs else 0,
        "avg_component_precision": round(avg_comp_precision, 3),
        "avg_component_recall": round(avg_comp_recall, 3),
        "total_rate_errors": total_rate_errors,
        "structure_errors": structure_errors,
        "computable_agreement": f"{computable_agree}/{len(computable_checked)}",
        "computable_agree": computable_agree,
        "computable_checked": len(computable_checked),
        "gold_failure_modes": mode_totals,
    }

    regressions = []
    if args.baseline:
        with open(args.baseline) as f:
            regressions = gold_regressions(json.load(f).get("summary") or {}, summary)
        print("\nAgainst baseline " + args.baseline + ": "
              + ("; ".join(regressions) if regressions else "no regression"))

    if args.output:
        report = {
            "summary": summary,
            "utilities": [asdict(r) for r in results],
        }
        with open(args.output, "w") as f:
            json.dump(report, f, indent=2, default=str)
        log.info(f"\nReport written to {args.output}")
    if regressions:
        sys.exit(1)


def _agree(summary: dict) -> int | None:
    if "computable_agree" in summary:
        return int(summary["computable_agree"])
    raw = str(summary.get("computable_agreement") or "")
    return int(raw.split("/")[0]) if "/" in raw else None


def gold_regressions(baseline: dict, current: dict) -> list[str]:
    """Human-readable regressions of ``current`` against ``baseline``."""
    out = []
    b, c = _agree(baseline), _agree(current)
    if b is not None and c is not None and c < b:
        out.append(f"computable agreement {c} < {b}")
    for key, label in (("total_rate_errors", "rate errors"), ("structure_errors", "structure misses")):
        if key in baseline and current.get(key, 0) > baseline[key]:
            out.append(f"{label} {current[key]} > {baseline[key]}")
    return out


if __name__ == "__main__":
    main()
