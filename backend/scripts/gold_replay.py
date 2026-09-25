"""Replay the TOU / seasonal gold set through the post-LLM pipeline (no LLM, no DB).

Each gold tariff is fed to Phase 4 as if a model had returned it, then mapped
to rate-component rows exactly as ``store_tariffs`` would, and scored with the
benchmark's strict comparison and the computable contract. It answers: *if
the model extracted the book perfectly, would the pipeline keep it?* A failure
here is a pipeline bug (mapping, dedupe, rider folding, guardrails), not a
model miss.

``--forms`` also replays each tariff in the shapes models actually emit
(cents as printed, "7:00 a.m.", inclusive ":59" ends, one "weekends and
holidays" row, month names), so mapping regressions fail too.

    cd backend
    python -m scripts.gold_replay            # exits 1 on any gold regression
    python -m scripts.gold_replay --forms
"""
from __future__ import annotations

import argparse
import copy
import json
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path

from scripts import tariff_pipeline as tp
from scripts.benchmark import TOU_SEASONAL_GOLD_PATH, Tolerance, _compare_components

_MONTH_NAMES = (
    "", "January", "February", "March", "April", "May", "June", "July",
    "August", "September", "October", "November", "December",
)


@dataclass
class ReplayResult:
    utility: str
    tariff: str
    code: str | None
    form: str
    expect_computable: bool | None
    computable: bool
    reasons: list[str] = field(default_factory=list)
    gold_components: int = 0
    stored_components: int = 0
    rate_errors: int = 0
    structure_errors: int = 0
    rejected: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return (
            not self.rejected
            and self.rate_errors == 0
            and (self.expect_computable is None or self.computable == self.expect_computable)
        )


def _clock_12h(hhmm: str | None) -> str | None:
    if not hhmm:
        return hhmm
    h, m = (int(x) for x in hhmm.split(":"))
    if (h, m) == (0, 0):
        return "12:00 a.m."
    if (h, m) == (12, 0):
        return "noon"
    return f"{(h % 12) or 12}:{m:02d} {'a.m.' if h < 12 else 'p.m.'}"


def _inclusive_end(hhmm: str | None) -> str | None:
    """'11:00' → '10:59 a.m.' (books that print inclusive window ends)."""
    if not hhmm:
        return hhmm
    h, m = (int(x) for x in hhmm.split(":"))
    total = (h * 60 + m - 1) % 1440
    return _clock_12h(f"{total // 60:02d}:{total % 60:02d}")


def model_forms(components: list[dict]) -> list[dict]:
    """Re-express gold rows the way models print them (same meaning)."""
    out: list[dict] = []
    for c in copy.deepcopy(components):
        if c.get("component_type") in ("energy", "adjustment") and c.get("unit") == "$/kWh":
            c["rate_value"] = round(float(c["rate_value"]) * 100, 6)
            c["unit"] = "¢/kWh"
        start, end = c.get("period_start_time"), c.get("period_end_time")
        if start and end and not (start == end == "00:00"):
            c["period_start_time"] = _clock_12h(start)
            c["period_end_time"] = _inclusive_end(end)
        if c.get("season_start_month"):
            c["season_start_month"] = _MONTH_NAMES[c["season_start_month"]]
        if c.get("day_type") == "weekday":
            c["day_type"] = "Monday to Friday"
        out.append(c)

    # Fold weekend + holiday twins into one "weekends and holidays" row.
    def twin_key(c: dict) -> str:
        return json.dumps({k: v for k, v in c.items() if k != "day_type"}, sort_keys=True, default=str)

    weekend = {twin_key(c): c for c in out if c.get("day_type") == "weekend"}
    folded: list[dict] = []
    for c in out:
        if c.get("day_type") == "holiday" and twin_key(c) in weekend:
            continue
        if c.get("day_type") == "weekend" and any(
            o.get("day_type") == "holiday" and twin_key(o) == twin_key(c) for o in out
        ):
            c = {**c, "day_type": "Weekends and holidays"}
        folded.append(c)
    return folded


def replay_tariff(utility: dict, gold: dict, tol: Tolerance, *, form: str = "gold") -> ReplayResult:
    from app.services.computable import evaluate_computable

    comps = copy.deepcopy(gold["components"])
    if form == "model":
        comps = model_forms(comps)
    item = {k: gold.get(k, "") for k in ("name", "code", "customer_class", "rate_type", "effective_date")}
    item.update(confidence=0.95, components=comps)
    extracted = tp._parse_extraction_response([item], utility.get("rate_url") or "")
    report, valid = tp.phase4_validate(extracted, utility["name"], utility["state"])
    result = ReplayResult(
        utility=utility["name"], tariff=gold["name"], code=gold.get("code"), form=form,
        expect_computable=gold.get("expect_computable"), computable=False,
        gold_components=len(gold["components"]),
    )
    if not valid:
        result.rejected = [i for issue in report["issues"] for i in issue["issues"]]
        return result
    et = valid[0]
    et.components = tp.dedupe_rate_components(et.components)
    rows = tp._build_rate_components(et)
    verdict = evaluate_computable(et.rate_type, rows, name=et.name)
    _p, _r, errors = _compare_components(gold["components"], rows, tol)
    result.computable = verdict.computable
    result.reasons = list(verdict.reasons)
    result.stored_components = len(rows)
    result.rate_errors = len(errors)
    result.structure_errors = sum(1 for e in errors if e.get("issue") == "missing_structure")
    return result


def replay(path: Path = TOU_SEASONAL_GOLD_PATH, *, forms: tuple[str, ...] = ("gold",)) -> list[ReplayResult]:
    data = json.loads(Path(path).read_text())
    tol = Tolerance.from_meta(data.get("_meta") or {})
    return [
        replay_tariff(u, t, tol, form=f)
        for u in data["utilities"] for t in u["tariffs"] for f in forms
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--fixtures", default=str(TOU_SEASONAL_GOLD_PATH))
    parser.add_argument("--forms", action="store_true", help="also replay model-style forms")
    parser.add_argument("--json", action="store_true", help="print JSON instead of a table")
    args = parser.parse_args(argv)
    logging.disable(logging.CRITICAL)

    results = replay(Path(args.fixtures), forms=("gold", "model") if args.forms else ("gold",))
    if args.json:
        print(json.dumps([{**r.__dict__, "ok": r.ok} for r in results], indent=2))
    else:
        for r in results:
            print(
                f"{'OK ' if r.ok else 'BAD'} {r.form:5s} {r.utility[:24]:24s} {str(r.code)[:14]:14s} "
                f"rows {r.stored_components:2d}/{r.gold_components:2d} "
                f"computable={r.computable!s:5s} errors={r.rate_errors:2d} "
                f"{', '.join(r.rejected or r.reasons)[:80]}"
            )
        agree = sum(1 for r in results if r.expect_computable is None or r.computable == r.expect_computable)
        print(f"\nComputable agreement: {agree}/{len(results)}; "
              f"rate errors: {sum(r.rate_errors for r in results)}; "
              f"structure misses: {sum(r.structure_errors for r in results)}")
    return 0 if all(r.ok for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
