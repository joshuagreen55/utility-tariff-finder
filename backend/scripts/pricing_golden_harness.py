#!/usr/bin/env python3
"""Offline / live harness for the component pricing path vs the golden set.

Offline (default)
    Compile each hand-encoded golden plan and score exact-match accuracy
    against cited official ¢/kWh (or delivery-only ¢ for texas_tdu).

Live
    When COMPONENT_EXTRACTION_ENABLED=1 (or --force-extract), run the dual
    extraction + pre-accept path. Goldens without ``document_text`` are
    skipped. Without a real ``--extract-fn`` / Anthropic wiring this mode
    uses a dry extractor that returns the golden's own components as both
    blind reads (proves gate wiring; does not spend LLM $).

Exit codes
    0 — all scored plans exact-match
    1 — one or more mismatches or compile failures
    2 — no plans scored

Usage
    cd backend && python -m scripts.pricing_golden_harness
    python -m scripts.pricing_golden_harness --mode live --force-extract
    python -m scripts.pricing_golden_harness --json /tmp/harness.json
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

# Allow `python -m scripts.pricing_golden_harness` from backend/
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.pricing.compiler import compile_plan, load_golden_plans  # noqa: E402
from app.services.pricing.extraction import (  # noqa: E402
    ExtractionAccept,
    ExtractionHold,
    component_extraction_enabled,
    dual_extract_components,
)
from app.services.pricing.types import PlanInput  # noqa: E402


def _places(vals: list[Decimal]) -> int:
    places = 0
    for o in vals:
        exp = o.as_tuple().exponent
        if isinstance(exp, int) and exp < 0:
            places = max(places, -exp)
    return places or 3


@dataclass
class PlanScore:
    plan_key: str
    utility_name: str | None
    recipe_code: str
    mode: str
    matched: bool
    has_all_in: bool
    official: list[str] = field(default_factory=list)
    compiled: list[str] = field(default_factory=list)
    error: str | None = None
    hold_reason: str | None = None


@dataclass
class HarnessReport:
    mode: str
    total: int
    scored: int
    matched: int
    held: int
    skipped: int
    accuracy: float
    plans: list[PlanScore] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "total": self.total,
            "scored": self.scored,
            "matched": self.matched,
            "held": self.held,
            "skipped": self.skipped,
            "accuracy": self.accuracy,
            "plans": [asdict(p) for p in self.plans],
        }


def _score_offline(plan: PlanInput) -> PlanScore:
    try:
        compiled = compile_plan(plan)
    except Exception as e:
        return PlanScore(
            plan_key=plan.plan_key,
            utility_name=plan.utility_name,
            recipe_code=plan.recipe_code,
            mode="offline",
            matched=False,
            has_all_in=True,
            error=str(e),
        )
    if compiled.has_all_in:
        want = sorted(plan.official_cents)
    else:
        want = sorted(plan.official_delivery_cents)
    places = _places(want) if want else 4
    got = compiled.cents_sorted(places=places)
    return PlanScore(
        plan_key=plan.plan_key,
        utility_name=plan.utility_name,
        recipe_code=plan.recipe_code,
        mode="offline",
        matched=got == want,
        has_all_in=compiled.has_all_in,
        official=[str(x) for x in want],
        compiled=[str(x) for x in got],
    )


def _unit_anchor(unit: str) -> str:
    """Text that satisfies quote_verifier unit-window patterns."""
    u = (unit or "").strip().lower().replace(" ", "")
    if u in {"¢/kwh", "c/kwh", "cents/kwh"} or u.startswith("¢"):
        return "¢/kWh"
    if u in {"$/kwh", "usd/kwh", "cad/kwh"}:
        return "$/kWh"
    if u in {"percent", "%", "pct"}:
        return "percent of base"
    if u in {"dimensionless", "factor", "x"}:
        return "loss factor multiplier"
    if "mill" in u:
        return "mills/kWh"
    return unit or ""


def _cell_label_prefix(cell: dict) -> str:
    """Season/period/day_type/tier tokens so dry quotes pass row/col G3."""
    parts = []
    season = str(cell.get("season") or "all")
    period = str(cell.get("period") or "all")
    day = str(cell.get("day_type") or "all")
    tier = str(cell.get("tier") or "all")
    if season != "all":
        parts.append(season.replace("_", " "))
    if period != "all":
        parts.append(period.replace("_", "-"))
    if day != "all":
        parts.append(day.replace("_", " "))
    if tier not in {"all", "1"}:
        parts.append(f"tier {tier}")
    return " ".join(parts)


def _dry_quote_and_line(c) -> tuple[str, str]:
    """Return (source_quote, document_line) that agree for G3 grounding.

    The quote must appear verbatim in the line, carry a stored cell amount,
    and include season/period labels so header-aware row/col checks pass.
    """
    anchor = _unit_anchor(c.unit)
    cell = (c.cells or [{}])[0] if c.cells else {}
    amount = str(cell.get("amount") or "")
    labels = _cell_label_prefix(cell)
    name = (c.name or c.code or "").strip()

    if c.source_quote:
        q = str(c.source_quote)
        # Reuse the golden quote only when it already carries the cell amount
        # (or has no digits). A quote citing a different number is rewritten
        # so dry-live G3 amount grounding stays honest.
        import re as _re
        has_digits = bool(_re.search(r"\d", q))
        if amount and amount in q:
            line = f"{labels} {q} ({anchor})".strip()
            return q, line
        if not has_digits:
            line = f"{labels} {q} {amount} ({anchor})".strip()
            # Quote stays the label span; amount sits beside it on the line.
            return q, line

    # Synthesize a quote that embeds amount + labels + unit.
    bits = [x for x in (labels, name, amount, anchor) if x]
    quote = " ".join(bits) if bits else f"{c.code} {anchor}"
    return quote, quote


def _dry_extract_fn(plan: PlanInput):
    """Return the golden components as both 'blind' reads (no LLM spend)."""
    payload = []
    for c in plan.components:
        quote, _line = _dry_quote_and_line(c)
        payload.append({
            "code": c.code,
            "kind": c.kind,
            "unit": c.unit,
            "cells": c.cells,
            "name": c.name,
            "charge_category": c.charge_category,
            "percent_base_codes": c.percent_base_codes,
            "multiplier_target_codes": c.multiplier_target_codes,
            "loss_sensitive": c.loss_sensitive,
            "source_page": c.source_page or "p.golden",
            "source_quote": quote,
        })
    return payload


def _score_live(plan: PlanInput, raw_plan: dict[str, Any], *, force: bool) -> PlanScore:
    doc = raw_plan.get("document_text")
    if not doc:
        # Build a minimal document from quotes so dry-live can still run.
        # Quote strings must match _dry_extract_fn exactly for G3.
        lines = [_dry_quote_and_line(c)[1] for c in plan.components]
        if not lines:
            return PlanScore(
                plan_key=plan.plan_key,
                utility_name=plan.utility_name,
                recipe_code=plan.recipe_code,
                mode="live",
                matched=False,
                has_all_in=True,
                error="skipped_no_document_text",
            )
        doc = "\n".join(lines)

    payload = _dry_extract_fn(plan)

    def extract_fn(_document: str, _model: str, _ctx: dict) -> list[dict]:
        return payload

    host = None
    url = raw_plan.get("source_url") or "https://golden.example/tariff.pdf"
    if "://" in url:
        from urllib.parse import urlparse
        host = urlparse(url).hostname

    result = dual_extract_components(
        doc,
        plan_meta={
            "plan_key": plan.plan_key,
            "name": plan.name,
            "recipe_code": plan.recipe_code,
            "code": plan.code,
            "rate_type": plan.rate_type,
            "utility_name": plan.utility_name,
            "source_url": url,
        },
        extract_fn=extract_fn,
        official_hosts=[host] if host else ["golden.example"],
        inventory=[],  # empty census = closed for plans with no inventory yet
        dispositions=[],
        force=force or component_extraction_enabled(),
    )
    if isinstance(result, ExtractionHold):
        if result.reason == "feature_flag_off":
            return PlanScore(
                plan_key=plan.plan_key,
                utility_name=plan.utility_name,
                recipe_code=plan.recipe_code,
                mode="live",
                matched=False,
                has_all_in=True,
                error="feature_flag_off",
                hold_reason=result.reason,
            )
        return PlanScore(
            plan_key=plan.plan_key,
            utility_name=plan.utility_name,
            recipe_code=plan.recipe_code,
            mode="live",
            matched=False,
            has_all_in=True,
            hold_reason=f"{result.reason}:{result.detail}",
        )

    assert isinstance(result, ExtractionAccept)
    compiled = result.preaccept.compiled
    assert compiled is not None
    if compiled.has_all_in:
        want = sorted(plan.official_cents)
    else:
        want = sorted(plan.official_delivery_cents)
    places = _places(want) if want else 4
    got = compiled.cents_sorted(places=places)
    return PlanScore(
        plan_key=plan.plan_key,
        utility_name=plan.utility_name,
        recipe_code=plan.recipe_code,
        mode="live",
        matched=got == want,
        has_all_in=compiled.has_all_in,
        official=[str(x) for x in want],
        compiled=[str(x) for x in got],
    )


def run_harness(
    *,
    mode: str = "offline",
    force_extract: bool = False,
    golden_path: Path | None = None,
) -> HarnessReport:
    from app.services.pricing.compiler import GOLDEN_DIR, plan_from_dict

    root = golden_path or GOLDEN_DIR
    raw = json.loads((root / "plans.json").read_text())
    raw_by_key = {p["plan_key"]: p for p in raw["plans"]}
    plans = [plan_from_dict(p) for p in raw["plans"]]

    scores: list[PlanScore] = []
    skipped = 0
    held = 0
    for plan in plans:
        if mode == "offline":
            scores.append(_score_offline(plan))
        else:
            s = _score_live(plan, raw_by_key[plan.plan_key], force=force_extract)
            if s.error == "skipped_no_document_text":
                skipped += 1
            elif s.hold_reason:
                held += 1
            scores.append(s)

    scored = [s for s in scores if s.error not in {"skipped_no_document_text", "feature_flag_off"}]
    # feature_flag_off counts as skipped for live accuracy denominator
    for s in scores:
        if s.error == "feature_flag_off":
            skipped += 1
    matched = sum(1 for s in scored if s.matched and not s.hold_reason)
    denom = len(scored)
    accuracy = (matched / denom) if denom else 0.0
    return HarnessReport(
        mode=mode,
        total=len(plans),
        scored=denom,
        matched=matched,
        held=held,
        skipped=skipped,
        accuracy=accuracy,
        plans=scores,
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--mode", choices=("offline", "live"), default="offline")
    p.add_argument(
        "--force-extract",
        action="store_true",
        help="Run live dual-extract path even when COMPONENT_EXTRACTION_ENABLED=0",
    )
    p.add_argument("--json", dest="json_out", default=None, help="Write report JSON")
    p.add_argument("--golden-dir", default=None, help="Override golden fixture dir")
    args = p.parse_args(argv)

    report = run_harness(
        mode=args.mode,
        force_extract=args.force_extract,
        golden_path=Path(args.golden_dir) if args.golden_dir else None,
    )
    print(
        f"pricing golden harness [{report.mode}]: "
        f"{report.matched}/{report.scored} exact "
        f"({report.accuracy:.1%}); held={report.held} skipped={report.skipped} "
        f"total={report.total}"
    )
    misses = [
        s for s in report.plans
        if not s.matched and s.error not in {"skipped_no_document_text", "feature_flag_off"}
    ]
    for s in misses[:20]:
        print(
            f"  MISS {s.plan_key}: official={s.official} compiled={s.compiled} "
            f"err={s.error} hold={s.hold_reason}"
        )
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(report.to_dict(), indent=2) + "\n")
        print(f"wrote {args.json_out}")

    if report.scored == 0:
        return 2
    return 0 if report.matched == report.scored and report.held == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
