#!/usr/bin/env python3
"""Offline / live harness for the component pricing path vs the golden set.

Offline (default)
    Compile each hand-encoded golden plan and score exact-match accuracy
    against cited official ¢/kWh (or delivery-only ¢ for texas_tdu). Also
    scores published TOU clocks / season calendars when present on the plan.

Live
    The real dual-extract path (folded from r27_live.py): document-set URLs,
    closed rider inventory, EXTRACTION_TOOL_SCHEMA, G0–G6, calculator, and
    clock scoring.

    By default live uses a *dry* extractor that returns the golden's own
    components (plus tou_schedule / season_calendar meta rows) as both blind
    reads — proves gate wiring without LLM spend (CI-safe).

    Pass ``--real-llm`` to call Haiku 5.5 + Sonnet 5.5 against fetched
    document-set text (requires ANTHROPIC_API_KEY; spends real $; not for CI).
    Real-LLM mode does **not** hand the model the golden component list
    (codes/names/units) or seed G5 dispositions from it — inventory comes
    from the utility's document set + document text, same as non-golden.

Exit codes
    0 — all scored plans exact-match (prices + clocks)
    1 — one or more mismatches or compile failures
    2 — no plans scored

Usage
    cd backend && python -m scripts.pricing_golden_harness
    python -m scripts.pricing_golden_harness --mode live --force-extract
    python -m scripts.pricing_golden_harness --mode live --real-llm --force-extract
    python -m scripts.pricing_golden_harness --json /tmp/harness.json
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

# Allow `python -m scripts.pricing_golden_harness` from backend/
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.pricing.clocks import (  # noqa: E402
    clocks_match,
    extract_clocks_from_components,
    meta_components_from_plan_schedule,
    seasons_match,
)
from app.services.pricing.compiler import compile_plan, load_golden_plans  # noqa: E402
from app.services.pricing.extract_schema import EXTRACTION_TOOL_SCHEMA  # noqa: E402
from app.services.pricing.extraction import (  # noqa: E402
    ExtractionAccept,
    ExtractionHold,
    component_extraction_enabled,
    dual_extract_components,
)
from app.services.pricing.inventory_from_docs import (  # noqa: E402
    build_inventory_from_document_set,
    dispositions_from_plan_components,
    riders_from_text,
)
from app.services.pricing.rider_census import (  # noqa: E402
    DispositionInput,
    InventoryRider,
)
from app.services.pricing.types import PlanInput  # noqa: E402

ExtractFn = Callable[[str, str, dict[str, Any]], list[dict[str, Any]]]

# Default model ids — env-overridable; never invent new ids.
HAIKU_MODEL = os.environ.get("HAIKU_MODEL", "claude-haiku-5-5")
SONNET_MODEL = os.environ.get("SONNET_MODEL", "claude-sonnet-5-5")
_PRICE = {
    "claude-haiku-5-5": (0.10, 0.50),
    "claude-sonnet-5-5": (2.0, 10.0),
}


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
    clocks_matched: bool | None = None
    seasons_matched: bool | None = None
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
    clocks_scored: int = 0
    clocks_matched: int = 0
    plans: list[PlanScore] = field(default_factory=list)
    llm_spend_usd: float | None = None

    def to_dict(self) -> dict[str, Any]:
        d = {
            "mode": self.mode,
            "total": self.total,
            "scored": self.scored,
            "matched": self.matched,
            "held": self.held,
            "skipped": self.skipped,
            "accuracy": self.accuracy,
            "clocks_scored": self.clocks_scored,
            "clocks_matched": self.clocks_matched,
            "plans": [asdict(p) for p in self.plans],
        }
        if self.llm_spend_usd is not None:
            d["llm_spend_usd"] = self.llm_spend_usd
        return d


def _score_schedule(
    plan: PlanInput,
    got_clocks: list[dict[str, Any]] | None = None,
    got_seasons: list[dict[str, Any]] | None = None,
) -> tuple[bool | None, bool | None]:
    """Compare clocks/seasons. None = nothing to score on the golden."""
    c_ok: bool | None = None
    s_ok: bool | None = None
    if plan.clocks:
        # Offline / dry: golden vs itself when got_* omitted.
        c_ok = clocks_match(plan.clocks, got_clocks if got_clocks is not None else plan.clocks)
    if plan.seasons:
        s_ok = seasons_match(
            plan.seasons, got_seasons if got_seasons is not None else plan.seasons
        )
    return c_ok, s_ok


def _prices_and_schedule_match(
    price_ok: bool,
    clocks_ok: bool | None,
    seasons_ok: bool | None,
) -> bool:
    if not price_ok:
        return False
    if clocks_ok is False:
        return False
    if seasons_ok is False:
        return False
    return True


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
    price_ok = got == want
    c_ok, s_ok = _score_schedule(plan)
    return PlanScore(
        plan_key=plan.plan_key,
        utility_name=plan.utility_name,
        recipe_code=plan.recipe_code,
        mode="offline",
        matched=_prices_and_schedule_match(price_ok, c_ok, s_ok),
        has_all_in=compiled.has_all_in,
        official=[str(x) for x in want],
        compiled=[str(x) for x in got],
        clocks_matched=c_ok,
        seasons_matched=s_ok,
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
    """Return (source_quote, document_line) that agree for G3 grounding."""
    anchor = _unit_anchor(c.unit)
    cell = (c.cells or [{}])[0] if c.cells else {}
    amount = str(cell.get("amount") or "")
    labels = _cell_label_prefix(cell)
    name = (c.name or c.code or "").strip()

    if c.source_quote:
        q = str(c.source_quote)
        has_digits = bool(re.search(r"\d", q))
        if amount and amount in q:
            line = f"{labels} {q} ({anchor})".strip()
            return q, line
        if not has_digits:
            line = f"{labels} {q} {amount} ({anchor})".strip()
            return q, line

    bits = [x for x in (labels, name, amount, anchor) if x]
    quote = " ".join(bits) if bits else f"{c.code} {anchor}"
    return quote, quote


def _dry_extract_payload(plan: PlanInput, raw_plan: dict[str, Any]) -> list[dict]:
    """Golden components + schedule meta as both 'blind' reads (no LLM spend)."""
    payload = []
    for c in plan.components:
        quote, _line = _dry_quote_and_line(c)
        payload.append({
            "code": c.code,
            "kind": c.kind,
            "unit": c.unit,
            "cells": c.cells,
            "name": c.name or c.code,
            "disposition": "applies",
            "charge_category": c.charge_category,
            "percent_base_codes": c.percent_base_codes,
            "multiplier_target_codes": c.multiplier_target_codes,
            "loss_sensitive": c.loss_sensitive,
            "source_page": c.source_page or "p.golden",
            "source_quote": quote,
        })
    # Preserve non-applies dispositions from the raw golden for G5 census.
    for raw in raw_plan.get("components") or []:
        disp = str(raw.get("disposition") or "applies")
        if disp == "applies":
            continue
        payload.append({
            "code": raw["code"],
            "kind": raw["kind"],
            "unit": raw.get("unit") or "¢/kWh",
            "cells": raw.get("cells") or [{"amount": "0"}],
            "name": raw.get("name") or raw["code"],
            "disposition": disp,
            "source_page": raw.get("source_page") or "p.golden",
            "source_quote": raw.get("source_quote") or raw.get("name") or raw["code"],
        })
    payload.extend(
        meta_components_from_plan_schedule(
            raw_plan.get("clocks") or plan.clocks,
            raw_plan.get("seasons") or plan.seasons,
        )
    )
    return payload


def _inventory_for_plan(
    raw_plan: dict[str, Any],
    *,
    from_document_set: bool,
    document_text: str | None = None,
    seed_from_golden: bool = True,
) -> tuple[list[InventoryRider], list[DispositionInput], Any]:
    """Build G5 inventory + dispositions + optional G6 typical-bill.

    Dry path (``seed_from_golden=True``) seeds inventory + dispositions from
    the golden's own rider components so the census closes without network.

    Real-LLM path (``seed_from_golden=False``) builds inventory only from
    document-set members + fetched document text — never from the golden
    component list. Dispositions come from the extract itself (caller
    passes ``dispositions=None`` into ``dual_extract_components``).
    """
    members = (raw_plan.get("document_set") or {}).get("documents") or []
    plan_components = (raw_plan.get("components") or []) if seed_from_golden else []
    doc_texts: list[tuple[str | None, str]] = []
    if document_text and from_document_set and not seed_from_golden:
        urls = _document_urls(raw_plan)
        doc_texts.append((urls[0] if urls else None, document_text))
    built = build_inventory_from_document_set(
        members=members if from_document_set else [],
        document_texts=doc_texts or None,
        plan_components=plan_components,
    )
    inventory = list(built.inventory)
    if document_text and from_document_set and not seed_from_golden:
        # Fold any extra "subject to Rider X" hits (merge is idempotent).
        from app.services.pricing.inventory_from_docs import merge_inventory
        inventory = merge_inventory(inventory, riders_from_text(document_text))

    dispositions: list[DispositionInput] = []
    if seed_from_golden:
        disp_dicts = dispositions_from_plan_components(
            raw_plan.get("components") or []
        )
        dispositions = [
            DispositionInput(
                rider_code=d["rider_code"],
                disposition=d["disposition"],
                disposition_page=d.get("disposition_page"),
                disposition_quote=d.get("disposition_quote"),
            )
            for d in disp_dicts
        ]
    typical = None
    if built.typical_bills:
        typical = built.typical_bills[0].cents_per_kwh
    return inventory, dispositions, typical


def build_real_llm_extract_prompt(
    *,
    plan_raw: dict[str, Any],
    document: str,
    inventory: list[InventoryRider] | None = None,
) -> str:
    """Prompt for live real-LLM extract — no golden component answer key.

    The model must discover priced components from the documents. Optional
    ``inventory`` is document-derived rider codes only (G5 closed world),
    never golden plan components.
    """
    inv_lines = ""
    if inventory:
        inv_lines = (
            "Closed rider inventory from the utility's own documents "
            "(codes only — find amounts in the text; do not invent riders):\n"
            + "\n".join(
                f"- {r.code}" + (f" ({r.name})" if r.name else "")
                for r in inventory
            )
            + "\n\n"
        )
    return (
        f'Today is 2026-10-09. You are copying numbers from official tariff '
        f'documents for {plan_raw.get("utility_name")}, residential plan '
        f'"{plan_raw.get("name")}" (code {plan_raw.get("code")}, rate type '
        f'{plan_raw.get("rate_type")}, pricing recipe {plan_raw.get("recipe_code")}).\n'
        f"Do NOT compute any totals. Discover every priced charge that a "
        f"residential customer on this plan pays (base energy, delivery, "
        f"supply/commodity, fuel and other per-kWh riders, and fixed monthly "
        f"customer charges when printed). For each, copy the CURRENT (in "
        f"effect today) value exactly as printed. Amounts must be decimal "
        f"strings. Codes and names must be unique. "
        f"Every component needs an explicit disposition "
        f"(applies / not_applicable / not_found / optional / "
        f"location_fee_or_tax / event_day). "
        f"If a charge is mentioned but no amount appears, disposition "
        f"not_found with empty cells — do not invent values. "
        f"Return one cell per distinct season/period/day_type/tier using "
        f"labels as printed in the document (\"all\" when not differentiated).\n"
        f"Also emit a tou_schedule component (kind tou_schedule) with cells "
        f"carrying period/season/day_type/start/end when the document states "
        f"clock windows, and a season_calendar component when inclusive season "
        f"dates are stated. Never invent clocks or season dates.\n"
        f"source_quote must be a short VERBATIM span containing the number. "
        f"source_page: the [DOC n] tag.\n"
        f"{inv_lines}"
        f"<document>\n{document}\n</document>"
    )


def _document_urls(raw_plan: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    for u in raw_plan.get("source_urls") or []:
        if u and u not in urls:
            urls.append(u)
    for d in (raw_plan.get("document_set") or {}).get("documents") or []:
        u = d.get("url") if isinstance(d, dict) else None
        if u and u not in urls:
            urls.append(u)
    if raw_plan.get("source_url") and raw_plan["source_url"] not in urls:
        urls.insert(0, raw_plan["source_url"])
    return urls


def _official_hosts(urls: list[str], raw_plan: dict[str, Any]) -> list[str]:
    hosts: list[str] = []
    for u in urls:
        h = urlparse(u).hostname
        if h:
            hosts.append(h.removeprefix("www."))
    for d in (raw_plan.get("document_set") or {}).get("documents") or []:
        ph = (d.get("publisher_host") or "").strip()
        if ph and ph not in hosts:
            hosts.append(ph)
    return hosts or ["golden.example"]


def _build_dry_document(plan: PlanInput, payload: list[dict]) -> str:
    lines = []
    for c in plan.components:
        lines.append(_dry_quote_and_line(c)[1])
    # Include schedule quotes so meta components ground if ever checked.
    for raw in payload:
        if raw.get("kind") in {"tou_schedule", "season_calendar"}:
            lines.append(str(raw.get("source_quote") or ""))
    return "\n".join(lines)


def _fetch_document_set_text(urls: list[str], *, max_chars: int = 120_000) -> tuple[str, list[tuple]]:
    """Fetch official document-set URLs (real-llm path). Returns (text, fetch_log)."""
    from scripts import tariff_pipeline as tp

    texts: list[tuple[int, str, str]] = []
    log: list[tuple] = []
    for i, u in enumerate(urls[:8]):
        try:
            if ".pdf" in u.lower():
                t = tp.fetch_pdf_text(u) or ""
            else:
                rp = tp._fetch_and_parse(u)
                t = (rp.content if rp else "") or ""
            log.append((u, len(t)))
            if t:
                texts.append((i, u, t))
        except Exception as e:
            log.append((u, f"ERR {e}"[:120]))
    if not texts:
        return "", log
    # Relevance window (same idea as r27_live.window)
    chunks = []
    for i, u, t in texts:
        for k in range(0, len(t), 3000):
            s = t[k : k + 3000]
            sc = 5 if re.search(r"\d+\.\d+\s*(¢|cents|\$)", s) else 0
            sc += s.lower().count("residential")
            chunks.append((sc, i, k, u, s))
    total = sum(len(c[4]) for c in chunks)
    keep = chunks if total <= max_chars else sorted(chunks, key=lambda c: -c[0])[: max_chars // 3000]
    keep.sort(key=lambda c: (c[1], c[2]))
    doc = "\n".join(f"[DOC {c[1]}] {c[3]}\n{c[4]}" for c in keep)
    return doc, log


class _LlmSpendMeter:
    def __init__(self, limit_usd: float) -> None:
        self.total = 0.0
        self.limit = limit_usd

    def charge(self, model: str, usage: dict) -> float:
        pi, po = _PRICE.get(model, (2.0, 10.0))
        c = (usage.get("input_tokens", 0) * pi + usage.get("output_tokens", 0) * po) / 1e6
        self.total += c
        return c


def _make_real_llm_extract_fn(
    *,
    plan_raw: dict[str, Any],
    document: str,
    meter: _LlmSpendMeter,
    inventory: list[InventoryRider] | None = None,
) -> ExtractFn:
    """Build an extract_fn that calls Anthropic with EXTRACTION_TOOL_SCHEMA.

    Does not embed the golden component list — only document-derived rider
    codes (optional) plus the plan's name/code/recipe.
    """
    import httpx
    from app.services import anthropic_compat

    prompt = build_real_llm_extract_prompt(
        plan_raw=plan_raw,
        document=document,
        inventory=inventory,
    )

    def extract_fn(_document: str, model: str, _ctx: dict) -> list[dict]:
        if meter.total >= meter.limit:
            raise RuntimeError("HARD_LIMIT")
        r = anthropic_compat.post(
            httpx.post,
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": os.environ["ANTHROPIC_API_KEY"],
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": model,
                "max_tokens": 4000,
                "tools": [EXTRACTION_TOOL_SCHEMA],
                "tool_choice": {
                    "type": "tool",
                    "name": EXTRACTION_TOOL_SCHEMA["name"],
                },
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=300,
        )
        d = r.json()
        meter.charge(model, d.get("usage") or {})
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code} {str(d)[:200]}")
        for b in d.get("content") or []:
            if b.get("type") == "tool_use":
                return list((b.get("input") or {}).get("components") or [])
        return []

    return extract_fn


def _score_live(
    plan: PlanInput,
    raw_plan: dict[str, Any],
    *,
    force: bool,
    real_llm: bool,
    meter: _LlmSpendMeter | None,
) -> PlanScore:
    urls = _document_urls(raw_plan)
    hosts = _official_hosts(urls, raw_plan)
    source_url = urls[0] if urls else (raw_plan.get("source_url") or "https://golden.example/tariff.pdf")

    if real_llm:
        if not os.environ.get("ANTHROPIC_API_KEY"):
            return PlanScore(
                plan_key=plan.plan_key,
                utility_name=plan.utility_name,
                recipe_code=plan.recipe_code,
                mode="live",
                matched=False,
                has_all_in=True,
                error="missing_anthropic_api_key",
            )
        assert meter is not None
        doc, fetch_log = _fetch_document_set_text(urls)
        if not doc:
            return PlanScore(
                plan_key=plan.plan_key,
                utility_name=plan.utility_name,
                recipe_code=plan.recipe_code,
                mode="live",
                matched=False,
                has_all_in=True,
                error=f"no_document_fetched:{fetch_log}",
            )
        # Inventory from docs only — never golden component list / dispositions.
        inventory, _disp_unused, typical = _inventory_for_plan(
            raw_plan,
            from_document_set=True,
            document_text=doc,
            seed_from_golden=False,
        )
        dispositions = None  # derive from extract (PR R28-5)
        extract_fn = _make_real_llm_extract_fn(
            plan_raw=raw_plan,
            document=doc,
            meter=meter,
            inventory=inventory,
        )
    else:
        inventory, dispositions, typical = _inventory_for_plan(
            raw_plan, from_document_set=False, seed_from_golden=True,
        )
        payload = _dry_extract_payload(plan, raw_plan)
        doc = raw_plan.get("document_text") or _build_dry_document(plan, payload)
        if not doc.strip():
            return PlanScore(
                plan_key=plan.plan_key,
                utility_name=plan.utility_name,
                recipe_code=plan.recipe_code,
                mode="live",
                matched=False,
                has_all_in=True,
                error="skipped_no_document_text",
            )

        def extract_fn(_document: str, _model: str, _ctx: dict) -> list[dict]:
            return payload

    result = dual_extract_components(
        doc,
        plan_meta={
            "plan_key": plan.plan_key,
            "name": plan.name,
            "recipe_code": plan.recipe_code,
            "code": plan.code,
            "rate_type": plan.rate_type,
            "utility_name": plan.utility_name,
            "source_url": source_url,
        },
        extract_fn=extract_fn,
        official_hosts=hosts,
        inventory=inventory,
        dispositions=dispositions,
        typical_bill_cents_per_kwh=typical,
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
    price_ok = got == want

    # Clock scoring from the accepted extract (incl. tou_schedule meta).
    got_clocks, got_seasons = extract_clocks_from_components([
        {"code": c.code, "kind": c.kind, "cells": c.cells}
        for c in (result.extract_a or [])
    ])

    c_ok, s_ok = _score_schedule(plan, got_clocks, got_seasons)
    return PlanScore(
        plan_key=plan.plan_key,
        utility_name=plan.utility_name,
        recipe_code=plan.recipe_code,
        mode="live",
        matched=_prices_and_schedule_match(price_ok, c_ok, s_ok),
        has_all_in=compiled.has_all_in,
        official=[str(x) for x in want],
        compiled=[str(x) for x in got],
        clocks_matched=c_ok,
        seasons_matched=s_ok,
    )


def run_harness(
    *,
    mode: str = "offline",
    force_extract: bool = False,
    real_llm: bool = False,
    llm_limit_usd: float = 22.0,
    golden_path: Path | None = None,
    only_keys: set[str] | None = None,
) -> HarnessReport:
    from app.services.pricing.compiler import GOLDEN_DIR, plan_from_dict

    root = golden_path or GOLDEN_DIR
    raw = json.loads((root / "plans.json").read_text())
    raw_by_key = {p["plan_key"]: p for p in raw["plans"]}
    plans = [plan_from_dict(p) for p in raw["plans"]]
    if only_keys:
        plans = [p for p in plans if p.plan_key in only_keys]

    meter = _LlmSpendMeter(llm_limit_usd) if real_llm else None
    scores: list[PlanScore] = []
    skipped = 0
    held = 0
    for plan in plans:
        if mode == "offline":
            scores.append(_score_offline(plan))
        else:
            s = _score_live(
                plan,
                raw_by_key[plan.plan_key],
                force=force_extract,
                real_llm=real_llm,
                meter=meter,
            )
            if s.error in {"skipped_no_document_text", "missing_anthropic_api_key"} or (
                s.error and s.error.startswith("no_document_fetched")
            ):
                skipped += 1
            elif s.hold_reason:
                held += 1
            scores.append(s)
            if meter and meter.total >= meter.limit:
                break

    scored = [
        s for s in scores
        if s.error not in {"skipped_no_document_text", "feature_flag_off", "missing_anthropic_api_key"}
        and not (s.error or "").startswith("no_document_fetched")
    ]
    for s in scores:
        if s.error == "feature_flag_off":
            skipped += 1
    matched = sum(1 for s in scored if s.matched and not s.hold_reason)
    denom = len(scored)
    accuracy = (matched / denom) if denom else 0.0
    clocks_scored = sum(1 for s in scored if s.clocks_matched is not None)
    clocks_matched = sum(1 for s in scored if s.clocks_matched is True)
    return HarnessReport(
        mode=mode if not real_llm else f"{mode}+real_llm",
        total=len(plans),
        scored=denom,
        matched=matched,
        held=held,
        skipped=skipped,
        accuracy=accuracy,
        clocks_scored=clocks_scored,
        clocks_matched=clocks_matched,
        plans=scores,
        llm_spend_usd=round(meter.total, 4) if meter else None,
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--mode", choices=("offline", "live"), default="offline")
    p.add_argument(
        "--force-extract",
        action="store_true",
        help="Run live dual-extract path even when COMPONENT_EXTRACTION_ENABLED=0",
    )
    p.add_argument(
        "--real-llm",
        action="store_true",
        help="Fetch document-set URLs and call Haiku+Sonnet (spends $; not for CI)",
    )
    p.add_argument(
        "--llm-limit",
        type=float,
        default=22.0,
        help="Hard USD cap for --real-llm (default 22)",
    )
    p.add_argument("--only", default=None, help="Comma-separated plan_key filter")
    p.add_argument("--json", dest="json_out", default=None, help="Write report JSON")
    p.add_argument("--golden-dir", default=None, help="Override golden fixture dir")
    args = p.parse_args(argv)

    only = set(args.only.split(",")) if args.only else None
    report = run_harness(
        mode=args.mode,
        force_extract=args.force_extract,
        real_llm=args.real_llm,
        llm_limit_usd=args.llm_limit,
        golden_path=Path(args.golden_dir) if args.golden_dir else None,
        only_keys=only,
    )
    spend = (
        f" spend=${report.llm_spend_usd:.4f}"
        if report.llm_spend_usd is not None
        else ""
    )
    print(
        f"pricing golden harness [{report.mode}]: "
        f"{report.matched}/{report.scored} exact "
        f"({report.accuracy:.1%}); held={report.held} skipped={report.skipped} "
        f"total={report.total}; clocks={report.clocks_matched}/{report.clocks_scored}"
        f"{spend}"
    )
    misses = [
        s for s in report.plans
        if not s.matched
        and s.error not in {"skipped_no_document_text", "feature_flag_off", "missing_anthropic_api_key"}
        and not (s.error or "").startswith("no_document_fetched")
    ]
    for s in misses[:20]:
        print(
            f"  MISS {s.plan_key}: official={s.official} compiled={s.compiled} "
            f"clocks={s.clocks_matched} seasons={s.seasons_matched} "
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
