"""Offline replay of live dual-extract outputs (no LLM).

Loads plans.jsonl + docs/url/*.json fixtures, re-runs schema → quote →
preaccept (G0–G6) → calculator on the saved Haiku/Sonnet component lists,
and scores accepted-correct / accepted-wrong / held against ``official``.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from functools import lru_cache
from typing import Any, Iterable

from app.services.pricing.extraction import (
    ExtractionAccept,
    ExtractionHold,
    dual_extract_components,
)
from app.services.pricing.inventory_from_docs import (
    build_inventory_from_document_set,
    comparable_oracles,
    merge_inventory,
    riders_from_text,
)
from app.services.pricing.official_sites import official_context
from app.services.pricing.rider_census import InventoryRider
from app.services.pricing.types import money

DEFAULT_FIXTURE_DIR = (
    Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "pricing_replay"
)

# Model ids used in R27 raw payloads (short keys) and live harness (full ids).
_HAIKU_KEYS = ("haiku", "claude-haiku-5-5")
_SONNET_KEYS = ("sonnet", "claude-sonnet-5-5")


@dataclass
class ReplayPlanResult:
    run: str
    set_name: str
    plan_key: str
    utility: str
    recipe_code: str
    outcome: str  # accepted_correct | accepted_wrong | held | skipped
    hold_reason: str | None = None
    official: list[str] = field(default_factory=list)
    compiled: list[str] = field(default_factory=list)
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "run": self.run,
            "set": self.set_name,
            "plan_key": self.plan_key,
            "utility": self.utility,
            "recipe_code": self.recipe_code,
            "outcome": self.outcome,
            "hold_reason": self.hold_reason,
            "official": list(self.official),
            "compiled": list(self.compiled),
            "detail": self.detail,
        }


@dataclass
class ReplayReport:
    results: list[ReplayPlanResult] = field(default_factory=list)

    @property
    def accepted_correct(self) -> int:
        return sum(1 for r in self.results if r.outcome == "accepted_correct")

    @property
    def accepted_wrong(self) -> int:
        return sum(1 for r in self.results if r.outcome == "accepted_wrong")

    @property
    def held(self) -> int:
        return sum(1 for r in self.results if r.outcome == "held")

    @property
    def skipped(self) -> int:
        return sum(1 for r in self.results if r.outcome == "skipped")

    @property
    def accepted_unscored(self) -> int:
        return sum(1 for r in self.results if r.outcome == "accepted_unscored")

    @property
    def scored(self) -> int:
        return self.accepted_correct + self.accepted_wrong + self.held

    @property
    def accept_correct_rate(self) -> float:
        # Rate over golden plans with recoverable raw (scored + skipped raw).
        n = self.scored
        return (self.accepted_correct / n) if n else 0.0

    def hold_reasons(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for r in self.results:
            if r.outcome != "held":
                continue
            key = (r.hold_reason or "held").split(":")[0]
            out[key] = out.get(key, 0) + 1
        return out

    def to_dict(self) -> dict[str, Any]:
        return {
            "accepted_correct": self.accepted_correct,
            "accepted_wrong": self.accepted_wrong,
            "held": self.held,
            "skipped": self.skipped,
            "accepted_unscored": self.accepted_unscored,
            "scored": self.scored,
            "accept_correct_rate": round(self.accept_correct_rate, 4),
            "hold_reasons": self.hold_reasons(),
            "plans": [r.to_dict() for r in self.results],
        }


def load_plans(
    fixture_dir: Path | None = None,
    *,
    run: str | None = "r27",
    set_name: str | None = "golden",
    recovered_only: bool = True,
) -> list[dict[str, Any]]:
    root = fixture_dir or DEFAULT_FIXTURE_DIR
    path = root / "plans.jsonl"
    rows: list[dict[str, Any]] = []
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if run and row.get("run") != run:
            continue
        if set_name and row.get("set") != set_name:
            continue
        raw = row.get("raw") or {}
        if recovered_only and not raw.get("recovered"):
            continue
        rows.append(row)
    return rows


def _url_hash(url: str) -> str:
    return hashlib.sha256((url or "").encode("utf-8")).hexdigest()[:16]


def _resolve_doc_json(root: Path, url: str, url_hash: str | None) -> Path | None:
    """Resolve the exact URL hash only — never a different edition."""
    candidates: list[Path] = []
    if url_hash:
        candidates.append(root / "docs" / "url" / f"{url_hash}.json")
    if url:
        candidates.append(root / "docs" / "url" / f"{_url_hash(url)}.json")
    for jp in candidates:
        if jp.exists():
            return jp
    return None


def load_document_text(row: dict[str, Any], fixture_dir: Path | None = None) -> str:
    """Concatenate per-URL re-fetched text for the plan's document set."""
    root = fixture_dir or DEFAULT_FIXTURE_DIR
    docs = row.get("documents") or {}
    # R30+: the exact <document> text the models saw.
    windows = [root / f for f in docs.get("window_files") or []]
    if windows and all(p.exists() for p in windows):
        return "\n".join(p.read_text(errors="replace") for p in windows)
    wsha = docs.get("window_sha")
    if wsha:
        wp = root / "docs" / f"window_{wsha}.txt"
        if wp.exists():
            return wp.read_text(errors="replace")
    hashes = list(docs.get("url_hashes") or [])
    urls = list(docs.get("urls") or [])
    # R27 rows often omit url_hashes — derive from URLs (sha256[:16]).
    if not hashes and urls:
        hashes = [_url_hash(u) for u in urls]
    while len(hashes) < len(urls):
        hashes.append(_url_hash(urls[len(hashes)]))
    parts: list[str] = []
    n = max(len(hashes), len(urls))
    for i in range(n):
        h = hashes[i] if i < len(hashes) else None
        u = urls[i] if i < len(urls) else ""
        jp = _resolve_doc_json(root, u, h)
        if jp is None:
            continue
        meta = json.loads(jp.read_text())
        text = meta.get("text") or ""
        url = meta.get("url") or u
        if text:
            parts.append(f"[DOC {i}] {url}\n{text}")
    return "\n".join(parts)


def _pick_raw_side(raw: dict[str, Any], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    for k in keys:
        v = raw.get(k)
        if isinstance(v, list) and v:
            return list(v)
    return []


def _official_cents(row: dict[str, Any]) -> list[Decimal]:
    out: list[Decimal] = []
    for x in row.get("official") or []:
        try:
            out.append(money(x))
        except Exception:
            continue
    return sorted(out)


def _compiled_cents(result: ExtractionAccept) -> list[Decimal]:
    compiled = result.preaccept.compiled
    if compiled is None:
        return []
    return list(compiled.cents_sorted(places=4))


def _cents_match(official: list[Decimal], compiled: list[Decimal]) -> bool:
    if not official or not compiled:
        return False
    # Quantize to 3 dp (official fixtures vary 3–4); compare as multisets.
    q = Decimal("0.001")
    a = sorted(v.quantize(q) for v in official)
    b = sorted(v.quantize(q) for v in compiled)
    return a == b


_GOLDEN_PLANS_PATH = (
    Path(__file__).resolve().parents[3]
    / "tests" / "fixtures" / "pricing_golden" / "plans.json"
)
_DOC_HEADER_RE = re.compile(r"^\[DOC \d+\]\s+(\S+)", re.M)


@lru_cache(maxsize=1)
def _golden_plans() -> dict[str, dict[str, Any]]:
    try:
        plans = json.loads(_GOLDEN_PLANS_PATH.read_text())["plans"]
    except (OSError, KeyError, ValueError):
        return {}
    return {p["plan_key"]: p for p in plans}


def _source_url(
    row: dict[str, Any], doc: str, golden: dict[str, Any] | None,
) -> str | None:
    urls = (row.get("documents") or {}).get("urls") or []
    if urls:
        return urls[0]
    if golden and golden.get("source_url"):
        return golden["source_url"]
    selected = (row.get("doc_set") or {}).get("selected") or []
    if selected:
        return selected[0]
    m = _DOC_HEADER_RE.search(doc)
    return m.group(1) if m else None


def _inventory_for_row(
    row: dict[str, Any],
    doc: str,
    source_url: str | None,
    golden: dict[str, Any] | None,
) -> tuple[list[InventoryRider], Any]:
    """Rebuild the live run's G5 inventory + G6 oracle from documents only."""
    members = ((golden or {}).get("document_set") or {}).get("documents") or []
    built = build_inventory_from_document_set(
        members=members, document_texts=[(source_url, doc)], plan_components=[],
    )
    recorded = [
        InventoryRider(code=str(c), name=str(c))
        for c in row.get("inventory") or [] if str(c).strip()
    ]
    inventory = merge_inventory(built.inventory, riders_from_text(doc), recorded)
    oracles = comparable_oracles(built.typical_bills)
    return inventory, (oracles[0].cents_per_kwh if oracles else None)


def replay_plan(
    row: dict[str, Any],
    *,
    fixture_dir: Path | None = None,
    force: bool = True,
) -> ReplayPlanResult:
    raw = row.get("raw") or {}
    base = ReplayPlanResult(
        run=str(row.get("run") or ""),
        set_name=str(row.get("set") or ""),
        plan_key=str(row.get("plan_key") or ""),
        utility=str(row.get("utility") or ""),
        recipe_code=str(row.get("recipe_code") or "bundled"),
        outcome="skipped",
        official=[str(x) for x in (row.get("official") or [])],
    )
    haiku = _pick_raw_side(raw, _HAIKU_KEYS)
    sonnet = _pick_raw_side(raw, _SONNET_KEYS)
    if not haiku or not sonnet:
        base.detail = "missing_raw_extract"
        return base

    doc = load_document_text(row, fixture_dir)
    if not doc.strip():
        base.detail = "missing_document_text"
        return base

    golden = _golden_plans().get(base.plan_key) if base.set_name == "golden" else None
    source_url = _source_url(row, doc, golden)
    source_ctx = official_context(base.utility)
    inventory, typical = _inventory_for_row(row, doc, source_url, golden)

    def extract_fn(_document: str, model: str, _ctx: dict) -> list[dict]:
        m = (model or "").lower()
        if "haiku" in m:
            return list(haiku)
        if "sonnet" in m:
            return list(sonnet)
        return list(haiku)

    result = dual_extract_components(
        doc,
        plan_meta={
            "plan_key": base.plan_key,
            "name": base.plan_key,
            "recipe_code": base.recipe_code,
            "utility_name": base.utility,
            "source_url": source_url,
        },
        extract_fn=extract_fn,
        source_ctx=source_ctx,
        source_url=source_url,
        inventory=inventory,
        dispositions=None,
        typical_bill_cents_per_kwh=typical,
        force=force,
    )
    if isinstance(result, ExtractionHold):
        base.outcome = "held"
        base.hold_reason = result.reason
        base.detail = (result.detail or "")[:400]
        return base

    assert isinstance(result, ExtractionAccept)
    compiled = _compiled_cents(result)
    base.compiled = [str(x) for x in compiled]
    official = _official_cents(row)
    if not official:
        # Non-golden sets have no oracle: accepted, but not scored.
        base.outcome = "accepted_unscored"
        base.detail = f"compiled={base.compiled}"
        return base
    if _cents_match(official, compiled):
        base.outcome = "accepted_correct"
    else:
        base.outcome = "accepted_wrong"
        base.detail = f"official={base.official} compiled={base.compiled}"
    return base


def run_replay(
    *,
    fixture_dir: Path | None = None,
    run: str | None = "r27",
    set_name: str | None = "golden",
    recovered_only: bool = True,
    plan_keys: Iterable[str] | None = None,
) -> ReplayReport:
    want = set(plan_keys) if plan_keys else None
    report = ReplayReport()
    for row in load_plans(
        fixture_dir, run=run, set_name=set_name, recovered_only=recovered_only,
    ):
        if want is not None and row.get("plan_key") not in want:
            continue
        report.results.append(replay_plan(row, fixture_dir=fixture_dir))
    return report


__all__ = [
    "DEFAULT_FIXTURE_DIR",
    "ReplayPlanResult",
    "ReplayReport",
    "load_document_text",
    "load_plans",
    "replay_plan",
    "run_replay",
]
