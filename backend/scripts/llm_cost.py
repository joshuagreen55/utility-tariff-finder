"""Per-phase, per-model LLM cost tracking for the tariff pipeline.

Goal: turn the opaque "LLM line item" on the cloud bill into an attributable
breakdown — how much each pipeline phase (Phase 3 extraction, Phase 5
navigation, Phase 6 Deep Research, Track B absorption) and each model
(Gemini Flash, Claude Haiku, Claude Opus) actually costs per utility and
per refresh run.

Design
------
* Token counts are ground truth, read straight from each provider's usage
  metadata. The dollar figure is `tokens x price`, where prices live in a
  table below and are overridable via the LLM_PRICING_JSON env var (model
  list prices change; never hardcode them as gospel).
* A process-local accumulator collects usage keyed by (phase, model). Celery
  prefork children process one pipeline at a time, so a thread-local store is
  safe; the current phase is tracked in a contextvar so nested calls restore
  cleanly.
* Capture is centralized by wrapping the Anthropic and Gemini client getters
  (see tariff_pipeline._get_*_client), so individual call sites need no
  changes. Phase 6 uses a separate Deep Research client and reports its own
  token counts, which we price via record_manual() — on completion and on
  every abort (timeout, token cap, poll error); aborts with no reported
  usage are counted as ``unpriced``.
* Scripts outside refresh runs (Track B, campaigns, auditor) record into the
  same accumulator and call append_ledger(); llm_cost_report includes them.
* Yield: ``tier_outcomes`` (did the tier return anything) is kept, but the
  honest metric is ``tier_acceptance`` — tariffs per tier that survive
  Phase 4 validation.

Prices are USD per 1,000,000 tokens. Verify against the current pricing pages
and override via LLM_PRICING_JSON if they drift, e.g.:
    LLM_PRICING_JSON='{"gemini": {"in": 0.30, "out": 2.50}}'
"""
from __future__ import annotations

import contextvars
import functools
import json
import logging
import os
import threading
from collections import defaultdict

log = logging.getLogger(__name__)

# USD per 1M tokens. cache_read / cache_write apply to Anthropic prompt
# caching (a cache hit bills ~10% of input; a cache write bills ~125%).
# Verify against current pricing pages; override via LLM_PRICING_JSON if they
# drift. Updated 2026-08-27: opus -> Opus 5 ($5/$25, was 4.7 @ $15/$75),
# gemini -> Gemini 3.8 Flash intro ($0.75/$3.75 through 2026-12-31, then
# $1.50/$7.50).
DEFAULT_PRICING: dict[str, dict[str, float]] = {
    "haiku":     {"in": 1.00,  "out": 5.00,  "cache_read": 0.10, "cache_write": 1.25},
    "opus":      {"in": 5.00,  "out": 25.00, "cache_read": 0.50, "cache_write": 6.25},
    "gemini":    {"in": 0.75,  "out": 3.75,  "cache_read": 0.075, "cache_write": 0.0},
    # Deep Research (Phase 6) — same token prices as Flash by default, but
    # broken out so its (typically large) spend is separately visible.
    "gemini_dr": {"in": 0.75,  "out": 3.75,  "cache_read": 0.0,  "cache_write": 0.0},
}


def _load_pricing() -> dict[str, dict[str, float]]:
    pricing = {k: dict(v) for k, v in DEFAULT_PRICING.items()}
    raw = os.environ.get("LLM_PRICING_JSON")
    if raw:
        try:
            override = json.loads(raw)
            for key, vals in override.items():
                pricing.setdefault(key, {}).update(vals)
        except Exception as e:  # noqa: BLE001 — never let pricing config crash a run
            log.warning("Ignoring invalid LLM_PRICING_JSON: %s", e)
    return pricing


PRICING = _load_pricing()

_phase: contextvars.ContextVar[str] = contextvars.ContextVar("llm_phase", default="other")
_local = threading.local()


def _new_acc() -> dict:
    return defaultdict(
        lambda: {"calls": 0, "in": 0, "out": 0, "cache_read": 0, "cache_write": 0, "cost": 0.0}
    )


def _new_outcomes() -> dict:
    # Per-tier extraction outcome counters. `hit` = the tier returned >=1
    # tariff; `miss` = it was invoked but returned nothing. Used to answer
    # "is the expensive Opus escalation actually earning its spend?".
    return defaultdict(lambda: {"hit": 0, "miss": 0})


def _new_acceptance() -> dict:
    # Per-tier tariff counts: `returned` into Phase 4, `accepted` after it.
    # A tier "hit" that Phase 4 rejects is not a yield.
    return defaultdict(lambda: {"returned": 0, "accepted": 0})


def _new_aborts() -> dict:
    # Calls that ended without a normal result (e.g. Phase 6 timeout /
    # token cap). `unpriced` counts aborts whose usage was not reported.
    return defaultdict(lambda: {"aborted": 0, "unpriced": 0})


def reset() -> None:
    """Start a fresh accumulation window (call at the top of run_pipeline)."""
    _local.acc = _new_acc()
    _local.outcomes = _new_outcomes()
    _local.acceptance = _new_acceptance()
    _local.aborts = _new_aborts()


def _acc() -> dict:
    acc = getattr(_local, "acc", None)
    if acc is None:
        reset()
        acc = _local.acc
    return acc


def _outcomes() -> dict:
    oc = getattr(_local, "outcomes", None)
    if oc is None:
        reset()
        oc = _local.outcomes
    return oc


def _store(name: str) -> dict:
    store = getattr(_local, name, None)
    if store is None:
        reset()
        store = getattr(_local, name)
    return store


def record_tier_acceptance(returned_tiers: list[str], accepted_tiers: list[str]) -> None:
    """Count tariffs per extraction tier going into and surviving Phase 4."""
    try:
        acc = _store("acceptance")
        for tier in returned_tiers:
            acc[tier or "unknown"]["returned"] += 1
        for tier in accepted_tiers:
            acc[tier or "unknown"]["accepted"] += 1
    except Exception as e:  # noqa: BLE001 — telemetry must never break a call
        log.debug("record_tier_acceptance failed: %s", e)


def record_abort(key: str, *, priced: bool) -> None:
    try:
        rec = _store("aborts")[key]
        rec["aborted"] += 1
        if not priced:
            rec["unpriced"] += 1
    except Exception as e:  # noqa: BLE001
        log.debug("record_abort failed: %s", e)


def record_extraction_outcome(model: str, produced: bool) -> None:
    """Record whether an extraction-tier call (gemini/haiku/opus) yielded
    any tariffs. Keyed by pricing model so we can compute per-tier hit rate
    and, crucially, the wasted-spend share of the Opus escalation."""
    try:
        rec = _outcomes()[model_key(model)]
        rec["hit" if produced else "miss"] += 1
    except Exception as e:  # noqa: BLE001 — telemetry must never break a call
        log.debug("record_extraction_outcome failed: %s", e)


def set_phase(name: str) -> None:
    _phase.set(name)


class phase:
    """Context manager that tags LLM usage with a phase name and restores
    the previous phase on exit (so nested phases attribute correctly)."""

    def __init__(self, name: str):
        self.name = name
        self._token = None

    def __enter__(self):
        self._token = _phase.set(self.name)
        return self

    def __exit__(self, *exc):
        if self._token is not None:
            _phase.reset(self._token)
        return False


def with_phase(name: str):
    """Decorator form of `phase` for tagging whole phase functions."""

    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            with phase(name):
                return fn(*args, **kwargs)

        return wrapper

    return deco


def model_key(model: str) -> str:
    """Map a concrete model id to a pricing/rollup key."""
    m = (model or "").lower()
    if "opus" in m:
        return "opus"
    if "haiku" in m:
        return "haiku"
    if "gemini" in m:
        return "gemini"
    return m or "other"


def _cost(key: str, tin: int, tout: int, cache_read: int = 0, cache_write: int = 0) -> float:
    p = PRICING.get(key) or {}
    return (
        tin * p.get("in", 0.0)
        + tout * p.get("out", 0.0)
        + cache_read * p.get("cache_read", 0.0)
        + cache_write * p.get("cache_write", 0.0)
    ) / 1_000_000.0


def _add(key: str, tin: int, tout: int, cache_read: int = 0, cache_write: int = 0) -> None:
    rec = _acc()[(_phase.get(), key)]
    rec["calls"] += 1
    rec["in"] += tin
    rec["out"] += tout
    rec["cache_read"] += cache_read
    rec["cache_write"] += cache_write
    rec["cost"] += _cost(key, tin, tout, cache_read, cache_write)


def record_anthropic(model: str, usage) -> None:
    """Record one Anthropic call from its `response.usage` object."""
    if usage is None:
        return
    try:
        _add(
            model_key(model),
            int(getattr(usage, "input_tokens", 0) or 0),
            int(getattr(usage, "output_tokens", 0) or 0),
            int(getattr(usage, "cache_read_input_tokens", 0) or 0),
            int(getattr(usage, "cache_creation_input_tokens", 0) or 0),
        )
    except Exception as e:  # noqa: BLE001 — cost tracking must never break a call
        log.debug("record_anthropic failed: %s", e)


def record_gemini(model: str, usage_metadata) -> None:
    """Record one Gemini call from its `response.usage_metadata` object."""
    if usage_metadata is None:
        return
    try:
        _add(
            model_key(model) if model else "gemini",
            int(getattr(usage_metadata, "prompt_token_count", 0) or 0),
            int(getattr(usage_metadata, "candidates_token_count", 0) or 0),
            int(getattr(usage_metadata, "cached_content_token_count", 0) or 0),
            0,
        )
    except Exception as e:  # noqa: BLE001
        log.debug("record_gemini failed: %s", e)


def record_manual(key: str, tin: int, tout: int) -> None:
    """Record usage when only raw token counts are available (e.g. Phase 6)."""
    try:
        _add(key, int(tin or 0), int(tout or 0), 0, 0)
    except Exception as e:  # noqa: BLE001
        log.debug("record_manual failed: %s", e)


def summary() -> dict:
    """Roll up the current accumulation window into a serializable dict."""
    acc = _acc()
    by_phase: dict[str, float] = defaultdict(float)
    by_model: dict[str, float] = defaultdict(float)
    detail: dict[str, dict] = {}
    total = 0.0
    for (ph, key), rec in acc.items():
        total += rec["cost"]
        by_phase[ph] += rec["cost"]
        by_model[key] += rec["cost"]
        detail.setdefault(ph, {})[key] = {**rec, "cost": round(rec["cost"], 6)}
    return {
        "total_usd": round(total, 6),
        "by_phase": {k: round(v, 6) for k, v in by_phase.items()},
        "by_model": {k: round(v, 6) for k, v in by_model.items()},
        "detail": detail,
        "tier_outcomes": {k: dict(v) for k, v in _outcomes().items()},
        "tier_acceptance": {k: dict(v) for k, v in _store("acceptance").items()},
        "aborts": {k: dict(v) for k, v in _store("aborts").items()},
    }


def merge_summaries(summaries: list[dict]) -> dict:
    """Combine many per-utility summaries (e.g. across a refresh run)."""
    by_phase: dict[str, float] = defaultdict(float)
    by_model: dict[str, float] = defaultdict(float)
    detail: dict[str, dict] = {}
    tier_outcomes: dict[str, dict] = defaultdict(lambda: {"hit": 0, "miss": 0})
    tier_acceptance: dict[str, dict] = defaultdict(lambda: {"returned": 0, "accepted": 0})
    aborts: dict[str, dict] = defaultdict(lambda: {"aborted": 0, "unpriced": 0})
    total = 0.0
    for s in summaries:
        if not s:
            continue
        total += s.get("total_usd", 0.0)
        for k, v in (s.get("by_phase") or {}).items():
            by_phase[k] += v
        for k, v in (s.get("by_model") or {}).items():
            by_model[k] += v
        for ph, models in (s.get("detail") or {}).items():
            for key, rec in models.items():
                agg = detail.setdefault(ph, {}).setdefault(
                    key,
                    {"calls": 0, "in": 0, "out": 0, "cache_read": 0, "cache_write": 0, "cost": 0.0},
                )
                for field in ("calls", "in", "out", "cache_read", "cache_write", "cost"):
                    agg[field] += rec.get(field, 0)
        for key, rec in (s.get("tier_outcomes") or {}).items():
            tier_outcomes[key]["hit"] += rec.get("hit", 0)
            tier_outcomes[key]["miss"] += rec.get("miss", 0)
        for key, rec in (s.get("tier_acceptance") or {}).items():
            tier_acceptance[key]["returned"] += rec.get("returned", 0)
            tier_acceptance[key]["accepted"] += rec.get("accepted", 0)
        for key, rec in (s.get("aborts") or {}).items():
            aborts[key]["aborted"] += rec.get("aborted", 0)
            aborts[key]["unpriced"] += rec.get("unpriced", 0)
    return {
        "total_usd": round(total, 6),
        "by_phase": {k: round(v, 6) for k, v in by_phase.items()},
        "by_model": {k: round(v, 6) for k, v in by_model.items()},
        "detail": detail,
        "tier_outcomes": {k: dict(v) for k, v in tier_outcomes.items()},
        "tier_acceptance": {k: dict(v) for k, v in tier_acceptance.items()},
        "aborts": {k: dict(v) for k, v in aborts.items()},
    }


# ---------------------------------------------------------------------------
# Script ledger: spend outside refresh runs (Track B, campaigns, auditor)
# ---------------------------------------------------------------------------

def ledger_path() -> str:
    return os.path.join(os.environ.get("APP_LOG_DIR", "/app/logs"), "llm_cost_ledger.jsonl")


def append_ledger(source: str, cost: dict | None = None) -> None:
    """Append one script run's cost summary so llm_cost_report sees it."""
    from datetime import datetime, timezone

    entry = {
        "at": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "cost": cost if cost is not None else summary(),
    }
    try:
        os.makedirs(os.path.dirname(ledger_path()), exist_ok=True)
        with open(ledger_path(), "a") as fh:
            fh.write(json.dumps(entry) + "\n")
    except OSError as e:
        log.warning("Could not append LLM cost ledger: %s", e)


def read_ledger(since_days: int | None = None) -> list[dict]:
    from datetime import datetime, timedelta, timezone

    try:
        with open(ledger_path()) as fh:
            rows = [json.loads(line) for line in fh if line.strip()]
    except (OSError, json.JSONDecodeError):
        return []
    if since_days is None:
        return rows
    cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
    return [r for r in rows if datetime.fromisoformat(r["at"]) >= cutoff]
