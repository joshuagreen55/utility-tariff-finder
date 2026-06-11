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
  token counts, which we price via record_manual().

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
DEFAULT_PRICING: dict[str, dict[str, float]] = {
    "haiku":     {"in": 1.00,  "out": 5.00,  "cache_read": 0.10, "cache_write": 1.25},
    "opus":      {"in": 15.00, "out": 75.00, "cache_read": 1.50, "cache_write": 18.75},
    "gemini":    {"in": 0.30,  "out": 2.50,  "cache_read": 0.075, "cache_write": 0.0},
    # Deep Research (Phase 6) — same token prices as Flash by default, but
    # broken out so its (typically large) spend is separately visible.
    "gemini_dr": {"in": 0.30,  "out": 2.50,  "cache_read": 0.0,  "cache_write": 0.0},
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


def reset() -> None:
    """Start a fresh accumulation window (call at the top of run_pipeline)."""
    _local.acc = _new_acc()


def _acc() -> dict:
    acc = getattr(_local, "acc", None)
    if acc is None:
        reset()
        acc = _local.acc
    return acc


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
    }


def merge_summaries(summaries: list[dict]) -> dict:
    """Combine many per-utility summaries (e.g. across a refresh run)."""
    by_phase: dict[str, float] = defaultdict(float)
    by_model: dict[str, float] = defaultdict(float)
    detail: dict[str, dict] = {}
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
    return {
        "total_usd": round(total, 6),
        "by_phase": {k: round(v, 6) for k, v in by_phase.items()},
        "by_model": {k: round(v, 6) for k, v in by_model.items()},
        "detail": detail,
    }
