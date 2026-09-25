"""Soft-supersede write helpers and the tariff change-event log.

Every automated or manual write that changes which rates are live goes
through these helpers so that:

- a retired row keeps its rate_components (the audit trail) and gets
  ``superseded_by_tariff_id`` / ``supersede_reason`` / ``superseded_at``;
- an append-only ``TariffChangeEvent`` records who did it and why;
- curated rows (approved / repair / manual) are never overwritten or
  retired by a heuristic path — those paths log a ``hold`` event instead.

Pure predicates (``is_live``, ``is_protected``, ``component_signature``)
accept ORM rows, SimpleNamespaces or dicts so they are unit-testable
without a database.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, time, timezone
from typing import Any, Iterable, Mapping

log = logging.getLogger(__name__)

# confidence_factors.origin values for human- or agent-curated rows.
CURATED_ORIGINS = frozenset({"manual", "repair", "agent_verified"})
# confidence_factors keys written by repair scripts / manual corrections.
_CURATED_KEYS = ("repair", "manual")
# Heuristic keys recomputed by the pipeline on every extraction. Anything
# else in confidence_factors is provenance and must survive a re-verify.
HEURISTIC_FACTOR_KEYS = frozenset({
    "domain_match",
    "llm_confidence",
    "rates_normal",
    "component_richness",
    "has_energy",
    "name_match",
    "needs_review",
    "tou_seasonal_incomplete",
    "tou_seasonal_incomplete_reasons",
})


def _factors(t: Any) -> Mapping[str, Any]:
    cf = t.get("confidence_factors") if isinstance(t, Mapping) else getattr(t, "confidence_factors", None)
    return cf if isinstance(cf, Mapping) else {}


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(key, default)
    return getattr(obj, key, default)


def is_live(t: Any) -> bool:
    return _get(t, "superseded_by_tariff_id") is None and _get(t, "supersede_reason") is None


def is_curated(t: Any) -> bool:
    """Human/agent-curated row: repair keeper, manual correction, agent-verified."""
    cf = _factors(t)
    if any(k in cf for k in _CURATED_KEYS):
        return True
    return cf.get("origin") in CURATED_ORIGINS


def is_manual_or_pinned(t: Any) -> bool:
    """Rows that even a trusted regulator feed must not overwrite unverified."""
    cf = _factors(t)
    return "manual" in cf or cf.get("origin") in {"manual", "agent_verified"} or bool(cf.get("pin"))


def is_protected(t: Any) -> bool:
    """Protected from heuristic writes (LLM re-extraction, dup cleanup,
    reconciliation, vintage collapse onto a scraped sibling)."""
    return bool(_get(t, "approved", False)) or is_curated(t)


def merge_confidence_factors(old: Mapping[str, Any] | None, new: Mapping[str, Any]) -> dict:
    """Recompute heuristic factors but keep provenance keys from ``old``."""
    kept = {k: v for k, v in (old or {}).items() if k not in HEURISTIC_FACTOR_KEYS}
    return {**dict(new), **kept}


# ---------------------------------------------------------------------------
# Content signature
# ---------------------------------------------------------------------------

def _enum_value(v: Any) -> str:
    if v is None:
        return ""
    if hasattr(v, "value"):
        v = v.value
    return str(v).strip().lower()


def _norm_label(v: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(v or "").lower())


def _norm_time(v: Any) -> str:
    if v is None or v == "":
        return ""
    if isinstance(v, time):
        return v.strftime("%H:%M:%S")
    s = str(v).strip()
    if s in ("24:00", "24:00:00"):
        return "00:00:00"
    parts = s.split(":")
    try:
        return time(int(parts[0]), int(parts[1]) if len(parts) > 1 else 0).strftime("%H:%M:%S")
    except (ValueError, IndexError):
        return s


def _norm_num(v: Any, ndigits: int) -> float | None:
    if v is None or v == "":
        return None
    try:
        return round(float(v), ndigits)
    except (TypeError, ValueError):
        return None


def _norm_int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def component_key(comp: Any) -> tuple:
    """Comparable identity of one rate component's priced content.

    ``tier_label`` is excluded: it is decorative (e.g. "(all-in +riders)")
    and LLM wording drifts between runs, which would churn revisions.
    """
    return (
        _enum_value(_get(comp, "component_type")),
        str(_get(comp, "unit") or "").strip().lower().replace(" ", ""),
        _norm_num(_get(comp, "rate_value"), 6),
        _norm_num(_get(comp, "tier_min_kwh"), 3),
        _norm_num(_get(comp, "tier_max_kwh"), 3),
        _norm_label(_get(comp, "period_label")),
        _norm_label(_get(comp, "season")),
        _norm_time(_get(comp, "period_start_time")),
        _norm_time(_get(comp, "period_end_time")),
        str(_get(comp, "day_type") or "").strip().lower(),
        _norm_int(_get(comp, "season_start_month")),
        _norm_int(_get(comp, "season_start_day")),
        _norm_int(_get(comp, "season_end_month")),
        _norm_int(_get(comp, "season_end_day")),
        bool(_get(comp, "included_in_energy") or False),
    )


def component_signature(components: Iterable[Any], *, types: Iterable[str] | None = None) -> tuple:
    """Order-independent multiset signature of a component list."""
    wanted = {str(t).lower() for t in types} if types is not None else None
    keys = [
        component_key(c)
        for c in components or []
        if wanted is None or _enum_value(_get(c, "component_type")) in wanted
    ]
    return tuple(sorted(keys, key=repr))


def serialize_components(components: Iterable[Any]) -> list[dict]:
    """JSON-safe snapshot of components (for hold proposals / payloads)."""
    out = []
    for c in components or []:
        out.append({
            "component_type": _enum_value(_get(c, "component_type")),
            "unit": _get(c, "unit"),
            "rate_value": _norm_num(_get(c, "rate_value"), 6),
            "tier_min_kwh": _get(c, "tier_min_kwh"),
            "tier_max_kwh": _get(c, "tier_max_kwh"),
            "tier_label": _get(c, "tier_label"),
            "period_label": _get(c, "period_label"),
            "period_start_time": _norm_time(_get(c, "period_start_time")) or None,
            "period_end_time": _norm_time(_get(c, "period_end_time")) or None,
            "day_type": _get(c, "day_type"),
            "season": _get(c, "season"),
            "season_start_month": _get(c, "season_start_month"),
            "season_start_day": _get(c, "season_start_day"),
            "season_end_month": _get(c, "season_end_month"),
            "season_end_day": _get(c, "season_end_day"),
            "included_in_energy": bool(_get(c, "included_in_energy") or False),
        })
    return out


# ---------------------------------------------------------------------------
# Writers (sync Session; async callers use AsyncSession.run_sync)
# ---------------------------------------------------------------------------

def record_event(
    session,
    *,
    decision: str,
    actor_type: str,
    utility_id: int | None = None,
    before_tariff_id: int | None = None,
    after_tariff_id: int | None = None,
    reason: str | None = None,
    actor_id: str | None = None,
    ticket_id: str | None = None,
    source_url: str | None = None,
    source_document_hash: str | None = None,
    refresh_run_id: int | None = None,
    idempotency_key: str | None = None,
    payload: dict | None = None,
    notes: str | None = None,
):
    from app.models.tariff_change_event import TariffChangeEvent

    ev = TariffChangeEvent(
        decision=decision,
        actor_type=actor_type,
        utility_id=utility_id,
        before_tariff_id=before_tariff_id,
        after_tariff_id=after_tariff_id,
        reason=reason,
        actor_id=actor_id,
        ticket_id=ticket_id,
        source_url=source_url,
        source_document_hash=source_document_hash,
        refresh_run_id=refresh_run_id,
        idempotency_key=idempotency_key,
        payload=payload,
        notes=notes,
    )
    session.add(ev)
    return ev


def supersede_tariff(
    session,
    loser,
    *,
    reason: str,
    actor_type: str,
    successor=None,
    successor_id: int | None = None,
    **event_fields: Any,
):
    """Soft-retire a live tariff; its components stay on the row.

    With a successor the event decision is ``supersede``; without one it is
    ``retire``. Returns the event, or None when ``loser`` is not live.
    """
    if not is_live(loser):
        log.warning(
            f"supersede_tariff: tariff {getattr(loser, 'id', '?')} is already "
            f"superseded ({loser.supersede_reason}); leaving it unchanged"
        )
        return None
    if successor is not None:
        if successor.id is None:
            session.flush()
        successor_id = successor.id
    if successor_id is not None and successor_id == loser.id:
        raise ValueError(f"tariff {loser.id} cannot supersede itself")

    loser.superseded_by_tariff_id = successor_id
    loser.supersede_reason = reason
    loser.superseded_at = datetime.now(timezone.utc)
    return record_event(
        session,
        decision="supersede" if successor_id is not None else "retire",
        reason=reason,
        actor_type=actor_type,
        utility_id=loser.utility_id,
        before_tariff_id=loser.id,
        after_tariff_id=successor_id,
        **event_fields,
    )
