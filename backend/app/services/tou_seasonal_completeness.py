"""TOU / seasonal completeness rules using structured rate_component columns.

Product rules (Joshua-approved):
1. Every TOU rate must have clock time periods AND energy rates.
2. Every seasonal rate must have season calendar dates AND energy rates.
3. Seasonal+TOU needs both.

Completeness is judged from structured columns on ENERGY rows — not from
``period_label`` / ``season`` regex. Do not invent clock times or season
dates from labels.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import time
from typing import Any, Iterable, Mapping

# rate_type values (lowercase string or enum .value)
TOU_FAMILY = frozenset({"tou", "tou_tiered", "demand_tou", "seasonal_tou"})
SEASONAL_FAMILY = frozenset({"seasonal", "seasonal_tiered", "seasonal_tou"})
VALID_DAY_TYPES = frozenset({"weekday", "weekend", "holiday", "all"})


def _as_mapping(comp: Any) -> Mapping[str, Any]:
    if isinstance(comp, Mapping):
        return comp
    # ORM / SimpleNamespace
    return {
        "component_type": getattr(comp, "component_type", None),
        "rate_value": getattr(comp, "rate_value", None),
        "period_label": getattr(comp, "period_label", None),
        "period_start_time": getattr(comp, "period_start_time", None),
        "period_end_time": getattr(comp, "period_end_time", None),
        "day_type": getattr(comp, "day_type", None),
        "season": getattr(comp, "season", None),
        "season_start_month": getattr(comp, "season_start_month", None),
        "season_start_day": getattr(comp, "season_start_day", None),
        "season_end_month": getattr(comp, "season_end_month", None),
        "season_end_day": getattr(comp, "season_end_day", None),
    }


def _component_type_value(comp: Mapping[str, Any]) -> str:
    raw = comp.get("component_type")
    if raw is None:
        return ""
    if hasattr(raw, "value"):
        return str(raw.value).strip().lower()
    return str(raw).strip().lower()


def _rate_type_value(rate_type: Any) -> str:
    if rate_type is None:
        return ""
    if hasattr(rate_type, "value"):
        return str(rate_type.value).strip().lower()
    return str(rate_type).strip().lower()


def _coerce_time(value: Any) -> time | None:
    if value is None:
        return None
    if isinstance(value, time):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # Accept HH:MM, HH:MM:SS, and 24:00 → 00:00 (end-of-day convention)
        if s in ("24:00", "24:00:00"):
            return time(0, 0, 0)
        parts = s.replace(".", ":").split(":")
        try:
            h = int(parts[0])
            m = int(parts[1]) if len(parts) > 1 else 0
            sec = int(parts[2]) if len(parts) > 2 else 0
            if h == 24 and m == 0 and sec == 0:
                return time(0, 0, 0)
            return time(h, m, sec)
        except (ValueError, IndexError):
            return None
    return None


def _coerce_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def has_energy_rate(comp: Any) -> bool:
    """True when the component is ENERGY with a numeric rate_value."""
    m = _as_mapping(comp)
    if _component_type_value(m) != "energy":
        return False
    try:
        float(m.get("rate_value"))
        return True
    except (TypeError, ValueError):
        return False


def has_clock_window(comp: Any) -> bool:
    """True when both period_start_time and period_end_time are set."""
    m = _as_mapping(comp)
    start = _coerce_time(m.get("period_start_time"))
    end = _coerce_time(m.get("period_end_time"))
    return start is not None and end is not None


def has_season_calendar(comp: Any) -> bool:
    """True when inclusive season month/day fields are all present and in range."""
    m = _as_mapping(comp)
    sm = _coerce_int(m.get("season_start_month"))
    sd = _coerce_int(m.get("season_start_day"))
    em = _coerce_int(m.get("season_end_month"))
    ed = _coerce_int(m.get("season_end_day"))
    if None in (sm, sd, em, ed):
        return False
    assert sm is not None and sd is not None and em is not None and ed is not None
    if not (1 <= sm <= 12 and 1 <= em <= 12):
        return False
    if not (1 <= sd <= 31 and 1 <= ed <= 31):
        return False
    return True


def energy_components(components: Iterable[Any]) -> list[Mapping[str, Any]]:
    return [_as_mapping(c) for c in components if has_energy_rate(c)]


def tou_clock_ok(rate_type: Any, components: Iterable[Any]) -> bool:
    """TOU-family rule: ≥1 ENERGY with rates, and every ENERGY has a clock window."""
    rt = _rate_type_value(rate_type)
    if rt not in TOU_FAMILY:
        return True
    energy = energy_components(components)
    if not energy:
        return False
    return all(has_clock_window(c) for c in energy)


def seasonal_calendar_ok(rate_type: Any, components: Iterable[Any]) -> bool:
    """Seasonal-family rule: ≥1 ENERGY with rates, and every ENERGY has season dates."""
    rt = _rate_type_value(rate_type)
    if rt not in SEASONAL_FAMILY:
        return True
    energy = energy_components(components)
    if not energy:
        return False
    return all(has_season_calendar(c) for c in energy)


@dataclass(frozen=True)
class CompletenessResult:
    rate_type: str
    tou_ok: bool
    seasonal_ok: bool
    complete: bool
    energy_count: int
    energy_with_clock: int
    energy_with_season_dates: int
    reasons: tuple[str, ...]


def evaluate_tariff_completeness(
    rate_type: Any,
    components: Iterable[Any],
) -> CompletenessResult:
    """Judge whether a tariff/components set satisfies the product rules."""
    rt = _rate_type_value(rate_type)
    comps = list(components)
    energy = energy_components(comps)
    n_energy = len(energy)
    n_clock = sum(1 for c in energy if has_clock_window(c))
    n_season = sum(1 for c in energy if has_season_calendar(c))

    needs_tou = rt in TOU_FAMILY
    needs_seasonal = rt in SEASONAL_FAMILY

    tou_ok = True
    seasonal_ok = True
    reasons: list[str] = []

    if needs_tou:
        tou_ok = n_energy > 0 and n_clock == n_energy
        if n_energy == 0:
            reasons.append("tou_missing_energy_rates")
        elif n_clock < n_energy:
            reasons.append("tou_missing_clock_windows")

    if needs_seasonal:
        seasonal_ok = n_energy > 0 and n_season == n_energy
        if n_energy == 0:
            reasons.append("seasonal_missing_energy_rates")
        elif n_season < n_energy:
            reasons.append("seasonal_missing_calendar_dates")

    if not needs_tou and not needs_seasonal:
        complete = n_energy > 0
        if not complete:
            reasons.append("missing_energy_rates")
    else:
        complete = tou_ok and seasonal_ok

    return CompletenessResult(
        rate_type=rt,
        tou_ok=tou_ok,
        seasonal_ok=seasonal_ok,
        complete=complete,
        energy_count=n_energy,
        energy_with_clock=n_clock,
        energy_with_season_dates=n_season,
        reasons=tuple(dict.fromkeys(reasons)),  # stable unique
    )


def is_complete(rate_type: Any, components: Iterable[Any]) -> bool:
    return evaluate_tariff_completeness(rate_type, components).complete
