"""Computable-tariff contract (completeness v2).

``evaluate_computable`` decides whether a tariff's structured rows are
enough for a machine to price every interval of the year without a human
reading the source document — the bar for Mysa's 30-second cost rollups
and TOU device scheduling. It never guesses: anything ambiguous is
``computable=False`` with machine-readable reasons.

Reasons (blocking) are ``code`` or ``code:detail``; consumers should match
on the part before ``:``. Warnings are non-blocking assumptions the
consumer must honour (see docs/MYSA_CONSUMER_CONTRACT.md).

Rules on ENERGY rows (structured columns only, never labels):

- TOU mode (TOU-family rate_type, or any ENERGY row with a clock window):
  every row has a clock window and a ``day_type``; per season × day type
  (weekday, weekend, and holiday when holiday rows exist) the windows
  partition 24 h exactly once.
- Seasons (seasonal-family rate_type, or any row with season dates): every
  row has valid inclusive month/day dates and the distinct windows cover
  the year exactly once.
- Tiers: within a season (and TOU period) tiers start at 0, are contiguous
  (a 1 kWh inclusive-bound step is allowed) and end with an open top tier.
- Otherwise one ENERGY price per season.

Shapes shipped as non-computable rather than guessed: demand charges,
TOU+tiered, complex, critical-peak / event / dynamic pricing.
"""
from __future__ import annotations

import calendar
import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from app.services.tou_seasonal_completeness import (
    SEASONAL_FAMILY,
    TOU_FAMILY,
    VALID_DAY_TYPES,
    _coerce_int,
    _coerce_time,
)

KNOWN_RATE_TYPES = frozenset({
    "flat", "tou", "tiered", "demand", "seasonal", "tou_tiered",
    "seasonal_tou", "seasonal_tiered", "demand_tou", "complex",
})

# Periodic charge units → billing basis. Keys are lower-case, no spaces.
PERIODIC_UNITS = {
    "$/month": "month",
    "$/mo": "month",
    "$/customer/month": "month",
    "$/meter/month": "month",
    "$/day": "day",
    "$/customer/day": "day",
    "$/bill": "bill",
    "$/billingperiod": "bill",
    "$/year": "year",
}
PER_KWH_UNITS = frozenset({"$/kwh"})

_EVENT_RE = re.compile(
    r"critical[\s-]*peak|\bcpp\b|peak[\s-]*time[\s-]*rebate|\bptr\b|"
    r"\bevent\b|peak[\s-]*day[\s-]*pricing|\bvpp\b",
    re.IGNORECASE,
)
_DYNAMIC_RE = re.compile(
    r"real[\s-]*time[\s-]*pric|hourly[\s-]*pric|dynamic[\s-]*pric|day[\s-]*ahead",
    re.IGNORECASE,
)
_ALL_IN_RE = re.compile(r"all[\s-]*in", re.IGNORECASE)
_OPEN_TOP_KWH = 99_999.0
_REF_LEAP_YEAR = 2024
_FEB29_DOY = 60


@dataclass(frozen=True)
class ComputableResult:
    computable: bool
    reasons: tuple[str, ...]
    warnings: tuple[str, ...]


def _get(comp: Any, key: str, default: Any = None) -> Any:
    if isinstance(comp, Mapping):
        return comp.get(key, default)
    return getattr(comp, key, default)


def _val(v: Any) -> str:
    if v is None:
        return ""
    if hasattr(v, "value"):
        v = v.value
    return str(v).strip().lower()


def normalize_unit(unit: Any) -> str:
    return str(unit or "").strip().lower().replace(" ", "")


def periodic_unit_basis(unit: Any) -> str | None:
    """'month' | 'day' | 'bill' | 'year' for a periodic charge unit, else None."""
    return PERIODIC_UNITS.get(normalize_unit(unit))


def _num(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _season_window(comp: Any) -> tuple[int, int, int, int] | None:
    vals = tuple(_coerce_int(_get(comp, k)) for k in (
        "season_start_month", "season_start_day", "season_end_month", "season_end_day",
    ))
    if any(v is None for v in vals):
        return None
    return vals  # type: ignore[return-value]


def _valid_month_day(m: int, d: int) -> bool:
    return 1 <= m <= 12 and 1 <= d <= calendar.monthrange(_REF_LEAP_YEAR, m)[1]


def _doy(m: int, d: int) -> int:
    from datetime import date

    return date(_REF_LEAP_YEAR, m, d).timetuple().tm_yday


def _fmt_window(w: tuple[int, int, int, int] | None) -> str:
    if w is None:
        return "year"
    return f"{w[0]:02d}/{w[1]:02d}-{w[2]:02d}/{w[3]:02d}"


def _season_coverage_reasons(windows: set[tuple[int, int, int, int]]) -> list[str]:
    days = 366
    cover = [0] * (days + 1)
    for sm, sd, em, ed in windows:
        start, end = _doy(sm, sd), _doy(em, ed)
        span = range(start, end + 1) if start <= end else list(range(start, days + 1)) + list(range(1, end + 1))
        for doy in span:
            cover[doy] += 1
    # A season ending Feb 28 and the next starting Mar 1 leaves Feb 29 to
    # the season covering Feb 28.
    if cover[_FEB29_DOY] == 0 and cover[_FEB29_DOY - 1] == 1:
        cover[_FEB29_DOY] = 1
    reasons = []
    if any(c == 0 for c in cover[1:]):
        reasons.append("season_gap")
    if any(c > 1 for c in cover[1:]):
        reasons.append("season_overlap")
    return reasons


def _minutes(t) -> int:
    return t.hour * 60 + t.minute


def _window_minutes(start, end) -> list[tuple[int, int]] | None:
    """Half-open minute ranges for a clock window; None when zero-length."""
    s, e = _minutes(start), _minutes(end)
    if s == e:
        return [(0, 1440)] if s == 0 else None
    if e == 0:
        return [(s, 1440)]
    if e < s:
        return [(s, 1440), (0, e)]
    return [(s, e)]


def _tier_reasons(rows: list[Any]) -> list[str]:
    tiers = []
    for r in rows:
        lo, hi = _num(_get(r, "tier_min_kwh")), _num(_get(r, "tier_max_kwh"))
        if hi is not None and hi >= _OPEN_TOP_KWH:
            hi = None
        tiers.append((lo or 0.0, hi))
    tiers.sort(key=lambda t: t[0])
    reasons = []
    if tiers[0][0] > 1.0:
        reasons.append("tier_not_from_zero")
    for (lo, hi), (nlo, _nhi) in zip(tiers, tiers[1:]):
        if hi is None:
            reasons.append("tier_overlap")
            continue
        step = nlo - hi
        if step < -1e-6:
            reasons.append("tier_overlap")
        elif step > 1.0 + 1e-6:
            reasons.append("tier_gap")
    if tiers[-1][1] is not None:
        reasons.append("tier_no_open_top")
    return reasons


def evaluate_computable(
    rate_type: Any,
    components: Iterable[Any],
    *,
    name: str | None = None,
    holiday_calendar: str | None = None,
) -> ComputableResult:
    rt = _val(rate_type)
    comps = list(components or [])
    reasons: list[str] = []
    warnings: list[str] = []

    if rt not in KNOWN_RATE_TYPES:
        reasons.append("unknown_rate_type")
    if rt in ("demand", "demand_tou") or any(_val(_get(c, "component_type")) == "demand" for c in comps):
        reasons.append("demand_charges_unsupported")
    if rt == "tou_tiered":
        reasons.append("tou_tiered_unsupported")
    if rt == "complex":
        reasons.append("complex_rate_type_unsupported")

    label_blob = " ".join(
        str(_get(c, k) or "") for c in comps for k in ("period_label", "tier_label", "season")
    ) + " " + (name or "")
    if _EVENT_RE.search(label_blob):
        reasons.append("event_pricing_unsupported")
    if _DYNAMIC_RE.search(label_blob):
        reasons.append("dynamic_pricing_unsupported")

    energy = []
    for c in comps:
        if _val(_get(c, "component_type")) != "energy":
            continue
        rv = _num(_get(c, "rate_value"))
        if rv is None:
            reasons.append("energy_rate_not_numeric")
            continue
        if normalize_unit(_get(c, "unit")) not in PER_KWH_UNITS:
            reasons.append(f"energy_unit_not_per_kwh:{_get(c, 'unit')}")
        if rv < 0:
            warnings.append("negative_energy_rate")
        energy.append(c)
    if not energy:
        reasons.append("missing_energy_rates")

    for c in comps:
        ctype = _val(_get(c, "component_type"))
        unit = normalize_unit(_get(c, "unit"))
        if ctype in ("fixed", "minimum") and unit not in PERIODIC_UNITS:
            reasons.append(f"unrecognized_unit:{_get(c, 'unit')}")
        elif ctype == "adjustment" and unit not in PERIODIC_UNITS and unit not in PER_KWH_UNITS:
            reasons.append(f"unrecognized_unit:{_get(c, 'unit')}")
        if ctype == "minimum":
            warnings.append("minimum_charge_is_bill_floor")

    all_in_energy = any(_ALL_IN_RE.search(str(_get(e, "tier_label") or "")) for e in energy)
    unflagged_kwh_riders = [
        c for c in comps
        if _val(_get(c, "component_type")) == "adjustment"
        and normalize_unit(_get(c, "unit")) in PER_KWH_UNITS
        and not _get(c, "included_in_energy", False)
    ]
    if all_in_energy and unflagged_kwh_riders:
        reasons.append("rider_inclusion_ambiguous")

    # --- seasons ---------------------------------------------------------
    windows = [_season_window(e) for e in energy]
    seasonal_mode = rt in SEASONAL_FAMILY or any(w is not None for w in windows)
    if seasonal_mode and energy:
        if any(w is None for w in windows):
            reasons.append(
                "seasonal_missing_calendar_dates" if rt in SEASONAL_FAMILY else "season_partial_calendar"
            )
        distinct = {w for w in windows if w is not None}
        bad = [w for w in distinct if not (_valid_month_day(w[0], w[1]) and _valid_month_day(w[2], w[3]))]
        for w in bad:
            reasons.append(f"season_invalid_date:{_fmt_window(w)}")
        if distinct and not bad:
            reasons.extend(_season_coverage_reasons(distinct))
    groups: dict = {}
    for e, w in zip(energy, windows):
        groups.setdefault(w if seasonal_mode else None, []).append(e)

    # --- TOU -------------------------------------------------------------
    clocks = [(_coerce_time(_get(e, "period_start_time")), _coerce_time(_get(e, "period_end_time"))) for e in energy]
    tou_mode = rt in TOU_FAMILY or any(s is not None and t is not None for s, t in clocks)
    has_tiers = any(
        _num(_get(e, "tier_min_kwh")) is not None or _num(_get(e, "tier_max_kwh")) is not None
        for e in energy
    )

    if tou_mode and energy:
        if has_tiers:
            reasons.append("tou_tiered_unsupported")
        if any(s is None or t is None for s, t in clocks):
            reasons.append("tou_missing_clock_windows")
        day_types = [_val(_get(e, "day_type")) for e in energy]
        if any(not d for d in day_types):
            reasons.append("tou_missing_day_type")
        if any(d and d not in VALID_DAY_TYPES for d in day_types):
            reasons.append("tou_invalid_day_type")
        has_holiday_rows = "holiday" in day_types
        if has_holiday_rows and not holiday_calendar:
            warnings.append("holiday_rows_require_calendar")
        if not any(r.startswith(("tou_missing", "tou_invalid")) for r in reasons):
            check_days = ["weekday", "weekend"] + (["holiday"] if has_holiday_rows else [])
            for w, rows in groups.items():
                for day in check_days:
                    cover = [0] * 1440
                    for e in rows:
                        if _val(_get(e, "day_type")) not in (day, "all"):
                            continue
                        spans = _window_minutes(
                            _coerce_time(_get(e, "period_start_time")),
                            _coerce_time(_get(e, "period_end_time")),
                        )
                        if spans is None:
                            reasons.append("tou_zero_length_window")
                            continue
                        for a, b in spans:
                            for m in range(a, b):
                                cover[m] += 1
                    where = f"{day}@{_fmt_window(w)}" if seasonal_mode else day
                    if any(c == 0 for c in cover):
                        reasons.append(f"tou_gap:{where}")
                    if any(c > 1 for c in cover):
                        reasons.append(f"tou_overlap:{where}")
    elif energy:
        for w, rows in groups.items():
            tiered = [r for r in rows if _num(_get(r, "tier_min_kwh")) is not None or _num(_get(r, "tier_max_kwh")) is not None]
            if tiered and len(tiered) != len(rows):
                reasons.append("tier_mixed")
            elif tiered:
                reasons.extend(_tier_reasons(tiered))
            elif len({round(float(_get(r, "rate_value")), 6) for r in rows}) > 1:
                reasons.append(
                    f"ambiguous_energy_rows:{_fmt_window(w)}" if seasonal_mode else "ambiguous_energy_rows"
                )
        if has_tiers:
            warnings.append("tiers_accumulate_per_billing_period")

    reasons = list(dict.fromkeys(reasons))
    return ComputableResult(
        computable=not reasons,
        reasons=tuple(reasons),
        warnings=tuple(dict.fromkeys(warnings)),
    )


def tariff_contract(tariff: Any, *, holiday_calendar: str | None = None) -> dict:
    """API contract fields for a tariff row (ORM or mapping with components)."""
    res = evaluate_computable(
        _get(tariff, "rate_type"),
        _get(tariff, "rate_components") or [],
        name=_get(tariff, "name"),
        holiday_calendar=holiday_calendar,
    )
    cf = _get(tariff, "confidence_factors") or {}
    warnings = list(res.warnings)
    code = str(_get(tariff, "code") or "").upper()
    if code.startswith("OEB-RPP") or cf.get("origin") == "oeb_feed":
        # OEB RPP rows are commodity only; delivery, regulatory, rebate and
        # HST are not on the OEB table unless carried as FIXED rows.
        warnings.append("commodity_only_bill_incomplete")
    return {
        "computable": res.computable,
        "computable_reasons": list(res.reasons),
        "computable_warnings": warnings,
        "needs_review": bool(cf.get("needs_review") or cf.get("tou_seasonal_incomplete")),
    }
