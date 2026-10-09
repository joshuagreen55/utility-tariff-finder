"""TOU clock + season calendar helpers for golden scoring (PR R27-5).

Golden plans carry optional top-level ``clocks`` and ``seasons`` arrays.
Clocks are only taken from published schedules (OEB, NS Power tariff book,
etc.) — never invented from period labels alone.
"""
from __future__ import annotations

from typing import Any, Iterable


def _norm_time(value: Any) -> str:
    """Normalize to HH:MM. ``24:00`` → ``00:00`` (Mysa midnight-end convention)."""
    s = str(value or "").strip()
    if not s:
        return ""
    s = s.replace(".", ":")
    parts = s.split(":")
    if len(parts) < 2:
        return s
    try:
        h = int(parts[0])
        m = int(parts[1])
    except ValueError:
        return s
    if h == 24 and m == 0:
        return "00:00"
    return f"{h:02d}:{m:02d}"


def _norm_label(value: Any, default: str = "all") -> str:
    s = str(value or default).strip().lower().replace(" ", "_").replace("-", "_")
    return s or default


def normalize_clock(raw: dict[str, Any]) -> tuple[str, str, str, str, str]:
    """Return (period, season, day_type, start, end) for exact comparison."""
    return (
        _norm_label(raw.get("period")),
        _norm_label(raw.get("season")),
        _norm_label(raw.get("day_type")),
        _norm_time(raw.get("start") or raw.get("period_start_time")),
        _norm_time(raw.get("end") or raw.get("period_end_time")),
    )


def normalize_season(raw: dict[str, Any]) -> tuple[str, int, int, int, int]:
    """Return (season, start_month, start_day, end_month, end_day)."""
    return (
        _norm_label(raw.get("season") or raw.get("name")),
        int(raw.get("start_month") or raw.get("season_start_month") or 0),
        int(raw.get("start_day") or raw.get("season_start_day") or 0),
        int(raw.get("end_month") or raw.get("season_end_month") or 0),
        int(raw.get("end_day") or raw.get("season_end_day") or 0),
    )


def clock_set(raws: Iterable[dict[str, Any]] | None) -> set[tuple]:
    return {normalize_clock(r) for r in (raws or []) if isinstance(r, dict)}


def season_set(raws: Iterable[dict[str, Any]] | None) -> set[tuple]:
    return {normalize_season(r) for r in (raws or []) if isinstance(r, dict)}


def clocks_match(
    expected: Iterable[dict[str, Any]] | None,
    got: Iterable[dict[str, Any]] | None,
) -> bool:
    """Exact multiset match on normalized clocks. Empty expected → vacuously True."""
    exp = clock_set(expected)
    if not exp:
        return True
    return exp == clock_set(got)


def seasons_match(
    expected: Iterable[dict[str, Any]] | None,
    got: Iterable[dict[str, Any]] | None,
) -> bool:
    exp = season_set(expected)
    if not exp:
        return True
    return exp == season_set(got)


def clocks_from_tou_schedule_component(comp: dict[str, Any]) -> list[dict[str, Any]]:
    """Pull clock rows from a ``tou_schedule`` meta component's cells."""
    out: list[dict[str, Any]] = []
    for cell in comp.get("cells") or []:
        if not isinstance(cell, dict):
            continue
        start = cell.get("start") or cell.get("period_start_time")
        end = cell.get("end") or cell.get("period_end_time")
        if not start or not end:
            continue
        out.append({
            "period": cell.get("period") or "all",
            "season": cell.get("season") or "all",
            "day_type": cell.get("day_type") or "all",
            "start": start,
            "end": end,
        })
    return out


def seasons_from_calendar_component(comp: dict[str, Any]) -> list[dict[str, Any]]:
    """Pull season rows from a ``season_calendar`` meta component's cells."""
    out: list[dict[str, Any]] = []
    for cell in comp.get("cells") or []:
        if not isinstance(cell, dict):
            continue
        sm = cell.get("start_month") or cell.get("season_start_month")
        if not sm:
            continue
        out.append({
            "season": cell.get("season") or cell.get("name") or "all",
            "start_month": int(sm),
            "start_day": int(cell.get("start_day") or cell.get("season_start_day") or 1),
            "end_month": int(cell.get("end_month") or cell.get("season_end_month") or 0),
            "end_day": int(cell.get("end_day") or cell.get("season_end_day") or 0),
        })
    return out


def extract_clocks_from_components(
    components: Iterable[dict[str, Any]] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Collect clocks/seasons from meta components in an extract payload."""
    clocks: list[dict[str, Any]] = []
    seasons: list[dict[str, Any]] = []
    for c in components or []:
        if not isinstance(c, dict):
            continue
        kind = str(c.get("kind") or "")
        if kind == "tou_schedule":
            clocks.extend(clocks_from_tou_schedule_component(c))
        elif kind == "season_calendar":
            seasons.extend(seasons_from_calendar_component(c))
    return clocks, seasons


def meta_components_from_plan_schedule(
    clocks: list[dict[str, Any]] | None,
    seasons: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Build dry-extract meta components from golden clocks/seasons."""
    out: list[dict[str, Any]] = []
    if clocks:
        out.append({
            "code": "tou_schedule",
            "kind": "tou_schedule",
            "unit": "dimensionless",
            "name": "TOU schedule",
            "disposition": "applies",
            "source_page": "p.golden",
            "source_quote": "golden TOU schedule",
            "cells": [
                {
                    "period": c.get("period") or "all",
                    "season": c.get("season") or "all",
                    "day_type": c.get("day_type") or "all",
                    "start": c.get("start"),
                    "end": c.get("end"),
                    "amount": "0",
                }
                for c in clocks
            ],
        })
    if seasons:
        out.append({
            "code": "season_calendar",
            "kind": "season_calendar",
            "unit": "dimensionless",
            "name": "Season calendar",
            "disposition": "applies",
            "source_page": "p.golden",
            "source_quote": "golden season calendar",
            "cells": [
                {
                    "season": s.get("season") or "all",
                    "start_month": s.get("start_month"),
                    "start_day": s.get("start_day"),
                    "end_month": s.get("end_month"),
                    "end_day": s.get("end_day"),
                    "amount": "0",
                }
                for s in seasons
            ],
        })
    return out


__all__ = [
    "clock_set",
    "clocks_from_tou_schedule_component",
    "clocks_match",
    "extract_clocks_from_components",
    "meta_components_from_plan_schedule",
    "normalize_clock",
    "normalize_season",
    "season_set",
    "seasons_from_calendar_component",
    "seasons_match",
]
