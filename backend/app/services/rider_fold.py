"""R22: fold rider schedules into the full per-kWh price, deterministically.

Rider schedules (fuel, environmental, DSM, FAC ...) that a base plan names
are often extracted as separate rider-only plans in the same batch (Georgia
FCR-27 / ECCR-15 / DSM-R-16) or parsed from the rider document itself
(app.services.rider_docs). This module adds them to the plan's ENERGY rows:

    energy = base * (1 + sum(percent-of-base riders)) + sum(per-kWh riders)

Rules (never guess):
* a rider is folded only when the plan names it (same rider family: fuel,
  eccr, dsm, ...) and it is not already in the price;
* exactly one rider schedule per family must match (TOU plans prefer the TOU
  variant, non-TOU plans never take a TOU-only variant);
* each ENERGY row must map to exactly one rider amount (single amount, or a
  unique season / TOU-period match); otherwise that rider is left unadded
  and the plan stays "base only";
* primary / transmission-voltage amounts are ignored when a secondary or
  unlabelled amount exists (residential service is secondary).
Percent-of-base riders also scale the fixed (basic service) charge.
"""
from __future__ import annotations

import re
from typing import Any

from app.services.price_basis import NOT_PRICE_RIDER_RE, _keys, unadded_price_riders

_MONTHS = {m: i for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}
_DEFAULT_SEASON = {
    "summer": {6, 7, 8, 9}, "winter": {12, 1, 2}, "non-summer": {10, 11, 12, 1, 2, 3, 4, 5},
    "nonsummer": {10, 11, 12, 1, 2, 3, 4, 5}, "non-winter": {6, 7, 8}, "spring": {4, 5}, "fall": {10, 11},
}
_HIGH_VOLTAGE_RE = re.compile(r"\bprimary\b|transmission\s+(?:voltage|level|service)|sub[\s-]*transmission|\btrans\b", re.I)
_SECONDARY_RE = re.compile(r"\bsecondary\b|\bsec\b", re.I)
_TOU_RE = re.compile(r"\btou\b|time[\s-]+of[\s-]+(?:use|day)|on[\s-]*peak|off[\s-]*peak", re.I)
_PCT_BASE_RE = re.compile(r"%|percent", re.I)


def _g(c: Any, k: str, d: Any = None):
    return c.get(k, d) if isinstance(c, dict) else getattr(c, k, d)


def _ctype(c: Any) -> str:
    v = _g(c, "component_type")
    return str(getattr(v, "value", v) or "").lower()


def season_months(label: Any) -> set[int]:
    """Months covered by a season label ("June-September", "Oct–May", "Summer")."""
    s = str(label or "").lower()
    if not s.strip():
        return set()
    found = re.findall(r"(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*", s)
    if len(found) >= 2:
        a, b = _MONTHS[found[0]], _MONTHS[found[1]]
        out, m = set(), a
        while True:
            out.add(m)
            if m == b:
                break
            m = m % 12 + 1
        return out
    for k, v in _DEFAULT_SEASON.items():
        if re.search(rf"\b{k}\b", s):
            return set(v)
    return set()


def _period(label: Any) -> str:
    s = str(label or "").lower()
    if re.search(r"super[\s-]*off", s):
        return "super_off"
    if re.search(r"off[\s-]*peak|all\s+other\s+hours", s):
        return "off"
    if re.search(r"on[\s-]*peak|\bpeak\b", s):
        return "on"
    if re.search(r"shoulder|mid[\s-]*peak", s):
        return "mid"
    return ""


def _is_tou_plan(t: Any) -> bool:
    rt = str(_g(t, "rate_type") or "").lower()
    return "tou" in rt or any(_period(_g(c, "period_label")) for c in (_g(t, "components") or [])
                              if _ctype(c) == "energy")


def _rider_text(r: Any) -> str:
    return " ".join([str(_g(r, "name") or "")] + [
        f"{_g(c, 'period_label') or ''} {_g(c, 'tier_label') or ''}" for c in (_g(r, "components") or [])])


def rider_family(r: Any) -> set[str]:
    return _keys(str(_g(r, "name") or "")) or _keys(_rider_text(r))


def _per_kwh_rows(r: Any) -> list[dict]:
    rows = [c for c in (_g(r, "components") or []) if _ctype(c) == "adjustment"
            and "kwh" in str(_g(c, "unit") or "").lower().replace(" ", "")
            and not NOT_PRICE_RIDER_RE.search(f"{_g(c, 'period_label') or ''} {_g(c, 'tier_label') or ''}")]
    low = [c for c in rows if not _HIGH_VOLTAGE_RE.search(f"{_g(c, 'period_label') or ''} {_g(c, 'tier_label') or ''}")]
    return low or []


def _pct_rows(r: Any) -> list[dict]:
    return [c for c in (_g(r, "components") or []) if _ctype(c) == "adjustment"
            and _PCT_BASE_RE.search(str(_g(c, "unit") or ""))
            and re.search(r"base", f"{_g(c, 'unit') or ''} {_g(c, 'period_label') or ''} {_g(c, 'tier_label') or ''}", re.I)]


def _pick(rows: list[dict], energy: dict, tou_rider: bool) -> float | None:
    """The one rider amount ($/kWh) for an ENERGY row, or None if not unique."""
    if not rows:
        return None
    vals = {round(float(_g(c, "rate_value")), 8) for c in rows}
    if len(vals) == 1:
        return vals.pop()
    cand = rows
    em = season_months(_g(energy, "season"))
    if any(season_months(_g(c, "season")) for c in cand):
        if not em:
            return None
        allyear = set(range(1, 13))
        cov = lambda c: (season_months(_g(c, "season")) or allyear) & em  # noqa: E731
        cand = [c for c in cand if cov(c)]
        if len({round(float(_g(c, "rate_value")), 8) for c in cand}) > 1:
            seasonal = [c for c in cand if season_months(_g(c, "season"))]
            if seasonal and not tou_rider:
                best = max(len(cov(c)) for c in seasonal)
                cand = [c for c in seasonal if len(cov(c)) == best]
    if tou_rider and len({round(float(_g(c, "rate_value")), 8) for c in cand}) > 1:
        ep = _period(_g(energy, "period_label"))
        if not ep:
            return None
        cand = [c for c in cand if _period(_g(c, "period_label")) == ep]
    if len({round(float(_g(c, "rate_value")), 8) for c in cand}) > 1:
        sec = [c for c in cand if _SECONDARY_RE.search(f"{_g(c, 'period_label') or ''} {_g(c, 'tier_label') or ''}")]
        cand = sec or cand
    vals = {round(float(_g(c, "rate_value")), 8) for c in cand}
    return vals.pop() if len(vals) == 1 else None


def plan_fold(plan: Any, riders: list[Any]) -> dict | None:
    """What to fold into ``plan`` from rider schedules (pure; None = nothing)."""
    comps = list(_g(plan, "components") or [])
    energy = [c for c in comps if _ctype(c) == "energy"]
    if not energy:
        return None
    unadded = unadded_price_riders(
        riders_referenced=_g(plan, "riders_referenced_not_shown"),
        missing_fields=_g(plan, "missing_fields"),
        energy_includes_riders=_g(plan, "energy_includes_riders"),
        components=comps,
    )
    need: set[str] = set()
    for u in unadded:
        need |= _keys(str(u))
    if not need:
        return None
    tou_plan = _is_tou_plan(plan)
    out = {"per_kwh": [], "pct": [], "families": set()}
    for fam in sorted(need):
        cands = [r for r in riders if fam in rider_family(r)]
        tou_c = [r for r in cands if _TOU_RE.search(str(_g(r, "name") or ""))]
        std_c = [r for r in cands if r not in tou_c]
        chosen = (tou_c or std_c) if tou_plan else std_c
        if len(chosen) != 1:
            continue
        r = chosen[0]
        pct = _pct_rows(r)
        kwh = _per_kwh_rows(r)
        if pct and not kwh:
            if len({float(_g(c, "rate_value")) for c in pct}) != 1:
                continue
            out["pct"].append((r, float(_g(pct[0], "rate_value")) / 100.0))
            out["families"].add(fam)
            continue
        if not kwh:
            continue
        tou_rider = r in tou_c
        picks = [_pick(kwh, e, tou_rider) for e in energy]
        if any(p is None for p in picks):
            continue
        out["per_kwh"].append((r, picks))
        out["families"].add(fam)
    return out if out["families"] else None


def apply_fold(plan: Any, fold: dict) -> list[str]:
    """Mutate ``plan``: new ENERGY values, audit rows, cleared hints. Returns rider names folded."""
    comps = list(_g(plan, "components") or [])
    energy = [c for c in comps if _ctype(c) == "energy"]
    pct_total = sum(p for _, p in fold["pct"])
    adds = [0.0] * len(energy)
    for _, picks in fold["per_kwh"]:
        adds = [a + p for a, p in zip(adds, picks)]
    audit = []
    for i, e in enumerate(energy):
        base = float(e["rate_value"])
        e.setdefault("base_rate_value", base)
        e["rate_value"] = round(base * (1 + pct_total) + adds[i], 6)
    for r, picks in fold["per_kwh"]:
        for e, p in zip(energy, picks):
            audit.append({
                "component_type": "adjustment", "unit": "$/kWh", "rate_value": p,
                "period_label": f"{_g(r, 'name')} (folded)"[:120], "season": e.get("season"),
                "tier_label": e.get("period_label") or e.get("tier_label"),
                "included_in_energy": True,
            })
    if pct_total:
        for c in comps:
            if _ctype(c) == "fixed":
                c.setdefault("base_rate_value", float(c["rate_value"]))
                c["rate_value"] = round(float(c["rate_value"]) * (1 + pct_total), 6)
    # one audit row per distinct (rider, season, value)
    seen, dedup = set(), []
    for a in audit:
        k = (a["period_label"], a["season"], a["tier_label"], a["rate_value"])
        if k not in seen:
            seen.add(k)
            dedup.append(a)
    comps.extend(dedup)
    plan.components = comps
    fams = fold["families"]

    def _resolved(h: str) -> bool:
        k = _keys(str(h))
        return bool(k) and k <= fams

    plan.riders_referenced_not_shown = [h for h in (plan.riders_referenced_not_shown or []) if not _resolved(h)]
    plan.missing_fields = [m for m in (plan.missing_fields or []) if not _resolved(m)]
    names = [str(_g(r, "name")) for r, _ in fold["per_kwh"]] + [str(_g(r, "name")) for r, _ in fold["pct"]]
    notes = dict(plan.confidence_notes or {})
    notes["riders_folded"] = names
    if fold["pct"]:
        notes["percent_of_base_riders"] = {str(_g(r, "name")): round(p * 100, 4) for r, p in fold["pct"]}
    plan.confidence_notes = notes
    return names
