"""R26 write gate: checks run before any tariff insert or supersede.

Pure functions over ORM rows or plain dicts. A non-empty result from
``evaluate`` means HOLD: the proposal is logged for review (a ``hold``
change event with reason ``write_gate:<rule>``) and nothing is written or
superseded.

Rules (R25 findings):
  tou_downgrade      a TOU plan would be replaced by one with no TOU or fewer periods (FPL RTR-1)
  tou_adders_unused  on/off-peak adders stored as side adjustments on a non-TOU plan (FPL RTR-1)
  price_jump         an energy price moves > PRICE_JUMP with no newer effective date and no
                     rider fold over an unchanged base to explain it
  unfiled_source     pro-forma / draft / "Effective: XXXX" document (PPL pro-forma supplement)
  event_in_everyday  critical-peak / event charges folded into everyday prices (DTE CPP)
  dup_same_code      a new row beside a live row with the same schedule code while the
                     reconciliation is skipped (DTE D1 / D1.2 / D1.8 / D1.11 / D2)
"""
from __future__ import annotations

import re
from datetime import date
from statistics import mean
from typing import Any, Iterable

PRICE_JUMP = 0.35
TOU_TYPES = {"tou", "tou_tiered", "seasonal_tou", "demand_tou"}

_EVENT_RE = re.compile(
    r"critical[\s-]*peak|\bcpp\b|peak[\s-]*(?:event|day)|event[\s-]*(?:hour|day|period|charge)|"
    r"conservation[\s-]*day|peak[\s-]*time[\s-]*rebate|\bdynamic[\s-]*peak[\s-]*event",
    re.I,
)
_TOU_ADDER_RE = re.compile(r"\b(?:on|off|mid|super[\s-]*off)[\s-]*peak\b", re.I)
_UNFILED_URL_RE = re.compile(r"pro[\s_-]*forma|[/_-]draft[/_.-]|proposed[\s_-]*tariff", re.I)
_UNFILED_TEXT_RE = re.compile(
    r"effective\s*(?:date)?\s*:?\s*x{4,}|issued\s*:?\s*x{4,}|\bpro[\s-]*forma\b|\bdraft\b\s+(?:tariff|rate|schedule)",
    re.I,
)


def _g(o: Any, k: str, default=None):
    if isinstance(o, dict):
        return o.get(k, default)
    return getattr(o, k, default)


def _val(x) -> str:
    return str(getattr(x, "value", x) or "").lower()


def _ctype(c) -> str:
    return _val(_g(c, "component_type"))


def _label(c) -> str:
    return " ".join(str(_g(c, k) or "") for k in ("period_label", "tier_label"))


def rate_type_of(row) -> str:
    return _val(_g(row, "rate_type"))


def energy_rows(components: Iterable) -> list:
    return [c for c in (components or []) if _ctype(c) == "energy"]


def energy_values(components: Iterable) -> list[float]:
    out = []
    for c in energy_rows(components):
        try:
            out.append(float(_g(c, "rate_value")))
        except (TypeError, ValueError):
            continue
    return out


def _period_class(label: str) -> str:
    t = re.sub(r"[^a-z]+", " ", label.lower())
    words = set(t.split())
    if "super" in words and ("off" in words or "offpeak" in words):
        return "super_off"
    if words & {"ultra", "overnight", "ulo"}:
        return "ultra_low"
    if words & {"off", "offpeak", "economy", "eco", "low"} or "off peak" in t:
        return "off"
    if words & {"mid", "midpeak", "shoulder", "intermediate", "intermedia", "partial", "part"}:
        return "mid"
    if words & {"critical", "cpp", "event"}:
        return "critical"
    if words & {"on", "onpeak", "peak", "pea"}:
        return "on"
    # Anything else (supplier product names, "Heat Pump kWh", "All hours", season-only
    # labels) is not a TOU period.
    return ""


def period_count(components: Iterable) -> int:
    """Distinct TOU period classes among ENERGY rows (season words ignored)."""
    keys = set()
    for c in energy_rows(components):
        lab = str(_g(c, "period_label") or "").strip()
        if lab:
            keys.add(_period_class(lab))
        elif _g(c, "period_start_time") not in (None, ""):
            keys.add(f"{_g(c, 'period_start_time')}-{_g(c, 'period_end_time')}")
    keys.discard("")
    # Event / critical-peak prices are not an everyday period: moving them out of everyday
    # energy (the correct treatment) must not look like a TOU downgrade.
    keys.discard("critical")
    return len(keys)


_TOU_NAME_RE = re.compile(r"\b(tou|time[\s-]*of[\s-]*(use|day)|tod|on[\s-]*peak|off[\s-]*peak)\b", re.I)


def is_tou(rate_type: str, components: Iterable, name: str | None = None) -> bool:
    """A plan is TOU when it has >= 2 real TOU periods, or is typed TOU and either has a
    period or is named as a time-of-use plan (a 1-row 'demand_tou' with 'All hours' is not)."""
    comps = list(components or [])
    n = period_count(comps)
    if n >= 2:
        return True
    if rate_type in TOU_TYPES:
        return n >= 1 or name is None or bool(_TOU_NAME_RE.search(name or ""))
    return False


def tou_downgrade(old_type: str, old_comps, new_type: str, new_comps, old_name: str | None = None) -> str | None:
    if not is_tou(old_type, old_comps, old_name if old_name is not None else ""):
        return None
    if not is_tou(new_type, new_comps):
        return f"live plan is TOU ({old_type}, {period_count(old_comps)} periods); new plan is {new_type} with no TOU periods"
    po, pn = period_count(old_comps), period_count(new_comps)
    if po >= 2 and pn < po:
        return f"TOU periods {po} -> {pn}"
    return None


def tou_adders_unused(new_type: str, new_comps) -> str | None:
    if new_type in TOU_TYPES:
        return None
    adders = [c for c in (new_comps or []) if _ctype(c) == "adjustment" and not _g(c, "included_in_energy")
              and _TOU_ADDER_RE.search(_label(c)) and abs(float(_g(c, "rate_value") or 0)) > 1e-9]
    if adders:
        return f"{len(adders)} on/off-peak adder(s) not applied to a {new_type or 'non-TOU'} plan: " + \
            "; ".join(_label(c).strip()[:60] for c in adders[:3])
    return None


def event_in_everyday(new_comps) -> str | None:
    bad = []
    for c in new_comps or []:
        lab = _label(c)
        if not _EVENT_RE.search(lab):
            continue
        ct = _ctype(c)
        if ct == "adjustment" and _g(c, "included_in_energy"):
            bad.append(lab.strip())
        elif ct == "energy" and not str(_g(c, "period_label") or "").strip() and _EVENT_RE.search(str(_g(c, "tier_label") or "")):
            bad.append(lab.strip())
    if bad:
        return "critical-peak / event charge inside everyday energy: " + "; ".join(b[:60] for b in bad[:3])
    return None


def _base_mean(extracted_comps) -> float | None:
    """Mean of rider-fold ``base_rate_value`` on extracted ENERGY rows (R22)."""
    bases = []
    for c in energy_rows(extracted_comps):
        b = _g(c, "base_rate_value")
        if b is not None:
            try:
                bases.append(float(b))
            except (TypeError, ValueError):
                pass
    return mean(bases) if bases else None


def price_jump(old_comps, new_comps, *, old_eff: date | None, new_eff: date | None,
               extracted_comps=None, old_scope: str = "", new_scope: str = "",
               limit: float = PRICE_JUMP) -> str | None:
    ov, nv = energy_values(old_comps), energy_values(new_comps)
    if not ov or not nv:
        return None
    om, nm = mean(ov), mean(nv)
    if om <= 0:
        return None
    change = nm / om - 1.0
    if abs(change) <= limit:
        return None
    if new_eff and old_eff and new_eff > old_eff:
        return None  # a newer edition explains it
    if new_scope == "delivery_plus_default_supply" and old_scope != new_scope and change > 0:
        return None  # half-plan -> delivery + default supply (Joshua default 2)
    base = _base_mean(extracted_comps) if extracted_comps is not None else None
    if base is not None and abs(base / om - 1.0) <= limit and change > 0:
        return None  # same base, riders folded in (R22): the move is the riders
    return (f"mean energy {om * 100:.3f} -> {nm * 100:.3f} c/kWh ({change:+.0%}); "
            f"effective {old_eff} -> {new_eff}")


def unfiled_source(source_url: str | None, page_text: str | None = None) -> str | None:
    if source_url and _UNFILED_URL_RE.search(source_url):
        return f"pro-forma / draft document URL: {source_url[:120]}"
    if page_text:
        m = _UNFILED_TEXT_RE.search(page_text[:20000])
        if m:
            return f"document is unfiled / pro-forma ('{m.group(0)[:40]}')"
    return None


_NOT_A_PLAN_RE = re.compile(
    r"\b(typical|sample|example|illustrative)\s+(monthly\s+)?bills?\b|\bbill\s+(illustration|example|comparison|calculator)\b|"
    r"\billustration\b", re.I)


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def has_energy_price(components: Iterable) -> bool:
    return any((_num(_g(c, "rate_value")) or 0) > 0 for c in energy_rows(components))


def evaluate(*, new_type: str, new_comps, old=None, extracted_comps=None,
             new_eff: date | None = None, new_scope: str = "", source_url: str | None = None,
             unfiled_reason: str | None = None, dup_row=None, reconcile_skipped: bool = False,
             old_stays_live: bool = False, new_name: str | None = None,
             is_new_plan: bool = False) -> list[tuple[str, str]]:
    """[(rule, detail)] — empty means the write may proceed.

    ``old_stays_live``: the live row compared with would NOT be retired by this
    write (not a same-name refresh and not a clear replacement) — with the
    reconcile skipped, the write would add a same-code duplicate (R26 DTE).
    ``is_new_plan``: no live row is replaced (a brand-new plan)."""
    out: list[tuple[str, str]] = []
    if new_name and _NOT_A_PLAN_RE.search(new_name):
        out.append(("not_a_plan", f"'{new_name[:60]}' is a bill illustration, not a tariff"))
    if is_new_plan and not has_energy_price(new_comps):
        out.append(("no_energy_price", "new plan has no energy price"))
    r = unfiled_reason or unfiled_source(source_url)
    if r:
        out.append(("unfiled_source", r))
    r = event_in_everyday(extracted_comps if extracted_comps is not None else new_comps)
    if r:
        out.append(("event_in_everyday", r))
    r = tou_adders_unused(new_type, extracted_comps if extracted_comps is not None else new_comps)
    if r:
        out.append(("tou_adders_unused", r))
    if old is not None:
        oc = list(_g(old, "rate_components") or _g(old, "components") or [])
        ot = rate_type_of(old)
        r = tou_downgrade(ot, oc, new_type, new_comps, old_name=str(_g(old, "name") or ""))
        if r:
            out.append(("tou_downgrade", r))
        ocf = _g(old, "confidence_factors") or {}
        r = price_jump(oc, new_comps, old_eff=_g(old, "effective_date"), new_eff=new_eff,
                       extracted_comps=extracted_comps,
                       old_scope=str(ocf.get("energy_scope") or ""), new_scope=new_scope)
        if r:
            out.append(("price_jump", r))
        if old_stays_live and reconcile_skipped:
            out.append(("dup_same_code", f"live row {_g(old, 'id')} '{str(_g(old, 'name'))[:60]}' has the same "
                                         f"schedule code and would stay live beside the new row "
                                         f"(not a clear replacement; reconciliation skipped)"))
    elif dup_row is not None and reconcile_skipped:
        out.append(("dup_same_code", f"live row {_g(dup_row, 'id')} '{str(_g(dup_row, 'name'))[:60]}' has the same "
                                     f"schedule code and the reconciliation is skipped"))
    return out


def pair_check(old, new) -> list[tuple[str, str]]:
    """Gate for supersedes outside the main write (clear replacement, vintage)."""
    oc = list(_g(old, "rate_components") or [])
    nc = list(_g(new, "rate_components") or [])
    out = []
    r = tou_downgrade(rate_type_of(old), oc, rate_type_of(new), nc, old_name=str(_g(old, "name") or ""))
    if r:
        out.append(("tou_downgrade", r))
    ocf, ncf = _g(old, "confidence_factors") or {}, _g(new, "confidence_factors") or {}
    r = price_jump(oc, nc, old_eff=_g(old, "effective_date"), new_eff=_g(new, "effective_date"),
                   old_scope=str(ocf.get("energy_scope") or ""), new_scope=str(ncf.get("energy_scope") or ""))
    if r and not ncf.get("riders_folded"):
        out.append(("price_jump", r))
    return out
