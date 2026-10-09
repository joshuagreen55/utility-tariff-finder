"""R21: "base only" price safety net.

A plan whose source says per-kWh fuel / cost-recovery riders apply, but
whose stored ENERGY price does not include them, is priced at the BASE rate
only. It is flagged and must not count as Mysa-complete (full price rule).

Shared by the extraction pipeline (sets ``price_basis='base_only'``), the
API contract (``tariff_contract``) and the health score, so rows stored
before this rule are judged the same way from their stored factors.
"""
from __future__ import annotations

import re
from typing import Any, Iterable

# Riders that change the per-kWh price (fuel, cost recovery, efficiency,
# transmission, storm ...). Kept broad on purpose: a miss here means a
# silently understated price.
PER_KWH_RIDER_RE = re.compile(
    r"\bfuel\b|\bfac\b|\bfca\b|\bfcr\b|\beccr\b|environmental\s+(?:compliance\s+)?cost|"
    r"\bdsm\b|\bdsim\b|demand[\s-]+side\s+management|energy\s+efficiency|\bee\s+rider|"
    r"conservation\s+(?:improvement|cost)|\bcip\b|transmission\s+(?:cost|service\s+charge|rider)|\btcr\b|"
    r"energy\s+cost\s+(?:adjustment|recovery)|\beca\b|\becr\b|off[\s-]*system\s+sales|"
    r"storm|power\s+cost|purchased\s+power|\bpca\b|\bppca\b|\bfam\b|\bdcrr\b|\bscrr\b|\bbac\b|"
    r"resource\s+adjustment|renewable\s+(?:energy\s+)?(?:standard|development)|\bres\s+rider|"
    r"state\s+energy\s+policy|decoupling|mercury\s+cost|environmental\s+improvement|"
    r"applicable\s+riders|non[\s-]*bypassable|schedule\s*1\d{2}\b|sch\s*1\d{2}\b|"
    r"cost\s+recovery|adjustment\s+clause|rider\s+amounts?|rider\s+rates?",
    re.I,
)
# Never per-kWh price riders (percent taxes / fees, credits, optional programs).
NOT_PRICE_RIDER_RE = re.compile(
    r"franchise|\btax\b|sales\s+tax|gross\s+receipts|low[\s-]+income|credit|discount|rebate|"
    r"net[\s-]*meter|optional|green\s+(?:power|energy)|medical|late\s+payment|deposit",
    re.I,
)
# Key words used to decide whether a declared rider was folded into ENERGY.
_KEYS = (
    ("fuel", r"\bfuel\b|\bfac\b|\bfca\b|\bfcr\b"),
    ("eccr", r"\beccr\b|environmental\s+(?:compliance\s+)?cost"),
    ("dsm", r"\bdsm\b|\bdsim\b|demand[\s-]+side|energy\s+efficiency|conservation|\bcip\b"),
    ("transmission", r"transmission|\btcr\b"),
    ("storm", r"storm"),
    ("power_cost", r"power\s+cost|purchased\s+power|\bpca\b|\bppca\b|resource\s+adjustment|"
                   r"energy\s+cost\s+(?:adjustment|recovery)|\beca\b|\becr\b"),
    ("renewable", r"renewable|\brdf\b|\bres\b"),
    ("fam", r"\bfam\b|\bdcrr\b|\bscrr\b|\bbac\b"),
)


def _g(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _ctype(c: Any) -> str:
    v = _g(c, "component_type")
    return str(getattr(v, "value", v) or "").lower()


def is_per_kwh_price_rider(hint: str) -> bool:
    h = str(hint or "")
    return bool(PER_KWH_RIDER_RE.search(h)) and not NOT_PRICE_RIDER_RE.search(h)


def _keys(text: str) -> set[str]:
    return {k for k, rx in _KEYS if re.search(rx, text or "", re.I)}


def unadded_price_riders(
    *,
    riders_referenced: Iterable[str] | None,
    missing_fields: Iterable[str] | None,
    energy_includes_riders: bool | None,
    components: Iterable[Any] | None,
) -> list[str]:
    """Per-kWh riders the source references that are not in the ENERGY price.

    Empty list → the price is not "base only". A rider counts as added when
    a per-kWh ADJUSTMENT folded into ENERGY (``included_in_energy``) or an
    "all-in" ENERGY label names the same kind of rider (fuel, ECCR, DSM ...).
    """
    comps = list(components or [])
    folded_text = " ".join(
        f"{_g(c, 'tier_label') or ''} {_g(c, 'period_label') or ''}"
        for c in comps
        if (_ctype(c) == "adjustment" and _g(c, "included_in_energy"))
        or (_ctype(c) == "energy" and re.search(r"all[\s-]*in|\+", str(_g(c, "tier_label") or ""), re.I))
    )
    folded = _keys(folded_text)
    declared = [str(r) for r in (riders_referenced or []) if is_per_kwh_price_rider(str(r))]
    out = []
    for r in declared:
        k = _keys(r)
        if k and k <= folded:
            continue
        if not k and energy_includes_riders:
            continue  # generic name, model says riders are in the price
        out.append(r)
    if energy_includes_riders is not True:
        for m in missing_fields or []:
            ms = str(m)
            if ms in ("referenced_riders_missing", "sch1xx_riders_missing"):
                if not out:
                    out.append(ms)
                continue
            if is_per_kwh_price_rider(ms) and re.search(r"amount|rate|charge|not\s+(?:shown|included|added)|missing", ms, re.I):
                k = _keys(ms)
                if k and k <= folded:
                    continue
                out.append(ms)
    return list(dict.fromkeys(out))


def base_only_from_factors(confidence_factors: dict | None, components: Iterable[Any] | None) -> list[str]:
    """Unadded per-kWh riders for a stored row (from its confidence_factors)."""
    cf = confidence_factors or {}
    if cf.get("price_basis") == "base_only":
        return list(cf.get("riders_not_added") or ["base_only"])
    if cf.get("price_basis") == "full":
        return []
    return unadded_price_riders(
        riders_referenced=cf.get("riders_referenced_not_shown"),
        missing_fields=cf.get("missing_fields"),
        energy_includes_riders=cf.get("energy_includes_riders"),
        components=components,
    )


def not_full_price_reasons(confidence_factors: dict | None, components: Iterable[Any] | None,
                           source_url: str | None = None, name: str | None = None) -> list[str]:
    """Why a stored row's price is not the full per-kWh price (empty → full)."""
    cf = confidence_factors or {}
    out = []
    if base_only_from_factors(cf, components):
        out.append("base_only_riders_not_added")
    from app.services.source_quality import is_retail_offer, marketing_rounded_price

    if cf.get("price_basis") == "marketing_rounded" or (
        source_url and cf.get("price_basis") != "full"
        and marketing_rounded_price(source_url, components)
    ):
        out.append("marketing_page_rounded_price")
    if cf.get("wrong_jurisdiction"):
        out.append("wrong_jurisdiction_document")
    if cf.get("price_basis") == "stale_document":
        out.append("stale_rate_document")
    if cf.get("retail_offer") or (name and source_url is not None and is_retail_offer(name, source_url)):
        out.append("retail_offer_not_tariff")
    return out
