"""Map a model's free-form component ``kind`` / unit onto the recipe vocabulary.

The extractors label components in their own words (``energy``,
``rate_rider``, ``fixed_monthly_customer_charge``, ``volumetric``, …). The
recipes consume a closed set of kinds. This step decides the kind from what
the row *is*, read off the stored unit and sign, and uses the model's own
words only to pick between kinds that price identically in the sum:

* $/month, $/day, $/year            → ``fixed_monthly`` (never in ¢/kWh)
* $/kW                              → ``demand_charge``
* percent                           → ``rider_percent`` (unit ``%``)
* dimensionless factor              → ``multiplier`` (unit ``factor``)
* per-kWh                           → credit / base / rider / delivery /
                                       supply per the recipe's sides

A bare "per kWh" unit takes its scale from the document: the ¢ or $ printed
at the cited figure. When neither (or both) ground, the unit stays
ambiguous and the quote check holds the row.

The model's original kind is kept on ``model_kind`` for the audit trail.
"""
from __future__ import annotations

import re
from typing import Any

from app.services.pricing.quote_verifier import parse_unit, verify_component_quote

META_KINDS = frozenset({
    "season_calendar", "tou_schedule", "holiday_list", "tier_structure",
    "excluded_item", "event_day",
})

_CANONICAL = frozenset({
    *META_KINDS,
    "fixed_charge", "fixed_monthly", "customer_charge", "demand_charge",
    "base_energy", "rider_per_kwh", "rider_percent", "credit", "multiplier",
    "delivery_per_kwh", "default_supply", "regulated_commodity",
})

_META_WORDS = (
    ("holiday", "holiday_list"),
    ("season", "season_calendar"),
    ("calendar", "season_calendar"),
    ("schedule", "tou_schedule"),
    ("clock", "tou_schedule"),
    ("threshold", "tier_structure"),
    ("tier_structure", "tier_structure"),
    ("block_size", "tier_structure"),
)

_CREDIT_WORDS = re.compile(r"credit|rebate|discount|refund")
_RIDER_WORDS = re.compile(
    r"rider|adjust|fuel|surcharge|recover|regulat|transmission|conservation|"
    r"storm|dsm|demand_side|capacity|environment|variance|adder|clause|"
    r"\bfac\b|\bpca\b|\bfam\b|\bcap\b|tax|fee"
)
_SUPPLY_WORDS = re.compile(
    r"supply|generation|commodity|default|standard_offer|\bsos\b|"
    r"basic_service|energy_service|price_to_compare|\bptc\b|eecc|rpp|rolr|"
    r"\brrt\b|regulated_rate|electricity"
)
_DELIVERY_WORDS = re.compile(
    r"delivery|distribution|transmission|wires|\budc\b|\btdu\b|network|"
    r"connection|regulat|wmsr|rrrp|system_access"
)


def _words(*parts: Any) -> str:
    text = " ".join(str(p or "") for p in parts).lower()
    return re.sub(r"[\s\-/]+", "_", text)


def _amounts_negative(raw: dict[str, Any]) -> bool:
    amounts = [str(c.get("amount") or "") for c in raw.get("cells") or []
               if isinstance(c, dict) and c.get("amount") not in (None, "")]
    return bool(amounts) and all(a.strip().startswith("-") for a in amounts)


def _meta_kind(kind: str) -> str | None:
    if kind in META_KINDS:
        return kind
    for word, canon in _META_WORDS:
        if word in kind:
            return canon
    return None


def _resolve_scale(raw: dict[str, Any], document_text: str | None) -> str | None:
    """Pick $/kWh or ¢/kWh for a bare "per kWh" from the printed figures."""
    if not document_text:
        return None
    quote = raw.get("source_quote") or raw.get("quote")
    cells = [c for c in raw.get("cells") or [] if isinstance(c, dict)]
    if not quote or not cells:
        return None
    grounded = []
    for unit in ("$/kWh", "¢/kWh"):
        if all(
            verify_component_quote(
                document_text,
                quote=c.get("source_quote") or quote,
                unit=unit,
                amount=c.get("amount"),
                reflow_lines=2 if len(cells) == 1 else 25,
            ).ok
            for c in cells
        ):
            grounded.append(unit)
    return grounded[0] if len(grounded) == 1 else None


def _per_kwh_kind(raw: dict[str, Any], recipe: str) -> tuple[str, str | None]:
    """(kind, charge_category) for a per-kWh row under ``recipe``."""
    kind_w = _words(raw.get("kind"))
    cat_w = _words(raw.get("charge_category"))
    all_w = _words(raw.get("kind"), raw.get("charge_category"),
                   raw.get("code"), raw.get("name"))
    category = raw.get("charge_category")

    if _CREDIT_WORDS.search(kind_w) or (
        _CREDIT_WORDS.search(all_w) and not _RIDER_WORDS.search(kind_w)
    ):
        kind = "credit"
    elif _amounts_negative(raw):
        kind = "credit"
    else:
        kind = ""

    if recipe in {"deregulated", "texas_tdu"}:
        side_w = cat_w or kind_w
        supply = bool(_SUPPLY_WORDS.search(side_w)) and not _DELIVERY_WORDS.search(side_w)
        if not cat_w and not supply and not _DELIVERY_WORDS.search(kind_w):
            supply = bool(_SUPPLY_WORDS.search(all_w)) and not _DELIVERY_WORDS.search(all_w)
        side = "supply" if supply and recipe == "deregulated" else "delivery"
        if not kind:
            kind = "default_supply" if side == "supply" else "delivery_per_kwh"
        return kind, side

    if recipe == "provincial_ontario":
        if kind:
            return kind, category
        if _SUPPLY_WORDS.search(cat_w or kind_w) and not _DELIVERY_WORDS.search(cat_w or kind_w):
            return "regulated_commodity", category
        if _RIDER_WORDS.search(kind_w) and not _DELIVERY_WORDS.search(kind_w):
            return "rider_per_kwh", category
        return "delivery_per_kwh", category

    if recipe == "provincial_alberta":
        if kind:
            return kind, category
        if _SUPPLY_WORDS.search(cat_w or kind_w) or "energy" in (cat_w or kind_w):
            return "default_supply", category
        if _RIDER_WORDS.search(kind_w):
            return "rider_per_kwh", category
        return "delivery_per_kwh", category

    if kind:
        return kind, category
    # bundled: base vs rider only decides which row anchors the price grid;
    # both add into the same all-in sum.
    if _RIDER_WORDS.search(kind_w) or _DELIVERY_WORDS.search(kind_w):
        return "rider_per_kwh", category
    if re.search(r"energy|base|volumetric|^rate$|per_kwh|supply|generation|bundled", kind_w) \
            or re.search(r"base|^energy$|bundled|electricity", cat_w):
        return "base_energy", category
    return "rider_per_kwh", category


def canonicalize_component(
    raw: dict[str, Any],
    recipe_code: str | None,
    document_text: str | None = None,
) -> dict[str, Any]:
    out = dict(raw)
    model_kind = str(raw.get("kind") or "").strip()
    kind_w = _words(model_kind)
    recipe = (recipe_code or "bundled").strip()

    meta = _meta_kind(kind_w)
    spec = parse_unit(str(raw.get("unit") or ""))
    if model_kind in _CANONICAL:
        if spec is not None and spec.scale == "%" and model_kind == "rider_percent":
            out["unit"] = "%"
        elif spec is not None and spec.scale == "factor" and model_kind == "multiplier":
            out["unit"] = "factor"
        elif spec is not None and spec.denom == "kwh" and spec.ambiguous:
            resolved = _resolve_scale(raw, document_text)
            if resolved:
                out["unit"] = resolved
        return out
    if meta and (spec is None or spec.scale is None or meta == "tier_structure"):
        out["kind"] = meta
    elif spec is None:
        if model_kind not in _CANONICAL and meta:
            out["kind"] = meta
        return out
    elif spec.scale == "factor":
        out["kind"], out["unit"] = "multiplier", "factor"
    elif spec.scale == "%":
        out["kind"], out["unit"] = "rider_percent", "%"
    elif spec.denom in {"month", "day", "year"}:
        out["kind"] = "fixed_monthly"
    elif spec.denom == "kw":
        out["kind"] = "demand_charge"
    elif spec.denom == "kwh":
        if spec.scale is None:
            if not spec.ambiguous:
                if meta or "tier" in kind_w:
                    out["kind"] = "tier_structure"
                return out
            resolved = _resolve_scale(raw, document_text)
            if resolved:
                out["unit"] = resolved
        out["kind"], category = _per_kwh_kind(raw, recipe)
        if category is not None:
            out["charge_category"] = category
    if out.get("kind") != model_kind:
        out["model_kind"] = model_kind
    return out


def canonicalize_extract(
    raw_components: list[dict[str, Any]],
    recipe_code: str | None,
    document_text: str | None = None,
) -> list[dict[str, Any]]:
    return [
        canonicalize_component(r, recipe_code, document_text)
        if isinstance(r, dict) else r
        for r in raw_components
    ]


__all__ = ["META_KINDS", "canonicalize_component", "canonicalize_extract"]
