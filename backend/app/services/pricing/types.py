"""In-memory types for the pricing compiler. All money is ``Decimal``."""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


def money(value: Any) -> Decimal:
    """Parse a exact decimal from str/int/Decimal. Rejects float."""
    if isinstance(value, float):
        raise TypeError(
            "float is not allowed in the pricing compiler; "
            "pass a Decimal or decimal string"
        )
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def to_dollars_per_kwh(amount: Decimal, unit: str) -> Decimal:
    """Normalize a per-kWh amount to $/kWh. Percent / dimensionless unchanged."""
    u = (unit or "").strip().lower().replace(" ", "")
    if u in {"$/kwh", "usd/kwh", "cad/kwh", "$/kwh"}:
        return amount
    if u in {"¢/kwh", "c/kwh", "cents/kwh", "¢/kwh"}:
        return amount / Decimal("100")
    if u in {"mills/kwh", "mill/kwh"}:
        return amount / Decimal("1000")
    if u in {"percent", "%", "pct"}:
        return amount  # caller treats as percent points
    if u in {"dimensionless", "factor", "x"}:
        return amount
    raise ValueError(f"unsupported pricing unit: {unit!r}")


@dataclass(frozen=True, order=True)
class CellKey:
    """One cell of the season × period × day_type × tier grid."""

    season: str = "all"
    period: str = "all"
    day_type: str = "all"
    tier: str = "all"

    @classmethod
    def from_mapping(cls, raw: dict[str, Any] | None) -> "CellKey":
        raw = raw or {}
        return cls(
            season=str(raw.get("season") or "all"),
            period=str(raw.get("period") or "all"),
            day_type=str(raw.get("day_type") or "all"),
            tier=str(raw.get("tier") or "all"),
        )

    def as_dict(self) -> dict[str, str]:
        return {
            "season": self.season,
            "period": self.period,
            "day_type": self.day_type,
            "tier": self.tier,
        }


@dataclass
class ComponentInput:
    """One applying component version fed to the compiler (fixture or ORM)."""

    code: str
    kind: str
    unit: str
    cells: list[dict[str, Any]]
    name: str = ""
    charge_category: str | None = None
    percent_base_codes: list[str] = field(default_factory=list)
    multiplier_target_codes: list[str] = field(default_factory=list)
    loss_sensitive: bool = False
    source_page: str | None = None
    source_quote: str | None = None

    def amounts_by_cell(self) -> dict[CellKey, Decimal]:
        out: dict[CellKey, Decimal] = {}
        for raw in self.cells:
            key = CellKey.from_mapping(raw)
            out[key] = money(raw["amount"])
        return out


@dataclass
class PlanInput:
    """A plan composition ready to compile."""

    plan_key: str
    name: str
    recipe_code: str
    components: list[ComponentInput]
    code: str | None = None
    rate_type: str | None = None
    utility_name: str | None = None
    # Official all-in ¢/kWh for golden assertions (display / rounded form).
    # Empty for texas_tdu plans (no all-in by policy).
    official_cents: list[Decimal] = field(default_factory=list)
    # Official delivery-only ¢/kWh (texas_tdu goldens).
    official_delivery_cents: list[Decimal] = field(default_factory=list)
    # Exact expected $/kWh cells (optional; derived from components when empty).
    expected_dollars: list[Decimal] = field(default_factory=list)
    # Bill-level notes (OER, taxes, franchise fees) — never folded into $/kWh.
    bill_level_notes: list[str] = field(default_factory=list)
    notes: str | None = None
