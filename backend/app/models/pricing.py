"""Pricing-component data model (PR A).

Components are the primary stored facts (base energy, riders, default
supply, regulated commodity, delivery, calendars, TOU schedules). All-in
per-kWh prices are *computed* by ``app.services.pricing.compiler`` from a
plan composition + market recipe — never written by an LLM.

Soft-supersede only: versions and compositions are retired with
``superseded_by_*`` / ``supersede_reason`` / ``superseded_at``, matching
the existing tariff history invariant. Live = both supersede columns NULL.
"""
from __future__ import annotations

import enum
from datetime import date, datetime

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class PricingComponentKind(str, enum.Enum):
    BASE_ENERGY = "base_energy"
    RIDER_PER_KWH = "rider_per_kwh"
    RIDER_PERCENT = "rider_percent"
    MULTIPLIER = "multiplier"
    CREDIT = "credit"
    DEFAULT_SUPPLY = "default_supply"
    REGULATED_COMMODITY = "regulated_commodity"
    DELIVERY_PER_KWH = "delivery_per_kwh"
    SEASON_CALENDAR = "season_calendar"
    TOU_SCHEDULE = "tou_schedule"
    HOLIDAY_LIST = "holiday_list"
    TIER_STRUCTURE = "tier_structure"
    FIXED_CHARGE = "fixed_charge"
    EVENT_DAY = "event_day"
    EXCLUDED_ITEM = "excluded_item"


class RiderDisposition(str, enum.Enum):
    """Closed-world census decision for one rider on one plan (PR B fills)."""

    APPLIES = "applies"
    NOT_APPLICABLE = "not_applicable"
    OPTIONAL = "optional"
    LOCATION_FEE_OR_TAX = "location_fee_or_tax"
    EVENT_DAY = "event_day"


class MarketRecipeCode(str, enum.Enum):
    BUNDLED = "bundled"
    DEREGULATED = "deregulated"
    TEXAS_TDU = "texas_tdu"  # delivery only; supply = choose_a_retailer
    PROVINCIAL_ONTARIO = "provincial_ontario"
    PROVINCIAL_ALBERTA = "provincial_alberta"


class PricingComponent(Base):
    """Stable identity for a charge or rule that persists across editions."""

    __tablename__ = "pricing_components"
    __table_args__ = (
        UniqueConstraint(
            "utility_id", "kind", "code",
            name="uq_pricing_components_utility_kind_code",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    utility_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("utilities.id"), nullable=False, index=True
    )
    kind: Mapped[str] = mapped_column(String(40), nullable=False, index=True)
    code: Mapped[str] = mapped_column(String(100), nullable=False)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Charge-map role for deregulated / provincial recipes.
    charge_category: Mapped[str | None] = mapped_column(String(40), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    versions: Mapped[list["PricingComponentVersion"]] = relationship(
        "PricingComponentVersion",
        back_populates="component",
        lazy="selectin",
        foreign_keys="PricingComponentVersion.component_id",
    )

    def __repr__(self) -> str:
        return (
            f"<PricingComponent(id={self.id}, utility_id={self.utility_id}, "
            f"kind={self.kind}, code={self.code!r})>"
        )


class PricingComponentVersion(Base):
    """Values of a component over a valid-time interval, with evidence."""

    __tablename__ = "pricing_component_versions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    component_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("pricing_components.id"),
        nullable=False,
        index=True,
    )
    effective_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    end_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    edition_label: Mapped[str | None] = mapped_column(String(200), nullable=True)

    # Soft-supersede (audit-preserving). Live when both NULL.
    superseded_by_version_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("pricing_component_versions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    supersede_reason: Mapped[str | None] = mapped_column(String(50), nullable=True)
    superseded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Unit of ``cells[].amount`` — never floats. Amounts are decimal *strings*.
    # Examples: "$/kWh", "¢/kWh", "percent", "dimensionless".
    unit: Mapped[str] = mapped_column(String(40), nullable=False)
    currency: Mapped[str | None] = mapped_column(String(3), nullable=True)

    # Cell grid: list of {season, period, day_type, tier, amount: "0.18324"}.
    cells: Mapped[list] = mapped_column(JSONB, nullable=False, server_default="[]")

    # For rider_percent: which component codes form the percent base (DAG).
    percent_base_codes: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    # For multiplier: which component codes are scaled.
    multiplier_target_codes: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    # Ontario / Alberta: mark delivery lines that take the loss factor.
    loss_sensitive: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="false"
    )

    # Evidence — every stored number must cite a page + verbatim quote (PR B/C).
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_document_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_page: Mapped[str | None] = mapped_column(String(80), nullable=True)
    source_quote: Mapped[str | None] = mapped_column(Text, nullable=True)
    applicability_quote: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    component: Mapped["PricingComponent"] = relationship(
        "PricingComponent",
        back_populates="versions",
        foreign_keys=[component_id],
    )

    def __repr__(self) -> str:
        return (
            f"<PricingComponentVersion(id={self.id}, "
            f"component_id={self.component_id}, unit={self.unit!r})>"
        )


class PlanComposition(Base):
    """Which component versions make up one residential plan under a recipe."""

    __tablename__ = "plan_compositions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    utility_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("utilities.id"), nullable=False, index=True
    )
    # Stable opaque key (golden set / future Plan ID). Never changes.
    plan_key: Mapped[str] = mapped_column(String(120), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    rate_type: Mapped[str | None] = mapped_column(String(40), nullable=True)
    recipe_code: Mapped[str] = mapped_column(String(40), nullable=False)
    # Optional link to a legacy tariffs row once dual-write lands.
    tariff_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("tariffs.id", ondelete="SET NULL"), nullable=True, index=True
    )
    is_closed: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="false"
    )
    # live | future | held | retired — only one live per plan_key (partial unique).
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default="live", index=True
    )
    price_basis_label: Mapped[str | None] = mapped_column(String(80), nullable=True)
    # False for texas_tdu: delivery stored, no household all-in.
    has_all_in: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="true"
    )
    # e.g. choose_a_retailer for ERCOT competitive wires-only.
    supply_status: Mapped[str | None] = mapped_column(String(40), nullable=True)
    # Bill-level notes (OER, taxes, franchise fees) — never in per-kWh.
    bill_level_notes: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    effective_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    superseded_by_composition_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("plan_compositions.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    supersede_reason: Mapped[str | None] = mapped_column(String(50), nullable=True)
    superseded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    members: Mapped[list["PlanCompositionMember"]] = relationship(
        "PlanCompositionMember",
        back_populates="composition",
        lazy="selectin",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return (
            f"<PlanComposition(id={self.id}, plan_key={self.plan_key!r}, "
            f"recipe={self.recipe_code}, status={self.status})>"
        )


class PlanCompositionMember(Base):
    """One component version (or rider disposition) in a plan composition."""

    __tablename__ = "plan_composition_members"
    __table_args__ = (
        UniqueConstraint(
            "composition_id", "component_version_id",
            name="uq_plan_composition_members_comp_version",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    composition_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("plan_compositions.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    component_version_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("pricing_component_versions.id"),
        nullable=False,
        index=True,
    )
    # applies | not_applicable | optional | location_fee_or_tax | event_day
    disposition: Mapped[str] = mapped_column(
        String(40), nullable=False, server_default="applies"
    )
    disposition_page: Mapped[str | None] = mapped_column(String(80), nullable=True)
    disposition_quote: Mapped[str | None] = mapped_column(Text, nullable=True)

    composition: Mapped["PlanComposition"] = relationship(
        "PlanComposition", back_populates="members"
    )
    component_version: Mapped["PricingComponentVersion"] = relationship(
        "PricingComponentVersion", lazy="joined"
    )
