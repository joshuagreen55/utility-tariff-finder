import enum
from datetime import date, datetime

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class CustomerClass(str, enum.Enum):
    RESIDENTIAL = "residential"
    COMMERCIAL = "commercial"
    INDUSTRIAL = "industrial"
    LIGHTING = "lighting"


class RateType(str, enum.Enum):
    FLAT = "flat"
    TOU = "tou"
    TIERED = "tiered"
    DEMAND = "demand"
    SEASONAL = "seasonal"
    TOU_TIERED = "tou_tiered"
    SEASONAL_TOU = "seasonal_tou"
    SEASONAL_TIERED = "seasonal_tiered"
    DEMAND_TOU = "demand_tou"
    COMPLEX = "complex"


class ComponentType(str, enum.Enum):
    ENERGY = "energy"
    DEMAND = "demand"
    FIXED = "fixed"
    MINIMUM = "minimum"
    ADJUSTMENT = "adjustment"


class Tariff(Base):
    __tablename__ = "tariffs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    utility_id: Mapped[int] = mapped_column(Integer, ForeignKey("utilities.id"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    customer_class: Mapped[CustomerClass] = mapped_column(Enum(CustomerClass), nullable=False, index=True)
    rate_type: Mapped[RateType] = mapped_column(Enum(RateType), nullable=False, index=True)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    effective_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    end_date: Mapped[date | None] = mapped_column(Date, nullable=True)

    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_document_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    approved: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    confidence_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    confidence_factors: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    openei_id: Mapped[str | None] = mapped_column(String(50), unique=True, nullable=True, index=True)
    raw_openei_data: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    energy_schedule_weekday: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    energy_schedule_weekend: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    demand_schedule_weekday: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    demand_schedule_weekend: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    utility: Mapped["Utility"] = relationship("Utility", back_populates="tariffs")
    rate_components: Mapped[list["RateComponent"]] = relationship("RateComponent", back_populates="tariff", lazy="selectin", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"<Tariff(id={self.id}, name='{self.name}', class={self.customer_class.value})>"


class RateComponent(Base):
    __tablename__ = "rate_components"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tariff_id: Mapped[int] = mapped_column(Integer, ForeignKey("tariffs.id", ondelete="CASCADE"), nullable=False, index=True)
    component_type: Mapped[ComponentType] = mapped_column(Enum(ComponentType), nullable=False)
    unit: Mapped[str] = mapped_column(String(50), nullable=False)
    rate_value: Mapped[float] = mapped_column(Numeric(16, 6), nullable=False)

    tier_min_kwh: Mapped[float | None] = mapped_column(Float, nullable=True)
    tier_max_kwh: Mapped[float | None] = mapped_column(Float, nullable=True)
    tier_label: Mapped[str | None] = mapped_column(String(100), nullable=True)

    period_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    period_label: Mapped[str | None] = mapped_column(String(100), nullable=True)

    season: Mapped[str | None] = mapped_column(String(50), nullable=True)
    adjustment: Mapped[float | None] = mapped_column(Float, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    tariff: Mapped["Tariff"] = relationship("Tariff", back_populates="rate_components")

    def __repr__(self) -> str:
        return f"<RateComponent(id={self.id}, type={self.component_type.value}, rate={self.rate_value})>"


from app.models.utility import Utility  # noqa: E402
