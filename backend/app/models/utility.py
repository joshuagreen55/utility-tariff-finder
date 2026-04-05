import enum
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class UtilityType(str, enum.Enum):
    IOU = "investor_owned"
    MUNICIPAL = "municipal"
    COOPERATIVE = "cooperative"
    POLITICAL_SUBDIVISION = "political_subdivision"
    FEDERAL = "federal"
    STATE = "state"
    RETAIL_MARKETER = "retail_marketer"
    BEHIND_METER = "behind_meter"
    COMMUNITY_CHOICE = "community_choice"
    OTHER = "other"


class Country(str, enum.Enum):
    US = "US"
    CA = "CA"


class Utility(Base):
    __tablename__ = "utilities"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False, index=True)
    eia_id: Mapped[int | None] = mapped_column(Integer, unique=True, nullable=True, index=True)
    country: Mapped[Country] = mapped_column(Enum(Country), nullable=False, index=True)
    state_province: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    utility_type: Mapped[UtilityType] = mapped_column(Enum(UtilityType), nullable=False)
    website_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    tariff_page_urls: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    rate_page_url_override: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    tariffs: Mapped[list["Tariff"]] = relationship("Tariff", back_populates="utility", lazy="noload")
    service_territories: Mapped[list["ServiceTerritory"]] = relationship("ServiceTerritory", back_populates="utility", lazy="noload")
    monitoring_sources: Mapped[list["MonitoringSource"]] = relationship("MonitoringSource", back_populates="utility", lazy="noload")

    def __repr__(self) -> str:
        return f"<Utility(id={self.id}, name='{self.name}', eia_id={self.eia_id})>"


from app.models.tariff import Tariff  # noqa: E402
from app.models.territory import ServiceTerritory  # noqa: E402
from app.models.monitoring import MonitoringSource  # noqa: E402
