from datetime import datetime

from geoalchemy2 import Geometry
from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class ServiceTerritory(Base):
    __tablename__ = "service_territories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    utility_id: Mapped[int] = mapped_column(Integer, ForeignKey("utilities.id"), nullable=False, index=True)
    geometry = mapped_column(Geometry("MULTIPOLYGON", srid=4326), nullable=True)
    zip_codes: Mapped[list[str] | None] = mapped_column(ARRAY(String(10)), nullable=True)
    postal_code_prefixes: Mapped[list[str] | None] = mapped_column(ARRAY(String(10)), nullable=True)
    source: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    utility: Mapped["Utility"] = relationship("Utility", back_populates="service_territories")

    def __repr__(self) -> str:
        return f"<ServiceTerritory(id={self.id}, utility_id={self.utility_id})>"


from app.models.utility import Utility  # noqa: E402
