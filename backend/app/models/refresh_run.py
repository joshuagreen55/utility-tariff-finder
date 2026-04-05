import enum
from datetime import datetime

from sqlalchemy import DateTime, Enum, Integer, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class RefreshType(str, enum.Enum):
    monthly = "monthly"
    quarterly = "quarterly"
    manual = "manual"


class RefreshRun(Base):
    __tablename__ = "refresh_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    refresh_type: Mapped[RefreshType] = mapped_column(Enum(RefreshType), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    utilities_targeted: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    utilities_processed: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    tariffs_added: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    tariffs_updated: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    tariffs_stale: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    errors: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    summary_json: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    error_details: Mapped[str | None] = mapped_column(Text, nullable=True)

    def __repr__(self) -> str:
        return f"<RefreshRun(id={self.id}, type={self.refresh_type}, started={self.started_at})>"
