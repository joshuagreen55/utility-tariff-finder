import enum
from datetime import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class MonitoringStatus(str, enum.Enum):
    UNCHANGED = "unchanged"
    CHANGED = "changed"
    ERROR = "error"
    PENDING = "pending"


class ReviewStatus(str, enum.Enum):
    PENDING = "pending"
    REVIEWED = "reviewed"
    DISMISSED = "dismissed"


class MonitoringSource(Base):
    __tablename__ = "monitoring_sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    utility_id: Mapped[int] = mapped_column(Integer, ForeignKey("utilities.id"), nullable=False, index=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    check_frequency_days: Mapped[int] = mapped_column(Integer, default=7, nullable=False)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_changed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[MonitoringStatus] = mapped_column(Enum(MonitoringStatus), default=MonitoringStatus.PENDING, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    utility: Mapped["Utility"] = relationship("Utility", back_populates="monitoring_sources")
    logs: Mapped[list["MonitoringLog"]] = relationship("MonitoringLog", back_populates="source", lazy="noload")

    def __repr__(self) -> str:
        return f"<MonitoringSource(id={self.id}, url='{self.url[:50]}...')>"


class MonitoringLog(Base):
    __tablename__ = "monitoring_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source_id: Mapped[int] = mapped_column(Integer, ForeignKey("monitoring_sources.id"), nullable=False, index=True)
    checked_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    changed: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    diff_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    review_status: Mapped[ReviewStatus] = mapped_column(Enum(ReviewStatus), default=ReviewStatus.PENDING, nullable=False)

    source: Mapped["MonitoringSource"] = relationship("MonitoringSource", back_populates="logs")

    def __repr__(self) -> str:
        return f"<MonitoringLog(id={self.id}, changed={self.changed})>"


from app.models.utility import Utility  # noqa: E402
