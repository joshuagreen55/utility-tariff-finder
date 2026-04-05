from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class RatePageFingerprint(Base):
    __tablename__ = "rate_page_fingerprints"

    utility_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("utilities.id", ondelete="CASCADE"), primary_key=True
    )
    url: Mapped[str] = mapped_column(Text, primary_key=True)
    content_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    etag: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_modified: Mapped[str | None] = mapped_column(Text, nullable=True)
    content_length: Mapped[int | None] = mapped_column(Integer, nullable=True)
    checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    changed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    def __repr__(self) -> str:
        return f"<RatePageFingerprint(utility_id={self.utility_id}, url='{self.url[:50]}...')>"
