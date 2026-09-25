from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class TariffPin(Base):
    """Document-scoped pin on a curated live tariff.

    While the pinned document is unchanged, heuristic refresh paths hold
    instead of overwriting the row. A CHANGED monitoring signal on
    ``pinned_source_url``, a scraper extract from a different document, or
    the periodic re-check opens a ``TariffVerification``; only an accepted
    verification moves the pin to a new row.

    ``pinned_source_hash`` uses monitoring's normalized-text hash for the
    URL (``MonitoringSource.last_content_hash``); NULL until the first check.
    """

    __tablename__ = "tariff_pins"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tariff_id: Mapped[int] = mapped_column(Integer, ForeignKey("tariffs.id"), nullable=False, index=True)
    utility_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    # manual | agent_verified
    origin: Mapped[str] = mapped_column(String(20), nullable=False)
    # extraction_error | source_error
    cause: Mapped[str | None] = mapped_column(String(30), nullable=True)
    pinned_source_url: Mapped[str] = mapped_column(Text, nullable=False)
    pinned_source_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    ticket_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    pinned_by: Mapped[str | None] = mapped_column(String(200), nullable=True)
    pinned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    # active | held | released
    state: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    hold_reason: Mapped[str | None] = mapped_column(String(60), nullable=True)
    consecutive_holds: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    released_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class TariffVerification(Base):
    """A proposed refresh of a pinned tariff and its automated decision.

    Status machine: ``proposed`` → ``accepted`` (the pinned row is
    soft-superseded by a verified row and the pin moves to it) or ``held``
    (pinned row untouched; ``hold_reason`` is machine-readable and the next
    signal retries). No human queue in steady state.
    """

    __tablename__ = "tariff_verifications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    pin_id: Mapped[int] = mapped_column(Integer, ForeignKey("tariff_pins.id"), nullable=False, index=True)
    tariff_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    utility_id: Mapped[int] = mapped_column(Integer, nullable=False)
    # source_changed | new_document | scraper_conflict | periodic
    trigger: Mapped[str] = mapped_column(String(30), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="proposed", index=True)
    new_source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    new_source_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    proposed: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    gate_results: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    hold_reason: Mapped[str | None] = mapped_column(String(60), nullable=True)
    accepted_tariff_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
