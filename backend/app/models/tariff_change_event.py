from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class TariffChangeEvent(Base):
    """Append-only audit log of tariff writes (who / what / why / before→after).

    UPDATE, DELETE and TRUNCATE are rejected by a DB trigger. Tariff ids are
    plain integers (no FK) so the log survives any legacy hard delete and a
    delete never needs to rewrite history.

    ``decision`` values: ``insert`` (new live row), ``supersede`` (row
    retired in favour of ``after_tariff_id``), ``retire`` (retired with no
    successor), ``hold`` (a write was refused because the live row is
    protected; the proposal is in ``payload``), ``metadata`` (non-rate field
    filled on a live row, e.g. reason ``effective_date_fill``),
    ``hard_delete`` (written by the DB trigger with a row snapshot).
    """

    __tablename__ = "tariff_change_events"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_tariff_change_events_idempotency_key"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )
    utility_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    decision: Mapped[str] = mapped_column(String(40), nullable=False)
    reason: Mapped[str | None] = mapped_column(String(50), nullable=True)
    # pipeline | oeb | cleanup | script | manual_api | agent_verify | db_trigger
    actor_type: Mapped[str] = mapped_column(String(20), nullable=False)
    actor_id: Mapped[str | None] = mapped_column(String(200), nullable=True)
    before_tariff_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    after_tariff_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    ticket_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_document_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    refresh_run_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(200), nullable=True)
    payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    def __repr__(self) -> str:
        return (
            f"<TariffChangeEvent(id={self.id}, decision={self.decision}, "
            f"before={self.before_tariff_id}, after={self.after_tariff_id})>"
        )
