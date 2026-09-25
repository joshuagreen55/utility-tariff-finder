"""Document-scoped pins and verification proposals (sync Session helpers).

A pin protects a curated live tariff from heuristic refresh paths while its
source document is unchanged. Signals that the document may have moved on
open a ``TariffVerification`` (status ``proposed``); ``pin_verification``
decides it automatically. See docs/MYSA_CONSUMER_CONTRACT.md and AGENTS.md.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select

log = logging.getLogger(__name__)

OPEN_PIN_STATES = ("active", "held")
PERIODIC_RECHECK_DAYS = 365


def active_pin_for(session, tariff_id: int):
    from app.models import TariffPin

    return session.execute(
        select(TariffPin).where(
            TariffPin.tariff_id == tariff_id,
            TariffPin.state.in_(OPEN_PIN_STATES),
        )
    ).scalar_one_or_none()


def pinned_tariff_ids(session, utility_id: int) -> set[int]:
    from app.models import TariffPin

    return set(session.execute(
        select(TariffPin.tariff_id).where(
            TariffPin.utility_id == utility_id,
            TariffPin.state.in_(OPEN_PIN_STATES),
        )
    ).scalars().all())


def ensure_monitoring_source(session, utility_id: int, url: str):
    """Pinned documents must be watched even if the utility's monitored
    rate page is a different URL."""
    from app.models import MonitoringSource, MonitoringStatus

    source = session.execute(
        select(MonitoringSource).where(
            MonitoringSource.utility_id == utility_id,
            MonitoringSource.url == url,
        )
    ).scalars().first()
    if source is None:
        source = MonitoringSource(utility_id=utility_id, url=url, status=MonitoringStatus.PENDING)
        session.add(source)
        session.flush()
    return source


def create_pin(
    session,
    tariff,
    *,
    source_url: str,
    origin: str,
    cause: str | None = None,
    ticket_id: str | None = None,
    pinned_by: str | None = None,
    source_hash: str | None = None,
):
    """Pin ``tariff`` to ``source_url``. The baseline hash defaults to the
    monitoring source's current hash (NULL until its first check)."""
    from app.models import TariffPin

    source = ensure_monitoring_source(session, tariff.utility_id, source_url)
    pin = TariffPin(
        tariff_id=tariff.id,
        utility_id=tariff.utility_id,
        origin=origin,
        cause=cause,
        pinned_source_url=source_url,
        pinned_source_hash=source_hash or source.last_content_hash,
        ticket_id=ticket_id,
        pinned_by=pinned_by,
        state="active",
        consecutive_holds=0,
    )
    session.add(pin)
    session.flush()
    return pin


def release_pins(session, tariff_id: int) -> int:
    from app.models import TariffPin

    now = datetime.now(timezone.utc)
    pins = session.execute(
        select(TariffPin).where(
            TariffPin.tariff_id == tariff_id,
            TariffPin.state.in_(OPEN_PIN_STATES),
        )
    ).scalars().all()
    for p in pins:
        p.state = "released"
        p.released_at = now
    return len(pins)


def propose_verification(
    session,
    pin,
    *,
    trigger: str,
    new_source_url: str | None = None,
    new_source_hash: str | None = None,
    proposed: dict | None = None,
):
    """Open (or return the already-open) verification for ``pin``."""
    from app.models import TariffVerification

    existing = session.execute(
        select(TariffVerification).where(
            TariffVerification.pin_id == pin.id,
            TariffVerification.status == "proposed",
        )
    ).scalars().first()
    if existing is not None:
        if proposed and not existing.proposed:
            existing.proposed = proposed
        return existing
    v = TariffVerification(
        pin_id=pin.id,
        tariff_id=pin.tariff_id,
        utility_id=pin.utility_id,
        trigger=trigger,
        status="proposed",
        new_source_url=new_source_url or pin.pinned_source_url,
        new_source_hash=new_source_hash,
        proposed=proposed,
    )
    session.add(v)
    session.flush()
    log.info(
        f"Opened verification {v.id} for pinned tariff {pin.tariff_id} "
        f"(trigger={trigger})"
    )
    return v


def on_source_changed(session, source_url: str, new_hash: str) -> list:
    """Monitoring saw ``source_url`` change: open verifications for every
    open pin on that document whose baseline differs."""
    from app.models import TariffPin

    pins = session.execute(
        select(TariffPin).where(
            TariffPin.pinned_source_url == source_url,
            TariffPin.state.in_(OPEN_PIN_STATES),
        )
    ).scalars().all()
    opened = []
    for pin in pins:
        if pin.pinned_source_hash == new_hash:
            continue
        if pin.pinned_source_hash is None:
            pin.pinned_source_hash = new_hash  # first observation: baseline only
            continue
        opened.append(propose_verification(
            session, pin, trigger="source_changed", new_source_hash=new_hash,
        ))
    return opened


def on_source_checked(session, source_url: str, content_hash: str) -> None:
    """Record a baseline for pins created before their first check."""
    from app.models import TariffPin

    now = datetime.now(timezone.utc)
    for pin in session.execute(
        select(TariffPin).where(
            TariffPin.pinned_source_url == source_url,
            TariffPin.state.in_(OPEN_PIN_STATES),
        )
    ).scalars().all():
        if pin.pinned_source_hash is None:
            pin.pinned_source_hash = content_hash
        pin.last_checked_at = now


def open_periodic_verifications(session, *, older_than_days: int = PERIODIC_RECHECK_DAYS) -> list:
    """Safety net for static wrong documents that never report CHANGED."""
    from app.models import TariffPin, TariffVerification

    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    recent = (
        select(TariffVerification.id)
        .where(
            TariffVerification.pin_id == TariffPin.id,
            TariffVerification.created_at >= cutoff,
        )
        .exists()
    )
    pins = session.execute(
        select(TariffPin).where(
            TariffPin.state.in_(OPEN_PIN_STATES),
            TariffPin.pinned_at < cutoff,
            ~recent,
        )
    ).scalars().all()
    return [propose_verification(session, p, trigger="periodic") for p in pins]
