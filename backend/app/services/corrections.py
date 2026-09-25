"""Apply an already-approved manual tariff correction (soft-supersede only).

Called from POST /api/tariff-corrections through ``AsyncSession.run_sync``.
One transaction: new live row (``approved``, ``origin='manual'``) + the
predecessor soft-superseded (reason ``manual``) or retired (reason
``manual_retire``) + a document pin + one change event carrying the ticket,
approver and idempotency key. The predecessor's components are never
touched. Replays of the same idempotency key return the original result.
"""
from __future__ import annotations

import hashlib
import json
import logging
from contextlib import contextmanager

from sqlalchemy import select

log = logging.getLogger(__name__)

UTILITY_LOCK_TTL = 120


class CorrectionError(Exception):
    def __init__(self, status: int, body: dict):
        super().__init__(body.get("detail") or body.get("reason"))
        self.status = status
        self.body = body


def payload_sha256(req) -> str:
    body = req.model_dump(mode="json", exclude={"idempotency_key"})
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


@contextmanager
def utility_lock(utility_id: int):
    """Share process_utility's per-utility Redis lock so a correction never
    races a refresh of the same utility. Degrades to no lock without Redis;
    the row lock on the predecessor still serializes corrections."""
    from app.config import settings

    lock = None
    try:
        import redis

        client = redis.Redis.from_url(
            settings.redis_url, socket_connect_timeout=0.5, socket_timeout=0.5
        )
        lock = client.lock(f"refresh:lock:utility:{utility_id}", timeout=UTILITY_LOCK_TTL)
        if not lock.acquire(blocking=False):
            raise CorrectionError(409, {
                "reason": "refresh_in_progress",
                "detail": f"utility {utility_id} is being refreshed; retry later",
            })
    except CorrectionError:
        raise
    except Exception as e:
        log.warning(f"Correction lock unavailable for utility {utility_id}: {e}")
        lock = None
    try:
        yield
    finally:
        if lock is not None:
            try:
                lock.release()
            except Exception:
                pass


def _live_successor(session, tariff) -> tuple[int | None, int | None]:
    """Follow the supersede chain to the current live row and its last event."""
    from app.models import Tariff, TariffChangeEvent
    from app.services.tariff_history import is_live

    seen = set()
    t = tariff
    while t is not None and not is_live(t) and t.superseded_by_tariff_id and t.id not in seen:
        seen.add(t.id)
        t = session.get(Tariff, t.superseded_by_tariff_id)
    live_id = t.id if t is not None and is_live(t) else None
    ev = session.execute(
        select(TariffChangeEvent.id)
        .where(TariffChangeEvent.before_tariff_id == tariff.id)
        .order_by(TariffChangeEvent.id.desc())
        .limit(1)
    ).scalar_one_or_none()
    return live_id, ev


def _response(session, event, *, replayed: bool, warnings: list[str] | None = None) -> dict:
    from app.models import Tariff, TariffPin
    from app.services.computable import evaluate_computable

    new = session.get(Tariff, event.after_tariff_id) if event.after_tariff_id else None
    pin_id = None
    contract: dict = {"computable": None, "computable_reasons": [], "computable_warnings": []}
    if new is not None:
        pin_id = session.execute(
            select(TariffPin.id).where(TariffPin.tariff_id == new.id).order_by(TariffPin.id.desc()).limit(1)
        ).scalar_one_or_none()
        res = evaluate_computable(new.rate_type, new.rate_components, name=new.name)
        contract = {
            "computable": res.computable,
            "computable_reasons": list(res.reasons),
            "computable_warnings": list(res.warnings),
        }
    return {
        "new_tariff_id": event.after_tariff_id,
        "superseded_tariff_id": event.before_tariff_id,
        "change_event_id": event.id,
        "pin_id": pin_id,
        **contract,
        "warnings": warnings or [],
        "replayed": replayed,
    }


def apply_correction(session, req, *, payload_sha: str) -> dict:
    from app.models import RateComponent, Tariff, TariffChangeEvent, Utility
    from app.services.pins import create_pin, ensure_monitoring_source, release_pins
    from app.services.tariff_history import is_live, record_event, supersede_tariff

    prior = session.execute(
        select(TariffChangeEvent).where(TariffChangeEvent.idempotency_key == req.idempotency_key)
    ).scalar_one_or_none()
    if prior is not None:
        if (prior.payload or {}).get("request_sha256") != payload_sha:
            raise CorrectionError(409, {
                "reason": "idempotency_key_reused",
                "detail": "idempotency_key was already used for a different correction",
            })
        return _response(session, prior, replayed=True)

    uid = req.target.utility_id
    if session.get(Utility, uid) is None:
        raise CorrectionError(404, {"detail": f"utility {uid} not found"})

    current = None
    if req.target.mode in ("replace", "retire"):
        current = session.execute(
            select(Tariff).where(Tariff.id == req.target.expected_live_tariff_id).with_for_update()
        ).scalar_one_or_none()
        if current is None:
            raise CorrectionError(404, {"detail": f"tariff {req.target.expected_live_tariff_id} not found"})
        if current.utility_id != uid:
            raise CorrectionError(409, {
                "reason": "wrong_utility",
                "detail": f"tariff {current.id} belongs to utility {current.utility_id}",
            })
        if not is_live(current):
            live_id, ev_id = _live_successor(session, current)
            raise CorrectionError(409, {
                "reason": "not_live",
                "current_live_tariff_id": live_id,
                "last_change_event_id": ev_id,
                "detail": (
                    f"tariff {current.id} is no longer live "
                    f"(superseded: {current.supersede_reason}); re-approve against the current row"
                ),
            })
    else:
        clash = session.execute(
            select(Tariff.id).where(
                Tariff.utility_id == uid,
                Tariff.name == req.tariff.name,
                Tariff.customer_class == req.tariff.customer_class,
                Tariff.superseded_by_tariff_id.is_(None),
                Tariff.supersede_reason.is_(None),
            ).limit(1)
        ).scalar_one_or_none()
        if clash is not None:
            raise CorrectionError(409, {
                "reason": "live_tariff_exists",
                "current_live_tariff_id": clash,
                "detail": "a live tariff with this name and class exists; use mode=replace",
            })

    event_kw = {
        "actor_type": "manual_api",
        "actor_id": req.approved_by,
        "ticket_id": req.ticket_id,
        "idempotency_key": req.idempotency_key,
        "source_url": req.evidence.source_url,
        "source_document_hash": req.evidence.source_document_sha256,
        "payload": {"request_sha256": payload_sha, "request": req.model_dump(mode="json")},
    }

    if req.target.mode == "retire":
        release_pins(session, current.id)
        event = supersede_tariff(session, current, reason="manual_retire", **event_kw)
        session.flush()
        return _response(session, event, replayed=False)

    t = req.tariff
    monitored = ensure_monitoring_source(session, uid, req.evidence.source_url)
    new = Tariff(
        utility_id=uid,
        name=t.name,
        code=t.code,
        customer_class=t.customer_class,
        rate_type=t.rate_type,
        is_default=t.is_default,
        description=t.description,
        effective_date=t.effective_date,
        source_url=req.evidence.source_url,
        source_document_hash=monitored.last_content_hash,
        last_verified_at=req.approved_at,
        approved=True,
        confidence_factors={
            "origin": "manual",
            "manual": {
                "ticket_id": req.ticket_id,
                "approved_by": req.approved_by,
                "approved_at": req.approved_at.isoformat(),
                "requested_by": req.requested_by,
                "evidence": req.evidence.model_dump(mode="json"),
            },
        },
    )
    for c in req.components:
        new.rate_components.append(RateComponent(**c.model_dump()))
    session.add(new)
    session.flush()

    if current is not None:
        release_pins(session, current.id)
        event = supersede_tariff(session, current, successor=new, reason="manual", **event_kw)
    else:
        event = record_event(
            session, decision="insert", reason="manual",
            utility_id=uid, after_tariff_id=new.id, **event_kw,
        )
    create_pin(
        session, new,
        source_url=req.evidence.source_url,
        origin="manual",
        cause=req.pin.cause if req.pin else None,
        ticket_id=req.ticket_id,
        pinned_by=req.approved_by,
    )
    session.flush()

    warnings = []
    if any(c.component_type.value == "energy" and c.rate_value > 1 for c in req.components):
        warnings.append("energy_rate_above_1_dollar_per_kwh")
    return _response(session, event, replayed=False, warnings=warnings)
