"""Celery task: decide proposed pin verifications (see app.services.pin_verification).

Not on beat by default. With the default Null verifier/arbiter every
proposal is held as ``verifier_unavailable`` without fetching or calling an
LLM, so enabling the beat entry is safe; real accepts need PIN_VERIFIER /
PIN_ARBITER adapters.
"""
import logging

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import TariffVerification
from app.services.corrections import CorrectionError, utility_lock
from app.services.pin_verification import default_gates, run_verification
from app.services.pins import open_periodic_verifications
from app.tasks.celery_app import celery_app

log = logging.getLogger(__name__)


@celery_app.task(
    name="app.tasks.verification.process_pin_verifications",
    time_limit=1800,
    soft_time_limit=1700,
)
def process_pin_verifications(limit: int = 20) -> dict:
    engine = get_sync_engine()
    gates = default_gates()
    with Session(engine) as session:
        opened = open_periodic_verifications(session)
        session.commit()
        pending = session.execute(
            select(TariffVerification.id, TariffVerification.utility_id)
            .where(TariffVerification.status == "proposed")
            .order_by(TariffVerification.created_at)
            .limit(limit)
        ).all()

    outcomes: dict[str, int] = {}
    for vid, uid in pending:
        try:
            with utility_lock(uid), Session(engine) as session:
                outcome = run_verification(session, session.get(TariffVerification, vid), gates)
                session.commit()
        except CorrectionError:
            outcome = "deferred_refresh_in_progress"
        except Exception as e:
            log.error(f"Verification {vid} crashed: {e}")
            outcome = "error"
        outcomes[outcome] = outcomes.get(outcome, 0) + 1
    summary = {"periodic_opened": len(opened), "processed": len(pending), "outcomes": outcomes}
    log.info(f"Pin verifications: {summary}")
    return summary
