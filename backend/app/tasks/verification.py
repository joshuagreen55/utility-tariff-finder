"""Celery task: decide proposed pin verifications (see app.services.pin_verification).

Not on beat by default. With the default Null verifier/arbiter every
proposal is held as ``verifier_unavailable`` without fetching or calling an
LLM. With PIN_VERIFIER=jev and PIN_ARBITER=opus it spends Mercury + Opus
money (recorded in the LLM cost ledger as ``pin_verifications``); schedule
it only after a deliberate cost check.
"""
import logging

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.models import TariffVerification
from app.services.corrections import CorrectionError, utility_lock
from app.services.pin_verification import NullArbiter, NullVerifier, default_gates, run_verification
from app.services.pins import open_periodic_verifications
from app.tasks.celery_app import celery_app

log = logging.getLogger(__name__)


@celery_app.task(
    name="app.tasks.verification.process_pin_verifications",
    time_limit=1800,
    soft_time_limit=1700,
)
def process_pin_verifications(limit: int = 20) -> dict:
    from scripts import llm_cost

    engine = get_sync_engine()
    gates = default_gates()
    spends = not (isinstance(gates.verifier, NullVerifier) and isinstance(gates.arbiter, NullArbiter))
    llm_cost.reset()
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
    if spends:
        summary["llm_cost_usd"] = llm_cost.summary()["total_usd"]
        summary["jev_gateway_usd"] = round(getattr(gates.verifier, "gateway_usd", 0.0), 6)
        llm_cost.append_ledger("pin_verifications")
    log.info(f"Pin verifications: {summary}")
    return summary
