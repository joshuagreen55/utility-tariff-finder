"""Automated verification of pinned tariffs — no human queue in steady state.

``run_verification`` decides one ``proposed`` TariffVerification by running
gates in order, cheapest first; the first failing gate holds it:

 0. ``verifier_unavailable`` — a claim verifier and an arbiter must both be
    configured. The defaults (Null*) hold here at zero cost, so an
    unconfigured deployment never spends LLM money or rewrites a pinned row.
 1. ``budget_exhausted`` — at most PIN_VERIFY_DAILY_MAX decisions per UTC day.
 2. ``fetch_failed`` — fetch the (new) document text.
 3. ``injection_suspected`` — verifier screens the text before any LLM sees it.
 4. ``no_proposal`` — the scraper's proposal, else extract from the document.
 5. deterministic rules: ``customer_class_changed``, ``not_computable``,
    ``effective_date_not_newer`` (≥ allowed when the pin's cause is
    ``source_error``), ``no_rate_change``.
 6. ``claims_contradicted`` / ``claims_unsupported`` — every rate, clock,
    season and effective-date claim must be verified against the text
    (Jev ``jev_verify``-style). Derived all-in values need higher confidence.
 7. ``arbiter_rejected`` — Opus-tier typed verdict over old row, proposal and
    document.

An adapter call that raises (Mercury / Anthropic down, bad response) holds
as ``verifier_error`` at whichever of gates 3, 6, 7 it happened.

Accept: insert the verified row (approved, origin ``agent_verified``),
soft-supersede the pinned row (reason ``agent_verify_accept``), move the pin
to the new row, log a change event with every gate verdict. Hold: the pinned
row is untouched; the pin goes to state ``held`` with ``hold_reason`` and the
next signal retries. Holds are visible on the verification rows.

Verifier / arbiter adapters implement the small protocols below. Defaults
are the Null adapters; ``PIN_VERIFIER=jev`` / ``PIN_ARBITER=opus`` select the
Mercury Jev verifier and the Opus arbiter in ``app.services.pin_adapters``.
Jev alone never accepts: gate 0 requires both.
"""
from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Protocol

from sqlalchemy import func, select

log = logging.getLogger(__name__)

PIN_VERIFY_DAILY_MAX = int(os.environ.get("PIN_VERIFY_DAILY_MAX", "20"))
CLAIM_MIN_CONFIDENCE = 0.90
DERIVED_CLAIM_MIN_CONFIDENCE = 0.95
_FREE_HOLDS = ("verifier_unavailable", "budget_exhausted")


@dataclass(frozen=True)
class Claim:
    kind: str  # rate | derived_rate | clock | season | effective_date
    text: str
    component_index: int | None = None


@dataclass(frozen=True)
class ClaimVerdict:
    claim: Claim
    verdict: str  # verified | contradicted | unsupported
    confidence: float


@dataclass(frozen=True)
class ArbiterVerdict:
    accept: bool
    reason: str
    model: str | None = None


class ClaimVerifier(Protocol):
    available: bool

    def screen(self, text: str) -> bool:
        """True when the document is safe to hand to an LLM."""

    def verify(self, text: str, claims: list[Claim]) -> list[ClaimVerdict]: ...


class Arbiter(Protocol):
    available: bool

    def decide(self, *, current: dict, proposal: dict, document_text: str,
               previous_text: str | None) -> ArbiterVerdict: ...


class DocumentFetcher(Protocol):
    def fetch(self, url: str) -> tuple[str, str]:
        """(normalized text, monitoring-compatible hash); raises on failure."""


class ProposalExtractor(Protocol):
    def extract(self, url: str, text: str, current) -> dict | None: ...


class NullVerifier:
    available = False

    def screen(self, text: str) -> bool:
        return False

    def verify(self, text: str, claims: list[Claim]) -> list[ClaimVerdict]:
        return [ClaimVerdict(c, "unsupported", 0.0) for c in claims]


class NullArbiter:
    available = False

    def decide(self, **_kw) -> ArbiterVerdict:
        return ArbiterVerdict(False, "no arbiter configured")


class MonitorFetcher:
    def fetch(self, url: str) -> tuple[str, str]:
        from app.services.monitor import fetch_document

        return asyncio.run(fetch_document(url))


class PipelineExtractor:
    """Re-extract the pinned product from one document with the pipeline
    (Phase 3 + Phase 4). Spends LLM money; only reached after gates 0–3."""

    def extract(self, url: str, text: str, current) -> dict | None:
        from scripts import tariff_pipeline as tp

        page = tp.RatePage(url=url, title="", page_type="pdf" if url.lower().endswith(".pdf") else "html",
                           content=text)
        info = tp.get_utility_info(current.utility_id)
        extracted = tp.phase3_extract_tariffs([page], info.get("name", ""), state=info.get("state", ""))
        _report, valid = tp.phase4_validate(extracted, info.get("name", ""), info.get("state", ""))
        for et in valid:
            if et.customer_class != current.customer_class.value:
                continue
            if tp.tariffs_likely_same(et.name, current.name) or tp.same_vintage_product(
                et.name, current.name, code_a=et.code, code_b=current.code,
                rate_type_a=et.rate_type, rate_type_b=current.rate_type,
            ):
                comps = tp._build_rate_components(et)
                from app.services.tariff_history import serialize_components

                return {
                    "name": et.name,
                    "code": et.code or None,
                    "customer_class": et.customer_class,
                    "rate_type": et.rate_type,
                    "effective_date": et.effective_date or None,
                    "components": serialize_components(comps),
                }
        return None


@dataclass
class Gates:
    verifier: ClaimVerifier = field(default_factory=NullVerifier)
    arbiter: Arbiter = field(default_factory=NullArbiter)
    fetcher: DocumentFetcher = field(default_factory=MonitorFetcher)
    extractor: ProposalExtractor = field(default_factory=PipelineExtractor)
    daily_max: int = PIN_VERIFY_DAILY_MAX


PIN_VERIFIERS = ("none", "jev")
PIN_ARBITERS = ("none", "opus")


def default_gates() -> Gates:
    """Gates from env: PIN_VERIFIER ∈ {none, jev}, PIN_ARBITER ∈ {none, opus}.
    Unknown values or missing credentials fall back to the Null adapter."""
    from app.services import pin_adapters

    gates = Gates()
    verifier = os.environ.get("PIN_VERIFIER", "none").strip().lower()
    arbiter = os.environ.get("PIN_ARBITER", "none").strip().lower()
    if verifier not in PIN_VERIFIERS:
        log.warning(f"PIN_VERIFIER={verifier!r} is not one of {PIN_VERIFIERS}; using none (holds)")
    if arbiter not in PIN_ARBITERS:
        log.warning(f"PIN_ARBITER={arbiter!r} is not one of {PIN_ARBITERS}; using none (holds)")
    gates.verifier = pin_adapters.build_verifier(verifier) or gates.verifier
    gates.arbiter = pin_adapters.build_arbiter(arbiter) or gates.arbiter
    return gates


def _is_all_in(c: dict) -> bool:
    return c.get("component_type") == "energy" and "all-in" in str(c.get("tier_label") or "").lower()


def _base_before_riders(c: dict, components: list[dict]) -> Decimal | None:
    """All-in ENERGY minus the ``included_in_energy`` riders that apply to it
    (same unit; season / period match when set). The book prints the base and
    each rider verbatim, so those are what get verified; the all-in is their
    sum by construction."""
    riders = [
        r for r in components
        if r.get("included_in_energy")
        and (r.get("unit") or "$/kWh") == (c.get("unit") or "$/kWh")
        and (not r.get("season") or r.get("season") == c.get("season"))
        and (not r.get("period_label") or r.get("period_label") == c.get("period_label"))
    ]
    if not riders:
        return None
    try:
        base = Decimal(str(c["rate_value"])) - sum(Decimal(str(r["rate_value"])) for r in riders)
    except (InvalidOperation, KeyError):
        return None
    return base if base > 0 else None


def _fmt(v) -> str:
    return format(Decimal(str(v)).normalize(), "f")


def build_claims(proposal: dict) -> list[Claim]:
    """Atomic, checkable claims for every proposed component. An all-in
    ENERGY value is verified through its parts (base + each rider) when the
    proposal carries its riders; otherwise it is one ``derived_rate`` claim."""
    claims: list[Claim] = []
    components = proposal.get("components") or []
    for i, c in enumerate(components):
        label = c.get("period_label") or c.get("tier_label") or c.get("season") or c["component_type"]
        unit = c.get("unit") or ""
        base = _base_before_riders(c, components) if _is_all_in(c) else None
        if base is not None:
            base_label = c.get("period_label") or c.get("season") or "Energy"
            claims.append(Claim("rate", f"{base_label}: base energy charge {_fmt(base)} {unit} before riders".strip(), i))
        else:
            claims.append(Claim(
                "derived_rate" if _is_all_in(c) else "rate",
                f"{label}: {c['component_type']} charge {c['rate_value']} {unit}".strip(),
                i,
            ))
        if c.get("period_start_time") and c.get("period_end_time"):
            claims.append(Claim(
                "clock",
                f"{label} applies {c['period_start_time']}–{c['period_end_time']} on {c.get('day_type') or 'all'} days",
                i,
            ))
        if c.get("season_start_month") and c.get("season_end_month"):
            claims.append(Claim(
                "season",
                f"{label} season runs {c['season_start_month']}/{c.get('season_start_day')} "
                f"to {c['season_end_month']}/{c.get('season_end_day')}",
                i,
            ))
    if proposal.get("effective_date"):
        claims.append(Claim("effective_date", f"Rates effective {proposal['effective_date']}"))
    return claims


def claims_failure(verdicts: list[ClaimVerdict]) -> str | None:
    for v in verdicts:
        if v.verdict == "contradicted":
            return "claims_contradicted"
    for v in verdicts:
        floor = DERIVED_CLAIM_MIN_CONFIDENCE if v.claim.kind == "derived_rate" else CLAIM_MIN_CONFIDENCE
        if v.verdict != "verified" or v.confidence < floor:
            return "claims_unsupported"
    return None


def _as_date(v) -> date | None:
    if v is None or isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v))
    except ValueError:
        return None


def rules_failure(current, proposal: dict, pin_cause: str | None) -> str | None:
    from app.services.computable import evaluate_computable
    from app.services.tariff_history import component_signature

    if proposal.get("customer_class") and proposal["customer_class"] != current.customer_class.value:
        return "customer_class_changed"
    if not evaluate_computable(proposal.get("rate_type"), proposal.get("components") or []).computable:
        return "not_computable"
    new_eff, old_eff = _as_date(proposal.get("effective_date")), current.effective_date
    if old_eff is not None:
        if new_eff is None or new_eff < old_eff:
            return "effective_date_not_newer"
        if new_eff == old_eff and pin_cause != "source_error":
            return "effective_date_not_newer"
    if component_signature(proposal.get("components") or []) == component_signature(current.rate_components):
        return "no_rate_change"
    return None


def _decisions_today(session) -> int:
    from app.models import TariffVerification

    start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return session.execute(
        select(func.count(TariffVerification.id)).where(
            TariffVerification.decided_at >= start,
            (TariffVerification.hold_reason.is_(None)) | (~TariffVerification.hold_reason.in_(_FREE_HOLDS)),
        )
    ).scalar() or 0


def _current_snapshot(t) -> dict:
    from app.services.tariff_history import serialize_components

    return {
        "tariff_id": t.id,
        "name": t.name,
        "rate_type": t.rate_type.value,
        "effective_date": t.effective_date.isoformat() if t.effective_date else None,
        "components": serialize_components(t.rate_components),
    }


def _hold(session, v, pin, current, reason: str, gates: dict) -> str:
    from app.services.tariff_history import record_event

    now = datetime.now(timezone.utc)
    v.status, v.hold_reason, v.gate_results, v.decided_at = "held", reason, gates, now
    pin.state, pin.hold_reason = "held", reason
    pin.consecutive_holds = (pin.consecutive_holds or 0) + 1
    pin.last_checked_at = now
    record_event(
        session,
        decision="hold",
        reason=f"verify:{reason}"[:50],
        actor_type="agent_verify",
        actor_id=f"verification:{v.id}",
        utility_id=current.utility_id,
        before_tariff_id=current.id,
        source_url=v.new_source_url,
        payload={"verification_id": v.id, "gates": gates},
    )
    return "held"


def _accept(session, v, pin, current, proposal: dict, gates: dict) -> str:
    from app.models import ComponentType, CustomerClass, RateComponent, RateType, Tariff
    from app.services.pins import create_pin, release_pins
    from app.services.tariff_history import supersede_tariff
    from app.services.tou_seasonal_completeness import _coerce_time

    now = datetime.now(timezone.utc)
    new = Tariff(
        utility_id=current.utility_id,
        name=proposal.get("name") or current.name,
        code=proposal.get("code") or current.code,
        customer_class=CustomerClass(proposal.get("customer_class") or current.customer_class.value),
        rate_type=RateType(proposal["rate_type"]),
        is_default=current.is_default,
        description=current.description,
        effective_date=_as_date(proposal.get("effective_date")) or current.effective_date,
        source_url=v.new_source_url,
        source_document_hash=v.new_source_hash,
        last_verified_at=now,
        approved=True,
        confidence_factors={
            "origin": "agent_verified",
            "verification_id": v.id,
            "predecessor_provenance": current.confidence_factors,
        },
    )
    for c in proposal["components"]:
        new.rate_components.append(RateComponent(
            component_type=ComponentType(c["component_type"]),
            unit=c.get("unit") or "$/kWh",
            rate_value=c["rate_value"],
            tier_min_kwh=c.get("tier_min_kwh"),
            tier_max_kwh=c.get("tier_max_kwh"),
            tier_label=c.get("tier_label"),
            period_label=c.get("period_label"),
            period_start_time=_coerce_time(c.get("period_start_time")),
            period_end_time=_coerce_time(c.get("period_end_time")),
            day_type=c.get("day_type"),
            season=c.get("season"),
            season_start_month=c.get("season_start_month"),
            season_start_day=c.get("season_start_day"),
            season_end_month=c.get("season_end_month"),
            season_end_day=c.get("season_end_day"),
            included_in_energy=bool(c.get("included_in_energy")),
        ))
    session.add(new)
    session.flush()
    supersede_tariff(
        session, current,
        successor=new,
        reason="agent_verify_accept",
        actor_type="agent_verify",
        actor_id=f"verification:{v.id}",
        source_url=v.new_source_url,
        source_document_hash=v.new_source_hash,
        payload={"verification_id": v.id, "gates": gates},
    )
    release_pins(session, current.id)
    create_pin(
        session, new,
        source_url=v.new_source_url,
        origin="agent_verified",
        cause=pin.cause,
        ticket_id=pin.ticket_id,
        pinned_by=f"verification:{v.id}",
        source_hash=v.new_source_hash,
    )
    v.status, v.accepted_tariff_id, v.gate_results, v.decided_at = "accepted", new.id, gates, now
    return "accepted"


def run_verification(session, verification, gates: Gates | None = None) -> str:
    """Decide one proposed verification. Returns accepted | held | rejected."""
    from app.models import Tariff, TariffPin
    from app.services.tariff_history import is_live

    gates = gates or default_gates()
    v = verification
    if v.status != "proposed":
        return v.status
    pin = session.get(TariffPin, v.pin_id)
    current = session.get(Tariff, v.tariff_id)
    if current is None or not is_live(current) or pin is None or pin.state == "released":
        v.status, v.hold_reason, v.decided_at = "rejected", "tariff_not_live", datetime.now(timezone.utc)
        return "rejected"

    results: dict = {"trigger": v.trigger}
    if not (gates.verifier.available and gates.arbiter.available):
        return _hold(session, v, pin, current, "verifier_unavailable", results)
    if _decisions_today(session) >= gates.daily_max:
        return _hold(session, v, pin, current, "budget_exhausted", results)

    url = v.new_source_url or pin.pinned_source_url
    try:
        text, doc_hash = gates.fetcher.fetch(url)
    except Exception as e:
        results["fetch_error"] = str(e)[:300]
        return _hold(session, v, pin, current, "fetch_failed", results)
    v.new_source_hash = v.new_source_hash or doc_hash
    try:
        safe = gates.verifier.screen(text)
    except Exception as e:
        results["verifier_error"] = f"screen: {e}"[:300]
        return _hold(session, v, pin, current, "verifier_error", results)
    if getattr(gates.verifier, "last_screen", None) is not None:
        results["screen"] = gates.verifier.last_screen
    if not safe:
        return _hold(session, v, pin, current, "injection_suspected", results)

    proposal = v.proposed or gates.extractor.extract(url, text, current)
    if not proposal:
        return _hold(session, v, pin, current, "no_proposal", results)
    results["proposal"] = proposal

    rule = rules_failure(current, proposal, pin.cause)
    if rule:
        return _hold(session, v, pin, current, rule, results)

    try:
        verdicts = gates.verifier.verify(text, build_claims(proposal))
    except Exception as e:
        results["verifier_error"] = f"verify: {e}"[:300]
        return _hold(session, v, pin, current, "verifier_error", results)
    results["claims"] = [
        {**asdict(vd.claim), "verdict": vd.verdict, "confidence": vd.confidence} for vd in verdicts
    ]
    failure = claims_failure(verdicts)
    if failure:
        return _hold(session, v, pin, current, failure, results)

    try:
        arb = gates.arbiter.decide(
            current=_current_snapshot(current), proposal=proposal,
            document_text=text, previous_text=None,
        )
    except Exception as e:
        results["verifier_error"] = f"arbiter: {e}"[:300]
        return _hold(session, v, pin, current, "verifier_error", results)
    results["arbiter"] = asdict(arb)
    if not arb.accept:
        return _hold(session, v, pin, current, "arbiter_rejected", results)

    return _accept(session, v, pin, current, proposal, results)
