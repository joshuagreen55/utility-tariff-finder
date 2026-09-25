"""Manual correction API, document pins, auto-verify hooks (F4, F10).

DB-backed (see tests/pg_harness.py) except TestClaimHelpers.

    TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres \\
        python -m unittest tests.test_corrections_and_pins -v
"""
from __future__ import annotations

import unittest
import uuid
from dataclasses import dataclass
from datetime import date

from app.config import settings
from app.services import pin_verification as pv
from scripts import tariff_pipeline as tp
from tests.pg_harness import PostgresTestCase, energy_values

CORRECTIONS_KEY = "test-corrections-key-not-a-secret"
ADMIN_KEY = "test-admin-key-not-a-secret"
BOOK_URL = "https://utility.example.com/rate-book-2026.pdf"


def _energy(v, **kw):
    return {"component_type": "energy", "unit": "$/kWh", "rate_value": v, **kw}


def _body(uid, *, mode="replace", expected=None, energy="0.200000", name="Residential Service",
          key=None, eff="2026-05-01"):
    body = {
        "idempotency_key": key or f"mysa-cs-{uuid.uuid4()}",
        "ticket_id": "CS-12345",
        "approved_by": "approver@getmysa.com",
        "approved_at": "2026-10-02T14:00:00Z",
        "requested_by": "agent@getmysa.com",
        "target": {"utility_id": uid, "mode": mode, "expected_live_tariff_id": expected},
        "evidence": {"source_url": BOOK_URL, "page_ref": "p. 12", "quote": "Energy charge 20.0¢/kWh"},
        "pin": {"cause": "extraction_error"},
    }
    if mode != "retire":
        body["tariff"] = {
            "name": name, "customer_class": "residential", "rate_type": "flat",
            "effective_date": eff,
        }
        body["components"] = [
            {"component_type": "energy", "unit": "$/kWh", "rate_value": energy},
            {"component_type": "fixed", "unit": "$/month", "rate_value": "12.50"},
        ]
    return body


class _SettingsPatch:
    def patch_settings(self, **values):
        for k, v in values.items():
            old = getattr(settings, k)
            setattr(settings, k, v)
            self.addCleanup(setattr, settings, k, old)


class TestCorrectionApi(PostgresTestCase, _SettingsPatch):
    def setUp(self):
        self.patch_settings(tariff_corrections_api_key=CORRECTIONS_KEY, admin_api_key=ADMIN_KEY)
        self.client = self.api_client()
        self.uid = self.make_utility(f"Correction Electric {uuid.uuid4().hex[:6]}")
        self.bad = self.make_tariff(self.uid, "Residential Service", [_energy(0.02)],
                                    source_url=BOOK_URL, effective_date=date(2026, 5, 1))

    def post(self, body, key=CORRECTIONS_KEY, **headers):
        if key:
            headers["X-Corrections-Key"] = key
        return self.client.post("/api/tariff-corrections", json=body, headers=headers)

    def test_disabled_without_configured_key(self):
        self.patch_settings(tariff_corrections_api_key="")
        self.assertEqual(self.post(_body(self.uid, expected=self.bad)).status_code, 503)

    def test_admin_key_and_missing_key_are_rejected(self):
        body = _body(self.uid, expected=self.bad)
        self.assertEqual(self.post(body, key=None).status_code, 401)
        self.assertEqual(self.post(body, key=None, **{"X-Admin-Key": ADMIN_KEY}).status_code, 401)
        self.assertEqual(self.post(body, key="wrong").status_code, 401)
        self.assertIsNone(self.get_tariff(self.bad).supersede_reason)

    def test_auth_gate_lets_only_the_scoped_key_through(self):
        self.patch_settings(auth_enabled=True)
        self.assertEqual(self.client.get(f"/api/tariffs/{self.bad}").status_code, 401)
        self.assertEqual(self.post(_body(self.uid, expected=self.bad), key=None).status_code, 401)
        self.assertEqual(self.post(_body(self.uid, expected=self.bad)).status_code, 201)

    def test_replace_soft_supersedes_pins_and_logs(self):
        r = self.post(_body(self.uid, expected=self.bad))
        self.assertEqual(r.status_code, 201, r.text)
        out = r.json()
        self.assertEqual(out["superseded_tariff_id"], self.bad)
        self.assertTrue(out["computable"])
        self.assertFalse(out["replayed"])

        old = self.get_tariff(self.bad)
        self.assertEqual(old.supersede_reason, "manual")
        self.assertEqual(old.superseded_by_tariff_id, out["new_tariff_id"])
        self.assertIsNotNone(old.superseded_at)
        self.assertEqual(energy_values(old), [0.02], "prior values retained")
        new = self.get_tariff(out["new_tariff_id"])
        self.assertTrue(new.approved)
        self.assertEqual(new.confidence_factors["origin"], "manual")
        self.assertEqual(new.confidence_factors["manual"]["ticket_id"], "CS-12345")
        self.assertEqual(energy_values(new), [0.2])

        from app.models import MonitoringSource, TariffPin
        with self.session() as s:
            pin = s.get(TariffPin, out["pin_id"])
            self.assertEqual((pin.tariff_id, pin.state, pin.origin, pin.cause),
                             (new.id, "active", "manual", "extraction_error"))
            self.assertEqual(pin.pinned_source_url, BOOK_URL)
            self.assertEqual(
                s.query(MonitoringSource).filter_by(utility_id=self.uid, url=BOOK_URL).count(), 1
            )
        (ev,) = [e for e in self.events_for(self.uid) if e.id == out["change_event_id"]]
        self.assertEqual((ev.decision, ev.reason, ev.actor_type, ev.actor_id, ev.ticket_id),
                         ("supersede", "manual", "manual_api", "approver@getmysa.com", "CS-12345"))

    def test_scraper_holds_against_pin_and_proposes_on_new_document(self):
        from app.models import TariffVerification

        new_id = self.post(_body(self.uid, expected=self.bad)).json()["new_tariff_id"]

        tp.store_tariffs(self.uid, [tp.ExtractedTariff(
            name="Residential Service", customer_class="residential", rate_type="flat",
            source_url=BOOK_URL, effective_date="2026-05-01", components=[_energy(0.02)],
        )], dry_run=False)
        self.assertIsNone(self.get_tariff(new_id).supersede_reason, "bad scrape of same doc must not win")
        self.assertEqual(energy_values(self.get_tariff(new_id)), [0.2])
        with self.session() as s:
            self.assertEqual(s.query(TariffVerification).filter_by(tariff_id=new_id).count(), 0)

        newer = "https://utility.example.com/rate-book-2027.pdf"
        tp.store_tariffs(self.uid, [tp.ExtractedTariff(
            name="Residential Service", customer_class="residential", rate_type="flat",
            source_url=newer, effective_date="2027-05-01", components=[_energy(0.21)],
        )], dry_run=False)
        self.assertIsNone(self.get_tariff(new_id).supersede_reason)
        with self.session() as s:
            (v,) = s.query(TariffVerification).filter_by(tariff_id=new_id).all()
            self.assertEqual((v.status, v.trigger, v.new_source_url), ("proposed", "new_document", newer))
            self.assertEqual(v.proposed["components"][0]["rate_value"], 0.21)

    def test_idempotent_replay_and_key_reuse(self):
        body = _body(self.uid, expected=self.bad)
        first = self.post(body)
        again = self.post(body)
        self.assertEqual((first.status_code, again.status_code), (201, 200))
        self.assertTrue(again.json()["replayed"])
        for k in ("new_tariff_id", "superseded_tariff_id", "change_event_id", "pin_id"):
            self.assertEqual(first.json()[k], again.json()[k])
        changed = dict(body, ticket_id="CS-99999")
        r = self.post(changed)
        self.assertEqual((r.status_code, r.json()["reason"]), (409, "idempotency_key_reused"))

    def test_conflict_when_target_is_no_longer_live(self):
        new_id = self.post(_body(self.uid, expected=self.bad)).json()["new_tariff_id"]
        r = self.post(_body(self.uid, expected=self.bad, energy="0.25"))
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.json()["reason"], "not_live")
        self.assertEqual(r.json()["current_live_tariff_id"], new_id)

    def test_create_then_name_clash(self):
        r = self.post(_body(self.uid, mode="create", name="Residential EV Rate"))
        self.assertEqual(r.status_code, 201, r.text)
        self.assertIsNone(r.json()["superseded_tariff_id"])
        clash = self.post(_body(self.uid, mode="create", name="Residential EV Rate"))
        self.assertEqual((clash.status_code, clash.json()["reason"]), (409, "live_tariff_exists"))

    def test_retire_mode(self):
        r = self.post(_body(self.uid, mode="retire", expected=self.bad))
        self.assertEqual(r.status_code, 201, r.text)
        old = self.get_tariff(self.bad)
        self.assertEqual((old.supersede_reason, old.superseded_by_tariff_id), ("manual_retire", None))
        self.assertEqual(energy_values(old), [0.02])

    def test_validation_rejects_impossible_values(self):
        for body in (
            _body(self.uid, expected=self.bad, energy="-0.1"),
            _body(self.uid, mode="replace", expected=None),
            dict(_body(self.uid, expected=self.bad), components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": "20"},
            ]),
        ):
            self.assertEqual(self.post(body).status_code, 422)
        self.assertIsNone(self.get_tariff(self.bad).supersede_reason)

    def test_delete_endpoint_soft_retires(self):
        r = self.client.delete(f"/api/tariffs/{self.bad}", headers={"X-Admin-Key": ADMIN_KEY})
        self.assertEqual(r.status_code, 200, r.text)
        t = self.get_tariff(self.bad)
        self.assertIsNotNone(t, "never hard-deleted")
        self.assertEqual(t.supersede_reason, "manual_retire")
        (ev,) = [e for e in self.events_for(self.uid) if e.id == r.json()["change_event_id"]]
        self.assertEqual((ev.decision, ev.actor_type, ev.actor_id), ("retire", "admin_api", "admin_key"))
        self.assertEqual(self.client.delete(
            f"/api/tariffs/{self.bad}", headers={"X-Admin-Key": ADMIN_KEY}
        ).status_code, 409)


@dataclass
class StaticVerifier:
    verdict: str = "verified"
    confidence: float = 0.99
    safe: bool = True
    available: bool = True

    def screen(self, text):
        return self.safe

    def verify(self, text, claims):
        return [pv.ClaimVerdict(c, self.verdict, self.confidence) for c in claims]


@dataclass
class StaticArbiter:
    accept: bool = True
    available: bool = True

    def decide(self, **_kw):
        return pv.ArbiterVerdict(self.accept, "static", "test-arbiter")


class StaticFetcher:
    def __init__(self, fail=False):
        self.fail = fail

    def fetch(self, url):
        if self.fail:
            raise RuntimeError("HTTP 403")
        return "energy charge 21.0 cents per kwh effective may 1 2027", "c" * 64


class NoExtractor:
    def extract(self, url, text, current):
        return None


class TestPinVerification(PostgresTestCase, _SettingsPatch):
    def setUp(self):
        from app.services.pins import create_pin

        self.uid = self.make_utility(f"Pinned Electric {uuid.uuid4().hex[:6]}")
        self.tid = self.make_tariff(
            self.uid, "Residential Service", [_energy(0.20)], approved=True,
            confidence_factors={"origin": "manual", "manual": {"ticket_id": "CS-1"}},
            source_url=BOOK_URL, effective_date=date(2026, 5, 1),
        )
        with self.session() as s:
            from app.models import Tariff

            self.pin_id = create_pin(
                s, s.get(Tariff, self.tid), source_url=BOOK_URL, origin="manual",
                cause="extraction_error", source_hash="a" * 64,
            ).id
            s.commit()

    def _proposal(self, v=0.21, eff="2027-05-01", **extra):
        return {"name": "Residential Service", "customer_class": "residential", "rate_type": "flat",
                "effective_date": eff, "components": [_energy(v)], **extra}

    def _open(self, proposal=None):
        from app.models import TariffPin
        from app.services.pins import propose_verification

        with self.session() as s:
            v = propose_verification(s, s.get(TariffPin, self.pin_id), trigger="source_changed",
                                     proposed=proposal)
            s.commit()
            return v.id

    def _run(self, vid, **gates):
        from app.models import TariffVerification

        g = pv.Gates(
            verifier=gates.get("verifier", StaticVerifier()),
            arbiter=gates.get("arbiter", StaticArbiter()),
            fetcher=gates.get("fetcher", StaticFetcher()),
            extractor=NoExtractor(),
            daily_max=gates.get("daily_max", 20),
        )
        with self.session() as s:
            outcome = pv.run_verification(s, s.get(TariffVerification, vid), g)
            s.commit()
            v = s.get(TariffVerification, vid)
            return outcome, v.hold_reason, v.accepted_tariff_id

    def test_monitoring_change_opens_verification_with_real_diff(self):
        from app.models import MonitoringSource, TariffVerification
        from app.services.monitoring_runner import _persist_result

        with self.session() as s:
            src = s.query(MonitoringSource).filter_by(utility_id=self.uid, url=BOOK_URL).one()
            src.last_content_hash = "a" * 64
            src.last_content_text = "energy charge 20.0 cents per kwh"
            s.commit()
            src_id = src.id
        _persist_result(self.engine, src_id, {
            "content_hash": "b" * 64, "content_preview": "x",
            "content_text": "energy charge 21.0 cents per kwh", "error": None,
        }, "a" * 64)
        with self.session() as s:
            (v,) = s.query(TariffVerification).filter_by(pin_id=self.pin_id).all()
            self.assertEqual((v.status, v.trigger, v.new_source_hash), ("proposed", "source_changed", "b" * 64))
            from app.models import MonitoringLog

            log_row = s.query(MonitoringLog).filter_by(source_id=src_id).one()
            self.assertIn("21.0", log_row.diff_summary)
            self.assertIn("20.0", log_row.diff_summary)

    def test_default_null_gates_hold_without_touching_the_row(self):
        from app.models import TariffPin, TariffVerification

        vid = self._open(self._proposal())
        with self.session() as s:
            outcome = pv.run_verification(s, s.get(TariffVerification, vid), pv.Gates(extractor=NoExtractor()))
            s.commit()
            pin = s.get(TariffPin, self.pin_id)
            self.assertEqual((outcome, pin.state, pin.hold_reason), ("held", "held", "verifier_unavailable"))
        self.assertIsNone(self.get_tariff(self.tid).supersede_reason)
        holds = [e for e in self.events_for(self.uid) if e.decision == "hold"]
        self.assertEqual(holds[-1].reason, "verify:verifier_unavailable")

    def test_accept_moves_pin_and_soft_supersedes(self):
        from app.models import TariffPin

        outcome, reason, new_id = self._run(self._open(self._proposal()))
        self.assertEqual((outcome, reason), ("accepted", None))
        old = self.get_tariff(self.tid)
        self.assertEqual((old.supersede_reason, old.superseded_by_tariff_id), ("agent_verify_accept", new_id))
        self.assertEqual(energy_values(old), [0.2])
        new = self.get_tariff(new_id)
        self.assertEqual(energy_values(new), [0.21])
        self.assertEqual(new.confidence_factors["origin"], "agent_verified")
        self.assertEqual(new.source_document_hash, "c" * 64)
        with self.session() as s:
            self.assertEqual(s.get(TariffPin, self.pin_id).state, "released")
            moved = s.query(TariffPin).filter_by(tariff_id=new_id).one()
            self.assertEqual((moved.state, moved.origin, moved.pinned_source_hash),
                             ("active", "agent_verified", "c" * 64))
        ev = [e for e in self.events_for(self.uid) if e.before_tariff_id == self.tid and e.decision == "supersede"]
        self.assertEqual(ev[0].actor_type, "agent_verify")
        self.assertIn("claims", ev[0].payload["gates"])

    def test_each_gate_holds(self):
        cases = [
            ({"fetcher": StaticFetcher(fail=True)}, self._proposal(), "fetch_failed"),
            ({"verifier": StaticVerifier(safe=False)}, self._proposal(), "injection_suspected"),
            ({}, None, "no_proposal"),
            ({}, self._proposal(eff="2025-01-01"), "effective_date_not_newer"),
            ({}, self._proposal(v=0.20, eff="2027-05-01"), "no_rate_change"),
            ({}, self._proposal(rate_type="demand"), "not_computable"),
            ({"verifier": StaticVerifier(verdict="contradicted")}, self._proposal(), "claims_contradicted"),
            ({"verifier": StaticVerifier(confidence=0.5)}, self._proposal(), "claims_unsupported"),
            ({"arbiter": StaticArbiter(accept=False)}, self._proposal(), "arbiter_rejected"),
            ({"daily_max": 0}, self._proposal(), "budget_exhausted"),
        ]
        for gates, proposal, expected in cases:
            with self.subTest(expected=expected):
                outcome, reason, _ = self._run(self._open(proposal), **gates)
                self.assertEqual((outcome, reason), ("held", expected))
                self.assertIsNone(self.get_tariff(self.tid).supersede_reason)

    def test_jev_verifier_and_opus_arbiter_end_to_end(self):
        from app.services import pin_adapters as pa
        from tests.test_pin_adapters import FakeMercury, opus_arbiter

        mercury = FakeMercury()
        arbiter = opus_arbiter(True)
        outcome, reason, new_id = self._run(self._open(self._proposal()),
                                            verifier=pa.JevVerifier(mercury), arbiter=arbiter)
        self.assertEqual((outcome, reason), ("accepted", None))
        self.assertEqual(self.get_tariff(self.tid).supersede_reason, "agent_verify_accept")
        self.assertEqual([t for t, _ in mercury.calls], ["jev_screen", "jev_verify"])
        self.assertEqual(mercury.calls[1][1]["claims"],
                         ["energy: energy charge 0.21 $/kWh", "Rates effective 2027-05-01"])
        self.assertEqual(len(arbiter._post.calls), 1)
        ev = [e for e in self.events_for(self.uid) if e.decision == "supersede" and e.after_tariff_id == new_id]
        gates = ev[0].payload["gates"]
        self.assertEqual((gates["screen"]["action"], gates["arbiter"]["model"]), ("pass", "claude-opus-test"))

    def test_jev_and_opus_adapter_holds(self):
        from app.services import pin_adapters as pa
        from tests.test_pin_adapters import FakeMercury, opus_arbiter

        cases = [
            (FakeMercury({"0.21": ("contradicted", 0.98, "auto")}), opus_arbiter(True), "claims_contradicted"),
            (FakeMercury({"0.21": ("verified", 0.85, "review")}), opus_arbiter(True), "claims_unsupported"),
            (FakeMercury(screen_action="review"), opus_arbiter(True), "injection_suspected"),
            (FakeMercury(), opus_arbiter(False), "arbiter_rejected"),
            (FakeMercury(fail=True), opus_arbiter(True), "verifier_error"),
        ]
        for mercury, arbiter, expected in cases:
            with self.subTest(expected=expected):
                outcome, reason, _ = self._run(self._open(self._proposal()),
                                               verifier=pa.JevVerifier(mercury), arbiter=arbiter)
                self.assertEqual((outcome, reason), ("held", expected))
                self.assertIsNone(self.get_tariff(self.tid).supersede_reason)
                if expected in ("claims_contradicted", "claims_unsupported", "injection_suspected"):
                    self.assertEqual(arbiter._post.calls, [])

    def test_jev_alone_never_accepts(self):
        from app.services import pin_adapters as pa
        from tests.test_pin_adapters import FakeMercury

        mercury = FakeMercury()
        outcome, reason, _ = self._run(self._open(self._proposal()),
                                       verifier=pa.JevVerifier(mercury), arbiter=pv.NullArbiter())
        self.assertEqual((outcome, reason), ("held", "verifier_unavailable"))
        self.assertEqual(mercury.calls, [])
        self.assertIsNone(self.get_tariff(self.tid).supersede_reason)

    def test_fingerprint_touch_skips_pinned_rows(self):
        other = self.make_tariff(self.uid, "Residential TOU", [_energy(0.1)])
        pinned_before = self.get_tariff(self.tid).last_verified_at
        tp._touch_tariff_verified(self.uid)
        self.assertEqual(self.get_tariff(self.tid).last_verified_at, pinned_before)
        self.assertGreater(self.get_tariff(other).last_verified_at, pinned_before)

    def test_store_persists_stable_source_document_hash(self):
        from app.services.monitor import stable_text_hash

        uid = self.make_utility(f"Hash Electric {uuid.uuid4().hex[:6]}")
        url = "https://hash.example.com/rates"
        tp.store_tariffs(uid, [tp.ExtractedTariff(
            name="Residential Service", customer_class="residential", rate_type="flat",
            source_url=url, components=[_energy(0.1)],
        )], dry_run=False, source_hashes={url: stable_text_hash("Energy  Charge 10¢")})
        (t,) = self.tariffs_for(uid)
        self.assertEqual(t.source_document_hash, stable_text_hash("energy charge 10¢"))


class TestClaimHelpers(unittest.TestCase):
    def test_claims_cover_values_clocks_seasons_and_date(self):
        claims = pv.build_claims({
            "effective_date": "2026-11-01",
            "components": [
                {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.203,
                 "period_label": "On-Peak", "period_start_time": "07:00", "period_end_time": "11:00",
                 "day_type": "weekday", "season_start_month": 11, "season_start_day": 1,
                 "season_end_month": 4, "season_end_day": 30},
                {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.19128,
                 "tier_label": "Domestic (all-in +riders)"},
            ],
        })
        self.assertEqual([c.kind for c in claims],
                         ["rate", "clock", "season", "derived_rate", "effective_date"])

    def test_derived_claims_need_higher_confidence(self):
        derived = pv.Claim("derived_rate", "x")
        plain = pv.Claim("rate", "y")
        self.assertIsNone(pv.claims_failure([pv.ClaimVerdict(plain, "verified", 0.91)]))
        self.assertEqual(pv.claims_failure([pv.ClaimVerdict(derived, "verified", 0.91)]), "claims_unsupported")
        self.assertIsNone(pv.claims_failure([pv.ClaimVerdict(derived, "verified", 0.96)]))


if __name__ == "__main__":
    unittest.main()
