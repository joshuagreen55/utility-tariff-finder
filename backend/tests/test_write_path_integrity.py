"""Regression tests for audit findings F1–F3 (write-path integrity).

Each test re-creates one of the audit's reproduction scenarios on a
throwaway Postgres (see tests/pg_harness.py) and asserts the soft-supersede
invariant: no approved / repair / manual keeper loses its rates, and every
retired row keeps its components.

  A1 / A2  OEB path after the Hydro One repair (same / different stale name)
  B        store_tariffs in-place overwrite + reconciliation hard delete
  C        post-chord duplicate cleanup retiring an approved keeper

    TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres \\
        python -m unittest tests.test_write_path_integrity -v
"""
from __future__ import annotations

import contextlib
import io
from datetime import date

from sqlalchemy import text

from scripts import tariff_pipeline as tp
from tests.pg_harness import PostgresTestCase, energy_values, fixed_values, run_alembic

DOMAIN = "https://utility.example.com"


def _et(name, components, *, rate_type="flat", customer_class="residential",
        source_url=f"{DOMAIN}/rates", effective_date=None):
    return tp.ExtractedTariff(
        name=name,
        code=None,
        customer_class=customer_class,
        rate_type=rate_type,
        description=None,
        source_url=source_url,
        effective_date=effective_date,
        components=components,
    )


def _energy(v):
    return {"component_type": "energy", "unit": "$/kWh", "rate_value": v}


class TestStoreTariffsScenarioB(PostgresTestCase):
    """F1: store_tariffs overwrote rates in place and hard-deleted siblings."""

    def test_b_protected_keeper_is_held_and_sibling_survives(self):
        uid = self.make_utility("Repro B Electric")
        keeper = self.make_tariff(
            uid, "Residential Service", [_energy(0.20)],
            approved=True, confidence_factors={"repair": "manual_fix", "ticket": "CS-1"},
        )
        sibling = self.make_tariff(uid, "Residential Time-of-Day", [_energy(0.25)])

        tp.store_tariffs(uid, [_et("Residential Service", [_energy(0.02)])], dry_run=False)

        k = self.get_tariff(keeper)
        self.assertIsNone(k.supersede_reason)
        self.assertEqual(energy_values(k), [0.20])
        self.assertEqual(k.confidence_factors, {"repair": "manual_fix", "ticket": "CS-1"})
        self.assertTrue(k.approved)
        s = self.get_tariff(sibling)
        self.assertIsNotNone(s, "sibling must never be hard-deleted")
        self.assertIsNone(s.supersede_reason)
        self.assertEqual(len(self.tariffs_for(uid)), 2)

        holds = [e for e in self.events_for(uid) if e.decision == "hold"]
        self.assertEqual(len(holds), 1)
        self.assertEqual(holds[0].before_tariff_id, keeper)
        proposed = holds[0].payload["proposed"]["components"]
        self.assertEqual([c["rate_value"] for c in proposed], [0.02])

    def test_b_unprotected_row_is_revised_with_prior_values_retained(self):
        uid = self.make_utility("Repro B2 Electric")
        old = self.make_tariff(uid, "Residential Service", [_energy(0.20)])
        sibling = self.make_tariff(uid, "Residential Time-of-Day", [_energy(0.25)])

        tp.store_tariffs(uid, [_et("Residential Service", [_energy(0.02)])], dry_run=False)

        prior = self.get_tariff(old)
        self.assertEqual(prior.supersede_reason, "refresh")
        self.assertIsNotNone(prior.superseded_at)
        self.assertEqual(energy_values(prior), [0.20], "prior rates stay on the superseded row")
        new = self.get_tariff(prior.superseded_by_tariff_id)
        self.assertEqual(new.name, "Residential Service")
        self.assertIsNone(new.supersede_reason)
        self.assertEqual(energy_values(new), [0.02])
        # 1 of 2 live residential rows re-extracted: partial, nothing retired.
        self.assertIsNone(self.get_tariff(sibling).supersede_reason)

        sup = [e for e in self.events_for(uid) if e.decision == "supersede"]
        self.assertEqual(len(sup), 1)
        self.assertEqual((sup[0].before_tariff_id, sup[0].after_tariff_id), (old, new.id))
        self.assertEqual(sup[0].actor_type, "pipeline")

    def test_identical_reextraction_only_reverifies(self):
        uid = self.make_utility("Repro Touch Electric")
        tid = self.make_tariff(
            uid, "Residential Service", [_energy(0.1234), {"component_type": "fixed", "unit": "$/month", "rate_value": 12.5}],
            confidence_factors={"ticket": "CS-9", "llm_confidence": 0.01},
        )
        before = self.get_tariff(tid).last_verified_at

        tp.store_tariffs(uid, [_et("Residential Service", [
            {"component_type": "fixed", "unit": "$/month", "rate_value": 12.5},
            _energy(0.1234),
        ])], dry_run=False)

        rows = self.tariffs_for(uid)
        self.assertEqual([r.id for r in rows], [tid])
        t = rows[0]
        self.assertGreater(t.last_verified_at, before)
        self.assertEqual(t.confidence_factors["ticket"], "CS-9", "provenance survives re-verify")
        self.assertEqual(self.events_for(uid), [])

    def test_same_name_live_and_superseded_rows_do_not_crash(self):
        uid = self.make_utility("Repro Dup-Name Electric")
        live = self.make_tariff(uid, "Residential Service", [_energy(0.15)])
        self.make_tariff(
            uid, "Residential Service", [_energy(0.11)],
            superseded_by=live, supersede_reason="vintage",
        )

        stored = tp.store_tariffs(uid, [_et("Residential Service", [_energy(0.15)])], dry_run=False)

        self.assertEqual(stored, 1)
        self.assertEqual([t.id for t in self.tariffs_for(uid, live_only=True)], [live])

    def test_reconcile_soft_retires_and_holds_protected(self):
        uid = self.make_utility("Repro Reconcile Electric")
        names = [f"Residential Plan {c}" for c in "ABCDEF"]
        for i, n in enumerate(names):
            self.make_tariff(uid, n, [_energy(0.10 + i / 100)])
        stale = self.make_tariff(uid, "Residential Legacy Plan", [_energy(0.30)])
        protected = self.make_tariff(
            uid, "Residential Repaired Plan", [_energy(0.40)],
            approved=True, confidence_factors={"repair": "x"},
        )

        tp.store_tariffs(
            uid, [_et(n, [_energy(0.10 + i / 100)]) for i, n in enumerate(names)], dry_run=False
        )

        s = self.get_tariff(stale)
        self.assertEqual(s.supersede_reason, "reconcile_missing")
        self.assertIsNone(s.superseded_by_tariff_id)
        self.assertEqual(energy_values(s), [0.30], "retired, not deleted")
        self.assertIsNone(self.get_tariff(protected).supersede_reason)
        events = self.events_for(uid)
        self.assertIn(("retire", stale), [(e.decision, e.before_tariff_id) for e in events])
        self.assertIn(("hold", protected), [(e.decision, e.before_tariff_id) for e in events])

    def test_residential_extraction_never_retires_commercial_rows(self):
        uid = self.make_utility("Repro Class-Scope Electric")
        names = [f"Residential Plan {c}" for c in "ABCD"]
        for n in names:
            self.make_tariff(uid, n, [_energy(0.12)])
        commercial = self.make_tariff(
            uid, "General Service", [_energy(0.14)], customer_class="commercial"
        )

        tp.store_tariffs(uid, [_et(n, [_energy(0.12)]) for n in names], dry_run=False)

        self.assertIsNone(self.get_tariff(commercial).supersede_reason)

    def test_vintage_never_retires_protected_row_onto_scraped_sibling(self):
        uid = self.make_utility("Repro Vintage Electric")
        repaired = self.make_tariff(
            uid, "Rate #1.1 Domestic Service", [_energy(0.16)],
            approved=True, confidence_factors={"repair": "x"},
            effective_date=date(2026, 5, 1),
        )

        tp.store_tariffs(uid, [_et(
            "Domestic Service (Flat)", [_energy(0.017)], effective_date="2026-05-01",
        )], dry_run=False)

        r = self.get_tariff(repaired)
        self.assertIsNone(r.supersede_reason, "protected wins a same-vintage tie")
        scraped = [t for t in self.tariffs_for(uid) if t.id != repaired][0]
        self.assertEqual(scraped.supersede_reason, "vintage")
        self.assertEqual(scraped.superseded_by_tariff_id, repaired)


class TestOebPathAfterHydroOneRepair(PostgresTestCase):
    """F2: store_oeb_tariffs crashed (A1) or wiped FIXED delivery (A2)."""

    TOU_NAME = "Time-of-Use (TOU) — Residential"

    def _repaired_hydro_one(self, stale_name: str, state: str) -> tuple[int, int, int]:
        from app.models import Utility
        from scripts import repair_hydro_one_oeb_residential as repair

        uid = self.make_utility("Hydro One", state=state, country="CA",
                                website_url="https://www.hydroone.com")
        stale = self.make_tariff(uid, stale_name, [
            {"component_type": "energy", "rate_value": 0.203, "period_label": "On-Peak"},
            {"component_type": "energy", "rate_value": 0.157, "period_label": "Mid-Peak"},
            {"component_type": "energy", "rate_value": 0.098, "period_label": "Off-Peak"},
            {"component_type": "fixed", "unit": "$/month", "rate_value": 33.41,
             "tier_label": "Delivery monthly charge"},
        ], rate_type="tou", source_url="https://www.hydroone.com/rates")
        with self.session() as s, contextlib.redirect_stdout(io.StringIO()):
            repair.repair_utility(
                s, s.get(Utility, uid), dry_run=False,
                plan_filter={"tou"}, force_plans={"tou"},
            )
            s.commit()
        keeper = self.get_tariff(stale).superseded_by_tariff_id
        self.assertIsNotNone(keeper)
        return uid, stale, keeper

    def _oeb_entries(self, rates=None):
        from scripts import repair_hydro_one_oeb_residential as repair
        from scripts.scrape_oeb_rates import build_tariff_entries

        return build_tariff_entries(rates or repair.gold_oeb_rate_set(), "residential")

    def _assert_keeper_intact(self, keeper_id: int):
        k = self.get_tariff(keeper_id)
        self.assertIsNone(k.supersede_reason)
        self.assertEqual(fixed_values(k), [33.41])
        self.assertEqual(len([rc for rc in k.rate_components if rc.component_type.value == "energy"]), 14)
        self.assertEqual(k.confidence_factors["repair"], "repair_hydro_one_oeb_residential")

    def test_a1_same_name_keeper_does_not_crash_or_lose_fixed(self):
        from scripts.scrape_oeb_rates import store_oeb_tariffs

        uid, stale, keeper = self._repaired_hydro_one(self.TOU_NAME, "ON")
        before = self.get_tariff(keeper).last_verified_at

        store_oeb_tariffs(uid, self._oeb_entries(), dry_run=False)

        self._assert_keeper_intact(keeper)
        self.assertGreater(self.get_tariff(keeper).last_verified_at, before)
        prior = self.get_tariff(stale)
        self.assertEqual(prior.supersede_reason, "vintage")
        self.assertEqual(fixed_values(prior), [33.41])
        live_tou = [t for t in self.tariffs_for(uid, live_only=True) if t.name == self.TOU_NAME]
        self.assertEqual([t.id for t in live_tou], [keeper])

    def test_a2_differently_named_stale_row_keeps_fixed(self):
        from scripts.scrape_oeb_rates import store_oeb_tariffs

        uid, _stale, keeper = self._repaired_hydro_one("Residential TOU (legacy)", "ON")

        store_oeb_tariffs(uid, self._oeb_entries(), dry_run=False)

        self._assert_keeper_intact(keeper)

    def test_oeb_rate_change_revises_and_carries_fixed_forward(self):
        from scripts.scrape_oeb_rates import OEBRateSet, TOURates, store_oeb_tariffs

        uid, _stale, keeper = self._repaired_hydro_one(self.TOU_NAME, "ON")
        nov_2026 = OEBRateSet(tou=TOURates(
            effective_date="2026-11-01", off_peak=0.101, mid_peak=0.161, on_peak=0.211,
        ))

        store_oeb_tariffs(uid, self._oeb_entries(nov_2026), dry_run=False)

        old = self.get_tariff(keeper)
        self.assertEqual(old.supersede_reason, "oeb_refresh")
        self.assertIn(0.203, energy_values(old), "prior commodity rates retained")
        self.assertEqual(fixed_values(old), [33.41])
        new = self.get_tariff(old.superseded_by_tariff_id)
        self.assertEqual(new.effective_date, date(2026, 11, 1))
        self.assertEqual(set(energy_values(new)), {0.101, 0.161, 0.211})
        self.assertEqual(fixed_values(new), [33.41], "delivery charge carried forward")
        self.assertEqual(new.confidence_factors["carried_forward_components"], ["fixed"])
        self.assertEqual(
            new.confidence_factors["predecessor_provenance"]["repair"],
            "repair_hydro_one_oeb_residential",
        )
        ev = [e for e in self.events_for(uid) if e.before_tariff_id == keeper]
        self.assertEqual([(e.decision, e.actor_type) for e in ev], [("supersede", "oeb")])

    def test_manual_correction_is_held_against_oeb_feed(self):
        from scripts.scrape_oeb_rates import store_oeb_tariffs

        uid = self.make_utility("Toronto Hydro", state="ON", country="CA")
        manual = self.make_tariff(uid, self.TOU_NAME, [_energy(0.5)], rate_type="seasonal_tou",
                                  approved=True, confidence_factors={"origin": "manual", "ticket": "CS-2"})

        store_oeb_tariffs(uid, self._oeb_entries(), dry_run=False)

        self.assertIsNone(self.get_tariff(manual).supersede_reason)
        self.assertEqual(energy_values(self.get_tariff(manual)), [0.5])
        holds = [e for e in self.events_for(uid) if e.decision == "hold"]
        self.assertEqual([h.before_tariff_id for h in holds], [manual])


class TestDupCleanupScenarioC(PostgresTestCase):
    """F3: the post-chord dup cleanup retired an approved manual keeper."""

    def _run(self, state):
        from scripts.cleanup_duplicate_tariffs import run_cleanup

        with contextlib.redirect_stdout(io.StringIO()):
            run_cleanup(country=None, province=None, states=[state], dry_run=False)

    def test_c_approved_keeper_survives_richer_scraped_sibling(self):
        uid = self.make_utility("Repro C Electric", state="NH")
        manual = self.make_tariff(uid, "Residential Service", [_energy(0.18)], approved=True)
        scraped = self.make_tariff(uid, "Residential Service Rate R-1", [
            _energy(0.018),
            {"component_type": "fixed", "unit": "$/month", "rate_value": 14.0},
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.001},
        ])

        self._run("NH")

        m = self.get_tariff(manual)
        self.assertIsNone(m.supersede_reason)
        self.assertEqual(energy_values(m), [0.18])
        s = self.get_tariff(scraped)
        self.assertEqual((s.supersede_reason, s.superseded_by_tariff_id), ("dup_cleanup", manual))
        self.assertIsNotNone(s.superseded_at)
        self.assertEqual(len(s.rate_components), 3)
        ev = [e for e in self.events_for(uid) if e.before_tariff_id == scraped]
        self.assertEqual([(e.decision, e.actor_type) for e in ev], [("supersede", "cleanup")])

    def test_two_protected_duplicates_are_left_alone(self):
        uid = self.make_utility("Repro C2 Electric", state="VT")
        a = self.make_tariff(uid, "Residential Service", [_energy(0.18)], approved=True)
        b = self.make_tariff(uid, "Residential Service Rate R-1", [_energy(0.19)],
                             confidence_factors={"repair": "x"})

        self._run("VT")

        self.assertIsNone(self.get_tariff(a).supersede_reason)
        self.assertIsNone(self.get_tariff(b).supersede_reason)

    def test_unprotected_pair_keeps_richer_row(self):
        uid = self.make_utility("Repro C3 Electric", state="ME")
        thin = self.make_tariff(uid, "Residential Service", [_energy(0.18)])
        rich = self.make_tariff(uid, "Residential Service Rate R-1", [
            _energy(0.18), {"component_type": "fixed", "unit": "$/month", "rate_value": 14.0},
        ])

        self._run("ME")

        self.assertEqual(self.get_tariff(thin).superseded_by_tariff_id, rich)
        self.assertIsNone(self.get_tariff(rich).supersede_reason)

    def test_protected_rider_only_row_is_not_retired(self):
        uid = self.make_utility("Repro C4 Electric", state="RI")
        rider = self.make_tariff(uid, "Storm Recovery Rider", [
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.002},
        ], confidence_factors={"origin": "manual"})

        self._run("RI")

        self.assertIsNone(self.get_tariff(rider).supersede_reason)


class TestHistoryTriggers(PostgresTestCase):
    def test_superseded_at_stamped_for_raw_sql_writers(self):
        uid = self.make_utility("Trigger Electric")
        tid = self.make_tariff(uid, "Residential Service", [_energy(0.1)])
        with self.engine.begin() as c:
            c.execute(text("UPDATE tariffs SET supersede_reason = 'out_of_scope' WHERE id = :i"), {"i": tid})
        self.assertIsNotNone(self.get_tariff(tid).superseded_at)
        with self.engine.begin() as c:
            c.execute(text("UPDATE tariffs SET supersede_reason = NULL WHERE id = :i"), {"i": tid})
        self.assertIsNone(self.get_tariff(tid).superseded_at)

    def test_change_events_are_append_only(self):
        from app.services.tariff_history import record_event

        uid = self.make_utility("Append Only Electric")
        with self.session() as s:
            ev = record_event(s, decision="insert", actor_type="script", utility_id=uid)
            s.commit()
            ev_id = ev.id
        for stmt in (
            "UPDATE tariff_change_events SET notes = 'x' WHERE id = :i",
            "DELETE FROM tariff_change_events WHERE id = :i",
            "TRUNCATE tariff_change_events",
        ):
            with self.assertRaises(Exception, msg=stmt):
                with self.engine.begin() as c:
                    c.execute(text(stmt), {"i": ev_id})

    def test_hard_delete_leaves_snapshot_event(self):
        uid = self.make_utility("Hard Delete Electric")
        tid = self.make_tariff(uid, "Residential Service", [
            _energy(0.1), {"component_type": "fixed", "unit": "$/month", "rate_value": 9.0},
        ])
        with self.engine.begin() as c:
            c.execute(text("DELETE FROM tariffs WHERE id = :i"), {"i": tid})
        ev = [e for e in self.events_for(uid) if e.decision == "hard_delete"]
        self.assertEqual(len(ev), 1)
        self.assertEqual(ev[0].before_tariff_id, tid)
        self.assertEqual(ev[0].payload["tariff"]["name"], "Residential Service")
        self.assertEqual(len(ev[0].payload["components"]), 2)


class TestMigrationRoundTrip(PostgresTestCase):
    # Not "downgrade base": the initial migration's downgrade leaves its
    # enum types behind, and applied migrations are never edited.
    def test_downgrade_history_revision_and_upgrade_head(self):
        run_alembic(self.db_url, "downgrade", "c0d1e2f3a4b5")
        with self.engine.connect() as c:
            self.assertIsNone(c.execute(text("SELECT to_regclass('tariff_change_events')")).scalar())
        run_alembic(self.db_url, "upgrade", "head")
        with self.engine.connect() as c:
            head = c.execute(text("SELECT version_num FROM alembic_version")).scalar()
        self.assertEqual(head, "d1e2f3a4b5c6")
