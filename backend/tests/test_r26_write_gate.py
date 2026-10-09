"""R26 write gate: hold harmful tariff writes for review.

Pure tests replay the R25 captured extractions (FPL RTR-1, DTE CPP) and R20
outputs (PPL pro-forma, Xcel MN 2019 book, SDG&E flattened TOU, plus good
refreshes that must pass) against the live rows they would have replaced,
taken from the R25 / R20 snapshots (tests/fixtures/r26/gate_cases.json).

DB-backed tests (skipped without TEST_DATABASE_URL) run store_tariffs and
check that a held proposal is not written, the live row stays live and a
``hold`` event with reason ``write_gate:<rule>`` is recorded.
"""
from __future__ import annotations

import json
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace as NS

from app.services import start_page as sp
from app.services import write_gate as wg

FIX = Path(__file__).parent / "fixtures" / "r26" / "gate_cases.json"


def _old(o):
    if o is None:
        return None
    return NS(id=o["id"], name=o["name"], code=o["code"], rate_type=o["rate_type"],
              effective_date=date.fromisoformat(o["effective_date"]) if o["effective_date"] else None,
              confidence_factors=o["confidence_factors"],
              rate_components=[NS(**{"period_start_time": None, "period_end_time": None,
                                     "included_in_energy": False, "adjustment": False,
                                     "period_label": None, "tier_label": None, "season": None, **c})
                               for c in o["components"]])


def _eval_case(c):
    n, old = c["new"], _old(c["old"])
    eff = date.fromisoformat(n["effective_date"]) if n["effective_date"] else (old.effective_date if old else None)
    return wg.evaluate(new_type=n["rate_type"], new_comps=n["components"], old=old,
                       extracted_comps=n["components"], new_eff=eff,
                       new_scope=n.get("energy_scope") or "", source_url=n["source_url"])


class TestCapturedCases(unittest.TestCase):
    """Every captured case: holds exactly where expected, passes otherwise."""

    @classmethod
    def setUpClass(cls):
        cls.cases = json.loads(FIX.read_text())["cases"]

    def test_fixture_covers_holds_and_passes(self):
        self.assertGreaterEqual(sum(1 for c in self.cases if c["expect"]), 7)
        self.assertGreaterEqual(sum(1 for c in self.cases if not c["expect"]), 10)

    def test_each_case(self):
        for c in self.cases:
            with self.subTest(c["id"], why=c["why"]):
                rules = [r for r, _ in _eval_case(c)]
                if c["expect"]:
                    for r in c["expect"]:
                        self.assertIn(r, rules, f"{c['id']}: expected {r}, got {rules}")
                else:
                    self.assertEqual(rules, [], f"{c['id']} should pass: {_eval_case(c)}")

    def _case(self, prefix):
        return next(c for c in self.cases if c["id"].startswith(prefix))

    def test_fpl_rtr1_tou_lost(self):
        hits = dict(_eval_case(self._case("R25-381-Residential TOU Rider")))
        self.assertIn("tou_downgrade", hits)
        self.assertIn("hours not shown", hits["tou_adders_unused"])

    def test_dte_cpp_event_in_everyday(self):
        hits = dict(_eval_case(self._case("R25-304-Rate Schedule No. D1.11")))
        self.assertIn("Critical Peak", hits["event_in_everyday"])

    def test_ppl_pro_forma(self):
        hits = dict(_eval_case(self._case("R20b2-895")))
        self.assertIn("pro-forma", hits["unfiled_source"])


def _e(v, **kw):
    return {"component_type": "energy", "unit": "$/kWh", "rate_value": v, **kw}


def _adj(v, label, inc=True):
    return {"component_type": "adjustment", "unit": "$/kWh", "rate_value": v,
            "period_label": label, "included_in_energy": inc, "adjustment": True}


def _row(rate_type, comps, eff=None, scope=None, name="Residential Service", rid=1, factors=None):
    cf = dict(factors or {})
    if scope:
        cf["energy_scope"] = scope
    return NS(id=rid, name=name, code=None, rate_type=rate_type, effective_date=eff,
              confidence_factors=cf, rate_components=[NS(**{"period_start_time": None, "period_end_time": None,
                                                            "included_in_energy": False, "period_label": None,
                                                            "tier_label": None, "season": None, **c}) for c in comps])


TOU2 = [_e(0.30, period_label="On-Peak"), _e(0.10, period_label="Off-Peak")]


class TestRules(unittest.TestCase):
    def test_tou_to_flat_and_fewer_periods(self):
        old = _row("tou", TOU2)
        self.assertEqual(wg.evaluate(new_type="flat", new_comps=[_e(0.15)], old=old)[0][0], "tou_downgrade")
        old3 = _row("tou", TOU2 + [_e(0.2, period_label="Mid-Peak")])
        self.assertEqual([r for r, _ in wg.evaluate(new_type="tou", new_comps=TOU2, old=old3)], ["tou_downgrade"])
        self.assertEqual(wg.evaluate(new_type="tou", new_comps=TOU2, old=_row("tou", TOU2)), [])

    def test_price_jump_unexplained_vs_newer_effective_date(self):
        old = _row("flat", [_e(0.10)], eff=date(2026, 1, 1))
        hits = wg.evaluate(new_type="flat", new_comps=[_e(0.15)], old=old, new_eff=date(2026, 1, 1))
        self.assertEqual([r for r, _ in hits], ["price_jump"])
        self.assertEqual(wg.evaluate(new_type="flat", new_comps=[_e(0.15)], old=old, new_eff=date(2026, 9, 1)), [])
        self.assertEqual(wg.evaluate(new_type="flat", new_comps=[_e(0.12)], old=old, new_eff=date(2026, 1, 1)), [])
        hits = wg.evaluate(new_type="flat", new_comps=[_e(0.06)], old=old, new_eff=date(2026, 1, 1))
        self.assertEqual([r for r, _ in hits], ["price_jump"])

    def test_price_jump_explained_by_default_supply_combine(self):
        old = _row("flat", [_e(0.06)], eff=date(2026, 1, 1), scope="delivery_only")
        self.assertEqual(wg.evaluate(new_type="flat", new_comps=[_e(0.19)], old=old, new_eff=date(2026, 1, 1),
                                     new_scope="delivery_plus_default_supply"), [])

    def test_price_jump_explained_by_rider_fold_over_same_base(self):
        old = _row("flat", [_e(0.10)], eff=date(2026, 1, 1))
        new = [_e(0.15, base_rate_value=0.101)]
        self.assertEqual(wg.evaluate(new_type="flat", new_comps=new, old=old, extracted_comps=new,
                                     new_eff=date(2026, 1, 1)), [])
        moved = [_e(0.15, base_rate_value=0.16)]
        self.assertTrue(wg.evaluate(new_type="flat", new_comps=moved, old=old, extracted_comps=moved,
                                    new_eff=date(2026, 1, 1)))

    def test_unfiled_url_and_text(self):
        self.assertTrue(wg.unfiled_source("https://x.com/docs/Pro-Forma-Retail-Tariff.pdf"))
        self.assertTrue(wg.unfiled_source("https://x.com/rates.pdf", "RATE RS\nEFFECTIVE: XXXXXXXXX\n"))
        self.assertTrue(wg.unfiled_source("https://x.com/rates.pdf", "DRAFT TARIFF - for discussion"))
        self.assertIsNone(wg.unfiled_source("https://x.com/rates.pdf", "Effective: October 1, 2026"))

    def test_event_in_everyday(self):
        comps = [_e(0.12), _adj(0.5, "Critical Peak Pricing event charge")]
        self.assertTrue(wg.event_in_everyday(comps))
        self.assertIsNone(wg.event_in_everyday([_e(0.12), _adj(0.5, "Critical Peak Pricing event charge", inc=False)]))
        self.assertIsNone(wg.event_in_everyday([_e(0.12), _adj(0.002, "Energy Efficiency Rider")]))

    def test_tou_adders_unused(self):
        comps = [_e(0.10), _adj(0.05, "On-Peak Energy Charge adder (hours not shown)", inc=False)]
        self.assertTrue(wg.tou_adders_unused("tiered", comps))
        self.assertIsNone(wg.tou_adders_unused("tou", comps))

    def test_dup_same_code_only_when_reconcile_skipped(self):
        dup = NS(id=7, name="Rate D1.11")
        self.assertEqual(wg.evaluate(new_type="tou", new_comps=TOU2, dup_row=dup, reconcile_skipped=True)[0][0],
                         "dup_same_code")
        self.assertEqual(wg.evaluate(new_type="tou", new_comps=TOU2, dup_row=dup, reconcile_skipped=False), [])

    def test_non_tou_labels_are_not_periods(self):
        self.assertEqual(wg.period_count([_e(0.13, period_label="SDCP PowerOn 45% Renewable"),
                                          _e(0.14, period_label="SDG&E Standard")]), 0)
        self.assertEqual(wg.period_count([_e(0.5, period_label="Critical peak period")] + TOU2), 2)
        self.assertFalse(wg.is_tou("demand_tou", [_e(0.08, period_label="All hours")], "Rate FD-D Family Dwelling"))
        self.assertTrue(wg.is_tou("tou", [_e(0.1)], "Time of Use – TOUDR"))

    def test_pair_check(self):
        old = _row("tou", TOU2, eff=date(2026, 1, 1))
        self.assertEqual(wg.pair_check(old, _row("flat", [_e(0.2)], eff=date(2026, 1, 1)))[0][0], "tou_downgrade")
        flat_old = _row("flat", [_e(0.10)], eff=date(2026, 1, 1))
        self.assertTrue(wg.pair_check(flat_old, _row("flat", [_e(0.2)], eff=date(2026, 1, 1))))
        self.assertEqual(wg.pair_check(flat_old, _row("flat", [_e(0.2)], eff=date(2026, 1, 1),
                                                      factors={"riders_folded": True})), [])
        self.assertEqual(wg.pair_check(flat_old, _row("flat", [_e(0.2)], eff=date(2026, 6, 1))), [])


PPL_HTML = (
    '<a href="/site/-/media/PPLElectric/At-Your-Service/Docs/Current-Electric-Tariff/2026/September/401master_posted-9-1-26.pdf">Sep</a>'
    '<a href="/site/-/media/PPLElectric/At-Your-Service/Docs/Current-Electric-Tariff/2026/October/402master4_posted-10-2-26.pdf">Oct</a>'
    '<a href="/site/-/media/PPLElectric/At-Your-Service/Docs/Current-Electric-Tariff/2026/September/Pro-Forma-Retail-Tariff-Supplement---CPTR.pdf">pf</a>'
)
PSEG_HTML = (
    '<a href="/-/media/pseg/public-site/documents/current-electric-tariff/electric-tariff-17-gprc-effective-20250101.ashx">a</a>'
    '<a href="/-/media/pseg/public-site/documents/current-electric-tariff/electric-tariff-17-cefev-iap-nits-usf-effective-20261001.ashx">b</a>'
    '<a href="/-/media/pseg/public-site/documents/current-electric-tariff/2026-09-01-electric-reconciliation-charge.ashx">c</a>'
)


class TestCurrentBooks(unittest.TestCase):
    """R26 fixes 5-6: start from the current official book."""

    def test_ppl_newest_master(self):
        url, why = sp.resolve_current_book("PPL Electric Utilities Corp", "PA", lambda u: PPL_HTML)
        self.assertTrue(url.endswith("/2026/October/402master4_posted-10-2-26.pdf"))
        self.assertTrue(url.startswith("https://www.pplelectric.com/"))
        self.assertIn("GSC-1", why)

    def test_pseg_newest_full_tariff(self):
        url, why = sp.resolve_current_book("Public Service Elec & Gas Co", "NJ", lambda u: PSEG_HTML)
        self.assertTrue(url.endswith("effective-20261001.ashx"))
        self.assertIn("BGS-RSCP", why)

    def test_xcel_mn_and_el_paso(self):
        url, _ = sp.resolve_current_book("Northern States Power Co - Minnesota", "MN", None)
        self.assertIn("xe-responsive", url)
        self.assertIsNone(sp.resolve_current_book("Northern States Power Co - Wisconsin", "WI", None))
        url, _ = sp.resolve_current_book("El Paso Electric Co", "TX", None)
        self.assertIn("eff_08-01-2026", url)
        self.assertIsNone(sp.resolve_current_book("El Paso Electric Co", "NM", None))

    def test_coned_has_no_single_supply_sheet(self):
        self.assertIsNone(sp.resolve_current_book("Consolidated Edison Co-NY Inc", "NY", None))

    def test_index_without_match_or_fetch_error(self):
        self.assertIsNone(sp.resolve_current_book("PPL Electric Utilities Corp", "PA", lambda u: "<html/>"))

        def boom(u):
            raise RuntimeError("down")
        self.assertIsNone(sp.resolve_current_book("PPL Electric Utilities Corp", "PA", boom))

    def test_stale_paths(self):
        old = "https://www.xcelenergy.com/staticfiles/xe/Regulatory/Regulatory%20PDFs/rates/MN/Me_Section_5.pdf"
        self.assertTrue(sp.known_stale_document(old))
        self.assertTrue(sp.is_stale_document(old))
        self.assertFalse(sp.is_stale_document(sp.XCEL_MN_CURRENT_BOOK))
        self.assertTrue(sp.known_stale_document(
            "https://www.epelectric.com/files/html/Rates_and_Regulatory/Texas/Schedule01.pdf"))


# ---------------------------------------------------------------- DB-backed
from tests.pg_harness import PostgresTestCase, energy_values  # noqa: E402


def _db_comp(c):
    c = {k: v for k, v in c.items() if k != "base_rate_value"}
    return c


class TestStoreTariffsGate(PostgresTestCase):
    def _et(self, name, comps, rate_type="flat", eff=None, url="https://utility.example.com/rates", code=""):
        from scripts import tariff_pipeline as tp
        return tp.ExtractedTariff(name=name, code=code, customer_class="residential", rate_type=rate_type,
                                  description=None, source_url=url, effective_date=eff, components=comps)

    def _holds(self, uid):
        return [e for e in self.events_for(uid) if e.decision == "hold" and (e.reason or "").startswith("write_gate:")]

    def test_tou_refresh_to_tiered_is_held(self):
        from scripts import tariff_pipeline as tp
        uid = self.make_utility("Gate TOU Electric")
        old = self.make_tariff(uid, "Residential TOU Rider (RTR-1)", TOU2, rate_type="tou",
                               effective_date=date(2026, 9, 1))
        new = [_e(0.12, tier_label="First 1000 kWh"),
               _adj(0.05, "On-Peak Energy Charge adder (hours not shown)", inc=False)]
        n = tp.store_tariffs(uid, [self._et("Residential TOU Rider (RTR-1)", new, "tiered", "2026-09-01")],
                             dry_run=False)
        self.assertEqual(n, 0)
        live = self.tariffs_for(uid, live_only=True)
        self.assertEqual([t.id for t in live], [old])
        self.assertEqual(sorted(energy_values(self.get_tariff(old))), [0.10, 0.30])
        holds = self._holds(uid)
        self.assertEqual(len(holds), 1)
        self.assertEqual(holds[0].before_tariff_id, old)
        rules = [g["rule"] for g in holds[0].payload["gate"]]
        self.assertIn("tou_downgrade", rules)
        self.assertTrue(any(h["utility_id"] == uid for h in tp.GATE_HOLDS))

    def test_benign_refresh_is_written(self):
        from scripts import tariff_pipeline as tp
        uid = self.make_utility("Gate Benign Electric")
        old = self.make_tariff(uid, "Residential Service", [_e(0.10)], effective_date=date(2026, 1, 1))
        n = tp.store_tariffs(uid, [self._et("Residential Service", [_e(0.11)], eff="2026-09-01")], dry_run=False)
        self.assertEqual(n, 1)
        self.assertIsNotNone(self.get_tariff(old).supersede_reason)
        self.assertEqual(self._holds(uid), [])

    def test_event_charge_plan_is_held_and_not_inserted(self):
        from scripts import tariff_pipeline as tp
        uid = self.make_utility("Gate CPP Electric")
        self.make_tariff(uid, "Rate D1.11 Standard TOU", TOU2, rate_type="seasonal_tou")
        new = TOU2 + [_adj(0.108, "Critical Peak Hours non-capacity energy")]
        tp.store_tariffs(uid, [self._et("Rate Schedule No. D1.11 Residential TOU", new, "seasonal_tou")],
                         dry_run=False)
        self.assertEqual(len(self.tariffs_for(uid)), 1)
        self.assertEqual(self._holds(uid)[0].reason, "write_gate:event_in_everyday")

    def test_unfiled_page_text_is_held(self):
        from scripts import tariff_pipeline as tp
        uid = self.make_utility("Gate ProForma Electric")
        old = self.make_tariff(uid, "Rate Schedule RS", [_e(0.10)], effective_date=date(2026, 1, 1))
        et = self._et("Rate Schedule RS", [_e(0.105)], eff="2026-10-01", url="https://utility.example.com/rs.pdf")
        page = NS(url="https://utility.example.com/rs.pdf", content="RATE RS\nEFFECTIVE: XXXXXXXXX")
        self.assertEqual(tp.mark_unfiled_sources([et], [page]), 1)
        tp.store_tariffs(uid, [et], dry_run=False)
        self.assertIsNone(self.get_tariff(old).supersede_reason)
        self.assertEqual(self._holds(uid)[0].reason, "write_gate:unfiled_source")

    def test_held_code_match_is_not_reconciled_away(self):
        from scripts import tariff_pipeline as tp
        uid = self.make_utility("Gate Reconcile Electric")
        a = self.make_tariff(uid, "Schedule R-1 Residential Time of Use", TOU2, rate_type="tou")
        b = self.make_tariff(uid, "Schedule R-2 Residential", [_e(0.11)])
        ets = [self._et("R-1 Time-of-Use Residential", [_e(0.15)], "flat"),
               self._et("Schedule R-2 Residential", [_e(0.115)])]
        tp.store_tariffs(uid, ets, dry_run=False)
        self.assertIsNone(self.get_tariff(a).supersede_reason, "held comparison row must stay live")
        self.assertEqual(self._holds(uid)[0].before_tariff_id, a)
        self.assertEqual(self._holds(uid)[0].reason, "write_gate:tou_downgrade")
        self.assertIsNotNone(self.get_tariff(b).supersede_reason, "the benign R-2 refresh still lands")


if __name__ == "__main__":
    unittest.main()
