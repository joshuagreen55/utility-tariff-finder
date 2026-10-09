"""R21: retire old copies that a row written this run clearly replaces,
even when the run picked up < 75% of a utility's plans.

Fixture ``fixtures/r21/r20_after_live_rows.json`` = live residential rows
after the R20 40-utility trial for LADWP (627), Hydro Ottawa (1725), Evergy
Metro (553), Alabama Power (10), ENWIN (1757), Niagara (1733), EPCOR (1717).
"Fresh" = written in the trial (id > 71311).

    cd backend && python -m unittest tests.test_r21_clear_replacement -v
"""
from __future__ import annotations

import json
import unittest
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

from scripts import tariff_pipeline as tp
from tests.pg_harness import PostgresTestCase, energy_values

FX = Path(__file__).resolve().parent / "fixtures" / "r21" / "r20_after_live_rows.json"
TODAY = date(2026, 10, 8)
NAMES = {"627": "Los Angeles Department of Water & Power", "1725": "Ottawa Hydro (Hydro Ottawa)",
         "553": "Evergy Metro", "10": "Alabama Power Co", "1757": "ENWIN Utilities",
         "1733": "Niagara Peninsula Energy", "1717": "EPCOR"}


def _rows(uid):
    out = []
    for d in json.loads(FX.read_text())[uid]:
        d = dict(d)
        d["effective_date"] = date.fromisoformat(d["effective_date"]) if d["effective_date"] else None
        d["customer_class"] = "residential"
        d["last_verified_at"] = datetime.fromisoformat(d["last_verified_at"]) if d["last_verified_at"] else None
        out.append(SimpleNamespace(**d))
    return out


def _plan(uid):
    rows = _rows(uid)
    fresh = {r.id for r in rows if r.id > 71311}
    return {(o.id, k.id) for o, k in tp.plan_clear_replacements(rows, fresh, utility_name=NAMES[uid], today=TODAY)}


class TestR20Replay(unittest.TestCase):
    def test_ladwp_duplicates_retired_onto_fresh_rows(self):
        self.assertEqual(_plan("627"), {(70212, 71342), (71015, 71350)})

    def test_hydro_ottawa_old_rows_retired_onto_oeb_feed(self):
        self.assertEqual(_plan("1725"), {(47349, 71424), (47350, 71424), (47351, 71426), (47352, 71425)})

    def test_enwin_covid_recovery_row_is_not_an_rpp_copy(self):
        retired = {o for o, _ in _plan("1757")}
        self.assertNotIn(59308, retired)  # "COVID-19 Recovery Rate for Time-of-Use Customers"
        self.assertIn(59302, retired)

    def test_niagara_third_party_rpp_rows_retired(self):
        self.assertEqual({o for o, _ in _plan("1733")}, {56724, 56725, 56726})

    def test_evergy_space_heat_variant_is_a_different_plan(self):
        retired = {o for o, _ in _plan("553")}
        self.assertNotIn(63142, retired)  # "General Use and Space Heat Two Meters" (code R)
        self.assertNotIn(63147, retired)  # RTOU-3 vs "... (Nights and Weekends Max Saver)"

    def test_alabama_editions_retired_without_chaining(self):
        plan = _plan("10")
        keepers = {k for _, k in plan}
        self.assertTrue(all(k > 71311 for k in keepers))
        self.assertFalse({o for o, _ in plan} & keepers)
        self.assertIn((70632, 71406), plan)

    def test_epcor_utility_name_is_not_a_qualifier(self):
        self.assertEqual({o for o, _ in _plan("1717")}, {59617, 69626})


def _r(id, name, code=None, rt="flat", eff=None, **kw):
    base = dict(id=id, name=name, code=code, rate_type=rt, effective_date=eff, customer_class="residential",
                source_type="official", approved=False, openei_id=None, last_verified_at=None,
                confidence_factors={}, rate_components=[])
    base.update(kw)
    return SimpleNamespace(**base)


class TestGuards(unittest.TestCase):
    def test_future_dated_fresh_row_never_replaces(self):
        old = _r(1, "Rate RS Residential", "RS", eff=date(2026, 1, 1))
        new = _r(2, "Rate RS - Residential Service", "RS", eff=date(2027, 1, 1))
        self.assertEqual(tp.plan_clear_replacements([old, new], {2}, today=TODAY), [])

    def test_older_dated_fresh_row_does_not_replace_newer_row(self):
        old = _r(1, "Rate RS Residential", "RS", eff=date(2026, 6, 1))
        new = _r(2, "Rate RS - Residential Service", "RS", eff=date(2024, 9, 1))
        self.assertEqual(tp.plan_clear_replacements([old, new], {2}, today=TODAY), [])

    def test_official_fresh_row_replaces_third_party_copy_regardless_of_date(self):
        old = _r(1, "Residential Service", "RS", eff=date(2026, 6, 1), source_type="third_party")
        new = _r(2, "Rate Schedule RS (Residential Service)", "RS", eff=date(2025, 7, 1))
        self.assertEqual([(o.id, k.id) for o, k in tp.plan_clear_replacements([old, new], {2}, today=TODAY)], [(1, 2)])

    def test_half_plan_is_never_a_keeper(self):
        old = _r(1, "Rate Schedule RS", "RS", eff=date(2026, 1, 1))
        new = _r(2, "Rate Schedule RS - Residential Service", "RS", eff=date(2026, 10, 1),
                 confidence_factors={"energy_scope": "delivery_only"})
        self.assertEqual(tp.plan_clear_replacements([old, new], {2}, today=TODAY), [])

    def test_commodity_only_oeb_row_is_not_a_keeper(self):
        old = _r(1, "Time-of-Use", "TOU", rt="tou", eff=date(2025, 11, 1))
        new = _r(2, "Time-of-Use (TOU) — Residential", "OEB-RPP-TOU", rt="seasonal_tou", eff=date(2025, 11, 1),
                 confidence_factors={"origin": "oeb_feed"})
        self.assertEqual(tp.plan_clear_replacements([old, new], {2}, today=TODAY), [])

    def test_different_codes_never_match(self):
        old = _r(1, "Residential Time of Use", "RTOU-2", rt="tou")
        new = _r(2, "Residential Time of Use", "RTOU-3", rt="tou")
        self.assertFalse(tp.clearly_same_plan(old, new))

    def test_ambiguous_old_row_stays_live(self):
        old = _r(1, "Rate A Residential", "A", rt="complex", eff=date(2025, 1, 1))
        n1 = _r(2, "Rate A Residential", "A", rt="tou", eff=date(2026, 1, 1))
        n2 = _r(3, "Rate A Residential", "A", rt="tiered", eff=date(2026, 1, 1))
        self.assertEqual(tp.plan_clear_replacements([old, n1, n2], {2, 3}, today=TODAY), [])


class TestStoreTariffsPartialExtraction(PostgresTestCase):
    def test_partial_extraction_still_retires_clear_copy(self):
        uid = self.make_utility("Example Power")
        dup = self.make_tariff(uid, "R-1B Time-of-Use Residential Service",
                               [{"component_type": "energy", "rate_value": 0.20}], rate_type="flat")
        others = [self.make_tariff(uid, f"Other plan {i}", [{"component_type": "energy", "rate_value": 0.1 + i / 100}])
                  for i in range(4)]
        with self.session() as s:
            from app.models import Tariff
            t = s.get(Tariff, dup)
            t.code = "R-1B"
            s.commit()
        et = tp.ExtractedTariff(
            name="Time of Use R-1B (TOU) Residential", code="R-1B", customer_class="residential",
            rate_type="flat", description=None, source_url="https://utility.example.com/rates",
            effective_date=None, components=[{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.21}],
        )
        tp.store_tariffs(uid, [et], dry_run=False)
        old = self.get_tariff(dup)
        self.assertEqual(old.supersede_reason, "replaced")
        self.assertEqual(energy_values(old), [0.20], "components retained")
        for o in others:
            self.assertIsNone(self.get_tariff(o).supersede_reason, "partial run: reconcile still skipped")


if __name__ == "__main__":
    unittest.main()
