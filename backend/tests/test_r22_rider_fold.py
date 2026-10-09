"""R22: rider schedules extracted in the same batch are folded into the full
per-kWh price (R20 Georgia Power phase-3 output: FCR-27, TOU-FCR-7, ECCR-15,
DSM-R-16 were extracted but never added)."""
import dataclasses
import json
import logging
import unittest
from pathlib import Path

from app.services.rider_fold import plan_fold, season_months
from scripts import tariff_pipeline as tp

FIX = json.loads((Path(__file__).parent / "fixtures/r21/georgia_419_r20_phase3.json").read_text())["419"]
F = {f.name for f in dataclasses.fields(tp.ExtractedTariff)}


def _run():
    ts = [tp.ExtractedTariff(**{k: v for k, v in t.items() if k in F}) for t in FIX["phase3_tariffs"]]
    logging.disable(logging.WARNING)
    try:
        return tp.phase4_validate(ts, FIX["utility_name"], FIX["state"] or "GA")
    finally:
        logging.disable(logging.NOTSET)


def _energy(t):
    return {(c.get("season"), c.get("tier_label") or c.get("period_label")): round(c["rate_value"] * 100, 4)
            for c in t.components if c["component_type"] == "energy"}


class Seasons(unittest.TestCase):
    def test_months(self):
        self.assertEqual(season_months("June-September"), {6, 7, 8, 9})
        self.assertEqual(season_months("October-May"), {10, 11, 12, 1, 2, 3, 4, 5})
        self.assertEqual(season_months("Summer"), {6, 7, 8, 9})
        self.assertEqual(season_months(None), set())


class GeorgiaFold(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rep, cls.valid = _run()
        cls.by = {t.name: t for t in cls.valid}

    def test_r31_full_price(self):
        t = self.by["Residential Service Schedule R-31"]
        e = _energy(t)
        pct = 1 + 0.130205 + 0.011969
        # base * (1 + ECCR% + DSM-R%) + FCR-27 (seasonal, secondary)
        self.assertAlmostEqual(e[("Summer", "First 650 kWh")], round(8.7738 * pct + 3.8069, 4), places=3)
        self.assertAlmostEqual(e[("Summer", "Over 1000 kWh")], round(15.0828 * pct + 3.8069, 4), places=3)
        self.assertAlmostEqual(e[("Winter", "All kWh")], round(8.2116 * pct + 3.8561, 4), places=3)
        self.assertNotEqual(t.confidence_notes.get("price_basis"), "base_only")
        self.assertIn("Fuel Cost Recovery Schedule FCR-27", t.confidence_notes["riders_folded"])
        # basic service charge scaled by the percent-of-base riders
        fixed = [c for c in t.components if c["component_type"] == "fixed"][0]
        self.assertAlmostEqual(fixed["rate_value"], round(0.4603 * pct, 6), places=5)

    def test_tou_uses_tou_fuel(self):
        t = self.by["Time of Use – Residential Demand"]
        vals = sorted(set(_energy(t).values()))
        pct = 1 + 0.130205 + 0.011969
        self.assertAlmostEqual(vals[-1], 14.2986 * pct + 5.2269, places=3)  # on-peak + TOU-FCR on-peak
        self.assertAlmostEqual(vals[0], 1.5288 * pct + 3.7441, places=3)   # off-peak + TOU-FCR off-peak
        self.assertIn("Time of Use Fuel Cost Recovery Schedule TOU-FCR-7", t.confidence_notes["riders_folded"])

    def test_audit_rows(self):
        t = self.by["Residential Service Schedule R-31"]
        adj = [c for c in t.components if c["component_type"] == "adjustment" and c.get("included_in_energy")]
        self.assertTrue(any("FCR-27" in c["period_label"] for c in adj))


class Guards(unittest.TestCase):
    def _plan(self, hints, rt="flat", energy=None):
        return tp.ExtractedTariff(name="Residential", customer_class="residential", rate_type=rt,
                                  riders_referenced_not_shown=hints,
                                  components=energy or [{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.10}])

    def _rider(self, name, rows):
        return tp.ExtractedTariff(name=name, customer_class="residential", rate_type="flat", components=rows)

    def test_unnamed_rider_not_folded(self):
        r = self._rider("Fuel Cost Recovery", [{"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.03}])
        self.assertIsNone(plan_fold(self._plan(["Storm Recovery Rider"]), [r]))

    def test_two_fuel_schedules_ambiguous(self):
        r1 = self._rider("Fuel Cost Recovery A", [{"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.03}])
        r2 = self._rider("Fuel Cost Recovery B", [{"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.04}])
        self.assertIsNone(plan_fold(self._plan(["Fuel Cost Recovery"]), [r1, r2]))

    def test_seasonal_rider_unseasoned_plan_skipped(self):
        r = self._rider("Fuel Cost Recovery", [
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.03, "season": "June-September"},
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.04, "season": "October-May"}])
        self.assertIsNone(plan_fold(self._plan(["Fuel Cost Recovery"]), [r]))

    def test_primary_voltage_ignored(self):
        r = self._rider("Fuel Adjustment Clause", [
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.02, "period_label": "Primary"},
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.021, "period_label": "Secondary"}])
        f = plan_fold(self._plan(["Schedule FAC"]), [r])
        self.assertEqual(f["per_kwh"][0][1], [0.021])


if __name__ == "__main__":
    unittest.main()
