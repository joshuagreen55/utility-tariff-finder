"""R21: delivery-only utilities — delivery + standard-offer supply, combined
and labelled (Joshua default 2); never half-plans.

Fixtures are the R20 trial phase-4 outputs:
* PPL (895): RS / RTS (R) distribution-only + GSC-1 Fixed Price supply
  (+ GSC-1 TOU program). R20 stored the halves separately.
* Mass Electric (664): four model-declared delivery-only plans, no basic
  service price in the run. R20 stored them as half-plans.
* SDG&E (1000): bundled plans — must be untouched.

    cd backend && python -m unittest tests.test_r21_supply_delivery -v
"""
from __future__ import annotations

import copy
import json
import unittest
from dataclasses import fields
from pathlib import Path

from scripts import tariff_pipeline as tp

FX = Path(__file__).resolve().parent / "fixtures" / "r21"
_F = {f.name for f in fields(tp.ExtractedTariff)}


def _load(name):
    return [tp.ExtractedTariff(**{k: copy.deepcopy(v) for k, v in d.items() if k in _F})
            for d in json.loads((FX / name).read_text())]


def _energy(t):
    return sorted(round(float(c["rate_value"]) * 100, 4) for c in t.components if c["component_type"] == "energy")


class TestPPL(unittest.TestCase):
    def setUp(self):
        self.kept, self.info = tp.combine_supply_with_delivery(_load("ppl_895_r20_phase4_valid.json"))

    def test_two_full_price_plans_no_halves(self):
        names = sorted(t.name for t in self.kept)
        self.assertEqual(names, [
            "Rate Schedule RS - Residential Service — delivery + default supply",
            "Rate Schedule RTS (R) - Residential Service – Thermal Storage — delivery + default supply",
        ])
        by = {t.name.split(" - ")[0]: t for t in self.kept}
        # 6.175 distribution + 13.079 GSC-1 (9.753) + TSC (3.326) = 19.254 ¢/kWh
        self.assertEqual(_energy(by["Rate Schedule RS"]), [19.254])
        self.assertEqual(_energy(by["Rate Schedule RTS (R)"]), [18.071])
        for t in self.kept:
            self.assertEqual(t.energy_scope, "delivery_plus_default_supply")
            self.assertEqual(t.confidence_notes["combined_supply"]["label"], "delivery + default supply")
            adj = [c for c in t.components if c["component_type"] == "adjustment"]
            self.assertTrue(all(c.get("included_in_energy") for c in adj))
            self.assertFalse(any("GSC" in m for m in t.missing_fields))
            self.assertTrue(any(c["component_type"] == "fixed" for c in t.components))

    def test_optional_supply_program_dropped(self):
        self.assertEqual(self.info.get("supply_only_dropped"), None)  # TOU program not in this fixture
        self.assertEqual(len(self.info["supply_combined"]), 2)

    def test_combined_plans_survive_same_plan_dedupe(self):
        kept, actions = tp.dedupe_same_plan_variants(self.kept, "PPL Electric Utilities Corp")
        self.assertEqual(len(kept), 2, actions)


class TestHalfPlansNeverStored(unittest.TestCase):
    def test_mass_electric_delivery_only_dropped(self):
        kept, info = tp.combine_supply_with_delivery(_load("meco_664_r20_phase4_valid.json"))
        self.assertFalse(any(t.energy_scope == "delivery_only" for t in kept))
        self.assertEqual(len(info["delivery_only_dropped"]), 4)

    def test_bundled_plans_untouched(self):
        ts = _load("sdge_1000_r20_phase4_valid.json")
        kept, info = tp.combine_supply_with_delivery(ts)
        self.assertEqual(len(kept), len(ts))
        self.assertEqual(info, {})


class TestDefaultSupply(unittest.TestCase):
    def _s(self, name, v=0.10):
        return tp.ExtractedTariff(name=name, customer_class="residential", rate_type="flat", energy_scope="supply_only",
                                  components=[{"component_type": "energy", "unit": "$/kWh", "rate_value": v}])

    def test_single_standard_offer_is_default(self):
        std = self._s("Basic Service Fixed Residential")
        self.assertIs(tp.default_supply_plan([std, self._s("Time of Use Supply Program"), self._s("2-year Fixed")]), std)

    def test_two_standard_offers_is_ambiguous(self):
        self.assertIsNone(tp.default_supply_plan([self._s("Supply A"), self._s("Supply B")]))

    def test_tou_delivery_with_flat_supply_keeps_periods(self):
        d = tp.ExtractedTariff(name="RT Residential TOU", customer_class="residential", rate_type="tou",
                               energy_scope="delivery_only", components=[
                                   {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.08, "period_label": "On"},
                                   {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.02, "period_label": "Off"}])
        kept, _ = tp.combine_supply_with_delivery([d, self._s("Standard Offer Service", 0.10)])
        self.assertEqual(_energy(kept[0]), [12.0, 18.0])
        self.assertEqual(kept[0].rate_type, "tou")


if __name__ == "__main__":
    unittest.main()
