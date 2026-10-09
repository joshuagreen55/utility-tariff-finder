"""R21: "base only" safety net — a plan whose source references per-kWh
fuel / cost-recovery riders that are not in its ENERGY price is flagged and
does NOT count as Mysa-complete (API contract + health score).

Fixture: R20 trial phase-4 plans for Georgia Power (419: fuel/ECCR/DSM not
added), Evergy Metro (553: FAC/DSIM), Xcel MN (819: TCR+RDF folded, fuel
clause not), Dominion (1213: "Exhibit of Applicable Riders"), SDG&E (1000:
bundled all-in, must stay full price).

    cd backend && python -m unittest tests.test_r21_base_only -v
"""
from __future__ import annotations

import copy
import json
import unittest
from dataclasses import fields
from pathlib import Path
from types import SimpleNamespace

from app.services.computable import tariff_contract
from app.services.price_basis import base_only_from_factors, unadded_price_riders
from scripts import tariff_pipeline as tp
from scripts.health_score import _mysa_ready_stats

FX = Path(__file__).resolve().parent / "fixtures" / "r21" / "riders_r20_phase4_valid.json"
_F = {f.name for f in fields(tp.ExtractedTariff)}


def _plans(uid):
    return [tp.ExtractedTariff(**{k: copy.deepcopy(v) for k, v in d.items() if k in _F})
            for d in json.loads(FX.read_text())[uid]]


class TestR20Replay(unittest.TestCase):
    def _marked(self, uid):
        ts = _plans(uid)
        tp.mark_base_only_plans(ts)
        return ts

    def test_georgia_evergy_dominion_all_base_only(self):
        for uid in ("419", "553", "1213"):
            for t in self._marked(uid):
                self.assertEqual(t.confidence_notes.get("price_basis"), "base_only", (uid, t.name))
                self.assertIn("base_only_riders_not_added", t.missing_fields)
                self.assertTrue(t.needs_review)

    def test_xcel_fuel_clause_missing_even_with_tcr_folded(self):
        ts = {t.name: t for t in self._marked("819")}
        tod = ts["Residential Time of Day Service (Standard)"]
        self.assertEqual(tod.confidence_notes["price_basis"], "base_only")
        self.assertIn("Fuel Clause Rider", tod.confidence_notes["riders_not_added"])
        self.assertNotIn("Transmission Cost Recovery Rider", tod.confidence_notes["riders_not_added"])

    def test_sdge_bundled_plans_untouched(self):
        for t in self._marked("1000"):
            self.assertIsNone((t.confidence_notes or {}).get("price_basis"), t.name)

    def test_prices_never_changed(self):
        before = [[c.get("rate_value") for c in t.components] for t in _plans("419")]
        after = [[c.get("rate_value") for c in t.components] for t in self._marked("419")]
        self.assertEqual(before, after)


class TestRules(unittest.TestCase):
    def test_folded_rider_is_not_missing(self):
        comps = [{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.12, "tier_label": "all-in"},
                 {"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.03,
                  "tier_label": "Fuel Cost Recovery", "included_in_energy": True}]
        self.assertEqual(unadded_price_riders(riders_referenced=["Fuel Cost Recovery Schedule"], missing_fields=[],
                                              energy_includes_riders=True, components=comps), [])

    def test_franchise_fee_and_tax_are_not_price_riders(self):
        self.assertEqual(unadded_price_riders(riders_referenced=["Municipal Franchise Fee", "Sales Tax", "Low Income Credit"],
                                              missing_fields=[], energy_includes_riders=False, components=[]), [])

    def test_stored_row_factors_judged_without_backfill(self):
        cf = {"riders_referenced_not_shown": ["Schedule FAC"], "energy_includes_riders": False}
        self.assertEqual(base_only_from_factors(cf, []), ["Schedule FAC"])
        self.assertEqual(base_only_from_factors({"price_basis": "full", **cf}, []), [])


def _row(cf):
    comps = [SimpleNamespace(component_type="energy", unit="$/kWh", rate_value=0.1, tier_min_kwh=None, tier_max_kwh=None,
                             period_label=None, tier_label=None, season=None, period_start_time=None, period_end_time=None,
                             day_type=None, season_start_month=None, season_start_day=None, season_end_month=None,
                             season_end_day=None, included_in_energy=False)]
    return SimpleNamespace(rate_type="flat", rate_components=comps, name="Residential", code=None,
                           confidence_factors=cf, utility_id=1)


class TestScoreAndContract(unittest.TestCase):
    def test_contract_marks_base_only_not_computable(self):
        c = tariff_contract(_row({"price_basis": "base_only", "riders_not_added": ["Fuel"]}))
        self.assertFalse(c["computable"])
        self.assertIn("base_only_riders_not_added", c["computable_reasons"])
        self.assertTrue(c["needs_review"])
        self.assertTrue(tariff_contract(_row({}))["computable"])

    def test_health_score_does_not_count_base_only(self):
        stats = _mysa_ready_stats([(_row({}), None), (_row({"price_basis": "base_only"}), None)])
        self.assertEqual(stats["mysa_ready"], 1)
        self.assertEqual(stats["top_blocking_reasons"].get("base_only_riders_not_added"), 1)


if __name__ == "__main__":
    unittest.main()
