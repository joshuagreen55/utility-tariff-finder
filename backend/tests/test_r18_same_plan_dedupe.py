"""R18: one live copy per plan, optional-variant reconcile, missing-plan hint.

Fixtures under ``tests/fixtures/r17/`` are the phase-4 accepted plans from the
R17 live run (2026-10-08) BEFORE this fix:

* SRP (995) — the 2025 standard ratebook, the temporary-fuel-cut ratebook and
  the compare-plans web page each produced copies of the same plans
  (Basic / M-Power identical; Conserve 6-9 p.m. standard vs temporary).
* Pedernales (890) — "… with Renewable Energy Rider" TOU folded TCOS as
  0.000688 instead of 0.020688 into off-/mid-peak (2.0¢ low).
* NS Power (1739) — MURB riders extracted, MURB TOU plan never extracted.

    cd backend && python -m unittest tests.test_r18_same_plan_dedupe -v
"""
from __future__ import annotations

import copy
from datetime import date, timedelta
import json
import logging
import unittest
from dataclasses import fields
from pathlib import Path

from scripts import tariff_pipeline as tp

R17 = Path(__file__).resolve().parent / "fixtures" / "r17"
_F = {f.name for f in fields(tp.ExtractedTariff)}


def _mk(d: dict) -> tp.ExtractedTariff:
    return tp.ExtractedTariff(**{k: copy.deepcopy(v) for k, v in d.items() if k in _F})


def _load(uid: str) -> dict:
    return json.loads((R17 / f"{uid}.json").read_text())


def _plans(uid: str) -> list[tp.ExtractedTariff]:
    return [_mk(d) for d in _load(uid)["phase4_valid"]]


def _riders(uid: str) -> list[tp.ExtractedTariff]:
    out = []
    for name, url, tier in _load(uid)["phase4_in_names"]:
        if not str(tier or "").startswith("rider"):
            continue
        out.append(tp.ExtractedTariff(
            name=name, code="", customer_class="residential", rate_type="flat",
            description="", source_url=url, effective_date="",
            components=[{
                "component_type": "adjustment", "rate_value": 0.002,
                "unit": "$/kWh", "included_in_energy": False,
            }],
            confidence=0.8,
        ))
    return out


def _cents(t: tp.ExtractedTariff) -> list[float]:
    return sorted({
        round(float(c["rate_value"]) * 100, 3)
        for c in t.components
        if str(c.get("component_type")).lower() == "energy"
    })


def _by_name(ts, needle):
    hits = [t for t in ts if needle in t.name]
    assert len(hits) == 1, (needle, [t.name for t in ts])
    return hits[0]


class SrpDuplicatePlans(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)
        fx = _load("995")
        self.kept, self.info = tp.reconcile_same_utility_plans(
            _plans("995"), fx["utility_name"],
        )

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_one_copy_per_plan(self):
        names = [t.name for t in self.kept]
        self.assertEqual(len(names), 7, names)
        for gone in ("Basic Price Plan", "SRP M-Power", "Conserve 6-9 p.m. and Save"):
            self.assertNotIn(gone, names)
        self.assertEqual(len(self.info["same_plan_dedupe"]), 3)

    def test_identical_copies_keep_the_tariff_book(self):
        e23 = _by_name(self.kept, "E-23")
        self.assertIn("2025_Ratebook_with_TCA", e23.source_url)
        self.assertEqual(_cents(e23), [10.97, 12.04, 13.98])
        self.assertFalse(e23.needs_review)
        merged = e23.confidence_notes["duplicate_copies_merged"]
        self.assertEqual(merged[0]["name"], "Basic Price Plan")
        e24 = _by_name(self.kept, "E-24")
        self.assertEqual(_cents(e24), [10.97, 12.04, 13.98])

    def test_conserve_conflict_prefers_standard_price(self):
        e28 = _by_name(self.kept, "Conserve 6-9")
        cents = _cents(e28)
        # Standard (non-temporary) summer prices from the web page …
        for std in (18.85, 15.06, 3.95, 40.2, 12.76, 6.61):
            self.assertIn(std, cents)
        # … and none of the temporary fuel-cut prices remain live.
        for tmp in (18.47, 14.68, 3.57, 39.82, 12.38, 6.23):
            self.assertNotIn(tmp, cents)
        # Winter was not discounted: same in both.
        for winter in (15.08, 13.55, 4.32):
            self.assertIn(winter, cents)

    def test_conserve_keeps_weekday_weekend_grid(self):
        e28 = _by_name(self.kept, "Conserve 6-9")
        days = {
            c.get("day_type") for c in e28.components
            if c.get("component_type") == "energy"
        }
        self.assertEqual(days, {"weekday", "weekend", "holiday"})
        wk_off = [
            float(c["rate_value"]) for c in e28.components
            if c.get("component_type") == "energy"
            and c.get("day_type") == "weekend"
            and c.get("season_start_month") == 5
            and c.get("period_start_time") == "15:00"
        ]
        self.assertEqual(wk_off, [0.1506])

    def test_temporary_prices_stored_with_dates(self):
        e28 = _by_name(self.kept, "Conserve 6-9")
        notes = e28.confidence_notes
        self.assertEqual(notes["temporary_price_until"], "2026-10")
        self.assertIn("Temporary-FPPAM", notes["temporary_prices"]["source_url"])
        self.assertIn(0.1847, [r["rate_value"] for r in notes["temporary_prices"]["energy"]])
        self.assertIn("compare-plans", notes["standard_prices_source_url"])
        self.assertNotIn("temporary_price_dates_unclear", e28.missing_fields or [])

    def test_temporary_only_plan_gets_expiry_from_same_book(self):
        e22 = _by_name(self.kept, "E-22")
        self.assertEqual(e22.confidence_notes["temporary_price_until"], "2026-10")
        self.assertFalse(e22.needs_review)

    def test_no_two_live_copies_with_different_prices(self):
        util = tp._r18_utility_tokens("Salt River Project")
        for i, a in enumerate(self.kept):
            for b in self.kept[i + 1:]:
                self.assertFalse(
                    tp._r18_same_plan_name(a, b, util)
                    and tp._r18_same_structure(a, b)[0],
                    (a.name, b.name),
                )

    def test_phase4_end_to_end(self):
        fx = _load("995")
        rep, valid = tp.phase4_validate(_plans("995"), fx["utility_name"], fx["state"])
        self.assertEqual(rep["valid"], 7)
        self.assertEqual(rep["duplicates_dropped"], 3)
        self.assertEqual(rep["invalid"], 0)
        self.assertIn(18.85, _cents(_by_name(valid, "Conserve 6-9")))


class DedupeEdgeCases(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def _flat(self, name, v, url="https://u.example/tariff.pdf", desc=""):
        return tp.ExtractedTariff(
            name=name, code="", customer_class="residential", rate_type="flat",
            description=desc, source_url=url, effective_date="",
            components=[{"component_type": "energy", "rate_value": v, "unit": "$/kWh"}],
            confidence=0.9,
        )

    def test_conflict_without_vintage_keeps_one_and_flags(self):
        a = self._flat("Residential Service", 0.12)
        b = self._flat("Residential Service", 0.13, url="https://u.example/rates")
        kept, actions = tp.dedupe_same_plan_variants([a, b], "Example Power")
        self.assertEqual(len(kept), 1)
        self.assertTrue(kept[0].needs_review)
        self.assertIn("duplicate_plan_price_conflict", kept[0].missing_fields)

    def test_temporary_without_dates_flags(self):
        std = self._flat("Basic Plan", 0.12, url="https://u.example/rates")
        tmp = self._flat("Basic Plan", 0.11, url="https://u.example/temporary-book.pdf")
        kept, _ = tp.dedupe_same_plan_variants([std, tmp], "Example Power")
        self.assertEqual(len(kept), 1)
        self.assertEqual(_cents(kept[0]), [12.0])
        self.assertIn("temporary_price_dates_unclear", kept[0].missing_fields)
        self.assertTrue(kept[0].needs_review)

    def test_distinct_products_not_merged(self):
        cases = [
            ("Domestic Service Time of Use Tariff",
             "Domestic Service Time of Use Tariff (Interim Energy Charge)"),
            ("Rate D", "Rate DP"),
            ("Residential TOU", "Residential TOU with Renewable Energy Rider"),
            ("Rate D", "Rate D 2"),
        ]
        for n1, n2 in cases:
            kept, _ = tp.dedupe_same_plan_variants(
                [self._flat(n1, 0.12), self._flat(n2, 0.12)], "Example Power",
            )
            self.assertEqual(len(kept), 2, (n1, n2))

    def test_future_dated_successor_kept(self):
        a = self._flat("Residential Service", 0.12)
        b = self._flat("Residential Service", 0.13)
        a.effective_date = "2026-01-01"
        b.effective_date = (date.today() + timedelta(days=200)).isoformat()
        kept, _ = tp.dedupe_same_plan_variants([a, b], "Example Power")
        self.assertEqual(len(kept), 2)


class PedernalesRenewableVariant(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_r17_slip_repaired_to_tariff_values(self):
        fx = _load("890")
        kept, info = tp.reconcile_same_utility_plans(_plans("890"), fx["utility_name"])
        var = _by_name(kept, "with Renewable Energy Rider")
        self.assertEqual(_cents(var), [8.714, 13.011, 13.683, 20.551])
        self.assertEqual(info["variant_prices_repaired"], 1)
        self.assertFalse(var.needs_review)
        self.assertNotIn("optional_plan_below_base_unexplained", var.missing_fields or [])
        note = var.confidence_notes["variant_price_repaired"]
        self.assertEqual(note["old_to_new"]["0.067145"], 0.087145)
        base = [t for t in kept if t.name.endswith("(TOU) Base Power Charge")][0]
        self.assertEqual(_cents(base), [8.671, 12.968, 13.64, 20.508])

    def test_matches_golden_expected(self):
        golden = json.loads(
            (Path(__file__).resolve().parent / "fixtures" / "r11" / "golden"
             / "EXPECTED_ENERGY_CENTS.json").read_text()
        )["890"]
        fx = _load("890")
        rep, valid = tp.phase4_validate(_plans("890"), fx["utility_name"], fx["state"])
        var = _by_name(valid, "with Renewable Energy Rider")
        self.assertEqual(
            _cents(var),
            golden["Residential, Farm and Ranch Service, Time of Use (TOU) "
                   "Base Power Charge, with Renewable Energy Rider"],
        )

    def _pair(self, var_scale: float, credit: bool = False):
        base, var = _plans("890")[:2]
        for c in var.components:
            if c.get("component_type") == "energy":
                c["rate_value"] = round(float(c["rate_value"]) * var_scale, 6)
        if credit:
            var.components.append({
                "component_type": "adjustment", "rate_value": -0.03,
                "unit": "$/kWh", "included_in_energy": True,
                "tier_label": "Renewable credit",
            })
        return base, var

    def test_guard_flags_unexplained_below_base(self):
        # Every period low (no anchor → no repair) → guard must flag.
        base, var = self._pair(0.5)
        kept, info = tp.reconcile_same_utility_plans([base, var], "PEC")
        v = _by_name(kept, "with Renewable")
        self.assertTrue(v.needs_review)
        self.assertIn("optional_plan_below_base_unexplained", v.missing_fields)
        self.assertEqual(info["optional_below_base_flagged"], 1)

    def test_guard_respects_explained_credit(self):
        base, var = self._pair(0.5, credit=True)
        kept, info = tp.reconcile_same_utility_plans([base, var], "PEC")
        v = _by_name(kept, "with Renewable")
        self.assertNotIn("optional_plan_below_base_unexplained", v.missing_fields or [])
        self.assertNotIn("optional_below_base_flagged", info)


class NspUnextractedPlanHint(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_murb_reported(self):
        fx = _load("1739")
        plans = _plans("1739")
        rep, valid = tp.phase4_validate(plans + _riders("1739"), fx["utility_name"], fx["state"])
        self.assertEqual(rep["valid"], 6)
        self.assertEqual(rep["plans_possibly_not_extracted"], ["MURB"])
        self.assertEqual(rep.get("duplicates_dropped"), 0)

    def test_no_hint_when_plan_present(self):
        fx = _load("1739")
        plans = _plans("1739")
        murb = copy.deepcopy(plans[-1])
        murb.name = "Domestic Service MURB Time of Use Tariff"
        rep, _ = tp.phase4_validate(
            plans + [murb] + _riders("1739"), fx["utility_name"], fx["state"],
        )
        self.assertNotIn("plans_possibly_not_extracted", rep)


if __name__ == "__main__":
    unittest.main()
