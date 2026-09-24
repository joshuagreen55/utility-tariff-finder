"""Unit tests for NS Power May 2026 residential repair + stacking riders.

Runnable without a database:

    cd backend && python3 -m unittest tests.test_ns_power_residential_2026 -v
"""
from __future__ import annotations

import unittest
from datetime import date
from types import SimpleNamespace

from scripts import tariff_pipeline as tp
from scripts import repair_ns_power_residential_2026 as repair


class TestNsAllInMath(unittest.TestCase):
    def test_domestic_all_in_matches_gold(self):
        # 18.324 + 0.156 + 0.648 = 19.128 ¢ = $0.19128/kWh
        self.assertEqual(repair.NS_DOMESTIC_ALL_IN, 0.19128)
        self.assertEqual(repair.all_in_domestic(18.324), 0.19128)
        self.assertEqual(repair.NS_CUSTOMER_CHARGE, 20.08)

    def test_do_not_use_2027_column(self):
        # Jan 1 2027 Domestic base 19.067 ¢ must NOT be the live energy.
        future = repair.cents_to_dollars(19.067)
        self.assertNotEqual(future, repair.NS_DOMESTIC_BASE_ENERGY)
        self.assertNotEqual(
            repair.all_in_domestic(19.067), repair.NS_DOMESTIC_ALL_IN
        )

    def test_domestic_service_components(self):
        comps = repair.build_domestic_service_components()
        energy = [c for c in comps if c["component_type"] == "energy"]
        fixed = [c for c in comps if c["component_type"] == "fixed"]
        self.assertEqual(len(energy), 1)
        self.assertEqual(energy[0]["rate_value"], 0.19128)
        self.assertEqual(fixed[0]["rate_value"], 20.08)

    def test_tod_has_winter_and_nonwinter_periods(self):
        comps = repair.build_domestic_tod_components()
        energy = [c for c in comps if c["component_type"] == "energy"]
        self.assertEqual(len(energy), 6)  # 4 winter + 2 mar-nov
        winter_peak = next(
            c for c in energy
            if c["season"].startswith("Winter") and "Morning" in c["period_label"]
        )
        # 24.384 + 0.804 = 25.188 ¢
        self.assertEqual(winter_peak["rate_value"], repair.all_in_domestic(24.384))

    def test_cpp_and_tou_interim_track_domestic(self):
        cpp = repair.build_domestic_cpp_interim_components()
        tou = repair.build_domestic_tou_interim_components()
        cpp_e = [c for c in cpp if c["component_type"] == "energy"][0]
        tou_e = [c for c in tou if c["component_type"] == "energy"][0]
        self.assertEqual(cpp_e["rate_value"], repair.NS_DOMESTIC_ALL_IN)
        self.assertEqual(tou_e["rate_value"], repair.NS_DOMESTIC_ALL_IN)

    def test_murb_uses_general_class_riders(self):
        comps = repair.build_murb_tou_components()
        energy = [c for c in comps if c["component_type"] == "energy"]
        self.assertEqual(len(energy), 6)
        # Off-peak non-winter 11.826 + 0.207 + 0.749 = 12.782 ¢
        off = next(
            c for c in energy
            if "Non-Winter" in c["season"] and "Off-Peak" in c["period_label"]
        )
        self.assertEqual(off["rate_value"], repair.all_in_murb(11.826))


class TestClassifyPlan(unittest.TestCase):
    def _t(self, name, code=None):
        return SimpleNamespace(name=name, code=code)

    def test_five_plan_keys(self):
        self.assertEqual(
            repair.classify_residential_plan(
                self._t("Domestic Service Tariff", "02/03/04")
            ),
            "domestic",
        )
        self.assertEqual(
            repair.classify_residential_plan(
                self._t("Domestic Service Time-Of-Day Tariff (Optional)", "05/06")
            ),
            "tod",
        )
        self.assertEqual(
            repair.classify_residential_plan(
                self._t("Domestic Service Critical Peak Pricing Tariff", "70")
            ),
            "cpp",
        )
        self.assertEqual(
            repair.classify_residential_plan(
                self._t("Domestic Service Time of Use Tariff", "80")
            ),
            "tou",
        )
        self.assertEqual(
            repair.classify_residential_plan(
                self._t("Multi-Unit Residential Buildings Time of Use Tariff", "89")
            ),
            "murb",
        )

    def test_green_power_unclassified(self):
        self.assertIsNone(
            repair.classify_residential_plan(self._t("Optional Green Power Rider"))
        )


class TestExpandStackingEnergyRiders(unittest.TestCase):
    def test_fam_dsm_folded_into_energy(self):
        comps = [
            {
                "component_type": "energy",
                "unit": "$/kWh",
                "rate_value": 0.18324,
            },
            {
                "component_type": "adjustment",
                "unit": "$/kWh",
                "rate_value": 0.00156,
                "tier_label": "FAM AA/BA",
            },
            {
                "component_type": "adjustment",
                "unit": "$/kWh",
                "rate_value": 0.00648,
                "tier_label": "DSM DCRR",
            },
            {
                "component_type": "fixed",
                "unit": "$/month",
                "rate_value": 20.08,
            },
        ]
        out = tp.expand_stacking_energy_riders(comps)
        energy = [c for c in out if c["component_type"] == "energy"]
        self.assertEqual(len(energy), 1)
        self.assertEqual(energy[0]["rate_value"], 0.19128)
        # Adjustments retained for audit by default.
        adjs = [c for c in out if c["component_type"] == "adjustment"]
        self.assertEqual(len(adjs), 2)
        fixed = [c for c in out if c["component_type"] == "fixed"]
        self.assertEqual(fixed[0]["rate_value"], 20.08)

    def test_noop_without_adjustments(self):
        comps = [
            {
                "component_type": "energy",
                "unit": "$/kWh",
                "rate_value": 0.19128,
            }
        ]
        out = tp.expand_stacking_energy_riders(comps)
        self.assertEqual(out, comps)

    def test_applies_to_each_tou_energy_row(self):
        comps = [
            {
                "component_type": "energy",
                "unit": "$/kWh",
                "rate_value": 0.24384,
                "season": "Winter",
                "period_label": "On-Peak",
            },
            {
                "component_type": "energy",
                "unit": "$/kWh",
                "rate_value": 0.11632,
                "season": "Winter",
                "period_label": "Off-Peak",
            },
            {
                "component_type": "adjustment",
                "unit": "cents/kWh",
                "rate_value": 0.804,  # already cents? unit says cents
                "tier_label": "FAM+DSM",
            },
        ]
        # Unit still "cents/kWh" — expand uses raw rate_value; normalize
        # happens earlier in phase4. Simulate post-normalize dollars:
        comps[2]["unit"] = "$/kWh"
        comps[2]["rate_value"] = 0.00804
        out = tp.expand_stacking_energy_riders(comps, keep_adjustments=False)
        energy = [c for c in out if c["component_type"] == "energy"]
        self.assertEqual(len(energy), 2)
        by_period = {c["period_label"]: c["rate_value"] for c in energy}
        self.assertEqual(by_period["On-Peak"], 0.25188)
        self.assertEqual(by_period["Off-Peak"], 0.12436)
        self.assertFalse(any(c["component_type"] == "adjustment" for c in out))


class TestPreferredNsSource(unittest.TestCase):
    def test_preferred_url(self):
        url = tp.preferred_rate_page_url("Nova Scotia Power")
        self.assertIsNotNone(url)
        self.assertIn("tariff-book-2026.pdf", url)

    def test_resolve_replaces_marketing_hub(self):
        primary, alts = tp.resolve_preferred_rate_page(
            "Nova Scotia Power",
            existing_url="https://www.nspower.ca/about-us/electricity/rates-tariffs",
        )
        self.assertIn("tariff-book-2026.pdf", primary)
        self.assertTrue(alts)

    def test_override_wins(self):
        primary, _ = tp.resolve_preferred_rate_page(
            "Nova Scotia Power",
            existing_url="https://example.com/old.pdf",
            override_url="https://example.com/manual.pdf",
        )
        self.assertEqual(primary, "https://example.com/manual.pdf")

    def test_tariff_book_year(self):
        self.assertEqual(
            tp._tariff_book_year(
                "https://www.nspower.ca/docs/default-source/regulatory/tariff-book-2026.pdf"
            ),
            2026,
        )
        self.assertIsNone(tp._tariff_book_year("https://www.nspower.ca/rates"))

    def test_components_match_requires_effective_date(self):
        target = repair.build_domestic_service_components()
        fake = SimpleNamespace(
            effective_date=date(2025, 1, 1),
            rate_components=[
                SimpleNamespace(
                    component_type="energy",
                    rate_value=0.19128,
                    season=None,
                    period_label=None,
                ),
                SimpleNamespace(
                    component_type="fixed",
                    rate_value=20.08,
                    season=None,
                    period_label=None,
                ),
            ],
        )
        self.assertFalse(repair.components_match_target(fake, target))
        fake.effective_date = repair.NS_EFFECTIVE
        self.assertTrue(repair.components_match_target(fake, target))


if __name__ == "__main__":
    unittest.main()
