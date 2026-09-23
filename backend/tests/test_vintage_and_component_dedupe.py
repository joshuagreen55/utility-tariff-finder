"""Unit tests for Jobs A+B: vintage soft-supersede helpers + component dedupe.

Runnable without a database:

    cd backend && python3 -m unittest tests.test_vintage_and_component_dedupe -v
"""
from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import date, datetime, timezone

from scripts import tariff_pipeline as tp


class TestRatebookCodes(unittest.TestCase):
    def test_hash_rate_code(self):
        self.assertEqual(tp.extract_ratebook_codes("Rate #1.1 Domestic Service"), {"1.1"})
        self.assertEqual(
            tp.extract_ratebook_codes("Rate #1.1 Domestic Service", "1.1"), {"1.1"}
        )

    def test_code_column_only(self):
        self.assertEqual(tp.extract_ratebook_codes("Domestic Service", "1.1"), {"1.1"})

    def test_flat_name_has_no_code(self):
        self.assertEqual(tp.extract_ratebook_codes("Domestic Service (Flat)"), set())


class TestSameVintageProduct(unittest.TestCase):
    def test_nf_rate_11_vs_flat_sibling(self):
        self.assertTrue(
            tp.same_vintage_product(
                "Rate #1.1 Domestic Service",
                "Domestic Service (Flat)",
                code_a="1.1",
                rate_type_a="flat",
                rate_type_b="flat",
            )
        )

    def test_nf_rate_11_vs_plain_domestic(self):
        self.assertTrue(
            tp.same_vintage_product(
                "Rate #1.1 Domestic Service",
                "Domestic Service",
                code_a="1.1",
                rate_type_a="flat",
                rate_type_b="flat",
            )
        )

    def test_does_not_merge_tou_vs_flat_rate_types(self):
        self.assertFalse(
            tp.same_vintage_product(
                "Residential TOU",
                "Residential Flat",
                rate_type_a="tou",
                rate_type_b="flat",
            )
        )

    def test_does_not_merge_same_code_when_name_has_tou(self):
        # Shared #1.1 must not collapse TOU into the flat domestic product.
        self.assertFalse(
            tp.same_vintage_product(
                "Rate #1.1 Domestic TOU",
                "Rate #1.1 Domestic Service",
                code_a="1.1",
                code_b="1.1",
                rate_type_a="flat",
                rate_type_b="flat",
            )
        )

    def test_does_not_merge_heating_via_one_sided_code(self):
        self.assertFalse(
            tp.same_vintage_product(
                "Rate #1.1 Domestic Service",
                "Domestic Space Heating",
                code_a="1.1",
                rate_type_a="flat",
                rate_type_b="flat",
            )
        )

    def test_does_not_widen_likely_same_for_tou_vs_flat(self):
        # Job A must NOT change tariffs_likely_same discriminators.
        self.assertFalse(tp.tariffs_likely_same("Rate D1 TOU", "Rate D1 Flat"))

    def test_optional_vs_standard_still_distinct_in_likely_same(self):
        self.assertFalse(
            tp.tariffs_likely_same(
                "Optional Residential Service", "Residential Service"
            )
        )


class TestVintageKeeper(unittest.TestCase):
    def test_keeper_is_newest_effective_date(self):
        @dataclass
        class T:
            id: int
            name: str
            code: str | None = None
            rate_type: str = "flat"
            effective_date: date | None = None
            last_verified_at: datetime | None = None

        old = T(
            1,
            "Domestic Service (Flat)",
            None,
            "flat",
            date(2025, 7, 1),
            datetime(2025, 8, 1, tzinfo=timezone.utc),
        )
        new = T(
            2,
            "Rate #1.1 Domestic Service",
            "1.1",
            "flat",
            date(2026, 7, 1),
            datetime(2026, 7, 2, tzinfo=timezone.utc),
        )
        groups = tp.group_live_tariffs_by_vintage([old, new])
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0]), 2)
        keeper = tp.choose_vintage_keeper(groups[0])
        self.assertEqual(keeper.id, 2)


class TestDedupeRateComponents(unittest.TestCase):
    def test_collapses_equal_fixed_and_minimum_amp_tiers(self):
        comps = [
            {
                "component_type": "fixed",
                "unit": "$/month",
                "rate_value": 17.36,
                "tier_label": "0-10 Amp",
            },
            {
                "component_type": "minimum",
                "unit": "$/month",
                "rate_value": 17.36,
                "tier_label": "Basic Customer Charge (0-10 Amp)",
            },
            {
                "component_type": "fixed",
                "unit": "$/month",
                "rate_value": 22.01,
                "tier_label": "11-100 Amp",
            },
            {
                "component_type": "minimum",
                "unit": "$/month",
                "rate_value": 22.01,
                "tier_label": "11-100 Amp",
            },
            {
                "component_type": "energy",
                "unit": "$/kWh",
                "rate_value": 0.15587,
            },
            {
                "component_type": "energy",
                "unit": "$/kWh",
                "rate_value": 0.15587,
            },
        ]
        out = tp.dedupe_rate_components(comps)
        self.assertEqual(len(out), 3)
        types = [c["component_type"] for c in out]
        self.assertEqual(types.count("fixed"), 2)
        self.assertEqual(types.count("minimum"), 0)
        self.assertEqual(types.count("energy"), 1)

    def test_prefers_fixed_when_minimum_comes_first(self):
        comps = [
            {
                "component_type": "minimum",
                "unit": "$/month",
                "rate_value": 17.36,
                "tier_label": "0-10 Amp",
            },
            {
                "component_type": "fixed",
                "unit": "$/month",
                "rate_value": 17.36,
                "tier_label": "0-10 Amp",
            },
        ]
        out = tp.dedupe_rate_components(comps)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["component_type"], "fixed")

    def test_keeps_distinct_amp_tiers(self):
        comps = [
            {
                "component_type": "fixed",
                "unit": "$/month",
                "rate_value": 17.36,
                "tier_label": "0-10 Amp",
            },
            {
                "component_type": "fixed",
                "unit": "$/month",
                "rate_value": 22.01,
                "tier_label": "11-100 Amp",
            },
        ]
        out = tp.dedupe_rate_components(comps)
        self.assertEqual(len(out), 2)


if __name__ == "__main__":
    unittest.main()
