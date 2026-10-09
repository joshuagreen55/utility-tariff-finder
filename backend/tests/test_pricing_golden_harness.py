"""PR D / R28-5: golden harness exact-match accuracy + no live answer-key leak."""
from __future__ import annotations

import unittest

from app.services.pricing.inventory_from_docs import InventoryRider
from scripts.pricing_golden_harness import (
    _inventory_for_plan,
    build_real_llm_extract_prompt,
    run_harness,
)


class TestPricingGoldenHarness(unittest.TestCase):
    def test_offline_perfect_accuracy(self):
        report = run_harness(mode="offline")
        self.assertGreaterEqual(report.scored, 40)
        self.assertEqual(report.matched, report.scored)
        self.assertEqual(report.accuracy, 1.0)
        self.assertEqual(report.held, 0)
        self.assertGreaterEqual(report.clocks_scored, 5)
        self.assertEqual(report.clocks_matched, report.clocks_scored)

    def test_live_dry_extract_perfect_when_forced(self):
        report = run_harness(mode="live", force_extract=True)
        self.assertGreaterEqual(report.scored, 40)
        self.assertEqual(report.matched, report.scored, [
            (s.plan_key, s.hold_reason, s.error, s.official, s.compiled)
            for s in report.plans if not s.matched
        ])
        self.assertEqual(report.accuracy, 1.0)
        self.assertGreaterEqual(report.clocks_scored, 5)
        self.assertEqual(report.clocks_matched, report.clocks_scored)

    def test_live_respects_feature_flag_off(self):
        report = run_harness(mode="live", force_extract=False)
        # Default flag off → all held/skipped, accuracy denominator empty or 0 matched.
        self.assertTrue(
            report.scored == 0 or report.matched == 0 or report.skipped == report.total
        )


class TestNoGoldenLeakInRealLlm(unittest.TestCase):
    """Real-LLM path must not embed golden component codes/names/units."""

    def test_prompt_omits_golden_component_codes(self):
        plan_raw = {
            "utility_name": "Example Power",
            "name": "Residential",
            "code": "RS",
            "rate_type": "flat",
            "recipe_code": "bundled",
            "components": [
                {
                    "code": "secret_golden_energy",
                    "kind": "base_energy",
                    "unit": "¢/kWh",
                    "name": "Secret Golden Energy",
                    "disposition": "applies",
                    "cells": [{"amount": "12.5"}],
                },
                {
                    "code": "secret_golden_fuel",
                    "kind": "rider_per_kwh",
                    "unit": "¢/kWh",
                    "name": "Secret Fuel Rider",
                    "disposition": "applies",
                    "cells": [{"amount": "0.4"}],
                },
            ],
        }
        prompt = build_real_llm_extract_prompt(
            plan_raw=plan_raw,
            document="Energy Charge 12.5 ¢/kWh\n",
            inventory=[InventoryRider(code="doc_fuel", name="Fuel", kind="rider_per_kwh")],
        )
        self.assertNotIn("secret_golden_energy", prompt)
        self.assertNotIn("secret_golden_fuel", prompt)
        self.assertNotIn("Secret Golden Energy", prompt)
        self.assertNotIn("Component inventory (no values given)", prompt)
        self.assertIn("Discover every priced charge", prompt)
        self.assertIn("doc_fuel", prompt)  # document-derived inventory OK
        self.assertIn("Example Power", prompt)
        self.assertIn("Residential", prompt)

    def test_inventory_without_golden_seed(self):
        raw = {
            "components": [
                {
                    "code": "golden_only_rider",
                    "kind": "rider_per_kwh",
                    "unit": "¢/kWh",
                    "name": "Golden Only",
                    "disposition": "applies",
                },
            ],
            "document_set": {
                "documents": [
                    {
                        "role": "rider_sheet",
                        "url": "https://example.com/rates/rider-pca.pdf",
                        "title": "PCA Rider",
                        "is_selected": True,
                    },
                ],
            },
            "source_urls": ["https://example.com/rates/tariff.pdf"],
        }
        inv, disps, _typ = _inventory_for_plan(
            raw,
            from_document_set=True,
            document_text="Subject to Rider PCA.\n",
            seed_from_golden=False,
        )
        codes = {r.code for r in inv}
        self.assertNotIn("golden_only_rider", codes)
        self.assertEqual(disps, [])
        # Document-derived PCA should appear (sheet and/or text).
        self.assertTrue(
            any("pca" in c for c in codes),
            f"expected pca-like code in {codes}",
        )


if __name__ == "__main__":
    unittest.main()
