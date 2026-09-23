"""Unit tests for relative seasonal rider → all-in ENERGY expansion
and Newfoundland Power Rate #1.1S repair helpers.

Runnable without a database:

    cd backend && python3 -m unittest tests.test_seasonal_relative_rider -v
"""
from __future__ import annotations

import unittest
from datetime import date
from types import SimpleNamespace

from scripts import tariff_pipeline as tp
from scripts import repair_nf_seasonal_11s as repair


class TestExpandRelativeSeasonalEnergy(unittest.TestCase):
    def test_nf_11s_style_base_plus_adjustments(self):
        comps = [
            {
                "component_type": "energy",
                "unit": "$/kWh",
                "rate_value": 0.15587,
            },
            {
                "component_type": "adjustment",
                "unit": "$/kWh",
                "rate_value": 0.00953,
                "season": "Winter (Dec–Apr)",
            },
            {
                "component_type": "adjustment",
                "unit": "$/kWh",
                "rate_value": -0.01297,
                "season": "Non-Winter (May–Nov)",
            },
        ]
        out = tp.expand_relative_seasonal_energy(comps)
        energy = [
            c for c in out if c.get("component_type") == "energy"
        ]
        self.assertEqual(tp.count_energy_seasons(out), 2)
        by_season = {
            tp._season_key(c.get("season")): round(float(c["rate_value"]), 5)
            for c in energy
        }
        self.assertEqual(by_season["winter"], 0.16540)
        self.assertEqual(by_season["non-winter"], 0.14290)
        # Adjustments retained for audit by default.
        adjs = [c for c in out if c.get("component_type") == "adjustment"]
        self.assertEqual(len(adjs), 2)

    def test_mislabeled_winter_energy_plus_adjustments(self):
        # Observed bad shape: ENERGY tagged Winter at base rate, plus
        # Winter/Non-Winter ADJUSTMENTs, no Non-Winter ENERGY.
        comps = [
            {
                "component_type": "energy",
                "unit": "$/kWh",
                "rate_value": 0.15213,
                "season": "Winter",
            },
            {
                "component_type": "adjustment",
                "unit": "$/kWh",
                "rate_value": 0.00953,
                "season": "Winter",
            },
            {
                "component_type": "adjustment",
                "unit": "$/kWh",
                "rate_value": -0.01297,
                "season": "Non-Winter",
            },
        ]
        out = tp.expand_relative_seasonal_energy(comps, keep_adjustments=False)
        energy = [
            c for c in out if c.get("component_type") == "energy"
        ]
        self.assertEqual(len(energy), 2)
        self.assertEqual(tp.count_energy_seasons(out), 2)
        by_season = {
            tp._season_key(c.get("season")): round(float(c["rate_value"]), 5)
            for c in energy
        }
        self.assertEqual(by_season["winter"], 0.16166)
        self.assertEqual(by_season["non-winter"], 0.13916)
        self.assertFalse(
            any(c.get("component_type") == "adjustment" for c in out)
        )

    def test_noop_without_seasonal_adjustments(self):
        comps = [
            {
                "component_type": "energy",
                "unit": "$/kWh",
                "rate_value": 0.12,
                "season": "Summer",
            },
            {
                "component_type": "energy",
                "unit": "$/kWh",
                "rate_value": 0.10,
                "season": "Winter",
            },
        ]
        out = tp.expand_relative_seasonal_energy(comps)
        self.assertIs(out, comps)

    def test_prompt_documents_relative_seasonal_rule(self):
        self.assertIn("RELATIVE SEASONAL RIDERS", tp.EXTRACTION_PROMPT)
        self.assertIn("all-in", tp.EXTRACTION_PROMPT.lower())
        self.assertIn("1.1S", tp.EXTRACTION_PROMPT)
        self.assertIn(
            "never leave a season as ADJUSTMENT-only",
            tp.PAGE_SCREENSHOT_EXTRACTION_PROMPT_BASE,
        )
        self.assertIn(
            "never leave a season as ADJUSTMENT-only",
            tp.PDF_VISION_EXTRACTION_PROMPT_BASE,
        )
        self.assertIn(
            "never leave a season as ADJUSTMENT-only",
            tp.TWOPASS_EXTRACT_PROMPT,
        )


class TestNf11sRepairHelpers(unittest.TestCase):
    def test_build_all_in_components(self):
        comps = repair.build_nf_11s_all_in_components()
        self.assertEqual(len(comps), 2)
        self.assertEqual(tp.count_energy_seasons(comps), 2)
        by_season = {
            tp._season_key(c["season"]): round(float(c["rate_value"]), 5)
            for c in comps
        }
        self.assertEqual(by_season["winter"], 0.16540)
        self.assertEqual(by_season["non-winter"], 0.14290)
        self.assertIn("Dec", comps[0]["season"])
        self.assertIn("May", comps[1]["season"])

    def test_candidate_matchers(self):
        seasonal = SimpleNamespace(
            name="Domestic Seasonal - Optional", code=None
        )
        coded = SimpleNamespace(name="Something", code="1.1S")
        base = SimpleNamespace(
            name="Rate #1.1 Domestic Service", code="1.1"
        )
        tou = SimpleNamespace(
            name="Rate #1.1 Domestic TOU", code="1.1"
        )
        self.assertTrue(repair.is_nf_11s_candidate(seasonal))
        self.assertTrue(repair.is_nf_11s_candidate(coded))
        self.assertTrue(repair.is_nf_11_base(base))
        self.assertFalse(repair.is_nf_11_base(tou))
        self.assertFalse(repair.is_nf_11s_candidate(base))

    def test_stale_row_does_not_match_2026_target(self):
        """Dry-run decision input: observed bad 2025 shape vs 2026 target."""
        target = repair.build_nf_11s_all_in_components(0.15587)
        stale = SimpleNamespace(
            id=60238,
            name="Domestic Seasonal - Optional",
            code=None,
            effective_date=date(2025, 7, 1),
            rate_components=[
                SimpleNamespace(
                    component_type="energy",
                    unit="$/kWh",
                    rate_value=0.15213,
                    season="Winter",
                ),
                SimpleNamespace(
                    component_type="adjustment",
                    unit="$/kWh",
                    rate_value=0.00953,
                    season="Winter",
                ),
                SimpleNamespace(
                    component_type="adjustment",
                    unit="$/kWh",
                    rate_value=-0.01297,
                    season="Non-Winter",
                ),
            ],
            superseded_by_tariff_id=None,
            supersede_reason=None,
            source_url="https://example/20250701.pdf",
            last_verified_at=None,
        )
        self.assertFalse(repair._components_match_target(stale, target))
        as_dicts = [
            {
                "component_type": rc.component_type,
                "unit": rc.unit,
                "rate_value": rc.rate_value,
                "season": rc.season,
            }
            for rc in stale.rate_components
        ]
        expanded = tp.expand_relative_seasonal_energy(as_dicts)
        self.assertGreaterEqual(tp.count_energy_seasons(expanded), 2)


if __name__ == "__main__":
    unittest.main()
