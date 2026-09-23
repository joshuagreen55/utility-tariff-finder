"""Unit tests for relative seasonal rider → all-in ENERGY expansion
and NL Rate #1.1S repair helpers (Newfoundland Power + NL Hydro).

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


def _hydro_b_nl_like_row(tariff_id: int = 65858) -> SimpleNamespace:
    """Observed NL Hydro Flux-invisible shape (audit B_nl_like)."""
    return SimpleNamespace(
        id=tariff_id,
        name="1.1S Domestic Optional",
        code="1.1S",
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
        source_url="https://nlhydro.com/example/2025.pdf",
        last_verified_at=None,
        utility_id=42,
    )


def _fixed_2026_keeper(tariff_id: int = 67028) -> SimpleNamespace:
    target = repair.build_nf_11s_all_in_components(0.15587)
    return SimpleNamespace(
        id=tariff_id,
        name=repair.NF_POWER_11S_NAME,
        code="1.1S",
        effective_date=repair.NF_11S_EFFECTIVE,
        rate_components=[
            SimpleNamespace(
                component_type="energy",
                unit=c["unit"],
                rate_value=c["rate_value"],
                season=c["season"],
            )
            for c in target
        ],
        superseded_by_tariff_id=None,
        supersede_reason=None,
        source_url="https://example/2026.pdf",
        last_verified_at=None,
        utility_id=7,
    )


def _live_2026_base_11(utility_id: int = 42) -> SimpleNamespace:
    return SimpleNamespace(
        id=9001,
        name="Rate No. 1.1 Domestic",
        code="1.1",
        effective_date=repair.NF_11S_EFFECTIVE,
        rate_components=[
            SimpleNamespace(
                component_type="energy",
                unit="$/kWh",
                rate_value=0.15587,
                season=None,
            )
        ],
        superseded_by_tariff_id=None,
        supersede_reason=None,
        source_url=repair.NF_HYDRO_SOURCE_URL,
        last_verified_at=None,
        utility_id=utility_id,
    )


class _FakeScalars:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return _FakeScalars(self._rows)


class _FakeSession:
    """Minimal session stub for repair_utility (no real DB)."""

    def __init__(self, live_tariffs):
        self.live_tariffs = list(live_tariffs)
        self.added = []
        self._next_id = 88000

    def execute(self, _stmt):
        return _FakeResult(self.live_tariffs)

    def add(self, obj):
        self.added.append(obj)

    def flush(self):
        for obj in self.added:
            if getattr(obj, "id", None) is None:
                obj.id = self._next_id
                self._next_id += 1


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
        hydro = SimpleNamespace(name="1.1S Domestic Optional", code="1.1S")
        hydro_optional = SimpleNamespace(
            name="Domestic - Optional", code="1.1S"
        )
        base = SimpleNamespace(
            name="Rate #1.1 Domestic Service", code="1.1"
        )
        hydro_base = SimpleNamespace(
            name="Rate No. 1.1 Domestic", code="1.1"
        )
        tou = SimpleNamespace(
            name="Rate #1.1 Domestic TOU", code="1.1"
        )
        self.assertTrue(repair.is_nf_11s_candidate(seasonal))
        self.assertTrue(repair.is_nf_11s_candidate(coded))
        self.assertTrue(repair.is_nf_11s_candidate(hydro))
        self.assertTrue(repair.is_nf_11s_candidate(hydro_optional))
        self.assertTrue(repair.is_nf_11_base(base))
        self.assertTrue(repair.is_nf_11_base(hydro_base))
        self.assertFalse(repair.is_nf_11_base(tou))
        self.assertFalse(repair.is_nf_11s_candidate(base))

    def test_hydro_display_name(self):
        self.assertEqual(
            repair.nf_11s_display_name("Newfoundland and Labrador Hydro"),
            repair.NF_HYDRO_11S_NAME,
        )
        self.assertEqual(
            repair.nf_11s_display_name("Newfoundland Power"),
            repair.NF_POWER_11S_NAME,
        )

    def test_stale_row_does_not_match_2026_target(self):
        """Dry-run decision input: observed bad 2025 shape vs 2026 target."""
        target = repair.build_nf_11s_all_in_components(0.15587)
        stale = _hydro_b_nl_like_row(60238)
        stale.name = "Domestic Seasonal - Optional"
        stale.code = None
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

    def test_hydro_stale_row_does_not_match_target(self):
        target = repair.build_nf_11s_all_in_components(0.15587)
        stale = _hydro_b_nl_like_row(65858)
        self.assertTrue(repair.is_nf_11s_candidate(stale))
        self.assertFalse(repair._components_match_target(stale, target))


class TestNf11sRepairUtilityPaths(unittest.TestCase):
    """dry-run / apply / already-fixed for Hydro-shaped #1.1S."""

    def test_dry_run_hydro_creates_and_supersedes_counts(self):
        utility = SimpleNamespace(
            id=42, name="Newfoundland and Labrador Hydro"
        )
        session = _FakeSession(
            [_hydro_b_nl_like_row(65858), _live_2026_base_11(42)]
        )
        result = repair.repair_utility(session, utility, dry_run=True)
        self.assertFalse(result["skipped"])
        self.assertEqual(result["created"], 1)
        self.assertEqual(result["updated"], 0)
        self.assertEqual(result["superseded"], 1)
        self.assertEqual(result["base_energy"], 0.15587)
        by_season = {
            tp._season_key(c["season"]): round(float(c["rate_value"]), 5)
            for c in result["target_components"]
        }
        self.assertEqual(by_season["winter"], 0.16540)
        self.assertEqual(by_season["non-winter"], 0.14290)
        # Dry-run must not write.
        self.assertEqual(session.added, [])
        stale = session.live_tariffs[0]
        self.assertIsNone(stale.superseded_by_tariff_id)
        self.assertIsNone(stale.supersede_reason)

    def test_apply_hydro_soft_supersedes_vintage(self):
        utility = SimpleNamespace(
            id=42, name="Newfoundland and Labrador Hydro"
        )
        stale = _hydro_b_nl_like_row(65858)
        session = _FakeSession([stale, _live_2026_base_11(42)])
        result = repair.repair_utility(session, utility, dry_run=False)
        self.assertEqual(result["created"], 1)
        self.assertEqual(result["superseded"], 1)
        self.assertEqual(len(session.added), 1)
        keeper = session.added[0]
        self.assertEqual(keeper.name, repair.NF_HYDRO_11S_NAME)
        self.assertEqual(keeper.code, "1.1S")
        self.assertEqual(keeper.effective_date, repair.NF_11S_EFFECTIVE)
        self.assertEqual(stale.supersede_reason, "vintage")
        self.assertEqual(stale.superseded_by_tariff_id, keeper.id)
        energy = [
            (round(float(rc.rate_value), 5), (rc.season or "").lower())
            for rc in keeper.rate_components
        ]
        self.assertEqual(len(energy), 2)
        seasons = {s for _, s in energy}
        self.assertTrue(any("winter" in s and "non" not in s for s in seasons))
        self.assertTrue(any("non-winter" in s for s in seasons))
        rates = {round(r, 5) for r, _ in energy}
        self.assertEqual(rates, {0.16540, 0.14290})

    def test_already_fixed_is_idempotent(self):
        utility = SimpleNamespace(id=7, name="Newfoundland Power")
        keeper = _fixed_2026_keeper(67028)
        session = _FakeSession([keeper])
        result = repair.repair_utility(session, utility, dry_run=False)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["updated"], 0)
        self.assertEqual(result["superseded"], 0)
        self.assertEqual(result["keeper_id"], 67028)
        self.assertEqual(session.added, [])
        self.assertIsNone(keeper.superseded_by_tariff_id)
        self.assertIsNone(keeper.supersede_reason)

    def test_already_fixed_hydro_dry_run(self):
        utility = SimpleNamespace(
            id=42, name="Newfoundland and Labrador Hydro"
        )
        target = repair.build_nf_11s_all_in_components(0.15587)
        keeper = SimpleNamespace(
            id=99111,
            name=repair.NF_HYDRO_11S_NAME,
            code="1.1S",
            effective_date=repair.NF_11S_EFFECTIVE,
            rate_components=[
                SimpleNamespace(
                    component_type="energy",
                    unit=c["unit"],
                    rate_value=c["rate_value"],
                    season=c["season"],
                )
                for c in target
            ],
            superseded_by_tariff_id=None,
            supersede_reason=None,
            source_url=repair.NF_HYDRO_SOURCE_URL,
            last_verified_at=None,
            utility_id=42,
        )
        session = _FakeSession([keeper])
        result = repair.repair_utility(session, utility, dry_run=True)
        self.assertEqual(result["created"], 0)
        self.assertEqual(result["superseded"], 0)
        self.assertEqual(session.added, [])


if __name__ == "__main__":
    unittest.main()
