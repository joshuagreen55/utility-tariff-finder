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

    def test_cpp_interim_tracks_domestic(self):
        cpp = repair.build_domestic_cpp_interim_components()
        cpp_e = [c for c in cpp if c["component_type"] == "energy"][0]
        self.assertEqual(cpp_e["rate_value"], repair.NS_DOMESTIC_ALL_IN)

    def test_tou_seasonal_energy_charge_not_interim_flat(self):
        comps = repair.build_domestic_tou_seasonal_components()
        energy = [c for c in comps if c["component_type"] == "energy"]
        fixed = [c for c in comps if c["component_type"] == "fixed"]
        self.assertEqual(fixed[0]["rate_value"], 20.08)
        # 1 non-winter + 4 winter periods (not a single interim row)
        self.assertEqual(len(energy), 5)
        nw = next(c for c in energy if "Non-winter" in c["season"])
        self.assertEqual(nw["period_label"], "All hours")
        # 12.860 + 0.804 = 13.664 ¢
        self.assertEqual(nw["rate_value"], 0.13664)
        self.assertEqual(nw["rate_value"], repair.all_in_domestic(12.860))
        winter = [c for c in energy if c["season"].startswith("Winter")]
        self.assertEqual(len(winter), 4)
        onpeak = [c for c in winter if "On-peak" in c["period_label"]]
        offpeak = [c for c in winter if "Off-peak" in c["period_label"]]
        self.assertEqual(len(onpeak), 2)
        self.assertEqual(len(offpeak), 2)
        # 36.517 + 0.804 = 37.321 ¢; 18.324 + 0.804 = 19.128 ¢
        self.assertEqual(onpeak[0]["rate_value"], 0.37321)
        self.assertEqual(offpeak[0]["rate_value"], 0.19128)
        # Jan 1 2027 escalate column must not appear
        future_on = repair.all_in_domestic(38.281)
        future_off = repair.all_in_domestic(19.067)
        self.assertNotIn(future_on, {c["rate_value"] for c in energy})
        self.assertNotIn(future_off, {c["rate_value"] for c in energy})

    def test_tou_interim_shape_does_not_match_seasonal_target(self):
        """PR #6 flat interim (prod 67033) must fail the seasonal match."""
        interim = repair.build_domestic_tou_interim_components()
        seasonal = repair.build_domestic_tou_seasonal_components()
        fake = SimpleNamespace(
            effective_date=repair.NS_EFFECTIVE,
            rate_components=[
                SimpleNamespace(
                    component_type=c["component_type"],
                    rate_value=c["rate_value"],
                    season=c.get("season"),
                    period_label=c.get("period_label"),
                )
                for c in interim
            ],
        )
        self.assertFalse(repair.components_match_target(fake, seasonal))

    def test_tou_plan_is_seasonal_tou(self):
        meta = repair.NS_RESIDENTIAL_PLANS["tou"]
        self.assertEqual(meta["rate_type"].value, "seasonal_tou")
        self.assertIs(meta["build"], repair.build_domestic_tou_seasonal_components)
        self.assertEqual(repair.KNOWN_STALE_LIVE["tou"]["id"], 67033)
        self.assertEqual(repair.KNOWN_STALE_LIVE["tou"]["code"], "80")

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

    def test_green_power_standin_maps_to_domestic(self):
        # Prod id 60200 is Green Power mis-baked as Domestic base.
        self.assertEqual(
            repair.classify_residential_plan(
                self._t(
                    "Domestic Service Tariff Optional Green Power Rider",
                    "02, 03, 04",
                )
            ),
            "domestic",
        )

    def test_prod_codes_c_d_murb(self):
        self.assertEqual(
            repair.classify_residential_plan(
                self._t("Domestic Service Critical Peak Pricing Tariff", "C")
            ),
            "cpp",
        )
        self.assertEqual(
            repair.classify_residential_plan(
                self._t("Domestic Service Time of Use Tariff", "D")
            ),
            "tou",
        )
        self.assertEqual(
            repair.classify_residential_plan(
                self._t(
                    "Multi-Unit Residential Buildings Time-of-Use Tariff",
                    "MURB",
                )
            ),
            "murb",
        )

    def test_known_stale_live_ids(self):
        self.assertEqual(repair.NS_POWER_UTILITY_ID, 1739)
        self.assertEqual(repair.KNOWN_STALE_LIVE["domestic"]["id"], 60200)
        self.assertEqual(repair.KNOWN_STALE_LIVE["cpp"]["id"], 46886)
        # Post-PR #6 flat-interim keeper for code 80 (not pre-PR #6 46887)
        self.assertEqual(repair.KNOWN_STALE_LIVE["tou"]["id"], 67033)
        self.assertEqual(repair.KNOWN_STALE_LIVE["tou"]["code"], "80")
        self.assertEqual(repair.KNOWN_STALE_LIVE["tod"]["id"], 60201)
        self.assertEqual(repair.KNOWN_STALE_LIVE["murb"]["id"], 60202)


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

    def test_stale_20250326_book_replaced(self):
        primary, _ = tp.resolve_preferred_rate_page(
            "Nova Scotia Power",
            existing_url=(
                "https://nspower.ca/docs/default-source/regulatory/"
                "tariff-book-20250326.pdf"
            ),
        )
        self.assertIn("tariff-book-2026.pdf", primary)

    def test_clip_component_strings_prevents_truncation_crash(self):
        long_season = "Non-winter Period April 1 through October 31 all hours " + (
            "x" * 40
        )
        self.assertGreater(len(long_season), tp._RC_SEASON_MAX)
        unit, tier, period, season = tp._clip_component_strings(
            {
                "unit": "$/kWh",
                "season": long_season,
                "period_label": "On-peak (morning) 7:00 am - 11:00 am weekday",
                "tier_label": "ok",
            },
            tariff_name="Domestic TOU",
        )
        self.assertLessEqual(len(season), tp._RC_SEASON_MAX)
        self.assertTrue(season.endswith("…") or len(season) <= tp._RC_SEASON_MAX)
        self.assertEqual(unit, "$/kWh")
        self.assertEqual(tier, "ok")


class TestInterimVsEnergyChargePrompts(unittest.TestCase):
    def test_main_extraction_prompt_prefers_energy_charge(self):
        self.assertIn("INTERIM vs APPROVED ENERGY CHARGE", tp.EXTRACTION_PROMPT)
        self.assertIn("Example 6", tp.EXTRACTION_PROMPT)
        self.assertIn("seasonal_tou", tp.EXTRACTION_PROMPT)
        self.assertIn("Do NOT emit a single flat interim ENERGY", tp.EXTRACTION_PROMPT)

    def test_vision_and_twopass_prompts(self):
        for prompt in (
            tp.PAGE_SCREENSHOT_EXTRACTION_PROMPT_BASE,
            tp.PDF_VISION_EXTRACTION_PROMPT_BASE,
            tp.TWOPASS_EXTRACT_PROMPT,
        ):
            self.assertIn("Interim vs approved Energy Charge", prompt)

    def test_tou_season_strings_fit_varchar(self):
        self.assertLessEqual(len(repair.NS_TOU_NONWINTER_SEASON), tp._RC_SEASON_MAX)
        self.assertLessEqual(len(repair.NS_TOU_WINTER_SEASON), tp._RC_SEASON_MAX)
        for c in repair.build_domestic_tou_seasonal_components():
            if c.get("period_label"):
                self.assertLessEqual(len(c["period_label"]), tp._RC_PERIOD_LABEL_MAX)
            if c.get("season"):
                self.assertLessEqual(len(c["season"]), tp._RC_SEASON_MAX)


class TestDryRunNarrativeRate80(unittest.TestCase):
    """Simulate prod 67033 flat interim → seasonal CREATE+SUPERSEDE narrative."""

    def test_before_after_numbers(self):
        before = repair.build_domestic_tou_interim_components()
        after = repair.build_domestic_tou_seasonal_components()
        before_e = [c for c in before if c["component_type"] == "energy"]
        after_e = [c for c in after if c["component_type"] == "energy"]
        self.assertEqual(len(before_e), 1)
        self.assertEqual(before_e[0]["rate_value"], 0.19128)
        self.assertEqual(len(after_e), 5)
        by_key = {
            (c["season"], c["period_label"]): c["rate_value"] for c in after_e
        }
        self.assertEqual(
            by_key[(repair.NS_TOU_NONWINTER_SEASON, "All hours")], 0.13664
        )
        self.assertEqual(
            by_key[
                (repair.NS_TOU_WINTER_SEASON, "On-peak morning (7am–11am)")
            ],
            0.37321,
        )
        self.assertEqual(
            by_key[
                (repair.NS_TOU_WINTER_SEASON, "Off-peak midday (11am–5pm)")
            ],
            0.19128,
        )


if __name__ == "__main__":
    unittest.main()
