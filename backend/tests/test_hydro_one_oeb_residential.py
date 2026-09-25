"""Unit tests for Hydro One OEB residential RPP soft-repair.

Runnable without a database:

    cd backend && PYTHONPATH=. python3 -m unittest tests.test_hydro_one_oeb_residential -v
"""
from __future__ import annotations

import unittest
from datetime import date
from types import SimpleNamespace

from app.services.tou_seasonal_completeness import (
    evaluate_tariff_completeness,
    is_complete,
)
from scripts import repair_hydro_one_oeb_residential as repair
from scripts.scrape_oeb_rates import ULO_SCHEDULE


class TestOebGoldMath(unittest.TestCase):
    def test_tou_prices_match_nov_2025_consumer_page(self):
        # 9.8 / 15.7 / 20.3 ¢ → $/kWh
        self.assertEqual(repair.OEB_TOU_OFF, 0.098)
        self.assertEqual(repair.OEB_TOU_MID, 0.157)
        self.assertEqual(repair.OEB_TOU_ON, 0.203)
        self.assertEqual(repair.OEB_EFFECTIVE, date(2025, 11, 1))

    def test_tiered_and_ulo_gold_from_same_page(self):
        self.assertEqual(repair.OEB_TIER_LOWER, 0.120)
        self.assertEqual(repair.OEB_TIER_HIGHER, 0.142)
        self.assertEqual(repair.OEB_TIER_SUMMER_KWH, 600)
        self.assertEqual(repair.OEB_TIER_WINTER_KWH, 1000)
        self.assertEqual(repair.OEB_ULO_OVERNIGHT, 0.039)
        self.assertEqual(repair.OEB_ULO_WEEKEND_OFF, 0.098)
        self.assertEqual(repair.OEB_ULO_MID, 0.157)
        self.assertEqual(repair.OEB_ULO_ON, 0.391)

    def test_gold_rate_set_feeds_build_tariff_entries(self):
        rates = repair.gold_oeb_rate_set()
        self.assertEqual(rates.tou.off_peak, 0.098)
        self.assertEqual(rates.tou.on_peak, 0.203)
        entries = repair.build_oeb_residential_entries(rates)
        self.assertEqual(set(entries), {"tou", "tiered", "ulo"})
        self.assertEqual(entries["tou"]["rate_type"], "seasonal_tou")
        self.assertEqual(entries["tou"]["code"], "OEB-RPP-TOU")


class TestTouSeasonalShape(unittest.TestCase):
    def test_tou_has_winter_and_summer_weekday_windows(self):
        comps = repair.build_plan_components("tou")
        energy = [c for c in comps if c["component_type"] == "energy"]
        winter = [c for c in energy if c["season_start_month"] == 11]
        summer = [c for c in energy if c["season_start_month"] == 5]
        self.assertTrue(winter and summer)
        # Seasons inclusive: Nov 1–Apr 30 / May 1–Oct 31
        self.assertEqual(winter[0]["season_end_month"], 4)
        self.assertEqual(winter[0]["season_end_day"], 30)
        self.assertEqual(summer[0]["season_end_month"], 10)
        self.assertEqual(summer[0]["season_end_day"], 31)

        winter_weekday = [c for c in winter if c["day_type"] == "weekday"]
        # Split overnight off-peak → 5 weekday slots (00–07 off, 07–11 on,
        # 11–17 mid, 17–19 on, 19–24 off)
        self.assertEqual(len(winter_weekday), 5)
        on_peak = [c for c in winter_weekday if c["period_label"] == "On-Peak"]
        mid_peak = [c for c in winter_weekday if c["period_label"] == "Mid-Peak"]
        off_peak = [c for c in winter_weekday if c["period_label"] == "Off-Peak"]
        self.assertEqual(len(on_peak), 2)  # 07–11 and 17–19
        self.assertEqual(len(mid_peak), 1)  # 11–17
        self.assertEqual(len(off_peak), 2)  # 00–07 and 19–00
        self.assertEqual({c["rate_value"] for c in on_peak}, {0.203})
        self.assertEqual({c["rate_value"] for c in mid_peak}, {0.157})
        self.assertEqual({c["rate_value"] for c in off_peak}, {0.098})

        summer_weekday = [c for c in summer if c["day_type"] == "weekday"]
        self.assertEqual(len(summer_weekday), 5)
        s_on = [c for c in summer_weekday if c["period_label"] == "On-Peak"]
        s_mid = [c for c in summer_weekday if c["period_label"] == "Mid-Peak"]
        self.assertEqual(len(s_on), 1)  # 11–17
        self.assertEqual(len(s_mid), 2)  # 07–11 and 17–19

    def test_weekend_and_holiday_off_peak_all_day_both_seasons(self):
        comps = repair.build_plan_components("tou")
        energy = [c for c in comps if c["component_type"] == "energy"]
        for day_type in ("weekend", "holiday"):
            rows = [c for c in energy if c["day_type"] == day_type]
            self.assertEqual(len(rows), 2)  # winter + summer
            for c in rows:
                self.assertEqual(c["period_label"], "Off-Peak")
                self.assertEqual(c["rate_value"], 0.098)
                self.assertEqual(c["period_start_time"], "00:00")
                self.assertEqual(c["period_end_time"], "00:00")

    def test_completeness_helper_passes_on_new_tou_shape(self):
        comps = repair.build_plan_components("tou")
        result = evaluate_tariff_completeness("seasonal_tou", comps)
        self.assertTrue(result.complete, result.reasons)
        self.assertTrue(result.tou_ok)
        self.assertTrue(result.seasonal_ok)
        self.assertEqual(result.energy_with_clock, result.energy_count)
        self.assertEqual(result.energy_with_season_dates, result.energy_count)
        self.assertTrue(is_complete("seasonal_tou", comps))

    def test_label_only_hydro_one_incomplete_does_not_match_target(self):
        """Prod-like keeper (~46746): prices OK, no clocks → must not KEEP."""
        incomplete = SimpleNamespace(
            effective_date=repair.OEB_EFFECTIVE,
            rate_components=[
                SimpleNamespace(
                    component_type="energy",
                    rate_value=0.203,
                    period_label="On-Peak",
                    period_start_time=None,
                    period_end_time=None,
                    day_type=None,
                    season=None,
                    season_start_month=None,
                    season_start_day=None,
                    season_end_month=None,
                    season_end_day=None,
                    tier_min_kwh=None,
                    tier_max_kwh=None,
                ),
                SimpleNamespace(
                    component_type="energy",
                    rate_value=0.157,
                    period_label="Mid-Peak",
                    period_start_time=None,
                    period_end_time=None,
                    day_type=None,
                    season=None,
                    season_start_month=None,
                    season_start_day=None,
                    season_end_month=None,
                    season_end_day=None,
                    tier_min_kwh=None,
                    tier_max_kwh=None,
                ),
                SimpleNamespace(
                    component_type="energy",
                    rate_value=0.098,
                    period_label="Off-Peak",
                    period_start_time=None,
                    period_end_time=None,
                    day_type=None,
                    season=None,
                    season_start_month=None,
                    season_start_day=None,
                    season_end_month=None,
                    season_end_day=None,
                    tier_min_kwh=None,
                    tier_max_kwh=None,
                ),
            ],
        )
        target = repair.build_plan_components("tou")
        self.assertFalse(repair.components_match_target(incomplete, target))
        self.assertTrue(repair.is_incomplete_keeper(incomplete, "tou"))
        self.assertFalse(is_complete("tou", incomplete.rate_components))
        self.assertFalse(is_complete("seasonal_tou", incomplete.rate_components))


class TestUloScheduleMatchesOebPage(unittest.TestCase):
    def test_ulo_overnight_is_23_to_07_not_tou_clone(self):
        """Consumer page: Ultra-Low Overnight every day 11pm–7am."""
        weekday = ULO_SCHEDULE["weekday"]
        overnight = [s for s in weekday if s["period"] == "ultra-low overnight"]
        self.assertEqual(len(overnight), 1)
        self.assertEqual(overnight[0]["start"], "23:00")
        self.assertEqual(overnight[0]["end"], "07:00")
        on_peak = [s for s in weekday if s["period"] == "on-peak"]
        self.assertEqual(on_peak[0]["start"], "16:00")
        self.assertEqual(on_peak[0]["end"], "21:00")

    def test_ulo_components_complete(self):
        comps = repair.build_plan_components("ulo")
        result = evaluate_tariff_completeness("tou", comps)
        self.assertTrue(result.complete, result.reasons)
        # No season dates required for plain TOU; clocks on every ENERGY
        self.assertTrue(all(c.get("period_start_time") for c in comps))


class TestClassifyAndFixed(unittest.TestCase):
    def _t(self, name, code=None, rate_type="tou"):
        return SimpleNamespace(name=name, code=code, rate_type=rate_type, rate_components=[])

    def test_classify_plans(self):
        self.assertEqual(
            repair.classify_residential_plan(
                self._t("Time-of-Use (TOU) — Residential", "OEB-RPP-TOU")
            ),
            "tou",
        )
        self.assertEqual(
            repair.classify_residential_plan(
                self._t("Tiered Pricing — Residential", "OEB-RPP-TIERED")
            ),
            "tiered",
        )
        self.assertEqual(
            repair.classify_residential_plan(
                self._t("Ultra-Low Overnight (ULO) — Residential", "OEB-RPP-ULO")
            ),
            "ulo",
        )
        self.assertIsNone(
            repair.classify_residential_plan(
                self._t("Some unrelated rate", "X", rate_type="flat")
            )
        )

    def test_extract_fixed_preserves_only_existing(self):
        t = SimpleNamespace(
            rate_components=[
                SimpleNamespace(
                    component_type="fixed",
                    unit="$/month",
                    rate_value=33.41,
                    tier_label="Delivery monthly charge",
                ),
                SimpleNamespace(
                    component_type="energy",
                    unit="$/kWh",
                    rate_value=0.098,
                    tier_label=None,
                ),
            ]
        )
        fixed = repair.extract_fixed_components(t)
        self.assertEqual(len(fixed), 1)
        self.assertEqual(fixed[0]["rate_value"], 33.41)
        # Empty when none — repair must not invent
        empty = repair.extract_fixed_components(SimpleNamespace(rate_components=[]))
        self.assertEqual(empty, [])

    def test_known_stale_audit_id_documented(self):
        self.assertEqual(repair.KNOWN_STALE_TOU_ID, 46746)

    def test_plan_meta_tou_is_seasonal_tou(self):
        meta = repair.plan_meta("tou")
        self.assertEqual(meta["rate_type"].value, "seasonal_tou")
        self.assertIn("oeb.ca", meta["source_url"])


class TestMatchingIdempotent(unittest.TestCase):
    def test_target_matches_itself(self):
        comps = repair.build_plan_components("tou")
        fake = SimpleNamespace(
            effective_date=repair.OEB_EFFECTIVE,
            rate_components=[
                SimpleNamespace(**{k: v for k, v in c.items()})
                for c in comps
            ],
        )
        self.assertTrue(repair.components_match_target(fake, comps))
        self.assertTrue(is_complete("seasonal_tou", comps))


if __name__ == "__main__":
    unittest.main()
