"""Unit tests for structured TOU / seasonal completeness rules.

Runnable without a database:

    cd backend && python3 -m unittest tests.test_tou_seasonal_completeness -v
"""
from __future__ import annotations

import unittest
from datetime import time

from app.services.tou_seasonal_completeness import (
    evaluate_tariff_completeness,
    has_clock_window,
    has_season_calendar,
    is_complete,
    seasonal_calendar_ok,
    tou_clock_ok,
)


def _energy(**kwargs) -> dict:
    base = {
        "component_type": "energy",
        "unit": "$/kWh",
        "rate_value": 0.15,
    }
    base.update(kwargs)
    return base


def _ns80_complete_components() -> list[dict]:
    """NS Power Rate 80–shaped seasonal_tou: clocks + calendar on every ENERGY."""
    winter = dict(
        season="Winter (Nov 1–Mar 31)",
        season_start_month=11,
        season_start_day=1,
        season_end_month=3,
        season_end_day=31,
        day_type="weekday",
    )
    nonwinter = dict(
        season="Non-winter (Apr 1–Oct 31)",
        season_start_month=4,
        season_start_day=1,
        season_end_month=10,
        season_end_day=31,
        day_type="all",
    )
    return [
        {
            "component_type": "fixed",
            "unit": "$/month",
            "rate_value": 20.08,
        },
        _energy(
            rate_value=0.13664,
            period_label="All hours",
            period_start_time=time(0, 0),
            period_end_time=time(0, 0),
            **nonwinter,
        ),
        _energy(
            rate_value=0.37321,
            period_label="On-peak morning (7am–11am)",
            period_start_time=time(7, 0),
            period_end_time=time(11, 0),
            **winter,
        ),
        _energy(
            rate_value=0.19128,
            period_label="Off-peak midday (11am–5pm)",
            period_start_time=time(11, 0),
            period_end_time=time(17, 0),
            **winter,
        ),
        _energy(
            rate_value=0.37321,
            period_label="On-peak evening (5pm–9pm)",
            period_start_time=time(17, 0),
            period_end_time=time(21, 0),
            **winter,
        ),
        _energy(
            rate_value=0.19128,
            period_label="Off-peak night (9pm–7am)",
            period_start_time=time(21, 0),
            period_end_time=time(7, 0),
            **winter,
        ),
    ]


def _hydro_one_incomplete_tou() -> list[dict]:
    """Hydro One–like TOU: On/Mid/Off prices, labels only, no clock windows."""
    return [
        _energy(rate_value=0.203, period_label="On-Peak"),
        _energy(rate_value=0.157, period_label="Mid-Peak"),
        _energy(rate_value=0.098, period_label="Off-Peak"),
    ]


def _vague_winter_incomplete() -> list[dict]:
    """Seasonal ENERGY with vague 'Winter' and no calendar dates."""
    return [
        _energy(rate_value=0.16, season="Winter"),
        _energy(rate_value=0.12, season="Summer"),
    ]


class TestStructuredFieldHelpers(unittest.TestCase):
    def test_clock_window_requires_both_ends(self):
        self.assertFalse(has_clock_window(_energy(period_start_time=time(7, 0))))
        self.assertFalse(has_clock_window(_energy(period_end_time=time(11, 0))))
        self.assertTrue(
            has_clock_window(
                _energy(period_start_time=time(7, 0), period_end_time=time(11, 0))
            )
        )

    def test_clock_window_accepts_hhmm_strings(self):
        self.assertTrue(
            has_clock_window(
                _energy(period_start_time="07:00", period_end_time="11:00")
            )
        )
        self.assertTrue(
            has_clock_window(
                _energy(period_start_time="21:00", period_end_time="24:00")
            )
        )

    def test_season_calendar_requires_all_four(self):
        self.assertFalse(
            has_season_calendar(
                _energy(
                    season_start_month=11,
                    season_start_day=1,
                    season_end_month=3,
                )
            )
        )
        self.assertTrue(
            has_season_calendar(
                _energy(
                    season_start_month=11,
                    season_start_day=1,
                    season_end_month=3,
                    season_end_day=31,
                )
            )
        )


class TestCompletenessRules(unittest.TestCase):
    def test_complete_seasonal_tou_ns80_shape(self):
        comps = _ns80_complete_components()
        result = evaluate_tariff_completeness("seasonal_tou", comps)
        self.assertTrue(result.tou_ok)
        self.assertTrue(result.seasonal_ok)
        self.assertTrue(result.complete)
        self.assertTrue(is_complete("seasonal_tou", comps))
        self.assertEqual(result.energy_count, 5)
        self.assertEqual(result.energy_with_clock, 5)
        self.assertEqual(result.energy_with_season_dates, 5)

    def test_incomplete_hydro_one_like_tou_prices_no_times(self):
        comps = _hydro_one_incomplete_tou()
        result = evaluate_tariff_completeness("tou", comps)
        self.assertFalse(result.tou_ok)
        self.assertTrue(result.seasonal_ok)  # not seasonal-family
        self.assertFalse(result.complete)
        self.assertIn("tou_missing_clock_windows", result.reasons)
        self.assertFalse(tou_clock_ok("tou", comps))
        # Labels alone must not count as complete
        self.assertEqual(result.energy_with_clock, 0)

    def test_incomplete_vague_winter_no_dates(self):
        comps = _vague_winter_incomplete()
        result = evaluate_tariff_completeness("seasonal", comps)
        self.assertTrue(result.tou_ok)  # not tou-family
        self.assertFalse(result.seasonal_ok)
        self.assertFalse(result.complete)
        self.assertIn("seasonal_missing_calendar_dates", result.reasons)
        self.assertFalse(seasonal_calendar_ok("seasonal", comps))

    def test_seasonal_tou_needs_both_rules(self):
        # Clocks present but no season dates → incomplete
        comps = [
            _energy(
                period_label="On-Peak",
                period_start_time=time(7, 0),
                period_end_time=time(11, 0),
                day_type="weekday",
                season="Winter",
            ),
            _energy(
                period_label="Off-Peak",
                period_start_time=time(11, 0),
                period_end_time=time(7, 0),
                day_type="weekday",
                season="Winter",
            ),
        ]
        result = evaluate_tariff_completeness("seasonal_tou", comps)
        self.assertTrue(result.tou_ok)
        self.assertFalse(result.seasonal_ok)
        self.assertFalse(result.complete)

    def test_flat_rate_not_subject_to_tou_season_rules(self):
        comps = [_energy(rate_value=0.12)]
        result = evaluate_tariff_completeness("flat", comps)
        self.assertTrue(result.complete)
        self.assertEqual(result.reasons, ())

    def test_oeb_style_expanded_tou_is_complete(self):
        """After OEB scraper emits structured windows, Hydro One shape is complete."""
        from scripts.scrape_oeb_rates import (
            TOURates,
            OEBRateSet,
            build_tariff_entries,
        )

        rates = OEBRateSet(
            tou=TOURates(
                effective_date="2025-11-01",
                off_peak=0.098,
                mid_peak=0.157,
                on_peak=0.203,
            )
        )
        entries = build_tariff_entries(rates, "residential")
        tou = next(e for e in entries if "Time-of-Use" in e["name"])
        self.assertEqual(tou["rate_type"], "seasonal_tou")
        result = evaluate_tariff_completeness(tou["rate_type"], tou["components"])
        self.assertTrue(result.complete, result.reasons)
        # Must not look like the old label-only Hydro One bug
        self.assertGreater(result.energy_with_clock, 3)
        self.assertTrue(all(
            c.get("period_start_time") and c.get("period_end_time")
            for c in tou["components"]
            if c["component_type"] == "energy"
        ))


class TestPipelineParsers(unittest.TestCase):
    def test_parse_period_time_variants(self):
        from scripts import tariff_pipeline as tp

        self.assertEqual(tp._parse_period_time("07:00"), time(7, 0))
        self.assertEqual(tp._parse_period_time("7:00 pm"), time(19, 0))
        self.assertEqual(tp._parse_period_time("24:00"), time(0, 0))
        self.assertIsNone(tp._parse_period_time(None))
        self.assertIsNone(tp._parse_period_time("On-Peak"))  # do not invent

    def test_parse_day_type(self):
        from scripts import tariff_pipeline as tp

        self.assertEqual(tp._parse_day_type("weekday"), "weekday")
        self.assertEqual(tp._parse_day_type("Weekends"), "weekend")
        self.assertIsNone(tp._parse_day_type("sometime"))


if __name__ == "__main__":
    unittest.main()
