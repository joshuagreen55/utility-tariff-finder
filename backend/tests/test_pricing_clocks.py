"""PR R27-5: TOU clock / season calendar golden scoring."""
from __future__ import annotations

import json
import unittest

from app.services.pricing.clocks import (
    clocks_match,
    extract_clocks_from_components,
    meta_components_from_plan_schedule,
    seasons_match,
)
from app.services.pricing.compiler import GOLDEN_DIR, plan_from_dict
from scripts.pricing_golden_harness import run_harness


class TestClockHelpers(unittest.TestCase):
    def test_normalize_24_to_midnight(self):
        self.assertTrue(clocks_match(
            [{"period": "off_peak", "season": "all", "day_type": "all",
              "start": "19:00", "end": "24:00"}],
            [{"period": "off_peak", "season": "all", "day_type": "all",
              "start": "19:00", "end": "00:00"}],
        ))

    def test_mismatch(self):
        self.assertFalse(clocks_match(
            [{"period": "on_peak", "season": "winter", "day_type": "weekday",
              "start": "07:00", "end": "11:00"}],
            [{"period": "on_peak", "season": "winter", "day_type": "weekday",
              "start": "07:00", "end": "12:00"}],
        ))

    def test_empty_expected_vacuous(self):
        self.assertTrue(clocks_match([], [{"period": "x", "start": "0", "end": "1"}]))
        self.assertTrue(seasons_match([], [{"season": "winter", "start_month": 1,
                                            "start_day": 1, "end_month": 2, "end_day": 1}]))

    def test_meta_roundtrip(self):
        clocks = [
            {"period": "off_peak", "season": "winter", "day_type": "weekday",
             "start": "00:00", "end": "07:00"},
        ]
        seasons = [
            {"season": "winter", "start_month": 11, "start_day": 1,
             "end_month": 4, "end_day": 30},
        ]
        meta = meta_components_from_plan_schedule(clocks, seasons)
        got_c, got_s = extract_clocks_from_components(meta)
        self.assertTrue(clocks_match(clocks, got_c))
        self.assertTrue(seasons_match(seasons, got_s))


class TestGoldenClocksPresent(unittest.TestCase):
    def test_oeb_and_nsp_have_clocks(self):
        raw = json.loads((GOLDEN_DIR / "plans.json").read_text())
        by_key = {p["plan_key"]: p for p in raw["plans"]}
        for key in ("toronto-tou", "hydroone-tou", "nsp-tou", "nsp-tod"):
            self.assertTrue(by_key[key].get("clocks"), f"{key} missing clocks")
        for key in ("toronto-tou", "nsp-tou", "nlh-1.1s"):
            self.assertTrue(by_key[key].get("seasons"), f"{key} missing seasons")
        plan = plan_from_dict(by_key["toronto-tou"])
        self.assertGreaterEqual(len(plan.clocks), 10)
        self.assertEqual(len(plan.seasons), 2)


class TestHarnessClockScoring(unittest.TestCase):
    def test_offline_scores_clocks(self):
        report = run_harness(mode="offline")
        self.assertGreaterEqual(report.clocks_scored, 5)
        self.assertEqual(report.clocks_matched, report.clocks_scored)
        self.assertEqual(report.matched, report.scored)

    def test_live_dry_scores_clocks(self):
        report = run_harness(mode="live", force_extract=True)
        self.assertGreaterEqual(report.clocks_scored, 5)
        self.assertEqual(
            report.clocks_matched, report.clocks_scored,
            [(s.plan_key, s.clocks_matched, s.hold_reason)
             for s in report.plans if s.clocks_matched is False],
        )
        self.assertEqual(report.matched, report.scored, [
            (s.plan_key, s.hold_reason, s.error, s.official, s.compiled)
            for s in report.plans if not s.matched
        ])


if __name__ == "__main__":
    unittest.main()
