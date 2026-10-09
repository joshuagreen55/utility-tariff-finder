"""PR D: golden harness exact-match accuracy."""
from __future__ import annotations

import unittest

from scripts.pricing_golden_harness import run_harness


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


if __name__ == "__main__":
    unittest.main()
