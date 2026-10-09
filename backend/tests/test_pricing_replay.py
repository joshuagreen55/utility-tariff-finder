"""Offline replay suite against real R27 dual-extract outputs (no LLM)."""
from __future__ import annotations

import unittest

from app.services.pricing.replay import (
    DEFAULT_FIXTURE_DIR,
    load_plans,
    run_replay,
)


class TestReplayFixturesPresent(unittest.TestCase):
    def test_fixture_dir_has_plans_and_docs(self):
        self.assertTrue((DEFAULT_FIXTURE_DIR / "plans.jsonl").is_file())
        docs = list((DEFAULT_FIXTURE_DIR / "docs" / "url").glob("*.json"))
        self.assertGreaterEqual(len(docs), 20)

    def test_r27_recovered_plans_loaded(self):
        plans = load_plans(run="r27", set_name="golden", recovered_only=True)
        self.assertGreaterEqual(len(plans), 35)


class TestReplayR27Golden(unittest.TestCase):
    def test_zero_accepted_wrong(self):
        report = run_replay(run="r27", set_name="golden", recovered_only=True)
        self.assertEqual(
            report.accepted_wrong, 0,
            [r.to_dict() for r in report.results if r.outcome == "accepted_wrong"],
        )

    def test_scored_plans_exist(self):
        report = run_replay(run="r27", set_name="golden", recovered_only=True)
        self.assertGreaterEqual(report.scored, 20)


if __name__ == "__main__":
    unittest.main()
