"""Phase 1: bot-blocked (403) official pages retry in Playwright.

Live dry-run on the 5.5 stack (main 38c0d93): SRP / Pedernales failed when
search found a low-scoring official hit that httpx got 403 on, and Phase 1
gave up instead of opening a browser or falling back to a known rate URL.

    cd backend && python -m unittest tests.test_phase1_403_fallback -v
"""
from __future__ import annotations

import unittest
from unittest import mock

from scripts import tariff_pipeline as tp


class TestPhase1PlaywrightOn403(unittest.TestCase):
    def setUp(self):
        self._prev_js = set(tp._js_rendered_domains)
        tp._js_rendered_domains.clear()

    def tearDown(self):
        tp._js_rendered_domains.clear()
        tp._js_rendered_domains.update(self._prev_js)

    def _run_phase1(self, scored_urls, *, fetch_status=403, js_html="<html>" + ("rates " * 50) + "</html>"):
        """Drive phase1 past search into the fetch / Playwright branch."""
        results = [{"url": u, "title": "Rates", "description": "residential rates"} for u, _ in scored_urls]

        def score(r, *a, **k):
            for url, s in scored_urls:
                if r["url"] == url:
                    return s
            return 0

        statuses = {u: fetch_status for u, _ in scored_urls}

        def fetch(url):
            return "", "text/html", statuses.get(url, 200)

        with mock.patch.object(tp, "preferred_rate_page_url", return_value=None), \
             mock.patch.object(tp, "_discover_utility_domain", return_value=None), \
             mock.patch.object(tp, "brave_search", return_value=results), \
             mock.patch.object(tp, "google_search", return_value=[]), \
             mock.patch.object(tp, "score_search_result", side_effect=score), \
             mock.patch.object(tp, "fetch_page", side_effect=fetch), \
             mock.patch.object(tp, "fetch_page_js", return_value=(js_html, "Rates")) as js:
            url, n, alts = tp.phase1_find_rate_page("Salt River Project", "AZ", None)
        return url, n, alts, js

    def test_low_score_403_retries_playwright_on_best(self):
        # Pre-fix: score 18 (< 50) + 403 → abandoned. Now Playwright runs.
        url, _n, _alts, js = self._run_phase1([
            ("https://www.srpnet.com/price-plans", 18),
        ])
        self.assertEqual(url, "https://www.srpnet.com/price-plans")
        js.assert_called_once()
        self.assertIn("srpnet.com", tp._js_rendered_domains)

    def test_connection_error_still_requires_high_score(self):
        # status==0 keeps the >=50 gate so junk DNS failures don't burn a browser.
        url, _n, _alts, js = self._run_phase1(
            [("https://www.srpnet.com/price-plans", 18)],
            fetch_status=0,
        )
        self.assertEqual(url, "")
        js.assert_not_called()

    def test_alternate_403_retries_playwright(self):
        # Best is a high-score 404 (not Playwright-eligible); alternate is 403.
        results = [
            ("https://www.example.com/dead", 80),
            ("https://www.mypec.com/rates", 25),
        ]
        statuses = {
            "https://www.example.com/dead": 404,
            "https://www.mypec.com/rates": 403,
        }

        def fetch(url):
            return "", "text/html", statuses[url]

        def score(r, *a, **k):
            return dict(results)[r["url"]]

        raw = [{"url": u, "title": "t", "description": "residential rates"} for u, _ in results]
        html = "<html>" + ("electric rates " * 40) + "</html>"
        with mock.patch.object(tp, "preferred_rate_page_url", return_value=None), \
             mock.patch.object(tp, "_discover_utility_domain", return_value=None), \
             mock.patch.object(tp, "brave_search", return_value=raw), \
             mock.patch.object(tp, "google_search", return_value=[]), \
             mock.patch.object(tp, "score_search_result", side_effect=score), \
             mock.patch.object(tp, "fetch_page", side_effect=fetch), \
             mock.patch.object(tp, "fetch_page_js", return_value=(html, "Rates")) as js:
            url, _n, _alts = tp.phase1_find_rate_page("Pedernales Electric", "TX", None)
        self.assertEqual(url, "https://www.mypec.com/rates")
        js.assert_called_once_with("https://www.mypec.com/rates")
        self.assertIn("mypec.com", tp._js_rendered_domains)

    def test_403_playwright_miss_still_fails_cleanly(self):
        url, _n, _alts, js = self._run_phase1(
            [("https://www.srpnet.com/price-plans", 18)],
            js_html="",
        )
        self.assertEqual(url, "")
        js.assert_called_once()


if __name__ == "__main__":
    unittest.main()
