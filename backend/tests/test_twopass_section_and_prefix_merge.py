"""Stack-5.5 12-utility dry run fixes (2026-10-07).

    cd backend && python -m unittest tests.test_twopass_section_and_prefix_merge -v
"""
from __future__ import annotations

import logging
import unittest
from unittest import mock

from scripts import tariff_pipeline as tp


def _toc_then_body() -> str:
    toc = (
        "TABLE OF CONTENTS Domestic Service Tariff 1 "
        "Domestic Service Time of Use Tariff Rate Code 80 9 "
        "Small General Tariff 20 " + "filler text " * 300
    )
    body = (
        "DOMESTIC SERVICE TIME OF USE TARIFF Rate Code 80 CUSTOMER CHARGE $20.08 "
        "ENERGY CHARGE Winter on-peak 36.517 cents per kWh, off-peak 18.324 cents "
        "per kWh, Non-winter 12.860 cents per kWh. " + "more terms " * 200
    )
    return toc + body


class TestTwopassSection(unittest.TestCase):
    def test_short_content_is_returned_whole(self):
        self.assertEqual(tp._twopass_section("short", "x", "y"), "short")

    def test_toc_hit_does_not_hide_the_priced_section(self):
        content = _toc_then_body()
        sec = tp._twopass_section(
            content, "Domestic Service Time of Use Tariff (Rate Code 80)",
            "domestic service time of use tariff rate code",
        )
        self.assertIn("36.517", sec)
        self.assertIn("12.860", sec)

    def test_no_priced_window_falls_back_to_full_content(self):
        content = "Rate X is described elsewhere. " * 400
        self.assertIs(tp._twopass_section(content, "Rate X", "rate x is described"), content)

    def test_missing_hint_uses_name(self):
        content = ("lorem ipsum " * 600) + "Rate Q energy 9.123 cents per kWh and $12.00 per month" + (" tail" * 600)
        sec = tp._twopass_section(content, "Rate Q", "this hint is not in the text")
        self.assertIn("9.123", sec)
        self.assertLess(len(sec), len(content))


class TestTwopassFallback(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_zero_from_section_retries_on_full_content(self):
        content = _toc_then_body() + (" padding" * 4000)
        page = tp.RatePage(url="https://example.com/book.pdf", title="Book", content=content, page_type="pdf")
        page.content_hash = ""
        identified = '[{"name": "Domestic Service Time of Use Tariff", "customer_class": "residential", "location_hint": "domestic service time of use tariff rate code"}]'
        good = [{"name": "Domestic Service Time of Use Tariff", "customer_class": "residential", "rate_type": "seasonal_tou",
                 "components": [{"component_type": "energy", "unit": "¢/kWh", "rate_value": 36.517}]}]
        calls = []

        def fake_tool(prompt, model=None):
            calls.append(len(prompt))
            return [] if len(calls) == 1 else good

        with mock.patch.object(tp, "_call_claude", return_value=identified), \
             mock.patch.object(tp, "_call_claude_tool", side_effect=fake_tool), \
             mock.patch.object(tp.time, "sleep"):
            tariffs, n = tp._extract_two_pass(page, "Nova Scotia Power", "NS")
        self.assertEqual(len(calls), 2)
        self.assertGreater(calls[1], calls[0])
        self.assertEqual([t.name for t in tariffs], ["Domestic Service Time of Use Tariff"])
        self.assertEqual(n, 3)


def _t(name, *energy, cc="residential"):
    return tp.ExtractedTariff(
        name=name, customer_class=cc,
        components=[{"component_type": "energy", "unit": "¢/kWh", "rate_value": v} for v in energy],
    )


class TestPrefixMerge(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_hq_rate_d_survives_rate_d_t(self):
        out = tp._merge_prefix_duplicates([_t("Rate D", 7.065, 11.142), _t("Rate D T", 5.131, 30.001, 46.154)])
        self.assertEqual(sorted(t.name for t in out), ["Rate D", "Rate D T"])

    def test_generic_tail_still_merges(self):
        out = tp._merge_prefix_duplicates([_t("Domestic Service", 18.324), _t("Domestic Service Tariff", 18.324, 19.067)])
        self.assertEqual([t.name for t in out], ["Domestic Service Tariff"])

    def test_subset_prices_still_merge(self):
        out = tp._merge_prefix_duplicates([_t("Rate R", 9.5), _t("Rate R Winter Detail", 9.5, 11.0)])
        self.assertEqual(len(out), 1)

    def test_detail_less_side_still_merges(self):
        a = tp.ExtractedTariff(name="Schedule 7", customer_class="residential",
                               components=[{"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 0.2}])
        out = tp._merge_prefix_duplicates([a, _t("Schedule 7 Residential Service", 12.1, 13.4)])
        self.assertEqual([t.name for t in out], ["Schedule 7 Residential Service"])


if __name__ == "__main__":
    unittest.main()


class TestDeadOverrideFallsBackToSearch(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_dead_override_runs_search_and_tries_its_hit(self):
        override = "https://utility.example/rates/old-dead-page"
        search_hit = "https://utility.example/tariffs/all_tariffs.pdf"
        info = {"name": "Example Electric", "state": "OR", "country": "US",
                "website_url": "https://utility.example", "rate_page_url_override": override}
        crawled = []

        def fake_phase2(url):
            crawled.append(url)
            if url == search_hit:
                return [tp.RatePage(url=url, title="All tariffs", content="Schedule 7 $11.00 per month 6.329 cents per kWh", page_type="pdf")]
            return []

        tariff = tp.ExtractedTariff(name="Schedule 7", customer_class="residential", rate_type="flat",
                                    components=[{"component_type": "energy", "unit": "¢/kWh", "rate_value": 6.329}])
        with mock.patch.object(tp, "get_utility_info", return_value=info), \
             mock.patch.object(tp, "_try_centralized_regulator", return_value=None), \
             mock.patch.object(tp, "resolve_preferred_rate_page", return_value=("", [])), \
             mock.patch.object(tp, "_known_rate_urls", return_value=["https://cdn.example/Sched_489.pdf"]), \
             mock.patch.object(tp, "phase1_find_rate_page", return_value=(search_hit, 5, [])) as p1, \
             mock.patch.object(tp, "phase2_discover_tariff_pages", side_effect=fake_phase2), \
             mock.patch.object(tp, "phase3_extract_tariffs", return_value=[tariff]):
            res = tp.run_pipeline(1, dry_run=True, force_extract=True)
        p1.assert_called_once()
        self.assertEqual(crawled[:2], [override, search_hit])
        self.assertEqual([t["name"] for t in res.phase3_tariffs], ["Schedule 7"])


class TestTranslationDuplicates(unittest.TestCase):
    def test_pge_language_switcher_collapses_to_english(self):
        langs = ["zh", "ko", "tl", "ja", "hmn", "ar", "es", "fa", "hi", "km"]
        links = [("https://www.pge.com/en/account/rate-plans/smartrate.html", "SmartRate")]
        links += [(f"https://www.pge.com/{l}/account/rate-plans/smartrate.html", "SmartRate") for l in langs]
        links += [("https://www.pge.com/en/account/rate-plans.html", "Rate plans")]
        out = tp._drop_translation_duplicates(links)
        self.assertEqual([u for u, _ in out], [
            "https://www.pge.com/en/account/rate-plans/smartrate.html",
            "https://www.pge.com/en/account/rate-plans.html",
        ])

    def test_state_code_paths_are_not_dropped(self):
        links = [("https://www.entergy.com/ar/residential/rates", "AR"),
                 ("https://www.entergy.com/residential/rates", "All"),
                 ("https://www.entergy.com/tx/residential/rates", "TX")]
        self.assertEqual(tp._drop_translation_duplicates(links), links)

    def test_foreign_only_set_is_kept(self):
        links = [("https://u.example/es/tarifas", "x"), ("https://u.example/zh/tarifas", "y")]
        self.assertEqual(tp._drop_translation_duplicates(links), links)


class TestNewThirdPartyDomains(unittest.TestCase):
    def test_srp_aggregators_are_hard_blocked(self):
        for u in ("https://utilitycheck.co/utilities/salt-river-project/rate-per-kwh",
                  "https://www.kadoa.com/energy-prices/electricity/arizona/salt-river-project",
                  "https://zipelectricity.com/electricity-rates/texas/austin"):
            with self.subTest(u=u):
                self.assertTrue(tp._is_third_party_domain(u))
