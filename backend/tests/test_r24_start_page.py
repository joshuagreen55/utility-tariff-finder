"""R24: phase 1 starts from the utility's own official tariff / rate-book
page, not supply-price sheets, third-party "about this utility" pages,
another utility's domain, or documents dated two or more years back.

Search fixtures are the production Brave cache entries (read-only copy,
2026-10-09) for the queries phase 1 issued for ComEd, Xcel MN (NSP-MN),
SMUD, SDG&E, Union Electric (Ameren Missouri), APS and JCP&L."""
import json
import logging
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

from app.services import start_page as sp
from app.services.source_type import UtilitySourceContext, is_third_party_host
from scripts import tariff_pipeline as tp

FIX = json.loads((Path(__file__).parent / "fixtures/r24/brave_results.json").read_text())
TODAY = date(2026, 10, 9)
BY_QUERY = {v["query"]: v["results"] for v in FIX.values()}


def setUpModule():
    logging.disable(logging.CRITICAL)


def tearDownModule():
    logging.disable(logging.NOTSET)


class DomainMatchTests(unittest.TestCase):
    def test_generic_and_state_words_do_not_match(self):
        self.assertEqual(sp.domain_match_strength(
            "mnpower.com", "Northern States Power Co - Minnesota",
            "Home - Minnesota Power is an ALLETE Company"), 0)

    def test_acronyms_brand_concatenations_and_title_abbreviations(self):
        cases = [
            ("smud.org", "Sacramento Municipal Util Dist", ""),
            ("sdge.com", "San Diego Gas & Electric Co", ""),
            ("aps.com", "Arizona Public Service Co", ""),
            ("ladwp.com", "Los Angeles Department of Water & Power", ""),
            ("lge-ku.com", "Kentucky Utilities Co", ""),
            ("alabamapower.com", "Alabama Power Co", ""),
            ("pplelectric.com", "PPL Electric Utilities Corp", ""),
            ("comed.com", "Commonwealth Edison Co", "Commonwealth Edison (ComEd"),
        ]
        for host, name, title in cases:
            with self.subTest(host=host):
                self.assertEqual(sp.domain_match_strength(host, name, title), 2)

    def test_abbreviation_on_a_reseller_host_does_not_count(self):
        self.assertEqual(sp.domain_match_strength(
            "hudsonenergy.net", "Commonwealth Edison Co", "ComEd Electric Service: Available in Chicago"), 0)

    def test_city_and_gov_hosts_only_for_city_run_utilities(self):
        self.assertFalse(sp.acceptable_utility_host("cityofsacramento.gov", "Sacramento Municipal Util Dist"))
        self.assertFalse(sp.acceptable_utility_host("unionmissouri.gov", "Union Electric Co - (MO)"))
        self.assertTrue(sp.acceptable_utility_host("cityofpaloalto.org", "City of Palo Alto - (CA)"))
        self.assertFalse(sp.acceptable_utility_host("https://www.solartopps.com", "Arizona Public Service Co"))


class DiscoveryTests(unittest.TestCase):
    def test_discovery_picks_the_utility_not_a_namesake(self):
        self.assertIsNone(sp.pick_discovered_domain(FIX["nsp_discover"]["results"], "Northern States Power Co - Minnesota"))
        self.assertEqual(sp.pick_discovered_domain(FIX["smud_discover"]["results"], "Sacramento Municipal Util Dist"), "smud.org")
        self.assertEqual(sp.pick_discovered_domain(FIX["sdge_discover"]["results"], "San Diego Gas & Electric Co"), "sdge.com")
        self.assertEqual(sp.pick_discovered_domain(FIX["comed_discover"]["results"], "Commonwealth Edison Co"), "comed.com")
        self.assertIsNone(sp.pick_discovered_domain(FIX["union_discover"]["results"], "Union Electric Co - (MO)"))

    def test_domain_inferred_from_a_tariff_document_carrying_the_legal_name(self):
        self.assertEqual(sp.infer_domain_from_candidates(
            FIX["nsp_search"]["results"], "Northern States Power Co - Minnesota"), "xcelenergy.com")
        self.assertEqual(sp.infer_domain_from_candidates(
            FIX["aps_search"]["results"], "Arizona Public Service Co"), "aps.com")


class CandidateTierTests(unittest.TestCase):
    def test_about_this_utility_pages(self):
        for url, name in [
            ("https://example.org/blog/commonwealth-edison-rates-explained", "Commonwealth Edison Co"),
            ("https://nectarclimate.com/rates/northern-states-power-xcel-mn", "Northern States Power Co - Minnesota"),
            ("https://example-rates.com/electricity-rates/california/san-diego/", "San Diego Gas & Electric Co"),
            ("https://example-dir.com/utility-companies/comed", "Commonwealth Edison Co"),
        ]:
            with self.subTest(url=url):
                self.assertTrue(sp.is_about_utility_page(url, name))
        self.assertFalse(sp.is_about_utility_page("https://www.sdge.com/total-electric-rates", "San Diego Gas & Electric Co"))
        for host in ("wattcosts.com", "utilityrates.com", "clearwaycommunitysolar.com", "nectarclimate.com"):
            self.assertTrue(is_third_party_host(host))

    def test_supply_and_dated_documents_are_demoted(self):
        self.assertTrue(sp.is_supply_page("https://www.firstenergycorp.com/customer_choice/new_jersey/price_to_compare.html"))
        self.assertFalse(sp.is_supply_page("https://www.firstenergycorp.com/customer_choice/new_jersey/new_jersey_tariffs.html"))
        self.assertTrue(sp.is_stale_document("https://www.ameren.com/-/media/rates/files/illinois/2022/aiifhss1022.ashx", TODAY))
        self.assertTrue(sp.is_stale_document(
            "https://www.sce.com/x/Residential%20Rates%20Fact%20Sheet%20English%20FINAL%20WCAG%20August%202023_edits.pdf", TODAY))
        self.assertFalse(sp.is_stale_document("https://www.nvenergy.com/x/bill_inserts/2026/01_jan/spp_nv_resrates.pdf", TODAY))

    def test_rerank_puts_own_current_pages_first(self):
        name, dom = "San Diego Gas & Electric Co", "sdge.com"
        scored = [(64, {"url": "https://wattcosts.com/electricity-rates/california/san-diego/"}),
                  (61, {"url": "https://www.sdge.com/total-electric-rates"}),
                  (50, {"url": "https://www.sdge.com/x/price-to-compare.pdf"}),
                  (43, {"url": "https://www.kpbs.org/news/economy/2026/03/09/why-your-bill"})]
        order = [r["url"] for _, r in sp.rerank_candidates(scored, name, dom, TODAY)]
        self.assertEqual(order[0], "https://www.sdge.com/total-electric-rates")
        self.assertEqual(order[1], "https://www.sdge.com/x/price-to-compare.pdf")
        self.assertEqual(set(order[2:]), {"https://wattcosts.com/electricity-rates/california/san-diego/",
                                          "https://www.kpbs.org/news/economy/2026/03/09/why-your-bill"})


class HubTests(unittest.TestCase):
    def test_hubs(self):
        self.assertEqual(sp.official_tariff_hub("xcelenergy.com", "MN"),
                         "https://www.xcelenergy.com/company/rates_and_regulations/rates/rate_books")
        self.assertEqual(sp.official_tariff_hub("https://www.firstenergycorp.com", "NJ"),
                         "https://www.firstenergycorp.com/customer_choice/new_jersey/new_jersey_tariffs.html")
        self.assertIsNone(sp.official_tariff_hub("firstenergycorp.com", "OH"))  # Ohio page has another path
        self.assertEqual(sp.official_tariff_hub("comed.com", "IL"), "https://www.comed.com/current-rates-tariffs")
        self.assertIsNone(sp.official_tariff_hub("alabamapower.com", "AL"))


def _brave(query, count=10):
    return [dict(r) for r in BY_QUERY.get(query, [])]


class Phase1EndToEndTests(unittest.TestCase):
    def _run(self, name, state, website=None):
        page = ("<html>" + "rates " * 200 + "</html>", "text/html", 200)
        with mock.patch.object(tp, "brave_search", side_effect=_brave), \
             mock.patch.object(tp, "google_search", return_value=[]), \
             mock.patch.object(tp, "fetch_page", return_value=page), \
             mock.patch.object(tp, "fetch_page_js", return_value=("", "")):
            return tp.phase1_find_rate_page(name, state, website)

    def test_xcel_mn_starts_from_rate_books(self):
        url, _, _ = self._run("Northern States Power Co - Minnesota", "MN")
        self.assertEqual(url, "https://www.xcelenergy.com/company/rates_and_regulations/rates/rate_books")

    def test_comed_starts_from_its_tariff_hub(self):
        url, _, _ = self._run("Commonwealth Edison Co", "IL")
        self.assertEqual(url, "https://www.comed.com/current-rates-tariffs")

    def test_jcpl_with_known_website_starts_from_nj_tariffs(self):
        url, _, _ = self._run("Jersey Central Power & Lt Co", "NJ", "https://www.firstenergycorp.com")
        self.assertEqual(url, "https://www.firstenergycorp.com/customer_choice/new_jersey/new_jersey_tariffs.html")

    def test_sdge_and_smud_start_on_their_own_sites(self):
        self.assertIn("sdge.com", self._run("San Diego Gas & Electric Co", "CA")[0])
        self.assertIn("smud.org", self._run("Sacramento Municipal Util Dist", "CA")[0])

    def test_union_electric_skips_the_city_of_union(self):
        url, _, _ = self._run("Union Electric Co - (MO)", "MO")
        self.assertIn("ameren.com", url)

    def test_third_party_website_url_is_ignored(self):
        url, _, _ = self._run("Arizona Public Service Co", "AZ", "https://www.solartopps.com")
        self.assertIn("aps.com", url)


class PreferOfficialTargetsTests(unittest.TestCase):
    def test_supply_sheet_yields_to_tariff_page_on_file(self):
        ctx = UtilitySourceContext(website_url="https://www.firstenergycorp.com", state_province="OH")
        known = ["https://www.firstenergycorp.com/customer_choice/new_jersey/new_jersey_tariffs.html",
                 "https://www.firstenergycorp.com/content/dam/x/BPU-12-Part-III-Effective-8-1-2019.pdf",
                 "https://www.firstenergycorp.com/fehome.html"]
        p, alts = tp.prefer_official_targets(
            "https://www.firstenergycorp.com/customer_choice/new_jersey/price_to_compare.html", [], known, ctx,
            utility_name="Jersey Central Power & Lt Co")
        self.assertEqual(p, known[0])
        self.assertIn("https://www.firstenergycorp.com/customer_choice/new_jersey/price_to_compare.html", alts)

    def test_dated_pdf_yields_to_state_rates_page(self):
        ctx = UtilitySourceContext(website_url="https://www.ameren.com/illinois", state_province="IL")
        known = ["https://www.ameren.com/-/media/rates/files/illinois/2022/aiifhss1022.ashx",
                 "https://www.ameren.com/rates", "https://ltdsolarconsulting.com/ameren-rates/",
                 "https://www.ameren.com/illinois/residential/rates/electric-rates", "https://www.ameren.com/"]
        p, _ = tp.prefer_official_targets("", [], known, ctx, utility_name="Ameren Illinois Company")
        self.assertEqual(p, "https://www.ameren.com/illinois/residential/rates/electric-rates")

    def test_locked_override_is_kept(self):
        ctx = UtilitySourceContext(website_url="https://www.sce.com", state_province="CA")
        old = "https://www.sce.com/x/Residential%20Rates%20Fact%20Sheet%20August%202023.pdf"
        p, _ = tp.prefer_official_targets(old, [], ["https://www.sce.com/regulatory/tariff-books"], ctx,
                                          locked=True, utility_name="Southern California Edison Co")
        self.assertEqual(p, old)

    def test_clean_primary_unchanged(self):
        ctx = UtilitySourceContext(website_url="https://www.fpl.com", state_province="FL")
        p, _ = tp.prefer_official_targets("https://www.fpl.com/rates.html", [], ["https://www.fpl.com/x.pdf"], ctx,
                                          utility_name="Florida Power & Light Co")
        self.assertEqual(p, "https://www.fpl.com/rates.html")


if __name__ == "__main__":
    unittest.main()
