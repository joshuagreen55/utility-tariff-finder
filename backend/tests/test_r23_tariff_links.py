"""R23: crawler reaches linked tariff documents (CPS, PSE&G .ashx, ConEd
azurefd book, LIPA 534-page book, Xcel MN Salesforce-hosted rate book),
publisher-of-record identity (Hydro-Sherbrooke applies Hydro-Québec rates),
and Xcel MN riders read deterministically from the current rate book sheets
plus the official rate_riders listing (monthly fuel). Fixture text fetched
live (free HTTP) 2026-10-08 from xcelenergy.com / xcelnew.my.salesforce.com."""
import logging
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

from app.services import tariff_docs as td
from app.services.price_basis import unadded_price_riders
from app.services.rate_publisher import flag_tariffs, identity_override, publisher_of_record
from app.services.rider_docs import (
    find_rider_sheets, hint_heading_re, parse_monthly_fuel_listing, parse_rider_listing, parse_rider_text,
)
from app.services.rider_fold import apply_fold, plan_fold
from scripts import tariff_pipeline as tp

FIX = Path(__file__).parent / "fixtures/r23"
BOOK = (FIX / "xcel_mn_section5_excerpt.txt").read_text()
GAS = (FIX / "xcel_mn_gas_interim_excerpt.txt").read_text()
LISTING = (FIX / "xcel_rate_riders_2026_10_08.txt").read_text()
SF = "https://xcelnew.my.salesforce.com/sfc/p/1U0000011ttV/a/R3000009eD7q/dG8j89hN0JiO6SAKQKFHcNhiXf0Osoz7XMTYXy0qV2o"
MN_RIDERS = ["Interim Rate Surcharge Rider", "Fuel Clause Rider", "Conservation Improvement Program Adjustment Rider",
             "State Energy Policy Rate Rider", "Renewable Development Fund Rider", "Transmission Cost Recovery Rider",
             "Renewable Energy Standard Rider", "Mercury Cost Recovery Rider", "Environmental Improvement Rider",
             "Sales True-Up Rider", "Revenue Decoupling Mechanism Rider"]


def setUpModule():
    logging.disable(logging.CRITICAL)


def tearDownModule():
    logging.disable(logging.NOTSET)


class TestLinkTiers(unittest.TestCase):
    def test_cps_residential_pdf_first_gas_last(self):
        base = "https://www.cpsenergy.com/rates"
        res = "https://www.cpsenergy.com/content/dam/corporate/en/Documents/2024_Rate_ResidentialElectric.pdf"
        gas = "https://www.cpsenergy.com/content/dam/corporate/en/Documents/2024_Rate_ResidentialGas.pdf"
        self.assertEqual(td.link_tier(res, "Residential Electric Rate", base), 0)
        self.assertEqual(td.link_tier(gas, "Residential Gas Rate", base), 5)
        links = [("https://www.cpsenergy.com/content/corporate/en/my-home.html", "My Home"), (gas, "Residential Gas Rate"),
                 (res, "Residential Electric Rate")]
        self.assertEqual(td.prioritize_tariff_links(links, base)[0][0], res)

    def test_pseg_ashx_current_tariff_is_a_book_prior_is_demoted(self):
        base = "https://nj.pseg.com/aboutpseg/regulatorypage/electrictariffs"
        cur = "https://nj.pseg.com/-/media/pseg/public-site/documents/current-electric-tariff/electric-tariff-17.ashx"
        prior = "https://nj.pseg.com/-/media/pseg/public-site/documents/prior-tariffs/electric-tariff-16.ashx"
        self.assertTrue(td.is_document_url(cur, "Current Electric Tariff B.P.U.N.J. No.17"))
        self.assertEqual(td.link_tier(cur, "Current Electric Tariff B.P.U.N.J. No.17", base), 1)
        self.assertEqual(td.link_tier(prior, "Prior Electric Tariff", base), 5)

    def test_coned_azurefd_book_is_own(self):
        base = "https://www.coned.com/en/rates-tariffs/rates/electric-rates-schedule/electric-psc-10"
        book = ("https://edge-c-dcxprod-web-enfbhbfkb2fghhey.a03.azurefd.net/-/media/files/coned/documents/"
                "rates/electric/psc-10/electric-tariff.pdf")
        self.assertTrue(td.same_owner(book, base))
        self.assertFalse(td.same_owner("https://cdn.example.azurefd.net/-/media/files/other/tariff.pdf", base))

    def test_salesforce_distribution_rate_book(self):
        base = "https://www.xcelenergy.com/company/rates_and_regulations/rates/rate_books"
        self.assertEqual(td.salesforce_distribution(SF)[0], "xcelnew")
        self.assertTrue(td.same_owner(SF, base))
        self.assertFalse(td.same_owner(SF.replace("xcelnew", "acmepower"), base))
        self.assertTrue(td.is_document_url(SF, "Section 5 - Rate Schedules Part 1 (PDF)"))
        self.assertFalse(td.is_document_url(SF, "Learn more"))
        viewer = 'x versionId=068R300000h2Qgz&amp;operationContext=DELIVERY y 00D1U0000011ttV z'
        url = td.salesforce_download_url(SF, viewer)
        self.assertIn("oid=00D1U0000011ttV", url)
        self.assertIn("ids=068R300000h2Qgz", url)
        self.assertIn("d=%2Fa%2FR3000009eD7q%2FdG8j89", url)
        self.assertIsNone(td.salesforce_download_url(SF, "<html></html>"))

    def test_download_pdf_routes_salesforce_links(self):
        with mock.patch.object(tp, "_download_salesforce_distribution", return_value=b"%PDF-1.7 x") as m:
            self.assertEqual(tp._download_pdf(SF), b"%PDF-1.7 x")
            m.assert_called_once_with(SF)

    def test_state_selector_cookie(self):
        client = tp._get_http_client()
        tp.set_state_selector_cookie("https://www.xcelenergy.com/company/rates_and_regulations/rates/rate_books", "MN")
        self.assertEqual(client.cookies.get("GeographicLocation", domain=".xcelenergy.com"), "/Geographic Location/Minnesota")
        tp.set_state_selector_cookie("https://www.xcelenergy.com/x", "")  # no state -> no-op, no error


class TestTariffBookPages(unittest.TestCase):
    PAGES = [
        "TABLE OF CONTENTS\nService Classification No. 1 ........ 273",
        "General rules\nno prices here",
        "SERVICE CLASSIFICATION NO. 1\nRESIDENTIAL SERVICE\nRate 180\nEnergy charge $0.1064 per kWh",
        "SERVICE CLASSIFICATION NO. 1 (continued)\nTerms",
        "SERVICE CLASSIFICATION NO. 2\nGENERAL SERVICE\nEnergy $0.1200 per kWh",
    ]

    def test_residential_schedule_pages(self):
        self.assertEqual(td.residential_schedule_pages(self.PAGES), [2, 3])

    def test_pdftotext_book_reader(self):
        out = mock.Mock(stdout="\f".join(self.PAGES).encode())
        with mock.patch("subprocess.run", return_value=out):
            text = tp._tariff_book_pages_pdftotext(b"%PDF-", front=1)
        self.assertIn("[Page 3]", text)
        self.assertIn("$0.1064 per kWh", text)
        self.assertNotIn("GENERAL SERVICE", text)
        with mock.patch("subprocess.run", side_effect=FileNotFoundError("pdftotext")):
            self.assertEqual(tp._tariff_book_pages_pdftotext(b"%PDF-"), "")


class TestPublisherOfRecord(unittest.TestCase):
    def test_hydro_sherbrooke_hq_pages_kept_and_flagged(self):
        pages = [tp.RatePage(url="https://www.hydroquebec.com/residential/customer-space/rates/price-electricity.html",
                             page_type="html", content="Rate D"),
                 tp.RatePage(url="https://www.hydroquebec.com/data/documents-donnees/pdf/electricity-rates.pdf",
                             page_type="pdf", content="Rate D")]
        e = identity_override(pages, "Hydro-Sherbrooke", "QC")
        self.assertIsNotNone(e)
        t = tp.ExtractedTariff(name="Rate D", customer_class="residential", rate_type="tiered",
                               source_url=pages[1].url, components=[])
        flag_tariffs([t], e)
        self.assertEqual(t.confidence_notes["rate_source"], "publisher_of_record")
        self.assertEqual(t.confidence_notes["rate_publisher"], "Hydro-Québec")
        self.assertIn("regie-energie.qc.ca", t.confidence_notes["rate_publisher_evidence_url"])

    def test_no_override_for_other_domains_or_utilities(self):
        mixed = [tp.RatePage(url="https://www.hydroquebec.com/a", page_type="html", content=""),
                 tp.RatePage(url="https://www.energyrates.ca/quebec", page_type="html", content="")]
        self.assertIsNone(identity_override(mixed, "Hydro-Sherbrooke", "QC"))
        self.assertIsNone(publisher_of_record("Hydro-Magog", "QC"))
        self.assertIsNone(publisher_of_record("Hydro-Sherbrooke", "ON"))


class TestXcelListing(unittest.TestCase):
    def test_listing_rows(self):
        rows = {r["name"]: r for r in parse_rider_listing(LISTING)}
        self.assertAlmostEqual(rows["Transmission Cost Recovery"]["rate_value"], 0.004436)
        self.assertAlmostEqual(rows["Conservation Improvement Program Adj."]["rate_value"], 0.001397)
        self.assertEqual(rows["Renewable Energy Standard Cost Recovery"]["unit"], "%")
        self.assertEqual(rows["State Energy Policy"]["rate_value"], 0.0)
        self.assertEqual(len(rows), 6)  # gas riders excluded

    def test_monthly_fuel(self):
        self.assertAlmostEqual(parse_monthly_fuel_listing(LISTING, 1), 0.01568)
        self.assertAlmostEqual(parse_monthly_fuel_listing(LISTING, 10), -0.00179)  # Prairie Island refund month

    def test_listing_page_defers_to_book_and_dates_fuel(self):
        page = tp.RatePage(url="https://www.xcelenergy.com/company/rates_and_regulations/rates/rate_riders",
                           page_type="html", content=LISTING)
        ts = tp.parse_rider_listing_page(page, today=date(2026, 10, 8),
                                         skip_names=["Transmission Cost Recovery Rider (tariff book)"])
        names = [t.name for t in ts]
        self.assertNotIn("Transmission Cost Recovery", names)
        self.assertIn("Fuel Cost Charge (October 2026)", names)
        # table "last updated 04/01/2026" -> no fuel for another year (never guess)
        ts27 = tp.parse_rider_listing_page(page, today=date(2027, 1, 5))
        self.assertFalse(any(t.name.startswith("Fuel Cost Charge") for t in ts27))
        self.assertEqual(tp.parse_rider_listing_page(tp.RatePage(url="u", page_type="html", content="no rows")), [])


class TestXcelBookSheets(unittest.TestCase):
    def test_rounding_sentence_is_not_an_amount(self):
        self.assertIsNone(parse_rider_text(
            "The factor shall be rounded to the nearest $0.000001 per kWh or $0.01 per kW."))

    def test_sheet_finder(self):
        rx = hint_heading_re("Renewable Energy Standard Rider")
        first = find_rider_sheets(BOOK, rx)
        cont = find_rider_sheets(BOOK, rx, continued=True)
        self.assertTrue(first)
        self.assertTrue(all(parse_rider_text(x) is None for x in first))  # factor is on the continuation sheet
        self.assertEqual(parse_rider_text(cont[0])["pct"], 2.463)
        self.assertNotIn("Date Filed", first[0])

    def test_named_riders_from_current_book(self):
        page = tp.RatePage(url=SF, page_type="pdf", content=BOOK)
        gas = tp.RatePage(url=SF + "g", page_type="pdf", content=GAS + "\n" * 50 + "x" * 2000)
        got = {t.name.replace(" (tariff book)", ""): t for t in tp.extract_named_riders_from_book([page, gas], MN_RIDERS)}

        def kwh(n):
            return [c["rate_value"] for c in got[n].components if c["unit"] == "$/kWh"][0]

        def pct(n):
            return [c["rate_value"] for c in got[n].components if c["unit"] == "% of base bill"][0]

        self.assertAlmostEqual(kwh("Transmission Cost Recovery Rider"), 0.006415)
        self.assertAlmostEqual(kwh("Conservation Improvement Program Adjustment Rider"), 0.001397)  # not CCRC 0.004955
        self.assertAlmostEqual(kwh("Renewable Development Fund Rider"), 0.001371)
        self.assertAlmostEqual(kwh("Sales True-Up Rider"), 0.00041)
        for z in ("State Energy Policy Rate Rider", "Mercury Cost Recovery Rider", "Environmental Improvement Rider"):
            self.assertEqual(kwh(z), 0.0)
        self.assertEqual(pct("Renewable Energy Standard Rider"), 2.463)
        self.assertEqual(pct("Interim Rate Surcharge Rider"), 7.14)  # gas book's 16.19% ignored
        self.assertTrue(got["Revenue Decoupling Mechanism Rider"].confidence_notes["rider_canceled_in_book"])
        self.assertNotIn("Fuel Clause Rider", got)  # monthly — comes from the listing


class TestPercentAndSharedFamily(unittest.TestCase):
    def test_shared_family_needs_name_match(self):
        comps = [{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.12, "tier_label": "All-in (base + riders)"},
                 {"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.001371, "included_in_energy": True,
                  "period_label": "Renewable Development Fund Rider (tariff book)"}]
        out = unadded_price_riders(riders_referenced=["Renewable Development Fund Rider", "Renewable Energy Standard Rider"],
                                   missing_fields=[], energy_includes_riders=None, components=comps)
        self.assertEqual(out, ["Renewable Energy Standard Rider"])
        out1 = unadded_price_riders(riders_referenced=["Renewable Development Fund Rider"], missing_fields=[],
                                    energy_includes_riders=None, components=comps)
        self.assertEqual(out1, [])

    def test_percent_riders_apply_to_base_not_stacked_riders(self):
        plan = tp.ExtractedTariff(
            name="Residential", customer_class="residential", rate_type="flat",
            riders_referenced_not_shown=["Interim Rate Surcharge Rider", "Renewable Energy Standard Rider"],
            components=[
                {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.11364 + 0.006415, "tier_label": "All-in (base + riders)"},
                {"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.006415, "included_in_energy": True,
                 "period_label": "Transmission Cost Recovery Rider (tariff book)"},
                {"component_type": "fixed", "unit": "$/month", "rate_value": 6.0},
            ])
        riders = [tp._r22_rider_tariff("Interim Rate Surcharge Rider (tariff book)", {"per_kwh": [], "pct": 7.14}, SF),
                  tp._r22_rider_tariff("Renewable Energy Standard Rider (tariff book)", {"per_kwh": [], "pct": 2.463}, SF)]
        fold = plan_fold(plan, riders)
        self.assertIsNotNone(fold)
        apply_fold(plan, fold)
        e = [c for c in plan.components if c["component_type"] == "energy"][0]
        self.assertAlmostEqual(e["rate_value"], 0.11364 * 1.09603 + 0.006415, places=6)
        f = [c for c in plan.components if c["component_type"] == "fixed"][0]
        self.assertAlmostEqual(f["rate_value"], 6.0 * 1.09603, places=5)
        self.assertEqual(plan.riders_referenced_not_shown, [])


if __name__ == "__main__":
    unittest.main()
