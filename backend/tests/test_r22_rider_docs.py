"""R22: deterministic rider-document reading + rider discovery from the
utility's own links. Fixture text was fetched live (free HTTP) 2026-10-09
from official sites: Georgia Power FCR/ECCR/DSM-R, El Paso Electric Sch 98 /
97 and its Texas tariff index, and the Evergy Missouri Metro tariff book."""
import json
import logging
import re
import unittest
from pathlib import Path
from urllib.parse import urljoin

from app.services.rider_docs import find_rider_sections, parse_rider_text, rider_link_candidates
from scripts import tariff_pipeline as tp

FIX = json.loads((Path(__file__).parent / "fixtures/r22/rider_docs_live_2026_10_09.json").read_text())


class Parse(unittest.TestCase):
    def test_georgia_fcr_seasonal_secondary(self):
        r = parse_rider_text(FIX["gp_fcr"])
        got = {p["season"]: p["rate_value"] for p in r["per_kwh"]}
        self.assertEqual(got, {"June through September": 0.038069, "October through May": 0.038561})

    def test_percent_of_base(self):
        self.assertEqual(parse_rider_text(FIX["gp_eccr"])["pct"], 13.0205)
        self.assertEqual(parse_rider_text(FIX["gp_dsm-r"])["pct"], 1.1969)

    def test_el_paso_column_and_class_row(self):
        self.assertEqual(parse_rider_text(FIX["ep98n"])["per_kwh"][0]["rate_value"], 0.016154)
        self.assertEqual(parse_rider_text(FIX["ep97"])["per_kwh"][0]["rate_value"], 0.000687)

    def test_tou_fuel_left_to_llm(self):
        self.assertIsNone(parse_rider_text(FIX["gp_tou-fcr"]))  # several period amounts: not unambiguous

    def test_evergy_book_sections(self):
        txt = FIX["evergy_book_excerpt"]
        fac = [parse_rider_text(s) for s in find_rider_sections(txt, re.compile(r"FUEL ADJUSTMENT CLAUSE"))]
        dsim = [parse_rider_text(s) for s in find_rider_sections(txt, re.compile(r"DEMAND SIDE INVESTMENT MECHANISM"))]
        self.assertEqual([r["per_kwh"][0]["rate_value"] for r in fac if r], [-0.00021])
        self.assertIn(0.00143, [r["per_kwh"][0]["rate_value"] for r in dsim if r])

    def test_conflict_returns_none(self):
        self.assertIsNone(parse_rider_text("Secondary customers 1.000¢ per kWh\nSecondary service 2.000¢ per kWh"))


class ScheduleRows(unittest.TestCase):
    def test_dominion_rows_tagged(self):
        r = parse_rider_text(FIX["dom_rider_e"])
        self.assertTrue(r["by_schedule"])
        row1 = [p for p in r["per_kwh"] if "1G" in p["schedules"]][0]
        self.assertEqual(row1["rate_value"], 0.000625)  # not Schedule 10 (Secondary) 0.0322c

    def test_single_statement(self):
        self.assertEqual(parse_rider_text(FIX["dom_rider_a"])["per_kwh"][0]["rate_value"], 0.037648)

    def test_fold_picks_plan_schedule_row(self):
        page = tp.RatePage(url="https://www.dominionenergy.com/x/rider-t1.pdf", page_type="pdf", content=FIX["dom_rider_t1"])
        plan = tp.ExtractedTariff(name="Schedule 1G Residential Service", code="1G", customer_class="residential",
                                  rate_type="flat", riders_referenced_not_shown=["Rider T1 Transmission"],
                                  components=[{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.05}])
        logging.disable(logging.WARNING)
        try:
            riders = tp.parse_rider_document_deterministic(page, "Rider T1 Transmission")
            tp.fold_batch_rider_schedules([plan], riders=riders)
        finally:
            logging.disable(logging.NOTSET)
        self.assertAlmostEqual(plan.components[0]["rate_value"], 0.05 + 0.01273, places=6)

    def test_no_schedule_match_not_folded(self):
        page = tp.RatePage(url="https://www.dominionenergy.com/x/rider-t1.pdf", page_type="pdf", content=FIX["dom_rider_t1"])
        plan = tp.ExtractedTariff(name="Residential Plan X", code="RX", customer_class="residential", rate_type="flat",
                                  riders_referenced_not_shown=["Rider T1 Transmission"],
                                  components=[{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.05}])
        logging.disable(logging.WARNING)
        try:
            riders = tp.parse_rider_document_deterministic(page, "Rider T1 Transmission")
            n = tp.fold_batch_rider_schedules([plan], riders=riders)
        finally:
            logging.disable(logging.NOTSET)
        self.assertEqual(n, 0)


class MonthlyTable(unittest.TestCase):
    def test_alabama_bill_calculation_factors(self):
        r = parse_rider_text(FIX["al_bcf_2026"])  # SEC column, mills/kWh
        got = {p["season"]: p["rate_value"] for p in r["per_kwh"]}
        self.assertEqual(got, {"October-May": 0.025392, "June-September": 0.028792})

    def test_alabama_factor_sheet_found(self):
        links = [urljoin("https://www.alabamapower.com/", u) for u in FIX["al_res_links"]]
        got = rider_link_candidates("Rate ECR (Energy Cost Recovery)", links, year=2026)
        self.assertTrue(got and got[0].endswith("bill-calculation-factors-2026.pdf"), got)


class Discovery(unittest.TestCase):
    def setUp(self):
        self.links = [urljoin("https://www.epelectric.com/", u) for u in FIX["ep_tx_links"]]

    def test_code_match(self):
        self.assertIn("schedule-98-fixed-fuel-factor-eff_07-01-2025",
                      rider_link_candidates("Rate Schedule No. 98 (Fixed Fuel Factor)", self.links)[0])

    def test_acronym_match(self):
        self.assertIn("no-97-eecrf", rider_link_candidates("Energy Efficiency Cost Recovery Factor", self.links)[0])

    def test_vague_hint_no_match(self):
        self.assertEqual(rider_link_candidates("rider amounts", self.links), [])


class Pipeline(unittest.TestCase):
    def test_deterministic_rider_page(self):
        page = tp.RatePage(url="https://www.georgiapower.com/x/fcr.pdf", page_type="pdf", content=FIX["gp_fcr"])
        logging.disable(logging.WARNING)
        try:
            out = tp.parse_rider_document_deterministic(page, "Fuel Cost Recovery Schedule")
        finally:
            logging.disable(logging.NOTSET)
        self.assertEqual(len(out), 1)
        self.assertEqual(sorted(c["rate_value"] for c in out[0].components), [0.038069, 0.038561])
        self.assertTrue(tp._is_rider_only_tariff(out[0]))

    def test_book_generic_riders(self):
        page = tp.RatePage(url="https://www.evergy.com/book.pdf", page_type="pdf", content=FIX["evergy_book_excerpt"])
        logging.disable(logging.WARNING)
        try:
            out = tp.extract_riders_from_main_tariff_book([page], ["Schedule FAC", "Schedule DSIM"])
        finally:
            logging.disable(logging.NOTSET)
        vals = {t.name: t.components[0]["rate_value"] for t in out}
        self.assertEqual(vals, {"Schedule FAC (tariff book)": -0.00021, "Schedule DSIM (tariff book)": 0.00143})

    def test_fold_el_paso(self):
        plan = tp.ExtractedTariff(name="Schedule No. 01 Residential Service Rate", customer_class="residential",
                                  rate_type="tiered", riders_referenced_not_shown=["Rate Schedule No. 98 (Fixed Fuel Factor)"],
                                  components=[{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.08885}])
        page = tp.RatePage(url="https://www.epelectric.com/98.pdf", page_type="pdf", content=FIX["ep98n"])
        logging.disable(logging.WARNING)
        try:
            riders = tp.parse_rider_document_deterministic(page, "Rate Schedule No. 98 (Fixed Fuel Factor)")
            n = tp.fold_batch_rider_schedules([plan], riders=riders)
        finally:
            logging.disable(logging.NOTSET)
        self.assertEqual(n, 1)
        self.assertAlmostEqual(plan.components[0]["rate_value"], 0.08885 + 0.016154, places=6)


if __name__ == "__main__":
    unittest.main()
