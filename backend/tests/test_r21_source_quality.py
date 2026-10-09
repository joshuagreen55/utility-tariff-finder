"""R21 fix 5: official tariff over retailer offers / rounded marketing pages.

Fixture: R20 phase-3 outputs (SCE 1064, Consumers 249, ATCO 1718, Alabama 10).
"""
import dataclasses
import json
import logging
import unittest
from pathlib import Path

from app.services.computable import tariff_contract
from app.services.source_quality import (
    is_marketing_page,
    is_retail_offer,
    marketing_rounded_price,
)
from scripts import tariff_pipeline as tp

FIX = json.loads((Path(__file__).parent / "fixtures/r21/source_quality_r20_phase3.json").read_text())
F = {f.name for f in dataclasses.fields(tp.ExtractedTariff)}


def _mk(d):
    return tp.ExtractedTariff(**{k: v for k, v in d.items() if k in F})


def _run(uid):
    rec = FIX[uid]
    logging.disable(logging.WARNING)
    try:
        return tp.phase4_validate([_mk(t) for t in rec["phase3_tariffs"]], rec["utility_name"], rec["state"] or "")
    finally:
        logging.disable(logging.NOTSET)


class RetailOffer(unittest.TestCase):
    def test_retail_names(self):
        for nm, url in [
            ("2-year Fixed Electricity", "https://energy.atco.com/offers/switch-today"),
            ("2-year Fixed Bundle (Electricity + Natural Gas)", "https://energy.atco.com/offers/switch-today"),
            ("3-year Guaranteed Rate Plan", "https://energy.atco.com/offers/switch-today"),
            ("Fixed Rate Plan", "https://energy.atco.com/home-energy/plan-selector"),
            ("Encor Floating", "https://www.epcor.com/ca/en/ab/edmonton/start-services/home/compare-electricity-plans"),
            ("24 Month Electric Vehicle FreeCharge Plan", "https://www.powerchoicetexas.org/providers/x"),
            ("6 Month Home Power Plan", "https://www.paenergyratings.com/electricity-rates/pa/x"),
        ]:
            self.assertTrue(is_retail_offer(nm, url), nm)

    def test_utility_tariffs_not_retail(self):
        for nm, url in [
            ("Residential Dual Fuel", "https://www.aelp.com/Customer-Service/Rates-Billing/Current-Rates"),
            ("Standard Residential Service", "https://electric.atco.com/x/2026-01-01-atco-electric-price-schedules.pdf"),
            ("Regulated Rate Option", "https://www.epcor.com/ca/en/ab/edmonton/rates/rro.html"),
            ("Rate Schedule RS - Residential Service", "https://www.pplelectric.com/rates"),
            ("Fixed Price Option GSC-1 Schedule", "https://www.pplelectric.com/rates"),
            ("TOU-D-PRIME", "https://www.sce.com/fr/save-money/rates-financing/residential-rate-plans/time-of-use-plans"),
        ]:
            self.assertFalse(is_retail_offer(nm, url), nm)

    def test_atco_retail_dropped_utility_kept(self):
        rep, valid = _run("1718")
        names = {t.name for t in valid}
        self.assertFalse(any("Fixed" in n or "Guaranteed" in n for n in names), names)


class MarketingRounded(unittest.TestCase):
    def test_paths(self):
        self.assertTrue(is_marketing_page("https://www.sce.com/fr/save-money/rates-financing/residential-rate-plans/time-of-use-plans"))
        self.assertTrue(is_marketing_page("https://www.sce.com/pt-pt/residential/rates/time-of-use"))
        self.assertTrue(is_marketing_page("https://www.consumersenergy.com/residential/account-and-billing/rates/electric-rates-and-programs/rate-plan-options/nighttime-savers"))
        self.assertFalse(is_marketing_page("https://www.oeb.ca/consumer-information-and-protection/electricity-rates"))
        self.assertFalse(is_marketing_page("https://www.torontohydro.com/for-home/rates"))
        self.assertFalse(is_marketing_page("https://vernonelectric.org/rates"))
        self.assertFalse(is_marketing_page("https://www.sce.com/save-money/x/tariff.pdf"))

    def test_rounding(self):
        u = "https://www.sce.com/save-money/rates-financing/residential-rate-plans/x"
        r = [{"component_type": "energy", "rate_value": 0.32}, {"component_type": "energy", "rate_value": 0.44}]
        p = [{"component_type": "energy", "rate_value": 0.31612}, {"component_type": "energy", "rate_value": 0.44}]
        self.assertTrue(marketing_rounded_price(u, r))
        self.assertFalse(marketing_rounded_price(u, p))
        self.assertFalse(marketing_rounded_price("https://www.oeb.ca/rates", r))

    def test_sce_flagged_not_complete(self):
        rep, valid = _run("1064")
        self.assertEqual(len(valid), 3)
        for t in valid:
            self.assertEqual(t.confidence_notes.get("price_basis"), "marketing_rounded", t.name)
            self.assertTrue(t.needs_review)
            self.assertIn("marketing_page_rounded_price", t.missing_fields)
        self.assertEqual(rep.get("marketing_rounded_plans"), 3)

    def test_consumers_nighttime_flagged(self):
        _, valid = _run("249")
        by = {t.name: t for t in valid}
        self.assertEqual(by["Nighttime Savers Rate"].confidence_notes.get("price_basis"), "marketing_rounded")

    def test_alabama_pdfs_untouched(self):
        rep, valid = _run("10")
        self.assertTrue(valid)
        for t in valid:
            self.assertNotEqual((t.confidence_notes or {}).get("price_basis"), "marketing_rounded")
        self.assertFalse(rep.get("retail_offers_dropped"))


class Contract(unittest.TestCase):
    def _row(self, name, url, vals, cf=None):
        return {
            "name": name, "source_url": url, "rate_type": "flat", "customer_class": "residential",
            "confidence_factors": cf or {},
            "rate_components": [{"component_type": "energy", "unit": "$/kWh", "rate_value": v} for v in vals]
            + [{"component_type": "fixed", "unit": "$/month", "rate_value": 10.0}],
        }

    def test_stored_retail_row_not_complete(self):
        c = tariff_contract(self._row("2-year Fixed Electricity", "https://energy.atco.com/offers/switch-today", [0.12]))
        self.assertIn("retail_offer_not_tariff", c["computable_reasons"])
        self.assertFalse(c["computable"])

    def test_stored_marketing_row_not_complete(self):
        c = tariff_contract(self._row("TOU-D-PRIME", "https://www.sce.com/fr/save-money/residential-rate-plans/x", [0.24, 0.61]))
        self.assertIn("marketing_page_rounded_price", c["computable_reasons"])

    def test_official_row_unaffected(self):
        c = tariff_contract(self._row("Standard Residential", "https://www.oeb.ca/rates", [0.098]))
        self.assertNotIn("marketing_page_rounded_price", c["computable_reasons"])
        self.assertNotIn("retail_offer_not_tariff", c["computable_reasons"])


if __name__ == "__main__":
    unittest.main()
