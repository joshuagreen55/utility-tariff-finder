"""R21 fix 7: source document for another state/province (R20 MidAmerican IA
priced from sd-electric-tariffs.pdf; DB-wide URLs checked 2026-10-08)."""
import logging
import unittest

from app.services.computable import tariff_contract
from app.services.jurisdiction import url_jurisdictions, wrong_jurisdiction
from scripts import tariff_pipeline as tp


class Jurisdiction(unittest.TestCase):
    def test_wrong(self):
        for url, st in [
            ("https://www.midamericanenergy.com/media/pdf/sd-electric-tariffs.pdf", "IA"),
            ("https://www.pacificpower.net/content/dam/pcorp/documents/en/pacificpower/rates-regulation/oregon/tariffs/a.pdf", "WA"),
            ("https://www.indianamichiganpower.com/lib/docs/ratesandtariffs/Michigan/IM_MI_TB_Bk_19_2026-04-27.pdf", "IN"),
            ("https://www.firstenergycorp.com/content/dam/customer/Customer%20Choice/Files/PA/tariffs/x.pdf", "OH"),
            ("https://www.eversource.com/x/rates-tariffs/electric-rates-new-hampshire", "ME"),
        ]:
            self.assertTrue(wrong_jurisdiction(url, st), url)

    def test_right_or_unknown(self):
        for url, st in [
            ("https://www.xcelenergy.com/staticfiles/xe/Regulatory/Regulatory%20PDFs/rates/MN/Me_Section_5.pdf", "MN"),
            ("https://www.evergy.com/-/media/documents/billing/missouri/kansas-city-rates.pdf", "MO"),
            ("https://comptroller.tn.gov/content/dam/cot/la/advanced-search/2023/utilities/x.pdf", "TN"),
            ("https://www.alabamapower.com/content/dam/alabama-power/pdfs-docs/Rates/xfdt.pdf", "AL"),
            ("https://www.sce.com/fr/save-money/rates", "CA"),
            ("https://www.pplelectric.com/-/media/pplelectric/at-your-service/docs/rates/in-effect.pdf", "PA"),
            ("https://example.com/rates.pdf", ""),
        ]:
            self.assertFalse(wrong_jurisdiction(url, st), url)
        self.assertEqual(url_jurisdictions("https://x/new-york/rates"), {"NY"})

    def test_phase4_flags_midamerican(self):
        ts = [tp.ExtractedTariff(name="Rate RS – Residential Service", customer_class="residential", rate_type="flat",
                                 source_url="https://www.midamericanenergy.com/media/pdf/sd-electric-tariffs.pdf",
                                 components=[{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.105}])]
        logging.disable(logging.WARNING)
        try:
            n = tp.flag_wrong_jurisdiction(ts, "IA")
        finally:
            logging.disable(logging.NOTSET)
        self.assertEqual(n, 1)
        self.assertIn("wrong_jurisdiction_document", ts[0].missing_fields)
        self.assertEqual(ts[0].confidence_notes["wrong_jurisdiction"]["document_states"], ["SD"])

    def test_contract(self):
        row = {"name": "Rate RS", "rate_type": "flat", "source_url": "https://x/a.pdf",
               "confidence_factors": {"wrong_jurisdiction": {"utility_state": "IA", "document_states": ["SD"]}},
               "rate_components": [{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.1},
                                   {"component_type": "fixed", "unit": "$/month", "rate_value": 9.0}]}
        self.assertIn("wrong_jurisdiction_document", tariff_contract(row)["computable_reasons"])


if __name__ == "__main__":
    unittest.main()
