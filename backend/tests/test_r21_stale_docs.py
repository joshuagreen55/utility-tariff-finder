"""R21 fix 6: out-of-date documents (PDF modified date + printed dates).

Fixture: R20 plans (Alabama 10, El Paso 338, Xcel MN 819, APS 51) and the
PDF modified dates read from the same documents on 2026-10-08.
"""
import json
import logging
import unittest
from datetime import date
from pathlib import Path

from app.services.computable import tariff_contract
from scripts import tariff_pipeline as tp

FIX = json.loads((Path(__file__).parent / "fixtures/r21/stale_docs_r20.json").read_text())
TODAY = date(2026, 10, 8)
CTX = {"page_vintages": {}, "known_urls": [], "pdf_modified": {u: tuple(v) for u, v in FIX["pdf_modified"].items()}}


def _plans(uid):
    return [tp.ExtractedTariff(name=p["name"], customer_class="residential", rate_type="flat",
                               effective_date=p["eff"], source_url=p["src"],
                               components=[{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.1}])
            for p in FIX["plans"][uid]]


def _flag(uid, ctx=CTX):
    ts = _plans(uid)
    logging.disable(logging.WARNING)
    try:
        tp.flag_stale_source_documents(ts, ctx, today=TODAY)
    finally:
        logging.disable(logging.NOTSET)
    return ts


class PdfModDate(unittest.TestCase):
    def test_info_and_xmp(self):
        s = FIX["pdf_moddate_snippets"]
        self.assertEqual(tp.pdf_modified_vintage(s["alabama_xfdt"].encode()), (2024, 12))
        self.assertEqual(tp.pdf_modified_vintage(s["elpaso_rate01_xmp"].encode()), (2018, 1))
        self.assertIsNone(tp.pdf_modified_vintage(b"%PDF-1.4 no metadata"))


class StaleDocs(unittest.TestCase):
    def test_alabama_2010_sheet_reissued_2024_is_current(self):
        for t in _flag("10"):
            self.assertNotIn("stale_rate_document", t.missing_fields, t.name)

    def test_xcel_mn_2019_book_not_complete(self):
        ts = _flag("819")
        self.assertTrue(ts)
        for t in ts:
            self.assertIn("stale_rate_document", t.missing_fields)
            self.assertTrue(t.needs_review)
            self.assertEqual(t.confidence_notes["price_basis"], "stale_document")
            self.assertEqual(t.confidence_notes["stale_rate_document"]["evidence"], "pdf_modified")

    def test_el_paso_2018_not_complete(self):
        for t in _flag("338"):
            self.assertEqual(t.confidence_notes.get("price_basis"), "stale_document")

    def test_aps_31_months_flag_only(self):
        for t in _flag("51"):
            self.assertIn("stale_rate_document", t.missing_fields)
            self.assertNotEqual(t.confidence_notes.get("price_basis"), "stale_document")

    def test_printed_date_only_is_flag_only(self):
        # No document-level evidence (HTML page / no metadata): review flag, still countable.
        for t in _flag("819", ctx={}):
            self.assertIn("stale_rate_document", t.missing_fields)
            self.assertNotEqual(t.confidence_notes.get("price_basis"), "stale_document")

    def test_contract_reason(self):
        row = {"name": "Residential Service", "rate_type": "flat", "source_url": "https://x/a.pdf",
               "confidence_factors": {"price_basis": "stale_document"},
               "rate_components": [{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.1},
                                   {"component_type": "fixed", "unit": "$/month", "rate_value": 9.0}]}
        c = tariff_contract(row)
        self.assertIn("stale_rate_document", c["computable_reasons"])
        self.assertFalse(c["computable"])


if __name__ == "__main__":
    unittest.main()
