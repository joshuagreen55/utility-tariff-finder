"""R19: newest official rate document + optional bill credits.

* SRP R18 live (``fixtures/r18/995_live.json``): the site blocked search, the
  run fell back to the undated ``ratebook.pdf`` (Nov 2023 book) although the
  utility's known URLs include the 2025 ratebook with the 2026 TCA.
* SRP R17 live (``fixtures/r17/995.json``): current books — must not flag.
* Pedernales R18 live (``fixtures/r18/890_live_phase3.json``): the flat plan
  (10.913¢) was rejected because autopay / paperless credits came back as
  negative fixed monthly charges.

    cd backend && python -m unittest tests.test_r19_newest_doc_and_credits -v
"""
from __future__ import annotations

import copy
import json
import logging
import unittest
from dataclasses import fields
from datetime import date
from pathlib import Path

from app.services.source_type import UtilitySourceContext
from scripts import tariff_pipeline as tp

FX = Path(__file__).resolve().parent / "fixtures"
_F = {f.name for f in fields(tp.ExtractedTariff)}
TODAY = date(2026, 10, 8)
SRP_KNOWN = json.loads((FX / "r18" / "995_known_urls.json").read_text())
NEW_BOOK = "2025-Ratebook-with-2026-TCA-with-cover.pdf"
OLD_BOOK = "blt38ed6b97faea59cb/ratebook.pdf"


def _mk(d):
    return tp.ExtractedTariff(**{k: copy.deepcopy(v) for k, v in d.items() if k in _F})


def _load(rel):
    return json.loads((FX / rel).read_text())


class Quiet(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)
        tp._RUN_DOC_CONTEXT.clear()

    def tearDown(self):
        logging.disable(logging.NOTSET)
        tp._RUN_DOC_CONTEXT.clear()


class DocumentVintage(Quiet):
    def test_url_vintage(self):
        v = tp.url_document_vintage
        self.assertEqual(v("https://a/x/2025-Ratebook-with-2026-TCA-with-cover.pdf", today=TODAY), (2026, 1))
        self.assertEqual(v("https://a/x/2024_-_11_Ratebook_Eff_Nov_2024.pdf", today=TODAY), (2024, 11))
        self.assertEqual(v("http://a/prices/pdfx/April2015/E-26.pdf", today=TODAY), (2015, 4))
        self.assertIsNone(v("https://a/x/" + OLD_BOOK, today=TODAY))
        self.assertIsNone(v("https://a/2027-rates.pdf", today=TODAY))  # future

    def test_text_vintage_from_cover(self):
        old = "SALT RIVER PROJECT\nPrice Plans Effective with the\nNovember 2023 Billing Cycle\n"
        self.assertEqual(tp.text_document_vintage(old, today=TODAY), (2023, 11))
        self.assertEqual(
            tp.text_document_vintage("Prices for all rate plans on this page are effective March 1, 2026.", today=TODAY),
            (2026, 3),
        )
        self.assertIsNone(tp.text_document_vintage("Rates effective January 2027", today=TODAY))

    def test_newest_dated_rate_document_ignores_proposed_and_news(self):
        urls = SRP_KNOWN + ["https://srp.example/2026-proposed-price-plans.pdf"]
        url, v = tp.newest_dated_rate_document(urls, today=TODAY)
        self.assertTrue(url.endswith(NEW_BOOK))
        self.assertEqual(v, (2026, 1))


class FallbackPicksNewest(Quiet):
    def test_srp_fallback_prefers_2026_book(self):
        primary, pool = tp.prefer_official_targets("", [], SRP_KNOWN, UtilitySourceContext())
        self.assertTrue(primary.endswith(NEW_BOOK), primary)
        self.assertIn(
            next(u for u in SRP_KNOWN if u.endswith(OLD_BOOK)), pool,
        )

    def test_no_dated_doc_keeps_existing_choice(self):
        urls = ["https://u.example/rates/residential", "https://u.example/tariffs/old.pdf"]
        primary, _ = tp.prefer_official_targets("", [], urls, UtilitySourceContext())
        self.assertEqual(primary, urls[0])

    def test_locked_primary_untouched(self):
        primary, _ = tp.prefer_official_targets(
            "https://u.example/override.pdf", [], SRP_KNOWN, UtilitySourceContext(), locked=True,
        )
        self.assertEqual(primary, "https://u.example/override.pdf")


class OlderDocumentFlag(Quiet):
    def test_r18_srp_plans_flagged(self):
        plans = [_mk(d) for d in _load("r18/995_live.json")["phase4_valid"]]
        n = tp.flag_older_source_documents(
            plans, {"page_vintages": {}, "known_urls": SRP_KNOWN}, today=TODAY,
        )
        self.assertEqual(n, 6)
        for t in plans:
            self.assertTrue(t.needs_review, t.name)
            self.assertIn("older_rate_document", t.missing_fields)
            note = t.confidence_notes["older_rate_document"]
            self.assertEqual(note["plan_document_date"], "2023-11")
            self.assertTrue(note["newer_official_document"].endswith(NEW_BOOK))

    def test_r18_srp_with_cover_text_vintage(self):
        plans = [_mk(d) for d in _load("r18/995_live.json")["phase4_valid"]]
        src = plans[0].source_url
        n = tp.flag_older_source_documents(
            plans, {"page_vintages": {src: (2023, 11)}, "known_urls": SRP_KNOWN}, today=TODAY,
        )
        self.assertEqual(n, 6)

    def test_r17_current_books_not_flagged(self):
        plans = [_mk(d) for d in _load("r17/995.json")["phase4_valid"]]
        pv = {}
        for t in plans:
            if "Temporary-FPPAM" in t.source_url:
                pv[t.source_url] = (2026, 5)
            elif "TCA_and_Export" in t.source_url:
                pv[t.source_url] = (2025, 11)
        n = tp.flag_older_source_documents(
            plans, {"page_vintages": pv, "known_urls": SRP_KNOWN}, today=TODAY,
        )
        self.assertEqual(n, 0)
        self.assertFalse(any("older_rate_document" in (t.missing_fields or []) for t in plans))

    def test_r17_without_page_text_not_flagged(self):
        plans = [_mk(d) for d in _load("r17/995.json")["phase4_valid"]]
        n = tp.flag_older_source_documents(
            plans, {"page_vintages": {}, "known_urls": SRP_KNOWN}, today=TODAY,
        )
        self.assertEqual(n, 0)

    def test_no_context_no_flag(self):
        plans = [_mk(d) for d in _load("r18/995_live.json")["phase4_valid"]]
        self.assertEqual(tp.flag_older_source_documents(plans, {}, today=TODAY), 0)

    def test_rider_doc_dates_do_not_count(self):
        # Base book 2025-01, a 2026 rider PDF in known URLs: not a rate book.
        t = tp.ExtractedTariff(
            name="Residential", code="R", customer_class="residential", rate_type="flat",
            description="", source_url="https://u.example/tariff-book.pdf",
            effective_date="2025-01-01", confidence=0.9,
            components=[{"component_type": "energy", "rate_value": 0.12, "unit": "$/kWh"}],
        )
        n = tp.flag_older_source_documents(
            [t], {"page_vintages": {}, "known_urls": ["https://u.example/2026-06-fuel-rider.pdf"]},
            today=TODAY,
        )
        self.assertEqual(n, 0)

    def test_phase4_uses_run_context(self):
        fx = _load("r18/995_live.json")
        tp._RUN_DOC_CONTEXT.update({"page_vintages": {}, "known_urls": SRP_KNOWN})
        rep, valid = tp.phase4_validate([_mk(d) for d in fx["phase4_valid"]], fx["utility_name"], fx["state"])
        self.assertEqual(rep["valid"], 6)
        self.assertEqual(rep["older_rate_document_flagged"], 6)
        self.assertTrue(all(t.needs_review for t in valid))


class NewestEffectiveDateWinsDuplicate(Quiet):
    def test_same_plan_two_vintages(self):
        def flat(v, eff, url):
            return tp.ExtractedTariff(
                name="Residential Service", code="", customer_class="residential",
                rate_type="flat", description="", source_url=url, effective_date=eff,
                confidence=0.9,
                components=[{"component_type": "energy", "rate_value": v, "unit": "$/kWh"}],
            )
        old = flat(0.11, "2023-11-01", "https://u.example/2023-book.pdf")
        new = flat(0.12, "2025-11-01", "https://u.example/2025-book.pdf")
        kept, actions = tp.dedupe_same_plan_variants([old, new], "Example Power")
        self.assertEqual(len(kept), 1)
        self.assertIs(kept[0], new)
        self.assertFalse(kept[0].needs_review)
        self.assertIn("older_copy_prices", kept[0].confidence_notes)


class BillCredits(Quiet):
    def _flat(self):
        fx = _load("r18/890_live_phase3.json")
        return fx, [_mk(d) for d in fx["phase3_tariffs"] if "Flat" in d["name"]][0]

    def test_pedernales_flat_survives(self):
        fx, flat = self._flat()
        rep, valid = tp.phase4_validate([flat], fx["utility_name"], fx["state"])
        self.assertEqual(rep["valid"], 1, rep["issues"])
        t = valid[0]
        energy = [c for c in t.components if c["component_type"] == "energy"]
        self.assertEqual([round(float(c["rate_value"]) * 100, 3) for c in energy], [10.913])
        fixed = [c for c in t.components if c["component_type"] == "fixed"]
        self.assertEqual([float(c["rate_value"]) for c in fixed], [32.5])
        credits = t.confidence_notes["bill_credits"]
        self.assertEqual(sorted(c["amount"] for c in credits), [-1.5, -1.0])
        self.assertTrue(all(c["optional"] for c in credits))

    def test_full_r18_pedernales_batch(self):
        fx = _load("r18/890_live_phase3.json")
        rep, valid = tp.phase4_validate([_mk(d) for d in fx["phase3_tariffs"]], fx["utility_name"], fx["state"])
        self.assertEqual(rep["invalid"], 0, rep["issues"])
        names = [t.name for t in valid]
        self.assertTrue(any("Flat" in n for n in names), names)

    def test_positive_fixed_untouched(self):
        t = tp.ExtractedTariff(
            name="X", code="", customer_class="residential", rate_type="flat",
            description="", source_url="", effective_date="", confidence=0.9,
            components=[
                {"component_type": "fixed", "rate_value": 10.0, "unit": "$/month"},
                {"component_type": "energy", "rate_value": 0.1, "unit": "$/kWh"},
            ],
        )
        self.assertEqual(tp.move_negative_fixed_credits_to_notes(t), 0)
        self.assertEqual(len(t.components), 2)


if __name__ == "__main__":
    unittest.main()
