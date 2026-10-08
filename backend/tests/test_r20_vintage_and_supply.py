"""R20: post-save "newest edition" clean-up + supply-only price sheets.

* DTE / FPL R19 trial (``fixtures/r19/{304,381}_vintage_rows.json``): the
  old clean-up chained distinct plans (Standard Base D1.11, Time of Day
  11-7 D1.2 ... all retired onto Geothermal D1.7; FPL "RS-1 EV Full" onto
  "RS-1 EV Equipment only"). Rows are the live set at the moment the
  clean-up ran.
* PSE&G R19 trial (``fixtures/r19/947_trial_phase3.json``): three
  supply-only BGS plans from the Sept-2025 price-to-compare sheet.

    cd backend && python -m unittest tests.test_r20_vintage_and_supply -v
"""
from __future__ import annotations

import copy
import json
import logging
import unittest
from dataclasses import dataclass, fields
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from scripts import tariff_pipeline as tp

FX = Path(__file__).resolve().parent / "fixtures"
_F = {f.name for f in fields(tp.ExtractedTariff)}
TODAY = date(2026, 10, 8)
PSEG_BOOK = ("https://nj.pseg.com/-/media/pseg/public-site/documents/current-electric-tariff/"
             "electric-tariff-17-cefev-iap-nits-usf-effective-20261001.ashx")
PSEG_PTC = ("https://nj.pseg.com/-/media/pseg/public-site/documents/current-electric-tariff/"
            "electric-ptc-website--2025-09-15.ashx")


def _mk(d):
    return tp.ExtractedTariff(**{k: copy.deepcopy(v) for k, v in d.items() if k in _F})


def _load(rel):
    return json.loads((FX / rel).read_text())


@dataclass
class Row:
    id: int
    name: str
    code: str | None
    rate_type: str
    effective_date: date | None
    source_url: str | None
    customer_class: str = "residential"
    source_document_hash: str | None = None
    approved: bool = False
    superseded_by_tariff_id: int | None = None
    supersede_reason: str | None = None
    last_verified_at: object = None


def _rows(uid, refreshed=()):
    out = []
    for r in _load(f"r19/{uid}_vintage_rows.json"):
        if r["id"] in refreshed:
            continue  # superseded by the refresh step before the clean-up ran
        out.append(Row(
            id=r["id"], name=r["name"], code=r["code"], rate_type=r["rate_type"].lower(),
            effective_date=date.fromisoformat(r["effective_date"]) if r["effective_date"] else None,
            source_url=r["source_url"], source_document_hash=r["source_document_hash"],
        ))
    return out


def _run_cleanup(rows):
    """Run supersede_older_vintages on an in-memory live set."""
    retired = []

    def _sup(session, loser, successor=None, reason="vintage", **kw):
        loser.supersede_reason = reason
        retired.append((loser.id, getattr(successor, "id", None)))

    session = SimpleNamespace(execute=lambda *_a, **_k: SimpleNamespace(
        scalars=lambda: SimpleNamespace(all=lambda: list(rows))))
    with mock.patch("app.services.tariff_history.supersede_tariff", _sup), \
            mock.patch("app.services.tariff_history.record_event", lambda *a, **k: None), \
            mock.patch("app.services.tariff_history.is_protected", lambda t: False):
        n = tp.supersede_older_vintages(session, 1)
    return n, retired


class Quiet(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)
        tp._RUN_DOC_CONTEXT.clear()

    def tearDown(self):
        logging.disable(logging.NOTSET)
        tp._RUN_DOC_CONTEXT.clear()


class SameEditionRules(Quiet):
    def test_different_codes_never_match(self):
        self.assertFalse(tp.same_vintage_product(
            "Standard Base Rate (D1.11)", "Geothermal Time of Day Rate (D1.7)",
            rate_type_a="seasonal_tou", rate_type_b="seasonal_tou"))

    def test_qualifier_words_mark_different_plans(self):
        for a, b in [
            ("Time of Day Rate", "Geothermal Time of Day Rate (D1.7)"),
            ("RS-1 EV Full Installation", "RS-1 EV Equipment only Installation"),
            ("Time of Day 3 p.m. – 7 p.m. Rate", "Time of Day 11 a.m. - 7 p.m. Rate (D1.2)"),
        ]:
            self.assertFalse(tp.same_vintage_product(a, b, rate_type_a="tou", rate_type_b="tou"), (a, b))

    def test_true_editions_still_match(self):
        self.assertTrue(tp.same_vintage_product(
            "Rate #1.1 Domestic Service", "Domestic Service (Flat)", code_a="1.1",
            rate_type_a="flat", rate_type_b="flat"))
        self.assertTrue(tp.same_vintage_product(
            "Overnight Savers Rate", "Overnight Savers Rate (D1.13)",
            rate_type_a="seasonal_tou", rate_type_b="seasonal_tou"))
        self.assertTrue(tp.same_vintage_product(
            "Residential Service 2025", "Residential Service - Revised 2026",
            rate_type_a="flat", rate_type_b="flat"))

    def test_no_chaining(self):
        # A~B and B~C (each via a shared code) but A !~ C: never one group.
        a = Row(1, "Residential Service (RS)", "RS", "flat", date(2025, 1, 1), "u1")
        b = Row(2, "Residential Service", None, "flat", date(2026, 1, 1), "u2")
        c = Row(3, "Residential Service (RS-2)", "RS-2", "flat", date(2026, 6, 1), "u3")
        for g in tp.group_live_tariffs_by_vintage([a, b, c]):
            self.assertFalse({1, 3} <= {t.id for t in g})

    def test_newer_edition_rules(self):
        new = Row(1, "X", None, "flat", date(2026, 7, 1), "doc-2026")
        old = Row(2, "X", None, "flat", date(2025, 7, 1), "doc-2025")
        same = Row(3, "X", None, "flat", date(2026, 7, 1), "doc-2026")
        undated_same_doc = Row(4, "X", None, "flat", None, "doc-2026")
        undated_other_doc = Row(5, "X", None, "flat", None, "page.html")
        future = Row(6, "X", None, "flat", date(2027, 1, 1), "doc-2027")
        self.assertTrue(tp.is_newer_edition(new, old, today=TODAY))
        self.assertFalse(tp.is_newer_edition(new, same, today=TODAY))
        self.assertFalse(tp.is_newer_edition(new, undated_same_doc, today=TODAY))
        self.assertTrue(tp.is_newer_edition(new, undated_other_doc, today=TODAY))
        self.assertFalse(tp.is_newer_edition(future, new, today=TODAY))
        self.assertFalse(tp.is_newer_edition(old, new, today=TODAY))


class R19TrialReplay(Quiet):
    def test_dte_real_plans_survive(self):
        rows = _rows(304, refreshed={60249})
        _n, retired = _run_cleanup(rows)
        retired_ids = {r[0] for r in retired}
        for keep in (71302, 71303, 71304, 71305, 71306, 71307, 71308, 71309, 71310, 71311):
            self.assertNotIn(keep, retired_ids)
        # Nothing may be retired onto Geothermal D1.7.
        self.assertFalse([r for r in retired if r[1] == 71307])

    def test_fpl_ev_options_survive(self):
        rows = _rows(381, refreshed={70852, 70859})
        _n, retired = _run_cleanup(rows)
        retired_ids = {r[0] for r in retired}
        for keep in (71292, 71293, 71294, 71295, 71297, 71298):
            self.assertNotIn(keep, retired_ids)

    def test_real_vintage_still_retired(self):
        old = Row(10, "Domestic Service (Flat)", None, "flat", date(2025, 7, 1), "nl-2025.pdf")
        new = Row(11, "Rate #1.1 Domestic Service", "1.1", "flat", date(2026, 7, 1), "nl-2026.pdf")
        other = Row(12, "Domestic Space Heating", None, "flat", date(2025, 7, 1), "nl-2025.pdf")
        _n, retired = _run_cleanup([old, new, other])
        self.assertEqual(retired, [(10, 11)])

    def test_ambiguous_old_row_kept(self):
        old = Row(20, "Time of Day", None, "tou", None, "page.html")
        a = Row(21, "Time of Day (D1.11)", "D1.11", "tou", date(2026, 2, 1), "card.pdf")
        b = Row(22, "Time of Day (D1.2)", "D1.2", "tou", date(2026, 2, 1), "card.pdf")
        _n, retired = _run_cleanup([old, a, b])
        self.assertEqual(retired, [])


class SupplySheets(Quiet):
    def test_url_rules(self):
        self.assertEqual(tp.url_document_vintage(PSEG_BOOK, today=TODAY), (2026, 10))
        self.assertEqual(tp.url_document_vintage(PSEG_PTC, today=TODAY), (2025, 9))
        self.assertEqual(tp.url_document_vintage(
            "https://x/electric-tariff-16-bpu-bgs-nitstec-effective-09012023.ashx", today=TODAY), (2023, 9))
        self.assertTrue(tp.is_supply_sheet_url(PSEG_PTC))
        self.assertFalse(tp.is_supply_sheet_url(PSEG_BOOK))
        self.assertFalse(tp.is_supply_sheet_url(
            "https://x/electric-tariff-16-bpu-bgs-nitstec-effective-09012023.ashx"))

    def test_pseg_supply_only_never_stored(self):
        ts = [_mk(t) for t in _load("r19/947_trial_phase3.json")]
        self.assertTrue(all(tp.is_supply_only_plan(t) for t in ts))
        tp.set_run_document_context([], [PSEG_BOOK])
        kept, info = tp.reconcile_same_utility_plans(ts, "PSE&G", ts)
        self.assertEqual(kept, [])
        self.assertEqual(len(info["supply_only_dropped"]), 3)

    def test_delivery_plus_supply_combined_and_labelled(self):
        supply = [_mk(t) for t in _load("r19/947_trial_phase3.json")]
        rs_supply = [t for t in supply if t.name.startswith("RS ")][0]
        rs_supply.source_url = PSEG_BOOK
        rs_supply.effective_date = "2026-06-01"
        delivery = tp.ExtractedTariff(
            name="RS - Residential Service", customer_class="residential", rate_type="seasonal",
            source_url=PSEG_BOOK, effective_date="2026-10-01",
            missing_fields=["BGS supply charges are on a separate sheet"],
            components=[
                {"component_type": "fixed", "rate_value": 7.07, "unit": "$/month", "tier_label": "Service Charge"},
                {"component_type": "energy", "rate_value": 0.061, "unit": "$/kWh", "tier_label": "Distribution charge"},
            ])
        kept, info = tp.combine_supply_with_delivery([delivery, rs_supply])
        self.assertEqual(len(kept), 1)
        plan = kept[0]
        self.assertIn("delivery + default supply", plan.name)
        types = [c["component_type"] for c in plan.components]
        self.assertIn("fixed", types)
        self.assertTrue(any(c["component_type"] == "adjustment" and "Delivery" in c["tier_label"]
                            for c in plan.components))
        self.assertTrue(any(c["component_type"] == "energy" for c in plan.components))
        self.assertEqual(plan.confidence_notes["combined_supply"]["label"], "delivery + default supply")
        self.assertFalse(any("supply" in m.lower() for m in plan.missing_fields))
        self.assertEqual(info["supply_combined"], [plan.name])

    def test_stale_supply_sheet_flagged(self):
        ts = [_mk(t) for t in _load("r19/947_trial_phase3.json")]
        n = tp.flag_supply_sheet_currency(ts, {"page_vintages": {}, "known_urls": [PSEG_BOOK]}, today=TODAY)
        self.assertEqual(n, 3)
        self.assertIn("supply_sheet_not_current", ts[0].missing_fields)

    def test_bundled_plans_untouched(self):
        for uid in (304, 381):
            ts = [_mk(t) for t in _load(f"r19/{uid}_trial_phase3.json")]
            self.assertFalse([t.name for t in ts if tp.is_supply_only_plan(t)], uid)
        pec = [_mk(t) for t in _load("r18/890_live_phase3.json")["phase3_tariffs"]]
        self.assertFalse([t.name for t in pec if tp.is_supply_only_plan(t)])


class Phase1SupplySheet(Quiet):
    def test_supply_sheet_not_chosen_as_rate_page(self):
        results = [
            {"url": PSEG_PTC, "title": "PTC"},
            {"url": "https://electricrates.org/blog/jersey-city-electricity-rates/", "title": "blog"},
            {"url": "https://nj.pseg.com/aboutpseg/regulatorypage/pricetocompare", "title": "ptc"},
            {"url": "https://nj.pseg.com/aboutpseg/regulatorypage/electrictariffs", "title": "tariffs"},
        ]
        scores = {results[0]["url"]: 58, results[1]["url"]: 34, results[2]["url"]: 16, results[3]["url"]: 9}
        with mock.patch.object(tp, "preferred_rate_page_url", lambda n: None), \
                mock.patch.object(tp, "brave_search", lambda q, count=10: results), \
                mock.patch.object(tp, "score_search_result", lambda r, *a: scores[r["url"]]), \
                mock.patch.object(tp, "fetch_page", lambda u: ("<html>rates</html>", "text/html", 200)):
            best, _n, alts = tp.phase1_find_rate_page("Public Service Elec & Gas Co", "NJ", "https://nj.pseg.com")
        self.assertEqual(best, "https://nj.pseg.com/aboutpseg/regulatorypage/electrictariffs")
        self.assertIn(PSEG_PTC, alts)


class ProtectedTie(Quiet):
    def test_protected_row_absorbs_scraped_same_date_copy_only(self):
        rep = Row(30, "Rate #1.1 Domestic Service", "1.1", "flat", date(2026, 5, 1), "u/rates", approved=True)
        scraped = Row(31, "Domestic Service (Flat)", None, "flat", date(2026, 5, 1), "u/rates")
        other = Row(32, "Geothermal Domestic Service", None, "flat", date(2026, 5, 1), "u/rates")
        retired = []

        def _sup(session, loser, successor=None, reason="vintage", **kw):
            retired.append((loser.id, getattr(successor, "id", None)))

        session = SimpleNamespace(execute=lambda *_a, **_k: SimpleNamespace(
            scalars=lambda: SimpleNamespace(all=lambda: [rep, scraped, other])))
        with mock.patch("app.services.tariff_history.supersede_tariff", _sup), \
                mock.patch("app.services.tariff_history.record_event", lambda *a, **k: None), \
                mock.patch("app.services.tariff_history.is_protected", lambda t: bool(t.approved)):
            tp.supersede_older_vintages(session, 1)
        self.assertEqual(retired, [(31, 30)])


class CrawlNewestFirst(Quiet):
    def test_current_tariff_before_purpa_and_old_sheets(self):
        base = "https://nj.pseg.com/-/media/pseg/public-site/documents/"
        links = [(base + f"purpa/pep_-rates_20{y}_12.ashx", "PEP") for y in range(14, 24)]
        links += [(base + "purpa/new_rates_residual_2026.ashx", "residual")]
        links += [(base + "current-electric-tariff/reconciliation-charge---dec-2023-qtr.ashx", "recon 2023")]
        links += [("https://nj.pseg.com/aboutpseg/regulatorypage/pricetocompare", "ptc")]
        links += [(PSEG_BOOK, "Electric Tariff")]
        out = tp.order_links_newest_first(links, today=TODAY)
        self.assertEqual(out[0][0], PSEG_BOOK)
        self.assertLess(out.index(links[-2]), out.index(links[0]))  # undated page before PURPA
        self.assertTrue(all("purpa" in u for u, _ in out[-11:]) or "dec-2023" in out[-12][0])
