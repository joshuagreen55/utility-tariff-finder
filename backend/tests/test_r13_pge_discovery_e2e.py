"""R13/R14: PGE discovery must fetch current Sched_007 — not only all_tariffs_56_.

The R12 golden handed Sched_007 into phase4 directly, so CI passed while the
live Phase 1→2 path still extracted the 2020 combined book. This test drives
the real discovery + fetch + rider-enrich + post-process path on saved
HTML/PDF inputs: search hit → combined book, schedule index page-data →
Sched_007 + Sch 1xx via ``enrich_tariffs_with_referenced_rider_docs``.

R14: deterministic Sch 7 carries ``riders_referenced_not_shown=["Schedule 100"]``
from the sheet footnote; Sch 1xx must arrive through the enrich/fetch step,
not by hand-injection past it.

No live network, no paid LLM.

    cd backend && python -m unittest tests.test_r13_pge_discovery_e2e -v
"""
from __future__ import annotations

import logging
import re
import unittest
from pathlib import Path
from unittest import mock

from scripts import tariff_pipeline as tp

R13 = Path(__file__).resolve().parent / "fixtures" / "r13" / "pge"
ALL_TARIFFS_URL = (
    "https://assets.ctfassets.net/416ywc1laqmd/2je1W1MvdK5QDMqTHP61tm/"
    "55061789ca52d46ab38ed16e348a7993/all_tariffs_56_.pdf"
)


def _energy_cents(t) -> list[float]:
    out: list[float] = []
    for c in t.components or []:
        if str(c.get("component_type") or "").lower() != "energy":
            continue
        try:
            v = float(c.get("rate_value") or 0)
        except (TypeError, ValueError):
            continue
        unit = str(c.get("unit") or "")
        out.append(round(v * 100, 3) if unit.startswith("$") else round(v, 3))
    return sorted(set(out))


def _fixture_text(name: str) -> str:
    return (R13 / name).read_text()


def _sched_num_from_url(url: str) -> str | None:
    m = re.search(r"Sched_(\d{3})\.pdf", url or "", re.I)
    return m.group(1) if m else None


class TestR13PgeDiscoveryE2E(unittest.TestCase):
    """Search returns 2020 combined book; pipeline must still use Sched_007."""

    def setUp(self):
        logging.disable(logging.CRITICAL)
        self.page_data = _fixture_text("page-data.json")
        self.url_map = tp._pge_sch1xx_url_map_from_text(self.page_data)
        self.assertIn("007", self.url_map)
        self.llm_calls = 0

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def _fake_fetch_pdf(self, url: str):
        if "all_tariffs" in (url or "").lower():
            return tp.RatePage(
                url=url,
                title="All Tariffs E-18",
                page_type="pdf",
                content=_fixture_text("all_tariffs_56_.txt"),
            )
        num = _sched_num_from_url(url)
        if num:
            path = R13 / f"Sched_{num}.txt"
            if path.is_file():
                return tp.RatePage(
                    url=url,
                    title=f"Schedule {num}",
                    page_type="pdf",
                    content=path.read_text(),
                )
        return None

    def _fake_fetch_page(self, url: str):
        if "page-data" in (url or "") or url in tp._PGE_TARIFF_INDEX_URLS:
            return self.page_data, "application/json", 200
        if (url or "").lower().endswith(".pdf"):
            page = self._fake_fetch_pdf(url)
            if page:
                return page.content, "application/pdf", 200
        return "", "text/html", 404

    def _fake_refetch_index(self, url: str) -> str:
        if "page-data" in (url or "") or "tariff" in (url or ""):
            return self.page_data
        return ""

    def _fake_fetch_and_parse(self, url: str):
        """Serve tariff-index HTML/JSON as RatePage for rider enrich."""
        if "page-data" in (url or ""):
            return tp.RatePage(
                url=url,
                title="PGE tariff page-data",
                page_type="html",
                content=self.page_data,
            )
        if url in tp._PGE_TARIFF_INDEX_URLS or "tariff" in (url or ""):
            # Minimal hub page; page-data / refetch supplies the Sched_* map.
            return tp.RatePage(
                url=url,
                title="PGE tariff index",
                page_type="html",
                content=(
                    "Portland General Electric tariff schedules. "
                    "See Schedule 100 for applicable adjustments. "
                    + self.page_data[:2000]
                ),
            )
        page = self._fake_fetch_pdf(url)
        return page

    def test_resolve_primary_demotes_combined_book(self):
        primary, alts = tp.resolve_pge_primary_rate_url(
            "Portland General Electric Co",
            ALL_TARIFFS_URL,
            website_url="https://portlandgeneral.com",
            url_map=self.url_map,
        )
        self.assertIn("Sched_007", primary)
        self.assertTrue(any("all_tariffs" in a for a in alts))

    def test_prefer_injects_sch7_and_drops_stale_book(self):
        book = self._fake_fetch_pdf(ALL_TARIFFS_URL)
        pages = tp.prefer_current_individual_schedule_pages(
            "Portland General Electric Co",
            [book],
            website_url="https://portlandgeneral.com",
            url_map=self.url_map,
            fetch_pdf=self._fake_fetch_pdf,
        )
        urls = [p.url for p in pages]
        self.assertTrue(any("Sched_007" in u for u in urls), urls)
        self.assertFalse(any("all_tariffs" in u for u in urls), urls)

    def test_deterministic_sch7_carries_schedule_100_hint(self):
        """R14: Sched_007 footnote → riders_referenced_not_shown."""
        text = _fixture_text("Sched_007.txt")
        plans = tp.extract_pge_sch7_from_text(
            text, source_url=self.url_map["007"],
        )
        self.assertEqual(len(plans), 2)
        for p in plans:
            self.assertIn("Schedule 100", p.riders_referenced_not_shown)
        # Parse the printed sentence (not a blind hardcode).
        self.assertEqual(
            tp._parse_pge_sch7_rider_references(text),
            ["Schedule 100"],
        )

    def test_end_to_end_discovery_fetch_postprocess(self):
        """Full path: Sched_007 → enrich(Schedule 100) → Sch1xx → phase4."""
        search_primary = ALL_TARIFFS_URL
        primary, alts = tp.resolve_pge_primary_rate_url(
            "Portland General Electric Co",
            search_primary,
            website_url="https://portlandgeneral.com",
            url_map=self.url_map,
        )
        self.assertIn("Sched_007", primary)

        book_pages = [self._fake_fetch_pdf(ALL_TARIFFS_URL)]
        pages = tp.prefer_current_individual_schedule_pages(
            "Portland General Electric Co",
            book_pages,
            website_url="https://portlandgeneral.com",
            url_map=self.url_map,
            fetch_pdf=self._fake_fetch_pdf,
        )
        self.assertTrue(any("Sched_007" in (p.url or "") for p in pages))

        def _boom(*a, **k):
            self.llm_calls += 1
            raise RuntimeError("LLM must not be called for PGE after R13")

        with mock.patch.object(tp, "_extract_with_model_routing", side_effect=_boom), \
             mock.patch.object(tp, "_extract_two_pass", side_effect=_boom), \
             mock.patch.object(tp, "_call_claude_tool", side_effect=_boom):
            tariffs = tp.phase3_extract_tariffs(
                pages, "Portland General Electric Co", state="OR",
            )

        self.assertEqual(self.llm_calls, 0, "PGE Sched_007 path must be $0 LLM")
        names = [t.name for t in tariffs]
        self.assertTrue(any("Default Plan" in n for n in names), names)
        self.assertTrue(any("Time-of-Use Portfolio" in n for n in names), names)
        self.assertFalse(any("Electric Vehicle" in n for n in names), names)
        # R14: rider pointer must be present so enrich actually runs.
        for t in tariffs:
            if "Schedule 7" in (t.name or ""):
                self.assertIn("Schedule 100", t.riders_referenced_not_shown or [])

        # Rider enrich via fake fetcher — NO hand-injected Sch 1xx past fetch.
        sch100_url = self.url_map.get("100") or (
            "https://assets.ctfassets.net/416ywc1laqmd/x/Sched_100.pdf"
        )
        brave_hits = [
            {
                "url": sch100_url,
                "title": "Schedule 100 Summary of Applicable Adjustments",
                "description": "PGE Schedule 100",
            },
            {
                "url": tp._PGE_TARIFF_INDEX_URLS[0],
                "title": "PGE tariff index",
                "description": "schedules",
            },
        ]

        with mock.patch.object(tp, "brave_search", return_value=brave_hits), \
             mock.patch.object(
                 tp, "_fetch_as_pdf_via_download", side_effect=self._fake_fetch_pdf
             ), \
             mock.patch.object(
                 tp, "_fetch_and_parse", side_effect=self._fake_fetch_and_parse
             ), \
             mock.patch.object(
                 tp, "_refetch_pge_index_raw", side_effect=self._fake_refetch_index
             ), \
             mock.patch.object(tp, "_extract_rider_document", side_effect=_boom):
            merged, enrich_pages = tp.enrich_tariffs_with_referenced_rider_docs(
                tariffs,
                "Portland General Electric Co",
                "OR",
                website_url="https://portlandgeneral.com",
                pages=pages,
            )

        # Enrich must have fetched Sch 100 / 1xx / 125 (not just Sched_007).
        enrich_urls = " ".join(p.url or "" for p in enrich_pages)
        self.assertTrue(
            any("Sched_1" in (p.url or "") for p in enrich_pages),
            f"enrich must fetch Sched_1xx via rider step, got {[p.url for p in enrich_pages]}",
        )
        self.assertIn("Sched_125", enrich_urls)
        self.assertEqual(self.llm_calls, 0, "rider path must stay deterministic")

        report, valid = tp.phase4_validate(
            merged,
            "Portland General Electric Co",
            "OR",
        )
        self.assertGreaterEqual(report["valid"], 2)
        default = next(t for t in valid if "Default Plan" in t.name)
        tou = next(t for t in valid if "Time-of-Use Portfolio" in t.name)
        self.assertEqual(_energy_cents(default), [19.43, 20.542])
        self.assertEqual(_energy_cents(tou), [11.452, 19.22, 45.653])
        self.assertEqual(default.rate_type, "tiered")
        self.assertFalse(default.needs_review)
        self.assertFalse(tou.needs_review)
        self.assertNotIn("sch125_tod_pca_missing", tou.missing_fields or [])
        self.assertNotIn("sch1xx_riders_missing", default.missing_fields or [])
        # Stale "= 11.289" must not survive rider fold (R14).
        for c in default.components or []:
            if str(c.get("component_type") or "").lower() != "energy":
                continue
            label = str(c.get("tier_label") or "")
            self.assertNotIn("= 11.289", label, label)
            self.assertNotIn("=11.289", label.replace(" ", ""))


class TestR13Sch125TodAttach(unittest.TestCase):
    """Sch 125 TOD PCA must attach to any Sch 7 TOD/EV vintage."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_filter_keeps_sch125_tod_rows(self):
        text = _fixture_text("Sched_125.txt")
        det = tp._try_deterministic_pge_sch1xx_extract(
            tp.RatePage(url="https://x/Sched_125.pdf", content=text, page_type="pdf")
        )
        useful = tp._filter_useful_rider_extracts(list(det))
        vals = sorted(float(c["rate_value"]) for c in useful[0].components)
        self.assertEqual(vals, [3.416, 5.555, 5.619, 12.868])

    def test_pca_attaches_to_2020_tou_and_flags_when_missing(self):
        tou = tp.ExtractedTariff(
            name="Schedule 7 Time-of-Use Portfolio Option (Whole Premises)",
            customer_class="residential",
            rate_type="seasonal_tou",
            components=[
                {
                    "component_type": "energy", "unit": "¢/kWh", "rate_value": 20.378,
                    "period_label": "On-Peak", "period_start_time": "15:00",
                    "period_end_time": "20:00", "day_type": "weekday",
                },
                {
                    "component_type": "energy", "unit": "¢/kWh", "rate_value": 15.049,
                    "period_label": "Mid-Peak", "period_start_time": "06:00",
                    "period_end_time": "15:00", "day_type": "weekday",
                },
                {
                    "component_type": "energy", "unit": "¢/kWh", "rate_value": 4.128,
                    "period_label": "Off-Peak", "period_start_time": "22:00",
                    "period_end_time": "06:00", "day_type": "weekday",
                },
            ],
        )
        text = _fixture_text("Sched_125.txt")
        sch125 = tp._try_deterministic_pge_sch1xx_extract(
            tp.RatePage(url="https://x/Sched_125.pdf", content=text, page_type="pdf")
        )
        useful = tp._filter_useful_rider_extracts(list(sch125))
        # Flat 1xx stack (minimal) so expand still runs.
        stack = tp.ExtractedTariff(
            name="Schedule 105 Adjustment - Schedule 7 Residential",
            customer_class="residential",
            rate_type="flat",
            code="105",
            components=[{
                "component_type": "adjustment", "unit": "¢/kWh", "rate_value": 0.123,
                "tier_label": "Schedule 105 Schedule 7",
            }],
        )
        _rep, valid = tp.phase4_validate(
            [tou] + useful + [stack],
            "Portland General Electric Co",
            "OR",
        )
        plan = next(t for t in valid if "Time-of-Use" in t.name)
        # Base + Sch125 TOD + 0.123 flat stack (no Sch 102).
        self.assertEqual(_energy_cents(plan), [7.667, 20.727, 33.369])
        self.assertNotIn("sch125_tod_pca_missing", plan.missing_fields or [])

        # Without Sch 125 in the batch → clear missing-PCA flag.
        bare = tp.ExtractedTariff(
            name="Schedule 7 Time-of-Use Portfolio Option (Whole Premises)",
            customer_class="residential",
            rate_type="tou",
            components=[
                {
                    "component_type": "energy", "unit": "¢/kWh", "rate_value": 20.378,
                    "period_label": "On-Peak", "period_start_time": "15:00",
                    "period_end_time": "20:00", "day_type": "weekday",
                },
                {
                    "component_type": "energy", "unit": "¢/kWh", "rate_value": 4.128,
                    "period_label": "Off-Peak", "period_start_time": "22:00",
                    "period_end_time": "06:00", "day_type": "weekday",
                },
            ],
        )
        _rep2, valid2 = tp.phase4_validate(
            [bare, stack], "Portland General Electric Co", "OR",
        )
        plan2 = next(t for t in valid2 if "Time-of-Use" in t.name)
        self.assertIn("sch125_tod_pca_missing", plan2.missing_fields or [])
        self.assertTrue(plan2.needs_review)

    def test_default_without_riders_is_flagged(self):
        """R14: Default with no Sch 1xx must never pass silently."""
        bare = tp.ExtractedTariff(
            name="Schedule 7 Residential Service Price Plan (Default Plan)",
            customer_class="residential",
            rate_type="flat",
            description="See Schedule 100 for applicable adjustments.",
            riders_referenced_not_shown=["Schedule 100"],
            components=[{
                "component_type": "energy",
                "unit": "¢/kWh",
                "rate_value": 11.289,
                "tier_label": "all-in: transmission + distribution + energy = 11.289",
            }],
        )
        _rep, valid = tp.phase4_validate(
            [bare], "Portland General Electric Co", "OR",
        )
        plan = next(t for t in valid if "Default" in t.name)
        self.assertTrue(plan.needs_review)
        self.assertIn("sch1xx_riders_missing", plan.missing_fields or [])

    def test_sch100_map_ignores_sch7_footnote(self):
        """R14: Sched_007 footnote must not look like the Sch 100 map."""
        sch7 = _fixture_text("Sched_007.txt")
        self.assertFalse(tp._is_pge_sch100_applicability_page(sch7, "https://x/Sched_007.pdf"))
        sch100 = _fixture_text("Sched_100.txt")
        self.assertTrue(
            tp._is_pge_sch100_applicability_page(sch100, "https://x/Sched_100.pdf")
        )
        nums = tp.parse_pge_sch100_applicable_schedules(sch100, base_schedule="7")
        self.assertGreaterEqual(len(nums), 20)
        self.assertIn("125", nums)


if __name__ == "__main__":
    unittest.main()
