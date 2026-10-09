"""PR R28-4: official document discovery (no pinned URLs)."""
from __future__ import annotations

import unittest
from datetime import date

from app.services.pricing.discover import (
    R28_NON_GOLDEN_FIXTURES,
    DiscoveredPage,
    assert_r28_fixture,
    discover_document_set,
    filter_discovery_candidates,
    is_bill_insert_url,
    is_faq_url,
    is_non_english_locale_url,
    pages_to_candidates,
    reject_reason_for_candidate,
    run_r28_fixture_discovery,
)
from app.services.pricing.document_set import DocumentCandidate


class TestRejectFilters(unittest.TestCase):
    def test_faq_and_bill_insert(self):
        self.assertTrue(is_faq_url("https://x.com/rates/faq", "Rates FAQ"))
        self.assertTrue(
            is_bill_insert_url(
                "https://x.com/docs/bill-insert-jan.pdf",
                "January Bill Insert",
            )
        )
        self.assertTrue(is_non_english_locale_url("https://x.com/es/rates.pdf"))

    def test_non_english_kept_without_english_twin(self):
        only_es = DocumentCandidate(
            url="https://x.com/es/rates/tou.pdf", title="TOU Español",
        )
        self.assertIsNone(reject_reason_for_candidate(only_es, siblings=[only_es]))

    def test_non_english_rejected_when_english_twin(self):
        es = DocumentCandidate(url="https://x.com/es/rates/tou.pdf", title="ES")
        en = DocumentCandidate(url="https://x.com/en/rates/tou.pdf", title="EN")
        self.assertEqual(
            reject_reason_for_candidate(es, siblings=[es, en]),
            "non_english_locale",
        )
        self.assertIsNone(reject_reason_for_candidate(en, siblings=[es, en]))

    def test_filter_drops_marketing(self):
        cands = pages_to_candidates([
            DiscoveredPage(
                url="https://x.com/marketing/brochure.pdf",
                title="Customer overview brochure",
            ),
            DiscoveredPage(
                url="https://x.com/rates/tariff-book.pdf",
                title="Tariff Book",
                content="Energy 10 ¢/kWh",
            ),
        ])
        kept, rejected = filter_discovery_candidates(cands)
        self.assertEqual(len(kept), 1)
        self.assertIn("tariff-book", kept[0].url)
        self.assertEqual({r["reason"] for r in rejected}, {"marketing_page"})


class TestDiscoverWiring(unittest.TestCase):
    def test_injectable_phase_fns_no_network(self):
        pages = [
            DiscoveredPage(
                url="https://example.com/rates/schedule.pdf",
                title="Schedule RS",
                content="Energy Charge 10 ¢/kWh",
            ),
        ]

        def phase1(name, state, website):
            return ("https://example.com/rates/", 1, [])

        def phase2(url):
            return pages

        result = discover_document_set(
            utility_name="Example Co",
            state="XX",
            recipe_code="bundled",
            as_of=date(2026, 10, 9),
            phase1_fn=phase1,
            phase2_fn=phase2,
        )
        self.assertEqual(result.rate_page_url, "https://example.com/rates/")
        selected = result.document_set.selected_urls()
        self.assertTrue(any("schedule.pdf" in u for u in selected), selected)


class TestR28NonGoldenFixtures(unittest.TestCase):
    def test_fixture_count_is_ten(self):
        self.assertEqual(len(R28_NON_GOLDEN_FIXTURES), 10)

    def test_each_r28_fixture(self):
        failures = []
        for name in R28_NON_GOLDEN_FIXTURES:
            try:
                assert_r28_fixture(name)
            except AssertionError as e:
                failures.append(str(e))
        self.assertEqual(failures, [], "\n".join(failures))

    def test_ku_prefers_current_edition(self):
        result = run_r28_fixture_discovery("Kentucky Utilities")
        selected = result.document_set.selected_urls()
        self.assertTrue(any("2026" in u for u in selected))
        self.assertFalse(any("2024" in u for u in selected))

    def test_comed_has_default_supply_and_delivery(self):
        result = run_r28_fixture_discovery("Commonwealth Edison")
        roles = {m.role for m in result.document_set.selected()}
        self.assertIn("default_supply", roles)
        self.assertIn("delivery", roles)

    def test_london_hydro_ontario_roles(self):
        result = run_r28_fixture_discovery("London Hydro")
        roles = {m.role for m in result.document_set.selected()}
        self.assertIn("provincial_commodity", roles)
        self.assertIn("delivery", roles)


if __name__ == "__main__":
    unittest.main()
