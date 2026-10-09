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

    def test_non_utility_domain_rejected(self):
        from app.services.pricing.document_set import DocumentCandidate
        from app.services.pricing.discover import reject_reason_for_candidate
        cand = DocumentCandidate(
            url="https://www.quickelectricity.com/oncor-rates",
            title="Oncor rates",
        )
        self.assertEqual(reject_reason_for_candidate(cand), "non_utility_domain")

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


class TestReviewDiscoveryFixes(unittest.TestCase):
    """Review of #91: domain filter beyond the blocklist, twin-only locale rule."""

    def _ctx(self, **kw):
        from app.services.pricing.discover import discovery_source_context
        return discovery_source_context(**kw)

    def test_foreign_domain_rejected_when_utility_site_known(self):
        ctx = self._ctx(website_url="https://www.consumersenergy.com", state="MI")
        news = DocumentCandidate(
            url="https://www.mlive.com/news/2026/rates-rise.html",
            title="Consumers Energy rate schedule changes",
        )
        own = DocumentCandidate(
            url="https://www.consumersenergy.com/rates/tariff-book.pdf",
            title="Rate Book",
        )
        self.assertEqual(
            reject_reason_for_candidate(news, source_ctx=ctx), "non_utility_domain",
        )
        self.assertIsNone(reject_reason_for_candidate(own, source_ctx=ctx))

    def test_foreign_domain_kept_without_known_site(self):
        cand = DocumentCandidate(url="https://www.smallcoop.org/rates.pdf", title="Rates")
        self.assertIsNone(reject_reason_for_candidate(cand, source_ctx=self._ctx(state="KS")))

    def test_rate_page_domain_counts_as_official(self):
        ctx = self._ctx(
            website_url="https://www.ku.com",
            state="KY",
            rate_page_url="https://lge-ku.com/rates",
        )
        book = DocumentCandidate(url="https://lge-ku.com/files/tariff-book.pdf")
        self.assertIsNone(reject_reason_for_candidate(book, source_ctx=ctx))

    def test_rate_page_alone_does_not_define_official_host(self):
        ctx = self._ctx(state="ON", rate_page_url="https://www.oeb.ca/rates")
        own = DocumentCandidate(url="https://www.londonhydro.com/rates/residential")
        self.assertIsNone(reject_reason_for_candidate(own, source_ctx=ctx))
        ctx = self._ctx(
            website_url="https://www.londonhydro.com", state="ON",
            rate_page_url="https://www.oeb.ca/rates",
        )
        self.assertEqual(ctx.official_urls, ())
        self.assertIsNone(reject_reason_for_candidate(own, source_ctx=ctx))

    def test_generic_government_board_and_supply_publishers_kept(self):
        ctx = self._ctx(website_url="https://www.comed.com", state="IL")
        for url in (
            "https://s3.amazonaws.com/bucket/rider-fuel.pdf",
            "https://www.icc.illinois.gov/docket/rates.pdf",
            "https://www.pluginillinois.org/FixedRateBreakdownComEd.aspx",
        ):
            self.assertIsNone(
                reject_reason_for_candidate(DocumentCandidate(url=url), source_ctx=ctx), url,
            )
        on_ctx = self._ctx(website_url="https://www.londonhydro.com", state="ON")
        oeb = DocumentCandidate(url="https://www.oeb.ca/consumer-information-and-protection/electricity-rates")
        self.assertIsNone(reject_reason_for_candidate(oeb, source_ctx=on_ctx))
        ab = DocumentCandidate(url="https://www.oeb.ca/rates")
        ab_ctx = self._ctx(website_url="https://www.epcor.com", state="AB")
        self.assertEqual(reject_reason_for_candidate(ab, source_ctx=ab_ctx), "non_utility_domain")

    def test_discover_document_set_rejects_foreign_page(self):
        pages = [
            DiscoveredPage(url="https://www.example-utility.com/rates/schedule-r.pdf",
                           title="Schedule R", content="Energy Charge 10 ¢/kWh"),
            DiscoveredPage(url="https://www.ratesblog.net/example-utility-rates",
                           title="Example Utility rate schedule explained",
                           content="Energy 99 ¢/kWh"),
        ]
        result = discover_document_set(
            utility_name="Example Utility",
            state="MO",
            website_url="https://www.example-utility.com",
            recipe_code="bundled",
            as_of=date(2026, 10, 9),
            phase1_fn=lambda n, s, w: (pages[0].url, 1, []),
            phase2_fn=lambda u: pages,
        )
        self.assertEqual(
            result.rejected,
            [{"url": "https://www.ratesblog.net/example-utility-rates",
              "reason": "non_utility_domain"}],
        )
        self.assertNotIn(pages[1].url, result.document_set.selected_urls())

    def test_french_only_document_kept_beside_unrelated_english_page(self):
        fr = DocumentCandidate(
            url="https://www.hydroquebec.com/fr/tarifs/tarif-d.pdf", title="Tarif D",
        )
        unrelated = DocumentCandidate(
            url="https://www.hydroquebec.com/en/rates/rider-schedule.pdf",
            title="Rider schedule",
        )
        self.assertIsNone(reject_reason_for_candidate(fr, siblings=[fr, unrelated]))

    def test_unmarked_same_path_copy_is_a_twin(self):
        es = DocumentCandidate(url="https://x.com/es/rates/tou.pdf")
        en = DocumentCandidate(url="https://x.com/rates/tou.pdf")
        self.assertEqual(
            reject_reason_for_candidate(es, siblings=[es, en]), "non_english_locale",
        )

    def test_title_twin_on_same_host(self):
        es = DocumentCandidate(url="https://x.com/docs/a1.pdf?lang=es", title="TOU-E (Español)")
        en = DocumentCandidate(url="https://x.com/docs/b7.pdf", title="TOU-E English")
        other_host = DocumentCandidate(url="https://y.com/docs/b7.pdf", title="TOU-E English")
        self.assertEqual(
            reject_reason_for_candidate(es, siblings=[es, en]), "non_english_locale",
        )
        self.assertIsNone(reject_reason_for_candidate(es, siblings=[es, other_host]))


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
