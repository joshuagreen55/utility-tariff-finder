"""Issue #28: official vs third-party source labelling (pure functions).

Classifier rules, fetch-target ranking, health-score Provenance math and the
Hydro-Québec repair's number verification. No database required.
"""
from __future__ import annotations

import unittest
from types import SimpleNamespace

from app.services.source_type import (
    OFFICIAL,
    THIRD_PARTY,
    UNKNOWN,
    UtilitySourceContext,
    classify_source,
    configured_urls,
    context_from_utility,
    normalize_host,
    rank_urls,
    registrable_domain,
)

HQ_PDF = "https://www.hydroquebec.com/data/documents-donnees/pdf/electricity-rates.pdf"
HQ_PAGE = "https://www.hydroquebec.com/residential/customer-space/rates/"
HQ_CALLMEPOWER = "https://callmepower.ca/en/quebec/hydro-quebec-rates"

HQ = UtilitySourceContext(
    website_url="https://www.hydroquebec.com",
    official_urls=(HQ_PAGE,),
    country="CA",
    state_province="QC",
)


class TestClassifier(unittest.TestCase):
    def test_hq_callmepower_is_third_party(self):
        r = classify_source(HQ_CALLMEPOWER, HQ)
        self.assertEqual((r.source_type, r.reason), (THIRD_PARTY, "aggregator_blocklist"))

    def test_hq_rate_book_pdf_is_official(self):
        r = classify_source(HQ_PDF, HQ)
        self.assertEqual((r.source_type, r.reason), (OFFICIAL, "domain_match"))

    def test_blocklist_wins_even_if_configured(self):
        ctx = UtilitySourceContext(website_url="https://www.hydroquebec.com", official_urls=(HQ_CALLMEPOWER,))
        self.assertEqual(classify_source(HQ_CALLMEPOWER, ctx).source_type, THIRD_PARTY)

    def test_www_and_subdomains_match_website(self):
        for url in (
            "http://hydroquebec.com/rates",
            "https://services.hydroquebec.com/x.pdf",
            "HTTPS://WWW.HYDROQUEBEC.COM:443/a",
        ):
            self.assertEqual(classify_source(url, HQ).source_type, OFFICIAL, url)

    def test_configured_host_for_separate_official_pdf_domain(self):
        ctx = UtilitySourceContext(
            website_url="https://www.examplecoop.coop",
            official_urls=("https://docs.examplecoop-rates.com/tariff-2026.pdf",),
        )
        r = classify_source("https://docs.examplecoop-rates.com/other.pdf", ctx)
        self.assertEqual((r.source_type, r.reason), (OFFICIAL, "configured_host"))

    def test_generic_host_needs_the_configured_url(self):
        ctx = UtilitySourceContext(
            website_url="https://smallcoop.org",
            official_urls=("https://static1.squarespace.com/static/abc123/",),
        )
        r = classify_source("https://static1.squarespace.com/static/abc123/rates.pdf", ctx)
        self.assertEqual((r.source_type, r.reason), (OFFICIAL, "configured_url"))
        r = classify_source("https://static1.squarespace.com/static/zzz/rates.pdf", ctx)
        self.assertEqual((r.source_type, r.reason), (UNKNOWN, "generic_host"))

    def test_regulator_publisher_only_in_its_jurisdiction(self):
        on = UtilitySourceContext(website_url="https://www.torontohydro.com", country="CA", state_province="ON")
        url = "https://www.oeb.ca/consumer-information-and-protection/electricity-rates"
        r = classify_source(url, on)
        self.assertEqual((r.source_type, r.reason), (OFFICIAL, "regulator_publisher"))
        self.assertEqual(classify_source(url, HQ).source_type, THIRD_PARTY)

    def test_government_host_is_unknown(self):
        r = classify_source("https://www.puc.nh.gov/Regulatory/Tariffs/x.pdf", HQ)
        self.assertEqual((r.source_type, r.reason), (UNKNOWN, "government_host"))
        r = classify_source("https://puc.state.nh.us/tariffs.pdf", HQ)
        self.assertEqual(r.reason, "government_host")

    def test_domain_mismatch_is_third_party(self):
        r = classify_source("https://some-energy-blog.com/hq", HQ)
        self.assertEqual((r.source_type, r.reason), (THIRD_PARTY, "domain_mismatch"))

    def test_no_url_and_no_official_host_are_unknown(self):
        self.assertEqual(classify_source(None, HQ).reason, "no_url")
        self.assertEqual(classify_source("", HQ).reason, "no_url")
        self.assertEqual(classify_source("not a url", HQ).reason, "no_url")
        r = classify_source("https://rates.example.org/x", UtilitySourceContext())
        self.assertEqual((r.source_type, r.reason), (UNKNOWN, "no_official_host"))

    def test_registrable_domain(self):
        self.assertEqual(registrable_domain("rates.hydroquebec.com"), "hydroquebec.com")
        self.assertEqual(registrable_domain("ville.sherbrooke.qc.ca"), "sherbrooke.qc.ca")
        self.assertEqual(registrable_domain("ci.anaheim.ca.us"), "ci.anaheim.ca.us")
        self.assertEqual(registrable_domain("hydroquebec.com"), "hydroquebec.com")
        self.assertEqual(normalize_host("www.HydroQuebec.com/x"), "hydroquebec.com")

    def test_context_from_utility_row_and_jsonb_shapes(self):
        u = SimpleNamespace(
            website_url="https://www.hydroquebec.com",
            tariff_page_urls={"residential": HQ_PAGE},
            rate_page_url_override=HQ_PDF,
            country=SimpleNamespace(value="CA"),
            state_province="QC",
        )
        ctx = context_from_utility(u)
        self.assertEqual(ctx.official_urls, (HQ_PDF, HQ_PAGE))
        self.assertEqual(ctx.country, "CA")
        self.assertEqual(configured_urls([HQ_PAGE, HQ_PAGE, "", None]), (HQ_PAGE,))


class TestRanking(unittest.TestCase):
    def test_official_first_third_party_last_nothing_dropped(self):
        urls = [HQ_CALLMEPOWER, "https://www.puc.nh.gov/x", HQ_PDF, HQ_PAGE, HQ_PDF]
        self.assertEqual(
            rank_urls(urls, HQ),
            [HQ_PDF, HQ_PAGE, "https://www.puc.nh.gov/x", HQ_CALLMEPOWER],
        )

    def test_only_url_is_kept_even_if_third_party(self):
        self.assertEqual(rank_urls([HQ_CALLMEPOWER], HQ), [HQ_CALLMEPOWER])

    def test_pipeline_prefers_official_primary(self):
        from scripts.tariff_pipeline import prefer_official_targets

        primary, alts = prefer_official_targets(
            "https://some-energy-blog.com/hq", ["https://www.puc.nh.gov/x"], [HQ_PAGE], HQ,
        )
        self.assertEqual(primary, HQ_PAGE)
        self.assertEqual(alts, ["https://www.puc.nh.gov/x", "https://some-energy-blog.com/hq"])

        primary, alts = prefer_official_targets("", [], [HQ_PDF], HQ)
        self.assertEqual((primary, alts), (HQ_PDF, []))

    def test_pipeline_keeps_locked_or_already_official_primary(self):
        from scripts.tariff_pipeline import prefer_official_targets

        blog = "https://some-energy-blog.com/hq"
        self.assertEqual(prefer_official_targets(blog, [], [HQ_PAGE], HQ, locked=True), (blog, [HQ_PAGE]))
        self.assertEqual(prefer_official_targets(HQ_PDF, [blog, HQ_PAGE], [], HQ), (HQ_PDF, [HQ_PAGE, blog]))
        self.assertEqual(prefer_official_targets(blog, [], [], HQ), (blog, []))

    def test_callmepower_is_hard_blocked_by_pipeline(self):
        from scripts.tariff_pipeline import _is_third_party_domain, score_search_result

        self.assertTrue(_is_third_party_domain(HQ_CALLMEPOWER))
        result = {"url": HQ_CALLMEPOWER, "title": "Hydro-Québec rates", "description": "electricity rates"}
        self.assertEqual(score_search_result(result, "Hydro-Québec", "www.hydroquebec.com", "QC"), -999)

    def test_phase6_drops_third_party_sources(self):
        import json

        from scripts.tariff_pipeline import _phase6_parse_tariffs

        items = [
            {"name": "Rate D", "customer_class": "residential", "rate_type": "tiered",
             "source_url": HQ_CALLMEPOWER, "components": [{"component_type": "energy", "rate_value": 0.07}]},
            {"name": "Rate DM", "customer_class": "residential", "rate_type": "tiered",
             "source_url": HQ_PDF, "components": [{"component_type": "energy", "rate_value": 0.07}]},
        ]
        out = _phase6_parse_tariffs(f"```json\n{json.dumps(items)}\n```", HQ_PAGE)
        self.assertEqual([t.name for t in out], ["Rate DM"])


class TestProvenanceScore(unittest.TestCase):
    def test_weights(self):
        from scripts.health_score import PROVENANCE_WEIGHTS, provenance_score

        self.assertEqual(PROVENANCE_WEIGHTS, {"official": 1.0, "unknown": 0.4, "third_party": 0.2})
        self.assertAlmostEqual(provenance_score({"official": 10}), 100.0)
        self.assertAlmostEqual(provenance_score({"unknown": 10}), 40.0)
        self.assertAlmostEqual(provenance_score({"third_party": 10}), 20.0)
        self.assertAlmostEqual(provenance_score({}), 0.0)
        # (6×1.0 + 3×0.4 + 1×0.2) / 10
        self.assertAlmostEqual(
            provenance_score({"official": 6, "unknown": 3, "third_party": 1}), 74.0
        )

    def test_unrecognized_type_scores_as_unknown(self):
        from scripts.health_score import provenance_score

        self.assertAlmostEqual(provenance_score({"weird": 5, "official": 5}), 70.0)

    def test_composite_weights_unchanged(self):
        from scripts.health_score import COMPOSITE_WEIGHTS

        self.assertEqual(
            COMPOSITE_WEIGHTS,
            {"coverage": 0.40, "freshness": 0.30, "completeness": 0.20, "provenance": 0.10},
        )


class TestHqRepairVerification(unittest.TestCase):
    DOC = (
        "Rate D  Access charge 46.154¢ per day. First 40 kWh per day 6,905 ¢/kWh; "
        "remaining consumption 10.652¢/kWh. Power over 50 kW: $6.228 per kW."
    )

    def _comp(self, ctype, value, unit, **kw):
        return {"component_type": ctype, "rate_value": value, "unit": unit, **kw}

    def test_dollars_or_cents_with_either_decimal_mark(self):
        from scripts.repair_hq_official_source import document_numbers, value_printed

        nums = document_numbers(self.DOC)
        self.assertTrue(value_printed("0.069050", nums))   # 6,905 ¢
        self.assertTrue(value_printed(0.46154, nums))      # 46.154¢
        self.assertTrue(value_printed(6.228, nums))        # $6.228
        self.assertFalse(value_printed(0.0699, nums))
        self.assertFalse(value_printed(None, nums))

    def test_missing_values_lists_rates_and_tier_bounds(self):
        from scripts.repair_hq_official_source import missing_values

        ok = [
            self._comp("fixed", 0.46154, "$/day"),
            self._comp("energy", 0.06905, "$/kWh", tier_min_kwh=0, tier_max_kwh=40),
            self._comp("energy", 0.10652, "$/kWh", tier_min_kwh=40),
        ]
        self.assertEqual(missing_values(ok, self.DOC), [])
        bad = [self._comp("energy", 0.0699, "$/kWh", tier_max_kwh=1200)]
        miss = missing_values(bad, self.DOC)
        self.assertEqual(len(miss), 2)
        self.assertIn("rate 0.0699", miss[0])
        self.assertIn("tier_max_kwh 1200", miss[1])
        self.assertEqual(missing_values([], self.DOC), ["no components"])

    def test_pick_official_document_prefers_first_that_verifies_all(self):
        from scripts.repair_hq_official_source import pick_official_document

        comps = [self._comp("energy", 0.10652, "$/kWh")]
        url, misses = pick_official_document(comps, {HQ_PDF: "", HQ_PAGE: self.DOC})
        self.assertEqual(url, HQ_PAGE)
        self.assertEqual(misses[HQ_PDF], ["document unavailable"])
        url, _ = pick_official_document([self._comp("energy", 0.777, "$/kWh")], {HQ_PDF: self.DOC})
        self.assertIsNone(url)


if __name__ == "__main__":
    unittest.main()
