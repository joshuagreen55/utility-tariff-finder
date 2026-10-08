"""Round-6 pipeline fixes (full-bill code match, seasonal base, one-hop,
optional programmes, needs_review, government skip, clocks, far-future,
two-pass gates).

    cd backend && python -m unittest tests.test_r6_pipeline_fixes -v
"""
from __future__ import annotations

import logging
import unittest
from datetime import date, timedelta
from unittest import mock

from scripts import tariff_pipeline as tp


class TestFullBillNoCodeMerge(unittest.TestCase):
    def test_differing_codes_never_match(self):
        a = tp.ExtractedTariff(
            name="Rate No. 1.1 – Domestic", code="1.1",
            customer_class="residential", rate_type="flat",
            components=[{"component_type": "energy", "unit": "¢/kWh", "rate_value": 15.587}],
        )
        b = tp.ExtractedTariff(
            name="Rate No. 1.2G Government Departments", code="1.2G",
            customer_class="residential", rate_type="flat",
            components=[{"component_type": "energy", "unit": "¢/kWh", "rate_value": 100.145}],
        )
        self.assertFalse(tp._full_bill_product_match(a, b))

    def test_stopword_no_does_not_merge(self):
        a = tp.ExtractedTariff(
            name="Rate No. 1.1 Domestic", code="1.1",
            customer_class="residential", rate_type="flat",
            components=[{"component_type": "energy", "unit": "¢/kWh", "rate_value": 15.587}],
        )
        b = tp.ExtractedTariff(
            name="Rate No. 1.2G Government", code="1.2G",
            customer_class="residential", rate_type="flat",
            components=[{"component_type": "energy", "unit": "¢/kWh", "rate_value": 100.145}],
        )
        out = tp._collapse_full_bill_siblings([a, b])
        self.assertEqual(len(out), 2)

    def test_price_cap_blocks_huge_delta(self):
        thin = tp.ExtractedTariff(
            name="Time of Use", customer_class="residential", rate_type="tou",
            components=[{"component_type": "energy", "unit": "¢/kWh", "rate_value": 4.35}],
        )
        huge = tp.ExtractedTariff(
            name="Time of Use Full", customer_class="residential", rate_type="tou",
            components=[{"component_type": "energy", "unit": "¢/kWh", "rate_value": 100.0}],
        )
        self.assertFalse(tp._base_energy_compatible_for_full_bill(thin, huge))


class TestAllowedSeasonalBaseDerivation(unittest.TestCase):
    def test_structured_rules_allow_same_doc_base(self):
        self.assertIn("same official document", tp._STRUCTURED_RULES.lower())
        self.assertIn("1.1S", tp._STRUCTURED_RULES)
        self.assertIn("allowed derivation e", tp._STRUCTURED_RULES.lower())

    def test_salvage_1_1s_with_sibling_1_1(self):
        base = tp.ExtractedTariff(
            name="Rate No. 1.1 Domestic", code="1.1",
            customer_class="residential", rate_type="flat",
            components=[{"component_type": "energy", "unit": "¢/kWh", "rate_value": 15.587}],
        )
        seasonal = tp.ExtractedTariff(
            name="Rate No. 1.1S Domestic Seasonal", code="1.1S",
            customer_class="residential", rate_type="seasonal",
            components=[
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 0.953,
                 "season": "Winter"},
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": -1.297,
                 "season": "Non-Winter"},
            ],
        )
        gov = tp.ExtractedTariff(
            name="1.2G Government Departments", code="1.2G",
            customer_class="residential", rate_type="flat",
            components=[{"component_type": "energy", "unit": "¢/kWh", "rate_value": 100.145}],
        )
        report, valid = tp.phase4_validate(
            [base, seasonal, gov], "Newfoundland Power", "NL",
        )
        codes = {t.code for t in valid}
        self.assertIn("1.1", codes)
        self.assertIn("1.1S", codes)
        self.assertNotIn("1.2G", codes)  # government skipped
        s = next(t for t in valid if t.code == "1.1S")
        by = {
            tp._season_key(c.get("season")): round(float(c["rate_value"]), 5)
            for c in s.components if c["component_type"] == "energy"
        }
        self.assertEqual(by["winter"], 0.16540)
        self.assertEqual(by["non-winter"], 0.14290)


class TestRiderOneHop(unittest.TestCase):
    def test_page_lacks_amounts(self):
        self.assertTrue(tp._page_lacks_rider_amounts("Click here for the FAM tariff PDF."))
        self.assertFalse(tp._page_lacks_rider_amounts(
            "FAM AA/BA 0.156 ¢/kWh applies. DSM DCRR 0.648 cents/kWh."
        ))

    def test_one_hop_extracts_pdf_links(self):
        page = tp.RatePage(
            url="https://www.nspower.ca/about-us/producing/rates-tariffs/fam",
            title="FAM", page_type="html",
            content=(
                '<html><body><p>See the tariff.</p>'
                '<a href="/docs/fam-tariff-2026.pdf">Fuel Adjustment Mechanism tariff PDF</a>'
                '<a href="/docs/dsm-dcrr.pdf">DSM DCRR rider</a>'
                '</body></html>'
            ),
        )
        links = tp._rider_page_one_hop_links(
            page, allowed_domains={"nspower.ca"},
        )
        self.assertTrue(any("fam-tariff" in u for u in links))
        self.assertTrue(any("dcrr" in u.lower() or "dsm" in u.lower() for u in links))


class TestOptionalProgrammes(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_green_future_not_folded(self):
        et = tp.ExtractedTariff(
            name="Schedule 7", customer_class="residential", rate_type="flat",
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 11.224},
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 0.8,
                 "tier_label": "Green Future"},
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": -1.0,
                 "tier_label": "Peak Time Rebate PTR"},
            ],
        )
        out = tp.expand_stacking_energy_riders(et.components, rate_type="flat")
        energy = [c for c in out if c["component_type"] == "energy"][0]
        # Optional Green Future / PTR must not change ENERGY (still ¢ here).
        self.assertAlmostEqual(float(energy["rate_value"]), 11.224, places=3)

    def test_green_power_fixed_stripped(self):
        et = tp.ExtractedTariff(
            name="Domestic Service", customer_class="residential", rate_type="flat",
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 18.324},
                {"component_type": "fixed", "unit": "$/month", "rate_value": 20.08},
                {"component_type": "fixed", "unit": "$/month", "rate_value": 5.0,
                 "tier_label": "Green Power optional"},
            ],
        )
        n = tp.strip_optional_program_components(et)
        self.assertEqual(n, 1)
        fixed = [c for c in et.components if c["component_type"] == "fixed"]
        self.assertEqual(len(fixed), 1)
        self.assertAlmostEqual(float(fixed[0]["rate_value"]), 20.08, places=2)

    def test_net_metering_option_dropped(self):
        opt = tp.ExtractedTariff(
            name="Rate D Option I Net Metering", customer_class="residential",
            rate_type="tiered",
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 11.795,
                 "tier_min_kwh": 0, "tier_max_kwh": 40},
            ],
        )
        self.assertTrue(tp._is_optional_program_tariff(opt))
        _r, valid = tp.phase4_validate([opt], "Hydro-Quebec", "QC")
        self.assertEqual(valid, [])


class TestNeedsReviewCriticalOnly(unittest.TestCase):
    def test_effective_date_alone_not_critical(self):
        self.assertFalse(
            tp._is_mysa_critical_review_reason(missing_fields=["effective_date"])
        )
        self.assertTrue(
            tp._is_mysa_critical_review_reason(missing_fields=["energy"])
        )
        self.assertTrue(
            tp._is_mysa_critical_review_reason(
                completeness_reasons=["tou_missing_clock_windows"]
            )
        )


class TestGovernmentNonResidential(unittest.TestCase):
    def test_skip_keywords_match_government(self):
        self.assertTrue(tp.SKIP_KEYWORDS.search("1.2G Domestic Diesel – Government Departments"))

    def test_phase4_drops_government(self):
        gov = tp.ExtractedTariff(
            name="1.2G Domestic Diesel – Government Departments",
            code="1.2G", customer_class="residential", rate_type="flat",
            components=[{"component_type": "energy", "unit": "¢/kWh", "rate_value": 100.145}],
        )
        _r, valid = tp.phase4_validate([gov], "NL Hydro", "NL")
        self.assertEqual(valid, [])


class TestBrokenTouClocksRejected(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_gap_rejected(self):
        et = tp.ExtractedTariff(
            name="EV2-A", customer_class="residential", rate_type="tou",
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 20.0,
                 "period_start_time": "16:00", "period_end_time": "21:00",
                 "day_type": "weekday", "period_label": "Peak"},
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 10.0,
                 "period_start_time": "21:00", "period_end_time": "15:00",
                 "day_type": "weekday", "period_label": "Off-Peak"},
                # gap 15:00–16:00
            ],
        )
        report, valid = tp.phase4_validate([et], "PG&E", "CA")
        self.assertEqual(valid, [])
        self.assertTrue(any("broken TOU" in str(i) for i in report["issues"]))

    def test_tou_tiered_reclassified(self):
        et = tp.ExtractedTariff(
            name="E-TOU-C", customer_class="residential", rate_type="tou",
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 32.0,
                 "period_start_time": "16:00", "period_end_time": "21:00",
                 "day_type": "weekday", "tier_min_kwh": 0, "tier_max_kwh": 300},
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 40.0,
                 "period_start_time": "16:00", "period_end_time": "21:00",
                 "day_type": "weekday", "tier_min_kwh": 300, "tier_max_kwh": None},
            ],
        )
        # Incomplete clocks → may reject; rate_type should become tou_tiered first.
        tp.phase4_validate([et], "PG&E", "CA")
        self.assertEqual(et.rate_type, "tou_tiered")


class TestFarFutureEffectiveDate(unittest.TestCase):
    def test_parses_18_months_ahead(self):
        today = date(2026, 10, 8)
        d = tp._parse_effective_date("2028-04-01", today=today)
        self.assertEqual(d, date(2028, 4, 1))

    def test_phase4_flags_far_future(self):
        far = (date.today() + timedelta(days=400)).isoformat()
        et = tp.ExtractedTariff(
            name="Residential", customer_class="residential", rate_type="flat",
            effective_date=far,
            components=[{"component_type": "energy", "unit": "¢/kWh", "rate_value": 12.0}],
        )
        _r, valid = tp.phase4_validate([et], "NorthWestern Energy", "MT")
        self.assertEqual(len(valid), 1)
        self.assertTrue(valid[0].needs_review)


class TestSupersededVisionEnergy(unittest.TestCase):
    def test_flat_dual_energy_keeps_higher(self):
        comps = [
            {"component_type": "energy", "unit": "¢/kWh", "rate_value": 9.84},
            {"component_type": "energy", "unit": "¢/kWh", "rate_value": 10.44},
        ]
        out = tp.drop_superseded_flat_energy_vintages(comps, rate_type="flat")
        energy = [c for c in out if c["component_type"] == "energy"]
        self.assertEqual(len(energy), 1)
        self.assertAlmostEqual(float(energy[0]["rate_value"]), 10.44, places=2)


class TestTwoPassGates(unittest.TestCase):
    def test_short_residential_span_skips_twopass(self):
        self.assertGreaterEqual(tp._TWOPASS_MIN_RESIDENTIAL_CHARS, 2000)
        # Long page with complexity signals but almost no residential slice.
        content = ("commercial industrial demand " * 500) + (
            " tier step block on-peak off-peak summer winter schedule rate"
        )
        self.assertGreater(len(content), 8000)
        # When the residential window is below the threshold, two-pass is off.
        with mock.patch.object(tp, "_residential_content_span", return_value=500):
            self.assertFalse(tp._is_complex_page(content))

    def test_dup_hash_skipped(self):
        content = "Residential Service Energy 12.500 cents per kWh. " * 50
        p1 = tp.RatePage(url="https://u.example/a.pdf", title="A", content=content, page_type="pdf")
        p2 = tp.RatePage(url="https://u.example/b.pdf", title="B", content=content, page_type="pdf")
        import hashlib
        h = hashlib.sha256(content.encode()).hexdigest()
        p1.content_hash = h
        p2.content_hash = h
        stats = {}
        with mock.patch.object(tp, "ANTHROPIC_API_KEY", "test-key"), \
             mock.patch.object(tp, "_is_complex_page", return_value=False), \
             mock.patch.object(tp, "_extract_with_model_routing", return_value=([
                 {"name": "Residential Service", "customer_class": "residential",
                  "rate_type": "flat",
                  "components": [{"component_type": "energy", "unit": "¢/kWh", "rate_value": 12.5}]},
             ], "sonnet")), \
             mock.patch.object(tp, "_page_has_rate_content", return_value=True), \
             mock.patch.object(tp.time, "sleep"):
            out = tp.phase3_extract_tariffs([p1, p2], "Example Electric", stats=stats, state="AZ")
        self.assertEqual(stats.get("pages_skipped_dup_hash"), 1)
        self.assertEqual(len(out), 1)


class TestSch1xxHints(unittest.TestCase):
    def test_all_schedules_from_adjustments_section(self):
        et = tp.ExtractedTariff(
            name="Schedule 7", customer_class="residential", rate_type="flat",
            description="Subject to adjustments: Schedule 100, Schedule 105, Schedule 125",
            riders_referenced_not_shown=["Schedule 100", "Schedule 105", "Schedule 125"],
            components=[{"component_type": "energy", "unit": "¢/kWh", "rate_value": 11.224}],
        )
        hints = tp._rider_search_hints([et])
        for n in ("100", "105", "125"):
            self.assertTrue(any(n in h for h in hints), msg=hints)


if __name__ == "__main__":
    unittest.main()
