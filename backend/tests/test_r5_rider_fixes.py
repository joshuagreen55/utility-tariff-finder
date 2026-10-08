"""Round-5 rider bugs (class-blind sharing, Sch-as-rider, stale docs, FAM mode,
BC Hydro prefix merge, full-bill siblings / superseded charges).

    cd backend && python -m unittest tests.test_r5_rider_fixes -v
"""
from __future__ import annotations

import logging
import unittest
from datetime import date
from unittest import mock

from scripts import tariff_pipeline as tp


class TestClassAwareRiderSharing(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_nsp_only_domestic_dcrr_on_residential(self):
        domestic = tp.ExtractedTariff(
            name="Domestic Service", code="02", customer_class="residential",
            rate_type="flat", confidence=0.9,
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 18.324},
                {"component_type": "fixed", "unit": "$/month", "rate_value": 20.08},
            ],
        )
        donors = [
            tp.ExtractedTariff(
                name="Small General DCRR rider", customer_class="commercial",
                rate_type="flat", confidence=0.9,
                components=[{
                    "component_type": "adjustment", "unit": "¢/kWh",
                    "rate_value": 0.729, "tier_label": "Small General DCRR",
                }],
            ),
            tp.ExtractedTariff(
                name="General DCRR rider", customer_class="commercial",
                rate_type="flat", confidence=0.9,
                components=[{
                    "component_type": "adjustment", "unit": "¢/kWh",
                    "rate_value": 0.749, "tier_label": "General DCRR",
                }],
            ),
            tp.ExtractedTariff(
                name="Domestic DCRR rider", customer_class="residential",
                rate_type="flat", confidence=0.9,
                components=[{
                    "component_type": "adjustment", "unit": "¢/kWh",
                    "rate_value": 0.648, "tier_label": "Domestic DCRR",
                }],
            ),
        ]
        report, valid = tp.phase4_validate(
            [domestic, *donors], "Nova Scotia Power", "NS",
        )
        self.assertEqual(report["valid"], 1)
        energy = [c for c in valid[0].components if c["component_type"] == "energy"][0]
        # 18.324 + 0.648 only — not +0.729 +0.749
        self.assertAlmostEqual(energy["rate_value"], 0.18972, places=5)
        adjs = [
            round(float(c["rate_value"]), 5)
            for c in valid[0].components
            if c["component_type"] == "adjustment"
        ]
        self.assertEqual(adjs, [0.00648])

    def test_never_stacks_three_dcrr_values_from_one_donor(self):
        domestic = tp.ExtractedTariff(
            name="Domestic Service", customer_class="residential",
            rate_type="flat", confidence=0.9,
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 18.324},
            ],
        )
        dcrr_page = tp.ExtractedTariff(
            name="DSM Cost Recovery Rider (DCRR)", customer_class="residential",
            rate_type="flat", confidence=0.9,
            components=[
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 0.729,
                 "tier_label": "Small General DCRR"},
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 0.749,
                 "tier_label": "General DCRR"},
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 0.648,
                 "tier_label": "Domestic DCRR"},
            ],
        )
        _report, valid = tp.phase4_validate(
            [domestic, dcrr_page], "Nova Scotia Power", "NS",
        )
        energy = [c for c in valid[0].components if c["component_type"] == "energy"][0]
        self.assertAlmostEqual(energy["rate_value"], 0.18972, places=5)


class TestRejectBaseScheduleAsRider(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_pge_schedule_32_td_not_shared_onto_sch7(self):
        sched7 = tp.ExtractedTariff(
            name="Schedule 7 Residential Service", code="7",
            customer_class="residential", rate_type="flat", confidence=0.9,
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 11.224,
                 "tier_label": "All-in (energy + transmission + distribution)"},
            ],
        )
        sch32 = tp.ExtractedTariff(
            name="Schedule 32 (riders)", code="32",
            customer_class="commercial", rate_type="flat", confidence=0.9,
            components=[
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 0.479,
                 "tier_label": "Schedule 32 Transmission"},
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 5.408,
                 "tier_label": "Schedule 32 Distribution first 5000 kWh"},
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 0.305,
                 "tier_label": "Schedule 32 Daily-Price wheeling"},
            ],
        )
        _report, valid = tp.phase4_validate(
            [sched7, sch32], "Portland General Electric", "OR",
        )
        sch7 = next(t for t in valid if "Schedule 7" in t.name)
        energy = [c for c in sch7.components if c["component_type"] == "energy"][0]
        self.assertAlmostEqual(energy["rate_value"], 0.11224, places=5)
        adjs = [c for c in sch7.components if c["component_type"] == "adjustment"]
        self.assertEqual(adjs, [])

    def test_filter_drops_base_schedule_extracts(self):
        sch32 = tp.ExtractedTariff(
            name="Schedule 32 Small Nonresidential", code="32",
            customer_class="commercial", rate_type="flat",
            components=[
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 5.408,
                 "tier_label": "Distribution"},
            ],
        )
        pca = tp.ExtractedTariff(
            name="Schedule 125 Power Cost Adjustment", code="125",
            customer_class="residential", rate_type="flat",
            components=[
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 8.4,
                 "tier_label": "Power Cost Adjustment"},
            ],
        )
        useful = tp._filter_useful_rider_extracts([sch32, pca])
        self.assertEqual([t.code for t in useful], ["125"])


class TestStaleRiderDocuments(unittest.TestCase):
    def test_2012_gra_is_stale(self):
        url = "https://www.nspower.ca/docs/nspi-2012-gra---1-de---redacted.pdf"
        self.assertTrue(tp._is_stale_rider_document(url, "NSPI 2012 GRA", ""))

    def test_current_tariff_book_not_stale(self):
        url = "https://www.nspower.ca/docs/default-source/regulatory/tariff-book-2026.pdf"
        self.assertFalse(
            tp._is_stale_rider_document(url, "Tariff Book 2026", "", today=date(2026, 10, 8))
        )

    def test_rank_prefers_utility_over_regulator_docket(self):
        utility = {
            "url": "https://portlandgeneral.com/rates/schedule-125.pdf",
            "title": "Schedule 125 PCA",
            "description": "Power Cost Adjustment",
        }
        docket = {
            "url": "https://edocs.puc.state.or.us/efdocs/UAA/ue394uaa155031.pdf",
            "title": "UE 394 filing",
            "description": "Oregon PUC rate case",
        }
        domains = {"portlandgeneral.com"}
        k_util = tp._rider_search_result_rank_key(
            utility, allowed_domains=domains, utility_domains=domains,
        )
        k_dock = tp._rider_search_result_rank_key(
            docket, allowed_domains=domains, utility_domains=domains,
        )
        self.assertLess(k_util, k_dock)

    def test_fetch_skips_stale_and_regulator_when_utility_domain_known(self):
        domestic = tp.ExtractedTariff(
            name="Domestic Service", customer_class="residential", rate_type="flat",
            description="FAM not shown on this page",
            source_url="https://www.nspower.ca/rates",
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 18.324},
            ],
        )
        stale = {
            "url": "https://www.nspower.ca/docs/nspi-2012-gra.pdf",
            "title": "2012 GRA", "description": "historical filing",
        }
        fam = {
            "url": "https://www.nspower.ca/about-us/producing/rates-tariffs/fam",
            "title": "Fuel Adjustment Mechanism", "description": "FAM current",
        }
        fam_page = tp.RatePage(
            url=fam["url"], title=fam["title"], page_type="html",
            content=(
                "Nova Scotia Power Fuel Adjustment Mechanism. "
                "FAM AA/BA 0.156 cents per kWh applies in addition to the energy "
                "charge for all Domestic Service customers. " + ("detail " * 40)
            ),
        )
        rider_extract = [
            tp.ExtractedTariff(
                name="Fuel Adjustment Mechanism", customer_class="residential",
                rate_type="flat", source_url=fam["url"],
                components=[{
                    "component_type": "adjustment", "unit": "¢/kWh",
                    "rate_value": 0.156, "tier_label": "FAM AA/BA",
                }],
            )
        ]
        with mock.patch.object(tp, "brave_search", return_value=[stale, fam]), \
             mock.patch.object(tp, "_fetch_and_parse", return_value=fam_page) as fetch_html, \
             mock.patch.object(tp, "_extract_rider_document", return_value=rider_extract):
            extra, pages, unresolved = tp.fetch_and_extract_referenced_riders(
                [domestic], "Nova Scotia Power", "NS",
                website_url="https://www.nspower.ca",
            )
        self.assertEqual(len(pages), 1)
        self.assertIn("fam", pages[0].url.lower())
        self.assertEqual(len(extra), 1)
        self.assertEqual(unresolved, [])
        # Stale URL must never be fetched.
        fetched_urls = [c.args[0] for c in fetch_html.call_args_list]
        self.assertTrue(all("2012" not in u for u in fetched_urls))


class TestRiderModeExtract(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_rider_mode_prompt_asks_for_adjustments_not_plans(self):
        self.assertIn("ADJUSTMENT", tp.RIDER_MODE_EXTRACT_PROMPT)
        self.assertIn("Do NOT extract base ENERGY", tp.RIDER_MODE_EXTRACT_PROMPT)
        self.assertIn("RIDER HINT", tp.RIDER_MODE_EXTRACT_PROMPT)

    def test_extract_rider_document_coerces_energy_to_adjustment(self):
        page = tp.RatePage(
            url="https://www.nspower.ca/fam", title="FAM",
            content="FAM 0.156 ¢/kWh " + ("x" * 200), page_type="html",
        )
        raw = [{
            "name": "Fuel Adjustment Mechanism",
            "customer_class": "residential",
            "rate_type": "flat",
            "components": [{
                "component_type": "energy", "unit": "¢/kWh", "rate_value": 0.156,
                "tier_label": "FAM",
            }],
        }]
        with mock.patch.object(tp, "_call_claude_tool", return_value=raw):
            out = tp._extract_rider_document(
                page, "Nova Scotia Power", "NS", hint="Fuel Adjustment Mechanism FAM",
            )
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0].components[0]["component_type"], "adjustment")
        self.assertEqual(out[0].extraction_tier, "rider_doc")


class TestBcHydroPrefixMergeEnergy(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_rider_only_tod_does_not_absorb_flat_with_energy(self):
        flat = tp.ExtractedTariff(
            name="Residential Flat Rate", customer_class="residential",
            rate_type="flat", confidence=0.9,
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 12.7},
                {"component_type": "fixed", "unit": "$/day", "rate_value": 0.2344},
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 0.0,
                 "tier_label": "placeholder"},
            ],
        )
        tod = tp.ExtractedTariff(
            name="Residential flat rate with time-of-day pricing",
            customer_class="residential", rate_type="tou", confidence=0.9,
            components=[
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 5.0,
                 "tier_label": "TOD on-peak"},
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": -5.0,
                 "tier_label": "TOD off-peak"},
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 0.1,
                 "tier_label": "other"},
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 0.2,
                 "tier_label": "other2"},
            ],
        )
        out = tp._merge_prefix_duplicates([flat, tod])
        names = {t.name for t in out}
        self.assertIn("Residential Flat Rate", names)
        # TOD may remain as its own product or be absorbed — but flat must survive.
        flat_kept = next(t for t in out if t.name == "Residential Flat Rate")
        self.assertTrue(tp._tariff_has_energy(flat_kept))


class TestFullBillSiblingAndSupersededCharge(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_prefer_full_bill_tou_over_base_only_duplicate(self):
        # R7: same product only when codes match or normalized names are
        # identical — different names (Pedernales flat vs Community Solar)
        # must not collapse. True siblings share the schedule name/code.
        base_only = tp.ExtractedTariff(
            name="Time-of-Use Rate", code="500.2.5",
            customer_class="residential",
            rate_type="tou", confidence=0.9,
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 4.3481,
                 "period_label": "Off-Peak"},
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 9.32,
                 "period_label": "Mid-Peak"},
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 16.18,
                 "period_label": "On-Peak"},
            ],
        )
        full = tp.ExtractedTariff(
            name="Time-of-Use Rate", code="500.2.5",
            customer_class="residential", rate_type="tou", confidence=0.9,
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 8.6715,
                 "period_label": "Off-Peak"},
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 13.6403,
                 "period_label": "Mid-Peak"},
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 20.5077,
                 "period_label": "On-Peak"},
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 2.2546,
                 "tier_label": "Delivery", "included_in_energy": True},
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 2.0688,
                 "tier_label": "TCOS", "included_in_energy": True},
            ],
        )
        out = tp._collapse_full_bill_siblings([base_only, full])
        self.assertEqual(len(out), 1)
        self.assertIn("500.2.5", out[0].name + (out[0].code or ""))
        # Differing names/codes stay separate (R7).
        other = tp.ExtractedTariff(
            name="TOU Portfolio", customer_class="residential", rate_type="tou",
            confidence=0.9,
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 4.3481,
                 "period_label": "Off-Peak"},
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 9.32,
                 "period_label": "Mid-Peak"},
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 16.18,
                 "period_label": "On-Peak"},
            ],
        )
        mixed = tp._collapse_full_bill_siblings([base_only, full, other])
        self.assertEqual(len(mixed), 2)
        self.assertIn("TOU Portfolio", {t.name for t in mixed})

    def test_drop_stale_and_current_tcos_as_two_tiers(self):
        comps = [
            {"component_type": "energy", "unit": "¢/kWh", "rate_value": 10.44},
            {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 1.3909,
             "tier_label": "TCOS (prior)"},
            {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 1.9930,
             "tier_label": "TCOS"},
        ]
        out = tp.drop_superseded_same_family_adjustments(comps)
        tcos = [
            c for c in out
            if c.get("component_type") == "adjustment" and "TCOS" in str(c.get("tier_label"))
        ]
        self.assertEqual(len(tcos), 1)
        self.assertAlmostEqual(float(tcos[0]["rate_value"]), 1.9930, places=4)

    def test_drop_two_unlabeled_tcos_keeps_higher(self):
        comps = [
            {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 1.3909,
             "tier_label": "TCOS"},
            {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 1.9930,
             "tier_label": "TCOS"},
        ]
        out = tp.drop_superseded_same_family_adjustments(comps)
        self.assertEqual(len(out), 1)
        self.assertAlmostEqual(float(out[0]["rate_value"]), 1.9930, places=4)


if __name__ == "__main__":
    unittest.main()
