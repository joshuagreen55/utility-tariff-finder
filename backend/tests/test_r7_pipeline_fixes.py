"""Round-7 pipeline fixes — offline unit tests only (no live LLM).

    cd backend && python -m unittest tests.test_r7_pipeline_fixes -v
"""
from __future__ import annotations

import logging
import unittest

from scripts import scrape_oeb_rates as oeb
from scripts import tariff_pipeline as tp


class TestOptionalBeforeMerge(unittest.TestCase):
    """R7.1: drop optional programmes before full-bill merge."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_community_solar_does_not_absorb_flat(self):
        flat = tp.ExtractedTariff(
            name="Residential Service", customer_class="residential", rate_type="flat",
            components=[{"component_type": "energy", "unit": "¢/kWh", "rate_value": 8.67}],
        )
        solar = tp.ExtractedTariff(
            name="Community Solar", customer_class="residential", rate_type="flat",
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 8.67},
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": -2.0,
                 "tier_label": "Solar received"},
            ],
        )
        _r, valid = tp.phase4_validate([flat, solar], "Pedernales", "TX")
        names = {t.name for t in valid}
        self.assertIn("Residential Service", names)
        self.assertNotIn("Community Solar", names)

    def test_different_names_never_full_bill_merge(self):
        ev = tp.ExtractedTariff(
            name="Schedule 52 EV", code="52", customer_class="residential", rate_type="tou",
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 4.128,
                 "period_start_time": "22:00", "period_end_time": "06:00",
                 "day_type": "all", "period_label": "Off-Peak"},
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 12.0,
                 "period_start_time": "06:00", "period_end_time": "22:00",
                 "day_type": "all", "period_label": "On-Peak"},
            ],
        )
        portfolio = tp.ExtractedTariff(
            name="TOU Portfolio", customer_class="residential", rate_type="tou",
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 3.41,
                 "period_start_time": "22:00", "period_end_time": "06:00",
                 "day_type": "all"},
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 10.0,
                 "period_start_time": "06:00", "period_end_time": "22:00",
                 "day_type": "all"},
            ],
        )
        self.assertFalse(tp._full_bill_product_match(ev, portfolio))
        out = tp._collapse_full_bill_siblings([ev, portfolio])
        self.assertEqual(len(out), 2)


class TestOptionalRateScheduleKept(unittest.TestCase):
    """R7.2: NL 1.2DS '- Optional' is a rate schedule, not an add-on."""

    def test_1_2ds_optional_kept(self):
        et = tp.ExtractedTariff(
            name="Rate No. 1.2DS Domestic Diesel Seasonal - Optional",
            code="1.2DS", customer_class="residential", rate_type="seasonal",
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 18.0,
                 "season": "Winter",
                 "season_start_month": 12, "season_start_day": 1,
                 "season_end_month": 4, "season_end_day": 30},
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 14.0,
                 "season": "Non-Winter",
                 "season_start_month": 5, "season_start_day": 1,
                 "season_end_month": 11, "season_end_day": 30},
            ],
        )
        self.assertFalse(tp._is_optional_program_tariff(et))

    def test_community_solar_still_dropped(self):
        et = tp.ExtractedTariff(
            name="Community Solar Subscription", customer_class="residential",
            rate_type="flat",
            components=[{"component_type": "energy", "unit": "¢/kWh", "rate_value": 5.0}],
        )
        self.assertTrue(tp._is_optional_program_tariff(et))


class TestBrokenTouClocksKept(unittest.TestCase):
    """R7.3: repair or keep+flag — never drop for imperfect clocks."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_one_hour_gap_repaired(self):
        # R8: never stretch a different period family across a gap. Same-family
        # neighbors may extend; Peak↔Off-Peak gaps stay open unless the
        # document states the hour is partial-peak.
        same_family = [
            {"component_type": "energy", "unit": "¢/kWh", "rate_value": 10.0,
             "period_start_time": "00:00", "period_end_time": "15:00",
             "day_type": "weekday", "period_label": "Off-Peak"},
            {"component_type": "energy", "unit": "¢/kWh", "rate_value": 11.0,
             "period_start_time": "16:00", "period_end_time": "24:00",
             "day_type": "weekday", "period_label": "Off-Peak Evening"},
            # gap 15:00–16:00 between two off-peak windows
        ]
        out, notes = tp.repair_one_hour_tou_gaps(same_family)
        self.assertTrue(any("tou_clock_gap_repaired" in n for n in notes))
        first = next(c for c in out if c["period_label"] == "Off-Peak")
        self.assertEqual(first["period_end_time"], "16:00")

        # Cross-family Peak/Off-Peak 1h gap (non-wrapping): leave open.
        cross = [
            {"component_type": "energy", "unit": "¢/kWh", "rate_value": 20.0,
             "period_start_time": "16:00", "period_end_time": "21:00",
             "day_type": "weekday", "period_label": "Peak"},
            {"component_type": "energy", "unit": "¢/kWh", "rate_value": 10.0,
             "period_start_time": "00:00", "period_end_time": "15:00",
             "day_type": "weekday", "period_label": "Off-Peak"},
        ]
        _out2, notes2 = tp.repair_one_hour_tou_gaps(cross)
        self.assertFalse(any("tou_clock_gap_repaired" in n for n in notes2))
        self.assertFalse(any("tou_clock_gap_filled" in n for n in notes2))

        # Stated partial-peak for 3–4 pm fills from a printed partial row.
        with_partial = [
            {"component_type": "energy", "unit": "¢/kWh", "rate_value": 39.0,
             "period_start_time": "14:00", "period_end_time": "15:00",
             "day_type": "weekday", "period_label": "Partial-Peak"},
            {"component_type": "energy", "unit": "¢/kWh", "rate_value": 20.0,
             "period_start_time": "16:00", "period_end_time": "21:00",
             "day_type": "weekday", "period_label": "Peak"},
            {"component_type": "energy", "unit": "¢/kWh", "rate_value": 10.0,
             "period_start_time": "21:00", "period_end_time": "14:00",
             "day_type": "weekday", "period_label": "Off-Peak"},
        ]
        out3, notes3 = tp.repair_one_hour_tou_gaps(
            with_partial, missing_fields=["3-4 p.m. partial peak"]
        )
        self.assertTrue(any("tou_clock_gap_filled_from_stated" in n for n in notes3))
        filled = [
            c for c in out3
            if c.get("period_start_time") == "15:00" and c.get("period_end_time") == "16:00"
        ]
        self.assertEqual(len(filled), 1)
        self.assertAlmostEqual(float(filled[0]["rate_value"]), 39.0, places=2)

    def test_gap_kept_with_needs_review_not_rejected(self):
        # Larger gap (not 1h) — keep plan, flag review.
        et = tp.ExtractedTariff(
            name="EV2-A", customer_class="residential", rate_type="tou",
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 20.0,
                 "period_start_time": "16:00", "period_end_time": "21:00",
                 "day_type": "weekday"},
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 10.0,
                 "period_start_time": "21:00", "period_end_time": "14:00",
                 "day_type": "weekday"},
                # gap 14:00–16:00 (2h) — not auto-repaired
            ],
        )
        _r, valid = tp.phase4_validate([et], "PG&E", "CA")
        self.assertEqual(len(valid), 1)
        self.assertTrue(valid[0].needs_review)
        reasons = " ".join(str(r) for r in (valid[0].computable_reasons or []))
        self.assertTrue("tou_gap" in reasons or "tou_clock" in reasons)

    def test_tou_tiered_reclassified_without_drop(self):
        et = tp.ExtractedTariff(
            name="E-TOU-C", customer_class="residential", rate_type="tou",
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 32.0,
                 "period_start_time": "16:00", "period_end_time": "21:00",
                 "day_type": "weekday", "tier_min_kwh": 0, "tier_max_kwh": 100},
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 40.0,
                 "period_start_time": "16:00", "period_end_time": "21:00",
                 "day_type": "weekday", "tier_min_kwh": 100, "tier_max_kwh": None},
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 15.0,
                 "period_start_time": "21:00", "period_end_time": "16:00",
                 "day_type": "weekday", "tier_min_kwh": 0, "tier_max_kwh": None},
            ],
        )
        _r, valid = tp.phase4_validate([et], "PG&E", "CA")
        self.assertEqual(len(valid), 1)
        self.assertEqual(valid[0].rate_type, "tou_tiered")


class TestNspRidersAndOptionalCharges(unittest.TestCase):
    """R7.4: FAM one-hop, DSM on all residential, Green Power charge strip."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_fam_link_hub_triggers_one_hop(self):
        page = tp.RatePage(
            url="https://www.nspower.ca/about-us/producing/rates-tariffs/fam",
            title="Fuel Adjustment Mechanism",
            page_type="html",
            content=(
                "<html><body><p>The Fuel Adjustment Mechanism (FAM) recovers fuel costs.</p>"
                "<p>See 0.00 note.</p>"
                '<a href="/docs/fam-tariff-2026.pdf">Fuel Adjustment Mechanism tariff PDF</a>'
                '<a href="/docs/dsm-dcrr.pdf">DSM DCRR rider</a>'
                "</body></html>"
            ),
        )
        self.assertTrue(tp._page_is_rider_link_hub(page))
        links = tp._rider_page_one_hop_links(
            page, allowed_domains={"nspower.ca"},
        )
        self.assertTrue(any("fam-tariff" in u for u in links))

    def test_dsm_shared_to_all_residential(self):
        domestic = tp.ExtractedTariff(
            name="Domestic Service", code="02", customer_class="residential",
            rate_type="flat",
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 18.324},
            ],
        )
        cpp = tp.ExtractedTariff(
            name="Domestic Service Critical Peak", code="80",
            customer_class="residential", rate_type="tou",
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 18.324,
                 "period_start_time": "00:00", "period_end_time": "00:00",
                 "day_type": "all"},
                {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 0.648,
                 "tier_label": "DSM DCRR"},
            ],
        )
        n = tp.apply_shared_stacking_riders_across_batch([domestic, cpp])
        self.assertGreaterEqual(n, 1)
        dsm = [
            c for c in domestic.components
            if str(c.get("component_type")).lower() == "adjustment"
            and "dsm" in str(c.get("tier_label") or "").lower()
        ]
        self.assertEqual(len(dsm), 1)
        self.assertAlmostEqual(float(dsm[0]["rate_value"]), 0.648, places=3)

    def test_green_power_fixed_stripped_from_domestic(self):
        et = tp.ExtractedTariff(
            name="Domestic Service", customer_class="residential", rate_type="flat",
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 18.324},
                {"component_type": "fixed", "unit": "$/month", "rate_value": 20.08,
                 "tier_label": "Customer Charge"},
                {"component_type": "fixed", "unit": "$/month", "rate_value": 5.0,
                 "tier_label": "Green Power"},
            ],
        )
        n = tp.strip_optional_program_components(et)
        self.assertEqual(n, 1)
        fixed = [c for c in et.components if c["component_type"] == "fixed"]
        self.assertEqual(len(fixed), 1)
        self.assertAlmostEqual(float(fixed[0]["rate_value"]), 20.08, places=2)


class TestPgeTiersAndSch1xx(unittest.TestCase):
    """R7.5: preserve tiers when folding riders; harvest Sch 1xx from index."""

    def test_stacking_preserves_tier_bounds(self):
        comps = [
            {"component_type": "energy", "unit": "¢/kWh", "rate_value": 17.01,
             "tier_min_kwh": 0, "tier_max_kwh": 1000, "tier_label": "First 1000 kWh"},
            {"component_type": "energy", "unit": "¢/kWh", "rate_value": 20.50,
             "tier_min_kwh": 1000, "tier_max_kwh": None, "tier_label": "Over 1000 kWh"},
            {"component_type": "adjustment", "unit": "¢/kWh", "rate_value": 2.5,
             "tier_label": "Schedule 125 PCA"},
        ]
        out = tp.expand_stacking_energy_riders(comps, rate_type="tiered")
        energy = [c for c in out if c["component_type"] == "energy"]
        self.assertEqual(len(energy), 2)
        by_max = {c.get("tier_max_kwh"): c for c in energy}
        self.assertIn(1000, by_max)
        self.assertAlmostEqual(float(by_max[1000]["rate_value"]), 19.51, places=2)
        self.assertEqual(by_max[1000].get("tier_min_kwh"), 0)
        open_top = next(c for c in energy if c.get("tier_max_kwh") is None)
        self.assertAlmostEqual(float(open_top["rate_value"]), 23.0, places=2)

    def test_harvest_sch1xx_from_index_html(self):
        html = """
        <html><body>
          <a href="/rates/schedule-100.pdf">Schedule 100</a>
          <a href="/rates/schedule-105-adjust.pdf">Schedule 105 Adjustment</a>
          <a href="/rates/schedule-125-pca.pdf">Schedule 125 PCA</a>
          <a href="/about/careers">Careers</a>
        </body></html>
        """
        links = tp._harvest_sch1xx_links_from_html(
            html,
            base_url="https://portlandgeneral.com/rates/index",
            allowed_domains={"portlandgeneral.com"},
        )
        joined = " ".join(links).lower()
        self.assertIn("schedule-100", joined)
        self.assertIn("schedule-105", joined)
        self.assertIn("schedule-125", joined)


class TestNeedsReviewCriticalOnly(unittest.TestCase):
    """R7.6: needs_review only for Mysa-critical gaps."""

    def test_holiday_and_baseline_not_critical(self):
        self.assertFalse(
            tp._is_mysa_critical_review_reason(
                completeness_reasons=["holiday_rows_require_calendar"],
            )
        )
        self.assertFalse(
            tp._is_mysa_critical_review_reason(
                missing_fields=["baseline_kwh", "effective_date", "fixed_charge"],
            )
        )

    def test_tou_gap_and_missing_energy_critical(self):
        self.assertTrue(
            tp._is_mysa_critical_review_reason(
                computable_reasons=["tou_gap:weekday"],
            )
        )
        self.assertTrue(
            tp._is_mysa_critical_review_reason(
                missing_fields=["energy"],
            )
        )
        self.assertTrue(
            tp._is_mysa_critical_review_reason(
                riders_missing=["Schedule 125"],
            )
        )


class TestOntarioLossFactor(unittest.TestCase):
    """R7.7 / R8b.4: BillData LF on commodity + transmission/regulatory."""

    def test_lf_applied_to_commodity_only(self):
        rates = oeb.OEBRateSet(
            tou=oeb.TOURates(
                effective_date="2025-11-01",
                off_peak=0.098, mid_peak=0.157, on_peak=0.203,
            ),
        )
        ldc = oeb.LDCDeliveryCharges(
            distributor="Toronto Hydro-Electric System Limited",
            rate_class="RESIDENTIAL",
            distribution_kwh=0.0016,
            transmission_network=0.01346,
            transmission_connection=0.00895,
            wholesale_market=0.0047,
            rural_remote=0.0006,
            loss_factor=1.0295,
            service_charge=51.56,
        )
        entries = oeb.build_tariff_entries(rates, "residential", ldc=ldc)
        tou = next(e for e in entries if e["code"] == "OEB-RPP-TOU")
        off = next(
            c for c in tou["components"]
            if c["component_type"] == "energy" and c.get("period_label") == "Off-Peak"
        )
        expected = round(0.098 * 1.0295 + ldc.per_kwh_adder, 6)
        self.assertAlmostEqual(float(off["rate_value"]), expected, places=5)
        self.assertEqual(tou["ldc_delivery"]["loss_factor"], 1.0295)
        # LF must not appear as a priced ADJUSTMENT (R8).
        self.assertFalse(
            any(
                abs(float(c.get("rate_value") or 0) - 1.0295) < 1e-9
                for c in tou["components"]
                if c.get("component_type") == "adjustment"
            )
        )


if __name__ == "__main__":
    unittest.main()
