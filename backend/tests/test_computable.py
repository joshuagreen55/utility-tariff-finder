"""Computable contract (completeness v2), unit normalization and rider flags.

The first class replays the audit's completeness-gap probe: shapes that pass
completeness v1 but cannot be priced for every interval must be
computable=False under v2 with a machine-readable reason.

    cd backend && python -m unittest tests.test_computable -v
"""
from __future__ import annotations

import unittest

from app.services.computable import evaluate_computable, periodic_unit_basis
from app.services.tou_seasonal_completeness import is_complete
from scripts import repair_hydro_one_oeb_residential as oeb
from scripts import tariff_pipeline as tp


def energy(v, **kw):
    return {"component_type": "energy", "unit": "$/kWh", "rate_value": v, **kw}


def window(v, start, end, day="all", **kw):
    return energy(v, period_start_time=start, period_end_time=end, day_type=day, **kw)


def season(sm, sd, em, ed):
    return {"season_start_month": sm, "season_start_day": sd,
            "season_end_month": em, "season_end_day": ed}


def codes(result):
    return {r.split(":", 1)[0] for r in result.reasons}


class TestAuditProbesNowNonComputable(unittest.TestCase):
    """Each shape passes v1 completeness but must fail v2."""

    def assert_v1_pass_v2_fail(self, rate_type, comps, expected_code):
        self.assertTrue(is_complete(rate_type, comps), "probe should pass v1")
        res = evaluate_computable(rate_type, comps)
        self.assertFalse(res.computable)
        self.assertIn(expected_code, codes(res), res.reasons)

    def test_single_six_hour_window(self):
        self.assert_v1_pass_v2_fail("tou", [window(0.3, "14:00", "20:00", "weekday")], "tou_gap")

    def test_overlapping_windows(self):
        self.assert_v1_pass_v2_fail("tou", [
            window(0.10, "00:00", "00:00"),
            window(0.30, "16:00", "21:00"),
        ], "tou_overlap")

    def test_seasonal_covering_four_months(self):
        self.assert_v1_pass_v2_fail("seasonal", [energy(0.2, **season(6, 1, 9, 30))], "season_gap")

    def test_tier_gap(self):
        self.assert_v1_pass_v2_fail("tiered", [
            energy(0.10, tier_min_kwh=0, tier_max_kwh=500),
            energy(0.14, tier_min_kwh=1000),
        ], "tier_gap")

    def test_invalid_calendar_date(self):
        self.assert_v1_pass_v2_fail("seasonal", [
            energy(0.2, **season(11, 1, 2, 31)),
            energy(0.1, **season(3, 1, 10, 31)),
        ], "season_invalid_date")

    def test_missing_day_type(self):
        self.assert_v1_pass_v2_fail("tou", [
            energy(0.1, period_start_time="00:00", period_end_time="00:00"),
        ], "tou_missing_day_type")


class TestComputableShapes(unittest.TestCase):
    def test_oeb_seasonal_tou_is_computable_with_holiday_warning(self):
        comps = oeb.build_plan_components("tou")
        res = evaluate_computable("seasonal_tou", comps)
        self.assertTrue(res.computable, res.reasons)
        self.assertIn("holiday_rows_require_calendar", res.warnings)
        with_cal = evaluate_computable("seasonal_tou", comps, holiday_calendar="CA-ON")
        self.assertNotIn("holiday_rows_require_calendar", with_cal.warnings)

    def test_oeb_ulo_and_tiered_are_computable(self):
        self.assertTrue(evaluate_computable("tou", oeb.build_plan_components("ulo")).computable)
        tiered = evaluate_computable("seasonal_tiered", oeb.build_plan_components("tiered"))
        self.assertTrue(tiered.computable, tiered.reasons)
        self.assertIn("tiers_accumulate_per_billing_period", tiered.warnings)

    def test_flat_with_daily_fixed_charge(self):
        res = evaluate_computable("flat", [
            energy(0.15),
            {"component_type": "fixed", "unit": "$/day", "rate_value": 0.455},
            {"component_type": "minimum", "unit": "$/month", "rate_value": 10},
        ])
        self.assertTrue(res.computable, res.reasons)
        self.assertIn("minimum_charge_is_bill_floor", res.warnings)

    def test_inclusive_tier_bounds_and_leap_day_boundary(self):
        self.assertTrue(evaluate_computable("tiered", [
            energy(0.10, tier_min_kwh=0, tier_max_kwh=500),
            energy(0.12, tier_min_kwh=501, tier_max_kwh=1000),
            energy(0.14, tier_min_kwh=1001),
        ]).computable)
        res = evaluate_computable("seasonal", [
            energy(0.2, **season(11, 1, 2, 28)),
            energy(0.1, **season(3, 1, 10, 31)),
        ])
        self.assertTrue(res.computable, res.reasons)

    def test_weekday_weekend_split(self):
        res = evaluate_computable("tou", [
            window(0.08, "21:00", "07:00", "weekday"),
            window(0.20, "07:00", "21:00", "weekday"),
            window(0.08, "00:00", "00:00", "weekend"),
        ])
        self.assertTrue(res.computable, res.reasons)
        missing_weekend = evaluate_computable("tou", [window(0.08, "00:00", "00:00", "weekday")])
        self.assertIn("tou_gap:weekend", missing_weekend.reasons)


class TestNonComputableShapes(unittest.TestCase):
    def test_demand(self):
        res = evaluate_computable("flat", [
            energy(0.1), {"component_type": "demand", "unit": "$/kW", "rate_value": 9},
        ])
        self.assertIn("demand_charges_unsupported", res.reasons)
        self.assertIn("demand_charges_unsupported", evaluate_computable("demand_tou", [energy(0.1)]).reasons)

    def test_tou_tiered(self):
        self.assertIn("tou_tiered_unsupported", evaluate_computable("tou_tiered", [energy(0.1)]).reasons)
        res = evaluate_computable("tou", [
            window(0.1, "00:00", "00:00", tier_min_kwh=0, tier_max_kwh=500),
            window(0.2, "00:00", "00:00", tier_min_kwh=500),
        ])
        self.assertIn("tou_tiered_unsupported", res.reasons)

    def test_critical_peak_and_dynamic(self):
        res = evaluate_computable("tou", [
            window(0.1, "00:00", "00:00"),
            energy(0.9, period_label="Critical Peak event"),
        ])
        self.assertIn("event_pricing_unsupported", res.reasons)
        self.assertIn(
            "dynamic_pricing_unsupported",
            evaluate_computable("flat", [energy(0.1)], name="Hourly Pricing Program").reasons,
        )

    def test_complex_and_unknown(self):
        self.assertIn("complex_rate_type_unsupported", evaluate_computable("complex", [energy(0.1)]).reasons)
        self.assertIn("unknown_rate_type", evaluate_computable("mystery", [energy(0.1)]).reasons)

    def test_ambiguous_flat_and_bad_units(self):
        self.assertIn("ambiguous_energy_rows", evaluate_computable("flat", [energy(0.1), energy(0.2)]).reasons)
        res = evaluate_computable("flat", [
            energy(0.1), {"component_type": "adjustment", "unit": "%", "rate_value": 3},
        ])
        self.assertIn("unrecognized_unit", codes(res))
        self.assertIn("energy_unit_not_per_kwh", codes(evaluate_computable("flat", [
            {"component_type": "energy", "unit": "¢/kWh", "rate_value": 12},
        ])))

    def test_missing_energy(self):
        res = evaluate_computable("flat", [{"component_type": "fixed", "unit": "$/month", "rate_value": 9}])
        self.assertIn("missing_energy_rates", res.reasons)


class TestRiderDoubleCount(unittest.TestCase):
    def test_folded_riders_are_flagged_and_computable(self):
        comps = tp.expand_stacking_energy_riders([
            energy(0.12),
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.01, "tier_label": "FAM"},
        ])
        adj = [c for c in comps if c["component_type"] == "adjustment"]
        self.assertEqual(len(adj), 1)
        self.assertTrue(adj[0]["included_in_energy"])
        e = [c for c in comps if c["component_type"] == "energy"]
        self.assertAlmostEqual(e[0]["rate_value"], 0.13)
        self.assertTrue(evaluate_computable("flat", comps).computable)

    def test_folding_is_idempotent_once_flagged(self):
        once = tp.expand_stacking_energy_riders([
            energy(0.12),
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.01, "tier_label": "FAM"},
        ])
        twice = tp.expand_stacking_energy_riders(once)
        self.assertEqual(
            sorted(c["rate_value"] for c in twice if c["component_type"] == "energy"), [0.13]
        )

    def test_relative_seasonal_riders_are_flagged(self):
        comps = tp.expand_relative_seasonal_energy([
            energy(0.15587),
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.00953, "season": "Winter"},
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": -0.01297, "season": "Non-Winter"},
        ])
        self.assertTrue(all(
            c["included_in_energy"] for c in comps if c["component_type"] == "adjustment"
        ))

    def test_legacy_all_in_with_unflagged_rider_is_ambiguous(self):
        res = evaluate_computable("flat", [
            energy(0.13, tier_label="Base (all-in +riders)"),
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.01},
        ])
        self.assertIn("rider_inclusion_ambiguous", res.reasons)


class TestCentsUnitNormalization(unittest.TestCase):
    def _norm(self, ctype, value, unit):
        t = tp.ExtractedTariff(name="x", components=[
            {"component_type": ctype, "unit": unit, "rate_value": value},
        ])
        tp._normalize_component_units(t, p99_energy=0.4)
        return t.components[0]["rate_value"], t.components[0]["unit"]

    def test_cents_per_day_stays_daily(self):
        v, u = self._norm("fixed", 45.5, "¢/day")
        self.assertAlmostEqual(v, 0.455)
        self.assertEqual(u, "$/day")
        self.assertEqual(periodic_unit_basis(u), "day")

    def test_other_cents_denominators(self):
        self.assertEqual(self._norm("energy", 12.5, "cents/kWh")[1], "$/kWh")
        self.assertEqual(self._norm("fixed", 1200, "cents per month")[1], "$/month")
        self.assertEqual(self._norm("demand", 900, "¢/kW")[1], "$/kW")
        self.assertEqual(self._norm("fixed", 1200, "¢")[1], "$/month")

    def test_daily_fixed_passes_bounds_as_monthly_equivalent(self):
        t = tp.ExtractedTariff(
            name="Residential", customer_class="residential", rate_type="flat",
            components=[energy(0.15), {"component_type": "fixed", "unit": "¢/day", "rate_value": 45.5}],
        )
        report, valid = tp.phase4_validate([t], "Utility", "NS")
        self.assertEqual(len(valid), 1, report)


if __name__ == "__main__":
    unittest.main()
