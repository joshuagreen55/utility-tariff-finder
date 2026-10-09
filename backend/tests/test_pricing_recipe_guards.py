"""Calculator recipe guards: never a silent zero, drop, or unit slip.

    cd backend && python -m unittest tests.test_pricing_recipe_guards -v
"""
from __future__ import annotations

import unittest
from decimal import Decimal

from app.services.pricing.compiler import compile_plan
from app.services.pricing.preaccept import gate_structure
from app.services.pricing.recipes import (
    RecipeError,
    recipe_bundled,
    recipe_deregulated,
    recipe_provincial_alberta,
    recipe_provincial_ontario,
    recipe_texas_tdu,
)
from app.services.pricing.types import (
    CellKey,
    ComponentInput,
    PlanInput,
    energy_dollars_per_kwh,
    normalize_cell_label,
)


def _c(code, kind, unit, *cells, **kw) -> ComponentInput:
    return ComponentInput(code=code, kind=kind, unit=unit, cells=list(cells), **kw)


def _base(*cells) -> ComponentInput:
    return _c("base", "base_energy", "¢/kWh", *cells)


def _values(out: dict[CellKey, Decimal]) -> dict[tuple, Decimal]:
    return {(k.season, k.period, k.day_type, k.tier): v for k, v in out.items()}


class TestNoSilentZero(unittest.TestCase):
    def test_sparse_tou_rider_holds(self):
        comps = [
            _base(
                {"amount": "20", "period": "on_peak"},
                {"amount": "10", "period": "off_peak"},
            ),
            _c("fuel", "rider_per_kwh", "¢/kWh",
               {"amount": "1", "period": "on_peak"}),
        ]
        with self.assertRaises(RecipeError):
            recipe_bundled(comps)

    def test_summer_only_rider_on_flat_base_holds(self):
        comps = [
            _base({"amount": "12"}),
            _c("adder", "rider_per_kwh", "¢/kWh",
               {"amount": "1", "season": "summer"}),
        ]
        with self.assertRaisesRegex(RecipeError, "partial coverage"):
            recipe_bundled(comps)

    def test_label_mismatch_holds(self):
        comps = [
            _base(
                {"amount": "20", "period": "on_peak"},
                {"amount": "10", "period": "off_peak"},
            ),
            _c("fuel", "rider_per_kwh", "¢/kWh",
               {"amount": "1", "period": "peak"},
               {"amount": "1", "period": "offpeak"}),
        ]
        with self.assertRaises(RecipeError):
            recipe_bundled(comps)

    def test_single_labelled_cell_not_applied_everywhere(self):
        comps = [
            _base(
                {"amount": "20", "season": "summer"},
                {"amount": "15", "season": "winter"},
            ),
            _c("storm", "rider_per_kwh", "¢/kWh",
               {"amount": "1", "season": "summer"}),
        ]
        with self.assertRaises(RecipeError):
            recipe_bundled(comps)


class TestGridShape(unittest.TestCase):
    def test_seasonal_rider_on_flat_base_splits(self):
        comps = [
            _base({"amount": "12"}),
            _c("adder", "rider_per_kwh", "¢/kWh",
               {"amount": "1", "season": "summer"},
               {"amount": "0.5", "season": "winter"}),
        ]
        got = _values(recipe_bundled(comps))
        self.assertEqual(got, {
            ("summer", "all", "all", "all"): Decimal("0.13"),
            ("winter", "all", "all", "all"): Decimal("0.125"),
        })

    def test_weekday_peak_does_not_grow_weekend_peak(self):
        comps = [
            _base(
                {"amount": "30", "period": "on_peak", "day_type": "weekday"},
                {"amount": "10", "period": "off_peak"},
            ),
            _c("fuel", "rider_per_kwh", "¢/kWh", {"amount": "2"}),
        ]
        got = _values(recipe_bundled(comps))
        self.assertEqual(got, {
            ("all", "off_peak", "all", "all"): Decimal("0.12"),
            ("all", "on_peak", "weekday", "all"): Decimal("0.32"),
        })

    def test_catch_all_mixed_with_specific_holds(self):
        comps = [
            _base({"amount": "12"}, {"amount": "15", "season": "summer"}),
        ]
        with self.assertRaises(RecipeError):
            recipe_bundled(comps)

    def test_label_spelling_normalized(self):
        comps = [
            _base(
                {"amount": "20", "period": "On-Peak"},
                {"amount": "10", "period": "off peak"},
            ),
            _c("fuel", "rider_per_kwh", "¢/kWh",
               {"amount": "1", "period": "on_peak"},
               {"amount": "1", "period": "Off-Peak"}),
        ]
        got = _values(recipe_bundled(comps))
        self.assertEqual(got[("all", "on_peak", "all", "all")], Decimal("0.21"))
        self.assertEqual(got[("all", "off_peak", "all", "all")], Decimal("0.11"))

    def test_duplicate_cell_with_different_amounts_holds(self):
        comps = [_base({"amount": "12"}, {"amount": "13"})]
        with self.assertRaises((RecipeError, ValueError)):
            recipe_bundled(comps)


class TestNoDroppedComponents(unittest.TestCase):
    def test_bundled_unconsumed_kind_holds(self):
        comps = [
            _base({"amount": "12"}),
            _c("del", "delivery_per_kwh", "¢/kWh", {"amount": "3"}),
        ]
        with self.assertRaisesRegex(RecipeError, "does not price"):
            recipe_bundled(comps)

    def test_deregulated_uncategorized_rider_holds(self):
        comps = [
            _c("del", "delivery_per_kwh", "¢/kWh", {"amount": "6"},
               charge_category="delivery"),
            _c("sup", "default_supply", "¢/kWh", {"amount": "9"},
               charge_category="supply"),
            _c("dsic", "rider_per_kwh", "¢/kWh", {"amount": "0.4"}),
        ]
        with self.assertRaisesRegex(RecipeError, "does not price"):
            recipe_deregulated(comps)

    def test_fixed_charge_is_not_priced(self):
        comps = [
            _base({"amount": "12"}),
            _c("cust", "customer_charge", "$/month", {"amount": "10.50"}),
        ]
        self.assertEqual(
            list(recipe_bundled(comps).values()), [Decimal("0.12")]
        )

    def test_ontario_rate_rider_is_priced(self):
        comps = [
            _c("rpp", "regulated_commodity", "¢/kWh", {"amount": "10"}),
            _c("dist", "delivery_per_kwh", "¢/kWh", {"amount": "2"}),
            _c("net", "delivery_per_kwh", "¢/kWh", {"amount": "1"},
               loss_sensitive=True),
            _c("lrc", "rider_per_kwh", "¢/kWh", {"amount": "0.1"}),
            _c("lf", "multiplier", "dimensionless", {"amount": "1.03"}),
        ]
        got = list(recipe_provincial_ontario(comps).values())
        self.assertEqual(
            got, [Decimal("0.10") * Decimal("1.03") + Decimal("0.021")
                  + Decimal("0.01") * Decimal("1.03")]
        )

    def test_alberta_multiplier_missing_target_holds(self):
        comps = [
            _c("rolr", "default_supply", "¢/kWh", {"amount": "12"}),
            _c("dist", "delivery_per_kwh", "¢/kWh", {"amount": "4"}),
            _c("btar", "multiplier", "dimensionless", {"amount": "0.99"},
               multiplier_target_codes=["trans"]),
        ]
        with self.assertRaisesRegex(RecipeError, "missing codes"):
            recipe_provincial_alberta(comps)


class TestUnits(unittest.TestCase):
    def test_percent_unit_rider_per_kwh_holds(self):
        comps = [
            _base({"amount": "12"}),
            _c("frac", "rider_per_kwh", "percent", {"amount": "3"}),
        ]
        with self.assertRaises(RecipeError):
            recipe_bundled(comps)

    def test_percent_rider_needs_percent_unit(self):
        comps = [
            _base({"amount": "10"}),
            _c("ecr", "rider_percent", "¢/kWh", {"amount": "5"},
               percent_base_codes=["base"]),
        ]
        with self.assertRaisesRegex(RecipeError, "percent unit"):
            recipe_bundled(comps)

    def test_texas_percent_rider_not_summed_as_dollars(self):
        comps = [
            _c("tdu", "delivery_per_kwh", "$/kWh", {"amount": "0.05"},
               charge_category="delivery"),
            _c("tcrf", "rider_percent", "percent", {"amount": "10"},
               charge_category="delivery", percent_base_codes=["tdu"]),
        ]
        self.assertEqual(
            list(recipe_texas_tdu(comps).values()), [Decimal("0.055")]
        )

    def test_credit_lowers_price_whatever_the_printed_sign(self):
        for printed in ("0.5", "-0.5"):
            comps = [
                _base({"amount": "12"}),
                _c("cr", "credit", "¢/kWh", {"amount": printed}),
            ]
            self.assertEqual(
                list(recipe_bundled(comps).values()), [Decimal("0.115")],
                printed,
            )

    def test_energy_dollars_per_kwh_table(self):
        cases = {
            "¢/kWh": Decimal("0.12"),
            "cents/kWh": Decimal("0.12"),
            "$/kWh": Decimal("12"),
            "mills/kWh": Decimal("0.012"),
        }
        for unit, want in cases.items():
            self.assertEqual(energy_dollars_per_kwh(Decimal("12"), unit), want, unit)
        for unit in ("percent", "$/month", "$/kW", "dimensionless", "kWh"):
            with self.assertRaises(ValueError, msg=unit):
                energy_dollars_per_kwh(Decimal("12"), unit)

    def test_normalize_cell_label(self):
        self.assertEqual(normalize_cell_label(" On-Peak "), "on_peak")
        self.assertEqual(normalize_cell_label("mid–peak"), "mid_peak")
        self.assertEqual(normalize_cell_label(None), "all")
        self.assertEqual(normalize_cell_label(""), "all")


class TestPlausibilityGate(unittest.TestCase):
    def _plan(self, unit: str, amount: str, recipe: str = "bundled") -> PlanInput:
        kind = "base_energy" if recipe == "bundled" else "delivery_per_kwh"
        return PlanInput(
            plan_key="p", name="p", recipe_code=recipe,
            components=[_c("e", kind, unit, {"amount": amount},
                           charge_category="delivery")],
        )

    def _reasons(self, plan: PlanInput) -> list[str]:
        return [f.reason for f in gate_structure(plan, compile_plan(plan))]

    def test_cents_read_as_dollars_holds(self):
        self.assertIn("implausible_price", self._reasons(self._plan("$/kWh", "12.4")))

    def test_dollars_read_as_cents_holds(self):
        self.assertIn("implausible_price", self._reasons(self._plan("¢/kWh", "0.124")))

    def test_normal_and_cpp_prices_pass(self):
        self.assertEqual(self._reasons(self._plan("¢/kWh", "12.4")), [])
        self.assertEqual(self._reasons(self._plan("¢/kWh", "182.871")), [])

    def test_texas_delivery_floor_is_zero(self):
        self.assertEqual(
            self._reasons(self._plan("$/kWh", "0.004", recipe="texas_tdu")), []
        )


if __name__ == "__main__":
    unittest.main()
