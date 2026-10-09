"""Joshua 2026-10-09 policy: Texas choose-retailer + OER bill-level note."""
from __future__ import annotations

import unittest
from decimal import Decimal

from app.services.pricing.compiler import compile_plan, load_golden_plans, plan_from_dict
from app.services.pricing.policy import (
    OER_BILL_LEVEL_NOTE,
    SUPPLY_STATUS_CHOOSE_RETAILER,
    TEXAS_TDU_RECIPE,
)
from app.services.pricing.recipes import RecipeError, recipe_texas_tdu
from app.services.pricing.types import ComponentInput, PlanInput


class TestTexasTdu(unittest.TestCase):
    def test_golden_delivery_only_no_all_in(self):
        plans = {p.plan_key: p for p in load_golden_plans()}
        plan = plans["oncor-tdu-residential"]
        self.assertEqual(plan.recipe_code, TEXAS_TDU_RECIPE)
        compiled = compile_plan(plan)
        self.assertFalse(compiled.has_all_in)
        self.assertEqual(compiled.supply_status, SUPPLY_STATUS_CHOOSE_RETAILER)
        self.assertEqual(compiled.cents_sorted(places=4), [Decimal("5.3579")])
        self.assertEqual(plan.official_cents, [])

    def test_rejects_invented_supply_price(self):
        comps = [
            ComponentInput(
                code="tdsp", kind="delivery_per_kwh", unit="¢/kWh",
                cells=[{"amount": "4.0"}], charge_category="delivery",
            ),
            ComponentInput(
                code="fake_polr", kind="default_supply", unit="¢/kWh",
                cells=[{"amount": "10.0"}],
            ),
        ]
        with self.assertRaises(RecipeError) as ctx:
            recipe_texas_tdu(comps)
        self.assertIn("choose_a_retailer", str(ctx.exception))


class TestOntarioOer(unittest.TestCase):
    def test_ontario_goldens_get_oer_bill_note(self):
        plans = [p for p in load_golden_plans() if p.recipe_code == "provincial_ontario"]
        self.assertGreaterEqual(len(plans), 3)
        for plan in plans:
            compiled = compile_plan(plan)
            self.assertIn(OER_BILL_LEVEL_NOTE, compiled.bill_level_notes)
            self.assertTrue(compiled.has_all_in)

    def test_oer_component_rejected_from_per_kwh(self):
        plan = PlanInput(
            plan_key="bad-oer",
            name="TOU with OER folded",
            recipe_code="provincial_ontario",
            components=[
                ComponentInput(
                    code="rpp", kind="regulated_commodity", unit="$/kWh",
                    cells=[{"amount": "0.098", "period": "off_peak"}],
                ),
                ComponentInput(
                    code="dc", kind="delivery_per_kwh", unit="$/kWh",
                    cells=[{"amount": "0.001"}], loss_sensitive=False,
                ),
                ComponentInput(
                    code="lf", kind="multiplier", unit="dimensionless",
                    cells=[{"amount": "1.03"}],
                ),
                ComponentInput(
                    code="oer", kind="credit", unit="percent",
                    name="Ontario Electricity Rebate",
                    cells=[{"amount": "-13.1"}],
                ),
            ],
        )
        with self.assertRaises(RecipeError) as ctx:
            compile_plan(plan)
        self.assertIn("bill-level note", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
