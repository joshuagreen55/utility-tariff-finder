"""PR A: deterministic pricing compiler vs hand-verified golden plans.

No LLM, no database. Loads ``tests/fixtures/pricing_golden/plans.json``,
compiles each plan with exact ``Decimal`` math, and asserts the all-in
¢/kWh matches the cited official value (quantized to the citation's
decimal places).

    cd backend && python -m unittest tests.test_pricing_compiler_golden -v
"""
from __future__ import annotations

import unittest
from decimal import Decimal

from app.services.pricing.compiler import compile_plan, load_golden_plans
from app.services.pricing.recipes import RecipeError, recipe_deregulated
from app.services.pricing.types import ComponentInput, PlanInput, money


def _official_places(official: list[Decimal]) -> int:
    """Max fractional digits among the cited official ¢/kWh values."""
    places = 0
    for o in official:
        exp = o.as_tuple().exponent
        if isinstance(exp, int) and exp < 0:
            places = max(places, -exp)
    return places


class TestGoldenExactMatch(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.plans = load_golden_plans()
        cls.by_key = {p.plan_key: p for p in cls.plans}

    def test_golden_count(self):
        self.assertGreaterEqual(len(self.plans), 40)

    def test_every_plan_matches_official(self):
        failures: list[str] = []
        for plan in self.plans:
            compiled = compile_plan(plan)
            places = _official_places(plan.official_cents)
            got = compiled.cents_sorted(places=places)
            want = sorted(plan.official_cents)
            if got != want:
                failures.append(
                    f"{plan.plan_key}: got {got} != official {want} "
                    f"(exact $/kWh={compiled.dollars_sorted()})"
                )
        self.assertEqual(failures, [], "\n".join(failures))

    def test_no_floats_in_fixture_amounts(self):
        import json
        from pathlib import Path

        raw = json.loads(
            (Path(__file__).parent / "fixtures/pricing_golden/plans.json").read_text()
        )
        for plan in raw["plans"]:
            for comp in plan["components"]:
                for cell in comp.get("cells") or []:
                    amt = cell["amount"]
                    self.assertIsInstance(amt, str, f"{plan['plan_key']} amount not str")
                    money(amt)  # rejects float; parses decimal string

    def test_nsp_domestic_exact_dollars(self):
        plan = self.by_key["nsp-domestic"]
        compiled = compile_plan(plan)
        self.assertEqual(compiled.dollars_sorted(), [Decimal("0.19128")])

    def test_georgia_percent_riders(self):
        plan = self.by_key["ga-r31"]
        compiled = compile_plan(plan)
        # Exact unrounded winter cell.
        winter = next(
            c for c in compiled.cells
            if c.key.season == "winter" and c.key.tier == "all"
        )
        pct = Decimal("1") + Decimal("0.130205") + Decimal("0.011969")
        expected = (
            Decimal("8.2116") * pct + Decimal("3.8561")
        ) / Decimal("100")
        self.assertEqual(winter.dollars_per_kwh, expected)

    def test_ppl_deregulated_label_components(self):
        plan = self.by_key["ppl-rs"]
        compiled = compile_plan(plan)
        self.assertEqual(plan.recipe_code, "deregulated")
        self.assertEqual(compiled.cents_sorted(places=3), [Decimal("19.254")])

    def test_ontario_loss_factor(self):
        plan = self.by_key["toronto-tou"]
        compiled = compile_plan(plan)
        self.assertEqual(plan.recipe_code, "provincial_ontario")
        self.assertEqual(
            compiled.cents_sorted(places=3),
            [Decimal("13.102"), Decimal("19.176"), Decimal("23.912")],
        )

    def test_alberta_btar_multiplier(self):
        plan = self.by_key["fortis-rolr"]
        compiled = compile_plan(plan)
        exact = compiled.dollars_sorted()[0] * Decimal("100")
        self.assertEqual(
            exact.quantize(Decimal("0.0001")),
            Decimal("19.7239"),
        )

    def test_event_day_excluded_from_everyday(self):
        plan = self.by_key["nsp-cpp-interim"]
        compiled = compile_plan(plan)
        # 182.871 ¢ event must not appear in everyday all-in.
        self.assertEqual(compiled.cents_sorted(places=3), [Decimal("19.128")])
        self.assertTrue(
            all(c.cents_per_kwh < Decimal("20") for c in compiled.cells)
        )


class TestRecipeGuards(unittest.TestCase):
    def test_deregulated_missing_supply_holds(self):
        comps = [
            ComponentInput(
                code="del", kind="delivery_per_kwh", unit="¢/kWh",
                cells=[{"amount": "6.175"}], charge_category="delivery",
            ),
        ]
        with self.assertRaises(RecipeError) as ctx:
            recipe_deregulated(comps)
        self.assertIn("missing_supply", str(ctx.exception))

    def test_float_rejected(self):
        with self.assertRaises(TypeError):
            money(0.19128)  # type: ignore[arg-type]

    def test_bundled_requires_base(self):
        plan = PlanInput(
            plan_key="x",
            name="x",
            recipe_code="bundled",
            components=[
                ComponentInput(
                    code="r", kind="rider_per_kwh", unit="¢/kWh",
                    cells=[{"amount": "1.0"}],
                ),
            ],
        )
        with self.assertRaises(RecipeError):
            compile_plan(plan)


if __name__ == "__main__":
    unittest.main()
