"""PR C: pre-accept gates G0–G6."""
from __future__ import annotations

import unittest
from decimal import Decimal

from app.services.pricing.preaccept import (
    gate_admissibility,
    gate_agreement,
    gate_edition,
    gate_oracle,
    run_preaccept,
)
from app.services.pricing.rider_census import DispositionInput, InventoryRider
from app.services.pricing.types import ComponentInput, PlanInput


DOC = (
    "Schedule RS\n"
    "Energy Charge 10.000 ¢ per kWh.\n"
    "Fuel rider 1.000 ¢/kWh applies to all residential.\n"
    "Effective for service on and after January 1, 2026.\n"
)


def _base(**kw):
    return ComponentInput(
        code="base", kind="base_energy", unit="¢/kWh",
        cells=[{"amount": "10.000"}],
        source_page="p.1", source_quote="Energy Charge 10.000 ¢ per kWh",
        **kw,
    )


def _fuel(**kw):
    return ComponentInput(
        code="fuel", kind="rider_per_kwh", unit="¢/kWh",
        cells=[{"amount": "1.000"}],
        source_page="p.2", source_quote="Fuel rider 1.000 ¢/kWh",
        **kw,
    )


class TestGates(unittest.TestCase):
    def test_g0_blocks_aggregator(self):
        fails = gate_admissibility(
            source_url="https://www.utilityrate.com/rates",
            official_hosts=["nspower.ca"],
        )
        # utilityrate.com may or may not be in THIRD_PARTY; domain_not_allowlisted
        self.assertTrue(fails)
        self.assertEqual(fails[0].gate, "G0")

    def test_g0_allowlisted_ok(self):
        fails = gate_admissibility(
            source_url="https://www.nspower.ca/rates.pdf",
            official_hosts=["nspower.ca"],
        )
        self.assertEqual(fails, [])

    def test_g1_rejects_draft(self):
        fails = gate_edition(edition_label="PRO FORMA Supplement")
        self.assertEqual(fails[0].reason, "forbidden_edition_marker")

    def test_g2_agreement(self):
        a = [_base(), _fuel()]
        b = [_base(), _fuel()]
        self.assertEqual(gate_agreement(a, b), [])
        b2 = [_base(), ComponentInput(
            code="fuel", kind="rider_per_kwh", unit="¢/kWh",
            cells=[{"amount": "1.500"}],
            source_page="p.2", source_quote="Fuel rider 1.000 ¢/kWh",
        )]
        fails = gate_agreement(a, b2)
        self.assertEqual(fails[0].reason, "extractors_disagree")

    def test_full_preaccept_pass(self):
        comps = [_base(), _fuel()]
        plan = PlanInput(
            plan_key="demo-rs", name="RS", recipe_code="bundled",
            components=comps,
        )
        inventory = [
            InventoryRider("fuel", "Fuel rider"),
        ]
        disps = [
            DispositionInput(
                "fuel", "applies", "p.2", "Fuel rider 1.000 ¢/kWh applies to all residential"
            ),
        ]
        result = run_preaccept(
            plan,
            document_text=DOC,
            source_url="https://www.example-utility.com/tariff.pdf",
            official_hosts=["example-utility.com"],
            extract_a=comps,
            extract_b=list(comps),
            inventory=inventory,
            dispositions=disps,
            edition_label="Original Sheet Effective January 1, 2026",
            typical_bill_cents_per_kwh=Decimal("11.000"),
        )
        self.assertTrue(result.accepted, [f.reason for f in result.failures])
        self.assertEqual(result.compiled.cents_sorted(places=3), [Decimal("11.000")])

    def test_oracle_mismatch_holds(self):
        comps = [_base(), _fuel()]
        plan = PlanInput(
            plan_key="demo-rs", name="RS", recipe_code="bundled",
            components=comps,
        )
        result = run_preaccept(
            plan,
            document_text=DOC,
            source_url="https://www.example-utility.com/tariff.pdf",
            official_hosts=["example-utility.com"],
            extract_a=comps,
            extract_b=list(comps),
            inventory=[],
            dispositions=[],
            typical_bill_cents_per_kwh=Decimal("15.000"),
        )
        self.assertFalse(result.accepted)
        self.assertTrue(any(f.gate == "G6" for f in result.failures))


if __name__ == "__main__":
    unittest.main()
