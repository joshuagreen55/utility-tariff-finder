"""PR R29-4: metadata-only dual-extract disagreement must not hold."""
from __future__ import annotations

import unittest

from app.services.pricing.preaccept import gate_agreement
from app.services.pricing.types import ComponentInput


def _energy(code="base", amount="10.0", unit="¢/kWh", **cell):
    return ComponentInput(
        code=code, kind="base_energy", unit=unit, name=code,
        cells=[{"amount": amount, "season": "all", "period": "all",
                "day_type": "all", "tier": "all", **cell}],
    )


class TestGateAgreementMetadata(unittest.TestCase):
    def test_priced_agree_metadata_differ_passes(self):
        a = [
            _energy(),
            ComponentInput(
                code="tou", kind="tou_schedule", unit="dimensionless",
                name="TOU", cells=[{"amount": "0", "period": "on_peak",
                                    "start": "07:00", "end": "19:00"}],
            ),
        ]
        b = [
            _energy(),
            ComponentInput(
                code="tou", kind="tou_schedule", unit="dimensionless",
                name="TOU", cells=[{"amount": "0", "period": "on_peak",
                                    "start": "08:00", "end": "20:00"}],
            ),
            ComponentInput(
                code="cust", kind="fixed_monthly", unit="$/month",
                name="Customer", cells=[{"amount": "20.08"}],
            ),
        ]
        self.assertEqual(gate_agreement(a, b), [])

    def test_priced_disagree_holds(self):
        a = [_energy(amount="10.0")]
        b = [_energy(amount="11.0")]
        fails = gate_agreement(a, b)
        self.assertEqual(len(fails), 1)
        self.assertEqual(fails[0].gate, "G2")

    def test_cents_vs_dollars_agree(self):
        a = [_energy(amount="10.0", unit="¢/kWh")]
        b = [_energy(amount="0.10", unit="$/kWh")]
        self.assertEqual(gate_agreement(a, b), [])


if __name__ == "__main__":
    unittest.main()
