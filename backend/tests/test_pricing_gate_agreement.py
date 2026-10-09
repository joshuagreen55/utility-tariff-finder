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

    def test_label_spelling_agrees(self):
        a = [_energy(period="On-Peak")]
        b = [_energy(period="on_peak")]
        self.assertEqual(gate_agreement(a, b), [])

    def test_credit_sign_agrees(self):
        def credit(amount):
            return ComponentInput(
                code="cr", kind="credit", unit="¢/kWh", cells=[{"amount": amount}],
            )
        a = [_energy(), credit("0.5")]
        b = [_energy(), credit("-0.5")]
        self.assertEqual(gate_agreement(a, b, recipe_code="bundled"), [])


def _ontario(lf="1.0358", net_loss_sensitive=True, rider=True):
    comps = [
        ComponentInput(code="rpp", kind="regulated_commodity", unit="¢/kWh",
                       cells=[{"amount": "10"}]),
        ComponentInput(code="dist", kind="delivery_per_kwh", unit="¢/kWh",
                       cells=[{"amount": "2"}]),
        ComponentInput(code="net", kind="delivery_per_kwh", unit="¢/kWh",
                       cells=[{"amount": "1"}],
                       loss_sensitive=net_loss_sensitive),
        ComponentInput(code="lf", kind="multiplier", unit="dimensionless",
                       cells=[{"amount": lf}]),
    ]
    if rider:
        comps.append(ComponentInput(
            code="lrc", kind="rider_per_kwh", unit="¢/kWh",
            cells=[{"amount": "0.1"}],
        ))
    return comps


class TestGateAgreementPriced(unittest.TestCase):
    def test_loss_factor_disagreement_holds(self):
        fails = gate_agreement(_ontario("1.0358"), _ontario("1.0538"))
        self.assertEqual([f.reason for f in fails], ["extractors_disagree"])

    def test_loss_sensitive_disagreement_holds_when_compiled(self):
        a, b = _ontario(), _ontario(net_loss_sensitive=False)
        self.assertEqual(gate_agreement(a, b), [])
        fails = gate_agreement(a, b, recipe_code="provincial_ontario")
        self.assertEqual([f.reason for f in fails], ["compiled_disagree"])

    def test_applying_set_disagreement_holds(self):
        # Model B called the rider optional, so it is not in B's applying set.
        fails = gate_agreement(_ontario(), _ontario(rider=False))
        self.assertEqual([f.reason for f in fails], ["extractors_disagree"])

    def test_unknown_priced_kind_compared(self):
        def odd(amount):
            return ComponentInput(code="x", kind="storm_recovery", unit="¢/kWh",
                                  cells=[{"amount": amount}])
        fails = gate_agreement([_energy(), odd("0.3")], [_energy(), odd("0.4")])
        self.assertEqual([f.reason for f in fails], ["extractors_disagree"])

    def test_unparseable_amount_not_skipped(self):
        a = [_energy(), _energy(code="fuel", amount="2.0")]
        b = [_energy(), _energy(code="fuel", amount="n/a")]
        self.assertEqual(len(gate_agreement(a, b)), 1)

    def _percent(self, bases):
        return [
            _energy(),
            ComponentInput(code="ecr", kind="rider_percent", unit="percent",
                           cells=[{"amount": "5"}], percent_base_codes=bases),
        ]

    def test_b_not_compilable_holds(self):
        fails = gate_agreement(
            self._percent(["base"]), self._percent([]), recipe_code="bundled",
        )
        self.assertEqual([f.reason for f in fails], ["extract_b_not_compilable"])

    def test_a_not_compilable_left_to_g4(self):
        fails = gate_agreement(
            self._percent([]), self._percent(["base"]), recipe_code="bundled",
        )
        self.assertEqual(fails, [])


if __name__ == "__main__":
    unittest.main()
