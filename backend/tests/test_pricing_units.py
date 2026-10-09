"""PR R29-2: unit normalization table."""
from __future__ import annotations

import unittest
from decimal import Decimal

from app.services.pricing.units import normalize_unit, to_monthly, unit_pattern
from app.services.pricing.quote_verifier import verify_quote


class TestNormalizeUnit(unittest.TestCase):
    def test_fixed_charge_aliases(self):
        for raw, fam in [
            ("$/month", "$/month"),
            ("USD per month", "$/month"),
            ("per billing period", "$/month"),
            ("meter-month", "$/month"),
            ("per_day", "$/day"),
            ("$/year", "$/year"),
            ("annually", "$/year"),
            ("$/kW/month", "$/kw/month"),
            ("per_kwh", "cents/kwh"),
            ("¢/kWh", "cents/kwh"),
        ]:
            self.assertEqual(normalize_unit(raw), fam, raw)

    def test_daily_yearly_to_monthly(self):
        self.assertEqual(
            to_monthly(Decimal("1"), "$/day"),
            Decimal("1") * Decimal("30.4167"),
        )
        self.assertEqual(to_monthly(Decimal("120"), "$/year"), Decimal("10"))

    def test_quote_accepts_billing_period_unit(self):
        doc = "Basic Charge $20.08 per billing period\n"
        r = verify_quote(
            doc, "Basic Charge $20.08 per billing period",
            unit="per billing period", amount="20.08",
        )
        self.assertTrue(r.ok, r.reason)

    def test_energy_charge_per_kwh_before_dollar(self):
        """Xcel-style: unit words precede the $ amount."""
        doc = (
            "Energy Charge per kWh June - September "
            "$0.10815 $0.10815 R Other Months $0.09241 $0.06287 R\n"
        )
        r = verify_quote(
            doc,
            "Energy Charge per kWh June - September $0.10815 $0.10815 R "
            "Other Months $0.09241 $0.06287 R",
            unit="$/kWh",
            amount="0.10815",
        )
        self.assertTrue(r.ok, r.reason)
        self.assertIsNotNone(unit_pattern("$/kWh"))
        self.assertTrue(unit_pattern("$/kWh").search(doc))


if __name__ == "__main__":
    unittest.main()
