"""PR B: verbatim quote + unit grounding verifier."""
from __future__ import annotations

import unittest

from app.services.pricing.quote_verifier import verify_component_quote, verify_quote


DOC = (
    "DOMESTIC SERVICE\n"
    "Energy Charge 18.324 ¢ per kWh for all consumption.\n"
    "Fuel Adjustment Mechanism (FAM) 0.156 ¢/kWh.\n"
    "DSM Cost Recovery Rider 0.648 cents per kWh.\n"
    "ECCR shall be increased by 13.0205% of their base bill calculations.\n"
)


class TestQuoteVerifier(unittest.TestCase):
    def test_verbatim_hit_with_unit(self):
        r = verify_quote(DOC, "18.324 ¢ per kWh", unit="¢/kWh")
        self.assertTrue(r.ok)
        self.assertEqual(r.reason, "ok")
        self.assertIsNotNone(r.quote_index)

    def test_missing_quote_fails(self):
        r = verify_quote(DOC, "99.999 ¢ per kWh", unit="¢/kWh")
        self.assertFalse(r.ok)
        self.assertEqual(r.reason, "quote_not_found")

    def test_unit_not_in_context_fails(self):
        # Quote is present but unit token $/kWh is not near it.
        r = verify_quote(DOC, "18.324", unit="$/kWh")
        self.assertFalse(r.ok)
        self.assertEqual(r.reason, "unit_not_in_context")

    def test_percent_unit_grounded(self):
        r = verify_quote(
            DOC, "13.0205% of their base bill calculations", unit="percent"
        )
        self.assertTrue(r.ok)

    def test_empty_quote(self):
        r = verify_component_quote(DOC, quote=None, unit="¢/kWh")
        self.assertFalse(r.ok)
        self.assertEqual(r.reason, "missing_quote")

    def test_collapsed_whitespace(self):
        messy = "Energy Charge 18.324   ¢   per   kWh for all."
        r = verify_quote(messy, "18.324 ¢ per kWh", unit="¢/kWh")
        self.assertTrue(r.ok)


if __name__ == "__main__":
    unittest.main()
