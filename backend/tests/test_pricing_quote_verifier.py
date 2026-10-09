"""G3: verbatim quote + unit (incl. table headers) + row/col grounding."""
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

# Unit only in the column header — the R27 false-hold shape.
TABLE_DOC = (
    "Residential Service\n"
    "Period                  Energy Charge (¢/kWh)\n"
    "Off-Peak                12.042\n"
    "On-Peak                 20.888\n"
    "Winter Season\n"
    "Off-Peak                12.198\n"
    "On-Peak                 26.395\n"
)

# Wrong period row: number 20.888 is On-Peak, not Off-Peak.
WRONG_ROW = TABLE_DOC


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
        # Quote is present but unit token $/kWh is nowhere nearby or in headers.
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


class TestHeaderUnitGrounding(unittest.TestCase):
    def test_unit_from_column_header_accepted(self):
        """R27 false hold: number alone, ¢/kWh only in column header."""
        r = verify_quote(TABLE_DOC, "12.042", unit="¢/kWh")
        self.assertTrue(r.ok, r.reason)

    def test_unit_from_section_still_required(self):
        bare = "Residential Service\nOff-Peak 12.042\nOn-Peak 20.888\n"
        r = verify_quote(bare, "12.042", unit="¢/kWh")
        self.assertFalse(r.ok)
        self.assertEqual(r.reason, "unit_not_in_context")


class TestRowColGrounding(unittest.TestCase):
    def test_correct_period_row_passes(self):
        r = verify_quote(
            TABLE_DOC,
            "12.042",
            unit="¢/kWh",
            amount="12.042",
            period="off_peak",
            component_name="Energy Charge",
            require_row_col=True,
        )
        self.assertTrue(r.ok, r.reason)

    def test_wrong_period_row_fails(self):
        """On-Peak number cited as Off-Peak → hold."""
        r = verify_quote(
            WRONG_ROW,
            "20.888",
            unit="¢/kWh",
            amount="20.888",
            period="off_peak",
            require_row_col=True,
        )
        self.assertFalse(r.ok)
        self.assertTrue(r.reason.startswith("label_not_in_row_col"), r.reason)

    def test_season_section_heading(self):
        r = verify_quote(
            TABLE_DOC,
            "26.395",
            unit="¢/kWh",
            amount="26.395",
            season="winter",
            period="on_peak",
            require_row_col=True,
        )
        self.assertTrue(r.ok, r.reason)

    def test_amount_mismatch_in_quote(self):
        """SDG&E-style: quote shows 0.52 but stored amount is 52."""
        r = verify_quote(
            "Total rate 0.52 $/kWh\n",
            "0.52",
            unit="$/kWh",
            amount="52",
        )
        self.assertFalse(r.ok)
        self.assertEqual(r.reason, "amount_not_in_quote")

    def test_amount_match_passes(self):
        r = verify_quote(
            DOC, "18.324 ¢ per kWh", unit="¢/kWh", amount="18.324"
        )
        self.assertTrue(r.ok)


if __name__ == "__main__":
    unittest.main()
