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


# OEB RPP: stored $/kWh, page prints ¢/kWh (R28 Ontario holds).
OEB_DOC = (
    "Electricity rates\n"
    "Time-of-use prices (¢/kWh)\n"
    "Off-peak     9.8\n"
    "Mid-peak    15.7\n"
    "On-peak     20.3\n"
)

# PPL default supply (GSC) in a tariff table.
PPL_DOC = (
    "Rate Schedule RS\n"
    "Charge                          ¢/kWh\n"
    "Distribution Charge             6.175\n"
    "Generation Supply Charge (GSC)  9.753\n"
    "Transmission Service Charge     3.326\n"
)

# Tier + season with merged-style headers (GA / FPL / BCH shapes).
TIER_DOC = (
    "Schedule R-31 Residential\n"
    "Summer (June through September)\n"
    "               Energy Charge (¢/kWh)\n"
    "First 650 kWh           8.7738\n"
    "Over 1000 kWh          15.0828\n"
    "Winter\n"
    "All kWh                 8.2116\n"
)

BCH_DOC = (
    "Residential Inclining Block Rate\n"
    "Step 1 Energy Charge (¢/kWh)   11.87\n"
    "Step 2 Energy Charge (¢/kWh)   14.08\n"
)

NLH_DOC = (
    "Rate #1.1S\n"
    "Non-Winter (May–November)\n"
    "Energy Charge 15.587 ¢/kWh\n"
    "Winter (December–April)\n"
    "Energy Charge 15.587 ¢/kWh\n"
)


class TestOntarioAndPplGrounding(unittest.TestCase):
    def test_oeb_rpp_cents_for_dollar_amount(self):
        """Stored 0.098 $/kWh; quote cites printed 9.8 ¢."""
        r = verify_quote(
            OEB_DOC,
            "9.8",
            unit="$/kWh",
            amount="0.098",
            period="off_peak",
            require_row_col=True,
        )
        self.assertTrue(r.ok, r.reason)

    def test_oeb_on_peak_row(self):
        r = verify_quote(
            OEB_DOC,
            "20.3",
            unit="$/kWh",
            amount="0.203",
            period="on_peak",
            require_row_col=True,
        )
        self.assertTrue(r.ok, r.reason)

    def test_ppl_gsc_amount(self):
        r = verify_quote(
            PPL_DOC,
            "9.753",
            unit="¢/kWh",
            amount="9.753",
            component_name="Generation Supply Charge",
            require_row_col=True,
        )
        self.assertTrue(r.ok, r.reason)

    def test_sdge_still_rejects_bare_100x(self):
        """¢ context + dollar unit for 0.52 vs 52 must still fail."""
        r = verify_quote(
            "Total rate 0.52 ¢/kWh\n",
            "0.52",
            unit="¢/kWh",
            amount="52",
        )
        self.assertFalse(r.ok)
        self.assertEqual(r.reason, "amount_not_in_quote")


class TestTierSeasonSynonyms(unittest.TestCase):
    def test_first_650_matches_tier_0_650(self):
        r = verify_quote(
            TIER_DOC,
            "8.7738",
            unit="¢/kWh",
            amount="8.7738",
            season="summer",
            tier="0-650",
            require_row_col=True,
        )
        self.assertTrue(r.ok, r.reason)

    def test_over_1000_matches_tier_plus(self):
        r = verify_quote(
            TIER_DOC,
            "15.0828",
            unit="¢/kWh",
            amount="15.0828",
            season="summer",
            tier="1000+",
            require_row_col=True,
        )
        self.assertTrue(r.ok, r.reason)

    def test_bch_step_labels(self):
        r = verify_quote(
            BCH_DOC,
            "11.87",
            unit="¢/kWh",
            amount="11.87",
            tier="step1",
            require_row_col=True,
        )
        self.assertTrue(r.ok, r.reason)

    def test_nlh_non_winter_season(self):
        r = verify_quote(
            NLH_DOC,
            "15.587 ¢/kWh",
            unit="¢/kWh",
            amount="15.587",
            season="non_winter",
            require_row_col=True,
        )
        self.assertTrue(r.ok, r.reason)

    def test_en_dash_normalized_in_quote(self):
        doc = "Off–Peak 12.042 ¢/kWh\n"
        r = verify_quote(doc, "Off-Peak 12.042", unit="¢/kWh", amount="12.042")
        self.assertTrue(r.ok, r.reason)


if __name__ == "__main__":
    unittest.main()


class TestNonEnergyUnits(unittest.TestCase):
    def test_monthly_fixed_charge_unit(self):
        doc = (
            "Domestic Service\n"
            "Customer Charge $20.08 per month\n"
            "Energy Charge 18.324 ¢/kWh\n"
        )
        r = verify_quote(
            doc,
            "Customer Charge $20.08 per month",
            unit="$/month",
            amount="20.08",
        )
        self.assertTrue(r.ok, r.reason)

    def test_kwh_tier_breakpoint_unit(self):
        doc = (
            "Inclining Block\n"
            "Tier threshold: First 1000 kWh\n"
            "Energy Charge 12.258 ¢/kWh\n"
        )
        r = verify_quote(
            doc,
            "First 1000 kWh",
            unit="kWh",
            amount="1000",
        )
        self.assertTrue(r.ok, r.reason)

    def test_unknown_unit_still_rejected(self):
        r = verify_quote("foo 1.0 bar\n", "1.0", unit="widgets")
        self.assertFalse(r.ok)
        self.assertTrue(r.reason.startswith("unsupported_unit"), r.reason)
