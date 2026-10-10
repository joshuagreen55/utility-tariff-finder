"""G3: verbatim quote + unit (incl. table headers) + row/col grounding."""
from __future__ import annotations

import unittest

from app.services.pricing.quote_verifier import (
    is_decimal_comma_document,
    verify_component_quote,
    verify_quote,
)

HQ_FR_DOC = (
    "Tarif D\n"
    "Redevance d'abonnement : 46,154 ¢ par jour\n"
    "Prix de l'énergie :\n"
    "6,905 ¢ le kWh pour les 40 premiers kilowattheures par jour\n"
    "10,652 ¢ le kWh pour le reste de l'énergie consommée\n"
)

EN_DOC = (
    "Residential Service\n"
    "First 1,000 kWh per month 9.120 ¢/kWh\n"
    "Over 1,000 kWh per month 11.430 ¢/kWh\n"
    "Customer Charge $6,605 annual minimum\n"
)


class TestDecimalComma(unittest.TestCase):
    def test_document_locale_detection(self):
        self.assertTrue(is_decimal_comma_document(HQ_FR_DOC))
        self.assertFalse(is_decimal_comma_document(EN_DOC))
        self.assertTrue(is_decimal_comma_document("Énergie 9,8 ¢/kWh"))
        self.assertFalse(is_decimal_comma_document(""))

    def test_french_three_decimal_figure_grounds(self):
        r = verify_quote(
            HQ_FR_DOC, "6,905 ¢ le kWh pour les 40 premiers kilowattheures",
            unit="¢/kWh", amount="6.905",
        )
        self.assertTrue(r.ok, r.reason)
        r = verify_quote(
            HQ_FR_DOC, "10,652 ¢ le kWh pour le reste", unit="¢/kWh", amount="10.652",
        )
        self.assertTrue(r.ok, r.reason)

    def test_french_wrong_amount_still_fails(self):
        r = verify_quote(
            HQ_FR_DOC, "6,905 ¢ le kWh pour les 40 premiers kilowattheures",
            unit="¢/kWh", amount="6905",
        )
        self.assertFalse(r.ok)

    def test_quote_with_own_figure_does_not_borrow_neighbour_row(self):
        r = verify_quote(
            HQ_FR_DOC, "10,652 ¢ le kWh pour le reste", unit="¢/kWh", amount="6.905",
        )
        self.assertFalse(r.ok)
        self.assertEqual(r.reason, "amount_not_in_quote")
        r = verify_quote(
            EN_DOC, "Over 1,000 kWh per month 11.430 ¢/kWh",
            unit="¢/kWh", amount="9.120",
        )
        self.assertFalse(r.ok)
        self.assertEqual(r.reason, "amount_not_in_quote")

    def test_label_only_quote_still_reads_reflowed_figure(self):
        doc = "Residential Service\nEnergy charge\n9.120 ¢/kWh\n"
        r = verify_quote(doc, "Energy charge", unit="¢/kWh", amount="9.120")
        self.assertTrue(r.ok, r.reason)

    def test_short_comma_decimal_in_any_document(self):
        doc = "Residential\nEnergy 9,8 ¢/kWh\nDelivery 3.100 ¢/kWh\nFuel 0.400 ¢/kWh\n"
        self.assertFalse(is_decimal_comma_document(doc))
        r = verify_quote(doc, "Energy 9,8 ¢/kWh", unit="¢/kWh", amount="9.8")
        self.assertTrue(r.ok, r.reason)

    def test_french_dollar_figure_with_sibling_cents(self):
        doc = "Prix de l'énergie : 0,06905 $ le kWh\nRedevance 0,46154 $ par jour\n"
        r = verify_quote(doc, "0,06905 $ le kWh", unit="$/kWh", amount="0.06905")
        self.assertTrue(r.ok, r.reason)

    def test_english_grouped_figures_are_not_decimals(self):
        r = verify_quote(
            EN_DOC, "Customer Charge $6,605 annual minimum", unit="$/month",
            require_unit=False, amount="6.605",
        )
        self.assertFalse(r.ok)
        self.assertEqual(r.reason, "amount_not_in_quote")
        r = verify_quote(
            EN_DOC, "Over 1,000 kWh per month 11.430 ¢/kWh", unit="¢/kWh", amount="1.000",
        )
        self.assertFalse(r.ok)
        r = verify_quote(
            EN_DOC, "Over 1,000 kWh per month 11.430 ¢/kWh", unit="¢/kWh", amount="11.430",
        )
        self.assertTrue(r.ok, r.reason)


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
        )
        self.assertTrue(r.ok, r.reason)

    def test_season_section_heading(self):
        r = verify_quote(
            TABLE_DOC,
            "26.395",
            unit="¢/kWh",
            amount="26.395",
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
        )
        self.assertTrue(r.ok, r.reason)

    def test_oeb_on_peak_row(self):
        r = verify_quote(
            OEB_DOC,
            "20.3",
            unit="$/kWh",
            amount="0.203",
        )
        self.assertTrue(r.ok, r.reason)

    def test_ppl_gsc_amount(self):
        r = verify_quote(
            PPL_DOC,
            "9.753",
            unit="¢/kWh",
            amount="9.753",
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


class TestTierSeasonSynonyms(unittest.TestCase):
    def test_first_650_matches_tier_0_650(self):
        r = verify_quote(
            TIER_DOC,
            "8.7738",
            unit="¢/kWh",
            amount="8.7738",
        )
        self.assertTrue(r.ok, r.reason)

    def test_over_1000_matches_tier_plus(self):
        r = verify_quote(
            TIER_DOC,
            "15.0828",
            unit="¢/kWh",
            amount="15.0828",
        )
        self.assertTrue(r.ok, r.reason)

    def test_bch_step_labels(self):
        r = verify_quote(
            BCH_DOC,
            "11.87",
            unit="¢/kWh",
            amount="11.87",
        )
        self.assertTrue(r.ok, r.reason)

    def test_nlh_non_winter_season(self):
        r = verify_quote(
            NLH_DOC,
            "15.587 ¢/kWh",
            unit="¢/kWh",
            amount="15.587",
        )
        self.assertTrue(r.ok, r.reason)

    def test_en_dash_normalized_in_quote(self):
        doc = "Off–Peak 12.042 ¢/kWh\n"
        r = verify_quote(doc, "Off-Peak 12.042", unit="¢/kWh", amount="12.042")
        self.assertTrue(r.ok, r.reason)


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


TOU_LINE = "Prices (¢/kWh): Off-peak 9.8; Mid-peak 15.7; On-peak 20.3\n"


class TestAmountMustBePrinted(unittest.TestCase):
    def test_quote_without_number_fails(self):
        r = verify_quote(DOC, "Energy Charge", unit="¢/kWh", amount="99.1")
        self.assertFalse(r.ok)
        self.assertEqual(r.reason, "amount_not_in_quote")

    def test_unparseable_amount_fails(self):
        r = verify_quote(DOC, "18.324 ¢ per kWh", unit="¢/kWh", amount="n/a")
        self.assertFalse(r.ok)
        self.assertEqual(r.reason, "amount_unparseable")

    def test_sibling_unit_needs_printed_figure(self):
        doc = "Time-of-use prices (¢/kWh)\nOn-peak\n\n\n\n20.3\n"
        r = verify_quote(doc, "On-peak", unit="$/kWh", amount="0.203")
        self.assertFalse(r.ok)


OEB_ULO_DOC = (
    "ULO period Hours Price\n"
    "Ultra-low overnight\nEvery day from 11 p.m. to 7 a.m.\n3.9¢ per kWh\n"
    "Weekend off-peak\nWeekends and statutory holidays from 7 a.m. to 11 p.m.\n"
    "9.8¢ per kWh\n"
)

AL_DOC = (
    "Charge for Energy:\n"
    "BILLING MONTHS JUNE - SEPTEMBER BILLING MONTHS OCTOBER - MAY\n"
    "13.4851¢ per kWh for the first 1000 kWh, 13.4851¢ per kWh for the first 750 kWh,\n"
    "plus plus\n"
    "13.7380¢ per kWh for all over 1000 kWh. 12.2851¢ per kWh for all over 750 kWh.\n"
)


class TestLabelsAreTrusted(unittest.TestCase):
    """Which row a figure is (season / period / tier) is the model's call.

    G3 checks only that the cited figure and unit are printed; G2 catches a
    mislabelled row because the second model prices it differently.
    """

    def test_any_label_on_a_printed_figure_grounds(self):
        doc = "Summer\nOn-Peak 20.100 ¢/kWh\nWinter\nOn-Peak 15.000 ¢/kWh\n"
        self.assertTrue(verify_quote(doc, "On-Peak 15.000", unit="¢/kWh",
                                     amount="15.000").ok)

    def test_figure_must_still_be_printed(self):
        doc = "Summer\nOn-Peak 20.100 ¢/kWh\n"
        r = verify_quote(doc, "On-Peak 20.100", unit="¢/kWh", amount="15.000")
        self.assertFalse(r.ok)
        self.assertEqual(r.reason, "amount_not_in_quote")

    def test_neighbor_row_figure_not_borrowed(self):
        r = verify_quote(TABLE_DOC, "On-Peak                 20.888",
                         unit="¢/kWh", amount="12.042")
        self.assertFalse(r.ok)


class TestEveryCell(unittest.TestCase):
    def _comp(self, cells, quote):
        from app.services.pricing.types import ComponentInput
        return ComponentInput(code="base", kind="base_energy", unit="¢/kWh",
                              name="Energy Charge", cells=cells,
                              source_quote=quote)

    def test_ungrounded_second_cell_fails(self):
        from app.services.pricing.quote_verifier import verify_component_cells
        c = self._comp(
            [{"amount": "12.042", "period": "off_peak"},
             {"amount": "99.999", "period": "on_peak"}],
            "Off-Peak                12.042",
        )
        r = verify_component_cells(TABLE_DOC, c)
        self.assertEqual(r.reason, "cell1:amount_not_in_quote")

    def test_table_rows_ground_each_cell(self):
        from app.services.pricing.quote_verifier import verify_component_cells
        c = self._comp(
            [{"amount": "12.042", "period": "off_peak"},
             {"amount": "20.888", "period": "on_peak"},
             {"amount": "26.395", "period": "on_peak", "season": "winter"}],
            "Off-Peak                12.042",
        )
        self.assertTrue(verify_component_cells(TABLE_DOC, c).ok)

    def test_swapped_labels_still_ground(self):
        """Both figures are printed; the swap is G2's to catch, not G3's."""
        from app.services.pricing.quote_verifier import verify_component_cells
        c = self._comp(
            [{"amount": "20.888", "period": "off_peak"},
             {"amount": "12.042", "period": "on_peak"}],
            "Off-Peak                12.042",
        )
        self.assertTrue(verify_component_cells(TABLE_DOC, c).ok)


if __name__ == "__main__":
    unittest.main()
