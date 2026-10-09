"""PR R29-1: robust amount parsing for messy model extracts."""
from __future__ import annotations

import unittest
from decimal import Decimal

from app.services.pricing.amount_parse import (
    normalize_amount_string,
    parse_amount,
    sanitize_extract_amounts,
)
from app.services.pricing.extract_schema import validate_extract_schema


class TestParseAmount(unittest.TestCase):
    def test_currency_and_commas(self):
        self.assertEqual(parse_amount("$0.10815"), Decimal("0.10815"))
        self.assertEqual(parse_amount("1,234.56"), Decimal("1234.56"))
        self.assertEqual(parse_amount("$1,234.50"), Decimal("1234.50"))

    def test_accounting_negative(self):
        self.assertEqual(parse_amount("(1.297)"), Decimal("-1.297"))

    def test_blank_and_na(self):
        self.assertIsNone(parse_amount(""))
        self.assertIsNone(parse_amount("—"))
        self.assertIsNone(parse_amount("n/a"))
        self.assertIsNone(parse_amount(None))

    def test_range_is_missing(self):
        self.assertIsNone(parse_amount("1.2–1.5"))
        self.assertIsNone(parse_amount("1.2-1.5"))

    def test_per_kwh_suffix(self):
        self.assertEqual(parse_amount("12.5 per kWh"), Decimal("12.5"))
        self.assertEqual(parse_amount("0.12/kWh"), Decimal("0.12"))

    def test_normalize_string(self):
        self.assertEqual(normalize_amount_string("$0.12"), "0.12")
        self.assertIsNone(normalize_amount_string("n/a"))


class TestSanitizeExtract(unittest.TestCase):
    def test_dollar_amount_becomes_numeric(self):
        raw = [{
            "code": "base", "kind": "base_energy", "unit": "¢/kWh",
            "name": "Energy", "disposition": "applies",
            "cells": [{"amount": "$0.10815"}],
            "source_quote": "0.10815", "source_page": "p1",
        }]
        out = sanitize_extract_amounts(raw)
        self.assertEqual(out[0]["cells"][0]["amount"], "0.10815")
        self.assertIsNone(validate_extract_schema(out))

    def test_blank_applies_becomes_not_found(self):
        raw = [{
            "code": "cpp", "kind": "rider_per_kwh", "unit": "¢/kWh",
            "name": "CPP", "disposition": "applies",
            "cells": [{"amount": "n/a"}],
            "source_quote": "n/a", "source_page": "p1",
        }, {
            "code": "base", "kind": "base_energy", "unit": "¢/kWh",
            "name": "Energy", "disposition": "applies",
            "cells": [{"amount": "18.324"}],
            "source_quote": "18.324", "source_page": "p1",
        }]
        out = sanitize_extract_amounts(raw)
        by = {c["code"]: c for c in out}
        self.assertEqual(by["cpp"]["disposition"], "not_found")
        self.assertEqual(by["cpp"]["cells"], [])
        self.assertEqual(by["base"]["disposition"], "applies")
        self.assertIsNone(validate_extract_schema(out))

    def test_disposition_commentary_clipped(self):
        raw = [{
            "code": "rpp", "kind": "base_energy", "unit": "$/kWh",
            "name": "RPP", "disposition": "applies; document prints tou prices",
            "cells": [{"amount": "0.098"}],
            "source_quote": "9.8", "source_page": "p1",
        }]
        out = sanitize_extract_amounts(raw)
        self.assertEqual(out[0]["disposition"], "applies")
        self.assertIsNone(validate_extract_schema(out))

    def test_quote_dollar_100x_reconcile(self):
        """Amount 0.1813 ¢ with quote $0.001813 → $0.001813 /kWh."""
        raw = [{
            "code": "cip", "kind": "rider_per_kwh", "unit": "¢/kWh",
            "name": "CIP", "disposition": "applies",
            "cells": [{"amount": "0.1813"}],
            "source_quote": "All Classes $0.001813 per kWh R",
            "source_page": "p1",
        }]
        out = sanitize_extract_amounts(raw)
        self.assertEqual(out[0]["cells"][0]["amount"], "0.001813")
        self.assertEqual(out[0]["unit"], "$/kWh")

    def test_pec_cents_matching_quote_dollar_keeps_value(self):
        """4.3481 ¢ with quote $0.043481 is the same rate (100×)."""
        raw = [{
            "code": "base", "kind": "base_energy", "unit": "¢/kWh",
            "name": "Energy", "disposition": "applies",
            "cells": [{"amount": "4.3481"}],
            "source_quote": "Off-Peak ... $0.043481",
            "source_page": "p1",
        }]
        out = sanitize_extract_amounts(raw)
        self.assertEqual(out[0]["unit"], "$/kWh")
        self.assertEqual(out[0]["cells"][0]["amount"], "0.043481")


def _row(unit, amounts, quote):
    return [{
        "code": "base", "kind": "base_energy", "unit": unit, "name": "Energy",
        "disposition": "applies", "cells": [{"amount": a} for a in amounts],
        "source_quote": quote, "source_page": "p1",
    }]


class TestDecimalComma(unittest.TestCase):
    def test_decimal_comma(self):
        self.assertEqual(parse_amount("9,8"), Decimal("9.8"))
        self.assertEqual(parse_amount("0,704"), Decimal("0.704"))
        self.assertEqual(parse_amount("12,5¢"), Decimal("12.5"))
        self.assertEqual(parse_amount("1.234,56"), Decimal("1234.56"))

    def test_thousands_groups(self):
        self.assertEqual(parse_amount("1,000"), Decimal("1000"))
        self.assertEqual(parse_amount("1,234,567"), Decimal("1234567"))

    def test_lone_group_is_decimal_for_per_kwh(self):
        self.assertEqual(parse_amount("6,704", per_kwh=True), Decimal("6.704"))
        out = sanitize_extract_amounts(_row("¢/kWh", ["6,704"], "6,704 ¢/kWh"))
        self.assertEqual(out[0]["cells"][0]["amount"], "6.704")


class TestReconcileComponentLevel(unittest.TestCase):
    def test_one_factor_for_every_cell(self):
        """Quote cites one cell in $; every cell is rescaled the same way."""
        out = sanitize_extract_amounts(_row(
            "¢/kWh", ["10.815", "9.241"],
            "Energy Charge per kWh June - September $0.10815",
        ))
        self.assertEqual(out[0]["unit"], "$/kWh")
        self.assertEqual(
            [c["amount"] for c in out[0]["cells"]], ["0.10815", "0.09241"],
        )

    def test_conflicting_factors_left_alone(self):
        out = sanitize_extract_amounts(_row(
            "¢/kWh", ["0.10815", "9.241"],
            "Summer $0.10815 Other months $0.09241",
        ))
        self.assertEqual(out[0]["unit"], "¢/kWh")
        self.assertEqual(
            [c["amount"] for c in out[0]["cells"]], ["0.10815", "9.241"],
        )

    def test_fixed_charge_dollar_figure_ignored(self):
        """$14.50 customer charge must not rescale a 0.145 ¢ rider."""
        out = sanitize_extract_amounts(_row(
            "¢/kWh", ["0.145"],
            "Base Charge $14.50 per customer; rider 0.145¢ per kWh",
        ))
        self.assertEqual(out[0]["unit"], "¢/kWh")
        self.assertEqual(out[0]["cells"][0]["amount"], "0.145")

    def test_small_cent_rider_not_promoted_to_dollars(self):
        out = sanitize_extract_amounts(_row(
            "¢/kWh", ["0.9"], "Rider 0.9¢ per kWh, minimum $1.50",
        ))
        self.assertEqual(out[0]["unit"], "¢/kWh")

    def test_ambiguous_per_kwh_resolved_from_quote(self):
        cents = sanitize_extract_amounts(_row("per kWh", ["9.8"], "9.8¢ per kWh"))
        self.assertEqual(cents[0]["unit"], "¢/kWh")
        dollars = sanitize_extract_amounts(
            _row("per kWh", ["0.098"], "$0.098 per kWh"),
        )
        self.assertEqual(dollars[0]["unit"], "$/kWh")
        unknown = sanitize_extract_amounts(_row("per kWh", ["9.8"], "9.8 per kWh"))
        self.assertEqual(unknown[0]["unit"], "per kWh")


if __name__ == "__main__":
    unittest.main()
