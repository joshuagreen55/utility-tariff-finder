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


if __name__ == "__main__":
    unittest.main()
