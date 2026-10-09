"""PR R27-4: extraction schema — numbers only, unique names, dispositions."""
from __future__ import annotations

import unittest

from app.services.pricing.extract_schema import (
    EXTRACTION_TOOL_SCHEMA,
    applying_raw_components,
    validate_extract_schema,
)
from app.services.pricing.extraction import (
    ExtractionHold,
    dual_extract_components,
)


def _ok(**overrides):
    base = {
        "code": "base",
        "kind": "base_energy",
        "unit": "¢/kWh",
        "name": "Energy Charge",
        "disposition": "applies",
        "cells": [{"amount": "10.000"}],
        "source_page": "p.1",
        "source_quote": "10.000 ¢/kWh",
    }
    base.update(overrides)
    return base


class TestValidateExtractSchema(unittest.TestCase):
    def test_ok(self):
        self.assertIsNone(validate_extract_schema([
            _ok(),
            _ok(code="fuel", name="Fuel", cells=[{"amount": "1.000"}]),
        ]))

    def test_non_numeric_amount(self):
        err = validate_extract_schema([
            _ok(cells=[{"amount": "see tariff"}]),
        ])
        self.assertIsNotNone(err)
        self.assertIn("amount_not_numeric", err)

    def test_float_amount_rejected(self):
        err = validate_extract_schema([
            _ok(cells=[{"amount": 10.0}]),
        ])
        self.assertEqual(err, "amount_is_float:base:cell0")

    def test_duplicate_code(self):
        err = validate_extract_schema([
            _ok(),
            _ok(name="Other"),
        ])
        self.assertEqual(err, "duplicate_code:base")

    def test_duplicate_name(self):
        err = validate_extract_schema([
            _ok(),
            _ok(code="base2", name="Energy Charge"),
        ])
        self.assertEqual(err, "duplicate_name:energy charge")

    def test_missing_disposition(self):
        raw = _ok()
        del raw["disposition"]
        err = validate_extract_schema([raw])
        self.assertEqual(err, "missing_disposition:base")

    def test_no_applies(self):
        err = validate_extract_schema([
            _ok(disposition="not_applicable"),
            _ok(code="fuel", name="Fuel", disposition="optional",
                cells=[{"amount": "1.000"}]),
        ])
        self.assertEqual(err, "no_applies_disposition")

    def test_applying_filter(self):
        raw = [
            _ok(),
            _ok(code="opt", name="Opt", disposition="optional",
                cells=[{"amount": "0.100"}]),
        ]
        applying = applying_raw_components(raw)
        self.assertEqual([c["code"] for c in applying], ["base"])

    def test_tool_schema_requires_disposition_and_string_amount(self):
        props = EXTRACTION_TOOL_SCHEMA["input_schema"]["properties"]["components"]
        item = props["items"]
        self.assertIn("disposition", item["required"])
        amt = item["properties"]["cells"]["items"]["properties"]["amount"]
        self.assertEqual(amt["type"], "string")


class TestDualExtractSchemaHold(unittest.TestCase):
    def test_schema_hold_before_quote(self):
        doc = "Energy Charge 10.000 ¢/kWh\n"

        def fn(document, model, ctx):
            return [_ok(cells=[{"amount": "N/A"}])]

        result = dual_extract_components(
            doc,
            plan_meta={
                "plan_key": "x", "name": "X", "recipe_code": "bundled",
                "source_url": "https://utility.example/rates.pdf",
            },
            extract_fn=fn,
            official_hosts=["utility.example"],
            force=True,
        )
        self.assertIsInstance(result, ExtractionHold)
        self.assertEqual(result.reason, "schema_invalid")
        self.assertIn("amount_not_numeric", result.detail)

    def test_duplicate_code_holds(self):
        def fn(document, model, ctx):
            return [
                _ok(),
                _ok(name="Dup"),
            ]

        result = dual_extract_components(
            "Energy Charge 10.000 ¢/kWh\n",
            plan_meta={
                "plan_key": "x", "name": "X", "recipe_code": "bundled",
                "source_url": "https://utility.example/rates.pdf",
            },
            extract_fn=fn,
            official_hosts=["utility.example"],
            force=True,
        )
        self.assertIsInstance(result, ExtractionHold)
        self.assertIn("duplicate_code", result.detail)


if __name__ == "__main__":
    unittest.main()
