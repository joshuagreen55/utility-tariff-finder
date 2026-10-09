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

    def test_not_found_empty_cells_ok(self):
        """R28: missing value → not_found, do not reject the whole extract."""
        err = validate_extract_schema([
            _ok(),
            _ok(code="storm", name="Storm", disposition="not_found",
                cells=[], source_quote="", source_page=""),
        ])
        self.assertIsNone(err)
        applying = applying_raw_components([
            _ok(),
            _ok(code="storm", name="Storm", disposition="not_found", cells=[]),
        ])
        self.assertEqual([c["code"] for c in applying], ["base"])

    def test_not_applicable_empty_cells_ok(self):
        err = validate_extract_schema([
            _ok(),
            _ok(code="cpp", name="CPP", disposition="not_applicable", cells=[]),
        ])
        self.assertIsNone(err)

    def test_applies_still_requires_cells(self):
        err = validate_extract_schema([_ok(cells=[])])
        self.assertEqual(err, "missing_cells:base")

    def test_tool_schema_requires_disposition_and_string_amount(self):
        props = EXTRACTION_TOOL_SCHEMA["input_schema"]["properties"]["components"]
        item = props["items"]
        self.assertIn("disposition", item["required"])
        self.assertNotIn("cells", item["required"])  # empty OK for not_found
        self.assertIn("not_found", item["properties"]["disposition"]["enum"])
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

    def test_not_found_rider_does_not_hold_extract(self):
        """Priced base found; missing rider marked not_found → accept path open."""
        from app.services.pricing.extraction import ExtractionAccept
        from app.services.pricing.rider_census import InventoryRider

        doc = "Energy Charge 10.000 ¢/kWh\n"

        def fn(document, model, ctx):
            return [
                _ok(),
                {
                    "code": "storm",
                    "kind": "rider_per_kwh",
                    "unit": "¢/kWh",
                    "name": "Storm Recovery",
                    "disposition": "not_found",
                    "cells": [],
                    "source_page": "not_found",
                    "source_quote": "Storm Recovery",
                },
            ]

        result = dual_extract_components(
            doc,
            plan_meta={
                "plan_key": "x", "name": "X", "recipe_code": "bundled",
                "source_url": "https://utility.example/rates.pdf",
            },
            extract_fn=fn,
            official_hosts=["utility.example"],
            inventory=[InventoryRider("storm", "Storm Recovery")],
            force=True,
        )
        self.assertIsInstance(result, ExtractionAccept, getattr(result, "detail", None))
        self.assertEqual([c.code for c in result.plan.components], ["base"])


if __name__ == "__main__":
    unittest.main()
