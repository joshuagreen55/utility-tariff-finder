"""PR C: dual-blind component extraction behind feature flag."""
from __future__ import annotations

import os
import unittest
from unittest import mock

from app.services.pricing import extraction as ex
from app.services.pricing.extraction import (
    ExtractionAccept,
    ExtractionHold,
    dual_extract_components,
)


DOC = (
    "Residential Service\n"
    "Base energy 8.000 ¢ per kWh.\n"
    "Fuel 2.000 ¢/kWh.\n"
)


def _good_payload():
    return [
        {
            "code": "base",
            "kind": "base_energy",
            "unit": "¢/kWh",
            "name": "Base energy",
            "disposition": "applies",
            "cells": [{"amount": "8.000"}],
            "source_page": "p.1",
            "source_quote": "Base energy 8.000 ¢ per kWh",
        },
        {
            "code": "fuel",
            "kind": "rider_per_kwh",
            "unit": "¢/kWh",
            "name": "Fuel",
            "disposition": "applies",
            "cells": [{"amount": "2.000"}],
            "source_page": "p.1",
            "source_quote": "Fuel 2.000 ¢/kWh",
        },
        {
            "code": "green",
            "kind": "rider_per_kwh",
            "unit": "¢/kWh",
            "name": "Green power option",
            "disposition": "optional",
            "cells": [],
            "source_page": "p.1",
        },
    ]


class TestDualExtract(unittest.TestCase):
    def test_flag_off_by_default(self):
        # Ensure default is off even if env was set in the parent process.
        with mock.patch.object(ex, "COMPONENT_EXTRACTION_ENABLED", False):
            result = dual_extract_components(
                DOC,
                plan_meta={
                    "plan_key": "x", "recipe_code": "bundled", "name": "X",
                },
                extract_fn=lambda *a, **k: _good_payload(),
            )
            self.assertIsInstance(result, ExtractionHold)
            self.assertEqual(result.reason, "feature_flag_off")

    def test_agreeing_extracts_accept(self):
        def fn(doc, model, ctx):
            self.assertTrue(ctx.get("blind"))
            self.assertNotIn("prior_values", ctx)
            return _good_payload()

        result = dual_extract_components(
            DOC,
            plan_meta={
                "plan_key": "rs",
                "name": "RS",
                "recipe_code": "bundled",
                "source_url": "https://utility.example/rates.pdf",
            },
            extract_fn=fn,
            official_hosts=["utility.example"],
            force=True,
        )
        self.assertIsInstance(result, ExtractionAccept)
        self.assertTrue(result.preaccept.accepted)
        self.assertEqual(
            result.preaccept.compiled.cents_sorted(places=3)[0].__str__(),
            "10.000",
        )

    def test_disagreement_holds(self):
        def fn(doc, model, ctx):
            payload = _good_payload()
            if model.endswith("sonnet-5-5"):
                # Keep quote/amount consistent so G3 passes; G2 catches the
                # value disagreement.
                payload[1]["cells"] = [{"amount": "2.500"}]
                payload[1]["source_quote"] = "Fuel 2.500 ¢/kWh"
            return payload

        result = dual_extract_components(
            DOC + "\nFuel 2.500 ¢/kWh\n",
            plan_meta={
                "plan_key": "rs", "name": "RS", "recipe_code": "bundled",
                "source_url": "https://utility.example/rates.pdf",
            },
            extract_fn=fn,
            official_hosts=["utility.example"],
            force=True,
        )
        self.assertIsInstance(result, ExtractionHold)
        self.assertEqual(result.reason, "preaccept_failed")
        self.assertIn("G2", result.detail)

    def test_disposition_disagreement_holds(self):
        def fn(doc, model, ctx):
            payload = _good_payload()
            if model.endswith("sonnet-5-5"):
                payload[1]["disposition"] = "optional"
            return payload

        result = dual_extract_components(
            DOC,
            plan_meta={
                "plan_key": "rs", "name": "RS", "recipe_code": "bundled",
                "source_url": "https://utility.example/rates.pdf",
            },
            extract_fn=fn,
            official_hosts=["utility.example"],
            force=True,
        )
        self.assertIsInstance(result, ExtractionHold)
        self.assertIn("G2:compiled_disagree", result.detail)

    def test_rider_applies_with_blank_amount_holds(self):
        """Never accept a possibly understated all-in: base alone is 8¢."""
        from app.services.pricing.rider_census import DispositionInput, InventoryRider

        def fn(doc, model, ctx):
            payload = _good_payload()
            payload[1]["cells"] = [{"amount": "n/a"}]
            return payload

        result = dual_extract_components(
            DOC,
            plan_meta={
                "plan_key": "rs", "name": "RS", "recipe_code": "bundled",
                "source_url": "https://utility.example/rates.pdf",
            },
            extract_fn=fn,
            official_hosts=["utility.example"],
            inventory=[InventoryRider("fuel", "Fuel")],
            dispositions=[DispositionInput("fuel", "not_found", "not_found")],
            force=True,
        )
        self.assertIsInstance(result, ExtractionHold)
        self.assertEqual(result.reason, "applies_amount_blank")
        self.assertEqual(result.detail, "model_a:fuel")

    def test_bad_quote_holds(self):
        def fn(doc, model, ctx):
            payload = _good_payload()
            payload[0]["source_quote"] = "NOT IN THE DOCUMENT 99.999"
            return payload

        result = dual_extract_components(
            DOC,
            plan_meta={
                "plan_key": "rs", "name": "RS", "recipe_code": "bundled",
                "source_url": "https://utility.example/rates.pdf",
            },
            extract_fn=fn,
            official_hosts=["utility.example"],
            force=True,
        )
        self.assertIsInstance(result, ExtractionHold)
        self.assertEqual(result.reason, "quote_verify_failed")

    def test_missing_energy_retries_once(self):
        calls: list[dict] = []

        def fn(doc, model, ctx):
            calls.append(dict(ctx))
            if ctx.get("require_applying_energy"):
                return _good_payload()
            # First pass: fixed charge only (no energy).
            return [{
                "code": "cust",
                "kind": "fixed_monthly",
                "unit": "$/month",
                "name": "Customer",
                "disposition": "applies",
                "cells": [{"amount": "10.00"}],
                "source_page": "p.1",
                "source_quote": "Customer charge $10.00",
            }]

        doc = DOC + "\nCustomer charge $10.00\n"
        result = dual_extract_components(
            doc,
            plan_meta={
                "plan_key": "rs", "name": "RS", "recipe_code": "bundled",
                "source_url": "https://utility.example/rates.pdf",
            },
            extract_fn=fn,
            official_hosts=["utility.example"],
            force=True,
        )
        self.assertIsInstance(result, ExtractionAccept)
        # Two models × (first pass + energy retry) = 4 calls.
        self.assertEqual(len(calls), 4)
        self.assertTrue(any(c.get("require_applying_energy") for c in calls))

    def test_missing_energy_holds_after_retry(self):
        def fn(doc, model, ctx):
            return [{
                "code": "cust",
                "kind": "fixed_monthly",
                "unit": "$/month",
                "name": "Customer",
                "disposition": "applies",
                "cells": [{"amount": "10.00"}],
                "source_page": "p.1",
                "source_quote": "Customer charge $10.00",
            }]

        result = dual_extract_components(
            "Customer charge $10.00\n",
            plan_meta={
                "plan_key": "rs", "name": "RS", "recipe_code": "bundled",
                "source_url": "https://utility.example/rates.pdf",
            },
            extract_fn=fn,
            official_hosts=["utility.example"],
            force=True,
        )
        self.assertIsInstance(result, ExtractionHold)
        self.assertEqual(result.reason, "missing_energy_charge")
        self.assertIn("after_retry", result.detail)

    def test_model_ids_are_repo_defaults(self):
        self.assertEqual(ex.HAIKU_MODEL, os.environ.get("HAIKU_MODEL", "claude-haiku-5-5"))
        self.assertEqual(ex.SONNET_MODEL, os.environ.get("SONNET_MODEL", "claude-sonnet-5-5"))
        self.assertEqual(ex.OPUS_MODEL, os.environ.get("OPUS_MODEL", "claude-opus-5-5"))


if __name__ == "__main__":
    unittest.main()
