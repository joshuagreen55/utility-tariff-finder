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

    def test_model_ids_are_repo_defaults(self):
        self.assertEqual(ex.HAIKU_MODEL, os.environ.get("HAIKU_MODEL", "claude-haiku-5-5"))
        self.assertEqual(ex.SONNET_MODEL, os.environ.get("SONNET_MODEL", "claude-sonnet-5-5"))
        self.assertEqual(ex.OPUS_MODEL, os.environ.get("OPUS_MODEL", "claude-opus-5-5"))


if __name__ == "__main__":
    unittest.main()
