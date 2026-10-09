"""PR B: closed-world rider census."""
from __future__ import annotations

import unittest

from app.services.pricing.rider_census import (
    DispositionInput,
    InventoryRider,
    evaluate_rider_census,
)


class TestRiderCensus(unittest.TestCase):
    def setUp(self):
        self.inventory = [
            InventoryRider("fam", "Fuel Adjustment", "rider_per_kwh"),
            InventoryRider("dsm", "DSM Rider", "rider_per_kwh"),
            InventoryRider("storm", "Storm Recovery", "rider_per_kwh"),
        ]

    def test_complete_when_every_rider_dispositioned(self):
        disps = [
            DispositionInput("fam", "applies", "p.12", "FAM applies to all residential"),
            DispositionInput("dsm", "applies", "p.14", "DSM Cost Recovery Rider"),
            DispositionInput(
                "storm", "not_applicable", "p.20",
                "Storm Recovery not applicable to Domestic Service",
            ),
        ]
        result = evaluate_rider_census(self.inventory, disps)
        self.assertTrue(result.complete)
        self.assertTrue(result.mysa_complete_eligible)
        self.assertEqual(result.missing_codes, [])

    def test_gap_blocks_mysa_complete(self):
        disps = [
            DispositionInput("fam", "applies", "p.12", "FAM applies"),
            DispositionInput("dsm", "applies", "p.14", "DSM applies"),
            # storm missing
        ]
        result = evaluate_rider_census(self.inventory, disps)
        self.assertFalse(result.complete)
        self.assertFalse(result.mysa_complete_eligible)
        self.assertEqual(result.missing_codes, ["storm"])
        self.assertIn("census_gap:storm", result.reasons)

    def test_missing_citation_invalid(self):
        disps = [
            DispositionInput("fam", "applies", "p.12", "FAM applies"),
            DispositionInput("dsm", "applies", None, "DSM applies"),
            DispositionInput("storm", "optional", "p.20", "optional green power"),
        ]
        result = evaluate_rider_census(self.inventory, disps)
        self.assertFalse(result.complete)
        self.assertIn("dsm", result.invalid)
        self.assertIn("missing_disposition_page:dsm", result.reasons)

    def test_invalid_disposition_value(self):
        disps = [
            DispositionInput("fam", "applies", "p.1", "ok"),
            DispositionInput("dsm", "maybe", "p.2", "ok"),
            DispositionInput("storm", "applies", "p.3", "ok"),
        ]
        result = evaluate_rider_census(self.inventory, disps)
        self.assertFalse(result.complete)
        self.assertIn("dsm", result.invalid)

    def test_not_found_closes_census_without_value_quote(self):
        disps = [
            DispositionInput("fam", "applies", "p.12", "FAM applies"),
            DispositionInput("dsm", "applies", "p.14", "DSM applies"),
            DispositionInput("storm", "not_found", "not_found", None),
        ]
        result = evaluate_rider_census(self.inventory, disps)
        self.assertTrue(result.complete, result.reasons)


if __name__ == "__main__":
    unittest.main()
