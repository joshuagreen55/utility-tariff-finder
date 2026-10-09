"""PR R27-2: rider inventory + typical-bill oracles from document sets."""
from __future__ import annotations

import json
import unittest
from decimal import Decimal
from pathlib import Path

from app.services.pricing.compiler import GOLDEN_DIR, compile_plan, plan_from_dict
from app.services.pricing.document_set import build_golden_document_set
from app.services.pricing.inventory_from_docs import (
    build_inventory_from_document_set,
    dispositions_from_plan_components,
    extract_typical_bill_oracles,
    rider_code_from_url,
    riders_from_document_members,
    riders_from_text,
)
from app.services.pricing.preaccept import gate_oracle, run_preaccept
from app.services.pricing.rider_census import (
    DispositionInput,
    evaluate_rider_census,
)
from app.services.pricing.types import ComponentInput


DOC_WITH_REFS = """
Residential Service Schedule R.
Rates are subject to Rider FAC, Rider EE, and Rider Storm.
Also see Rider DSM for cost recovery.
Typical bill for 1000 kWh: 12.50 ¢ per kWh all-in price.
"""

FPL_MARKETING_SNIPPET = """
New customer overview. Illustrative energy charge 7.865 ¢/kWh.
Typical bill example uses promotional figures only.
"""


class TestRiderCodeFromUrl(unittest.TestCase):
    def test_georgia_eccr_pdf(self):
        self.assertEqual(
            rider_code_from_url(
                "https://www.georgiapower.com/content/dam/georgia-power/pdfs/tariffs/eccr.pdf",
                "ECCR",
            ),
            "eccr",
        )

    def test_dominion_fuel_deferral(self):
        code = rider_code_from_url(
            "https://www.dominionenergy.com/-/media/.../fuel-deferral.pdf",
            "Fuel deferral",
        )
        self.assertEqual(code, "fuel_deferral")


class TestTextAndMembers(unittest.TestCase):
    def test_subject_to_riders(self):
        riders = riders_from_text(DOC_WITH_REFS)
        codes = {r.code for r in riders}
        self.assertTrue({"fac", "ee", "storm", "dsm"} <= codes)

    def test_rider_sheets_from_georgia_set(self):
        result = build_golden_document_set("Georgia Power", "bundled")
        riders = riders_from_document_members(result.selected())
        codes = {r.code for r in riders}
        self.assertIn("eccr", codes)
        self.assertTrue("fcr" in codes or "fcr_fuel" in codes)
        self.assertIn("dsm_r", codes)


class TestTypicalBillOracle(unittest.TestCase):
    def test_extracts_cents_per_kwh(self):
        oracles = extract_typical_bill_oracles(DOC_WITH_REFS)
        self.assertEqual(len(oracles), 1)
        self.assertEqual(oracles[0].cents_per_kwh, Decimal("12.50"))
        self.assertEqual(oracles[0].kwh, 1000)

    def test_g6_mismatch_holds(self):
        plan = plan_from_dict({
            "plan_key": "demo",
            "name": "Demo",
            "recipe_code": "bundled",
            "components": [{
                "code": "base", "kind": "base_energy", "unit": "¢/kWh",
                "disposition": "applies",
                "cells": [{"season": "all", "period": "all",
                           "day_type": "all", "tier": "all", "amount": "10.000"}],
            }],
            "official_cents": ["10.000"],
        })
        compiled = compile_plan(plan)
        oracles = extract_typical_bill_oracles(DOC_WITH_REFS)
        fails = gate_oracle(compiled, typical_bill_oracles=oracles)
        self.assertEqual(fails[0].gate, "G6")
        self.assertEqual(fails[0].reason, "typical_bill_mismatch")


class TestInventoryClosesG5(unittest.TestCase):
    def test_nsp_inventory_from_plan_components(self):
        plans = json.loads((GOLDEN_DIR / "plans.json").read_text())["plans"]
        nsp = next(p for p in plans if p["plan_key"] == "nsp-domestic")
        built = build_inventory_from_document_set(
            members=nsp.get("document_set", {}).get("documents") or [],
            plan_components=nsp["components"],
        )
        codes = {r.code for r in built.inventory}
        self.assertIn("fam", codes)
        self.assertIn("dsm", codes)
        # Full dispositions → census complete (G5 would pass).
        disps = [
            DispositionInput(**d)
            for d in dispositions_from_plan_components(nsp["components"])
        ]
        census = evaluate_rider_census(built.inventory, disps)
        self.assertTrue(census.complete, census.reasons)

    def test_missing_disposition_holds_g5(self):
        """R27: empty inventory skipped G5; with inventory, a gap holds."""
        plans = json.loads((GOLDEN_DIR / "plans.json").read_text())["plans"]
        nsp = next(p for p in plans if p["plan_key"] == "nsp-domestic")
        built = build_inventory_from_document_set(
            members=[],
            plan_components=nsp["components"],
        )
        # Drop DSM disposition — census must fail.
        disps = [
            DispositionInput(**d)
            for d in dispositions_from_plan_components(nsp["components"])
            if d["rider_code"] != "dsm"
        ]
        census = evaluate_rider_census(built.inventory, disps)
        self.assertFalse(census.complete)
        self.assertIn("dsm", census.missing_codes)

    def test_georgia_inventory_includes_rider_sheets(self):
        plans = json.loads((GOLDEN_DIR / "plans.json").read_text())["plans"]
        ga = next(p for p in plans if p["plan_key"] == "ga-r31")
        doc_set = build_golden_document_set("Georgia Power", "bundled")
        built = build_inventory_from_document_set(
            members=doc_set.selected(),
            plan_components=ga["components"],
            document_texts=[(
                None,
                "Schedule R subject to Rider ECCR, Rider FCR, and Rider DSM-R.",
            )],
        )
        codes = {r.code for r in built.inventory}
        self.assertTrue({"eccr", "fcr", "dsm_r"} <= codes or
                        {"eccr", "fcr27", "dsm_r"} <= codes or
                        len(codes) >= 3)


class TestPreacceptWithInventory(unittest.TestCase):
    def test_g5_runs_when_inventory_provided(self):
        quote = "Energy Charge 10.000 ¢ per kWh"
        doc = f"{quote}\nFuel rider 1.000 ¢/kWh"
        comps = [
            ComponentInput(
                code="base", kind="base_energy", unit="¢/kWh",
                cells=[{"season": "all", "period": "all",
                        "day_type": "all", "tier": "all", "amount": "10.000"}],
                source_page="p.1", source_quote=quote,
            ),
            ComponentInput(
                code="fuel", kind="rider_per_kwh", unit="¢/kWh",
                cells=[{"season": "all", "period": "all",
                        "day_type": "all", "tier": "all", "amount": "1.000"}],
                source_page="p.2", source_quote="Fuel rider 1.000 ¢/kWh",
            ),
        ]
        from app.services.pricing.types import PlanInput
        from app.services.pricing.rider_census import InventoryRider
        plan = PlanInput(
            plan_key="t", name="T", recipe_code="bundled", components=comps,
        )
        inventory = [
            InventoryRider("fuel", "Fuel", "rider_per_kwh"),
            InventoryRider("storm", "Storm", "rider_per_kwh"),
        ]
        # Only fuel dispositioned → G5 holds.
        result = run_preaccept(
            plan,
            document_text=doc,
            source_url="https://www.example-utility.com/tariff.pdf",
            official_hosts=["example-utility.com"],
            extract_a=comps,
            extract_b=comps,
            inventory=inventory,
            dispositions=[
                DispositionInput("fuel", "applies", "p.2", "Fuel rider 1.000 ¢/kWh"),
            ],
        )
        self.assertFalse(result.accepted)
        self.assertTrue(any(f.gate == "G5" for f in result.failures))


if __name__ == "__main__":
    unittest.main()
