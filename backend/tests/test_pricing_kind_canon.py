"""Kind canonicalization, value-based G2 and G5 completeness (R30 replay)."""
from __future__ import annotations

import unittest

from app.services.pricing.kind_canon import canonicalize_component
from app.services.pricing.preaccept import gate_agreement, gate_completeness
from app.services.pricing.types import ComponentInput


def _raw(kind, unit, amount="1.0", **kw):
    return {"code": kw.pop("code", kind), "kind": kind, "unit": unit,
            "disposition": "applies", "cells": [{"amount": amount}], **kw}


class TestKindCanon(unittest.TestCase):
    def test_kind_follows_unit(self):
        cases = [
            (("fixed_monthly_customer_charge", "$/30 days"), "fixed_monthly"),
            (("basic_customer_charge", "per month"), "fixed_monthly"),
            (("fixed_daily_charge", "$/day"), "fixed_monthly"),
            (("demand", "$/kW"), "demand_charge"),
            (("percent_surcharge", "percent"), "rider_percent"),
            (("energy", "cents/kWh"), "base_energy"),
            (("energy_charge", "$/kWh"), "base_energy"),
            (("rate_rider", "cents/kWh"), "rider_per_kwh"),
            (("fuel_adjustment", "$/kWh"), "rider_per_kwh"),
            (("rebate", "cents/kWh"), "credit"),
            (("season", "date_range"), "season_calendar"),
            (("tier_threshold", "kWh"), "tier_structure"),
        ]
        for (kind, unit), want in cases:
            out = canonicalize_component(_raw(kind, unit), "bundled")
            self.assertEqual(out["kind"], want, (kind, unit))

    def test_canonical_kind_is_kept(self):
        for kind in ("excluded_item", "event_day", "regulated_commodity",
                     "rider_per_kwh"):
            out = canonicalize_component(_raw(kind, "¢/kWh"), "bundled")
            self.assertEqual(out["kind"], kind)
            self.assertNotIn("model_kind", out)

    def test_percent_and_factor_units(self):
        self.assertEqual(
            canonicalize_component(_raw("percent", "percent_of_base_revenue"),
                                   "bundled")["unit"], "%")
        self.assertEqual(
            canonicalize_component(_raw("loss_factor", "factor"),
                                   "provincial_ontario")["kind"], "multiplier")

    def test_deregulated_side_from_category(self):
        supply = canonicalize_component(
            _raw("energy", "$/kWh", charge_category="supply"), "deregulated")
        self.assertEqual((supply["kind"], supply["charge_category"]),
                         ("default_supply", "supply"))
        wires = canonicalize_component(
            _raw("volumetric", "$/kWh", charge_category="distribution"),
            "deregulated")
        self.assertEqual((wires["kind"], wires["charge_category"]),
                         ("delivery_per_kwh", "delivery"))

    def test_bare_per_kwh_takes_printed_scale(self):
        doc = "Energy Charge 9.8¢ per kWh\n"
        out = canonicalize_component(
            _raw("energy", "per kWh", "9.8", source_quote="Energy Charge 9.8¢"),
            "bundled", doc)
        self.assertEqual(out["unit"], "¢/kWh")

    def test_bare_per_kwh_without_printed_scale_stays_ambiguous(self):
        doc = "Energy Charge 9.8 per kWh\n"
        out = canonicalize_component(
            _raw("energy", "per kWh", "9.8", source_quote="Energy Charge 9.8"),
            "bundled", doc)
        self.assertEqual(out["unit"], "per kWh")


def _c(code, kind, amount, unit="¢/kWh", disposition="applies", **cell):
    return ComponentInput(code=code, kind=kind, unit=unit, name=code,
                          cells=[{"amount": amount, **cell}] if amount else [],
                          disposition=disposition)


class TestAgreementByValue(unittest.TestCase):
    def test_different_codes_and_labels_same_prices_agree(self):
        a = [_c("ENERGY_BASE", "base_energy", "18.324"),
             _c("FAM", "rider_per_kwh", "0.156")]
        b = [_c("energy_charge", "base_energy", "0.18324", unit="$/kWh"),
             _c("fuel_rider", "rider_per_kwh", "0.156")]
        self.assertEqual(gate_agreement(a, b, recipe_code="bundled"), [])

    def test_different_all_in_disagrees(self):
        a = [_c("base", "base_energy", "18.324"), _c("fam", "rider_per_kwh", "0.156")]
        b = [_c("base", "base_energy", "18.324")]
        f = gate_agreement(a, b, recipe_code="bundled")
        self.assertEqual([x.reason for x in f], ["compiled_disagree"])


class TestCompleteness(unittest.TestCase):
    DOC = "Energy 18.324\nDemand Side Management Rider (DSM)\n"

    def test_named_rider_without_value_holds(self):
        comps = [_c("base", "base_energy", "18.324"),
                 _c("dsm", "rider_per_kwh", None, disposition="not_found")]
        comps[1].name = "Demand Side Management Rider"
        f = gate_completeness(self.DOC, [comps, comps], recipe_code="bundled")
        self.assertIn("charge_value_not_found", [x.reason for x in f])

    def test_unnamed_template_row_does_not_hold(self):
        comps = [_c("base", "base_energy", "18.324"),
                 _c("storm_xyz", "rider_per_kwh", None, disposition="not_found"),
                 _c("green", "rider_per_kwh", None, disposition="optional")]
        self.assertEqual(
            gate_completeness(self.DOC, [comps, comps], recipe_code="bundled"), [])

    def test_no_rider_census_holds_full_bill(self):
        comps = [_c("base", "base_energy", "7.865"),
                 _c("cip", "rider_per_kwh", "0.18")]
        f = gate_completeness("", [comps, comps], recipe_code="bundled")
        self.assertEqual([x.reason for x in f], ["no_rider_census"])
        self.assertEqual(gate_completeness(
            "", [comps, comps], recipe_code="bundled", inventory_closed=True), [])
        self.assertEqual(
            gate_completeness("", [comps, comps], recipe_code="texas_tdu"), [])


if __name__ == "__main__":
    unittest.main()
