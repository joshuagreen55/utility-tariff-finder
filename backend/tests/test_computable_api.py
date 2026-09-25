"""API exposes the computable contract (DB-backed; see tests/pg_harness.py).

    TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres \\
        python -m unittest tests.test_computable_api -v
"""
from __future__ import annotations

from scripts import repair_hydro_one_oeb_residential as oeb
from tests.pg_harness import PostgresTestCase


class TestComputableApi(PostgresTestCase):
    def setUp(self):
        self.on_uid = self.make_utility("Hydro One", state="ON", country="CA")
        self.tou_id = self.make_tariff(
            self.on_uid, "Time-of-Use (TOU) — Residential",
            oeb.build_plan_components("tou"), rate_type="seasonal_tou", approved=True,
        )
        self.demand_id = self.make_tariff(self.on_uid, "Residential Demand", [
            {"component_type": "energy", "rate_value": 0.1},
            {"component_type": "demand", "unit": "$/kW", "rate_value": 8.0},
        ], rate_type="demand")
        self.ny_uid = self.make_utility("Upstate Electric", state="NY")
        self.flat_id = self.make_tariff(self.ny_uid, "Residential Service", [
            {"component_type": "energy", "rate_value": 0.18},
            {"component_type": "fixed", "unit": "$/day", "rate_value": 0.55},
        ], confidence_factors={"needs_review": True})

    def test_utility_tariff_list_exposes_contract(self):
        body = self.api_client().get(f"/api/utilities/{self.on_uid}/tariffs").json()
        by_id = {t["id"]: t for t in body}
        self.assertTrue(by_id[self.tou_id]["computable"])
        self.assertEqual(by_id[self.tou_id]["computable_reasons"], [])
        self.assertIn("holiday_rows_require_calendar", by_id[self.tou_id]["computable_warnings"])
        self.assertFalse(by_id[self.demand_id]["computable"])
        self.assertIn("demand_charges_unsupported", by_id[self.demand_id]["computable_reasons"])

    def test_tariff_detail_exposes_contract_timezone_currency(self):
        client = self.api_client()
        on = client.get(f"/api/tariffs/{self.tou_id}").json()
        self.assertTrue(on["computable"])
        self.assertEqual(on["clock_basis"], "local_wall_clock")
        self.assertIsNone(on["timezone"], "Ontario spans two zones")
        self.assertEqual(on["currency"], "CAD")
        ny = client.get(f"/api/tariffs/{self.flat_id}").json()
        self.assertTrue(ny["computable"])
        self.assertTrue(ny["needs_review"])
        self.assertEqual((ny["timezone"], ny["timezone_source"]), ("America/New_York", "state_default"))
        self.assertEqual(ny["currency"], "USD")
        fixed = [c for c in ny["rate_components"] if c["component_type"] == "fixed"]
        self.assertEqual((fixed[0]["unit"], fixed[0]["included_in_energy"]), ("$/day", False))

    def test_browse_exposes_contract(self):
        body = self.api_client().get("/api/tariffs/browse", params={"utility_id": self.on_uid}).json()
        by_id = {t["id"]: t for t in body["items"]}
        self.assertTrue(by_id[self.tou_id]["computable"])
        self.assertFalse(by_id[self.demand_id]["computable"])

    def test_utility_detail_timezone(self):
        body = self.api_client().get(f"/api/utilities/{self.ny_uid}").json()
        self.assertEqual(body["timezone"], "America/New_York")
        self.assertEqual(body["currency"], "USD")

    def test_store_persists_rider_inclusion_flag(self):
        from scripts import tariff_pipeline as tp

        uid = self.make_utility("Rider Electric", state="NS", country="CA")
        comps = tp.expand_stacking_energy_riders([
            {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.18},
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.004, "tier_label": "FAM"},
        ])
        tp.store_tariffs(uid, [tp.ExtractedTariff(
            name="Domestic Service", customer_class="residential", rate_type="flat",
            source_url="https://rider.example.com/rates", components=comps,
        )], dry_run=False)

        (tariff,) = self.tariffs_for(uid)
        adj = [c for c in tariff.rate_components if c.component_type.value == "adjustment"]
        self.assertEqual([c.included_in_energy for c in adj], [True])
        body = self.api_client().get(f"/api/tariffs/{tariff.id}").json()
        self.assertTrue(body["computable"], body["computable_reasons"])

    def test_lookup_matches_get_computable_counts(self):
        from app.schemas.lookup import UtilityMatch
        from app.services.territory_lookup import _attach_contract_fields

        matches = [
            UtilityMatch(id=self.on_uid, name="Hydro One", country="CA", state_province="ON",
                         utility_type="investor_owned", match_method="polygon",
                         residential_tariff_count=2),
            UtilityMatch(id=self.ny_uid, name="Upstate Electric", country="US", state_province="NY",
                         utility_type="investor_owned", match_method="polygon",
                         residential_tariff_count=1),
        ]
        self.async_session_run(lambda s: _attach_contract_fields(matches, s))
        self.assertEqual(matches[0].computable_residential_tariff_count, 1)
        self.assertEqual(matches[0].currency, "CAD")
        self.assertEqual(matches[1].computable_residential_tariff_count, 1)
        self.assertEqual(matches[1].timezone, "America/New_York")
