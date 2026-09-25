"""Auditor and benchmark read live rows only (DB-backed; see tests/pg_harness.py)."""
from __future__ import annotations

from scripts import benchmark, opus_audit
from scripts import repair_hydro_one_oeb_residential as ho
from tests.pg_harness import PostgresTestCase


class TestLiveOnlyReads(PostgresTestCase):
    def setUp(self):
        # Once per class: name resolution must find exactly one "Hydro One".
        cls = type(self)
        if getattr(cls, "uid", None) is None:
            cls.uid = self.make_utility("Hydro One", state="ON", country="CA")
            meta = ho.plan_meta("tou")
            cls.live = self.make_tariff(cls.uid, meta["name"], meta["components"],
                                        rate_type="seasonal_tou", approved=True)
            self.make_tariff(cls.uid, meta["name"], [
                {"component_type": "energy", "rate_value": 0.5, "period_label": "On-Peak"},
            ], rate_type="tou", superseded_by=cls.live, supersede_reason="vintage")

    def test_auditor_ignores_superseded_rows(self):
        with self.session() as s:
            self.assertEqual([t.id for t in opus_audit.load_live_tariffs(s, self.uid)], [self.live])

    def test_benchmark_resolves_by_name_and_scores_live_gold_exactly(self):
        import json

        with open(benchmark.TOU_SEASONAL_GOLD_PATH) as fh:
            data = json.load(fh)
        gold = next(u for u in data["utilities"] if u["name"] == "Hydro One")
        gold = {**gold, "tariffs": [t for t in gold["tariffs"] if t["code"] == "OEB-RPP-TOU"]}
        with self.session() as s:
            r = benchmark.benchmark_utility(s, gold, benchmark.Tolerance.from_meta(data["_meta"]))
        self.assertEqual((r.utility_id, r.db_tariff_count, r.tariff_recall), (self.uid, 1, 1.0))
        (m,) = r.matches
        self.assertEqual((m.component_recall, m.rate_errors), (1.0, []))
        self.assertTrue(m.db_computable and m.expect_computable)
