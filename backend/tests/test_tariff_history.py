"""Unit tests for soft-supersede helpers (no database).

    cd backend && python -m unittest tests.test_tariff_history -v
"""
from __future__ import annotations

import unittest
from datetime import date, datetime, time, timezone
from decimal import Decimal
from types import SimpleNamespace

from app.db.session import normalize_sync_url
from app.services import tariff_history as th
from scripts import tariff_pipeline as tp


def _row(**kw):
    base = dict(approved=False, confidence_factors=None,
                superseded_by_tariff_id=None, supersede_reason=None)
    base.update(kw)
    return SimpleNamespace(**base)


class TestProtection(unittest.TestCase):
    def test_scraped_row_is_not_protected(self):
        self.assertFalse(th.is_protected(_row(confidence_factors={"llm_confidence": 0.2})))

    def test_approved_repair_manual_rows_are_protected(self):
        self.assertTrue(th.is_protected(_row(approved=True)))
        self.assertTrue(th.is_protected(_row(confidence_factors={"repair": "repair_x"})))
        self.assertTrue(th.is_protected(_row(confidence_factors={"origin": "manual"})))
        self.assertTrue(th.is_protected({"approved": False, "confidence_factors": {"manual": True}}))

    def test_oeb_feed_rows_are_protected_but_not_manual(self):
        oeb = _row(approved=True, confidence_factors={"origin": "oeb_feed"})
        self.assertTrue(th.is_protected(oeb))
        self.assertFalse(th.is_curated(oeb))
        self.assertFalse(th.is_manual_or_pinned(oeb))
        self.assertTrue(th.is_manual_or_pinned(_row(confidence_factors={"origin": "manual"})))

    def test_live(self):
        self.assertTrue(th.is_live(_row()))
        self.assertFalse(th.is_live(_row(supersede_reason="out_of_scope")))
        self.assertFalse(th.is_live(_row(superseded_by_tariff_id=3)))


class TestComponentSignature(unittest.TestCase):
    def test_order_decimal_and_time_forms_are_equivalent(self):
        a = [
            {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.1,
             "period_start_time": "07:00", "period_end_time": "24:00", "day_type": "weekday"},
            {"component_type": "fixed", "unit": "$/month", "rate_value": 10},
        ]
        b = [
            SimpleNamespace(component_type=SimpleNamespace(value="fixed"), unit="$/month",
                            rate_value=Decimal("10.000000")),
            SimpleNamespace(component_type=SimpleNamespace(value="energy"), unit="$/kWh",
                            rate_value=Decimal("0.100000"), period_start_time=time(7, 0),
                            period_end_time=time(0, 0), day_type="weekday"),
        ]
        self.assertEqual(th.component_signature(a), th.component_signature(b))

    def test_decorative_tier_label_is_ignored_but_rates_are_not(self):
        a = [{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.1, "tier_label": "Base"}]
        b = [{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.1, "tier_label": "Base (all-in +riders)"}]
        c = [{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.11}]
        self.assertEqual(th.component_signature(a), th.component_signature(b))
        self.assertNotEqual(th.component_signature(a), th.component_signature(c))

    def test_type_filter(self):
        comps = [
            {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.1},
            {"component_type": "fixed", "unit": "$/month", "rate_value": 33.41},
        ]
        self.assertEqual(
            th.component_signature(comps, types={"energy"}),
            th.component_signature(comps[:1]),
        )

    def test_duplicate_rows_count(self):
        one = [{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.1}]
        self.assertNotEqual(th.component_signature(one), th.component_signature(one * 2))


class TestFactorsMerge(unittest.TestCase):
    def test_provenance_survives_heuristic_refresh(self):
        old = {"repair": "x", "ticket": "CS-1", "llm_confidence": 0.1, "needs_review": True}
        new = {"llm_confidence": 0.19, "has_energy": 0.1}
        self.assertEqual(
            th.merge_confidence_factors(old, new),
            {"llm_confidence": 0.19, "has_energy": 0.1, "repair": "x", "ticket": "CS-1"},
        )


class TestVintageKeeper(unittest.TestCase):
    def _t(self, tid, eff, protected=False):
        return SimpleNamespace(
            id=tid, effective_date=eff, approved=protected, confidence_factors=None,
            last_verified_at=datetime(2026, 9, tid, tzinfo=timezone.utc),
        )

    def test_protected_wins_same_vintage_tie(self):
        scraped_newer_verify = self._t(9, date(2026, 5, 1))
        repaired = self._t(1, date(2026, 5, 1), protected=True)
        self.assertIs(tp.choose_vintage_keeper([scraped_newer_verify, repaired]), repaired)

    def test_newer_effective_date_still_wins(self):
        repaired_old = self._t(1, date(2025, 5, 1), protected=True)
        scraped_new = self._t(2, date(2026, 5, 1))
        self.assertIs(tp.choose_vintage_keeper([repaired_old, scraped_new]), scraped_new)


class TestSyncUrl(unittest.TestCase):
    def test_bare_urls_pin_psycopg2(self):
        self.assertEqual(
            normalize_sync_url("postgresql://u:p@h:5432/db"), "postgresql+psycopg2://u:p@h:5432/db"
        )
        self.assertEqual(normalize_sync_url("postgres://h/db"), "postgresql+psycopg2://h/db")
        self.assertEqual(
            normalize_sync_url("postgresql+psycopg2://h/db"), "postgresql+psycopg2://h/db"
        )


if __name__ == "__main__":
    unittest.main()
