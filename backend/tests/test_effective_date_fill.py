"""Effective-date fill on re-extract match (issue #26).

Pure tests cover the parser and the match rule; DB-backed tests (skipped
without ``TEST_DATABASE_URL``) cover ``store_tariffs``:

- blank DB date + dated extract, same rates → filled in place, re-verified;
- dated DB row + undated extract → date kept;
- older extract date → date kept, no revision;
- newer extract date → soft-supersede (reason ``refresh``) carrying it;
- protected row with a blank date → left blank, ``hold`` logged.

    TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres \\
        python -m unittest tests.test_effective_date_fill -v
"""
from __future__ import annotations

import unittest
from datetime import date, datetime
from types import SimpleNamespace

from scripts import tariff_pipeline as tp
from tests.pg_harness import PostgresTestCase

TODAY = date(2026, 9, 25)
DOMAIN = "https://utility.example.com"


class TestParseEffectiveDate(unittest.TestCase):
    def p(self, raw):
        return tp._parse_effective_date(raw, today=TODAY)

    def test_iso_and_variants(self):
        self.assertEqual(self.p("2026-01-01"), date(2026, 1, 1))
        self.assertEqual(self.p("2026-01-01T00:00:00Z"), date(2026, 1, 1))
        self.assertEqual(self.p("2026/3/5"), date(2026, 3, 5))
        self.assertEqual(self.p(" 2025-10-01 "), date(2025, 10, 1))

    def test_month_name_forms(self):
        self.assertEqual(self.p("January 1, 2026"), date(2026, 1, 1))
        self.assertEqual(self.p("Sept. 15th, 2025"), date(2025, 9, 15))
        self.assertEqual(self.p("1 Nov 2025"), date(2025, 11, 1))

    def test_unambiguous_slash_dates_only(self):
        self.assertEqual(self.p("6/15/2026"), date(2026, 6, 15))
        self.assertEqual(self.p("15/06/2026"), date(2026, 6, 15))
        self.assertEqual(self.p("7/7/2026"), date(2026, 7, 7))
        self.assertIsNone(self.p("6/7/2026"), "M/D vs D/M is ambiguous")

    def test_date_objects(self):
        self.assertEqual(self.p(date(2026, 2, 1)), date(2026, 2, 1))
        self.assertEqual(self.p(datetime(2026, 2, 1, 9, 30)), date(2026, 2, 1))

    def test_never_invents_a_day(self):
        for raw in ("", None, "2026", "2026-05", "May 2026", "Spring 2026",
                    "effective upon approval", "n/a", "2026-02-30"):
            self.assertIsNone(self.p(raw), raw)

    def test_implausible_years_rejected(self):
        self.assertIsNone(self.p("1899-01-01"))
        self.assertIsNone(self.p("2031-01-01"))
        self.assertEqual(self.p("2027-01-01"), date(2027, 1, 1), "next-year editions allowed")


class TestEffectiveDateMatchRule(unittest.TestCase):
    RT = "flat"
    COMPS = [{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.12}]

    def existing(self, eff):
        return SimpleNamespace(rate_type=self.RT, effective_date=eff, rate_components=self.COMPS)

    def matches(self, db_date, extract_date):
        return tp._content_matches(self.existing(db_date), self.RT, extract_date, self.COMPS)

    def test_undated_extract_matches(self):
        self.assertTrue(self.matches(date(2025, 1, 1), None))
        self.assertTrue(self.matches(None, None))

    def test_blank_db_date_matches_and_fills(self):
        self.assertTrue(self.matches(None, date(2026, 1, 1)))
        self.assertTrue(tp._should_fill_effective_date(None, date(2026, 1, 1)))

    def test_older_or_equal_extract_date_matches_without_fill(self):
        self.assertTrue(self.matches(date(2026, 1, 1), date(2025, 1, 1)))
        self.assertTrue(self.matches(date(2026, 1, 1), date(2026, 1, 1)))
        self.assertFalse(tp._should_fill_effective_date(date(2026, 1, 1), date(2025, 1, 1)))
        self.assertFalse(tp._should_fill_effective_date(date(2026, 1, 1), None))

    def test_newer_extract_date_is_a_revision(self):
        self.assertFalse(self.matches(date(2025, 1, 1), date(2026, 1, 1)))
        self.assertTrue(tp._is_newer_effective_date(date(2025, 1, 1), date(2026, 1, 1)))

    def test_changed_rates_still_break_the_match(self):
        other = [{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.13}]
        self.assertFalse(tp._content_matches(self.existing(None), self.RT, None, other))


def _et(name, components, *, effective_date=""):
    return tp.ExtractedTariff(
        name=name, code=None, customer_class="residential", rate_type="flat",
        description=None, source_url=f"{DOMAIN}/rates",
        effective_date=effective_date, components=components,
    )


def _energy(v):
    return {"component_type": "energy", "unit": "$/kWh", "rate_value": v}


class TestStoreTariffsEffectiveDate(PostgresTestCase):
    NAME = "Residential Service"

    def _store(self, uid, effective_date, rate=0.12):
        return tp.store_tariffs(
            uid, [_et(self.NAME, [_energy(rate)], effective_date=effective_date)], dry_run=False,
        )

    def test_blank_date_filled_on_identical_reextract(self):
        uid = self.make_utility("Blank Date Electric")
        tid = self.make_tariff(uid, self.NAME, [_energy(0.12)])
        before = self.get_tariff(tid).last_verified_at

        self.assertEqual(self._store(uid, "2026-01-01"), 1)

        rows = self.tariffs_for(uid)
        self.assertEqual([r.id for r in rows], [tid], "no revision churn for a date fill")
        t = rows[0]
        self.assertEqual(t.effective_date, date(2026, 1, 1))
        self.assertGreater(t.last_verified_at, before)
        events = self.events_for(uid)
        self.assertEqual(
            [(e.decision, e.reason, e.before_tariff_id) for e in events],
            [("metadata", "effective_date_fill", tid)],
        )
        self.assertEqual(events[0].payload, {"effective_date": {"from": None, "to": "2026-01-01"}})

    def test_printed_date_form_fills(self):
        uid = self.make_utility("Printed Date Electric")
        tid = self.make_tariff(uid, self.NAME, [_energy(0.12)])

        self._store(uid, "January 1, 2026")

        self.assertEqual(self.get_tariff(tid).effective_date, date(2026, 1, 1))

    def test_undated_extract_keeps_existing_date(self):
        uid = self.make_utility("Keep Date Electric")
        tid = self.make_tariff(uid, self.NAME, [_energy(0.12)], effective_date=date(2025, 6, 1))

        self._store(uid, "")

        self.assertEqual(self.get_tariff(tid).effective_date, date(2025, 6, 1))
        self.assertEqual(self.events_for(uid), [])

    def test_undated_extract_leaves_blank_date_blank(self):
        uid = self.make_utility("Still Blank Electric")
        tid = self.make_tariff(uid, self.NAME, [_energy(0.12)])

        self._store(uid, "effective upon approval")

        self.assertIsNone(self.get_tariff(tid).effective_date)
        self.assertEqual(self.events_for(uid), [])

    def test_older_extract_date_does_not_regress(self):
        uid = self.make_utility("Older Date Electric")
        tid = self.make_tariff(uid, self.NAME, [_energy(0.12)], effective_date=date(2026, 1, 1))

        self._store(uid, "2024-01-01")

        rows = self.tariffs_for(uid)
        self.assertEqual([r.id for r in rows], [tid])
        self.assertEqual(rows[0].effective_date, date(2026, 1, 1))
        self.assertEqual(self.events_for(uid), [])

    def test_newer_extract_date_soft_supersedes(self):
        uid = self.make_utility("Newer Date Electric")
        tid = self.make_tariff(uid, self.NAME, [_energy(0.12)], effective_date=date(2025, 1, 1))

        self._store(uid, "2026-01-01")

        old = self.get_tariff(tid)
        self.assertEqual(old.supersede_reason, "refresh")
        self.assertEqual(old.effective_date, date(2025, 1, 1), "prior edition keeps its date")
        new = self.get_tariff(old.superseded_by_tariff_id)
        self.assertEqual(new.effective_date, date(2026, 1, 1))
        self.assertIsNone(new.supersede_reason)

    def test_protected_blank_date_is_held_not_filled(self):
        uid = self.make_utility("Protected Date Electric")
        tid = self.make_tariff(
            uid, self.NAME, [_energy(0.12)],
            approved=True, confidence_factors={"repair": "x"},
        )

        self._store(uid, "2026-01-01")

        t = self.get_tariff(tid)
        self.assertIsNone(t.effective_date)
        self.assertIsNone(t.supersede_reason)
        holds = [e for e in self.events_for(uid) if e.decision == "hold"]
        self.assertEqual([h.before_tariff_id for h in holds], [tid])
        self.assertEqual(holds[0].payload, {"proposed": {"effective_date": "2026-01-01"}})

    def test_changed_rates_without_date_carry_existing_date(self):
        uid = self.make_utility("Rate Change Electric")
        tid = self.make_tariff(uid, self.NAME, [_energy(0.12)], effective_date=date(2025, 6, 1))

        self._store(uid, "", rate=0.14)

        old = self.get_tariff(tid)
        self.assertEqual(old.supersede_reason, "refresh")
        new = self.get_tariff(old.superseded_by_tariff_id)
        self.assertEqual(new.effective_date, date(2025, 6, 1))


if __name__ == "__main__":
    unittest.main()
