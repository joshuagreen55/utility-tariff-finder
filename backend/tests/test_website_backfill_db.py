"""Issue #30 DB-backed regressions: website_url backfill + reclassify.

    TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres \\
        python -m unittest tests.test_website_backfill_db -v
"""
from __future__ import annotations

import contextlib
import csv
import io
import json
import os
import tempfile

from sqlalchemy import text

from tests.pg_harness import PostgresTestCase, energy_values

OPENEI = "https://openei.org/apps/USURDB/rate/view/5"


def _energy(v):
    return {"component_type": "energy", "unit": "$/kWh", "rate_value": v}


class TestWebsiteBackfill(PostgresTestCase):
    def setUp(self):
        fd, self.log_path = tempfile.mkstemp(suffix=".jsonl")
        os.close(fd)
        os.remove(self.log_path)
        self.addCleanup(lambda: os.path.exists(self.log_path) and os.remove(self.log_path))

    def run_main(self, *args):
        from scripts.backfill_website_url import main

        with contextlib.redirect_stdout(io.StringIO()) as out:
            result = main([*map(str, args), "--log-path", self.log_path])
        result["stdout"] = out.getvalue()
        return result

    def website(self, uid):
        with self.engine.connect() as c:
            return c.execute(text("SELECT website_url FROM utilities WHERE id = :id"), {"id": uid}).scalar()

    def label(self, tid):
        t = self.get_tariff(tid)
        return t.source_type, t.source_type_reason

    def log_lines(self):
        if not os.path.exists(self.log_path):
            return []
        with open(self.log_path, encoding="utf-8") as fh:
            return [json.loads(line) for line in fh]

    def test_dry_run_writes_nothing_apply_fills_blank_and_reclassifies_only_labels(self):
        uid = self.make_utility("Coop A", website_url=None)
        own1 = self.make_tariff(uid, "Res", [_energy(0.10)], source_url="https://www.coopa.coop/rates.pdf")
        own2 = self.make_tariff(uid, "Res TOU", [_energy(0.12)], source_url="https://coopa.coop/tou")
        seed = self.make_tariff(uid, "Res seed", [_energy(0.08)], source_url=OPENEI)
        old = self.make_tariff(uid, "Res 2025", [_energy(0.09)], source_url="https://coopa.coop/2025.pdf",
                               supersede_reason="refresh")
        self.assertEqual(self.label(own1), ("unknown", "no_official_host"))
        before = {t.id: (energy_values(t), t.last_verified_at, t.supersede_reason) for t in self.tariffs_for(uid)}
        events_before = len(self.events_for(uid))

        dry = self.run_main("--utility-id", uid)
        self.assertIsNone(dry["applied"])
        self.assertIsNone(self.website(uid))
        self.assertEqual(self.label(own1), ("unknown", "no_official_host"))
        self.assertEqual(self.log_lines(), [])
        report = dry["report"]
        self.assertEqual([(p.utility_id, p.new_website_url) for p in report.plan], [(uid, "https://coopa.coop")])
        self.assertEqual(report.projection[("unknown", "no_official_host", "official", "domain_match")], 2)
        self.assertIn("DRY RUN", dry["stdout"])

        applied = self.run_main("--utility-id", uid, "--apply")["applied"]
        self.assertEqual(applied["updated"], [uid])
        self.assertEqual(self.website(uid), "https://coopa.coop")
        self.assertEqual(self.label(own1), ("official", "domain_match"))
        self.assertEqual(self.label(own2), ("official", "domain_match"))
        self.assertEqual(self.label(old), ("official", "domain_match"), "superseded rows relabelled too")
        self.assertEqual(self.label(seed), ("third_party", "aggregator_blocklist"))

        after = {t.id: (energy_values(t), t.last_verified_at, t.supersede_reason) for t in self.tariffs_for(uid)}
        self.assertEqual(after, before, "rates, verification and supersede state untouched")
        self.assertEqual(len(self.events_for(uid)), events_before, "no change events")
        lines = self.log_lines()
        self.assertEqual(len(lines), 1)
        self.assertEqual((lines[0]["utility_id"], lines[0]["old_website_url"], lines[0]["new_website_url"],
                          lines[0]["origin"], lines[0]["domain"]),
                         (uid, None, "https://coopa.coop", "inferred", "coopa.coop"))

        again = self.run_main("--utility-id", uid, "--apply")
        self.assertIsNone(again["applied"], "idempotent: nothing left to write")

    def test_existing_website_is_kept_unless_force(self):
        uid = self.make_utility("Renamed Co", website_url="https://old-brand.com")
        tids = [self.make_tariff(uid, f"R{i}", [_energy(0.1)], source_url=f"https://www.newbrand.com/r{i}.pdf")
                for i in range(2)]
        self.assertEqual(self.label(tids[0]), ("third_party", "domain_mismatch"))

        res = self.run_main("--utility-id", uid, "--apply")
        self.assertEqual(res["report"].in_scope, [])
        self.assertEqual(self.website(uid), "https://old-brand.com")

        res = self.run_main("--utility-id", uid, "--apply", "--force")
        self.assertEqual(res["applied"]["updated"], [uid])
        self.assertEqual(self.website(uid), "https://www.newbrand.com")
        self.assertEqual(self.label(tids[0]), ("official", "domain_match"))
        self.assertEqual(self.log_lines()[-1]["old_website_url"], "https://old-brand.com")

    def test_write_time_guard_skips_rows_changed_since_audit(self):
        from scripts.backfill_website_url import apply_plan, audit

        uid = self.make_utility("Racy Coop", website_url=None)
        for i in range(2):
            self.make_tariff(uid, f"R{i}", [_energy(0.1)], source_url=f"https://racycoop.org/r{i}")
        with self.engine.connect() as c:
            report = audit(c, utility_ids=[uid])
        self.assertEqual(len(report.plan), 1)
        with self.engine.begin() as c:
            c.execute(text("UPDATE utilities SET website_url = 'https://set-by-hand.org' WHERE id = :id"),
                      {"id": uid})
        res = apply_plan(self.engine, report.plan, log_path=self.log_path)
        self.assertEqual((res["updated"], res["raced"]), ([], [uid]))
        self.assertEqual(self.website(uid), "https://set-by-hand.org")
        self.assertEqual(self.log_lines(), [])

    def test_review_candidates_are_not_written_but_curated_csv_is(self):
        thin = self.make_utility("Thin Coop", website_url=None)
        tid = self.make_tariff(thin, "R", [_energy(0.1)], source_url="https://thincoop.org/r.pdf")
        has_site = self.make_utility("Has Site", website_url="https://hassite.org")

        res = self.run_main("--utility-id", thin, "--apply")
        self.assertEqual([i.reasons for i in res["report"].inferences], [["thin_evidence"]])
        self.assertIsNone(res["applied"])
        self.assertIsNone(self.website(thin))

        fd, path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        self.addCleanup(os.remove, path)
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["utility_id", "website_url", "note"])
            w.writerow([thin, "www.thincoop.org", "reviewed"])
            w.writerow([has_site, "https://other.org", ""])
        res = self.run_main("--csv", path, "--csv-only", "--apply")
        self.assertEqual(res["applied"]["updated"], [thin])
        self.assertEqual(self.website(thin), "https://www.thincoop.org")
        self.assertEqual(self.website(has_site), "https://hassite.org", "curated rows obey blank-only too")
        self.assertEqual(self.label(tid), ("official", "domain_match"))
        self.assertEqual(self.log_lines()[-1]["origin"], "csv")

    def test_bad_curated_csv_aborts_before_any_write(self):
        uid = self.make_utility("Blocked Coop", website_url=None)
        fd, path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        self.addCleanup(os.remove, path)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(f"utility_id,website_url\n{uid},https://openei.org/coop\n")
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            self.run_main("--csv", path, "--apply")
        self.assertIsNone(self.website(uid))
