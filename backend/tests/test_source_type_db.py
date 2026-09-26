"""Issue #28 DB-backed regressions: source_type persistence, store paths,
health-score Provenance output and the Hydro-Québec repair.

    TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres \\
        python -m unittest tests.test_source_type_db -v
"""
from __future__ import annotations

import contextlib
import io

from sqlalchemy import text

from scripts import tariff_pipeline as tp
from tests.pg_harness import PostgresTestCase, energy_values

HQ_SITE = "https://www.hydroquebec.com"
HQ_PAGE = "https://www.hydroquebec.com/residential/customer-space/rates/"
HQ_PDF = "https://www.hydroquebec.com/data/documents-donnees/pdf/electricity-rates.pdf"
CALLMEPOWER = "https://callmepower.ca/en/quebec/hydro-quebec-rates"


def _energy(v, **kw):
    return {"component_type": "energy", "unit": "$/kWh", "rate_value": v, **kw}


def _fixed(v):
    return {"component_type": "fixed", "unit": "$/day", "rate_value": v}


def _et(name, components, *, source_url, rate_type="tiered"):
    return tp.ExtractedTariff(
        name=name, code=None, customer_class="residential", rate_type=rate_type,
        description=None, source_url=source_url, effective_date=None, components=components,
    )


class _HQCase(PostgresTestCase):
    def make_hq(self, name="Hydro-Quebec Test"):
        from app.models import Utility

        uid = self.make_utility(name, state="QC", country="CA", website_url=HQ_SITE)
        with self.session() as s:
            s.get(Utility, uid).tariff_page_urls = [HQ_PAGE]
            s.commit()
        return uid

    def source(self, tariff_id):
        t = self.get_tariff(tariff_id)
        return t.source_type, t.source_type_reason


class TestSourceTypePersistence(_HQCase):
    def test_insert_and_source_url_change_are_stamped(self):
        uid = self.make_hq()
        blog = self.make_tariff(uid, "Rate D", [_energy(0.07)], source_url=CALLMEPOWER)
        book = self.make_tariff(uid, "Rate DM", [_energy(0.07)], source_url=HQ_PDF)
        none = self.make_tariff(uid, "Rate X", [_energy(0.07)], source_url=None)
        self.assertEqual(self.source(blog), ("third_party", "aggregator_blocklist"))
        self.assertEqual(self.source(book), ("official", "domain_match"))
        self.assertEqual(self.source(none), ("unknown", "no_url"))

        from app.models import Tariff

        with self.session() as s:
            s.get(Tariff, blog).source_url = HQ_PDF
            s.commit()
        self.assertEqual(self.source(blog), ("official", "domain_match"))
        with self.session() as s:
            s.get(Tariff, book).last_verified_at = None  # unrelated update keeps the label
            s.commit()
        self.assertEqual(self.source(book), ("official", "domain_match"))

    def test_check_constraint(self):
        uid = self.make_hq()
        tid = self.make_tariff(uid, "Rate D", [_energy(0.07)], source_url=HQ_PDF)
        with self.assertRaises(Exception):
            with self.engine.begin() as c:
                c.execute(text("UPDATE tariffs SET source_type = 'blog' WHERE id = :id"), {"id": tid})

    def test_reclassify_backfills_raw_rows(self):
        from app.services.source_type import reclassify_tariffs

        uid = self.make_hq()
        blog = self.make_tariff(uid, "Rate D", [_energy(0.07)], source_url=CALLMEPOWER)
        book = self.make_tariff(uid, "Rate DM", [_energy(0.07)], source_url=HQ_PDF)
        with self.engine.begin() as c:  # as the migration finds them: default, no reason
            c.execute(text(
                "UPDATE tariffs SET source_type = 'unknown', source_type_reason = NULL "
                "WHERE utility_id = :u"
            ), {"u": uid})
        with self.engine.begin() as c:
            dry = reclassify_tariffs(c, utility_ids=[uid], dry_run=True)
        self.assertEqual(self.source(blog), ("unknown", None))
        self.assertEqual(dry["changed"], {("unknown", "third_party"): 1, ("unknown", "official"): 1})
        with self.engine.begin() as c:
            reclassify_tariffs(c, utility_ids=[uid])
        self.assertEqual(self.source(blog), ("third_party", "aggregator_blocklist"))
        self.assertEqual(self.source(book), ("official", "domain_match"))

    def test_migration_backfills_existing_rows(self):
        from tests.pg_harness import run_alembic

        uid = self.make_hq("HQ Migration")
        run_alembic(self.db_url, "downgrade", "f3a4b5c6d7e8")
        ids = {}
        with self.engine.begin() as c:
            for name, url in (("Rate D", CALLMEPOWER), ("Rate DM", HQ_PDF), ("Rate X", None)):
                ids[name] = c.execute(text(
                    "INSERT INTO tariffs (utility_id, name, customer_class, rate_type, is_default, "
                    "approved, source_url) VALUES (:u, :n, 'RESIDENTIAL', 'FLAT', false, false, :url) "
                    "RETURNING id"
                ), {"u": uid, "n": name, "url": url}).scalar()
        run_alembic(self.db_url, "upgrade", "head")
        self.assertEqual(self.source(ids["Rate D"]), ("third_party", "aggregator_blocklist"))
        self.assertEqual(self.source(ids["Rate DM"]), ("official", "domain_match"))
        self.assertEqual(self.source(ids["Rate X"]), ("unknown", "no_url"))

    def test_api_exposes_source_type(self):
        uid = self.make_hq()
        tid = self.make_tariff(uid, "Rate D", [_energy(0.07)], source_url=CALLMEPOWER)
        client = self.api_client()
        listed = client.get(f"/api/utilities/{uid}/tariffs").json()
        self.assertEqual(listed[0]["source_type"], "third_party")
        detail = client.get(f"/api/tariffs/{tid}").json()
        self.assertEqual((detail["source_type"], detail["source_type_reason"]),
                         ("third_party", "aggregator_blocklist"))
        browse = client.get("/api/tariffs/browse", params={"utility_id": uid}).json()
        self.assertEqual(browse["items"][0]["source_type"], "third_party")
        src = client.get(f"/api/tariffs/{tid}/source").json()
        self.assertEqual(src["source_type"], "third_party")


class TestStorePaths(_HQCase):
    def test_official_reextraction_upgrades_third_party_row(self):
        uid = self.make_hq()
        old = self.make_tariff(uid, "Rate D", [_energy(0.07)], source_url=CALLMEPOWER)
        tp.store_tariffs(uid, [_et("Rate D", [_energy(0.07)], source_url=HQ_PDF, rate_type="flat")], dry_run=False)

        o = self.get_tariff(old)
        self.assertEqual(o.supersede_reason, "source_upgrade")
        new = self.get_tariff(o.superseded_by_tariff_id)
        self.assertEqual((new.source_url, new.source_type), (HQ_PDF, "official"))
        self.assertEqual(energy_values(new), [0.07])
        self.assertEqual(energy_values(o), [0.07], "prior components stay on the retired row")
        ev = [e for e in self.events_for(uid) if e.decision == "supersede"]
        self.assertEqual([e.reason for e in ev], ["source_upgrade"])

    def test_third_party_extraction_cannot_replace_official_row(self):
        uid = self.make_hq()
        live = self.make_tariff(uid, "Rate D", [_energy(0.07)], source_url=HQ_PDF)
        blog_url = "https://some-energy-blog.com/hq-rates"
        tp.store_tariffs(uid, [_et("Rate D", [_energy(0.09)], source_url=blog_url, rate_type="flat")], dry_run=False)

        t = self.get_tariff(live)
        self.assertIsNone(t.supersede_reason)
        self.assertEqual(energy_values(t), [0.07])
        holds = [e for e in self.events_for(uid) if e.decision == "hold"]
        self.assertEqual([h.reason for h in holds], ["source_downgrade"])
        self.assertEqual(holds[0].payload["live_source_url"], HQ_PDF)
        self.assertEqual(len(self.tariffs_for(uid)), 1)

    def test_identical_official_reextraction_of_official_row_only_reverifies(self):
        uid = self.make_hq()
        live = self.make_tariff(uid, "Rate D", [_energy(0.07)], source_url=HQ_PDF, verified=False)
        tp.store_tariffs(uid, [_et("Rate D", [_energy(0.07)], source_url=HQ_PAGE, rate_type="flat")], dry_run=False)
        t = self.get_tariff(live)
        self.assertIsNone(t.supersede_reason)
        self.assertIsNotNone(t.last_verified_at)
        self.assertEqual(len(self.tariffs_for(uid)), 1)

    def test_protected_third_party_row_is_not_upgraded(self):
        uid = self.make_hq()
        live = self.make_tariff(uid, "Rate D", [_energy(0.07)], source_url=CALLMEPOWER, approved=True)
        tp.store_tariffs(uid, [_et("Rate D", [_energy(0.07)], source_url=HQ_PDF, rate_type="flat")], dry_run=False)
        self.assertIsNone(self.get_tariff(live).supersede_reason)
        self.assertEqual(len(self.tariffs_for(uid)), 1)


class TestHealthScoreProvenance(_HQCase):
    def test_provenance_counts_and_score(self):
        from sqlalchemy.orm import Session

        from scripts import health_score

        uid = self.make_hq()
        self.make_tariff(uid, "Rate DM", [_energy(0.07)], source_url=HQ_PDF)
        self.make_tariff(uid, "Rate DP", [_energy(0.07)], source_url=HQ_PAGE)
        self.make_tariff(uid, "Rate D", [_energy(0.07)], source_url=CALLMEPOWER)
        self.make_tariff(uid, "Rate X", [_energy(0.07)], source_url=None)
        self.make_tariff(uid, "Old", [_energy(0.07)], source_url=CALLMEPOWER, supersede_reason="refresh")

        with Session(self.engine) as s:
            r = health_score.compute(s)
        counts = r["quality"]["source_type_counts"]
        self.assertEqual(counts, {"official": 2, "unknown": 1, "third_party": 1})
        self.assertEqual(r["components"]["provenance"], round(100 * (2 + 0.4 + 0.2) / 4, 1))
        self.assertEqual(r["quality"]["has_source_url"], 3)  # legacy key kept
        self.assertEqual(r["provenance_method"], "source_type_v2")
        self.assertEqual(r["provenance"]["weights"], health_score.PROVENANCE_WEIGHTS)
        self.assertEqual(r["official_source"]["best_residential_official"], 1)
        self.assertEqual(r["official_source"]["utilities_with_verified_residential"], 1)

        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            health_score.print_scorecard(r)
        self.assertIn("PROVENANCE (source quality", out.getvalue())
        self.assertIn("third_party", out.getvalue())


class TestHqRepair(_HQCase):
    DOC = "Rate D access charge 46.154¢/day; first 40 kWh 6.905¢; balance 10.652¢. Rate DT 4.250¢"

    def _run(self, uid, *, apply, docs=None, reextracted=None):
        from app.models import Utility
        from scripts.repair_hq_official_source import repair

        with self.session() as s:
            with contextlib.redirect_stdout(io.StringIO()):
                report = repair(
                    s, s.get(Utility, uid), docs or {HQ_PDF: self.DOC, HQ_PAGE: ""},
                    apply=apply, reextracted=reextracted,
                )
            s.commit()
        return report

    def test_verified_rows_carried_unverified_left_live(self):
        uid = self.make_hq()
        rate_d = self.make_tariff(uid, "Rate D", [
            _fixed(0.46154),
            _energy(0.06905, tier_min_kwh=0, tier_max_kwh=40),
            _energy(0.10652, tier_min_kwh=40),
        ], source_url=CALLMEPOWER)
        wrong = self.make_tariff(uid, "Rate Flex D", [_energy(0.5555)], source_url=CALLMEPOWER)
        protected = self.make_tariff(uid, "Rate DPC", [_energy(0.0425)], source_url=CALLMEPOWER, approved=True)
        official = self.make_tariff(uid, "Rate DM", [_energy(0.0425)], source_url=HQ_PDF)

        dry = self._run(uid, apply=False)
        self.assertEqual([c[0] for c in dry.carried], [rate_d])
        self.assertEqual(len(self.tariffs_for(uid)), 4, "dry run writes nothing")

        report = self._run(uid, apply=True)
        self.assertEqual(set(report.unresolved), {wrong})
        self.assertEqual(report.skipped_protected, [protected])

        old = self.get_tariff(rate_d)
        self.assertEqual(old.supersede_reason, "source_repair")
        new = self.get_tariff(old.superseded_by_tariff_id)
        self.assertEqual((new.source_url, new.source_type), (HQ_PDF, "official"))
        self.assertIsNotNone(new.source_document_hash)
        self.assertEqual(energy_values(new), [0.06905, 0.10652])
        self.assertEqual(new.confidence_factors["source_repair"]["mode"], "verify_carry")
        self.assertFalse(new.approved)
        for tid in (wrong, protected, official):
            self.assertIsNone(self.get_tariff(tid).supersede_reason)

        ev = [e for e in self.events_for(uid) if e.decision == "supersede"]
        self.assertEqual(len(ev), 1)
        self.assertEqual((ev[0].reason, ev[0].actor_type), ("source_repair", "script"))
        self.assertEqual(ev[0].payload["prior_source_url"], CALLMEPOWER)

        from app.models import MonitoringSource
        with self.session() as s:
            self.assertEqual(s.query(MonitoringSource).filter_by(utility_id=uid, url=HQ_PDF).count(), 1)

        again = self._run(uid, apply=True)
        self.assertEqual(again.carried, [])
        self.assertEqual(set(again.unresolved), {wrong})

    def test_reextracted_match_replaces_unverified_row(self):
        uid = self.make_hq()
        wrong = self.make_tariff(uid, "Rate Flex D", [_energy(0.5555)], source_url=CALLMEPOWER)
        extracted = _et("Rate Flex D", [_energy(0.0425)], source_url=HQ_PDF, rate_type="flat")

        self._run(uid, apply=True, reextracted={extracted.name: extracted})

        old = self.get_tariff(wrong)
        self.assertEqual(old.supersede_reason, "source_repair")
        self.assertEqual(energy_values(old), [0.5555])
        new = self.get_tariff(old.superseded_by_tariff_id)
        self.assertEqual((new.source_type, energy_values(new)), ("official", [0.0425]))
        self.assertEqual(new.confidence_factors["source_repair"]["mode"], "reextract")
