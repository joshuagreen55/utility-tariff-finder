"""Issue #30: website_url inference + backfill plan (pure functions, no database)."""
from __future__ import annotations

import csv
import os
import tempfile
import unittest

from app.services.source_type import OFFICIAL, UNKNOWN, UtilitySourceContext, classify_source
from scripts.backfill_website_url import (
    ACCEPT,
    NONE,
    REVIEW,
    CuratedRow,
    Inference,
    UtilityEvidence,
    build_plan,
    export_rows,
    host_exclusion,
    infer_website,
    load_curated_csv,
    mark_shared_domains,
    project_source_types,
    website_for_domain,
)

FPL = ("https://www.fpl.com/rates/pdf/res.pdf", "https://fpl.com/rates", "https://rates.fpl.com/tou.pdf")


def ev(*urls, uid=1, utility_type="IOU", website=None):
    return UtilityEvidence(uid, f"Utility {uid}", utility_type, website, tuple(urls))


class TestHostExclusion(unittest.TestCase):
    def test_blocklist_generic_publisher_and_missing(self):
        self.assertEqual(host_exclusion("https://openei.org/apps/USURDB/rate/view/1"), "third_party")
        self.assertEqual(host_exclusion("https://www.callmepower.ca/en/quebec"), "third_party")
        self.assertEqual(host_exclusion("https://static1.squarespace.com/static/a/rates.pdf"), "generic_host")
        self.assertEqual(host_exclusion("https://drive.google.com/file/d/x"), "generic_host")
        self.assertEqual(host_exclusion("https://www.oeb.ca/rates"), "regulator_publisher")
        self.assertEqual(host_exclusion("https://www.auc.ab.ca/rolr"), "regulator_publisher")
        self.assertEqual(host_exclusion(""), "no_url")
        self.assertEqual(host_exclusion(None), "no_url")
        self.assertIsNone(host_exclusion("https://www.fpl.com/rates"))
        self.assertIsNone(host_exclusion("https://www.puc.nh.gov/x.pdf"), "gov is judged later, not excluded")


class TestInference(unittest.TestCase):
    def test_www_and_subdomains_collapse_to_one_domain(self):
        inf = infer_website(ev(*FPL))
        self.assertEqual(inf.status, ACCEPT)
        self.assertEqual((inf.domain, inf.support, inf.eligible), ("fpl.com", 3, 3))
        self.assertIn(inf.candidate_url, ("https://www.fpl.com", "https://fpl.com"))

    def test_candidate_origin_as_cited(self):
        self.assertEqual(website_for_domain("fpl.com", ["https://www.fpl.com/a", "https://www.fpl.com/b",
                                                        "https://fpl.com/c"]), "https://www.fpl.com")
        self.assertEqual(website_for_domain("coop.org", ["http://coop.org/a", "http://coop.org/b"]),
                         "http://coop.org")
        self.assertEqual(website_for_domain("fpl.com", ["https://rates.fpl.com/x.pdf"]), "https://fpl.com")
        self.assertEqual(website_for_domain("fpl.com", ["https://www.fpl.com/a", "http://www.fpl.com/b"]),
                         "https://www.fpl.com", "tie prefers https")

    def test_blocklisted_hosts_never_become_the_website(self):
        inf = infer_website(ev(
            "https://openei.org/apps/USURDB/rate/view/1", "https://openei.org/apps/USURDB/rate/view/2",
            "https://openei.org/apps/USURDB/rate/view/3", "https://www.energysage.com/x",
            "https://www.examplecoop.coop/rates", "https://examplecoop.coop/tariffs.pdf",
        ))
        self.assertEqual((inf.status, inf.domain), (ACCEPT, "examplecoop.coop"))
        self.assertEqual(inf.excluded["third_party"], 4)
        self.assertEqual((inf.support, inf.eligible, inf.share), (2, 2, 1.0))

    def test_only_excluded_hosts_gives_no_candidate(self):
        inf = infer_website(ev(
            "https://openei.org/x", "https://static1.squarespace.com/s/r.pdf", "https://www.oeb.ca/rates", "",
        ))
        self.assertEqual((inf.status, inf.candidate_url), (NONE, None))
        self.assertEqual(inf.reasons, ["no_eligible_host"])
        self.assertEqual(dict(inf.excluded),
                         {"third_party": 1, "generic_host": 1, "regulator_publisher": 1, "no_url": 1})

    def test_no_verified_residential(self):
        inf = infer_website(ev())
        self.assertEqual((inf.status, inf.reasons), (NONE, ["no_verified_residential"]))

    def test_thin_evidence_needs_review_unless_threshold_lowered(self):
        self.assertEqual(infer_website(ev(FPL[0])).reasons, ["thin_evidence"])
        self.assertEqual(infer_website(ev(FPL[0]), min_tariffs=1).status, ACCEPT)

    def test_no_majority_and_tie(self):
        split = ev(*FPL[:2], "https://neighbor.com/a", "https://neighbor.com/b", "https://third.com/c")
        inf = infer_website(split)
        self.assertEqual((inf.status, inf.reasons), (REVIEW, ["tie"]))
        minority = ev(*FPL, "https://neighbor.com/a", "https://neighbor.com/b")
        inf = infer_website(minority)  # 3/5 = 0.6 passes the default
        self.assertEqual((inf.status, inf.domain), (ACCEPT, "fpl.com"))
        self.assertEqual(infer_website(minority, min_share=0.75).reasons, ["no_majority"])

    def test_government_host_only_for_public_utilities(self):
        city = ("https://www.seattle.gov/city-light/rates", "https://seattle.gov/city-light/res.pdf")
        inf = infer_website(ev(*city, utility_type="MUNICIPAL"))
        self.assertEqual((inf.status, inf.domain), (ACCEPT, "seattle.gov"))
        inf = infer_website(ev(*city, utility_type="IOU"))
        self.assertEqual((inf.status, inf.reasons), (REVIEW, ["government_host"]))

    def test_jurisdiction_wide_government_domain_always_reviewed(self):
        for urls in (
            ("https://www.puc.nh.gov/Tariffs/a.pdf", "https://www.puc.nh.gov/Tariffs/b.pdf"),
            ("https://www.psc.texas.gov/a", "https://psc.texas.gov/b"),
            ("https://puc.state.nh.us/a", "https://puc.state.nh.us/b"),
            ("https://www2.gov.bc.ca/a", "https://www2.gov.bc.ca/b"),
            ("https://elibrary.ferc.gov/a", "https://elibrary.ferc.gov/b"),
        ):
            for utype in ("MUNICIPAL", "IOU"):
                inf = infer_website(ev(*urls, utility_type=utype))
                self.assertEqual((inf.status, inf.reasons), (REVIEW, ["jurisdiction_government_domain"]),
                                 (urls[0], utype))

    def test_jurisdiction_domain_rules(self):
        from scripts.backfill_website_url import is_jurisdiction_government_domain as j

        for d in ("nh.gov", "texas.gov", "mass.gov", "gov.bc.ca", "canada.gc.ca", "puc.state.nh.us", "ferc.gov"):
            self.assertTrue(j(d), d)
        for d in ("seattle.gov", "austintexas.gov", "bpa.gov", "ci.anaheim.ca.us", "fpl.com", "hydro.mb.ca"):
            self.assertFalse(j(d), d)

    def test_existing_website_on_same_domain_is_a_no_op(self):
        inf = infer_website(ev(*FPL, website="http://FPL.com/"))
        self.assertEqual((inf.status, inf.reasons), (NONE, ["matches_existing"]))


class TestSharedDomains(unittest.TestCase):
    def test_shared_with_existing_or_other_candidate_goes_to_review(self):
        a = infer_website(ev(*FPL, uid=1))
        b = infer_website(ev(*FPL, uid=2))
        c = infer_website(ev("https://coop.org/a", "https://coop.org/b", uid=3))
        mark_shared_domains([a, b, c], {1: None, 2: None, 3: None, 9: "https://www.coop.org"})
        self.assertEqual((a.status, a.shared_with, a.reasons), (REVIEW, (2,), ["shared_domain"]))
        self.assertEqual(b.shared_with, (1,))
        self.assertEqual((c.status, c.shared_with), (REVIEW, (9,)))

    def test_allow_shared_accepts_parent_company_but_not_government(self):
        a = infer_website(ev("https://xcelenergy.com/a", "https://xcelenergy.com/b", uid=1))
        g = infer_website(ev("https://www.countyutilities.gov/a", "https://countyutilities.gov/b",
                             uid=2, utility_type="MUNICIPAL"))
        self.assertEqual(g.status, ACCEPT)
        mark_shared_domains([a, g], {1: None, 2: None, 7: "https://www.xcelenergy.com",
                                     8: "https://countyutilities.gov"}, allow_shared=True)
        self.assertEqual((a.status, a.shared_with), (ACCEPT, (7,)))
        self.assertEqual((g.status, g.reasons), (REVIEW, ["shared_government_host"]))


class TestPlan(unittest.TestCase):
    def _accepted(self, uid, url="https://www.fpl.com"):
        return Inference(uid, ACCEPT, candidate_url=url, domain="fpl.com", support=3, eligible=3, share=1.0)

    def test_blank_only_unless_force(self):
        infs = [self._accepted(1), self._accepted(2), self._accepted(3)]
        current = {1: None, 2: "  ", 3: "https://old-site.com"}
        plan, skipped = build_plan(infs, current)
        self.assertEqual([p.utility_id for p in plan], [1, 2])
        self.assertEqual(skipped, [(3, "has_website")])
        self.assertEqual(plan[0].detail["domain"], "fpl.com")

        plan, skipped = build_plan(infs, current, force=True)
        self.assertEqual([(p.utility_id, p.old_website_url) for p in plan],
                         [(1, None), (2, "  "), (3, "https://old-site.com")])

    def test_review_and_none_are_never_planned(self):
        infs = [Inference(1, REVIEW, candidate_url="https://x.com"), Inference(2, NONE)]
        self.assertEqual(build_plan(infs, {1: None, 2: None}), ([], []))

    def test_curated_overrides_inference_and_obeys_force(self):
        infs = [self._accepted(1)]
        curated = {
            1: CuratedRow(1, "https://www.fpl-official.com", "reviewed"),
            2: CuratedRow(2, "https://coop.org"),
            3: CuratedRow(3, "https://other.com"),
            4: CuratedRow(4, "https://gone.com"),
        }
        current = {1: None, 2: None, 3: "https://set.com"}
        plan, skipped = build_plan(infs, current, curated)
        self.assertEqual([(p.utility_id, p.new_website_url, p.origin) for p in plan],
                         [(1, "https://www.fpl-official.com", "csv"), (2, "https://coop.org", "csv")])
        self.assertEqual(plan[0].detail, {"note": "reviewed"})
        self.assertEqual(sorted(skipped), [(3, "has_website"), (4, "unknown_or_inactive_utility")])
        plan, _ = build_plan(infs, current, curated, force=True)
        self.assertIn(3, [p.utility_id for p in plan])

    def test_csv_only_skips_inference(self):
        plan, _ = build_plan([self._accepted(1)], {1: None, 2: None},
                             {2: CuratedRow(2, "https://coop.org")}, csv_only=True)
        self.assertEqual([p.utility_id for p in plan], [2])

    def test_force_does_not_rewrite_identical_value(self):
        plan, skipped = build_plan([], {1: "https://coop.org"}, {1: CuratedRow(1, "https://coop.org")}, force=True)
        self.assertEqual((plan, skipped), ([], [(1, "unchanged")]))


class TestCuratedCsv(unittest.TestCase):
    def _write(self, rows, header=("utility_id", "website_url", "note")):
        fd, path = tempfile.mkstemp(suffix=".csv")
        os.close(fd)
        self.addCleanup(os.remove, path)
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(header)
            w.writerows(rows)
        return path

    def test_valid_rows_blank_skipped_scheme_added(self):
        rows, errors = load_curated_csv(self._write([
            ("10", "www.coop.org", "from annual report"), ("11", "", ""), ("12", "https://city.gov/power", ""),
        ]))
        self.assertEqual(errors, [])
        self.assertEqual(rows, {
            10: CuratedRow(10, "https://www.coop.org", "from annual report"),
            12: CuratedRow(12, "https://city.gov/power", None),
        })

    def test_rejects_blocklist_generic_publisher_bad_id_and_conflicts(self):
        _, errors = load_curated_csv(self._write([
            ("1", "https://openei.org/x", ""), ("2", "https://sites.google.com/view/coop", ""),
            ("3", "https://www.oeb.ca", ""), ("x", "https://coop.org", ""), ("5", "not a url", ""),
            ("6", "https://a.org", ""), ("6", "https://b.org", ""),
        ]))
        self.assertEqual(len(errors), 6)
        self.assertIn("third-party blocklist", errors[0])
        self.assertIn("generic file host", errors[1])
        self.assertIn("regulator publisher", errors[2])
        self.assertIn("bad utility_id", errors[3])
        self.assertIn("not a URL", errors[4])
        self.assertIn("listed twice", errors[5])

    def test_missing_columns(self):
        _, errors = load_curated_csv(self._write([("1",)], header=("utility_id",)))
        self.assertEqual(len(errors), 1)

    def test_export_round_trip_only_accepts_prefilled(self):
        acc = Inference(1, ACCEPT, candidate_url="https://www.fpl.com", domain="fpl.com", support=3, eligible=3,
                        share=1.0)
        rev = Inference(2, REVIEW, candidate_url="https://psc.gov", domain="psc.gov", reasons=["government_host"])
        rows = export_rows([acc, rev, Inference(3, NONE)], {1: {"name": "FPL"}, 2: {"name": "Co-op"}})
        self.assertEqual([r["utility_id"] for r in rows], [1, 2])
        self.assertEqual(rows[1]["website_url"], "")
        self.assertEqual(rows[1]["candidate_url"], "https://psc.gov")
        path = self._write([[r[k] for k in rows[0]] for r in rows], header=tuple(rows[0]))
        curated, errors = load_curated_csv(path)
        self.assertEqual((errors, list(curated)), ([], [1]))


class TestProjection(unittest.TestCase):
    def test_filled_website_moves_no_official_host_to_official(self):
        url = "https://www.fpl.com/rates/pdf/res.pdf"
        self.assertEqual(classify_source(url, UtilitySourceContext()).reason, "no_official_host")
        tariffs = [
            (1, url, UNKNOWN, "no_official_host"),
            (1, "https://openei.org/x", "third_party", "aggregator_blocklist"),
            (1, "https://neighbor.com/r.pdf", UNKNOWN, "no_official_host"),
            (2, url, UNKNOWN, "no_official_host"),
        ]
        out = project_source_types(tariffs, {1: UtilitySourceContext(country="US", state_province="FL")},
                                   {1: "https://www.fpl.com"})
        self.assertEqual(out, {
            (UNKNOWN, "no_official_host", OFFICIAL, "domain_match"): 1,
            ("third_party", "aggregator_blocklist", "third_party", "aggregator_blocklist"): 1,
            (UNKNOWN, "no_official_host", "third_party", "domain_mismatch"): 1,
        })


if __name__ == "__main__":
    unittest.main()
