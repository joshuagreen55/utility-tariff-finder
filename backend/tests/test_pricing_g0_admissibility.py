"""G0 admissibility from the utility's known official site, not fetched URLs."""
from __future__ import annotations

import json
import unittest

from app.services.pricing.official_sites import official_context
from app.services.pricing.preaccept import gate_admissibility
from app.services.pricing.replay import DEFAULT_FIXTURE_DIR
from app.services.pricing.compiler import GOLDEN_DIR
from app.services.source_type import UtilitySourceContext


def _g0(url, ctx=None, hosts=()):
    return [(f.reason, f.detail) for f in gate_admissibility(
        source_url=url, official_hosts=hosts, source_ctx=ctx,
    )]


class TestGateAdmissibility(unittest.TestCase):
    def setUp(self):
        self.dominion = UtilitySourceContext(
            website_url="https://www.dominionenergy.com", country="US", state_province="VA",
        )

    def test_nothing_known_holds(self):
        self.assertEqual(
            _g0("https://www.example-utility.com/tariff.pdf"),
            [("no_official_host", "www.example-utility.com")],
        )
        site_unknown = UtilitySourceContext(country="US", state_province="VA")
        self.assertEqual(
            _g0("https://www.example-utility.com/tariff.pdf", site_unknown)[0][0],
            "no_official_host",
        )

    def test_utility_domain_admitted(self):
        self.assertEqual(
            _g0("https://cdn.dominionenergy.com/rates/schedule-1.pdf", self.dominion), [],
        )

    def test_other_utility_domain_held(self):
        self.assertEqual(
            _g0("https://www.vpuc.com/products/rates/", self.dominion),
            [("domain_not_official", "www.vpuc.com:domain_mismatch")],
        )

    def test_government_docket_copy_held(self):
        ga = official_context("Georgia Power")
        self.assertEqual(
            _g0("https://psc.ga.gov/utilities/electric/residential-rate-survey/", ga),
            [("domain_not_official", "psc.ga.gov:government_host")],
        )

    def test_blocklisted_aggregator_held_even_if_listed(self):
        self.assertEqual(
            _g0("https://www.quickelectricity.com/oncor", hosts=["quickelectricity.com"])[0][0],
            "third_party_blocked",
        )

    def test_regulator_publisher_only_in_its_jurisdiction(self):
        oeb = "https://www.oeb.ca/consumer-information-and-protection/electricity-rates"
        self.assertEqual(_g0(oeb, official_context("Toronto Hydro")), [])
        self.assertEqual(
            _g0(oeb, official_context("FortisAlberta"))[0][0], "domain_not_official",
        )
        self.assertEqual(_g0("https://www.auc.ab.ca/rro", official_context("FortisAlberta")), [])

    def test_state_supply_publisher_admitted(self):
        comed = UtilitySourceContext(
            website_url="https://www.comed.com", country="US", state_province="IL",
        )
        self.assertEqual(_g0("https://www.pluginillinois.org/FixedRateBreakdownComEd.aspx", comed), [])

    def test_generic_host_needs_configured_prefix(self):
        pge = official_context("Portland General Electric")
        own = ("https://assets.ctfassets.net/416ywc1laqmd/3zEUpUmuwinmwjn08u9kad/"
               "83c57cdcf033ecd58c4900a930584ea0/2026-6-1-standard-service-schedules.pdf")
        other = "https://assets.ctfassets.net/zzzzzzzzzzzz/abc/rates.pdf"
        self.assertEqual(_g0(own, pge), [])
        self.assertEqual(
            _g0(other, pge), [("domain_not_official", "assets.ctfassets.net:generic_host")],
        )

    def test_explicit_official_hosts_still_honoured(self):
        self.assertEqual(_g0("https://www.nspower.ca/rates.pdf", hosts=["nspower.ca"]), [])
        self.assertEqual(
            _g0("https://www.other.ca/rates.pdf", hosts=["nspower.ca"]),
            [("domain_not_allowlisted", "www.other.ca")],
        )


class TestOfficialSitesFixture(unittest.TestCase):
    def test_every_golden_and_replay_utility_is_on_file(self):
        plans = json.loads((GOLDEN_DIR / "plans.json").read_text())["plans"]
        names = {p["utility_name"] for p in plans}
        for line in (DEFAULT_FIXTURE_DIR / "plans.jsonl").read_text().splitlines():
            if line.strip():
                names.add(json.loads(line)["utility"])
        missing = sorted(n for n in names if official_context(n) is None)
        self.assertEqual(missing, [])

    def test_golden_primary_sources_admitted(self):
        plans = json.loads((GOLDEN_DIR / "plans.json").read_text())["plans"]
        held = [
            (p["plan_key"], _g0(p["source_url"], official_context(p["utility_name"])))
            for p in plans
            if _g0(p["source_url"], official_context(p["utility_name"]))
        ]
        self.assertEqual(held, [])

    def test_unknown_utility_has_no_context(self):
        self.assertIsNone(official_context("Not A Real Utility Co"))


if __name__ == "__main__":
    unittest.main()
