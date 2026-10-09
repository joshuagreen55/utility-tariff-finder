"""R22c: Dominion Energy Virginia — 'Exhibit of Applicable Riders' expansion.

The residential schedules (1, 1G, 1P, 1S) only say "plus the riders in the
Exhibit of Applicable Riders". The exhibit lists codes; each rider's own
sheet carries the per-schedule amount. Fixture text fetched live (free HTTP)
2026-10-09 from dominionenergy.com; plans are the R20 phase-3 output."""
import dataclasses
import json
import logging
import unittest
from pathlib import Path

from app.services.rider_docs import (
    GENERIC_EXHIBIT_HINT_RE, parse_applicable_riders_exhibit, parse_rider_text,
)
from scripts import tariff_pipeline as tp

FIX = json.loads((Path(__file__).parent / "fixtures/r22/dominion_exhibit_live_2026_10_09.json").read_text())
EXHIBIT_URL = "https://www.dominionenergy.com/-/media/content/rates-and-tariffs/pdfs/virginia/shared/exhibit-of-applicable-riders.pdf"
# Schedule 1 per-kWh amounts on each rider's own sheet (¢/kWh)
SCH1 = {"A": 3.7648, "C1A": 0.0454, "C4A": 0.1322, "CERC": 0.0754, "DIST": 0.7685, "E": 0.0625, "GEN": 0.5729,
        "RGGI": 0.0, "SMR": 0.0147, "SNA": 0.4027, "T1": 1.2730, "CCR": 0.1765, "CE": 0.6054, "OSW": 1.1362, "RPS": 0.5520}
TOTAL = sum(SCH1.values()) / 100  # $/kWh = 0.095822


def _fetch(skip=()):
    def f(url):
        code = url.rsplit("rider-", 1)[-1].split(".pdf")[0]
        if code in skip or f"rider_{code}" not in FIX:
            return None
        return tp.RatePage(url=url, page_type="pdf", content=FIX[f"rider_{code}"])
    return f


def _plans():
    F = {f.name for f in dataclasses.fields(tp.ExtractedTariff)}
    out = []
    for d in FIX["phase3_tariffs"]:
        t = tp.ExtractedTariff(**{k: v for k, v in d.items() if k in F})
        for c in t.components:  # phase-3 rows are ¢/kWh; pass 0 normalises to $/kWh
            if c.get("unit") == "¢/kWh":
                c["unit"], c["rate_value"] = "$/kWh", round(c["rate_value"] / 100, 6)
        out.append(t)
    return out


def _quiet(fn, *a, **k):
    logging.disable(logging.WARNING)
    try:
        return fn(*a, **k)
    finally:
        logging.disable(logging.NOTSET)


class ExhibitParse(unittest.TestCase):
    def test_sections(self):
        secs = parse_applicable_riders_exhibit(FIX["exhibit"])
        self.assertEqual([c for c, _ in secs[0]["riders"]],
                         ["A", "C1A", "C4A", "CERC", "DIST", "E", "GEN", "RGGI", "SMR", "SNA", "T1"])
        self.assertTrue({"1", "1G", "1P", "1S"} <= secs[0]["schedules"])
        self.assertEqual(secs[1]["schedules"], {"MBR", "SCR"})
        nb = [s for s in secs if s["non_bypassable"]]
        self.assertEqual([c for c, _ in nb[0]["riders"]], ["CCR", "CE", "OSW", "RPS"])
        # Section III ('may apply based upon the circumstances': EDR, G, PIPP ...) never returned.
        self.assertFalse(any(c in ("EDR", "G", "PIPP", "TRG") for s in secs for c, _ in s["riders"]))

    def test_every_rider_sheet_reads_schedule_1(self):
        for code, cents in SCH1.items():
            p = parse_rider_text(FIX[f"rider_{code.lower()}"])
            rows = [r for r in p["per_kwh"] if not r.get("schedules") or "1" in r["schedules"]]
            self.assertEqual({round(r["rate_value"] * 100, 4) for r in rows}, {cents}, code)

    def test_generic_hints(self):
        for h in ("rider amounts", "non-bypassable charges", "rider amounts (Exhibit of Applicable Riders)"):
            self.assertTrue(GENERIC_EXHIBIT_HINT_RE.search(h), h)
        self.assertFalse(GENERIC_EXHIBIT_HINT_RE.search("Fuel Cost Recovery"))


class ExhibitFold(unittest.TestCase):
    def setUp(self):  # deterministic path only: any LLM call fails the test
        from unittest import mock

        from app.services import anthropic_compat as ac
        p = mock.patch.object(ac, "create", side_effect=AssertionError("LLM called"))
        p.start()
        self.addCleanup(p.stop)

    def _run(self, skip=()):
        page = tp.RatePage(url=EXHIBIT_URL, page_type="pdf", content=FIX["exhibit"])
        riders = _quiet(tp.expand_applicable_riders_exhibit, page, FIX["index_links"], _fetch(skip))
        plans = _plans()
        rep, valid = _quiet(tp.phase4_validate, plans + riders, "Virginia Electric & Power Co", "VA")
        return {t.name: t for t in valid}

    def test_all_residential_schedules_get_full_price(self):
        before = {t.name: sorted(c["rate_value"] for c in t.components if c["component_type"] == "energy") for t in _plans()}
        got = self._run()
        for name, vals in before.items():
            t = got[name]
            after = sorted(c["rate_value"] for c in t.components if c["component_type"] == "energy")
            for b, a in zip(vals, after):
                self.assertAlmostEqual(a, b + TOTAL, places=5, msg=name)
            self.assertNotEqual((t.confidence_notes or {}).get("price_basis"), "base_only", name)
        # Residential Service, winter over 800 kWh: 6.0261 + 9.5822 = 15.6083 ¢/kWh
        rs = sorted(c["rate_value"] for c in got["Residential Service"].components if c["component_type"] == "energy")
        self.assertAlmostEqual(rs[0], 0.156083, places=6)

    def test_missing_sheet_adds_nothing(self):
        got = self._run(skip=("rps",))
        rs = got["Residential Service"]
        self.assertAlmostEqual(min(c["rate_value"] for c in rs.components if c["component_type"] == "energy"), 0.060261, places=6)
        self.assertEqual((rs.confidence_notes or {}).get("price_basis"), "base_only")

    def test_exhibit_riders_never_name_stacked(self):
        page = tp.RatePage(url=EXHIBIT_URL, page_type="pdf", content=FIX["exhibit"])
        riders = _quiet(tp.expand_applicable_riders_exhibit, page, FIX["index_links"], _fetch())
        self.assertEqual(tp._donor_rider_candidates(riders), [])


class Labels(unittest.TestCase):
    def test_riders_not_included_is_not_an_all_in_claim(self):
        for lab in ("all-in: distribution 1.5465 + generation 3.1424 + transmission 0.970 (riders not included)",
                    "all-in: distribution 1.4111 + ES 2.4897 + transmission 0.970 (excl. riders)"):
            self.assertFalse(tp._energy_already_includes_stacking_riders({"tier_label": lab}), lab)
        self.assertTrue(tp._energy_already_includes_stacking_riders({"tier_label": "all-in incl. riders"}))


if __name__ == "__main__":
    unittest.main()
