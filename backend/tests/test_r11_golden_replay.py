"""R11/R12 end-to-end golden: whole post-extraction pipeline → exact ¢/kWh table.

Frozen phase-3 (+ deterministic book/Sch 1xx / Sch 7) inputs under
``tests/fixtures/r11/golden/``. Ontario LDC delivery is pinned to
``tests/fixtures/oeb/BillData.xml``. Any future change that moves a number
must update ``EXPECTED_ENERGY_CENTS.json`` explicitly.

Covers: NSP, PGE (Default / TOU — no fabricated EV), PG&E, Pedernales,
NL Hydro, Toronto, BC Hydro, HQ, Hydro One. No live LLM calls.

    cd backend && python -m unittest tests.test_r11_golden_replay -v
"""
from __future__ import annotations

import json
import logging
import re
import unittest
from dataclasses import fields
from pathlib import Path

from scripts import scrape_oeb_rates as oeb
from scripts import tariff_pipeline as tp

GOLDEN_DIR = Path(__file__).resolve().parent / "fixtures" / "r11" / "golden"
R10 = Path(__file__).resolve().parent / "fixtures" / "r10"
FIXTURES = Path(__file__).resolve().parent / "fixtures"
BILLDATA_XML = FIXTURES / "oeb" / "BillData.xml"
_F = {f.name for f in fields(tp.ExtractedTariff)}

EXPECTED = json.loads((GOLDEN_DIR / "EXPECTED_ENERGY_CENTS.json").read_text())

# Utility ids in the golden set (string keys as stored in JSON).
_NSP = "1739"
_PGE = "927"
_PGE_CA = "870"
_PED = "890"
_NL = "1742"
_TOR = "1723"
_BC = "1714"
_HQ = "1737"
_HO = "1722"


def _mk(d: dict) -> tp.ExtractedTariff:
    return tp.ExtractedTariff(**{k: v for k, v in d.items() if k in _F})


def _energy_cents(t) -> list[float]:
    out: list[float] = []
    comps = t.components if hasattr(t, "components") else t.get("components") or []
    for c in comps or []:
        if str(c.get("component_type") or "").lower() != "energy":
            continue
        try:
            v = float(c.get("rate_value") or 0)
        except (TypeError, ValueError):
            continue
        unit = str(c.get("unit") or "")
        out.append(round(v * 100, 3) if unit.startswith("$") else round(v, 3))
    return sorted(set(out))


def _load(uid: str) -> dict:
    return json.loads((GOLDEN_DIR / f"{uid}.json").read_text())


def _sch1xx_extras() -> list[tp.ExtractedTariff]:
    sch100 = (R10 / "pge_sch100_full.txt").read_text()
    applicable = set(tp.parse_pge_sch100_applicable_schedules(sch100))
    out: list[tp.ExtractedTariff] = []
    for num in (
        "102", "105", "109", "115", "120", "121", "122", "126",
        "135", "136", "137", "138", "146", "150", "151", "152", "153",
    ):
        if num not in applicable:
            continue
        body = (R10 / f"pge_sched_{num}.txt").read_text()
        amounts = [
            {
                **a,
                "tier_label": f"Schedule {num} {a.get('tier_label') or ''}".strip(),
            }
            for a in tp.parse_pge_sch1xx_kwh_amounts_for_schedule(body, schedule="7")
            if abs(float(a.get("rate_value") or 0)) > 1e-12
            or re.search(r"\b(?:first|over)\b", str(a.get("tier_label") or ""), re.I)
        ]
        if not amounts:
            continue
        out.append(_mk({
            "name": f"Schedule {num} Adjustment - Schedule 7 Residential",
            "customer_class": "residential",
            "rate_type": "flat",
            "code": num,
            "components": amounts,
            "extraction_tier": "rider_doc",
            "energy_scope": "bundled",
        }))
    return out


def _book_riders(kinds: list[str]) -> list[tp.ExtractedTariff]:
    text = (R10 / "nsp_tariff_book_full.txt").read_text()
    out: list[tp.ExtractedTariff] = []
    if "fam" in kinds:
        out.extend(tp.extract_nsp_fam_aa_ba_from_text(text))
    if "dcrr" in kinds:
        out.extend(tp.extract_nsp_dcrr_from_text(text))
    return out


def _run_phase4(uid: str) -> list[tp.ExtractedTariff]:
    d = _load(uid)
    tariffs = [_mk(t) for t in d.get("phase3_tariffs") or []]
    extras = [_mk(t) for t in d.get("rider_extras") or []]
    book = _book_riders(d.get("book_riders") or [])
    if d.get("sch1xx_from_r10"):
        extras.extend(_sch1xx_extras())
    _rep, valid = tp.phase4_validate(
        tariffs + extras + book,
        d["utility_name"],
        d.get("state") or "",
    )
    return valid


def _run_oeb(uid: str) -> list[dict]:
    """Build Ontario tariffs from golden RPP + pinned BillData LDC row."""
    d = _load(uid)
    # Prefer LDC fields from the pinned BillData.xml when present so the
    # golden stays consistent with today's feed (R12).
    ldc = None
    if BILLDATA_XML.is_file():
        rows = oeb.parse_billdata_xml(BILLDATA_XML.read_text())
        matched = oeb.match_ldc_delivery(d["utility_name"], rows)
        # Hydro One: prefer UR RESIDENTIAL (urban) when several classes match.
        if d["utility_name"].lower().startswith("hydro one") and matched:
            ur = [
                r for r in rows
                if r.distributor == matched.distributor
                and (r.rate_class or "").upper() == "UR RESIDENTIAL"
            ]
            if ur:
                matched = ur[0]
        ldc = matched
    if ldc is None:
        ldc_d = d["ldc"]
        ldc = oeb.LDCDeliveryCharges(
            **{k: ldc_d[k] for k in oeb.LDCDeliveryCharges.__dataclass_fields__ if k in ldc_d}
        )
    rpp = d["rpp"]
    rates = oeb.OEBRateSet(
        tou=oeb.TOURates(**rpp["tou"]),
        ulo=oeb.ULORates(**rpp["ulo"]),
        tiered=oeb.TieredRates(**rpp["tiered"]),
    )
    return oeb.build_tariff_entries(rates, "residential", ldc=ldc)


def _assert_expected(uid: str, plans: list) -> None:
    expected = EXPECTED[uid]
    got = {}
    for t in plans:
        name = t.name if hasattr(t, "name") else t.get("name")
        got[name] = _energy_cents(t)
    # Every expected plan must be present with exact cents.
    for name, cents in expected.items():
        self_msg = f"{uid} missing plan {name!r}; got {sorted(got)}"
        assert name in got, self_msg
        assert got[name] == cents, (
            f"{uid} {name}: expected {cents}, got {got[name]}"
        )
    # No surprise extra residential ENERGY plans (ignore exact name extras
    # only when expected is a subset — flag extras for visibility).
    extras = sorted(set(got) - set(expected))
    assert not extras, f"{uid} unexpected plans: {extras}"


class TestR11GoldenEndToEnd(unittest.TestCase):
    """One assertion table: every utility's final ENERGY ¢/kWh per plan."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_nsp_golden(self):
        _assert_expected(_NSP, _run_phase4(_NSP))

    def test_pge_or_golden(self):
        plans = _run_phase4(_PGE)
        _assert_expected(_PGE, plans)
        # R12: current Sch 7 Default (flat + Sch 102 @ 2,000 kWh) and TOD.
        tou = next(t for t in plans if "Time-of-Use Portfolio" in t.name)
        self.assertEqual(_energy_cents(tou), [11.452, 19.22, 45.653])
        default = next(t for t in plans if "Default Plan" in t.name)
        self.assertEqual(_energy_cents(default), [19.43, 20.542])
        self.assertEqual(default.rate_type, "tiered")  # Sch 102 2k split (R13)
        # No fabricated separate EV plan — whole-premise TOD covers EV-only.
        self.assertFalse(any("Electric Vehicle" in t.name for t in plans))

    def test_pge_ca_golden(self):
        _assert_expected(_PGE_CA, _run_phase4(_PGE_CA))

    def test_pedernales_golden(self):
        plans = _run_phase4(_PED)
        _assert_expected(_PED, plans)
        flat = next(t for t in plans if "Flat Base Power" in t.name)
        self.assertFalse(flat.needs_review)

    def test_nl_hydro_golden(self):
        plans = _run_phase4(_NL)
        _assert_expected(_NL, plans)
        for t in plans:
            if "1.1S" in t.name or "1.2DS" in t.name:
                self.assertFalse(
                    t.needs_review,
                    f"{t.name} still needs_review: {t.missing_fields} "
                    f"{t.riders_referenced_not_shown}",
                )

    def test_bc_hydro_golden(self):
        _assert_expected(_BC, _run_phase4(_BC))

    def test_hydro_quebec_golden(self):
        _assert_expected(_HQ, _run_phase4(_HQ))

    def test_toronto_oeb_golden(self):
        _assert_expected(_TOR, _run_oeb(_TOR))

    def test_hydro_one_oeb_golden(self):
        _assert_expected(_HO, _run_oeb(_HO))

    def test_expected_table_covers_all_utilities(self):
        self.assertEqual(
            set(EXPECTED),
            {_NSP, _PGE, _PGE_CA, _PED, _NL, _TOR, _BC, _HQ, _HO},
        )

    def test_billdata_fixture_pinned(self):
        self.assertTrue(BILLDATA_XML.is_file(), "pin tests/fixtures/oeb/BillData.xml")


class TestR12UnitFixes(unittest.TestCase):
    """Targeted unit checks for the R12 fix bullets."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_dcrr_murb_0_749_from_full_book(self):
        text = (R10 / "nsp_tariff_book_full.txt").read_text()
        rows = tp.extract_nsp_dcrr_from_text(text)
        by = {et.name: float(et.components[0]["rate_value"]) for et in rows}
        self.assertAlmostEqual(by["DSM Cost Recovery Rider (DCRR) - Domestic Service"], 0.648, places=3)
        self.assertAlmostEqual(by["DSM Cost Recovery Rider (DCRR) - MURB / General"], 0.749, places=3)

    def test_sch125_tod_deterministic(self):
        text = (R10 / "pge_sched_125.txt").read_text()
        rows = tp.parse_pge_sch125_adjustment_rates(text)
        by_period = {
            str(r.get("period_label") or r.get("tier_label")): float(r["rate_value"])
            for r in rows
        }
        self.assertAlmostEqual(by_period["Schedule 7 (Residential) Schedule 125 adjustment"], 5.619, places=3)
        self.assertAlmostEqual(by_period["7-TOD On-Peak Period"], 12.868, places=3)
        self.assertAlmostEqual(by_period["7-TOD Mid-Peak Period"], 5.555, places=3)
        self.assertAlmostEqual(by_period["7-TOD Off-Peak Period"], 3.416, places=3)
        page = tp.RatePage(
            url="https://example.com/Sched_125.pdf",
            content=text,
            title="Schedule 125",
            page_type="pdf",
        )
        det = tp._try_deterministic_pge_sch1xx_extract(page)
        self.assertIsNotNone(det)
        self.assertEqual(len(det), 1)

    def test_sch7_deterministic_no_ev(self):
        text = (R10 / "pge_sched_007.txt").read_text()
        plans = tp.extract_pge_sch7_from_text(text, source_url="https://x/Sched_007.pdf")
        names = [p.name for p in plans]
        self.assertEqual(len(plans), 2)
        self.assertTrue(any("Default Plan" in n for n in names))
        self.assertTrue(any("Time-of-Use Portfolio" in n for n in names))
        self.assertFalse(any("Electric Vehicle" in n for n in names))
        default = next(p for p in plans if "Default" in p.name)
        e = next(c for c in default.components if c["component_type"] == "energy")
        self.assertAlmostEqual(float(e["rate_value"]), 11.289, places=3)
        self.assertEqual(default.effective_date, "2026-07-08")

    def test_zero_percent_sch1xx_skips_llm_sentinel(self):
        """Recognized zero/% schedule returns [] (not None) so caller skips LLM."""
        text = (R10 / "pge_sched_103.txt").read_text() if (R10 / "pge_sched_103.txt").is_file() else (
            "SCHEDULE 103\nMSHS RATE\nThe MSHS Rate is:\n0.000% of the total billed amount\n"
        )
        page = tp.RatePage(
            url="https://example.com/Sched_103.pdf",
            content=text,
            title="Schedule 103",
            page_type="pdf",
        )
        det = tp._try_deterministic_pge_sch1xx_extract(page)
        self.assertIsNotNone(det)
        self.assertEqual(det, [])

    def test_prefer_fresher_sch7_over_combined_book(self):
        sch7 = tp.RatePage(
            url="https://assets.ctfassets.net/x/Sched_007.pdf",
            content=(R10 / "pge_sched_007.txt").read_text(),
            title="Schedule 7",
            page_type="pdf",
        )
        book = tp.RatePage(
            url="https://example.com/all_tariffs_56_.pdf",
            content=(
                "P.U.C. Oregon No. E-18\nSCHEDULE 7\nRESIDENTIAL SERVICE\n"
                "Effective for service on and after January 1, 2020\n"
                "Energy Charge 6.329 ¢ per kWh\n" * 20
            ),
            title="All Tariffs",
            page_type="pdf",
        )
        kept = tp._drop_stale_combined_pages_when_fresher_schedule_exists([sch7, book])
        urls = [p.url for p in kept]
        self.assertIn(sch7.url, urls)
        self.assertNotIn(book.url, urls)
        # Rank key: individual fresher page sorts before combined.
        ordered = sorted([sch7, book], key=tp._phase3_page_rank_key)
        self.assertEqual(ordered[0].url, sch7.url)

    def test_bare_all_in_still_takes_flat_1xx(self):
        e = {"tier_label": "", "period_label": "Off-Peak (all-in)"}
        self.assertFalse(tp._energy_already_includes_stacking_riders(e))

    def test_sch125_tod_not_universal_flat(self):
        adj = {
            "component_type": "adjustment",
            "unit": "¢/kWh",
            "rate_value": 12.868,
            "tier_label": "Schedule 7-TOD Schedule 125 adjustment",
            "period_label": "7-TOD On-Peak Period",
        }
        name = "Schedule 125 Net Variable Power Cost Adjustment - Schedule 7 Residential"
        self.assertFalse(
            tp._is_universal_stacking_rider(adj, tariff_name=name, allow_tod=False)
        )
        self.assertTrue(tp._is_sch125_tod_period_rider(adj, tariff_name=name))

    def test_sch1xx_fetch_budget_separate(self):
        self.assertGreaterEqual(tp.MAX_PGE_SCH1XX_FETCH, 26)

    def test_flat_default_splits_at_sch102_2000(self):
        """Flat Sch 7 + Sch 102 First/Over → two ENERGY tiers at 2,000 kWh."""
        default = tp.ExtractedTariff(
            name="Schedule 7 Residential Service Price Plan (Default Plan)",
            customer_class="residential",
            rate_type="flat",
            components=[{
                "component_type": "energy",
                "unit": "¢/kWh",
                "rate_value": 11.289,
                "tier_label": "all-in: transmission + distribution + energy = 11.289",
            }],
        )
        sch102 = tp.ExtractedTariff(
            name="Schedule 102 Adjustment - Schedule 7 Residential",
            customer_class="residential",
            rate_type="flat",
            code="102",
            components=[
                {
                    "component_type": "adjustment",
                    "unit": "¢/kWh",
                    "rate_value": -1.112,
                    "tier_label": "Schedule 102 Sch first 2,000 kWh",
                    "included_in_energy": False,
                },
                {
                    "component_type": "adjustment",
                    "unit": "¢/kWh",
                    "rate_value": 0.0,
                    "tier_label": "Schedule 102 Sch over 2,000 kWh",
                    "included_in_energy": False,
                },
            ],
        )
        # Minimal flat stack so First/Over is not alone.
        flat_stack = tp.ExtractedTariff(
            name="Schedule 105 Adjustment - Schedule 7 Residential",
            customer_class="residential",
            rate_type="flat",
            code="105",
            components=[{
                "component_type": "adjustment",
                "unit": "¢/kWh",
                "rate_value": 0.123,
                "tier_label": "Schedule 105 Schedule 7",
                "included_in_energy": False,
            }],
        )
        _rep, valid = tp.phase4_validate(
            [default, sch102, flat_stack],
            "Portland General Electric Co",
            "OR",
        )
        plan = next(t for t in valid if "Default" in t.name)
        cents = _energy_cents(plan)
        # 11.289 + 0.123 - 1.112 = 10.3; over = 11.289 + 0.123 = 11.412
        self.assertEqual(cents, [10.3, 11.412])


if __name__ == "__main__":
    unittest.main()
