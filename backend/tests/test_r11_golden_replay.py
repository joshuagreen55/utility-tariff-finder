"""R11 end-to-end golden: whole post-extraction pipeline → exact ¢/kWh table.

Frozen phase-3 (+ deterministic book/Sch 1xx) inputs under
``tests/fixtures/r11/golden/``. Any future change that moves a number must
update ``EXPECTED_ENERGY_CENTS.json`` explicitly.

Covers: NSP, PGE (Default / TOU / EV), PG&E, Pedernales, NL Hydro, Toronto,
BC Hydro, HQ, Hydro One. No live LLM calls.

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
    d = _load(uid)
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
        # Spot-check the R11 TOD/EV contract explicitly.
        tou = next(t for t in plans if "Time-of-Use Portfolio" in t.name)
        self.assertEqual(_energy_cents(tou), [10.169, 23.293, 36.155])
        ev = next(t for t in plans if "Electric Vehicle" in t.name)
        self.assertEqual(_energy_cents(ev), [10.169, 23.293, 36.155])
        default = next(t for t in plans if "Default Plan" in t.name)
        # Sch 102 credit through 2,000 kWh → three tier prices.
        self.assertEqual(_energy_cents(default), [19.534, 20.256, 21.368])

    def test_pge_ca_golden(self):
        _assert_expected(_PGE_CA, _run_phase4(_PGE_CA))

    def test_pedernales_golden(self):
        _assert_expected(_PED, _run_phase4(_PED))

    def test_nl_hydro_golden(self):
        _assert_expected(_NL, _run_phase4(_NL))

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


class TestR11UnitFixes(unittest.TestCase):
    """Targeted unit checks for the four R11 fix bullets."""

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

    def test_sch125_tod_not_universal_flat(self):
        adj = {
            "component_type": "adjustment",
            "unit": "¢/kWh",
            "rate_value": 13.255,
            "tier_label": "Schedule 7-TOD Schedule 125 adjustment",
            "period_label": "7-TOD On-Peak Period",
        }
        name = "Schedule 125 Net Variable Power Cost Adjustment - Schedule 7 Residential"
        self.assertFalse(
            tp._is_universal_stacking_rider(adj, tariff_name=name, allow_tod=False)
        )
        self.assertTrue(tp._is_sch125_tod_period_rider(adj, tariff_name=name))
        flat = {
            "component_type": "adjustment",
            "unit": "¢/kWh",
            "rate_value": 5.788,
            "tier_label": "Schedule 7 (Residential) Schedule 125 adjustment",
        }
        self.assertTrue(tp._is_sch125_default_flat_rider(flat, tariff_name=name))

    def test_sch1xx_fetch_budget_separate(self):
        self.assertGreaterEqual(tp.MAX_PGE_SCH1XX_FETCH, 26)

    def test_all_in_sch125_still_takes_flat_1xx(self):
        e = {"tier_label": "", "period_label": "Off-Peak (all-in +Sch125)"}
        self.assertFalse(tp._energy_already_includes_stacking_riders(e))


if __name__ == "__main__":
    unittest.main()
