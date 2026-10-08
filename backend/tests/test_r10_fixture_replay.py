"""Round-10 fixes — full-book FAM, Sch 100 pipe table, fetch_page, Sch 1xx all plans.

Fixtures under tests/fixtures/r10/ are the real full NSP tariff book text and
PGE Sched_100 / Sched_1xx pdftotext extracts (not FAM-section-only slices).
No live LLM calls.

    cd backend && python -m unittest tests.test_r10_fixture_replay -v
"""
from __future__ import annotations

import inspect
import json
import logging
import re
import unittest
from dataclasses import fields
from pathlib import Path
from unittest import mock

from scripts import tariff_pipeline as tp

R7 = Path(__file__).resolve().parent / "fixtures" / "r7"
R10 = Path(__file__).resolve().parent / "fixtures" / "r10"
_F = {f.name for f in fields(tp.ExtractedTariff)}

# 17 priced Sched_1xx that apply to Sch 7 (zeros / % / per-bill skipped).
_PRICED_SCH7 = [
    "102", "105", "109", "115", "120", "121", "122", "126",
    "135", "136", "137", "138", "146", "150", "151", "152", "153",
]


def _load_r7(name: str) -> dict:
    return json.loads((R7 / name).read_text())


def _mk(d: dict) -> tp.ExtractedTariff:
    return tp.ExtractedTariff(**{k: v for k, v in d.items() if k in _F})


def _energy_cents(t: tp.ExtractedTariff) -> list[float]:
    out = []
    for c in t.components or []:
        if str(c.get("component_type") or "").lower() != "energy":
            continue
        try:
            v = float(c.get("rate_value") or 0)
        except (TypeError, ValueError):
            continue
        unit = str(c.get("unit") or "")
        out.append(round(v * 100, 4) if unit.startswith("$") else round(v, 4))
    return sorted(set(out))


def _sch1xx_rider_extras(applicable: set[str]) -> list[dict]:
    extras: list[dict] = []
    for num in _PRICED_SCH7:
        if num not in applicable:
            continue
        path = R10 / f"pge_sched_{num}.txt"
        if not path.exists():
            continue
        amounts = tp.parse_pge_sch1xx_kwh_amounts_for_schedule(
            path.read_text(), schedule="7",
        )
        amounts = [
            {
                **a,
                "tier_label": f"Schedule {num} {a.get('tier_label') or ''}".strip(),
            }
            for a in amounts
            if abs(float(a.get("rate_value") or 0)) > 1e-12
            or re.search(r"\b(?:first|over)\b", str(a.get("tier_label") or ""), re.I)
        ]
        if not amounts:
            continue
        extras.append({
            "name": f"Schedule {num} Adjustment - Schedule 7 Residential",
            "customer_class": "residential",
            "rate_type": "flat",
            "code": num,
            "components": amounts,
            "extraction_tier": "rider_doc",
            "energy_scope": "bundled",
        })
    return extras


class TestNspFamFullBook(unittest.TestCase):
    """R10.1: FAM parse scoped to 2026 FAM Tariff table on the FULL book."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_full_book_domestic_0_156_murb_0_207(self):
        text = (R10 / "nsp_tariff_book_full.txt").read_text()
        self.assertGreater(len(text), 100_000)  # full book, not a slice
        fams = tp.extract_nsp_fam_aa_ba_from_text(text)
        by_name = {et.name: et for et in fams}
        domestic = by_name["Fuel Adjustment Mechanism (FAM) AA/BA - Domestic"]
        self.assertAlmostEqual(
            float(domestic.components[0]["rate_value"]), 0.156, places=3,
        )
        murb = by_name["Fuel Adjustment Mechanism (FAM) AA/BA - MURB"]
        self.assertAlmostEqual(
            float(murb.components[0]["rate_value"]), 0.207, places=3,
        )
        # Must NOT grab Domestic base energy 15.411¢ as FAM.
        for et in fams:
            for c in et.components:
                self.assertLess(
                    abs(float(c["rate_value"])), tp.RIDER_FAM_SANITY_MAX_CENTS + 0.01,
                )

    def test_domestic_equals_19_128_against_full_book(self):
        fixture = _load_r7("nsp_phase3_plus_riders.json")
        fam = tp.extract_nsp_fam_aa_ba_from_text(
            (R10 / "nsp_tariff_book_full.txt").read_text()
        )
        tariffs = (
            [_mk(t) for t in fixture["phase3_tariffs"]]
            + [_mk(t) for t in fixture.get("rider_extras") or []]
            + fam
        )
        _rep, valid = tp.phase4_validate(
            tariffs, fixture["utility_name"], fixture.get("state") or "",
        )
        domestic = next(
            t for t in valid
            if t.name == "Domestic Service Tariff"
        )
        cents = _energy_cents(domestic)
        # 18.324 + 0.648 (DCRR) + 0.156 (FAM) = 19.128¢
        self.assertTrue(
            any(abs(c - 19.128) < 0.02 for c in cents),
            f"expected ≈19.128¢, got {cents}",
        )
        self.assertFalse(
            domestic.needs_review,
            f"Domestic flagged: {domestic.missing_fields} "
            f"{domestic.computable_reasons}",
        )

    def test_fam_sanity_rejects_base_energy_as_rider(self):
        adj = {
            "component_type": "adjustment",
            "unit": "¢/kWh",
            "rate_value": 15.411,
            "tier_label": "FAM AA/BA Domestic Service",
        }
        reason = tp._rider_fails_sanity(adj, base_energy_cents=18.324)
        self.assertIsNotNone(reason)
        self.assertIn("fam_exceeds_sanity_cap", reason or "")


class TestPgeSch100PipeTable(unittest.TestCase):
    """R10.2: Sch 100 pipe-delimited grid → 17 priced Sched_1xx for Sch 7."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_full_sch100_pipe_lists_sch7_applicable(self):
        text = (R10 / "pge_sch100_full.txt").read_text()
        self.assertIn("Schs. |", text)  # pipe copy present
        nums = tp.parse_pge_sch100_applicable_schedules(text, base_schedule="7")
        expected = [
            "102", "103", "105", "106", "108", "109", "115", "118", "120",
            "121", "122", "123", "125", "126", "135", "136", "137", "138",
            "143", "145", "146", "149", "150", "151", "152", "153",
        ]
        self.assertEqual(nums, expected)
        # Column-align on the collapsed plain grid must NOT win.
        self.assertNotIn("111", nums)
        self.assertNotIn("128", nums)
        self.assertNotIn("139", nums)

    def test_seventeen_priced_sum_to_2_522(self):
        text = (R10 / "pge_sch100_full.txt").read_text()
        applicable = set(
            tp.parse_pge_sch100_applicable_schedules(text, base_schedule="7")
        )
        total = 0.0
        seen = []
        for num in _PRICED_SCH7:
            self.assertIn(num, applicable, num)
            body = (R10 / f"pge_sched_{num}.txt").read_text()
            amounts = tp.parse_pge_sch1xx_kwh_amounts_for_schedule(
                body, schedule="7",
            )
            for a in amounts:
                lab = str(a.get("tier_label") or "").lower()
                v = float(a["rate_value"])
                if "over" in lab:
                    continue  # Sch 102 Over 0.000 — first-block sum only
                total += v
                seen.append(num)
        self.assertEqual(sorted(set(seen)), sorted(_PRICED_SCH7))
        self.assertAlmostEqual(total, 2.522, places=3)


class TestPgeIndexFetchPage(unittest.TestCase):
    """R10.3: refetch uses pipeline fetch_page; import errors fail loudly."""

    def test_refetch_helper_uses_module_fetch_page(self):
        src = inspect.getsource(tp._refetch_pge_index_raw)
        self.assertIn("fetch_page(url)", src)
        self.assertNotIn("from app.services.monitor", src)
        self.assertNotIn("import fetch_page", src)

    def test_rider_fetch_does_not_import_monitor_fetch_page(self):
        src = inspect.getsource(tp.fetch_and_extract_referenced_riders)
        self.assertNotIn(
            "from app.services.monitor import fetch_page",
            src,
        )
        self.assertIn("_refetch_pge_index_raw", src)

    def test_refetch_calls_pipeline_fetch_page(self):
        with mock.patch.object(
            tp, "fetch_page", return_value=("Sched_151.pdf ctfassets", "text/html", 200)
        ) as fp:
            raw = tp._refetch_pge_index_raw("https://example.com/page-data.json")
        fp.assert_called_once()
        self.assertIn("Sched_151", raw)

    def test_missing_monitor_fetch_page_is_not_swallowed(self):
        """Regression: ImportError on monitor.fetch_page must not be caught.

        The helper must not reference a non-existent monitor symbol at all;
        if a future edit reintroduces that import inside a bare ``except``,
        this source check fails closed.
        """
        src = inspect.getsource(tp.fetch_and_extract_referenced_riders)
        # No try/except ImportError around a monitor.fetch_page import.
        self.assertFalse(
            re.search(
                r"except\s+Exception:[\s\S]{0,80}pass",
                src[src.find("_refetch_pge_index_raw"):src.find("_refetch_pge_index_raw") + 400]
                if "_refetch_pge_index_raw" in src else "",
            ),
        )


class TestPgeSch1xxAllSch7Plans(unittest.TestCase):
    """R10.4: Sch 1xx on ALL Sch 7 plans; Sch 102 first-2k only; live math."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_sch1xx_folds_onto_default_and_tou(self):
        fixture = _load_r7("pge_phase3_plus_riders.json")
        sch100 = (R10 / "pge_sch100_full.txt").read_text()
        applicable = set(
            tp.parse_pge_sch100_applicable_schedules(sch100, base_schedule="7")
        )
        rider_extras = list(fixture.get("rider_extras") or [])
        rider_extras.extend(_sch1xx_rider_extras(applicable))
        tariffs = [_mk(t) for t in fixture["phase3_tariffs"]] + [
            _mk(t) for t in rider_extras
        ]
        _rep, valid = tp.phase4_validate(
            tariffs, fixture["utility_name"], fixture.get("state") or "",
        )
        sch7_plans = [t for t in valid if re.search(r"schedule\s*7\b", t.name, re.I)]
        self.assertGreaterEqual(len(sch7_plans), 2, [t.name for t in valid])
        for t in sch7_plans:
            cents = _energy_cents(t)
            # Riders must have moved ENERGY well above base-only (~11¢ / ~4¢).
            self.assertTrue(
                max(cents) > 15,
                f"{t.name} looks base-only: {cents}",
            )

    def test_sch102_credit_first_tier_only(self):
        fixture = _load_r7("pge_phase3_plus_riders.json")
        sch100 = (R10 / "pge_sch100_full.txt").read_text()
        applicable = set(
            tp.parse_pge_sch100_applicable_schedules(sch100, base_schedule="7")
        )
        rider_extras = list(fixture.get("rider_extras") or [])
        rider_extras.extend(_sch1xx_rider_extras(applicable))
        tariffs = [_mk(t) for t in fixture["phase3_tariffs"]] + [
            _mk(t) for t in rider_extras
        ]
        _rep, valid = tp.phase4_validate(
            tariffs, fixture["utility_name"], fixture.get("state") or "",
        )
        sch7 = next(t for t in valid if "Default Plan" in t.name)
        cents = _energy_cents(sch7)
        self.assertGreaterEqual(len(cents), 2, cents)
        # Fixture base 11.224/11.946 + Sch125 5.788 + flat 3.634 −1.112 first
        # → ≈19.534 / 21.368. Gap between tiers = base gap + 1.112.
        lo, hi = min(cents), max(cents)
        self.assertTrue(19.4 < lo < 19.7, cents)
        self.assertAlmostEqual(hi - lo, (11.946 - 11.224) + 1.112, places=2)

    def test_live_base_plus_sch125_equals_19_43_20_54(self):
        """Current official math (Jul 8 2026): 11.289 + 5.619 + 2.522."""
        sch100 = (R10 / "pge_sch100_full.txt").read_text()
        applicable = set(
            tp.parse_pge_sch100_applicable_schedules(sch100, base_schedule="7")
        )
        comps = [
            {
                "component_type": "energy",
                "unit": "¢/kWh",
                "rate_value": 11.289,
                "tier_label": "First 2,000 kWh",
                "tier_min_kwh": 0,
                "tier_max_kwh": 2000,
            },
            {
                "component_type": "energy",
                "unit": "¢/kWh",
                "rate_value": 11.289,
                "tier_label": "Over 2,000 kWh",
                "tier_min_kwh": 2000,
                "tier_max_kwh": None,
            },
            {
                "component_type": "adjustment",
                "unit": "¢/kWh",
                "rate_value": 5.619,
                "tier_label": "Schedule 125 Schedule 7",
                "included_in_energy": False,
            },
        ]
        for extra in _sch1xx_rider_extras(applicable):
            comps.extend(extra["components"])
        t = tp.ExtractedTariff(
            name="Schedule 7 Residential Service Price Plan (Default Plan)",
            customer_class="residential",
            rate_type="tiered",
            code="7",
            components=comps,
            energy_scope="bundled",
        )
        _rep, valid = tp.phase4_validate([t], "Portland General Electric", "OR")
        sch7 = valid[0]
        cents = _energy_cents(sch7)
        self.assertTrue(any(abs(c - 19.43) < 0.02 for c in cents), cents)
        self.assertTrue(any(abs(c - 20.54) < 0.02 for c in cents), cents)


class TestMaxPgeSch1xxFetchBudget(unittest.TestCase):
    def test_budget_covers_seventeen_plus_sch100(self):
        self.assertGreaterEqual(tp.MAX_PGE_SCH1XX_FETCH, 18)


if __name__ == "__main__":
    unittest.main()
