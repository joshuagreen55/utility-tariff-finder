"""R14b: referenced_riders_missing scope + Sch 102 TOD note.

1. NSP Domestic TOD with FAM+DCRR folded must NOT be flagged
   ``referenced_riders_missing`` (has-riders must see included / FAM by name
   even on Optional TOD plans).
2. Outside PGE, only per-kWh energy-price changers raise the flag — HQ
   supply credits and SRP discounts / net-metering must not.
3. PGE TOD that receives Sch 102 First credit gets a confidence_notes
   entry; needs_review is not set for that alone.

No live network, no paid LLM.

    cd backend && python -m unittest tests.test_r14b_rider_flag_scope -v
"""
from __future__ import annotations

import json
import logging
import unittest
from dataclasses import fields
from pathlib import Path

from scripts import tariff_pipeline as tp

GOLDEN = Path(__file__).resolve().parent / "fixtures" / "r11" / "golden"
R10 = Path(__file__).resolve().parent / "fixtures" / "r10"
R13 = Path(__file__).resolve().parent / "fixtures" / "r13" / "pge"
_F = {f.name for f in fields(tp.ExtractedTariff)}


def _mk(d: dict) -> tp.ExtractedTariff:
    return tp.ExtractedTariff(**{k: v for k, v in d.items() if k in _F})


def _energy_cents(t) -> list[float]:
    out: list[float] = []
    for c in t.components or []:
        if str(c.get("component_type") or "").lower() != "energy":
            continue
        try:
            v = float(c.get("rate_value") or 0)
        except (TypeError, ValueError):
            continue
        unit = str(c.get("unit") or "")
        out.append(round(v * 100, 3) if unit.startswith("$") else round(v, 3))
    return sorted(set(out))


class TestNspTodNotFlagged(unittest.TestCase):
    """NSP Domestic TOD + FAM 0.156 + DCRR 0.648 → 25.188¢, unflagged."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_domestic_tod_unflagged_with_fam_dcrr(self):
        d = json.loads((GOLDEN / "1739.json").read_text())
        tariffs = [_mk(t) for t in d["phase3_tariffs"]]
        book_text = (R10 / "nsp_tariff_book_full.txt").read_text()
        book = list(tp.extract_nsp_fam_aa_ba_from_text(book_text))
        book.extend(tp.extract_nsp_dcrr_from_text(book_text))
        _rep, valid = tp.phase4_validate(
            tariffs + book, d["utility_name"], d["state"],
        )
        tod = next(t for t in valid if "Time-Of-Day" in t.name)
        self.assertIn(25.188, _energy_cents(tod))
        self.assertNotIn("referenced_riders_missing", tod.missing_fields or [])
        # May still have informational effective_date — not this flag.
        self.assertFalse(
            "referenced_riders_missing" in (tod.missing_fields or [])
        )
        # Has-riders must see folded FAM/DCRR on Optional TOD.
        self.assertTrue(tp._tariff_has_sch1xx_stacking_riders(tod))


class TestHqCreditsIgnored(unittest.TestCase):
    """HQ Rate D supply credits must not raise referenced_riders_missing."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_rate_d_credit_not_referenced_riders_missing(self):
        d = json.loads((GOLDEN / "1737.json").read_text())
        tariffs = [_mk(t) for t in d["phase3_tariffs"]]
        extras = [_mk(t) for t in d.get("rider_extras") or []]
        _rep, valid = tp.phase4_validate(
            tariffs + extras, d["utility_name"], d["state"],
        )
        rate_d = next(t for t in valid if t.name == "Rate D")
        self.assertNotIn("referenced_riders_missing", rate_d.missing_fields or [])
        for t in valid:
            self.assertNotIn(
                "referenced_riders_missing",
                t.missing_fields or [],
                msg=f"{t.name} should not flag credits as missing riders",
            )
        self.assertFalse(
            tp._is_per_kwh_energy_price_rider_hint("Credit for supply (Article 12.3)")
        )


class TestSrpE22DiscountsIgnored(unittest.TestCase):
    """SRP E-22 discount / net-metering hints must not raise the flag."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_e22_optional_riders_not_flagged(self):
        e22 = tp.ExtractedTariff(
            name="E-22 Residential Time-of-Use Price Plan (4-7 p.m. On-Peak)",
            code="E-22",
            customer_class="residential",
            rate_type="seasonal_tou",
            description=(
                "SRP residential TOU plan with on-peak 4-7 p.m. weekdays; "
                "summer, summer peak (Jul-Aug) and winter prices."
            ),
            riders_referenced_not_shown=[
                "Economy Discount Rider",
                "Medical Life Support Equipment Discount Rider",
                "Energy Attribute Certificate Rider",
                "Renewable Net Metering Rider",
                "Carbon Reduction Rider",
            ],
            missing_fields=[
                "effective date as full date",
                "season calendar dates",
            ],
            needs_review=True,
            components=[
                {
                    "component_type": "energy",
                    "unit": "¢/kWh",
                    "rate_value": 30.87,
                    "period_label": "On-Peak",
                    "period_start_time": "16:00",
                    "period_end_time": "19:00",
                    "day_type": "weekday",
                    "season": "summer",
                    "season_start_month": 5,
                    "season_start_day": 1,
                    "season_end_month": 10,
                    "season_end_day": 31,
                },
                {
                    "component_type": "energy",
                    "unit": "¢/kWh",
                    "rate_value": 9.64,
                    "period_label": "Off-Peak",
                    "period_start_time": "19:00",
                    "period_end_time": "16:00",
                    "day_type": "weekday",
                    "season": "summer",
                    "season_start_month": 5,
                    "season_start_day": 1,
                    "season_end_month": 10,
                    "season_end_day": 31,
                },
            ],
        )
        self.assertEqual(tp._declared_per_kwh_price_riders(e22), [])
        _rep, valid = tp.phase4_validate(
            [e22], "Salt River Project", "AZ",
        )
        plan = next(t for t in valid if "E-22" in t.name)
        self.assertNotIn("referenced_riders_missing", plan.missing_fields or [])


class TestSch102TodConfidenceNote(unittest.TestCase):
    """PGE TOD keeps Sch 102 First credit; note it, don't needs_review."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_tod_sch102_note_without_review_flag(self):
        tou = tp.ExtractedTariff(
            name="Schedule 7 Time-of-Use Portfolio Option (Whole Premises)",
            customer_class="residential",
            rate_type="tou",
            riders_referenced_not_shown=["Schedule 100"],
            components=[
                {
                    "component_type": "energy", "unit": "¢/kWh", "rate_value": 30.263,
                    "period_label": "On-Peak", "period_start_time": "17:00",
                    "period_end_time": "21:00", "day_type": "weekday",
                },
                {
                    "component_type": "energy", "unit": "¢/kWh", "rate_value": 11.143,
                    "period_label": "Mid-Peak", "period_start_time": "07:00",
                    "period_end_time": "17:00", "day_type": "weekday",
                },
                {
                    "component_type": "energy", "unit": "¢/kWh", "rate_value": 5.514,
                    "period_label": "Off-Peak", "period_start_time": "21:00",
                    "period_end_time": "07:00", "day_type": "weekday",
                },
            ],
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
                },
                {
                    "component_type": "adjustment",
                    "unit": "¢/kWh",
                    "rate_value": 0.0,
                    "tier_label": "Schedule 102 Sch over 2,000 kWh",
                },
            ],
        )
        sch105 = tp.ExtractedTariff(
            name="Schedule 105 Adjustment - Schedule 7 Residential",
            customer_class="residential",
            rate_type="flat",
            code="105",
            components=[{
                "component_type": "adjustment",
                "unit": "¢/kWh",
                "rate_value": 0.123,
                "tier_label": "Schedule 105 Schedule 7",
            }],
        )
        text = (R13 / "Sched_125.txt").read_text()
        sch125 = tp._try_deterministic_pge_sch1xx_extract(
            tp.RatePage(url="https://x/Sched_125.pdf", content=text, page_type="pdf")
        )
        useful = tp._filter_useful_rider_extracts(list(sch125 or []))
        _rep, valid = tp.phase4_validate(
            [tou, sch102, sch105] + useful,
            "Portland General Electric Co",
            "OR",
        )
        plan = next(t for t in valid if "Time-of-Use" in t.name)
        notes = getattr(plan, "confidence_notes", None) or {}
        self.assertIn("sch102_credit_first_2000_kwh_only", notes)
        self.assertIn("1.112", str(notes["sch102_credit_first_2000_kwh_only"]))
        self.assertNotIn("sch102_credit_first_2000_kwh_only", plan.missing_fields or [])
        # Sch 102 note alone must not force needs_review; PCA present → clean.
        self.assertNotIn("sch125_tod_pca_missing", plan.missing_fields or [])


class TestHintClassifier(unittest.TestCase):
    def test_price_vs_credit_hints(self):
        self.assertTrue(
            tp._is_per_kwh_energy_price_rider_hint("Fuel Adjustment Mechanism (FAM)")
        )
        self.assertTrue(tp._is_per_kwh_energy_price_rider_hint("Schedule 125"))
        self.assertTrue(tp._is_per_kwh_energy_price_rider_hint("DSM Cost Recovery Rider"))
        self.assertFalse(
            tp._is_per_kwh_energy_price_rider_hint("Credit for supply (Article 12.3)")
        )
        self.assertFalse(tp._is_per_kwh_energy_price_rider_hint("Economy Discount Rider"))
        self.assertFalse(
            tp._is_per_kwh_energy_price_rider_hint("Renewable Net Metering Rider")
        )


if __name__ == "__main__":
    unittest.main()
