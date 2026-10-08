"""Round-8b fixes — FAM from tariff book, PGE Sch 100 map, review scope, ON LF.

Fixtures under tests/fixtures/r7/ include pdftotext extracts of the NSP FAM
tariff section and PGE Sched_100 / Sched_1xx rate tables. No live LLM calls.

    cd backend && python -m unittest tests.test_r8b_fixture_replay -v
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

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "r7"
_F = {f.name for f in fields(tp.ExtractedTariff)}


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


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


class TestNspFamFromTariffBook(unittest.TestCase):
    """R8b.1: FAM AA/BA 0.156¢ from already-fetched tariff book section."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_parse_fam_aa_ba_from_book_text(self):
        text = (FIXTURES / "nsp_fam_tariff_section.txt").read_text()
        fams = tp.extract_nsp_fam_aa_ba_from_text(text)
        self.assertGreaterEqual(len(fams), 1)
        vals = []
        for et in fams:
            for c in et.components:
                if str(c.get("component_type")).lower() == "adjustment":
                    vals.append(float(c["rate_value"]))
        self.assertTrue(any(abs(v - 0.156) < 1e-9 for v in vals), vals)

    def test_domestic_equals_19_128_with_fam_and_dcrr(self):
        fixture = _load("nsp_phase3_plus_riders.json")
        fam_text = (FIXTURES / "nsp_fam_tariff_section.txt").read_text()
        fam_extras = tp.extract_nsp_fam_aa_ba_from_text(fam_text)
        for et in fam_extras:
            et.source_url = (
                "https://www.nspower.ca/docs/default-source/regulatory/"
                "tariff-book-2026.pdf"
            )
        tariffs = (
            [_mk(t) for t in fixture["phase3_tariffs"]]
            + [_mk(t) for t in fixture.get("rider_extras") or []]
            + fam_extras
        )
        # Book-path helper also finds FAM when the page is already in hand.
        book_page = tp.RatePage(
            url=fam_extras[0].source_url,
            title="NSP Tariff Book 2026",
            page_type="pdf",
            content=fam_text,
        )
        from_book = tp.extract_riders_from_main_tariff_book(
            [book_page],
            hints=["Fuel Adjustment Mechanism FAM"],
        )
        self.assertGreaterEqual(len(from_book), 1)

        _rep, valid = tp.phase4_validate(
            tariffs, fixture["utility_name"], fixture.get("state") or "",
        )
        domestic = next(t for t in valid if t.name.startswith("Domestic Service Tariff"))
        cents = _energy_cents(domestic)
        # 18.324 + 0.648 (DCRR) + 0.156 (FAM) = 19.128¢
        self.assertTrue(
            any(abs(c - 19.128) < 0.02 for c in cents),
            f"expected ≈19.128¢, got {cents}",
        )


class TestPgeSch100MapAndAmounts(unittest.TestCase):
    """R8b.2: Sch 100 applicability → fetch priced Sched_1xx; fold onto Sch 7."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_sch100_lists_sch7_applicable(self):
        text = (FIXTURES / "pge_sch100.txt").read_text()
        nums = tp.parse_pge_sch100_applicable_schedules(text, base_schedule="7")
        self.assertIn("102", nums)
        self.assertIn("105", nums)
        self.assertIn("109", nums)
        self.assertIn("125", nums)
        self.assertIn("151", nums)
        self.assertNotIn("100", nums)  # map itself
        self.assertGreaterEqual(len(nums), 20)

    def test_parse_sch1xx_amounts_for_schedule_7(self):
        excerpts = (FIXTURES / "pge_sch1xx_rate_excerpts.txt").read_text()
        # Sch 109 → 1.004¢; Sch 102 → First (1.112) credit.
        parts = re.split(r"===== SCHEDULE (\d+) =====", excerpts)
        by_num = {}
        for i in range(1, len(parts), 2):
            by_num[parts[i]] = parts[i + 1]
        a109 = tp.parse_pge_sch1xx_kwh_amounts_for_schedule(by_num["109"], schedule="7")
        self.assertEqual(len(a109), 1)
        self.assertAlmostEqual(float(a109[0]["rate_value"]), 1.004, places=3)
        a102 = tp.parse_pge_sch1xx_kwh_amounts_for_schedule(by_num["102"], schedule="7")
        self.assertEqual(len(a102), 2)
        first = next(c for c in a102 if "first" in c["tier_label"].lower())
        self.assertAlmostEqual(float(first["rate_value"]), -1.112, places=3)

    def test_sch7_near_live_after_sch1xx_fold(self):
        fixture = _load("pge_phase3_plus_riders.json")
        excerpts = (FIXTURES / "pge_sch1xx_rate_excerpts.txt").read_text()
        sch100 = (FIXTURES / "pge_sch100.txt").read_text()
        applicable = set(
            tp.parse_pge_sch100_applicable_schedules(sch100, base_schedule="7")
        )
        parts = re.split(r"===== SCHEDULE (\d+) =====", excerpts)
        rider_extras = list(fixture.get("rider_extras") or [])
        # Build deterministic rider extracts for every applicable Sched_1xx
        # with a non-zero Sch 7 amount (skip 125 — already in fixture extras).
        for i in range(1, len(parts), 2):
            num, body = parts[i], parts[i + 1]
            if num not in applicable or num == "125":
                continue
            amounts = tp.parse_pge_sch1xx_kwh_amounts_for_schedule(body, schedule="7")
            amounts = [
                {**a, "tier_label": f"Schedule {num} {a.get('tier_label') or ''}".strip()}
                for a in amounts
                if abs(float(a.get("rate_value") or 0)) > 1e-12
                or re.search(r"\b(?:first|over)\b", str(a.get("tier_label") or ""), re.I)
            ]
            if not amounts:
                continue
            rider_extras.append({
                "name": f"Schedule {num} Adjustment - Schedule 7 Residential",
                "customer_class": "residential",
                "rate_type": "flat",
                "code": num,
                "components": amounts,
                "extraction_tier": "rider_doc",
                "energy_scope": "bundled",
            })
        tariffs = [_mk(t) for t in fixture["phase3_tariffs"]] + [
            _mk(t) for t in rider_extras
        ]
        _rep, valid = tp.phase4_validate(
            tariffs, fixture["utility_name"], fixture.get("state") or "",
        )
        sch7 = next(
            t for t in valid
            if "Default Plan" in t.name and "Time-of-Use" not in t.name
        )
        cents = _energy_cents(sch7)
        self.assertGreaterEqual(len(cents), 2, cents)
        # Sch 125 + Sch 100-mapped 1xx, with Sch 102 First credit on the
        # bottom tier only (Over 0.000). First ≈19.5¢; Over is First plus
        # the base tier gap (+1.112¢ from Sch 102).
        lo, hi = min(cents), max(cents)
        self.assertTrue(19.4 < lo < 19.7, cents)
        self.assertAlmostEqual(hi - lo, (11.946 - 11.224) + 1.112, delta=0.05)


class TestNeedsReviewScope(unittest.TestCase):
    """R8b.3: warnings scoped to owning plan; fixed-only gaps not critical."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_dcrr_label_does_not_mark_domestic_as_event(self):
        from app.services.computable import evaluate_computable

        comps = [
            {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.18324},
            {
                "component_type": "adjustment",
                "unit": "$/kWh",
                "rate_value": 0.00648,
                "tier_label": (
                    "DCRR Domestic Service, Time-of-Day, Time of Use, "
                    "Critical Peak Pricing"
                ),
            },
        ]
        res = evaluate_computable("flat", comps, name="Domestic Service Tariff")
        self.assertNotIn("event_pricing_unsupported", res.reasons)

    def test_nsp_non_cpp_not_flagged_for_event(self):
        fixture = _load("nsp_phase3_plus_riders.json")
        fam = tp.extract_nsp_fam_aa_ba_from_text(
            (FIXTURES / "nsp_fam_tariff_section.txt").read_text()
        )
        tariffs = (
            [_mk(t) for t in fixture["phase3_tariffs"]]
            + [_mk(t) for t in fixture.get("rider_extras") or []]
            + fam
        )
        _rep, valid = tp.phase4_validate(
            tariffs, fixture["utility_name"], fixture.get("state") or "",
        )
        domestic = next(t for t in valid if t.name == "Domestic Service Tariff")
        # FAM+DSM resolved → no rider-missing reason; flat plan computable.
        self.assertFalse(
            domestic.needs_review,
            f"Domestic flagged: missing={domestic.missing_fields} "
            f"riders={domestic.riders_referenced_not_shown} "
            f"reasons={domestic.computable_reasons}",
        )
        tod = next(t for t in valid if "Time-Of-Day" in t.name)
        self.assertNotIn(
            "event_pricing_unsupported",
            list(tod.computable_reasons or []),
        )

    def test_eelec_fixed_charge_gap_not_critical(self):
        fixture = _load("pge_ca_phase3.json")
        _rep, valid = tp.phase4_validate(
            [_mk(t) for t in fixture["phase3_tariffs"]],
            fixture["utility_name"],
            fixture.get("state") or "",
        )
        eelec = next(t for t in valid if "E-ELEC" in (t.code or t.name))
        self.assertFalse(
            eelec.needs_review,
            f"E-ELEC flagged for {eelec.missing_fields} / {eelec.computable_reasons}",
        )
        self.assertTrue(
            tp._is_informational_missing_field("Base Services Charge amount")
        )


class TestOntarioLfOnTransmissionRegulatory(unittest.TestCase):
    """R8b.4: LF scales transmission + regulatory (≈0.08¢), not just commodity."""

    def test_lf_scales_transmission_and_regulatory(self):
        fixture = _load("toronto_billdata_ldc.json")
        ldc_d = fixture["ldc"]
        ldc = oeb.LDCDeliveryCharges(
            distributor=ldc_d["distributor"],
            rate_class=ldc_d["rate_class"],
            service_charge=ldc_d.get("service_charge"),
            distribution_kwh=ldc_d.get("distribution_kwh"),
            transmission_network=ldc_d.get("transmission_network"),
            transmission_connection=ldc_d.get("transmission_connection"),
            wholesale_market=ldc_d.get("wholesale_market"),
            rural_remote=ldc_d.get("rural_remote"),
            sss_admin=ldc_d.get("sss_admin"),
            other_fixed=ldc_d.get("other_fixed"),
            loss_factor=ldc_d.get("loss_factor"),
            year=ldc_d.get("year"),
            source_url=ldc_d.get("source_url"),
        )
        lf = float(ldc.loss_factor)
        # ≈0.08¢ from (Net+Conn+WMSR+RRRP)×(LF−1)
        loss_extra = ldc.loss_sensitive_kwh * (lf - 1.0)
        self.assertAlmostEqual(loss_extra * 100, 0.0817, places=2)

        rates = oeb.OEBRateSet(
            tou=oeb.TOURates(
                effective_date="2025-11-01",
                off_peak=0.098, mid_peak=0.157, on_peak=0.203,
            ),
        )
        entries = oeb.build_tariff_entries(rates, "residential", ldc=ldc)
        tou = next(e for e in entries if e["code"] == "OEB-RPP-TOU")
        off = next(
            c for c in tou["components"]
            if c["component_type"] == "energy" and c.get("period_label") == "Off-Peak"
        )
        expected = round(0.098 * lf + ldc.per_kwh_adder, 6)
        self.assertAlmostEqual(float(off["rate_value"]), expected, places=5)
        # Adder includes LF on transmission+regulatory (not bare sum).
        bare = (
            float(ldc.distribution_kwh or 0)
            + ldc.transmission_kwh
            + ldc.regulatory_kwh
        )
        self.assertGreater(ldc.per_kwh_adder, bare)
        self.assertAlmostEqual(
            ldc.per_kwh_adder,
            float(ldc.distribution_kwh or 0) + ldc.loss_sensitive_kwh * lf,
            places=6,
        )


if __name__ == "__main__":
    unittest.main()
