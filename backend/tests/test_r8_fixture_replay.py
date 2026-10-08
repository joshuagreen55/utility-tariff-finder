"""Round-8 fixes — offline replay of SAVED R6/R7/R8 model outputs.

Fixtures under tests/fixtures/r7/ are slimmed phase-3 extracts (+ rider
extras) from the dry-run cache. No live LLM calls.

    cd backend && python -m unittest tests.test_r8_fixture_replay -v
"""
from __future__ import annotations

import json
import logging
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


def _phase4(fixture: dict, extra_key: str = "rider_extras"):
    tariffs = [_mk(t) for t in fixture.get("phase3_tariffs") or []]
    extras = [_mk(t) for t in fixture.get(extra_key) or []]
    return tp.phase4_validate(
        tariffs + extras,
        fixture["utility_name"],
        fixture.get("state") or "",
    )


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


class TestNLOptionalSchedulesKept(unittest.TestCase):
    """R8.1: 1.1S / 1.2DS survive — salvage before optional drop + code guard."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_1_1s_and_1_2ds_kept_with_base_energy(self):
        fixture = _load("nl_hydro_phase3.json")
        # Preconditions: saved extracts are adjustment-only before salvage.
        raw = [_mk(t) for t in fixture["phase3_tariffs"]]
        seasonal = [t for t in raw if (t.code or "") in ("1.1S", "1.2DS")]
        self.assertEqual(len(seasonal), 2)
        for t in seasonal:
            self.assertFalse(tp._tariff_has_energy(t))
            self.assertFalse(tp._is_optional_program_tariff(t))

        _rep, valid = _phase4(fixture)
        by_code = {str(t.code or ""): t for t in valid}
        self.assertIn("1.1", by_code)
        self.assertIn("1.1S", by_code)
        self.assertIn("1.2D", by_code)
        self.assertIn("1.2DS", by_code)
        # Salvaged seasonal ENERGY ≈ 15.587 ± premiums → ~14.29 / 16.54¢.
        s_cents = _energy_cents(by_code["1.1S"])
        self.assertTrue(any(14.0 < c < 15.0 for c in s_cents), s_cents)
        self.assertTrue(any(16.0 < c < 17.0 for c in s_cents), s_cents)


class TestTorontoLossFactorMetadata(unittest.TestCase):
    """R8.2: LF scales commodity only; never a priced ADJUSTMENT."""

    def test_lf_not_priced_adjustment(self):
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
        rates = oeb.OEBRateSet(
            tou=oeb.TOURates(
                effective_date="2025-11-01",
                off_peak=0.098, mid_peak=0.157, on_peak=0.203,
            ),
        )
        entries = oeb.build_tariff_entries(rates, "residential", ldc=ldc)
        tou = next(e for e in entries if e["code"] == "OEB-RPP-TOU")
        lf = float(ldc.loss_factor)
        off = next(
            c for c in tou["components"]
            if c["component_type"] == "energy" and c.get("period_label") == "Off-Peak"
        )
        self.assertAlmostEqual(
            float(off["rate_value"]),
            round(0.098 * lf + ldc.per_kwh_adder, 6),
            places=5,
        )
        adjs = [c for c in tou["components"] if c["component_type"] == "adjustment"]
        self.assertFalse(
            any(abs(float(c.get("rate_value") or 0) - lf) < 1e-9 for c in adjs)
        )
        self.assertEqual(tou["ldc_delivery"]["loss_factor"], lf)


class TestNspFamLinksAndRiders(unittest.TestCase):
    """R8.3–5: FAM one-hop from raw HTML links; DSM shared; Green Power stripped."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_fam_hub_uses_page_links_not_plain_text(self):
        html = (FIXTURES / "nsp_fam_hub.html").read_text()
        links = tp._collect_page_hrefs(
            html, "https://nspower.ca/about-us/producing/rates-tariffs/fam",
        )
        self.assertTrue(any(".pdf" in u.lower() for u in links), links)
        page = tp.RatePage(
            url="https://nspower.ca/about-us/producing/rates-tariffs/fam",
            title="Fuel Adjustment Mechanism",
            page_type="html",
            content="Fuel Adjustment Mechanism marketing copy with no hrefs.",
            links=links,
        )
        self.assertTrue(tp._page_is_rider_link_hub(page))
        hops = tp._rider_page_one_hop_links(
            page, allowed_domains={"nspower.ca"},
        )
        self.assertTrue(any("tariff-book" in u.lower() or "fam" in u.lower() for u in hops), hops)

    def test_dcrr_shared_to_all_residential_and_green_power_stripped(self):
        fixture = _load("nsp_phase3_plus_riders.json")
        self.assertEqual(len(fixture["rider_extras"]), 1)
        _rep, valid = _phase4(fixture)
        self.assertGreaterEqual(len(valid), 5)
        domestic = next(t for t in valid if t.name.startswith("Domestic Service Tariff"))
        # Green Power $5 ADJUSTMENT gone.
        labels = [
            " ".join(str(c.get(k) or "") for k in ("tier_label", "period_label"))
            for c in domestic.components
        ]
        self.assertFalse(
            any(re_search_green(lab) for lab in labels),
            labels,
        )
        # DSM/DCRR 0.648¢ folded into ENERGY (≈18.324+0.648 = 18.972¢).
        cents = _energy_cents(domestic)
        self.assertTrue(
            any(18.7 < c < 19.2 for c in cents),
            f"expected DSM-folded domestic energy, got {cents}",
        )
        # Every residential ENERGY plan received the stacking rider (or was
        # already all-in). Spot-check TOD.
        tod = next(
            t for t in valid
            if "Time-Of-Day" in t.name or "Time-of-Day" in t.name
        )
        tod_cents = _energy_cents(tod)
        # At least one period should be above the printed base (riders added).
        self.assertTrue(max(tod_cents) > 24.0 or any(
            abs(c - (24.384 + 0.648)) < 0.05 for c in tod_cents
        ), tod_cents)


def re_search_green(lab: str) -> bool:
    import re
    return bool(re.search(r"green\s+power", lab, re.I))


class TestPgeTiersSch1xxAndEv(unittest.TestCase):
    """R8.6–8: Sch 7 tiers preserved; Sch 1xx harvest; EV not merged into Portfolio."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_sch7_two_tiers_after_rider_fold(self):
        fixture = _load("pge_phase3_plus_riders.json")
        _rep, valid = _phase4(fixture)
        sch7 = next(
            t for t in valid
            if "Default Plan" in t.name and "Time-of-Use" not in t.name
        )
        cents = _energy_cents(sch7)
        # First-1,000 ≈ 17.012¢ and Over-1,000 ≈ 17.734¢ (base + Sch 125).
        self.assertGreaterEqual(len(cents), 2, cents)
        self.assertTrue(any(16.8 < c < 17.3 for c in cents), cents)
        self.assertTrue(any(17.5 < c < 18.0 for c in cents), cents)

    def test_ev_not_merged_into_tou_portfolio(self):
        fixture = _load("pge_phase3_plus_riders.json")
        ev = fixture.get("ev_plan")
        self.assertIsNotNone(ev)
        tariffs = [_mk(t) for t in fixture["phase3_tariffs"]] + [_mk(ev)]
        self.assertFalse(
            tp._full_bill_product_match(tariffs[1], _mk(ev)),
            "EV and TOU Portfolio must not product-match despite shared code 7",
        )
        out = tp._collapse_full_bill_siblings(tariffs)
        names = {t.name for t in out}
        self.assertIn("Schedule 7 Plug-In Electric Vehicle Time of Use Option (Separately Metered)", names)
        self.assertIn("Schedule 7 Time-of-Use Portfolio Option (Whole Premises)", names)

    def test_harvest_sch1xx_from_page_data(self):
        raw = (FIXTURES / "pge_tariff_page_data.json").read_text()
        urls = tp._harvest_pge_sch1xx_from_text(raw, wanted={"100", "105", "125"})
        joined = " ".join(urls).lower()
        self.assertIn("sched_100", joined)
        self.assertIn("sched_105", joined)
        self.assertIn("sched_125", joined)
        self.assertNotIn("sched_007", joined)  # filtered by wanted


class TestPgeCaClocks(unittest.TestCase):
    """R8.9: E-ELEC/EV2-A gap filled from stated partial-peak; E-TOU-C → tou_tiered."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_eelec_gap_uses_partial_peak_not_offpeak(self):
        fixture = _load("pge_ca_phase3.json")
        _rep, valid = _phase4(fixture)
        eelec = next(t for t in valid if "E-ELEC" in (t.code or t.name))
        # Off-peak must still end at 15:00 (not stretched to 16:00).
        off = [
            c for c in eelec.components
            if str(c.get("component_type")).lower() == "energy"
            and "off" in str(c.get("period_label") or "").lower()
        ]
        for c in off:
            end = str(c.get("period_end_time") or "")
            self.assertFalse(
                end.startswith("16:"),
                f"off-peak wrongly stretched to {end} on {c}",
            )
        # A partial-peak window covering 15:00–16:00 should exist after fill.
        partial = [
            c for c in eelec.components
            if str(c.get("component_type")).lower() == "energy"
            and "partial" in str(c.get("period_label") or "").lower()
        ]
        covers = False
        for c in partial:
            s = tp._hhmm_to_minutes(c.get("period_start_time"))
            e = tp._hhmm_to_minutes(c.get("period_end_time"))
            if s is not None and e is not None and s <= 15 * 60 < e:
                covers = True
                # Price should be partial-peak (39¢ summer), not off-peak 33¢.
                self.assertAlmostEqual(float(c["rate_value"]), 0.39, places=2)
                break
        self.assertTrue(covers, "expected 15:00–16:00 partial-peak fill")

    def test_etouc_reclassified_tou_tiered(self):
        fixture = _load("pge_ca_phase3.json")
        _rep, valid = _phase4(fixture)
        etouc = next(t for t in valid if "E-TOU-C" in (t.code or t.name))
        self.assertEqual(etouc.rate_type, "tou_tiered")


class TestPedernalesSampleBillAndBaseTou(unittest.TestCase):
    """R8.10: drop sample-bill flat + base-only TOU duplicate."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_sample_bill_and_base_tou_dropped(self):
        fixture = _load("pedernales_phase3.json")
        _rep, valid = _phase4(fixture)
        names = {t.name for t in valid}
        # Official flat kept; sample-bill Farm/Ranch flat dropped.
        self.assertTrue(
            any("Flat Base Power" in n for n in names),
            names,
        )
        self.assertFalse(
            any(n == "Residential & Farm/Ranch" for n in names),
            names,
        )
        # Base-only 4.35¢ TOU dropped beside coded 500.2.5.
        self.assertNotIn("Time-of-Use Rate", names)
        self.assertTrue(any("500.2.5" in (t.code or "") for t in valid))


class TestNeedsReviewHasReason(unittest.TestCase):
    """R8.11: every needs_review flag carries a Mysa-critical reason."""

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def _assert_flagged_have_reason(self, valid):
        for t in valid:
            if not t.needs_review:
                continue
            ok = tp._is_mysa_critical_review_reason(
                missing_fields=list(t.missing_fields or []),
                riders_missing=list(t.riders_referenced_not_shown or []),
                completeness_reasons=list(t.completeness_reasons or []),
                computable_reasons=list(t.computable_reasons or []),
                energy_scope=str(t.energy_scope or ""),
            )
            self.assertTrue(
                ok,
                f"{t.name} flagged needs_review with no Mysa-critical reason "
                f"(missing={t.missing_fields} computable={t.computable_reasons})",
            )

    def test_nsp_flags_have_reasons(self):
        _rep, valid = _phase4(_load("nsp_phase3_plus_riders.json"))
        self._assert_flagged_have_reason(valid)

    def test_pedernales_flags_have_reasons(self):
        _rep, valid = _phase4(_load("pedernales_phase3.json"))
        self._assert_flagged_have_reason(valid)

    def test_pge_ca_evb_not_flagged_without_reason(self):
        _rep, valid = _phase4(_load("pge_ca_phase3.json"))
        evb = next(t for t in valid if "EV-B" in (t.code or t.name))
        if evb.needs_review:
            self._assert_flagged_have_reason([evb])


if __name__ == "__main__":
    unittest.main()
