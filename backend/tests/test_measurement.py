"""Measurement fixes (audit F7 / F8): cache keys, yield, metering, auditor, gold set.

    cd backend && python -m unittest tests.test_measurement -v
"""
from __future__ import annotations

import copy
import importlib
import json
import os
import tempfile
import unittest
from datetime import time
from types import SimpleNamespace
from unittest import mock

from scripts import benchmark, llm_cost, llm_cost_report
from scripts import repair_hydro_one_oeb_residential as ho
from scripts import repair_nf_seasonal_11s as nf
from scripts import repair_ns_power_residential_2026 as ns
from scripts import tariff_pipeline as tp


class TestModelAwareCache(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        for name, value in (("LLM_CACHE_DIR", self.tmp.name), ("_LLM_CACHE_LEGACY_READ", False)):
            patcher = mock.patch.object(tp, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_env_model_swap_does_not_replay_other_model_output(self):
        with mock.patch.object(tp, "OPUS_MODEL", "claude-opus-5"):
            tp._set_llm_cache("abc", "opus", [{"name": "from opus 5"}])
            self.assertEqual(tp._get_llm_cache("abc", "opus"), [{"name": "from opus 5"}])
        with mock.patch.object(tp, "OPUS_MODEL", "claude-opus-next"):
            self.assertIsNone(tp._get_llm_cache("abc", "opus"))

    def test_twopass_key_tracks_both_models(self):
        with mock.patch.object(tp, "HAIKU_MODEL", "h1"), mock.patch.object(tp, "OPUS_MODEL", "o1"):
            a = tp._llm_cache_path("abc", "twopass")
        with mock.patch.object(tp, "HAIKU_MODEL", "h1"), mock.patch.object(tp, "OPUS_MODEL", "o2"):
            b = tp._llm_cache_path("abc", "twopass")
        self.assertNotEqual(a, b)

    def test_legacy_entries_only_read_when_opted_in(self):
        legacy = tp._llm_cache_path("abc", "haiku", legacy=True)
        os.makedirs(os.path.dirname(legacy), exist_ok=True)
        with open(legacy, "w") as fh:
            json.dump([{"name": "legacy"}], fh)
        self.assertIsNone(tp._get_llm_cache("abc", "haiku"))
        with mock.patch.object(tp, "_LLM_CACHE_LEGACY_READ", True):
            self.assertEqual(tp._get_llm_cache("abc", "haiku"), [{"name": "legacy"}])


class TestYieldAndMetering(unittest.TestCase):
    def setUp(self):
        llm_cost.reset()

    def test_phase4_records_accepted_after_validation_per_tier(self):
        good = tp.ExtractedTariff(
            name="Residential", customer_class="residential", rate_type="flat",
            components=[{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.15}],
            extraction_tier="opus",
        )
        rider_only = tp.ExtractedTariff(
            name="Storm Rider", customer_class="residential", rate_type="flat",
            components=[{"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.01}],
            extraction_tier="opus",
        )
        tp.phase4_validate([good, rider_only], "Utility", "NY")
        self.assertEqual(llm_cost.summary()["tier_acceptance"]["opus"], {"returned": 2, "accepted": 1})

    def test_merge_carries_acceptance_and_aborts(self):
        llm_cost.record_tier_acceptance(["haiku", "haiku"], ["haiku"])
        llm_cost.record_abort("gemini_dr", priced=False)
        merged = llm_cost.merge_summaries([llm_cost.summary(), llm_cost.summary()])
        self.assertEqual(merged["tier_acceptance"]["haiku"], {"returned": 4, "accepted": 2})
        self.assertEqual(merged["aborts"]["gemini_dr"], {"aborted": 2, "unpriced": 2})

    def test_phase6_abort_is_priced_from_partial_usage(self):
        stats = {"phase6_status": "token_cap", "phase6_total_tokens": 0,
                 "phase6_input_tokens": 0, "phase6_output_tokens": 0}
        interaction = SimpleNamespace(usage=SimpleNamespace(
            total_tokens=3_100_000, total_input_tokens=3_000_000, total_output_tokens=100_000,
        ))
        tp._phase6_meter(interaction, stats)
        s = llm_cost.summary()
        self.assertGreater(s["by_model"]["gemini_dr"], 0)
        self.assertEqual(s["aborts"]["gemini_dr"], {"aborted": 1, "unpriced": 0})

    def test_phase6_timeout_without_usage_counts_unpriced(self):
        stats = {"phase6_status": "client_timeout", "phase6_total_tokens": 0,
                 "phase6_input_tokens": 0, "phase6_output_tokens": 0}
        tp._phase6_meter(SimpleNamespace(usage=None), stats)
        self.assertEqual(llm_cost.summary()["aborts"]["gemini_dr"], {"aborted": 1, "unpriced": 1})

    def test_track_b_pairing_is_metered(self):
        from scripts import supersede_via_llm

        block = SimpleNamespace(type="tool_use", name="report_pairings", input={"pairings": []})
        resp = SimpleNamespace(content=[block], usage=SimpleNamespace(
            input_tokens=1000, output_tokens=200, cache_read_input_tokens=0,
            cache_creation_input_tokens=0,
        ))
        client = SimpleNamespace(messages=SimpleNamespace(create=lambda **kw: resp))
        stranded = [{"id": 1, "name": "x", "customer_class": "residential", "rate_type": "flat"}]
        supersede_via_llm._pair_one_utility(client, "U", "NS", [], stranded)
        self.assertIn("haiku", llm_cost.summary()["detail"]["trackb"])

    def test_report_excludes_identify_from_wasted_opus(self):
        cost = {"by_model": {"opus": 10.0}, "detail": {
            "phase3": {"opus": {"cost": 4.0}}, "phase3_identify": {"opus": {"cost": 6.0}},
        }}
        self.assertEqual(llm_cost_report._opus_escalation_cost(cost), 4.0)

    def test_script_ledger_round_trip(self):
        with tempfile.TemporaryDirectory() as d, mock.patch.dict(os.environ, {"APP_LOG_DIR": d}):
            llm_cost.record_manual("haiku", 1_000_000, 0)
            llm_cost.append_ledger("trackb")
            rows = llm_cost.read_ledger(since_days=1)
        self.assertEqual([r["source"] for r in rows], ["trackb"])
        self.assertAlmostEqual(rows[0]["cost"]["total_usd"], 1.0)


class TestAuditorAndCliModels(unittest.TestCase):
    def test_auditor_model_comes_from_env(self):
        from scripts import opus_audit

        with mock.patch.dict(os.environ, {"OPUS_MODEL": "claude-opus-x", "AUDITOR_MODEL": ""}):
            self.assertEqual(importlib.reload(opus_audit).OPUS_MODEL, "claude-opus-x")
        with mock.patch.dict(os.environ, {"AUDITOR_MODEL": "claude-opus-audit"}):
            self.assertEqual(importlib.reload(opus_audit).OPUS_MODEL, "claude-opus-audit")
        importlib.reload(opus_audit)
        self.assertNotIn("20250514", opus_audit.OPUS_MODEL)

    def test_auditor_sees_structured_tou_season_fields(self):
        from scripts import opus_audit

        rc = SimpleNamespace(
            component_type=SimpleNamespace(value="energy"), unit="$/kWh", rate_value=0.203,
            tier_label=None, tier_min_kwh=None, tier_max_kwh=None, period_label="On-Peak",
            period_start_time=time(7, 0), period_end_time=time(11, 0), day_type="weekday",
            season="Winter", season_start_month=11, season_start_day=1,
            season_end_month=4, season_end_day=30, included_in_energy=False,
        )
        line = opus_audit._format_component(rc)
        for fragment in ("clock: 07:00-11:00", "days: weekday", "season dates: 11/1-4/30"):
            self.assertIn(fragment, line)

    def test_browser_cli_model_comes_from_env(self):
        from scripts import browser_interaction

        with mock.patch.dict(os.environ, {"HAIKU_MODEL": "claude-haiku-next"}):
            self.assertEqual(importlib.reload(browser_interaction).LLM_MODEL, "claude-haiku-next")
        importlib.reload(browser_interaction)
        self.assertNotIn("3-5-haiku", browser_interaction.LLM_MODEL)


def _gold():
    with open(benchmark.TOU_SEASONAL_GOLD_PATH) as fh:
        return json.load(fh)


def _as_db(comps):
    out = []
    for c in comps:
        d = dict(c)
        for k in ("period_start_time", "period_end_time"):
            if d.get(k):
                h, m = d[k].split(":")[:2]
                d[k] = time(int(h) % 24, int(m))
        d["component_type"] = SimpleNamespace(value=d["component_type"])
        out.append(SimpleNamespace(**{**{f: None for f in (
            "period_start_time", "period_end_time", "day_type", "season_start_month",
            "season_start_day", "season_end_month", "season_end_day", "tier_min_kwh", "tier_max_kwh",
        )}, **d}))
    return out


class TestTouSeasonalGold(unittest.TestCase):
    def test_gold_matches_repair_script_sources(self):
        gold = {(u["name"], t["code"]): t for u in _gold()["utilities"] for t in u["tariffs"]}
        for key, meta in (("tou", ho.plan_meta("tou")), ("tiered", ho.plan_meta("tiered")),
                          ("ulo", ho.plan_meta("ulo"))):
            g = gold[("Hydro One", meta["code"])]
            self.assertEqual(sorted(c["rate_value"] for c in g["components"]),
                             sorted(round(float(c["rate_value"]), 6) for c in meta["components"]), key)
        ns_tou = gold[(ns.NS_POWER_NAME, "80")]
        repaired = {round(float(c["rate_value"]), 6) for c in ns.build_domestic_tou_seasonal_components()}
        self.assertEqual({c["rate_value"] for c in ns_tou["components"]}, repaired)
        nl = gold[("Newfoundland Power", nf.NF_11S_CODE)]
        self.assertEqual(sorted(c["rate_value"] for c in nl["components"]),
                         sorted(c["rate_value"] for c in nf.build_nf_11s_all_in_components()))

    def test_gold_covers_tou_and_seasonal_and_is_computable(self):
        tariffs = [t for u in _gold()["utilities"] for t in u["tariffs"]]
        types = {t["rate_type"] for t in tariffs}
        self.assertTrue({"seasonal_tou", "tou", "seasonal", "seasonal_tiered"} <= types)
        self.assertTrue(all(t["expect_computable"] for t in tariffs))

    def test_strict_structural_comparison(self):
        tol = benchmark.Tolerance.from_meta(_gold()["_meta"])
        comps = next(t for u in _gold()["utilities"] for t in u["tariffs"]
                     if t["code"] == "OEB-RPP-TOU")["components"]
        prec, rec, errs = benchmark._compare_components(comps, _as_db(comps), tol)
        self.assertEqual((prec, rec, errs), (1.0, 1.0, []))

        shifted = copy.deepcopy(comps)
        shifted[1]["period_start_time"] = "08:00"
        _, rec, errs = benchmark._compare_components(comps, _as_db(shifted), tol)
        self.assertLess(rec, 1.0)
        self.assertIn("missing_structure", {e.get("issue") for e in errs})

        season = copy.deepcopy(comps)
        season[0]["season_end_day"] = 29
        self.assertLess(benchmark._compare_components(comps, _as_db(season), tol)[1], 1.0)

        off_by_one_pct = copy.deepcopy(comps)
        off_by_one_pct[0]["rate_value"] = round(comps[0]["rate_value"] * 1.01, 6)
        _, rec, errs = benchmark._compare_components(comps, _as_db(off_by_one_pct), tol)
        self.assertLess(rec, 1.0)
        # The legacy 15% tolerance would have accepted the same 1% error.
        self.assertEqual(benchmark._compare_components(comps, _as_db(off_by_one_pct))[1], 1.0)


if __name__ == "__main__":
    unittest.main()
