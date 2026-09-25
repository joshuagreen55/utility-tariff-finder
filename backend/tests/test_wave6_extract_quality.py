"""Wave 6: extraction quality on the TOU / seasonal gold set.

    cd backend && python -m unittest tests.test_wave6_extract_quality -v
"""
from __future__ import annotations

import copy
import logging
import os
import unittest
from datetime import time
from types import SimpleNamespace
from unittest import mock

from app.services import anthropic_compat as ac
from scripts import benchmark, gold_replay, llm_cost
from scripts import tariff_pipeline as tp
from tests.pg_harness import PostgresTestCase


class TestGoldReplay(unittest.TestCase):
    """A perfect extraction of every gold tariff must survive the pipeline."""

    @classmethod
    def setUpClass(cls):
        logging.disable(logging.CRITICAL)
        cls.results = gold_replay.replay(forms=("gold", "model"))

    @classmethod
    def tearDownClass(cls):
        logging.disable(logging.NOTSET)

    def test_gold_set_is_non_trivial(self):
        self.assertGreaterEqual(len({(r.utility, r.tariff) for r in self.results}), 12)

    def test_every_gold_shape_survives(self):
        for r in self.results:
            with self.subTest(form=r.form, utility=r.utility, code=r.code):
                self.assertEqual(r.rejected, [])
                self.assertEqual(r.stored_components, r.gold_components)
                self.assertEqual(r.rate_errors, 0)
                self.assertEqual(r.computable, r.expect_computable, r.reasons)

    def test_cli_exit_code(self):
        with mock.patch("builtins.print"):
            self.assertEqual(gold_replay.main(["--forms"]), 0)


class TestComponentDedupe(unittest.TestCase):
    def test_equal_priced_distinct_windows_are_kept(self):
        base = {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.098,
                "period_label": "Off-Peak", "season": "Winter"}
        rows = [
            {**base, "period_start_time": "19:00", "period_end_time": "07:00", "day_type": "weekday"},
            {**base, "period_start_time": "00:00", "period_end_time": "00:00", "day_type": "weekend"},
            {**base, "period_start_time": "00:00", "period_end_time": "00:00", "day_type": "holiday"},
        ]
        self.assertEqual(len(tp.dedupe_rate_components(rows)), 3)

    def test_equal_priced_tiers_in_different_seasons_are_kept(self):
        base = {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.12, "tier_min_kwh": 0}
        rows = [
            {**base, "tier_max_kwh": 600, "season_start_month": 5, "season_start_day": 1,
             "season_end_month": 10, "season_end_day": 31},
            {**base, "tier_max_kwh": 1000, "season_start_month": 11, "season_start_day": 1,
             "season_end_month": 4, "season_end_day": 30},
        ]
        self.assertEqual(len(tp.dedupe_rate_components(rows)), 2)

    def test_exact_duplicates_still_collapse(self):
        row = {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.2,
               "period_start_time": "07:00", "period_end_time": "11:00", "day_type": "weekday"}
        self.assertEqual(len(tp.dedupe_rate_components([row, dict(row, period_start_time="7:00 am")])), 1)


class TestRiderFolding(unittest.TestCase):
    def test_all_in_energy_is_not_double_counted(self):
        rows = [
            {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.19128,
             "tier_label": "All-in (base + FAM + DSM)"},
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.00156, "tier_label": "FAM"},
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.00648, "tier_label": "DSM"},
        ]
        out = tp.expand_stacking_energy_riders(rows)
        energy = [r for r in out if r["component_type"] == "energy"]
        self.assertEqual([r["rate_value"] for r in energy], [0.19128])
        self.assertTrue(all(r["included_in_energy"] for r in out if r["component_type"] == "adjustment"))

    def test_base_energy_still_gets_riders(self):
        rows = [
            {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.18324},
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.00804, "tier_label": "FAM"},
        ]
        energy = [r for r in tp.expand_stacking_energy_riders(rows) if r["component_type"] == "energy"]
        self.assertAlmostEqual(energy[0]["rate_value"], 0.19128, places=6)

    def test_relative_seasonal_expansion_keeps_season_dates(self):
        rows = [
            {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.15587},
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": 0.00953, "season": "Winter",
             "season_start_month": 12, "season_start_day": 1, "season_end_month": 4, "season_end_day": 30},
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": -0.01297, "season": "Non-Winter",
             "season_start_month": 5, "season_start_day": 1, "season_end_month": 11, "season_end_day": 30},
        ]
        energy = [r for r in tp.expand_relative_seasonal_energy(rows) if r["component_type"] == "energy"]
        by_season = {r["season"]: r for r in energy}
        self.assertEqual(by_season["Winter"]["season_start_month"], 12)
        self.assertEqual(by_season["Non-Winter"]["season_end_day"], 30)


class TestStructuredNormalization(unittest.TestCase):
    def test_clock_forms(self):
        cases = {
            "7:00 a.m.": time(7, 0), "5 PM": time(17, 0), "noon": time(12, 0),
            "12 midnight": time(0, 0), "24:00": time(0, 0), "21:00": time(21, 0),
        }
        for raw, want in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(tp._parse_period_time(raw), want)

    def test_inclusive_end_rounds_up_only_on_end(self):
        out = tp.normalize_structured_components([{
            "component_type": "energy", "unit": "$/kWh", "rate_value": 0.2,
            "period_start_time": "6:59 a.m.", "period_end_time": "10:59 a.m.", "day_type": "weekday",
        }])
        self.assertEqual((out[0]["period_start_time"], out[0]["period_end_time"]), ("06:59", "11:00"))
        out = tp.normalize_structured_components([{"period_start_time": "9 pm", "period_end_time": "11:59 pm"}])
        self.assertEqual(out[0]["period_end_time"], "00:00")

    def test_day_type_aliases(self):
        cases = {
            "Monday to Friday": ["weekday"], "Mon-Fri": ["weekday"], "Weekdays": ["weekday"],
            "weekdays excluding holidays": ["weekday"], "Saturday and Sunday": ["weekend"],
            "every day": ["all"], "Weekends and holidays": ["weekend", "holiday"],
            "weekends/holidays": ["weekend", "holiday"], ("weekend", "holiday"): ["weekend", "holiday"],
            "statutory holidays": ["holiday"], "sometimes": [], None: [],
        }
        for raw, want in cases.items():
            with self.subTest(raw=raw):
                self.assertEqual(tp._day_types(list(raw) if isinstance(raw, tuple) else raw), want)

    def test_compound_day_type_splits_rows(self):
        out = tp.normalize_structured_components([{
            "component_type": "energy", "rate_value": 0.098, "unit": "$/kWh",
            "period_start_time": "00:00", "period_end_time": "00:00",
            "day_type": "Weekends and holidays",
        }])
        self.assertEqual([r["day_type"] for r in out], ["weekend", "holiday"])

    def test_season_forms_and_aliases(self):
        out = tp.normalize_structured_components([{
            "season_start": "Nov 1", "season_end": "April 30", "start_time": "7am", "end_time": "11am",
            "days": "weekdays", "season_start_month": None,
        }])[0]
        self.assertEqual((out["season_start_month"], out["season_start_day"]), (11, 1))
        self.assertEqual((out["season_end_month"], out["season_end_day"]), (4, 30))
        self.assertEqual((out["period_start_time"], out["period_end_time"], out["day_type"]),
                         ("07:00", "11:00", "weekday"))
        out = tp.normalize_structured_components([{"season_start_month": "June", "season_end_month": "Sept"}])[0]
        self.assertEqual((out["season_start_month"], out["season_end_month"]), (6, 9))

    def test_never_fills_missing_fields(self):
        out = tp.normalize_structured_components([{"component_type": "energy", "rate_value": 0.1,
                                                   "period_label": "On-Peak", "season": "Winter"}])[0]
        for key in ("period_start_time", "period_end_time", "day_type", "season_start_month"):
            self.assertIsNone(out.get(key))

    def test_cents_as_printed_normalize_in_phase4(self):
        et = tp.ExtractedTariff(
            name="Residential", customer_class="residential", rate_type="flat", confidence=0.9,
            components=[{"component_type": "energy", "unit": "¢/kWh", "rate_value": 19.128},
                        {"component_type": "fixed", "unit": "$/month", "rate_value": 20.08}],
        )
        logging.disable(logging.CRITICAL)
        try:
            _report, valid = tp.phase4_validate([et], "Nova Scotia Power", "NS")
        finally:
            logging.disable(logging.NOTSET)
        energy = [c for c in valid[0].components if c["component_type"] == "energy"][0]
        self.assertAlmostEqual(energy["rate_value"], 0.19128, places=6)
        self.assertEqual(energy["unit"], "$/kWh")


class TestPrompts(unittest.TestCase):
    def test_structured_rules_in_every_extraction_prompt(self):
        for prompt in (tp.EXTRACTION_PROMPT, tp.TWOPASS_EXTRACT_PROMPT,
                       tp.PAGE_SCREENSHOT_EXTRACTION_PROMPT_BASE, tp.PDF_VISION_EXTRACTION_PROMPT_BASE):
            self.assertIn("DAY TYPES", prompt)
            self.assertNotIn("{structured_rules}", prompt)
            self.assertNotIn("Convert cents to dollars", prompt)

    def test_prompts_still_format(self):
        tp.EXTRACTION_PROMPT.format(url="u", title="t", content="c", utility_name="n", state="s")
        tp.TWOPASS_EXTRACT_PROMPT.format(tariff_name="a", customer_class="r",
                                         utility_name="n", state="s", content="c")

    def test_ns_code_80_example_emits_weekend_and_holiday_rows(self):
        example = tp.EXTRACTION_PROMPT.split("Example 6")[1].split("Example 7")[0]
        self.assertIn('day_type "weekend"', example)
        self.assertIn('day_type "holiday"', example)
        self.assertNotIn("Mention weekend/holiday off-peak in description", tp.EXTRACTION_PROMPT)

    def test_prompt_version_bumped(self):
        self.assertEqual(tp._LLM_PROMPT_VERSION, "v3")


class TestGeminiSchema(unittest.TestCase):
    def test_no_type_unions(self):
        def walk(node):
            if isinstance(node, dict):
                self.assertFalse(isinstance(node.get("type"), list), node)
                self.assertNotIn("cache_control", node)
                for v in node.values():
                    walk(v)
            elif isinstance(node, list):
                for v in node:
                    walk(v)

        schema = tp._gemini_response_schema()
        walk(schema)
        comp = schema["properties"]["tariffs"]["items"]["properties"]["components"]["items"]
        self.assertEqual(comp["properties"]["period_start_time"], {
            "type": "string", "nullable": True,
            "description": "HH:MM 24h clock start; null if not stated (do not invent)",
        })

    def test_sdk_accepts_schema(self):
        try:
            from google.genai import types
        except ImportError:
            self.skipTest("google-genai not installed")
        types.GenerateContentConfig(response_mime_type="application/json",
                                    response_schema=tp._gemini_response_schema())

    def test_top_level_list_response_is_used(self):
        fake = SimpleNamespace(text='[{"name": "R", "components": []}]', usage_metadata=None)
        client = SimpleNamespace(models=SimpleNamespace(generate_content=lambda **kw: fake))
        with mock.patch.object(tp, "_get_gemini_client", return_value=client):
            self.assertEqual(tp._call_gemini("p"), [{"name": "R", "components": []}])


class _BadRequest(Exception):
    status_code = 400

    def __init__(self, message):
        super().__init__(message)
        self.message = message


class TestAnthropicCompat(unittest.TestCase):
    TOOL_REQ = {
        "max_tokens": 8192,
        "system": [{"type": "text", "text": "S", "cache_control": {"type": "ephemeral"}}],
        "messages": [{"role": "user", "content": "x"}],
        "tools": [{"name": "store_tariffs"}],
        "tool_choice": {"type": "tool", "name": "store_tariffs"},
    }

    def setUp(self):
        patcher = mock.patch.dict(os.environ, {}, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)
        for k in ("ANTHROPIC_THINKING", "ANTHROPIC_EFFORT", "ANTHROPIC_THINKING_MIN_MAX_TOKENS"):
            os.environ.pop(k, None)

    def test_haiku_request_unchanged(self):
        body = {**self.TOOL_REQ, "model": "claude-haiku-4-5-20251001", "temperature": 0}
        self.assertEqual(ac.adapt_request(body, sdk=True), body)

    def test_opus_5_5_unforces_tool_choice_and_keeps_cache_prefix(self):
        out = ac.adapt_request({**self.TOOL_REQ, "model": "claude-opus-5-5"}, sdk=True)
        self.assertEqual(out["tool_choice"], {"type": "auto"})
        self.assertEqual(out["system"][0], self.TOOL_REQ["system"][0])
        self.assertIn("store_tariffs", out["system"][-1]["text"])
        self.assertEqual(out["max_tokens"], 16000)

    def test_sonnet_5_keeps_forced_tool_and_drops_sampling(self):
        out = ac.adapt_request({**self.TOOL_REQ, "model": "claude-sonnet-5", "temperature": 0.0, "top_k": 5})
        self.assertEqual(out["tool_choice"]["type"], "tool")
        self.assertNotIn("temperature", out)
        self.assertNotIn("top_k", out)

    def test_thinking_disabled_only_where_allowed(self):
        os.environ["ANTHROPIC_THINKING"] = "disabled"
        sonnet = ac.adapt_request({**self.TOOL_REQ, "model": "claude-sonnet-5"}, sdk=True)
        self.assertEqual(sonnet["extra_body"]["thinking"], {"type": "disabled"})
        self.assertEqual(sonnet["max_tokens"], 8192)
        opus = ac.adapt_request({**self.TOOL_REQ, "model": "claude-opus-5-5"}, sdk=True)
        self.assertNotIn("thinking", opus.get("extra_body", {}))
        haiku = ac.adapt_request({**self.TOOL_REQ, "model": "claude-haiku-4-5-20251001"}, sdk=True)
        self.assertNotIn("extra_body", haiku)

    def test_effort_goes_to_output_config(self):
        os.environ["ANTHROPIC_EFFORT"] = "low"
        out = ac.adapt_request({**self.TOOL_REQ, "model": "claude-sonnet-5"})
        self.assertEqual(out["output_config"], {"effort": "low"})

    def test_create_retries_once_on_400(self):
        calls = []

        def create(**kw):
            calls.append(kw)
            if len(calls) == 1:
                raise _BadRequest("tool_choice: forced tool use is not supported on this model")
            return SimpleNamespace(content=[])

        ac.create(SimpleNamespace(create=create), **{**self.TOOL_REQ, "model": "claude-future-9"})
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1]["tool_choice"], {"type": "auto"})

    def test_create_does_not_retry_unrelated_errors(self):
        def create(**kw):
            raise _BadRequest("messages: roles must alternate")

        with self.assertRaises(_BadRequest):
            ac.create(SimpleNamespace(create=create), **{**self.TOOL_REQ, "model": "claude-sonnet-5"})

    def test_raw_post_retry(self):
        responses = [SimpleNamespace(status_code=400, text='{"error": "temperature is not supported"}'),
                     SimpleNamespace(status_code=200, text="{}")]
        sent = []

        def post(url, json, **kw):
            sent.append(json)
            return responses[len(sent) - 1]

        resp = ac.post(post, "u", json={"model": "claude-future-9", "temperature": 0, "max_tokens": 10})
        self.assertEqual(resp.status_code, 200)
        self.assertNotIn("temperature", sent[1])

    def test_response_text_skips_thinking(self):
        content = [SimpleNamespace(type="thinking", thinking=""), SimpleNamespace(type="text", text="[1]")]
        self.assertEqual(ac.response_text(content), "[1]")
        self.assertEqual(ac.response_text([{"type": "thinking"}, {"type": "text", "text": "ok"}]), "ok")
        self.assertEqual(ac.response_text([]), "")

    def test_pipeline_proxy_routes_through_compat(self):
        seen = {}

        def create(**kw):
            seen.update(kw)
            return SimpleNamespace(content=[], usage=None)

        proxy = tp._AnthropicMessagesProxy(SimpleNamespace(create=create))
        proxy.create(**{**self.TOOL_REQ, "model": "claude-opus-5-5"})
        self.assertEqual(seen["tool_choice"], {"type": "auto"})


class TestPricing(unittest.TestCase):
    def setUp(self):
        llm_cost.reset()

    def test_pricing_keys(self):
        self.assertEqual(llm_cost.pricing_key("claude-opus-5-5"), "claude-opus-5-5")
        self.assertEqual(llm_cost.pricing_key("claude-opus-5-5-20261101"), "claude-opus-5-5")
        self.assertEqual(llm_cost.pricing_key("claude-opus-5"), "opus")
        self.assertEqual(llm_cost.pricing_key("claude-sonnet-5"), "sonnet")
        self.assertEqual(llm_cost.pricing_key("claude-haiku-4-5-20251001"), "haiku")
        self.assertEqual(llm_cost.model_key("claude-sonnet-5"), "sonnet")

    def test_costs_use_list_prices(self):
        usage = SimpleNamespace(input_tokens=1_000_000, output_tokens=1_000_000,
                                cache_read_input_tokens=0, cache_creation_input_tokens=0)
        for model, usd in (("claude-opus-5-5", 24.0), ("claude-opus-5", 30.0),
                           ("claude-sonnet-5", 12.0), ("claude-haiku-4-5-20251001", 6.0)):
            llm_cost.reset()
            llm_cost.record_anthropic(model, usage)
            self.assertAlmostEqual(llm_cost.summary()["total_usd"], usd, places=6, msg=model)

    def test_opus_5_5_rolls_up_under_opus(self):
        usage = SimpleNamespace(input_tokens=10, output_tokens=10,
                                cache_read_input_tokens=0, cache_creation_input_tokens=0)
        llm_cost.record_anthropic("claude-opus-5-5", usage)
        self.assertIn("opus", llm_cost.summary()["by_model"])


def _energy(start, end, day, rate=0.1):
    return {"component_type": "energy", "unit": "$/kWh", "rate_value": rate,
            "period_start_time": start, "period_end_time": end, "day_type": day}


class TestComputableGuardrails(unittest.TestCase):
    FULL = [_energy("07:00", "19:00", "weekday", 0.2), _energy("19:00", "07:00", "weekday"),
            _energy("00:00", "00:00", "weekend")]
    GAPPY = [_energy("07:00", "19:00", "weekday", 0.2), _energy("19:00", "07:00", "weekday")]

    def test_phase4_records_computable_reasons(self):
        et = tp.ExtractedTariff(name="TOU", customer_class="residential", rate_type="tou",
                                confidence=0.9, components=[dict(c) for c in self.GAPPY])
        logging.disable(logging.CRITICAL)
        try:
            _r, valid = tp.phase4_validate([et], "U", "ON")
        finally:
            logging.disable(logging.NOTSET)
        self.assertTrue(valid[0].needs_review)
        self.assertIn("tou_gap:weekend", valid[0].computable_reasons)

    def test_phase4_leaves_computable_tou_unflagged(self):
        et = tp.ExtractedTariff(name="TOU", customer_class="residential", rate_type="tou",
                                confidence=0.9, components=[dict(c) for c in self.FULL])
        logging.disable(logging.CRITICAL)
        try:
            _r, valid = tp.phase4_validate([et], "U", "ON")
        finally:
            logging.disable(logging.NOTSET)
        self.assertEqual(valid[0].computable_reasons, [])

    def test_regression_detected_only_when_live_row_is_computable(self):
        live_ok = SimpleNamespace(rate_type="tou", rate_components=self.FULL, name="TOU")
        live_bad = SimpleNamespace(rate_type="tou", rate_components=self.GAPPY, name="TOU")
        self.assertIn("tou_gap:weekend", tp._computable_regression(live_ok, "tou", self.GAPPY))
        self.assertIsNone(tp._computable_regression(live_bad, "tou", self.GAPPY))
        self.assertIsNone(tp._computable_regression(live_ok, "tou", self.FULL))

    def test_unsupported_shapes_are_not_held(self):
        live = SimpleNamespace(rate_type="flat", name="GS",
                               rate_components=[{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.1}])
        demand = [{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.1},
                  {"component_type": "demand", "unit": "$/kW", "rate_value": 9.0}]
        self.assertIsNone(tp._computable_regression(live, "demand", demand))


class TestBenchmarkScoreboard(unittest.TestCase):
    def setUp(self):
        import json

        data = json.loads(benchmark.TOU_SEASONAL_GOLD_PATH.read_text())
        self.tol = benchmark.Tolerance.from_meta(data["_meta"])
        self.gold = data["utilities"][0]["tariffs"][0]["components"]

    def _modes(self, db):
        db = tp._build_rate_components(tp.ExtractedTariff(name="x", components=db))
        _p, _r, errors = benchmark._compare_components(self.gold, db, self.tol)
        return benchmark.failure_taxonomy(self.gold, db, errors, self.tol)

    def test_perfect_match_has_no_failures(self):
        modes = self._modes(copy.deepcopy(self.gold))
        self.assertEqual(sum(modes.values()), 0)
        self.assertEqual(benchmark.top_failure(modes, matched=True), "none")

    def test_modes_name_the_differing_field(self):
        cases = {
            "wrong_price": lambda c: c.update(rate_value=c["rate_value"] + 0.01),
            "missing_clock": lambda c: c.update(period_start_time=None, period_end_time=None),
            "day_type": lambda c: c.update(day_type=None),
            "missing_season": lambda c: c.update(season_start_month=None),
        }
        for mode, mutate in cases.items():
            with self.subTest(mode=mode):
                db = copy.deepcopy(self.gold)
                mutate(db[1])
                modes = self._modes(db)
                self.assertEqual(benchmark.top_failure(modes, matched=True), mode, modes)

    def test_unmatched_is_product_match(self):
        self.assertEqual(benchmark.top_failure({}, matched=False), "product_match")

    def test_baseline_gate(self):
        base = {"computable_agreement": "3/12", "total_rate_errors": 157, "structure_errors": 120}
        self.assertEqual(benchmark.gold_regressions(base, {
            "computable_agree": 9, "total_rate_errors": 40, "structure_errors": 10}), [])
        self.assertEqual(len(benchmark.gold_regressions(base, {
            "computable_agree": 2, "total_rate_errors": 158, "structure_errors": 121})), 3)


class TestStoreComputableHold(PostgresTestCase):
    """store_tariffs never retires a computable live row for a gappy extraction."""

    def _et(self, components):
        return tp.ExtractedTariff(
            name="Residential TOU", customer_class="residential", rate_type="tou",
            source_url="https://utility.example.com/rates", components=[dict(c) for c in components],
        )

    def test_gappy_extraction_is_held_against_computable_live_row(self):
        uid = self.make_utility("Wave6 Hold Electric")
        live = self.make_tariff(uid, "Residential TOU", TestComputableGuardrails.FULL, rate_type="tou")

        tp.store_tariffs(uid, [self._et(TestComputableGuardrails.GAPPY)], dry_run=False)

        row = self.get_tariff(live)
        self.assertIsNone(row.supersede_reason)
        self.assertEqual(len(self.tariffs_for(uid)), 1)
        holds = [e for e in self.events_for(uid) if e.decision == "hold"]
        self.assertEqual([h.reason for h in holds], ["computable_regression"])
        self.assertIn("tou_gap:weekend", holds[0].payload["computable_reasons"])

    def test_computable_extraction_supersedes_gappy_live_row(self):
        uid = self.make_utility("Wave6 Fix Electric")
        live = self.make_tariff(uid, "Residential TOU", TestComputableGuardrails.GAPPY, rate_type="tou")

        tp.store_tariffs(uid, [self._et(TestComputableGuardrails.FULL)], dry_run=False)

        old = self.get_tariff(live)
        self.assertEqual(old.supersede_reason, "refresh")
        new = self.get_tariff(old.superseded_by_tariff_id)
        self.assertEqual(len(new.rate_components), 3)
        self.assertNotIn("extract_not_computable", new.confidence_factors)


if __name__ == "__main__":
    unittest.main()
