"""Mysa Completeness: general-pipeline extract must fill structured fields.

Covers prompt/schema contract + parse → Phase 4 → RateComponent round-trip:
  - clocks/dates/day_type persist when the LLM emits them
  - label-only TOU/season still parses but fails computable / needs_review
  - flat energy-only still works

    cd backend && python -m unittest tests.test_mysa_structured_extract -v
"""
from __future__ import annotations

import logging
import unittest
from datetime import time

from app.services.computable import evaluate_computable
from app.services.tou_seasonal_completeness import evaluate_tariff_completeness
from scripts import browser_interaction as bi
from scripts import tariff_pipeline as tp


def _phase4(et: tp.ExtractedTariff) -> tp.ExtractedTariff:
    logging.disable(logging.CRITICAL)
    try:
        _report, valid = tp.phase4_validate([et], "Test Utility", "ON")
    finally:
        logging.disable(logging.NOTSET)
    assert valid, "expected tariff to survive phase4"
    return valid[0]


class TestMysaPromptContract(unittest.TestCase):
    """Every general-pipeline extract surface asks for Mysa structured fields."""

    REQUIRED_SNIPPETS = (
        "period_start_time",
        "period_end_time",
        "day_type",
        "season_start/end",
        "do NOT invent hours",
        "MYSA FIELDS",
    )

    def test_shared_rules_name_mysa_fields(self):
        for snippet in self.REQUIRED_SNIPPETS:
            self.assertIn(snippet, tp._STRUCTURED_RULES)

    def test_phase3_prompts_include_structured_rules(self):
        for prompt in (
            tp.EXTRACTION_PROMPT,
            tp.TWOPASS_EXTRACT_PROMPT,
            tp.PAGE_SCREENSHOT_EXTRACTION_PROMPT_BASE,
            tp.PDF_VISION_EXTRACTION_PROMPT_BASE,
        ):
            with self.subTest(prompt=prompt[:40]):
                self.assertIn("MYSA FIELDS", prompt)
                self.assertIn("period_start_time", prompt)
                self.assertIn("season_start_month", prompt)
                self.assertNotIn("{structured_rules}", prompt)
                self.assertNotIn("Convert cents to dollars", prompt)

    def test_phase6_prompt_aligned(self):
        text = tp._phase6_prompt("Hydro One", "ON", attempted_urls=None)
        self.assertIn("MYSA FIELDS", text)
        self.assertIn("seasonal_tou", text)
        self.assertIn("period_start_time", text)
        self.assertIn("Do not convert cents to dollars", text)
        self.assertNotIn('If you use "$/kWh" as the unit, convert cents', text)

    def test_browser_cli_prompt_aligned(self):
        # Build the same string the CLI would send (no Anthropic call).
        from unittest import mock

        from scripts.browser_interaction import PageSnapshot

        snap = PageSnapshot(
            url="https://example.com",
            title="Rates",
            text="rates 9.5¢/kWh",
            html="",
            interactions_performed=[],
        )
        with mock.patch.object(bi, "ANTHROPIC_API_KEY", "test-key"), \
                mock.patch("anthropic.Anthropic") as Anthropic, \
                mock.patch("app.services.anthropic_compat.create") as create, \
                mock.patch("app.services.anthropic_compat.response_text",
                           return_value="[]"), \
                mock.patch("scripts.llm_cost.record_anthropic"):
            create.return_value = mock.Mock(usage=None, content=[])
            bi.extract_tariffs_from_snapshots([snap], "Test Co")
        prompt = create.call_args.kwargs["messages"][0]["content"]
        self.assertIn("MYSA FIELDS", prompt)
        self.assertIn("period_start_time", prompt)
        self.assertIn("season_start_month", prompt)
        self.assertNotIn("Convert cents/kWh to $/kWh", prompt)

    def test_tool_schema_has_structured_fields(self):
        props = (
            tp.TARIFF_EXTRACTION_TOOL["input_schema"]["properties"]["tariffs"]
            ["items"]["properties"]["components"]["items"]["properties"]
        )
        for key in (
            "period_start_time", "period_end_time", "day_type",
            "season_start_month", "season_start_day",
            "season_end_month", "season_end_day",
        ):
            self.assertIn(key, props)
        self.assertIn("do not invent", props["period_start_time"]["description"].lower())

    def test_prompt_version_bumped_for_mysa_rules(self):
        self.assertEqual(tp._LLM_PROMPT_VERSION, "v7")


class TestStructuredRoundTrip(unittest.TestCase):
    """LLM JSON with clocks/dates survives parse → phase4 → RateComponent ORM."""

    def test_tou_seasonal_json_round_trips_into_components(self):
        raw = [{
            "name": "Residential TOU",
            "code": "TOU-R",
            "customer_class": "residential",
            "rate_type": "seasonal_tou",
            "confidence": 0.95,
            "components": [
                {
                    "component_type": "fixed", "unit": "$/month", "rate_value": 10.0,
                },
                {
                    "component_type": "energy", "unit": "$/kWh", "rate_value": 0.20,
                    "period_label": "On-Peak", "period_start_time": "14:00",
                    "period_end_time": "20:00", "day_type": "all",
                    "season": "Summer",
                    "season_start_month": 6, "season_start_day": 1,
                    "season_end_month": 9, "season_end_day": 30,
                },
                {
                    "component_type": "energy", "unit": "$/kWh", "rate_value": 0.12,
                    "period_label": "Off-Peak", "period_start_time": "20:00",
                    "period_end_time": "14:00", "day_type": "all",
                    "season": "Summer",
                    "season_start_month": 6, "season_start_day": 1,
                    "season_end_month": 9, "season_end_day": 30,
                },
                {
                    "component_type": "energy", "unit": "$/kWh", "rate_value": 0.15,
                    "period_label": "On-Peak", "period_start_time": "14:00",
                    "period_end_time": "20:00", "day_type": "all",
                    "season": "Winter",
                    "season_start_month": 10, "season_start_day": 1,
                    "season_end_month": 5, "season_end_day": 31,
                },
                {
                    "component_type": "energy", "unit": "$/kWh", "rate_value": 0.10,
                    "period_label": "Off-Peak", "period_start_time": "20:00",
                    "period_end_time": "14:00", "day_type": "all",
                    "season": "Winter",
                    "season_start_month": 10, "season_start_day": 1,
                    "season_end_month": 5, "season_end_day": 31,
                },
            ],
        }]
        parsed = tp._parse_extraction_response(raw, "https://utility.example/rates")
        valid = _phase4(parsed[0])
        self.assertFalse(valid.needs_review)
        self.assertEqual(valid.computable_reasons, [])
        self.assertTrue(evaluate_tariff_completeness("seasonal_tou", valid.components).complete)
        self.assertTrue(evaluate_computable("seasonal_tou", valid.components).computable)

        orm_rows = tp._build_rate_components(valid)
        energy = [r for r in orm_rows if r.component_type.value == "energy"]
        self.assertEqual(len(energy), 4)
        summer_on = next(
            r for r in energy
            if r.period_label == "On-Peak" and r.season_start_month == 6
        )
        self.assertEqual(summer_on.period_start_time, time(14, 0))
        self.assertEqual(summer_on.period_end_time, time(20, 0))
        self.assertEqual(summer_on.day_type, "all")
        self.assertEqual(
            (summer_on.season_start_month, summer_on.season_start_day,
             summer_on.season_end_month, summer_on.season_end_day),
            (6, 1, 9, 30),
        )
        self.assertAlmostEqual(float(summer_on.rate_value), 0.20, places=6)
        self.assertEqual(summer_on.unit, "$/kWh")

    def test_cents_as_printed_still_round_trip_with_clocks(self):
        """¢/kWh extracts normalize to $/kWh and keep Mysa structured cols."""
        et = tp.ExtractedTariff(
            name="Plan TOU-R", customer_class="residential", rate_type="tou",
            confidence=0.9,
            components=[
                {
                    "component_type": "energy", "unit": "¢/kWh", "rate_value": 32.1,
                    "period_label": "On-Peak", "period_start_time": "16:00",
                    "period_end_time": "21:00", "day_type": "weekday",
                },
                {
                    "component_type": "energy", "unit": "¢/kWh", "rate_value": 11.4,
                    "period_label": "Off-Peak", "period_start_time": "21:00",
                    "period_end_time": "16:00", "day_type": "weekday",
                },
                {
                    "component_type": "energy", "unit": "¢/kWh", "rate_value": 11.4,
                    "period_label": "Off-Peak", "period_start_time": "00:00",
                    "period_end_time": "00:00", "day_type": "weekend",
                },
            ],
        )
        valid = _phase4(et)
        # Cents→dollars unit notes soft-flag needs_review; clocks still persist.
        self.assertEqual(valid.computable_reasons, [])
        orm = tp._build_rate_components(valid)
        on_peak = next(r for r in orm if r.period_label == "On-Peak")
        self.assertEqual(on_peak.period_start_time, time(16, 0))
        self.assertEqual(on_peak.day_type, "weekday")
        self.assertAlmostEqual(float(on_peak.rate_value), 0.321, places=6)
        self.assertEqual(on_peak.unit, "$/kWh")

    def test_phase6_json_parser_keeps_structured_keys(self):
        report = """Some research narrative.

```json
[{
  "name": "Residential TOU",
  "code": "R-TOU",
  "customer_class": "residential",
  "rate_type": "tou",
  "confidence": 0.9,
  "source_url": "https://utility.example/tou",
  "components": [{
    "component_type": "energy",
    "unit": "¢/kWh",
    "rate_value": 32.1,
    "period_label": "On-Peak",
    "period_start_time": "16:00",
    "period_end_time": "21:00",
    "day_type": "weekday",
    "season": null,
    "season_start_month": null,
    "season_start_day": null,
    "season_end_month": null,
    "season_end_day": null
  }, {
    "component_type": "energy",
    "unit": "¢/kWh",
    "rate_value": 11.4,
    "period_label": "Off-Peak",
    "period_start_time": "21:00",
    "period_end_time": "16:00",
    "day_type": "weekday"
  }, {
    "component_type": "energy",
    "unit": "¢/kWh",
    "rate_value": 11.4,
    "period_label": "Off-Peak",
    "period_start_time": "00:00",
    "period_end_time": "00:00",
    "day_type": "weekend"
  }]
}]
```
"""
        tariffs = tp._phase6_parse_tariffs(report, "https://fallback.example")
        self.assertEqual(len(tariffs), 1)
        on_peak = tariffs[0].components[0]
        self.assertEqual(on_peak["period_start_time"], "16:00")
        self.assertEqual(on_peak["day_type"], "weekday")
        valid = _phase4(tariffs[0])
        orm = tp._build_rate_components(valid)
        weekday_on = next(r for r in orm if r.day_type == "weekday" and r.period_label == "On-Peak")
        self.assertEqual(weekday_on.period_start_time, time(16, 0))
        self.assertEqual(weekday_on.period_end_time, time(21, 0))


class TestLabelOnlyIncomplete(unittest.TestCase):
    """Label-only TOU/season parses but is flagged incomplete / not computable."""

    def test_label_only_tou_needs_review(self):
        et = tp.ExtractedTariff(
            name="Residential TOU",
            customer_class="residential",
            rate_type="tou",
            confidence=0.8,
            components=[
                {"component_type": "fixed", "unit": "$/month", "rate_value": 12.0},
                {
                    "component_type": "energy", "unit": "$/kWh", "rate_value": 0.22,
                    "period_label": "On-Peak",
                },
                {
                    "component_type": "energy", "unit": "$/kWh", "rate_value": 0.08,
                    "period_label": "Off-Peak",
                },
            ],
        )
        # Still parses / normalizes without inventing clocks.
        normalized = tp.normalize_structured_components(et.components)
        for row in normalized:
            if row["component_type"] == "energy":
                self.assertIsNone(row.get("period_start_time"))
                self.assertIsNone(row.get("period_end_time"))
                self.assertIsNone(row.get("day_type"))

        valid = _phase4(et)
        self.assertTrue(valid.needs_review)
        self.assertTrue(valid.completeness_reasons)
        self.assertIn("tou_missing_clock_windows", valid.completeness_reasons)
        self.assertFalse(evaluate_computable("tou", valid.components).computable)
        self.assertTrue(valid.computable_reasons)

        # Persist path keeps display labels and null structured cols.
        orm = tp._build_rate_components(valid)
        energy = [r for r in orm if r.component_type.value == "energy"]
        self.assertEqual({r.period_label for r in energy}, {"On-Peak", "Off-Peak"})
        for r in energy:
            self.assertIsNone(r.period_start_time)
            self.assertIsNone(r.period_end_time)
            self.assertIsNone(r.day_type)

    def test_label_only_seasonal_needs_review(self):
        et = tp.ExtractedTariff(
            name="Domestic Seasonal",
            customer_class="residential",
            rate_type="seasonal",
            confidence=0.8,
            components=[
                {
                    "component_type": "energy", "unit": "$/kWh", "rate_value": 0.16,
                    "season": "Winter",
                },
                {
                    "component_type": "energy", "unit": "$/kWh", "rate_value": 0.12,
                    "season": "Summer",
                },
            ],
        )
        valid = _phase4(et)
        self.assertTrue(valid.needs_review)
        self.assertIn("seasonal_missing_calendar_dates", valid.completeness_reasons)
        self.assertFalse(evaluate_computable("seasonal", valid.components).computable)
        orm = tp._build_rate_components(valid)
        for r in orm:
            self.assertIsNone(r.season_start_month)
            self.assertIsNone(r.season_end_month)


class TestFlatStillWorks(unittest.TestCase):
    def test_flat_energy_only_computable_without_review(self):
        et = tp.ExtractedTariff(
            name="Residential Service",
            code="RS",
            customer_class="residential",
            rate_type="flat",
            confidence=0.95,
            components=[
                {"component_type": "fixed", "unit": "$/month", "rate_value": 12.50},
                {"component_type": "energy", "unit": "$/kWh", "rate_value": 0.0956},
            ],
        )
        valid = _phase4(et)
        self.assertFalse(valid.needs_review)
        self.assertEqual(valid.completeness_reasons, [])
        self.assertEqual(valid.computable_reasons, [])
        verdict = evaluate_computable("flat", valid.components)
        self.assertTrue(verdict.computable)

        orm = tp._build_rate_components(valid)
        energy = [r for r in orm if r.component_type.value == "energy"][0]
        self.assertAlmostEqual(float(energy.rate_value), 0.0956, places=6)
        self.assertEqual(energy.unit, "$/kWh")
        self.assertIsNone(energy.period_start_time)
        self.assertIsNone(energy.season_start_month)


if __name__ == "__main__":
    unittest.main()
