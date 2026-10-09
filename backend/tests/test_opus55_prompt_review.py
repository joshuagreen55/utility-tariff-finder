"""Opus 5.5 prompt-review behaviours (v9): schema flags, mills, truncation,
allowed derivations, residential-by-who-served, future-dated TOU, empty_reason.

    cd backend && python -m unittest tests.test_opus55_prompt_review -v
"""
from __future__ import annotations

import logging
import unittest
from datetime import date, timedelta
from types import SimpleNamespace
from unittest import mock

from app.services import tariff_history as th
from scripts import tariff_pipeline as tp


def _et(**kw) -> tp.ExtractedTariff:
    defaults = dict(
        name="Residential",
        code="R",
        customer_class="residential",
        rate_type="flat",
        components=[{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.10}],
        confidence=0.9,
    )
    defaults.update(kw)
    return tp.ExtractedTariff(**defaults)


class TestAllowedDerivationsAndNoGuess(unittest.TestCase):
    def test_allowed_derivations_listed_beside_source_only(self):
        rules = tp._STRUCTURED_RULES
        self.assertIn("SOURCE ONLY", rules)
        self.assertIn("Allowed derivations", rules)
        for needle in (
            "adding printed numbers",
            "all other hours",
            "month range",
            "12-hour clock",
            "Nothing else",
        ):
            self.assertIn(needle, rules)

    def test_examples_are_synthetic_round_numbers(self):
        # Real NSP / NL cents must not appear in worked examples — copying is detectable.
        examples = tp.EXTRACTION_SYSTEM_PROMPT.split("EXAMPLES", 1)[-1]
        for banned in ("18.324", "19.128", "0.156", "0.648", "15.587", "0.953", "1.297"):
            self.assertNotIn(banned, examples)


class TestResidentialByWhoServed(unittest.TestCase):
    def test_farm_and_home_and_general_service_kept_when_serving_homes(self):
        for prompt in (
            tp._STRUCTURED_RULES,
            tp.EXTRACTION_SYSTEM_PROMPT,
            tp.HAIKU_EXTRACTION_SYSTEM_PROMPT,
            tp.TWOPASS_IDENTIFY_PROMPT,
        ):
            self.assertIn("Farm & Home", prompt)
            self.assertIn("General Service", prompt)
            self.assertRegex(prompt.lower(), r"who the rate serves|applies to residences")
        # Vision user messages defer who-served detail to the cached system prompt.
        self.assertIn("residential-by-who-served", tp.PAGE_SCREENSHOT_EXTRACTION_PROMPT_BASE)
        self.assertIn("residential-by-who-served", tp.PDF_VISION_EXTRACTION_PROMPT_BASE)


class TestSchemaFlagsAndParse(unittest.TestCase):
    def test_tool_schema_has_review_and_empty_fields(self):
        schema = tp.TARIFF_EXTRACTION_TOOL["input_schema"]["properties"]
        tariff_props = schema["tariffs"]["items"]["properties"]
        for key in (
            "needs_review",
            "missing_fields",
            "riders_referenced_not_shown",
            "energy_scope",
            "closed_to_new",
            "energy_includes_riders",
        ):
            self.assertIn(key, tariff_props)
        self.assertIn("empty_reason", schema)
        self.assertIn("linked_document_hint", schema)
        self.assertIn(
            "delivery_plus_default_supply",
            tariff_props["energy_scope"]["enum"],
        )

    def test_parse_sets_needs_review_from_missing_and_riders(self):
        raw = [{
            "name": "Farm & Home",
            "customer_class": "residential",
            "rate_type": "flat",
            "confidence": 0.8,
            "missing_fields": ["storm rider amount"],
            "riders_referenced_not_shown": ["Schedule 1xx"],
            "closed_to_new": True,
            "energy_scope": "delivery_plus_default_supply",
            "components": [
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 10.0},
            ],
        }]
        out = tp._parse_extraction_response(raw, "https://example.com/rates")
        self.assertEqual(len(out), 1)
        t = out[0]
        self.assertTrue(t.needs_review)
        self.assertEqual(t.missing_fields, ["storm rider amount"])
        self.assertEqual(t.riders_referenced_not_shown, ["Schedule 1xx"])
        self.assertTrue(t.closed_to_new)
        self.assertEqual(t.energy_scope, "delivery_plus_default_supply")

    def test_parse_carries_top_level_empty_reason(self):
        out = tp._parse_extraction_response(
            [], "https://example.com",
            empty_reason="no_residential_rates",
            linked_document_hint="Rate Book PDF",
        )
        self.assertEqual(out, [])
        # When tariffs exist, empty_reason still lands on each row for audit.
        raw = [{
            "name": "R", "customer_class": "residential", "rate_type": "flat",
            "confidence": 0.5,
            "components": [{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.1}],
        }]
        out = tp._parse_extraction_response(
            raw, "u", empty_reason="other", linked_document_hint="hint",
        )
        self.assertEqual(out[0].empty_reason, "other")
        self.assertEqual(out[0].linked_document_hint, "hint")


class TestMillsNormalization(unittest.TestCase):
    def test_mills_per_kwh_divide_by_1000(self):
        t = _et(components=[
            {"component_type": "energy", "unit": "mills/kWh", "rate_value": 25.0},
            {"component_type": "adjustment", "unit": "2.5 mills/kWh", "rate_value": 2.5},
        ])
        notes = tp._normalize_component_units(t, p99_energy=0.40)
        self.assertEqual(len(notes), 2)
        self.assertAlmostEqual(t.components[0]["rate_value"], 0.025)
        self.assertEqual(t.components[0]["unit"], "$/kWh")
        self.assertAlmostEqual(t.components[1]["rate_value"], 0.0025)
        self.assertEqual(t.components[1]["unit"], "$/kWh")


class TestTruncationRetry(unittest.TestCase):
    def test_max_tokens_stop_reason_retries_once(self):
        truncated = SimpleNamespace(
            stop_reason="max_tokens",
            content=[SimpleNamespace(type="tool_use", name="store_tariffs", input={"tariffs": []})],
            usage=None,
        )
        complete = SimpleNamespace(
            stop_reason="tool_use",
            content=[SimpleNamespace(
                type="tool_use", name="store_tariffs",
                input={"tariffs": [{
                    "name": "R", "customer_class": "residential", "rate_type": "flat",
                    "confidence": 0.9,
                    "components": [{"component_type": "energy", "unit": "$/kWh", "rate_value": 0.1}],
                }]},
            )],
            usage=None,
        )
        client = mock.Mock()
        client.messages.create = mock.Mock(side_effect=[truncated, complete])
        with mock.patch.object(tp, "_get_anthropic_client", return_value=client):
            out = tp._call_claude_tool("prompt", model="claude-sonnet-5-5", max_tokens=100)
        self.assertEqual(len(out), 1)
        self.assertEqual(client.messages.create.call_count, 2)
        second_kwargs = client.messages.create.call_args_list[1].kwargs
        self.assertGreaterEqual(second_kwargs["max_tokens"], 32000)
        self.assertTrue(tp.last_tool_meta().get("truncated_retried"))


class TestEmptyReasonSkipsOpus(unittest.TestCase):
    def test_sonnet_empty_reason_skips_opus(self):
        page = tp.RatePage(url="https://ex.com", content="Residential rates $0.10/kWh", content_hash="")
        with mock.patch.object(tp, "_select_model", return_value="sonnet"), \
                mock.patch.object(tp, "_call_claude_tool", return_value=[]), \
                mock.patch.object(tp, "last_tool_meta", return_value={"empty_reason": "no_residential_rates"}), \
                mock.patch.object(tp, "_call_opus_tool") as opus:
            result, tier = tp._extract_with_model_routing("prompt", page)
        self.assertEqual(result, [])
        self.assertEqual(tier, "sonnet")
        opus.assert_not_called()


class TestTwopassIdentifySharedSections(unittest.TestCase):
    def test_parse_object_and_legacy_array(self):
        plans, shared = tp._parse_twopass_identify(
            '{"plans":[{"name":"Farm & Home","customer_class":"residential",'
            '"location_hint":"Farm & Home Service"}],'
            '"shared_sections":[{"kind":"riders","location_hint":"Fuel Adjustment"}]}'
        )
        self.assertEqual(len(plans), 1)
        self.assertEqual(plans[0]["name"], "Farm & Home")
        self.assertEqual(shared[0]["kind"], "riders")

        plans2, shared2 = tp._parse_twopass_identify(
            '[{"name":"R","customer_class":"residential","location_hint":"Rate R"}]'
        )
        self.assertEqual(len(plans2), 1)
        self.assertEqual(shared2, [])

    def test_shared_blob_appends_hinted_slice(self):
        content = (
            "Table of contents\nFarm & Home Service .... 12\n"
            + ("x" * 5000)
            + "\nFuel Adjustment Clause\nFuel rider 2.5 mills/kWh applies to all residential.\n"
            + ("y" * 2000)
            + "\nFarm & Home Service\nEnergy 10.000 cents/kWh. Customer $15.\n"
        )
        blob = tp._twopass_shared_blob(
            content, [{"kind": "riders", "location_hint": "Fuel Adjustment Clause"}],
        )
        self.assertIn("Fuel Adjustment", blob)
        self.assertIn("2.5 mills", blob)


class TestTodayInUserMessages(unittest.TestCase):
    def test_today_not_in_cached_system(self):
        self.assertNotIn("{today}", tp.EXTRACTION_SYSTEM_PROMPT)
        self.assertNotIn("TODAY:", tp.EXTRACTION_SYSTEM_PROMPT)
        self.assertNotIn("Jan 1 2027", tp.EXTRACTION_SYSTEM_PROMPT)

    def test_format_extraction_user_stamps_today(self):
        u = tp.format_extraction_user(
            utility_name="Co-op", state="IA", content="rates",
            today="2026-10-07",
        )
        self.assertTrue(u.startswith("TODAY: 2026-10-07"))


class TestFutureDatedEffectiveness(unittest.TestCase):
    def test_is_currently_effective_excludes_future(self):
        today = date(2026, 10, 7)
        live_now = SimpleNamespace(
            superseded_by_tariff_id=None, supersede_reason=None,
            effective_date=date(2026, 3, 1),
        )
        future = SimpleNamespace(
            superseded_by_tariff_id=None, supersede_reason=None,
            effective_date=date(2026, 11, 1),
        )
        self.assertTrue(th.is_live(future))
        self.assertTrue(th.is_currently_effective(live_now, today=today))
        self.assertFalse(th.is_currently_effective(future, today=today))

    def test_vintage_keeper_prefers_currently_effective(self):
        today = date.today()
        interim = SimpleNamespace(
            id=1, effective_date=today - timedelta(days=30),
            approved=False, confidence_factors=None,
            last_verified_at=None,
        )
        future = SimpleNamespace(
            id=2, effective_date=today + timedelta(days=60),
            approved=False, confidence_factors=None,
            last_verified_at=None,
        )
        keeper = tp.choose_vintage_keeper([interim, future])
        self.assertIs(keeper, interim)


class TestClosedAndDeliveryScopeInStoreFactors(unittest.TestCase):
    def test_phase4_preserves_model_needs_review_flags(self):
        t = _et(
            needs_review=True,
            missing_fields=["winter off-peak hours"],
            riders_referenced_not_shown=["FAM"],
            # R21: a lone "delivery_only" plan is never stored (no half-plans,
            # see test_r21_supply_delivery); scope is still carried through.
            energy_scope="delivery_plus_default_supply",
            closed_to_new=True,
            components=[
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 10.0},
                {
                    "component_type": "adjustment", "unit": "¢/kWh", "rate_value": 0.5,
                    "tier_label": "Fuel", "included_in_energy": True,
                },
            ],
        )
        logging.disable(logging.CRITICAL)
        try:
            _report, valid = tp.phase4_validate([t], "Test Co-op", "IA")
        finally:
            logging.disable(logging.NOTSET)
        self.assertEqual(len(valid), 1)
        self.assertTrue(valid[0].needs_review)
        self.assertEqual(valid[0].energy_scope, "delivery_plus_default_supply")
        self.assertTrue(valid[0].closed_to_new)


class TestHaikuQualityEscalate(unittest.TestCase):
    def test_tou_missing_clocks_escalates(self):
        bad = [{
            "name": "TOU", "customer_class": "residential", "rate_type": "tou",
            "confidence": 0.9,
            "components": [
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 10.0,
                 "period_label": "On-Peak"},
            ],
        }]
        self.assertTrue(tp._haiku_result_needs_escalate(bad))

    def test_flat_ok_does_not_escalate(self):
        ok = [{
            "name": "Flat", "customer_class": "residential", "rate_type": "flat",
            "confidence": 0.9,
            "components": [
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 10.0},
            ],
        }]
        self.assertFalse(tp._haiku_result_needs_escalate(ok))

    def test_haiku_bad_output_escalates_to_sonnet(self):
        page = tp.RatePage(url="https://ex.com", content="Energy 10 cents/kWh peak", content_hash="")
        bad_haiku = [{
            "name": "TOU", "customer_class": "residential", "rate_type": "tou",
            "confidence": 0.9,
            "components": [
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 10.0},
            ],
        }]
        good_sonnet = [{
            "name": "TOU", "customer_class": "residential", "rate_type": "tou",
            "confidence": 0.95,
            "components": [
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 10.0,
                 "period_start_time": "16:00", "period_end_time": "21:00", "day_type": "weekday"},
                {"component_type": "energy", "unit": "¢/kWh", "rate_value": 5.0,
                 "period_start_time": "21:00", "period_end_time": "16:00", "day_type": "weekday"},
            ],
        }]

        def fake_call(prompt, model=None, **kw):
            if model == tp.HAIKU_MODEL:
                return bad_haiku
            return good_sonnet

        with mock.patch.object(tp, "_select_model", return_value="haiku"), \
                mock.patch.object(tp, "_call_claude_tool", side_effect=fake_call), \
                mock.patch.object(tp, "_call_opus_tool") as opus:
            result, tier = tp._extract_with_model_routing("prompt", page)
        self.assertEqual(tier, "sonnet")
        self.assertEqual(result, good_sonnet)
        opus.assert_not_called()


class TestScreenshotTiling(unittest.TestCase):
    def test_tall_image_splits_into_tiles(self):
        from PIL import Image
        import io
        img = Image.new("RGB", (800, 4200), color=(255, 255, 255))
        buf = io.BytesIO()
        img.save(buf, format="JPEG")
        tiles = tp._tile_screenshot_jpeg(buf.getvalue(), tile_height=1400)
        self.assertGreaterEqual(len(tiles), 3)


class TestPhase5LinkRanking(unittest.TestCase):
    def test_rate_links_rank_before_nav_noise(self):
        links = [
            (f"https://u.example/page{i}", f"Menu item {i}") for i in range(60)
        ]
        links.append(("https://u.example/rates/residential.pdf", "Residential Rates PDF"))
        ranked = tp._rank_links_for_nav(links, limit=50)
        self.assertEqual(ranked[0][1], "Residential Rates PDF")
        self.assertIn("rates/residential.pdf", ranked[0][0])

    def test_www_alias_same_site(self):
        self.assertEqual(tp._host_key("www.example.com"), tp._host_key("example.com"))


class TestEffortPerCall(unittest.TestCase):
    def test_call_level_effort_beats_env(self):
        from app.services import anthropic_compat as ac
        import os
        os.environ["ANTHROPIC_EFFORT"] = "high"
        try:
            out = ac.adapt_request({
                "model": "claude-sonnet-5-5",
                "max_tokens": 1024,
                "messages": [],
                "output_config": {"effort": "low"},
            })
            self.assertEqual(out.get("output_config", {}).get("effort"), "low")
        finally:
            os.environ.pop("ANTHROPIC_EFFORT", None)

    def test_haiku_system_is_slimmer_than_full(self):
        self.assertLess(
            len(tp.HAIKU_EXTRACTION_SYSTEM_PROMPT),
            len(tp.EXTRACTION_SYSTEM_PROMPT),
        )
        self.assertIn("SOURCE ONLY", tp.HAIKU_EXTRACTION_SYSTEM_PROMPT)
        # Slim prompt keeps three short examples, not seven long ones.
        self.assertNotIn("Example 6", tp.HAIKU_EXTRACTION_SYSTEM_PROMPT)
        self.assertIn("Example 6", tp.EXTRACTION_SYSTEM_PROMPT)


class TestPinAndAuditFullPriceWording(unittest.TestCase):
    def test_arbiter_allows_all_in_energy_sums(self):
        from app.services import pin_adapters as pa
        self.assertIn("included_in_energy=true", pa.ARBITER_PROMPT)
        self.assertIn("sum of printed amounts", pa.ARBITER_PROMPT)

    def test_opus_audit_full_price_and_residential_only(self):
        from scripts import opus_audit
        self.assertIn("rider_missing_from_energy", opus_audit.AUDIT_PROMPT)
        self.assertIn("sum of printed", opus_audit.AUDIT_PROMPT)
        self.assertIn("Ignore commercial-only", opus_audit.AUDIT_PROMPT)
        self.assertNotIn("residential or small commercial", opus_audit.AUDIT_PROMPT)


if __name__ == "__main__":
    unittest.main()
