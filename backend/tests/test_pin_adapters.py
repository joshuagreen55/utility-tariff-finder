"""Jev claim verifier + Opus arbiter adapters (issue #20). No network, no DB.

Mercury and Anthropic are mocked with responses shaped like the real ones
(``jev_verify`` rows with verdict / confidence / action; ``jev_screen``
recommendation; Anthropic ``tool_use`` blocks). The end-to-end gate runs with
these adapters live in tests/test_corrections_and_pins.py (DB-backed).
"""
from __future__ import annotations

import json
import os
import unittest
from unittest import mock

from app.services import pin_adapters as pa
from app.services import pin_verification as pv


def jev_row(i, claim, verdict="verified", confidence=0.99, action="auto"):
    return {"id": f"claim{i}", "claim": claim, "verdict": verdict, "confidence": confidence,
            "action": action, "probabilities": {}, "supporting_evidence": None}


class FakeMercury:
    """Stands in for HttpMercuryClient. ``verdicts`` maps a substring of a
    claim to (verdict, confidence, action); anything else is verified@0.99."""

    def __init__(self, verdicts=None, screen_action="pass", fail=False):
        self.verdicts = verdicts or {}
        self.screen_action = screen_action
        self.fail = fail
        self.calls: list[tuple[str, dict]] = []

    def call(self, tool, arguments):
        self.calls.append((tool, arguments))
        if self.fail:
            raise pa.MercuryError("HTTP 503 on tools/call")
        usage = {"input_tokens": 500, "output_tokens": 50}
        cost = {"basis": "gateway_reported", "usd": 0.0}
        if tool == "jev_screen":
            return {"recommendation": {"action": self.screen_action, "reason": "x"},
                    "probabilities": {"injection": 0.03}, "rows": [], "usage": usage, "cost": cost}
        rows = []
        for i, claim in enumerate(arguments["claims"]):
            match = next((v for k, v in self.verdicts.items() if k in claim), ("verified", 0.99, "auto"))
            rows.append(jev_row(i, claim, *match))
        return {"rows": rows, "usage": usage, "cost": cost, "complete": True}


class FakeResponse:
    def __init__(self, status_code=200, body=None, headers=None, text=None):
        self.status_code = status_code
        self._body = body
        self.headers = headers or {"content-type": "application/json"}
        self.text = text if text is not None else json.dumps(body)

    def json(self):
        return self._body


def anthropic_tool_reply(accept=True, reason="matches the document", issues=None):
    return FakeResponse(body={
        "content": [{"type": "tool_use", "name": "record_verdict",
                     "input": {"accept": accept, "reason": reason, "issues": issues or []}}],
        "usage": {"input_tokens": 3000, "output_tokens": 80},
    })


class FakeAnthropic:
    def __init__(self, response):
        self.response = response
        self.calls: list[dict] = []

    def __call__(self, url, **kw):
        self.calls.append({"url": url, **kw})
        return self.response


def opus_arbiter(accept=True, **kw):
    return pa.OpusArbiter("test-key-not-a-secret", model="claude-opus-test",
                          post=FakeAnthropic(anthropic_tool_reply(accept, **kw)))


CLAIMS = [pv.Claim("rate", "On-Peak: energy charge 0.203 $/kWh", 0),
          pv.Claim("clock", "On-Peak applies 07:00–11:00 on weekday days", 0)]


class TestDefaultGates(unittest.TestCase):
    ENV = {"MERCURY_URL": "https://mercury.example/mcp", "MERCURY_API_TOKEN": "t",
           "ANTHROPIC_API_KEY": "k"}

    def gates(self, **env):
        with mock.patch.dict(os.environ, env, clear=True):
            return pv.default_gates()

    def test_defaults_are_null(self):
        g = self.gates()
        self.assertIsInstance(g.verifier, pv.NullVerifier)
        self.assertIsInstance(g.arbiter, pv.NullArbiter)
        g = self.gates(**self.ENV)
        self.assertIsInstance(g.verifier, pv.NullVerifier)
        self.assertIsInstance(g.arbiter, pv.NullArbiter)

    def test_jev_and_opus_selected_by_env(self):
        g = self.gates(PIN_VERIFIER="jev", PIN_ARBITER="Opus", OPUS_MODEL="claude-opus-x", **self.ENV)
        self.assertIsInstance(g.verifier, pa.JevVerifier)
        self.assertIsInstance(g.arbiter, pa.OpusArbiter)
        self.assertEqual(g.arbiter.model, "claude-opus-x")
        self.assertEqual(g.verifier.client.url, "https://mercury.example/mcp")

    def test_auditor_model_overrides_opus_model(self):
        g = self.gates(PIN_ARBITER="opus", OPUS_MODEL="a", AUDITOR_MODEL="b", ANTHROPIC_API_KEY="k")
        self.assertEqual(g.arbiter.model, "b")

    def test_missing_credentials_or_unknown_names_fall_back_to_null(self):
        for env in ({"PIN_VERIFIER": "jev"}, {"PIN_VERIFIER": "jev", "MERCURY_URL": "https://m"},
                    {"PIN_ARBITER": "opus"}, {"PIN_VERIFIER": "gpt", "PIN_ARBITER": "haiku", **self.ENV}):
            with self.subTest(env=env):
                g = self.gates(**env)
                self.assertFalse(g.verifier.available or g.arbiter.available)


class TestJevVerifier(unittest.TestCase):
    def test_screen_passes_only_on_pass(self):
        for action, expected in (("pass", True), ("review", False), ("block", False), ("skip", False)):
            with self.subTest(action=action):
                v = pa.JevVerifier(FakeMercury(screen_action=action))
                self.assertIs(v.screen("On-peak 20.3¢/kWh"), expected)
                self.assertEqual(v.last_screen["action"], action)

    def test_screen_covers_every_chunk_and_refuses_oversized_documents(self):
        m = FakeMercury()
        v = pa.JevVerifier(m, chunk_chars=10, max_chunks=3)
        self.assertTrue(v.screen("x" * 30))
        self.assertEqual([t for t, _ in m.calls], ["jev_screen"] * 3)
        m.calls.clear()
        self.assertFalse(v.screen("x" * 31))
        self.assertEqual(m.calls, [])
        self.assertEqual(v.last_screen["action"], "too_long")

    def test_verify_maps_verdicts_and_sends_atomic_claims(self):
        m = FakeMercury({"07:00": ("contradicted", 0.97, "auto")})
        v = pa.JevVerifier(m)
        out = v.verify("doc", CLAIMS)
        self.assertEqual([(o.verdict, o.confidence) for o in out], [("verified", 0.99), ("contradicted", 0.97)])
        tool, args = m.calls[0]
        self.assertEqual(tool, "jev_verify")
        self.assertEqual(args["claims"], [c.text for c in CLAIMS])
        self.assertEqual(args["auto_accept"], 0.90)
        self.assertEqual(args["evidence"], [{"id": "doc-0", "text": "doc"}])

    def test_review_is_never_verified_and_bad_rows_are_unsupported(self):
        rows = [jev_row(0, CLAIMS[0].text, "verified", 0.84, "review"),
                jev_row(1, CLAIMS[1].text, "invalid_response", 0.9)]
        m = FakeMercury()
        m.call = lambda tool, args: {"rows": rows}
        out = pa.JevVerifier(m).verify("doc", CLAIMS)
        self.assertEqual([o.verdict for o in out], ["unsupported", "unsupported"])
        self.assertEqual(pv.claims_failure(out), "claims_unsupported")
        m.call = lambda tool, args: {"rows": []}
        self.assertEqual([o.verdict for o in pa.JevVerifier(m).verify("doc", CLAIMS)],
                         ["unsupported", "unsupported"])

    def test_verify_batches_claims(self):
        m = FakeMercury()
        claims = [pv.Claim("rate", f"Tier {i}: energy charge 0.1{i} $/kWh") for i in range(5)]
        out = pa.JevVerifier(m, claims_per_call=2).verify("doc", claims)
        self.assertEqual(len(out), 5)
        self.assertEqual([len(a["claims"]) for _, a in m.calls], [2, 2, 1])
        self.assertEqual([o.claim for o in out], claims)


class TestOpusArbiter(unittest.TestCase):
    def decide(self, arbiter):
        return arbiter.decide(current={"tariff_id": 1}, proposal={"components": []},
                              document_text="Energy 21.0¢/kWh", previous_text=None)

    def test_typed_accept_and_reject(self):
        a = opus_arbiter(True)
        verdict = self.decide(a)
        self.assertEqual((verdict.accept, verdict.model), (True, "claude-opus-test"))
        body = a._post.calls[0]["json"]
        self.assertEqual(body["model"], "claude-opus-test")
        self.assertEqual(body["tool_choice"], {"type": "tool", "name": "record_verdict"})
        self.assertIn("Energy 21.0¢/kWh", body["messages"][0]["content"])
        rejected = self.decide(opus_arbiter(False, reason="winter column", issues=["future rate"]))
        self.assertFalse(rejected.accept)
        self.assertIn("future rate", rejected.reason)

    def test_fails_closed(self):
        no_tool = FakeResponse(body={"content": [{"type": "text", "text": "accept"}], "usage": {}})
        self.assertFalse(self.decide(pa.OpusArbiter("k", model="m", post=FakeAnthropic(no_tool))).accept)
        truthy = anthropic_tool_reply(accept="yes")
        self.assertFalse(self.decide(pa.OpusArbiter("k", model="m", post=FakeAnthropic(truthy))).accept)
        with self.assertRaises(RuntimeError):
            self.decide(pa.OpusArbiter("k", model="m", post=FakeAnthropic(FakeResponse(529, {}))))

    def test_document_is_capped(self):
        a = pa.OpusArbiter("k", model="m", max_chars=5, post=FakeAnthropic(anthropic_tool_reply()))
        a.decide(current={}, proposal={}, document_text="ABCDEFGHIJ", previous_text=None)
        prompt = a._post.calls[0]["json"]["messages"][0]["content"]
        self.assertIn("ABCDE", prompt)
        self.assertNotIn("ABCDEF", prompt)


class TestHttpMercuryClient(unittest.TestCase):
    ENVELOPE = {"capability": "jev.verify", "decision": "allow", "effect": "read",
                "data": {"rows": [jev_row(0, "c")], "usage": {}}}

    def run_call(self, tool_response):
        responses = [
            FakeResponse(body={"jsonrpc": "2.0", "id": 1, "result": {}},
                         headers={"content-type": "application/json", "mcp-session-id": "s1"}),
            FakeResponse(202, None, text=""),
            tool_response,
        ]
        sent = []

        def post(url, headers, json, timeout):
            sent.append((headers, json))
            return responses.pop(0)

        with mock.patch.object(pa.httpx, "post", post):
            data = pa.HttpMercuryClient("https://m.example/mcp", "tok").call("jev_verify", {"claims": ["c"]})
        return data, sent

    def test_initializes_then_calls_tool_with_session(self):
        body = {"jsonrpc": "2.0", "id": 2,
                "result": {"content": [{"type": "text", "text": json.dumps(self.ENVELOPE)}]}}
        data, sent = self.run_call(FakeResponse(body=body))
        self.assertEqual(data["rows"][0]["verdict"], "verified")
        self.assertEqual([j["method"] for _, j in sent], ["initialize", "notifications/initialized", "tools/call"])
        self.assertEqual(sent[2][1]["params"], {"name": "jev_verify", "arguments": {"claims": ["c"]}})
        self.assertEqual(sent[2][0]["Mcp-Session-Id"], "s1")
        self.assertEqual(sent[2][0]["Authorization"], "Bearer tok")

    def test_event_stream_and_structured_content(self):
        msg = {"jsonrpc": "2.0", "id": 2, "result": {"structuredContent": self.ENVELOPE}}
        sse = FakeResponse(headers={"content-type": "text/event-stream"},
                           text=f"event: message\ndata: {json.dumps(msg)}\n\n")
        data, _ = self.run_call(sse)
        self.assertEqual(data["rows"][0]["id"], "claim0")

    def test_errors_raise(self):
        denied = {**self.ENVELOPE, "decision": "deny"}
        for result in ({"isError": True, "content": [{"type": "text", "text": "nope"}]},
                       {"structuredContent": denied}):
            with self.subTest(result=result), self.assertRaises(pa.MercuryError):
                self.run_call(FakeResponse(body={"jsonrpc": "2.0", "id": 2, "result": result}))
        with self.assertRaises(pa.MercuryError):
            self.run_call(FakeResponse(401, {}))


class TestPartsClaims(unittest.TestCase):
    def test_all_in_is_verified_through_base_and_riders(self):
        claims = pv.build_claims({"components": [
            {"component_type": "energy", "unit": "$/kWh", "rate_value": "0.19128",
             "tier_label": "All-in (base + FAM + DSM)"},
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": "0.00156",
             "tier_label": "FAM", "included_in_energy": True},
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": "0.00648",
             "tier_label": "DSM", "included_in_energy": True},
        ]})
        self.assertEqual([c.kind for c in claims], ["rate", "rate", "rate"])
        self.assertEqual(claims[0].text, "Energy: base energy charge 0.18324 $/kWh before riders")
        self.assertIn("0.00156", claims[1].text)

    def test_all_in_without_riders_stays_derived(self):
        (claim,) = pv.build_claims({"components": [
            {"component_type": "energy", "unit": "$/kWh", "rate_value": "0.19128", "tier_label": "All-in"}]})
        self.assertEqual(claim.kind, "derived_rate")

    def test_riders_only_apply_to_matching_season(self):
        claims = pv.build_claims({"components": [
            {"component_type": "energy", "unit": "$/kWh", "rate_value": "0.2", "tier_label": "All-in",
             "season": "Winter"},
            {"component_type": "adjustment", "unit": "$/kWh", "rate_value": "0.01",
             "season": "Summer", "included_in_energy": True},
        ]})
        self.assertEqual(claims[0].kind, "derived_rate")


if __name__ == "__main__":
    unittest.main()
