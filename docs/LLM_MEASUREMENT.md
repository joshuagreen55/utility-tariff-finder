# Measuring extraction quality and LLM spend before changing models

Model choices stay behind env vars (`GEMINI_MODEL`, `HAIKU_MODEL`,
`OPUS_MODEL`, `AUDITOR_MODEL`, `PHASE6_AGENT`). This page covers how to
judge an env-only swap honestly. **Production defaults are unchanged by
this work.**

## What changed (audit F7 / F8)

| Gap | Now |
|---|---|
| LLM extraction cache keyed on tier name, so an env model swap replayed the old model's output | Key = content hash + tier + **concrete model id(s)** + prompt version (`_cache_model_id`). `twopass` keys on both Haiku and Opus ids. |
| Tier "hit" meant "returned anything" | `tier_outcomes` is kept for continuity, and `tier_acceptance` adds per-tier tariffs **returned into Phase 4 vs accepted after it**. Every extracted tariff carries `extraction_tier` (`gemini` / `haiku` / `opus` / `twopass` / `vision` / `gemini_dr`). |
| Long-document Opus *identify* spend counted as "wasted escalation" | Tagged `phase3_identify`; the report's wasted-Opus estimate excludes it. |
| Phase 6 spend unrecorded on timeout / token cap / poll error | `_phase6_meter` prices partial usage on every exit. `aborts.gemini_dr` counts aborts, and `unpriced` marks aborts with no reported usage. |
| Track B, campaign Track B, `opus_audit`, browser CLI unmetered | Metered into `llm_cost`. Script runs append to `logs/llm_cost_ledger.jsonl`, which `llm_cost_report.py` rolls up next to refresh runs. |
| `opus_audit.py` hardcoded `claude-opus-4-20250514`, read superseded rows, ignored clocks/seasons | Model from `AUDITOR_MODEL` or `OPUS_MODEL`; live rows only; prompt includes clock windows, day types, season dates and `included_in_energy`. |
| `browser_interaction.py` CLI hardcoded Haiku 3.5 | Uses `HAIKU_MODEL`. |
| Benchmark: 35 flat/tiered tariffs, 15% tolerance, 0 TOU/seasonal, superseded rows counted | Adds `tests/fixtures/ground_truth_tou_seasonal.json` (7 residential TOU / seasonal / seasonal-tiered tariffs across Hydro One, NS Power, Newfoundland Power and NL Hydro). It matches **exactly** to 1e-5 $/kWh, with exact clock windows, day types, season dates and tier bounds, and reports computable-contract agreement. The benchmark reads live rows only. |

### Cache transition cost
The cache key format changed, so existing entries are misses. Pages whose
fingerprints are unchanged still skip the LLM entirely (fingerprint skip).
Only pages re-extracted for another reason pay again, once. To avoid that
for one run, set `LLM_CACHE_LEGACY_READ=1`. It also reads pre-change
entries, which may replay another model's output, so do not use it for probes.

## How to run a model probe (dry-run, no DB writes)

1. **Baseline (read-only, on the VM):**
   `python /app/scripts/llm_cost_report.py --runs 5` and
   `python -m scripts.benchmark --output /tmp/bench_baseline.json`.
2. **Probe with the candidate model** in an isolated log dir so no cache or
   probe output leaks between configs:
   ```bash
   APP_LOG_DIR=/tmp/probe_opus_next OPUS_MODEL=<candidate id> \
     LLM_PRICING_JSON='{"opus": {"in": <list>, "out": <list>}}' \
     python -m scripts.opus_yield_probe <ids>
   ```
   Use a stratified id list: flat, tiered, TOU, seasonal-TOU, scanned PDF,
   and a long rate book. Always set `LLM_PRICING_JSON` for a new model
   family; unknown families price at $0.
3. **Score correctness, not non-emptiness:** compare `tier_acceptance`
   (accepted ÷ returned) and benchmark component recall on the TOU/seasonal
   gold at strict tolerance. Also check computable agreement.
4. **Cost per correct tariff** = probe `total_usd` ÷ gold-correct tariffs.

## Follow-up A/B note (not done here)

Once a baseline with these metrics exists:

- **Tier 3:** try the newest Opus id via `OPUS_MODEL` with the same cap
  (`OPUS_MAX_PER_UTILITY=2`); set `LLM_PRICING_JSON.opus` to its list price.
- **Tier 2:** a global `HAIKU_MODEL` swap changes seven call sites (tier-2
  extraction, vision, nav, identify, Track B, browser CLI). A/B a stronger
  mid-tier on two-pass and vision only via a new per-site knob (code change).
- **Tier 1:** keep Gemini Flash; add `response_schema` (code) and measure
  JSON-shape failures.
- **Gold set:** extend beyond 7 Canadian tariffs to 30–40 with US TOU and
  seasonal shapes, labelled by a human from source documents (audit §7.3).
