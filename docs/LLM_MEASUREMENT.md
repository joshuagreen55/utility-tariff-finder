# Measuring extraction quality and LLM spend before changing models

Model choices stay behind env vars (`HAIKU_MODEL`, `SONNET_MODEL`,
`OPUS_MODEL`, `AUDITOR_MODEL`). This page covers how to judge an env-only
swap honestly.

**Production defaults (2026-10-07):** Anthropic-only three-tier stack —
`claude-haiku-5-5` (tier 1) → `claude-sonnet-5-5` (tier 2) →
`claude-opus-5-5` (tier 3). Gemini Flash and Gemini Deep Research (Phase 6)
are removed from the runtime path.

## Role map (former → current)

| Role | Former default | Current default | Env var |
|---|---|---|---|
| Tier-1 cheap first pass over pages | `gemini-3.8-flash` | `claude-haiku-5-5` | `HAIKU_MODEL` |
| Tier-2 main extract, PDF/screenshot vision, Phase 5 nav, two-pass, Track B, browser CLI | `claude-haiku-4-5-20251001` | `claude-sonnet-5-5` | `SONNET_MODEL` |
| Tier-3 escalation + long-doc identify, auditor / pin arbiter | `claude-opus-5` | `claude-opus-5-5` | `OPUS_MODEL` / `AUDITOR_MODEL` |

## What changed (audit F7 / F8 + 2026-10 Anthropic-only)

| Gap | Now |
|---|---|
| LLM extraction cache keyed on tier name, so an env model swap replayed the old model's output | Key = content hash + tier + **concrete model id(s)** + prompt version (`_cache_model_id`). `twopass` keys on both Sonnet and Opus ids. |
| Tier "hit" meant "returned anything" | `tier_outcomes` is kept for continuity, and `tier_acceptance` adds per-tier tariffs **returned into Phase 4 vs accepted after it**. Every extracted tariff carries `extraction_tier` (`haiku` / `sonnet` / `opus` / `twopass` / `vision`). |
| Long-document Opus *identify* spend counted as "wasted escalation" | Tagged `phase3_identify`; the report's wasted-Opus estimate excludes it. |
| Forced `tool_choice` turned thinking off on 5.5 models | `anthropic_compat` rewrites forced tool use to `auto` + "call the tool exactly once" for Haiku 5.5 / Sonnet 5.5 / Opus 5.5, and lists `haiku-5` under thinking-default-on. |
| Haiku 5.5 priced as Haiku 4.5 (~10× too high) | Explicit `claude-haiku-5-5` row at $0.10/$0.50 (≤100k prompts). |
| Track B, campaign Track B, `opus_audit`, browser CLI unmetered | Metered into `llm_cost`. Script runs append to `logs/llm_cost_ledger.jsonl`, which `llm_cost_report.py` rolls up next to refresh runs. |
| `opus_audit.py` hardcoded old Opus, read superseded rows | Model from `AUDITOR_MODEL` or `OPUS_MODEL`; live rows only. |
| `browser_interaction.py` CLI hardcoded Haiku 3.5 | Uses `SONNET_MODEL`. |
| Gemini tier-1 + Phase 6 Deep Research | Removed. No `GOOGLE_AI_API_KEY` / `GEMINI_MODEL` / `google-genai` runtime path. |

### Cache transition cost
The cache key format changed with the model swap (and prompt version is
`v5` for the regulator-attribution prompt tweak), so existing entries are
misses. Pages whose fingerprints are unchanged still skip the LLM entirely
(fingerprint skip). Only pages re-extracted for another reason pay again,
once. To avoid that for one run, set `LLM_CACHE_LEGACY_READ=1`. It also
reads pre-change entries, which may replay another model's output, so do
not use it for probes.

## TOU / seasonal gold set

`tests/fixtures/ground_truth_tou_seasonal.json` (v1.1, 12 tariffs):

| Utility | Code | Shape | Effective | Source |
|---|---|---|---|---|
| Hydro One (ON) | OEB-RPP-TOU / -TIERED / -ULO | seasonal_tou, seasonal_tiered, tou | 2025-11-01 | [OEB RPP prices](https://www.oeb.ca/consumer-information-and-protection/electricity-rates) |
| Nova Scotia Power (1739) | 02/03/04, 80 | flat, seasonal_tou | 2026-05-01 | [NS Power tariff book May 2026](https://www.nspower.ca/docs/default-source/regulatory/tariff-book-2026.pdf) |
| Newfoundland Power, NL Hydro | 1.1S | seasonal | 2026-07-01 | [NL Hydro schedule Jul 2026](https://nlhydro.com/wp-content/uploads/2026/07/Schedule-of-Rates-Rules-and-Regulations_Jul_2026.pdf) |
| **Nova Scotia Power (1739)** | **05/06** TOD | seasonal_tou | 2026-05-01 | NS Power tariff book May 2026, p.6 + FAM / DCRR / SCRR rider tables |
| **Toronto Hydro (ON)** | **OEB-RPP-TOU** | seasonal_tou | 2025-11-01 | OEB RPP prices (commodity only) |
| **San Diego Gas & Electric (1000)** | **EV-TOU-5** | seasonal_tou | 2026-08-01 | [EV-TOU-5 total rates 8/1/2026](https://www.sdge.com/sites/default/files/regulatory/8-1-26%20Schedule%20EV-TOU-5%20Total%20Rates%20Table.pdf) + periods/seasons on [Total Electric Rates](https://www.sdge.com/total-electric-rates) |
| **Pacific Gas & Electric Co. (CA)** | **EV2-A** | seasonal_tou | 2026-06-01 | [PG&E Schedule EV2](https://www.pge.com/tariffs/assets/pdf/tariffbook/ELEC_SCHEDS_EV2%20(Sch).pdf) Sheet 2 + Special Conditions 1–2 |
| **Salt River Project (AZ)** | **E-28** | seasonal_tou | 2026-05-01 | [SRP ratebook with Temporary FPPAM](https://www.srpnet.com/assets/srpnet/pdf/price-plans/2025-Ratebook-with-Temporary-FPPAM.pdf), E-28 |

Bold rows were added for issue #19. Every clock window and season date is
taken from source text. Each `gold_note` records the judgement calls.

**Ontario note:** production routes ON utilities through the deterministic
OEB scraper (`CENTRALIZED_PROVINCES={"ON"}`). The gold set still exercises
the LLM fallback on the OEB page; the attribution prompt now treats
province-wide regulator commodity prices as attributable to LDCs in that
province, and `_page_has_numeric_rates` accepts bare decimals under a
¢/kWh heading so Opus can escalate when the middle tier returns empty.

## How to run a model probe (dry-run, no DB writes)

1. **Baseline (read-only, on the VM):**
   `python /app/scripts/llm_cost_report.py --runs 5` and
   `python -m scripts.benchmark --output /tmp/bench_baseline.json`.
2. **Probe with the candidate model** in an isolated log dir so no cache or
   probe output leaks between configs:
   ```bash
   APP_LOG_DIR=/tmp/probe_opus_next OPUS_MODEL=<candidate id> \
     LLM_PRICING_JSON='{"claude-opus-5-5": {"in": 4.0, "out": 20.0}}' \
     python -m scripts.opus_yield_probe <ids>
   ```
   Use a stratified id list: flat, tiered, TOU, seasonal-TOU, scanned PDF,
   and a long rate book. Always set `LLM_PRICING_JSON` for a new model
   family; unknown families price at $0.
3. **Score correctness, not non-emptiness:** compare `tier_acceptance`
   (accepted ÷ returned) and benchmark component recall on the TOU/seasonal
   gold at strict tolerance. Also check computable agreement.
4. **Cost per correct tariff** = probe `total_usd` ÷ gold-correct tariffs.

## Wave 6 / Anthropic-only: gold-driven extraction quality

### Three ways to run the gold set

| Command | Needs | Answers |
|---|---|---|
| `python -m scripts.gold_replay --forms` | nothing (CI step) | If a model returned the book exactly, does the pipeline keep it? |
| `python -m scripts.gold_model_probe --no-cache --output X.json` | LLM keys, network; no DB | What do the env-selected models extract from the gold documents? |
| `python -m scripts.benchmark --fixtures tests/fixtures/ground_truth_tou_seasonal.json --output X.json [--baseline Y.json]` | live DB (VM) | What is live today? |

### Anthropic model compatibility (`app/services/anthropic_compat.py`)

All Anthropic calls (pipeline tiers, vision, nav, two-pass, Track B,
browser CLI, `opus_audit`, pin arbiter) go through it. Per the Claude docs
(checked 2026-10-07):

| Model | Thinking | Forced `tool_choice` | `temperature`/`top_p`/`top_k` |
|---|---|---|---|
| `claude-haiku-4-5-20251001` | off | OK | OK |
| `claude-haiku-5-5` | on by default (adaptive) | rewritten → `auto` + tool instruction (else thinking stays 0) | 400 |
| `claude-sonnet-5` | on by default, may be disabled | OK | 400 |
| `claude-sonnet-5-5` | on by default (adaptive) | rewritten → `auto` + tool instruction | 400 |
| `claude-opus-5` | on by default, may be disabled (≤ high effort) | OK | 400 |
| `claude-opus-5-5` | on, **cannot** be disabled | rewritten → `auto` + tool instruction | 400 |

The module strips sampling knobs, converts forced tool use where required,
raises `max_tokens` to `ANTHROPIC_THINKING_MIN_MAX_TOKENS` (16000) while
thinking is on (thinking counts against `max_tokens`), reads the first
*text* block (the first block may be a thinking block), and retries once
on a 400 that names one of these parameters. `ANTHROPIC_THINKING=disabled`
turns thinking off where allowed;
`ANTHROPIC_EFFORT` sets `output_config.effort`.

### Pricing (`DEFAULT_PRICING`, USD / MTok, docs.claude.com 2026-10-07)

| Key | In | Out | Cache hit | 5-min cache write |
|---|---:|---:|---:|---:|
| `haiku` (Haiku 4.5, legacy family) | 1.00 | 5.00 | 0.10 | 1.25 |
| `claude-haiku-5-5` (≤100k prompts) | 0.10 | 0.50 | 0.01 | 0.125 |
| `sonnet` (Sonnet 5 family) | 2.00 | 10.00 | 0.20 | 2.50 |
| `claude-sonnet-5-5` | 2.00 | 10.00 | 0.10 | 2.50 |
| `opus` (Opus 5 family) | 5.00 | 25.00 | 0.50 | 6.25 |
| `claude-opus-5-5` | 4.00 | 20.00 | 0.20 | 5.00 |

Haiku 5.5 prompts **over** 100k tokens are $0.50/$2.50 — rare in this
pipeline; override via `LLM_PRICING_JSON` if needed. A call is priced by
the longest matching model-id key, else its family, and rolls up under its
family. Sonnet 5.5 cache-read price follows the 2026-10-07 Haiku launch
cut ($0.10). Thinking tokens bill as output.

### Proposing an env model bump

1. On the VM, baseline: `python -m scripts.benchmark --fixtures
   tests/fixtures/ground_truth_tou_seasonal.json --output /tmp/gold_live.json`
   and `python /app/scripts/llm_cost_report.py --runs 5`.
2. Probe current defaults and the candidate on the same documents:
   `python -m scripts.gold_model_probe --no-cache --output /tmp/gold_base.json`, then
   e.g. `SONNET_MODEL=<candidate> python -m scripts.gold_model_probe --no-cache --output /tmp/gold_cand.json`
   (one knob per run).
3. `python -m scripts.gold_model_probe --compare /tmp/gold_base.json /tmp/gold_cand.json`.
4. Promote a default only if computable agreement rises (or holds, for a
   cheaper model) with no rise in rate errors, and projected spend stays in
   the ~$2k/year intent. Put the table in the PR.
5. Change the env (VM `.env`) first; change code defaults only in a PR
   with that table. Restart `celery-worker celery-beat api`.

## VM deploy steps (Anthropic-only flip)

1. Sync this PR's code to the VM (`./deploy/sync-to-vm.sh`). Dependency
   change (`google-genai` removed) needs `./deploy/sync-to-vm.sh --rebuild`
   once, or leave the unused package in the image until the next rebuild.
2. In the VM `.env`, set (or confirm):
   ```
   HAIKU_MODEL=claude-haiku-5-5
   SONNET_MODEL=claude-sonnet-5-5
   OPUS_MODEL=claude-opus-5-5
   PHASE6_ENABLED=0
   ```
   Remove (or leave unused): `GEMINI_MODEL`, `GEMINI_TIMEOUT_MS`,
   `GOOGLE_AI_API_KEY`, `PHASE6_AGENT`. `ANTHROPIC_API_KEY` stays as-is.
   Optional: `LLM_PRICING_JSON` only if you need the >100k Haiku 5.5 tier.
3. Restart so process-start imports reload:
   `docker compose restart celery-worker celery-beat api`.
4. Caps / schedules unchanged: do not touch `MONTHLY_MAX_UTILITIES` or
   beat schedules.

## Gold set growth

Now 12 tariffs (9 Canadian, 3 US TOU). Keep growing toward 30–40 with
more US TOU / seasonal shapes, labelled from source documents and
human-checked.
