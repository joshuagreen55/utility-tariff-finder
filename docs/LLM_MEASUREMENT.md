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
| Benchmark: 35 flat/tiered tariffs, 15% tolerance, 0 TOU/seasonal, superseded rows counted | Adds `tests/fixtures/ground_truth_tou_seasonal.json`, now **12** residential TOU / seasonal / seasonal-tiered tariffs across 8 utilities (Canada and US; see "TOU / seasonal gold set" below). It matches **exactly** to 1e-5 $/kWh, with exact clock windows, day types, season dates and tier bounds, and reports computable-contract agreement. The benchmark reads live rows only. |

### Cache transition cost
The cache key format changed, so existing entries are misses. Pages whose
fingerprints are unchanged still skip the LLM entirely (fingerprint skip).
Only pages re-extracted for another reason pay again, once. To avoid that
for one run, set `LLM_CACHE_LEGACY_READ=1`. It also reads pre-change
entries, which may replay another model's output, so do not use it for probes.

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
taken from source text. Each `gold_note` records the judgement calls:

- **All-in energy.** NS TOD adds the Domestic riders (FAM 0.156¢ + DCRR
  0.648¢ + SCRR 0.000¢), like codes 02/03/04 and 80. SDG&E, PG&E and SRP
  publish bundled totals, which are used as-is.
- **Weekends and holidays.** NS TOD bills Saturdays, Sundays and statutory
  holidays at the 11 PM–7 AM rate, the same pattern as code 80 Note 1.
  SDG&E and SRP also have `holiday` rows, so these tariffs warn
  `holiday_rows_require_calendar`. PG&E EV2 periods apply every day, so its
  rows use `day_type=all`.
- **Billing-cycle seasons.** SRP defines seasons by billing cycle. The gold
  encodes them as calendar months.
- **Commodity only.** Toronto Hydro is the OEB RPP commodity price only
  (warning `commodity_only_bill_incomplete`), the same as Hydro One.
- **Fixed charges.** Where the fixed charge depends on the customer, the gold
  uses the standard tier: the $0.79343/day non-CARE Base Services Charge
  for SDG&E and PG&E. SRP's amperage-tiered service charge is omitted.

These were considered and **not** added:

- **NS MURB 89.** Note 1 says the "applicable peak price" applies all
  weekend hours in both seasons. That is ambiguous next to winter's two
  on-peak windows and opposite to code 80.
- **SCE TOU-D PRIME.** The Cal. PUC sheet could not be retrieved, and the
  public pages round prices to the cent.
- **SDG&E TOU-DR1 / TOU-DR2.** Both have an "up to 130% of baseline" credit
  that the computable contract does not model.
- **SRP E-26.** It mixes calendar-date clock seasons with billing-cycle
  price seasons, and it is frozen to new customers.

The US entries are labelled from the utilities' own published schedules
but have not yet been independently re-checked by a human.

`tests/test_measurement.py` ties the NS TOD rates to
`build_domestic_tod_components()`. It also ties Toronto Hydro to the Hydro
One OEB TOU gold, requires every `expect_computable` to equal the
`evaluate_computable` verdict, and requires every tariff to self-score 1.0 /
1.0 under strict structural comparison.

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

## Wave 6: gold-driven extraction quality (issue #24)

### Three ways to run the gold set

| Command | Needs | Answers |
|---|---|---|
| `python -m scripts.gold_replay --forms` | nothing (CI step) | If a model returned the book exactly, does the pipeline keep it? Replays each gold tariff through Phase 4 + component mapping, once as gold and once in the forms models print (cents, `7:00 a.m.`, inclusive `:59` ends, one "weekends and holidays" row, month names). Exits 1 on any miss. |
| `python -m scripts.gold_model_probe --no-cache --output X.json` | LLM keys, network; no DB | What do the env-selected models extract from the gold documents? Phase 3 + 4 on each `rate_url` (add pages with `--add-url "NAME=URL"`), strict score, failure taxonomy, computable, spend. `--compare A.json B.json` prints before/after. |
| `python -m scripts.benchmark --fixtures tests/fixtures/ground_truth_tou_seasonal.json --output X.json [--baseline Y.json]` | live DB (VM) | What is live today? Adds a per-gold scoreboard (top failure per tariff) and `gold_failure_modes`; `--baseline` exits 1 if computable agreement drops or rate / structure errors rise. |

Failure modes (per gold component, `benchmark.failure_taxonomy`):
`product_match` (no live row matched the gold name), `wrong_price` (a row
with the gold structure has another value), `missing_clock`, `day_type`,
`missing_season`, `tier_bounds` (the closest same-priced row differs in
that field), `missing_component` (no same-priced row), `extra_component`.

**Computable agreement** is the number of gold tariffs whose live (or
probed) rows get the same `evaluate_computable` verdict as the gold's
`expect_computable` (all `true` today). Rate errors and structure misses
count gold components; one tariff can carry many.

### What the replay found on `main` (pre-Wave 6)

A perfect extraction was kept computable for only **5/12** gold tariffs
(82 structure misses), and **1/12** in model-printed forms (223 structure
misses across both forms). Causes, all fixed here (the first row and the
mapping row are what the replay measured; the others were found reading the
code and are covered by unit tests):

| Cause | Effect on gold |
|---|---|
| `dedupe_rate_components` keyed on (label, unit, rate, season) only | Equal-priced windows collapsed: Hydro One / Toronto TOU 14→6 rows, ULO 8→4, NS 80 8→4, NS 05/06 11→9, SDG&E 29→7, PG&E 9→7, SRP 44→12 → `tou_gap` |
| Stacking riders re-added to an ENERGY row already all-in | NS Power all-in 0.19128 became 0.19932 when the model also emitted audit FAM/DSM rows |
| Relative-seasonal expansion dropped the adjustment's season dates | NF / NL 1.1S `seasonal_missing_calendar_dates` when riders carried the dates |
| No mapping for `a.m./p.m.`, `noon`, `:59` ends, `Monday to Friday`, "weekends and holidays", month names | clocks / day types / seasons nulled |
| Prompt example for NS code 80 said "mention weekend/holiday off-peak in description" | `tou_gap:weekend` by instruction |
| Gemini (tier 1) had no response schema | free-form keys; a top-level list was read as 0 tariffs |

Prompts now also ask for numbers and units verbatim (Phase 4 converts
cents deterministically), `day_type` on every TOU row, 24 h coverage per
season × day type from stated hours only ("all other hours" is emitted as
its complement), and exact month-only season bounds. Never invent clocks
or dates. Prompt cache version is `v3`, so every page re-extracts once.

### Guardrails

- Phase 4 stores `confidence_factors.extract_not_computable` (reasons) and
  `needs_review` on a `tou` / `seasonal*` extraction that fails the
  computable contract.
- `store_tariffs` **holds** (change event `hold`, reason
  `computable_regression`, proposal in the payload) instead of
  soft-superseding a computable live row with an extraction that is not
  computable for a structural reason. Protected / pinned rows are held
  earlier, as before. Nothing is deleted.

### Anthropic model compatibility (`app/services/anthropic_compat.py`)

All Anthropic calls (pipeline tiers, vision, nav, two-pass, Track B,
browser CLI, `opus_audit`, pin arbiter) go through it. Per the Claude docs
(checked 2026-09-25):

| Model | Thinking | Forced `tool_choice` | `temperature`/`top_p`/`top_k` |
|---|---|---|---|
| `claude-haiku-4-5-20251001` | off | OK | OK |
| `claude-sonnet-5` | on by default, may be disabled | OK | 400 |
| `claude-opus-5` | on by default, may be disabled (≤ high effort) | OK | 400 |
| `claude-opus-5-5` | on, **cannot** be disabled | **400** → sent as `auto` + "call the tool" instruction | 400 |

The module strips sampling knobs, converts forced tool use where required,
raises `max_tokens` to `ANTHROPIC_THINKING_MIN_MAX_TOKENS` (16000) while
thinking is on (thinking counts against `max_tokens`), reads the first
*text* block (the first block may be a thinking block), and retries once
on a 400 that names one of these parameters. `ANTHROPIC_THINKING=disabled`
turns thinking off where allowed (cheaper, Haiku-like);
`ANTHROPIC_EFFORT` sets `output_config.effort`.

### Pricing (`DEFAULT_PRICING`, USD / MTok, docs.claude.com 2026-09-25)

| Key | In | Out | Cache hit | 5-min cache write |
|---|---:|---:|---:|---:|
| `haiku` (Haiku 4.5) | 1.00 | 5.00 | 0.10 | 1.25 |
| `sonnet` (Sonnet 5; intro price is now standard) | 2.00 | 10.00 | 0.20 | 2.50 |
| `opus` (Opus 5) | 5.00 | 25.00 | 0.50 | 6.25 |
| `claude-opus-5-5` | 4.00 | 20.00 | 0.20 | 5.00 |

A call is priced by the longest matching model-id key, else its family,
and rolls up under its family (`opus` includes Opus 5.5). Claude 4.7+
models (Sonnet 5, Opus 5, Opus 5.5) use a tokenizer that yields about 30%
more tokens for the same text than Haiku 4.5, and thinking tokens bill as
output — so Sonnet 5 per page is more than 2× Haiku. Measure it with the
probe; do not assume $/utility.

### Proposing an env model bump

1. On the VM, baseline: `python -m scripts.benchmark --fixtures
   tests/fixtures/ground_truth_tou_seasonal.json --output /tmp/gold_live.json`
   and `python /app/scripts/llm_cost_report.py --runs 5`.
2. Probe current defaults and the candidate on the same documents:
   `python -m scripts.gold_model_probe --no-cache --output /tmp/gold_base.json`, then
   `HAIKU_MODEL=claude-sonnet-5 python -m scripts.gold_model_probe --no-cache --output /tmp/gold_sonnet.json`,
   and `OPUS_MODEL=claude-opus-5-5 ...` separately (one knob per run).
   Add `--add-url "San Diego Gas & Electric=https://www.sdge.com/total-electric-rates"`
   and a Newfoundland Power document so every gold tariff is scored.
3. `python -m scripts.gold_model_probe --compare /tmp/gold_base.json /tmp/gold_sonnet.json`.
4. Promote a default only if computable agreement rises (or holds, for a
   cheaper model) with no rise in rate errors, and projected spend stays in
   the ~$2k/year intent: scale the tier's share of recent `llm_cost_report`
   spend by the probe's cost ratio. Put the table in the PR.
5. Change the env (VM `.env`) first; change code defaults only in a PR
   with that table. Restart `celery-worker celery-beat api`.

### Wave 6 go / no-go on defaults

No LLM keys or DB were available to the Wave 6 implementer, so no live
before/after exists yet: **defaults are unchanged** (`HAIKU_MODEL`
Haiku 4.5, `OPUS_MODEL` / `AUDITOR_MODEL` / pin arbiter Opus 5,
`GEMINI_MODEL` Flash, `PHASE6_AGENT` as-is). Decision rules for the
VM re-run:

- **Opus → `claude-opus-5-5`:** go if the probe holds or improves gold.
  List price is 0.8× Opus 5 and Opus is capped at `OPUS_MAX_PER_UTILITY=2`.
  Risk to check in the probe: Opus 5.5 rejects forced tool use, so it
  answers with `tool_choice: auto`; watch for text-only replies (parsed by
  the JSON fallback) in `tier_acceptance`. `AUDITOR_MODEL` / the arbiter
  follow `OPUS_MODEL` unless set.
- **Haiku → `claude-sonnet-5`:** go only if gold computable agreement
  jumps clearly and projected spend fits the intent (≥2× per token,
  ~30% more tokens, plus thinking unless `ANTHROPIC_THINKING=disabled`).
  Otherwise keep Haiku and use Sonnet via env for targeted runs.

## Gold set growth

Now 12 tariffs (9 Canadian, 3 US TOU). Keep growing toward 30–40 with
more US TOU / seasonal shapes, labelled from source documents and
human-checked (audit §7.3).
