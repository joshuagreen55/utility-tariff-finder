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

## Follow-up A/B note (not done here)

Once a baseline with these metrics exists:

- **Tier 3:** try the newest Opus id via `OPUS_MODEL` with the same cap
  (`OPUS_MAX_PER_UTILITY=2`); set `LLM_PRICING_JSON.opus` to its list price.
- **Tier 2:** a global `HAIKU_MODEL` swap changes seven call sites (tier-2
  extraction, vision, nav, identify, Track B, browser CLI). A/B a stronger
  mid-tier on two-pass and vision only via a new per-site knob (code change).
- **Tier 1:** keep Gemini Flash; add `response_schema` (code) and measure
  JSON-shape failures.
- **Gold set:** now 12 tariffs (9 Canadian, 3 US TOU). Keep
  growing toward 30–40 with more US TOU / seasonal shapes, labelled from
  source documents and human-checked (audit §7.3).
