# Pricing replay fixtures (R27 golden raw)

Real model outputs from live R27 (`raw.haiku` / `raw.sonnet`) plus
re-fetched document text under `docs/url/`. No LLM calls in replay.

```bash
cd backend && python -m scripts.pricing_replay
python -m scripts.pricing_replay --gate-rate 0.6 --require-zero-wrong
```

## R27 baseline (post R29-1…4)

| Metric | Value |
|--------|-------|
| accepted_correct | 4 |
| accepted_wrong | 0 |
| held | 22 |
| skipped | 13 |
| scored | 26 |
| accepted-correct rate | **15.4%** |

Holds: `quote_verify_failed` 14 · `preaccept_failed` 5 · `schema_invalid` 3.
Gate (≥60% + 0 wrong) is **not** claimed on this re-fetch bundle — validate
on R30 when same-run raw + docs land.

## Limits (R27 bundle)

Document text is a **2026-10-09 re-fetch**, not the original live bytes.
Some R27 plans have empty `url_hashes` and missing URLs in `docs/url/`
(NLH Jan schedule, HQ, Alberta ROLR, NWE, Dominion) → skipped.
Quote grounding against reflows is the main hold driver; R30 (full raw +
same-run docs) is required to fairly hit the ≥60% accepted-correct gate.

## R29 0-plan diagnosis (from live results, not this fixture)

| Utility | Cause |
|---------|--------|
| Consumers Energy | Discovery returned marketing/FAQ hub; skeleton emitted 0 plans |
| Waterloo North Hydro | No usable official rate docs after filter; 0 plans |
| IID | Tariff book missing from discovery set; 0 plans |
| Oncor (risk) | `quickelectricity.com` aggregator — now rejected as `non_utility_domain` |
