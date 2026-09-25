# NS Power residential repair — May 2026 book (rate 80 seasonal TOU fix)

Authoritative source:
https://www.nspower.ca/docs/default-source/regulatory/tariff-book-2026.pdf
Cover: **Tariffs May 2026**. Customer charge current column = “Effective upon
the date of the Board’s Order” (**2026-05-01**). Do **not** store the Jan 1
2027 escalate column as live winter TOU.

## Rate code 80 — before → after

Live keeper **67033** (created by PR #6) flattened Domestic TOU to a single
interim ENERGY row. The first seasonal repair (PR #7) restored the Energy
Charge schedule but emitted winter rows as **weekday only**, so winter
Saturdays, Sundays and holidays had no price and the computable contract
(completeness v2) rejected it with `tou_gap:weekend@11/01-03/31`.

| | 67033 (PR #6) | PR #7 keeper | After (this fix) |
|---|---|---|---|
| rate_type | `TOU` | `SEASONAL_TOU` | `SEASONAL_TOU` |
| FIXED | $20.08 | $20.08 | $20.08 |
| Non-winter (Apr 1–Oct 31), `day_type=all`, 00:00–00:00 | — | $0.13664 | $0.13664 |
| Winter weekday on-peak 07–11, 17–21 | — | $0.37321 | $0.37321 |
| Winter weekday off-peak 11–17, 21–07 | — | $0.19128 | $0.19128 |
| Winter `weekend`, 00:00–00:00 (Note 1) | — | missing | $0.19128 |
| Winter `holiday`, 00:00–00:00 (Note 1) | — | missing | $0.19128 |
| ENERGY (interim) | single all-hours $0.19128 | — | — |
| Computable | no | no (`tou_gap:weekend@11/01-03/31`) | **yes** |

All-in = base ¢ + FAM 0.156 + DSM 0.648 (= +0.804 ¢). Energy Charge bases:
non-winter 12.860 ¢ (eff Apr 1 2027); winter on-peak 36.517 / off-peak 18.324 ¢
(eff Nov 1 2026). Winter season dates are Nov 1–Mar 31 inclusive.

Note 1 of the book: in Winter, off-peak applies all hours on Saturdays,
Sundays and holidays (Jan 1, NS Heritage Day, Good Friday, Easter Monday,
Nov 11, Dec 25–26; observed weekday if on a weekend). The builder emits
these as `weekend` and `holiday` ENERGY rows; the gold fixture
`backend/tests/fixtures/ground_truth_tou_seasonal.json` (NS code 80) holds
the same rows and a unit test asserts the two are identical.

### Computable expectation

After `--plan tou --apply` the new keeper is `computable=true` with one
warning, `holiday_rows_require_calendar`. `utilities.holiday_calendar` is
deliberately left unset for 1739: setting a code (e.g. `CA-NS`) suppresses
that warning, but no holiday calendar table exists yet to resolve it, so
consumers still have to supply NS holidays themselves
(`docs/MYSA_CONSUMER_CONTRACT.md` §7). Set it in the PR that adds the
calendar data.

### KEEP / supersede rules

A live keeper is kept only if its ENERGY rows match the target on every
priced field: rate, unit, tiers, period label, season label, clock window,
`day_type` and season dates (`tariff_history.component_signature`; the
decorative `tier_label` is ignored). FIXED must match too, and so must the
effective date. So the PR #7 weekday-only keeper, or one with the right
labels but the wrong `day_type` or clocks, is not kept. Instead it is
soft-superseded (`supersede_reason='vintage'`) via
`tariff_history.supersede_tariff()`. That writes a `supersede` change event,
and the new keeper gets an `insert` event (`actor_id=repair_ns_power_residential_2026`).
Nothing is hard-deleted. The dry run prints the computable verdict for the
target and for every live candidate.

## Other residential plans (unchanged by `--plan tou`)

Domestic 02/03/04, TOD 05/06, CPP 70 interim, MURB 89 — KEEP when already
matching PR #6 shapes (a keeper built from the same builder still matches
under the stricter comparison). **Code 70 CPP** still uses interim-only
ENERGY in the repair; the book also has a fuller Critical Peak Energy Charge
section — out of scope unless reopened.

## How to run on the VM (after merge)

```bash
./deploy/sync-to-vm.sh
docker compose restart celery-worker celery-beat api

# Dry-run — rate 80 only (recommended). Expect the live keeper to print
# NOT computable reasons=['tou_gap:weekend@11/01-03/31'] and CREATE+SUPERSEDE.
./deploy/run-on-vm.sh \
  "python -m scripts.repair_ns_power_residential_2026 --plan tou" \
  --name ns-rate80

# Apply: soft-supersede the live keeper + create the Note 1 seasonal TOU keeper
./deploy/run-on-vm.sh \
  "python -m scripts.repair_ns_power_residential_2026 --plan tou --apply" \
  --name ns-rate80-apply
```

A second `--plan tou` run should report `KEEP` and `computable
warnings=['holiday_rows_require_calendar']`.

Full five-plan repair (idempotent KEEP for matching plans):

```bash
./deploy/run-on-vm.sh "python -m scripts.repair_ns_power_residential_2026" --name ns-repair
./deploy/run-on-vm.sh "python -m scripts.repair_ns_power_residential_2026 --apply" --name ns-repair-apply
```
