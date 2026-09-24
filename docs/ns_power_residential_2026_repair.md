# NS Power residential repair — May 2026 book (rate 80 seasonal TOU fix)

Authoritative source:
https://www.nspower.ca/docs/default-source/regulatory/tariff-book-2026.pdf
Cover: **Tariffs May 2026**. Customer charge current column = “Effective upon
the date of the Board’s Order” (**2026-05-01**). Do **not** store the Jan 1
2027 escalate column as live winter TOU.

## Rate code 80 — before → after (this fix)

Live keeper **67033** (created by PR #6) flattened Domestic TOU to:

| | Before (67033) | After (Energy Charge seasonal TOU) |
|---|---|---|
| rate_type | `TOU` | `SEASONAL_TOU` |
| FIXED | $20.08 | $20.08 |
| ENERGY | single Interim all-hours **$0.19128** | Non-winter all hours **$0.13664**; Winter on-peak **$0.37321**; Winter off-peak **$0.19128** |

All-in = base ¢ + FAM 0.156 + DSM 0.648 (= +0.804 ¢). Energy Charge bases:
non-winter 12.860 ¢ (eff Apr 1 2027); winter on-peak 36.517 / off-peak 18.324 ¢
(eff Nov 1 2026). Soft-supersede **67033** only — never hard-delete.
Predecessor **46887** (already superseded by 67033) had the correct seasonal
shape at old Mar-2025 prices.

Weekend/holiday off-peak (Note 1) is documented on the tariff description /
`confidence_factors`; Flux does not require a full schedule matrix for this
repair.

## Other residential plans (unchanged by `--plan tou`)

Domestic 02/03/04, TOD 05/06, CPP 70 interim, MURB 89 — KEEP when already
matching PR #6 shapes. **Code 70 CPP** still uses interim-only ENERGY in the
repair; the book also has a fuller Critical Peak Energy Charge section — out
of scope unless reopened.

## How to run on the VM (after merge)

```bash
./deploy/sync-to-vm.sh
docker compose restart celery-worker celery-beat api

# Dry-run — rate 80 only (recommended)
./deploy/run-on-vm.sh \
  "python -m scripts.repair_ns_power_residential_2026 --plan tou" \
  --name ns-rate80

# Apply soft-supersede 67033 + create seasonal TOU keeper
./deploy/run-on-vm.sh \
  "python -m scripts.repair_ns_power_residential_2026 --plan tou --apply" \
  --name ns-rate80-apply
```

Full five-plan repair (idempotent KEEP for matching plans):

```bash
./deploy/run-on-vm.sh "python -m scripts.repair_ns_power_residential_2026" --name ns-repair
./deploy/run-on-vm.sh "python -m scripts.repair_ns_power_residential_2026 --apply" --name ns-repair-apply
```
