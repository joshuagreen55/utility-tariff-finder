# NS Power residential repair — live DB before→after (May 2026 book)

Authoritative source:
https://www.nspower.ca/docs/default-source/regulatory/tariff-book-2026.pdf
Cover: **Tariffs May 2026**. Current column = “Effective upon the date of the Board’s Order”
(rates in effect **2026-05-01**). Do **not** store the Jan 1 2027 column as live.

Live prod facts (utility **1739**, read-only pull — see uploads/live-db-summary):
five residential keepers, all from **`tariff-book-20250326.pdf`**, last_verified
**2026-05-07**, all `approved=false` / `is_default=false`. The May 2026 PDF URL
has **0 hits** in tariffs / monitoring / fingerprints.

## Root cause

1. **Stale source PDF**: Flux ENERGY/FIXED still come from the Mar 2025 book
   (`…/tariff-book-20250326.pdf`). Monitoring only polls HTML marketing pages
   (`residential-rates`, `rates-tariffs`, `rate-options`) — never the 2026 PDF.
2. **Sept 1 refresh crash**: utility 1739 `refresh_last_reason` =
   `crash: … StringDataRightTruncation` (long season/period_label vs VARCHAR).
   Rates were **not** rewritten after May. Pipeline now clips those fields on
   write so a future refresh cannot die the same way.
3. **Missing Domestic + Green Power stand-in**: there is no clean Domestic
   Service row. Id **60200** is “Domestic … Optional Green Power Rider” with
   stale ENERGY $0.15744 + $5 ADJUSTMENT — not the May 2026 Domestic base.
4. Even when extraction runs, Board’s Order vs Jan 2027 columns + omitted
   FAM/DSM understate customer-facing ENERGY (Flux shows ENERGY only).

## Before (live) → after (repair targets)

| id | Before (Mar 2025 book) | After (May 2026 all-in) |
|---:|------------------------|-------------------------|
| **60200** | Green Power stand-in: FIXED $19.17; ENERGY $0.15744 + $5 ADJ | **SUPERSEDE** → new Domestic Service |
| *(new)* | *(missing standard Domestic)* | **CREATE** Domestic 02/03/04: FIXED **$20.08**; ENERGY **$0.19128**/kWh |
| **46886** | CPP: FIXED $19.17; Critical $1.42256 / Non-Crit $0.14222 | **SUPERSEDE** → CPP interim ENERGY **$0.19128** all hours |
| **46887** | TOU: FIXED $19.17; seasonal pre-interim ENERGY | **SUPERSEDE** → TOU interim ENERGY **$0.19128** all hours |
| **60201** | TOD: FIXED $19.17; Summer/Winter ENERGY | **SUPERSEDE** → TOD Board’s Order + FAM/DSM all-in |
| **60202** | MURB: FIXED $21.28; seasonal ENERGY | **SUPERSEDE** → MURB Board’s Order + General riders; min **$22.00** |

Domestic all-in = 18.324 + 0.156 (FAM) + 0.648 (DSM) + 0.000 (Storm) = **19.128 ¢/kWh**.

## How to run on the VM (after merge)

```bash
./deploy/sync-to-vm.sh
docker compose restart celery-worker celery-beat api

# Dry-run (defaults to utility 1739)
./deploy/run-on-vm.sh "python -m scripts.repair_ns_power_residential_2026" --name ns-repair

# Apply soft-supersede + create keepers
./deploy/run-on-vm.sh "python -m scripts.repair_ns_power_residential_2026 --apply" --name ns-repair-apply
```
