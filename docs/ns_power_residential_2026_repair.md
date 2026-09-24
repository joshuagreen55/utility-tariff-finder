# NS Power residential repair — dry-run before→after (May 2026 book)

Authoritative source:
https://www.nspower.ca/docs/default-source/regulatory/tariff-book-2026.pdf
Cover: **Tariffs May 2026**. Current column = “Effective upon the date of the Board’s Order”
(rates in effect **2026-05-01**). Do **not** store the Jan 1 2027 column as live.

This artifact shows the **expected** dry-run shape from
`python -m scripts.repair_ns_power_residential_2026` (no production DB write).
Re-run on the VM after merge to capture live id→id supersedes.

## Root cause (why we didn’t have the latest from this doc)

1. **Source discovery** seeded / monitored the marketing hub
   `…/about-us/electricity/rates-tariffs` (and residential marketing pages)
   instead of anchoring on the regulatory `tariff-book-YYYY.pdf`. Refreshes
   often never re-fetched the May 2026 book as the primary extraction source.
2. **Table layout** splits “Board’s Order” vs “Effective January 1, 2027” across
   lines; extraction can grab the **2027** vintage or omit **FAM + DSM** riders.
3. **Product display**: Flux / Lookup only show **ENERGY** (not ADJUSTMENT).
   Base-only ENERGY (18.324 ¢) understates the customer-facing charge
   (19.128 ¢ all-in). NS Power’s own bill copy says the energy charge includes
   fuel (FAM) and efficiency programs (DSM).

## Riders (Domestic class, 2026)

| Rider | ¢/kWh |
|-------|------:|
| FAM AA/BA combined | 0.156 |
| DSM DCRR (PCR 0.642 + BA 0.006) | 0.648 |
| Storm SCRR | 0.000 |
| **Sum** | **0.804** |

## Before → after (five residential keepers)

### 1. Domestic Service (codes 02/03/04)

| | Before (typical wrong) | After (May 2026 all-in) |
|--|------------------------|-------------------------|
| Effective | stale / 2025 / wrong column | **2026-05-01** |
| Fixed | often ≠ $20.08 | **$20.08/mo** |
| ENERGY | 18.324 ¢ base-only **or** 19.067 ¢ (2027) | **19.128 ¢ = $0.19128/kWh** |

### 2. Domestic Service Time-Of-Day (05/06)

| Season / period | Board’s Order base ¢ | All-in ENERGY $/kWh |
|-----------------|---------------------:|--------------------:|
| Winter Dec–Feb weekday 7am–12pm | 24.384 | 0.25188 |
| Winter 12pm–4pm | 19.459 | 0.20263 |
| Winter 4pm–11pm | 24.384 | 0.25188 |
| Winter 11pm–7am (also weekends/holidays) | 11.632 | 0.12436 |
| Mar–Nov weekday 7am–11pm | 19.459 | 0.20263 |
| Mar–Nov 11pm–7am | 11.632 | 0.12436 |
| Customer charge | | **$20.08/mo** |

### 3. Domestic CPP (70) — interim

While TVP systems are down: ENERGY = Domestic all-in **$0.19128/kWh** all hours;
CPP events **n/a**. Fixed **$20.08**. (Do not put Nov 2026 full CPP schedule live as current.)

### 4. Domestic TOU (80) — interim

Interim tracks standard Domestic offer: ENERGY **$0.19128/kWh** all hours.
Fixed **$20.08**.

### 5. MURB TOU (89)

Fifth residential-named schedule in the book. Uses **General-class** riders
(FAM 0.207 + DSM 0.749 ¢). Board’s Order energy periods → all-in ENERGY;
minimum monthly **$22.00**. Soft-supersedes any other live residential that
doesn’t match these five (e.g. Green Power extracted as a fake schedule).

## How to run on the VM (after merge)

```bash
./deploy/sync-to-vm.sh
docker compose restart celery-worker celery-beat api

# Dry-run (default)
./deploy/run-on-vm.sh "python -m scripts.repair_ns_power_residential_2026" --name ns-repair

# Apply soft-supersede + create keepers
./deploy/run-on-vm.sh "python -m scripts.repair_ns_power_residential_2026 --apply" --name ns-repair-apply
```

Optional: set `utilities.rate_page_url_override` to the May 2026 PDF, or rely on
the new preferred-URL injection in `tariff_pipeline` on the next refresh.
