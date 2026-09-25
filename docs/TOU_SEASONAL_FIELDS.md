# Structured TOU clock windows & seasonal calendar dates

_Added 2026-09-25 (migration `c0d1e2f3a4b5`)._

## Why

Flux (and audits) need machine-readable periods so UIs can show
**“On-peak 7–11am”** instead of bare **“On-Peak”** prices. Free-text
`period_label` / `season` remain for display but are not sufficient for
completeness.

## Columns on `rate_components` (ENERGY rows)

| Field | Type | Meaning |
|-------|------|---------|
| `period_start_time` | `TIME` nullable | Clock window start (local wall time) |
| `period_end_time` | `TIME` nullable | Clock window end |
| `day_type` | `varchar(20)` | `weekday` \| `weekend` \| `holiday` \| `all` |
| `season_start_month` / `season_start_day` | `int` nullable | Inclusive season start (1–12 / 1–31) |
| `season_end_month` / `season_end_day` | `int` nullable | Inclusive season end; Nov→Mar wrap OK |
| `period_label` / `season` | varchar (existing) | Human display labels |

**Time conventions:** overnight wraps have `end < start` (e.g. 21:00→07:00).
`24:00` from extractors is stored as `00:00`. When `end == 00:00` and
`start != 00:00`, treat end as exclusive midnight (through end of day).
Both `00:00` with `day_type=all` means all hours.

## Product completeness rules

Implemented in `app/services/tou_seasonal_completeness.py`:

1. Every **TOU** rate (`tou`, `tou_tiered`, `demand_tou`, and the TOU half of
   `seasonal_tou`) must have clock windows **and** energy rates on every
   ENERGY component.
2. Every **seasonal** rate (`seasonal`, `seasonal_tiered`, and the seasonal
   half of `seasonal_tou`) must have season calendar dates **and** energy
   rates on every ENERGY component.
3. **`seasonal_tou`** must satisfy both.

Judged from structured columns only — **never invent** times or dates from
labels like “On-Peak” or “Winter”.

## How Flux should read the API

`GET /api/tariffs/{id}` (and any detail payload that embeds
`rate_components`) exposes the new fields on each component. Prefer:

```text
period_label + period_start_time–period_end_time (+ day_type)
season + season_start_month/day – season_end_month/day
```

Do not parse hours out of `period_label` when structured times are present.
If times are null on a TOU ENERGY row, the tariff is incomplete for display
of clock windows (show price + label only, or surface a data-quality hint).

## Extraction / refresh

LLM tool schema + prompts ask for structured fields. Phase 4 flags
incomplete TOU/seasonal shapes with `needs_review` and
`confidence_factors.tou_seasonal_incomplete` (does not invent values).
OEB scraper expands TOU/ULO using the official fixed schedules into
structured rows on the next scrape (no mass backfill of existing keepers).

## Backfill

One-shot soft-repair for **Hydro One** residential RPP (label-only TOU →
structured seasonal clocks):

```bash
python -m scripts.repair_hydro_one_oeb_residential          # dry-run
python -m scripts.repair_hydro_one_oeb_residential --plan tou --apply
```

See that script’s docstring for VM apply steps. Other incomplete Ontario
LDC keepers fill on the next `scrape_oeb_rates` run (structured persistence
is wired; no mass soft-supersede of every LDC in the Hydro One repair).
Do not invent times from labels.
