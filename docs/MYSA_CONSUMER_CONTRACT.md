# Consumer contract: pricing and TOU scheduling from UTF tariffs

For Mysa cloud (30-second cost rollups, TOU device schedules) and any other
machine consumer of the Utility Tariff Finder API. The rules are
implemented in `backend/app/services/computable.py`; this document is the
consumer-side reading of them.

## 1. The gate: `computable`

Every tariff returned by `GET /api/utilities/{id}/tariffs`,
`GET /api/tariffs/{id}` and `GET /api/tariffs/browse` carries:

| Field | Meaning |
|---|---|
| `computable` | `true` only when the structured rows are enough to price **every** interval of the year without reading the source document. |
| `computable_reasons` | Why not (empty when computable). Each entry is `code` or `code:detail`; match on the part before `:`. **Treat an unknown code as blocking.** |
| `computable_warnings` | Assumptions you must honour even when computable (§4). |
| `needs_review` | The pipeline flagged the values (above p95, unit auto-corrected, incomplete TOU/season shape). Prices may still be usable; show with caution. |
| `source_type` | `official` (utility's own site / documents, or the board that publishes the rate), `third_party` (aggregator / rate blog / foreign domain) or `unknown`. Informational; prefer `official` rows when a utility has alternatives. |

`GET /api/lookup` adds `computable_residential_tariff_count`, `timezone`
and `currency` per matched utility.

**Only automate cost or schedules for `computable: true` tariffs.** For the
rest, show the rates as informational ("rate not machine-readable yet").
Demand charges, TOU+tiered, critical-peak/event and dynamic/real-time
pricing are deliberately shipped as non-computable instead of guessed.

## 2. Pricing one interval

Given an interval `[t, t+Δt)` with energy `kWh`:

1. **Local time.** Clock windows are local wall-clock time at the *service
   address* (`clock_basis: "local_wall_clock"`). Use the device's own IANA
   zone. `timezone` on the tariff/utility is only a fallback: it is set for
   single-zone states/provinces or by an operator override, and is `null`
   in multi-zone jurisdictions (e.g. ON, TX, FL). DST follows the IANA zone.
2. **Season.** ENERGY rows with `season_start_month/day` and
   `season_end_month/day` apply on the inclusive date range (a range whose
   end is before its start wraps the new year). Rows without season fields
   apply all year. A computable tariff's seasons cover every date exactly
   once (Feb 29 belongs to the season covering Feb 28).
3. **Day type.** If the tariff has `day_type = "holiday"` rows **and** the
   date is a holiday, use `holiday`; otherwise `weekday` (Mon–Fri) or
   `weekend` (Sat–Sun). Rows with `day_type = "all"` apply to every day.
4. **Period.** Pick the ENERGY row whose `[period_start_time,
   period_end_time)` contains the local time. `end = 00:00` (with a non-zero
   start) means end of day; `00:00–00:00` means all day; `end < start`
   wraps midnight. A computable tariff guarantees exactly one match per
   season × day type × minute.
5. **Tiers** (tiered / seasonal_tiered). Track cumulative kWh in the billing
   period; the marginal price is the tier whose `[tier_min_kwh,
   tier_max_kwh]` contains it. The top tier has `tier_max_kwh = null`. A
   1 kWh step between bounds (0–500, 501–1000) is the inclusive-integer
   convention, not a gap.
6. **Energy price** = the selected ENERGY `rate_value` (`$/kWh`) **plus**
   every ADJUSTMENT row with unit `$/kWh` and `included_in_energy = false`
   (restricted to its season window when it has one). **Skip ADJUSTMENT rows
   with `included_in_energy = true`**: they are already folded into the
   all-in ENERGY rate and are kept only for audit.
7. **Periodic charges.** FIXED and ADJUSTMENT rows with a periodic unit
   accrue by time, not by kWh:

   | unit | accrual per interval |
   |---|---|
   | `$/day` | `value × Δt / 1 day` |
   | `$/month`, `$/bill` | `value × Δt / billing-period length` |
   | `$/year` | `value × Δt / 365 days` |

8. **Minimum.** MINIMUM rows are a monthly **bill floor**, not an additive
   charge: `bill = max(sum of charges, minimum)`.
9. **Currency** is `currency` (`USD` / `CAD`). Taxes are not modelled.

## 3. TOU scheduling

Use the same rows: per season and day type, the ENERGY windows are a full
partition of the day, so a schedule can be enumerated directly (e.g. shift
load out of the most expensive window). Season changeover dates come from
the season fields.

## 4. Warnings

| Warning | What to do |
|---|---|
| `holiday_rows_require_calendar` | The tariff prices holidays separately but the utility has no `holiday_calendar` yet. Supply the jurisdiction's statutory holidays yourself; treating them as normal days misprices ~10 days a year. |
| `tiers_accumulate_per_billing_period` | Tier thresholds are assumed to be per billing period (not per day or prorated). |
| `minimum_charge_is_bill_floor` | See §2.8. |
| `negative_energy_rate` | A credit rate; price as given. |
| `commodity_only_bill_incomplete` | Ontario OEB RPP commodity rates. Delivery, regulatory, OER rebate and HST are not included unless present as FIXED rows, so cost is understated. TOU windows are still correct for scheduling. |

## 5. Blocking reason codes

| Code | Meaning |
|---|---|
| `missing_energy_rates`, `energy_rate_not_numeric`, `energy_unit_not_per_kwh` | No usable `$/kWh` ENERGY price. |
| `ambiguous_energy_rows` | Several ENERGY prices with nothing (period, tier, season) saying which applies. |
| `tou_missing_clock_windows`, `tou_missing_day_type`, `tou_invalid_day_type`, `tou_zero_length_window` | TOU rows lack structured clocks or day types. |
| `tou_gap:<day>[@<season>]`, `tou_overlap:<day>[@<season>]` | The windows do not cover the day exactly once. |
| `seasonal_missing_calendar_dates`, `season_partial_calendar`, `season_invalid_date:<window>`, `season_gap`, `season_overlap` | Seasons are missing, invalid, or do not cover the year exactly once. |
| `tier_gap`, `tier_overlap`, `tier_not_from_zero`, `tier_no_open_top`, `tier_mixed` | Tiers are not a contiguous ladder from 0 to an open top. |
| `rider_inclusion_ambiguous` | Legacy row: ENERGY is labelled all-in but `$/kWh` riders are not flagged `included_in_energy`. Adding them would double count; skipping them might undercount. Clears on the next re-extraction. |
| `unrecognized_unit:<unit>` | A FIXED / MINIMUM / ADJUSTMENT unit outside §2.7. |
| `demand_charges_unsupported` | Needs a demand interval and billing-demand method, which are not modelled. |
| `tou_tiered_unsupported` | Tier accumulation across TOU periods is not modelled. |
| `event_pricing_unsupported`, `dynamic_pricing_unsupported` | Critical peak / event / peak-time rebate, or real-time / hourly / day-ahead pricing. |
| `complex_rate_type_unsupported`, `unknown_rate_type` | Shape not modelled. |

## 6. Lifecycle

- Rates are never edited in place. A change creates a new tariff id and
  soft-supersedes the old one; the old id returns **410** with
  `successor_tariff_id`. Re-resolve through the utility's tariff list or
  follow the successor.
- `superseded_at` and the append-only `tariff_change_events` log record when
  and why. An `as_of` read API is a follow-up.

## 7. Holiday calendar (design note)

`utilities.holiday_calendar` holds a calendar code (e.g. `CA-ON`,
`US-NERC`) that `day_type = "holiday"` rows refer to. The column exists but
is not populated yet. The planned shape is a `holiday_calendars` table
(`code`, `date`, `name`, `source_url`), populated per jurisdiction and
year, plus an endpoint returning a calendar's dates. Until then, consumers
supply holidays themselves when `holiday_rows_require_calendar` is present.
