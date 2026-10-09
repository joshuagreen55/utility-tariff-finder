#!/usr/bin/env python3
"""One-shot: inject published clocks/seasons into pricing golden plans.

Sources: OEB scrape_oeb_rates schedules, NS Power repair script / ground_truth,
NL Hydro / seasonal gold seasons. Does not invent clocks for plans without an
in-repo published schedule.
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "pricing_golden"
PLANS = ROOT / "plans.json"

# OEB RPP TOU — scrape_oeb_rates.WINTER/SUMMER_TOU_SCHEDULE (24:00 → 00:00)
OEB_TOU_CLOCKS = [
    # Winter weekdays
    {"period": "off_peak", "season": "winter", "day_type": "weekday", "start": "00:00", "end": "07:00"},
    {"period": "on_peak", "season": "winter", "day_type": "weekday", "start": "07:00", "end": "11:00"},
    {"period": "mid_peak", "season": "winter", "day_type": "weekday", "start": "11:00", "end": "17:00"},
    {"period": "on_peak", "season": "winter", "day_type": "weekday", "start": "17:00", "end": "19:00"},
    {"period": "off_peak", "season": "winter", "day_type": "weekday", "start": "19:00", "end": "00:00"},
    {"period": "off_peak", "season": "winter", "day_type": "weekend", "start": "00:00", "end": "00:00"},
    {"period": "off_peak", "season": "winter", "day_type": "holiday", "start": "00:00", "end": "00:00"},
    # Summer weekdays
    {"period": "off_peak", "season": "summer", "day_type": "weekday", "start": "00:00", "end": "07:00"},
    {"period": "mid_peak", "season": "summer", "day_type": "weekday", "start": "07:00", "end": "11:00"},
    {"period": "on_peak", "season": "summer", "day_type": "weekday", "start": "11:00", "end": "17:00"},
    {"period": "mid_peak", "season": "summer", "day_type": "weekday", "start": "17:00", "end": "19:00"},
    {"period": "off_peak", "season": "summer", "day_type": "weekday", "start": "19:00", "end": "00:00"},
    {"period": "off_peak", "season": "summer", "day_type": "weekend", "start": "00:00", "end": "00:00"},
    {"period": "off_peak", "season": "summer", "day_type": "holiday", "start": "00:00", "end": "00:00"},
]
OEB_TOU_SEASONS = [
    {"season": "winter", "start_month": 11, "start_day": 1, "end_month": 4, "end_day": 30},
    {"season": "summer", "start_month": 5, "start_day": 1, "end_month": 10, "end_day": 31},
]

# OEB ULO — year-round (ground_truth_tou_seasonal / scrape_oeb_rates.ULO_SCHEDULE)
OEB_ULO_CLOCKS = [
    {"period": "ulo", "season": "all", "day_type": "weekday", "start": "23:00", "end": "07:00"},
    {"period": "mid_peak", "season": "all", "day_type": "weekday", "start": "07:00", "end": "16:00"},
    {"period": "on_peak", "season": "all", "day_type": "weekday", "start": "16:00", "end": "21:00"},
    {"period": "mid_peak", "season": "all", "day_type": "weekday", "start": "21:00", "end": "23:00"},
    {"period": "ulo", "season": "all", "day_type": "weekend", "start": "23:00", "end": "07:00"},
    {"period": "weekend_off", "season": "all", "day_type": "weekend", "start": "07:00", "end": "23:00"},
    {"period": "ulo", "season": "all", "day_type": "holiday", "start": "23:00", "end": "07:00"},
    {"period": "weekend_off", "season": "all", "day_type": "holiday", "start": "07:00", "end": "23:00"},
]

# NS Power Domestic TOU code 80 (ground_truth + repair_ns_power)
NSP_TOU_CLOCKS = [
    {"period": "all", "season": "non_winter", "day_type": "all", "start": "00:00", "end": "00:00"},
    {"period": "on_peak", "season": "winter", "day_type": "weekday", "start": "07:00", "end": "11:00"},
    {"period": "off_peak", "season": "winter", "day_type": "weekday", "start": "11:00", "end": "17:00"},
    {"period": "on_peak", "season": "winter", "day_type": "weekday", "start": "17:00", "end": "21:00"},
    {"period": "off_peak", "season": "winter", "day_type": "weekday", "start": "21:00", "end": "07:00"},
    {"period": "all", "season": "winter", "day_type": "weekend", "start": "00:00", "end": "00:00"},
    {"period": "all", "season": "winter", "day_type": "holiday", "start": "00:00", "end": "00:00"},
]
NSP_TOU_SEASONS = [
    {"season": "non_winter", "start_month": 4, "start_day": 1, "end_month": 10, "end_day": 31},
    {"season": "winter", "start_month": 11, "start_day": 1, "end_month": 3, "end_day": 31},
]

# NS Power Domestic TOD 05/06 — collapsed golden periods map to winter clocks
# (amounts match winter off/mid/on); seasons from ground_truth.
NSP_TOD_CLOCKS = [
    {"period": "on_peak", "season": "winter", "day_type": "weekday", "start": "07:00", "end": "12:00"},
    {"period": "mid_peak", "season": "winter", "day_type": "weekday", "start": "12:00", "end": "16:00"},
    {"period": "on_peak", "season": "winter", "day_type": "weekday", "start": "16:00", "end": "23:00"},
    {"period": "off_peak", "season": "winter", "day_type": "weekday", "start": "23:00", "end": "07:00"},
    {"period": "off_peak", "season": "winter", "day_type": "weekend", "start": "00:00", "end": "00:00"},
    {"period": "off_peak", "season": "winter", "day_type": "holiday", "start": "00:00", "end": "00:00"},
    {"period": "mid_peak", "season": "non_winter", "day_type": "weekday", "start": "07:00", "end": "23:00"},
    {"period": "off_peak", "season": "non_winter", "day_type": "weekday", "start": "23:00", "end": "07:00"},
    {"period": "off_peak", "season": "non_winter", "day_type": "weekend", "start": "00:00", "end": "00:00"},
    {"period": "off_peak", "season": "non_winter", "day_type": "holiday", "start": "00:00", "end": "00:00"},
]
NSP_TOD_SEASONS = [
    {"season": "winter", "start_month": 12, "start_day": 1, "end_month": 2, "end_day": 28},
    {"season": "non_winter", "start_month": 3, "start_day": 1, "end_month": 11, "end_day": 30},
]

# NS Power MURB TOU — golden uses p0..p4 for the five Board's Order amounts;
# clocks from repair_ns_power build_murb_tou_components labels.
NSP_MURB_CLOCKS = [
    {"period": "p0", "season": "non_winter", "day_type": "all", "start": "21:00", "end": "07:00"},  # off 11.826
    {"period": "p2", "season": "non_winter", "day_type": "all", "start": "07:00", "end": "21:00"},  # on 14.043
    {"period": "p4", "season": "winter", "day_type": "weekday", "start": "07:00", "end": "11:00"},  # on am 29.565
    {"period": "p3", "season": "winter", "day_type": "weekday", "start": "11:00", "end": "17:00"},  # mid 14.782
    {"period": "p4", "season": "winter", "day_type": "weekday", "start": "17:00", "end": "21:00"},  # on pm 29.565
    {"period": "p1", "season": "winter", "day_type": "weekday", "start": "21:00", "end": "07:00"},  # off 12.565
]
NSP_MURB_SEASONS = [
    {"season": "non_winter", "start_month": 4, "start_day": 1, "end_month": 10, "end_day": 31},
    {"season": "winter", "start_month": 11, "start_day": 1, "end_month": 3, "end_day": 31},
]

# Seasonal-only calendars from ground_truth_tou_seasonal.json (published only).
NLH_SEASONS = [
    {"season": "winter", "start_month": 12, "start_day": 1, "end_month": 4, "end_day": 30},
    {"season": "non_winter", "start_month": 5, "start_day": 1, "end_month": 11, "end_day": 30},
]

# Plan-key → (clocks|None, seasons|None). None means leave empty.
# Only encode schedules with an in-repo published source (OEB, NS Power,
# NL Hydro gold). Do not invent clocks/dates from period labels alone.
SCHEDULES: dict[str, tuple[list | None, list | None]] = {
    "toronto-tou": (OEB_TOU_CLOCKS, OEB_TOU_SEASONS),
    "hydroone-tou": (OEB_TOU_CLOCKS, OEB_TOU_SEASONS),
    "kwh-tou": (OEB_TOU_CLOCKS, OEB_TOU_SEASONS),
    "toronto-ulo": (OEB_ULO_CLOCKS, None),
    "hydroone-ulo": (OEB_ULO_CLOCKS, None),
    "nsp-tou": (NSP_TOU_CLOCKS, NSP_TOU_SEASONS),
    "nsp-tod": (NSP_TOD_CLOCKS, NSP_TOD_SEASONS),
    "nsp-murb-tou": (NSP_MURB_CLOCKS, NSP_MURB_SEASONS),
    "nlh-1.1s": (None, NLH_SEASONS),
}


def main() -> None:
    data = json.loads(PLANS.read_text())
    updated = 0
    for plan in data["plans"]:
        key = plan["plan_key"]
        clocks, seasons = SCHEDULES.get(key, (None, None))
        if clocks is not None:
            plan["clocks"] = clocks
            updated += 1
        elif "clocks" not in plan:
            plan["clocks"] = []
        if seasons is not None:
            plan["seasons"] = seasons
            if clocks is None:
                updated += 1
        elif "seasons" not in plan:
            plan["seasons"] = []
    # Ensure every plan has the keys (empty lists for non-TOU).
    for plan in data["plans"]:
        plan.setdefault("clocks", [])
        plan.setdefault("seasons", [])
    data["description"] = (
        "Hand-verified residential plan components for the pricing compiler. "
        "Amounts are decimal strings. official_cents is the cited official "
        "all-in (¢/kWh). The compiler must reproduce the exact Decimal sum of "
        "components; that sum matches official_cents when quantized to the "
        "same places as the citation. Each plan carries source_url / "
        "source_urls / document_set from the curated official document set "
        "(PR R27-1). TOU clocks and season calendars (PR R27-5) come from "
        "published schedules only."
    )
    PLANS.write_text(json.dumps(data, indent=2) + "\n")
    with_clocks = sum(1 for p in data["plans"] if p.get("clocks"))
    with_seasons = sum(1 for p in data["plans"] if p.get("seasons"))
    print(f"wrote {PLANS}: {with_clocks} plans with clocks, {with_seasons} with seasons")


if __name__ == "__main__":
    main()
