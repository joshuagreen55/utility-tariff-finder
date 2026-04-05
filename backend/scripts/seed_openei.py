"""
Seed tariff data from the OpenEI Utility Rate Database (URDB).

Downloads the full URDB bulk JSON files and maps them into our normalized schema.

Usage:
    python -m scripts.seed_openei [--country US|CA]
"""

import argparse
import gzip
import json
import sys
import time
from datetime import datetime, date, timezone

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.db.session import get_sync_engine
from app.db.base import Base
from app.models import (
    Utility, Tariff, RateComponent,
    CustomerClass, RateType, ComponentType, Country,
)


USURDB_BULK_URL = "https://openei.org/apps/USURDB/download/usurdb.json.gz"
IURDB_BULK_URL = "https://openei.org/apps/IURDB/download/iurdb.json.gz"


def normalize_bulk_record(item: dict) -> dict:
    """Normalize bulk JSON camelCase field names to the API-style lowercase names."""
    eid = item.get("eiaId") or item.get("eiaid")
    oid = item.get("_id")
    label = None
    if isinstance(oid, dict):
        label = oid.get("$oid")
    elif isinstance(oid, str):
        label = oid

    eff_date = item.get("effectiveDate") or item.get("startdate")
    start_ts = None
    if isinstance(eff_date, dict) and "$date" in eff_date:
        try:
            start_ts = datetime.fromisoformat(eff_date["$date"].replace("Z", "+00:00")).timestamp()
        except Exception:
            pass
    elif isinstance(eff_date, (int, float)):
        start_ts = eff_date

    end_date = item.get("endDate") or item.get("enddate")
    end_ts = None
    if isinstance(end_date, dict) and "$date" in end_date:
        try:
            end_ts = datetime.fromisoformat(end_date["$date"].replace("Z", "+00:00")).timestamp()
        except Exception:
            pass
    elif isinstance(end_date, (int, float)):
        end_ts = end_date

    energy_strux = item.get("energyRateStrux") or item.get("energyratestructure", [])
    normalized_energy = []
    for period in energy_strux:
        if isinstance(period, dict) and "energyRateTiers" in period:
            normalized_energy.append(period["energyRateTiers"])
        elif isinstance(period, list):
            normalized_energy.append(period)

    demand_strux = item.get("demandRateStrux") or item.get("demandstructure", [])
    normalized_demand = []
    for period in (demand_strux or []):
        if isinstance(period, dict) and "demandRateTiers" in period:
            normalized_demand.append(period["demandRateTiers"])
        elif isinstance(period, list):
            normalized_demand.append(period)

    flat_demand = item.get("flatDemandStrux") or item.get("flatdemandstructure", [])
    normalized_flat_demand = []
    for period in (flat_demand or []):
        if isinstance(period, dict) and "flatDemandTiers" in period:
            normalized_flat_demand.append(period["flatDemandTiers"])
        elif isinstance(period, list):
            normalized_flat_demand.append(period)

    return {
        "label": label or item.get("label"),
        "eiaid": int(eid) if eid else None,
        "utility": item.get("utilityName") or item.get("utility", ""),
        "name": item.get("rateName") or item.get("name", "Unknown Rate"),
        "sector": item.get("sector", ""),
        "startdate": start_ts,
        "enddate": end_ts,
        "source": item.get("sourceReference") or item.get("source"),
        "description": item.get("description", ""),
        "approved": item.get("approved", False),
        "is_default": item.get("isDefault") or item.get("is_default", False),
        "energyratestructure": normalized_energy,
        "energyweekdayschedule": item.get("energyWeekdaySched") or item.get("energyweekdayschedule"),
        "energyweekendschedule": item.get("energyWeekendSched") or item.get("energyweekendschedule"),
        "demandstructure": normalized_demand,
        "flatdemandstructure": normalized_flat_demand,
        "demandweekdayschedule": item.get("demandWeekdaySched") or item.get("demandweekdayschedule"),
        "demandweekendschedule": item.get("demandWeekendSched") or item.get("demandweekendschedule"),
        "fixedchargefirstmeter": item.get("fixedChargeFirstMeter") or item.get("fixedchargefirstmeter"),
        "fixedchargeunits": item.get("fixedChargeUnits") or item.get("fixedchargeunits", "$/month"),
        "mincharge": item.get("minCharge") or item.get("mincharge"),
        "minchargeunits": item.get("minChargeUnits") or item.get("minchargeunits", "$/month"),
        "demandunits": item.get("demandUnits") or item.get("demandunits", "kW"),
        "country": item.get("country", ""),
        "_raw": item,
    }

SECTOR_MAP = {
    "Residential": CustomerClass.RESIDENTIAL,
    "Commercial": CustomerClass.COMMERCIAL,
    "Industrial": CustomerClass.INDUSTRIAL,
    "Lighting": CustomerClass.LIGHTING,
}


def classify_rate_type(data: dict) -> RateType:
    """Classify an OpenEI rate record into our RateType enum based on
    the structure of its energy/demand schedule and rate tiers."""
    energy_structure = data.get("energyratestructure", [])
    weekday_schedule = data.get("energyweekdayschedule", [])
    has_demand = bool(data.get("demandstructure") or data.get("flatdemandstructure"))

    num_periods = len(energy_structure)
    has_tiers = any(len(period) > 1 for period in energy_structure) if energy_structure else False

    has_tou = False
    if weekday_schedule:
        unique_values = set()
        for month_row in weekday_schedule:
            for val in month_row:
                unique_values.add(val)
        has_tou = len(unique_values) > 1

    is_seasonal = False
    if weekday_schedule and len(weekday_schedule) >= 12:
        month_patterns = [tuple(row) for row in weekday_schedule]
        is_seasonal = len(set(month_patterns)) > 1

    if has_demand and has_tou:
        return RateType.DEMAND_TOU
    if has_tou and has_tiers:
        return RateType.TOU_TIERED
    if is_seasonal and has_tou:
        return RateType.SEASONAL_TOU
    if is_seasonal and has_tiers:
        return RateType.SEASONAL_TIERED
    if is_seasonal:
        return RateType.SEASONAL
    if has_demand:
        return RateType.DEMAND
    if has_tou:
        return RateType.TOU
    if has_tiers:
        return RateType.TIERED
    return RateType.FLAT


def extract_rate_components(data: dict) -> list[dict]:
    """Extract normalized rate components from OpenEI rate record."""
    components = []

    fixed_charge = data.get("fixedchargefirstmeter")
    if fixed_charge is not None:
        components.append({
            "component_type": ComponentType.FIXED,
            "unit": data.get("fixedchargeunits", "$/month"),
            "rate_value": float(fixed_charge),
        })

    min_charge = data.get("mincharge")
    if min_charge is not None:
        components.append({
            "component_type": ComponentType.MINIMUM,
            "unit": data.get("minchargeunits", "$/month"),
            "rate_value": float(min_charge),
        })

    energy_structure = data.get("energyratestructure", [])
    for period_idx, period in enumerate(energy_structure):
        for tier_idx, tier in enumerate(period):
            rate = tier.get("rate", 0)
            adj = tier.get("adj", 0)
            comp = {
                "component_type": ComponentType.ENERGY,
                "unit": tier.get("unit", "kWh"),
                "rate_value": float(rate),
                "period_index": period_idx,
                "adjustment": float(adj) if adj else None,
            }
            max_kwh = tier.get("max")
            if max_kwh is not None:
                comp["tier_max_kwh"] = float(max_kwh)
                if tier_idx > 0:
                    prev_max = period[tier_idx - 1].get("max")
                    if prev_max is not None:
                        comp["tier_min_kwh"] = float(prev_max)
            elif tier_idx > 0 and period[tier_idx - 1].get("max"):
                comp["tier_min_kwh"] = float(period[tier_idx - 1]["max"])
            components.append(comp)

    for struct_key in ("demandstructure", "flatdemandstructure"):
        demand_structure = data.get(struct_key, [])
        for period_idx, period in enumerate(demand_structure):
            for tier_idx, tier in enumerate(period):
                rate = tier.get("rate", 0)
                comp = {
                    "component_type": ComponentType.DEMAND,
                    "unit": data.get("demandunits", "kW"),
                    "rate_value": float(rate),
                    "period_index": period_idx,
                }
                max_val = tier.get("max")
                if max_val is not None:
                    comp["tier_max_kwh"] = float(max_val)
                components.append(comp)

    return components


def download_bulk_rates(url: str) -> list[dict]:
    """Download and decompress a bulk URDB JSON file."""
    print(f"  Downloading {url}...")
    with httpx.Client(timeout=300, follow_redirects=True) as client:
        resp = client.get(url)
        resp.raise_for_status()

    print(f"  Downloaded {len(resp.content) / 1024 / 1024:.1f} MB, decompressing...")
    raw = gzip.decompress(resp.content)
    items = json.loads(raw)
    if isinstance(items, dict):
        items = items.get("items", [])
    print(f"  Loaded {len(items)} rate records")
    return items


def _make_json_safe(obj):
    """Convert BSON-style values (e.g., {$oid: ...}, {$date: ...}) to plain strings."""
    if isinstance(obj, dict):
        if "$oid" in obj:
            return obj["$oid"]
        if "$date" in obj:
            return obj["$date"]
        return {k: _make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_make_json_safe(v) for v in obj]
    return obj


import re as _re
import unicodedata as _unicodedata

_STRIP_SUFFIXES = _re.compile(
    r'\s*\b(inc\.?|ltd\.?|llc|corp\.?|co\.?|company|limited|corporation|'
    r'l\.?p\.?|plc|the|of|and)\b\.?\s*', _re.IGNORECASE
)


def _normalize_name(name: str) -> str:
    """Normalize a utility name for fuzzy matching.
    Transliterates accented chars (é→e), converts hyphens to spaces,
    strips corporate suffixes, and lowercases."""
    n = _unicodedata.normalize("NFKD", name)
    n = "".join(c for c in n if not _unicodedata.combining(c))
    n = n.replace("-", " ")
    n = _STRIP_SUFFIXES.sub(" ", n)
    n = _re.sub(r'[^a-z0-9\s]', '', n.lower())
    return " ".join(n.split())


def _build_fuzzy_name_map(utilities: list) -> dict[str, object]:
    """Build a map from normalized name -> utility, plus exact name map."""
    exact = {}
    normalized = {}
    for u in utilities:
        exact[u.name] = u
        norm = _normalize_name(u.name)
        normalized[norm] = u
    return exact, normalized


def _fuzzy_match(name: str, exact_map: dict, norm_map: dict):
    """Try exact match, then normalized match, then substring containment."""
    if name in exact_map:
        return exact_map[name]
    norm = _normalize_name(name)
    if norm in norm_map:
        return norm_map[norm]
    for db_norm, u in norm_map.items():
        if norm in db_norm or db_norm in norm:
            return u
    return None


def seed_rates(session: Session, raw_items: list[dict], country_code: str):
    """Insert OpenEI rate records into the database."""
    print("  Building utility lookup maps...")
    all_utilities = session.execute(select(Utility)).scalars().all()
    eia_id_map = {u.eia_id: u for u in all_utilities if u.eia_id is not None}
    exact_name_map, norm_name_map = _build_fuzzy_name_map(all_utilities)

    existing_openei_ids = set(
        session.execute(select(Tariff.openei_id).where(Tariff.openei_id.isnot(None))).scalars().all()
    )
    print(f"  {len(eia_id_map)} utilities by EIA ID, {len(exact_name_map)} by name, {len(existing_openei_ids)} existing tariffs")

    created = 0
    skipped = 0

    for raw_item in raw_items:
        item = normalize_bulk_record(raw_item)
        openei_id = item.get("label")
        if not openei_id:
            skipped += 1
            continue

        if openei_id in existing_openei_ids:
            skipped += 1
            continue

        utility_name = item.get("utility", "Unknown")
        eia_id = item.get("eiaid")

        utility = None
        if eia_id and eia_id != 0:
            try:
                utility = eia_id_map.get(int(eia_id))
            except (ValueError, TypeError):
                pass

        if not utility:
            utility = _fuzzy_match(utility_name, exact_name_map, norm_name_map)

        if not utility:
            skipped += 1
            continue

        sector = item.get("sector", "")
        customer_class = SECTOR_MAP.get(sector)
        if not customer_class:
            skipped += 1
            continue

        rate_type = classify_rate_type(item)

        start_ts = item.get("startdate")
        effective = None
        if start_ts:
            try:
                effective = date.fromtimestamp(start_ts)
            except (ValueError, OSError):
                pass

        end_ts = item.get("enddate")
        end_dt = None
        if end_ts:
            try:
                end_dt = date.fromtimestamp(end_ts)
            except (ValueError, OSError):
                pass

        tariff = Tariff(
            utility_id=utility.id,
            name=item.get("name", "Unknown Rate"),
            customer_class=customer_class,
            rate_type=rate_type,
            is_default=bool(item.get("is_default")),
            description=item.get("description"),
            effective_date=effective,
            end_date=end_dt,
            source_url=item.get("source"),
            approved=bool(item.get("approved")),
            openei_id=openei_id,
            raw_openei_data=_make_json_safe(item.get("_raw", item)),
            energy_schedule_weekday=item.get("energyweekdayschedule"),
            energy_schedule_weekend=item.get("energyweekendschedule"),
            demand_schedule_weekday=item.get("demandweekdayschedule"),
            demand_schedule_weekend=item.get("demandweekendschedule"),
        )
        session.add(tariff)
        session.flush()

        raw_components = extract_rate_components(item)
        for comp_data in raw_components:
            comp = RateComponent(
                tariff_id=tariff.id,
                component_type=comp_data["component_type"],
                unit=comp_data["unit"],
                rate_value=comp_data["rate_value"],
                tier_min_kwh=comp_data.get("tier_min_kwh"),
                tier_max_kwh=comp_data.get("tier_max_kwh"),
                period_index=comp_data.get("period_index"),
                adjustment=comp_data.get("adjustment"),
            )
            session.add(comp)

        created += 1
        if created % 100 == 0:
            session.commit()
            print(f"  Committed {created} tariffs...")

    session.commit()
    print(f"  Done: {created} tariffs created, {skipped} skipped")
    return created


def main():
    parser = argparse.ArgumentParser(description="Seed tariff data from OpenEI URDB")
    parser.add_argument("--country", choices=["US", "CA", "both"], default="both")
    args = parser.parse_args()

    engine = get_sync_engine()
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        if args.country in ("US", "both"):
            print("Downloading US URDB bulk data...")
            us_items = download_bulk_rates(USURDB_BULK_URL)
            print(f"Seeding {len(us_items)} US rate records...")
            seed_rates(session, us_items, "US")

        if args.country in ("CA", "both"):
            print("\nDownloading International URDB bulk data (filtering for Canada)...")
            intl_items = download_bulk_rates(IURDB_BULK_URL)
            ca_items = [i for i in intl_items if i.get("country", "").upper() in ("CAN", "CANADA", "CA")]
            print(f"Found {len(ca_items)} Canadian rate records out of {len(intl_items)} international")
            seed_rates(session, ca_items, "CA")

    print("\nOpenEI seeding complete!")


if __name__ == "__main__":
    main()
