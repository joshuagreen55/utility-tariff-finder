"""
Ontario Energy Board (OEB) centralized rate scraper.

The OEB sets electricity commodity rates (RPP) for ALL regulated utilities
in Ontario. Rather than scraping 15+ individual utility websites (many of
which block bots), we scrape the single OEB source of truth and apply the
rates to every Ontario utility in our database.

Three rate plans exist:
  1. Time-of-Use (TOU)        — on-peak / mid-peak / off-peak
  2. Tiered                    — lower tier + higher tier
  3. Ultra-Low Overnight (ULO) — overnight / weekend off-peak / mid-peak / on-peak

Rates change once a year on November 1.

Usage:
    python -m scripts.scrape_oeb_rates
    python -m scripts.scrape_oeb_rates --dry-run
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

import httpx
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("oeb_scraper")

OEB_RATES_URL = "https://www.oeb.ca/consumer-information-and-protection/electricity-rates/historical-electricity-rates"
SOURCE_URL = OEB_RATES_URL


@dataclass
class TOURates:
    effective_date: str
    off_peak: float
    mid_peak: float
    on_peak: float


@dataclass
class TieredRates:
    effective_date: str
    lower_tier_price: float
    higher_tier_price: float
    summer_threshold_kwh: int
    winter_threshold_kwh: int


@dataclass
class ULORates:
    effective_date: str
    ultra_low_overnight: float
    weekend_off_peak: float
    mid_peak: float
    on_peak: float


@dataclass
class OEBRateSet:
    tou: TOURates | None = None
    tiered: TieredRates | None = None
    ulo: ULORates | None = None


# TOU schedules are fixed by the OEB — only the prices change each Nov 1.
WINTER_TOU_SCHEDULE = {
    "season": "winter",
    "period": "Nov 1 - Apr 30",
    "weekday": [
        {"start": "00:00", "end": "07:00", "period": "off-peak"},
        {"start": "07:00", "end": "11:00", "period": "on-peak"},
        {"start": "11:00", "end": "17:00", "period": "mid-peak"},
        {"start": "17:00", "end": "19:00", "period": "on-peak"},
        {"start": "19:00", "end": "24:00", "period": "off-peak"},
    ],
    "weekend_holiday": "off-peak (all day)",
}

SUMMER_TOU_SCHEDULE = {
    "season": "summer",
    "period": "May 1 - Oct 31",
    "weekday": [
        {"start": "00:00", "end": "07:00", "period": "off-peak"},
        {"start": "07:00", "end": "11:00", "period": "mid-peak"},
        {"start": "11:00", "end": "17:00", "period": "on-peak"},
        {"start": "17:00", "end": "19:00", "period": "mid-peak"},
        {"start": "19:00", "end": "24:00", "period": "off-peak"},
    ],
    "weekend_holiday": "off-peak (all day)",
}

ULO_SCHEDULE = {
    "weekday": [
        {"start": "00:00", "end": "07:00", "period": "ultra-low overnight"},
        {"start": "07:00", "end": "11:00", "period": "on-peak"},
        {"start": "11:00", "end": "17:00", "period": "mid-peak"},
        {"start": "17:00", "end": "19:00", "period": "on-peak"},
        {"start": "19:00", "end": "24:00", "period": "ultra-low overnight"},
    ],
    "weekend": [
        {"start": "00:00", "end": "07:00", "period": "ultra-low overnight"},
        {"start": "07:00", "end": "19:00", "period": "weekend off-peak"},
        {"start": "19:00", "end": "24:00", "period": "ultra-low overnight"},
    ],
}


def fetch_oeb_page() -> str:
    """Fetch the OEB historical rates page."""
    log.info(f"Fetching OEB rates page: {OEB_RATES_URL}")
    resp = httpx.get(
        OEB_RATES_URL,
        headers={"User-Agent": "UtilityTariffFinder/1.0"},
        timeout=30.0,
        follow_redirects=True,
    )
    resp.raise_for_status()
    return resp.text


def _parse_cents(val: str) -> float | None:
    """Parse a cents/kWh value to $/kWh float.

    Returns None only when the input is empty or unparseable.
    A legitimate 0.0 rate is returned as 0.0.
    """
    val = val.strip().replace(",", "")
    if not val:
        return None
    try:
        cents = float(val)
        dollars = round(cents / 100.0, 5)
        if dollars < 0 or dollars > 1.0:
            log.warning(f"  Rate value {dollars} $/kWh outside expected range (0–1.0), raw='{val}'")
            return None
        return dollars
    except ValueError:
        return None


def _parse_table_rows(table) -> list[dict]:
    """Parse an HTML table, returning list of dicts keyed by header."""
    headers = []
    for th in table.find_all("th"):
        headers.append(th.get_text(strip=True))

    rows = []
    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue
        row = {}
        for i, cell in enumerate(cells):
            key = headers[i] if i < len(headers) else f"col_{i}"
            row[key] = cell.get_text(strip=True)
        rows.append(row)
    return rows


def parse_oeb_rates(html: str) -> OEBRateSet:
    """Parse the OEB historical rates page and extract the most recent rates."""
    soup = BeautifulSoup(html, "lxml")
    rates = OEBRateSet()

    tables = soup.find_all("table")
    log.info(f"Found {len(tables)} tables on OEB page")

    for table in tables:
        rows = _parse_table_rows(table)
        if not rows:
            continue

        headers = list(rows[0].keys())
        header_text = " ".join(headers).lower()

        def _find_header(target: str) -> str | None:
            """Find the header key containing target text (case-insensitive)."""
            for h in headers:
                if target.lower() in h.lower():
                    return h
            return None

        if "off-peak" in header_text and "mid-peak" in header_text and "ultra-low" not in header_text:
            # TOU table — look up columns by header text instead of fixed index
            off_key = _find_header("off-peak") or (headers[1] if len(headers) > 1 else "")
            mid_key = _find_header("mid-peak") or (headers[2] if len(headers) > 2 else "")
            on_key = _find_header("on-peak") or (headers[3] if len(headers) > 3 else "")
            for row in rows:
                eff = row.get(headers[0], "")
                if not eff or not re.search(r"\d{4}", eff):
                    continue
                off = _parse_cents(row.get(off_key, ""))
                mid = _parse_cents(row.get(mid_key, ""))
                on = _parse_cents(row.get(on_key, ""))
                if off is not None and mid is not None and on is not None:
                    eff_date = _parse_effective_date(eff)
                    rates.tou = TOURates(
                        effective_date=eff_date,
                        off_peak=off, mid_peak=mid, on_peak=on,
                    )
                    log.info(f"TOU rates found: {eff_date} — Off={off}, Mid={mid}, On={on}")
                    break

        elif "ultra-low" in header_text:
            # ULO table
            ulo_key = _find_header("ultra-low") or (headers[1] if len(headers) > 1 else "")
            wop_key = _find_header("weekend") or (headers[2] if len(headers) > 2 else "")
            mid_key = _find_header("mid-peak") or (headers[3] if len(headers) > 3 else "")
            on_key = _find_header("on-peak") or (headers[4] if len(headers) > 4 else "")
            for row in rows:
                eff = row.get(headers[0], "")
                if not eff or not re.search(r"\d{4}", eff):
                    continue
                ulo = _parse_cents(row.get(ulo_key, ""))
                wop = _parse_cents(row.get(wop_key, ""))
                mid = _parse_cents(row.get(mid_key, ""))
                on = _parse_cents(row.get(on_key, ""))
                if all(v is not None for v in (ulo, wop, mid, on)):
                    eff_date = _parse_effective_date(eff)
                    rates.ulo = ULORates(
                        effective_date=eff_date,
                        ultra_low_overnight=ulo,
                        weekend_off_peak=wop,
                        mid_peak=mid,
                        on_peak=on,
                    )
                    log.info(f"ULO rates found: {eff_date} — ULO={ulo}, WOP={wop}, Mid={mid}, On={on}")
                    break

        elif "lower" in header_text and "tier" in header_text:
            # Tiered table
            lower_key = _find_header("lower") or (headers[1] if len(headers) > 1 else "")
            higher_key = _find_header("higher") or (headers[3] if len(headers) > 3 else "")
            threshold_key = _find_header("threshold") or (headers[2] if len(headers) > 2 else "")
            for row in rows:
                eff = row.get(headers[0], "")
                if not eff or not re.search(r"\d{4}", eff):
                    continue
                lower = _parse_cents(row.get(lower_key, ""))
                threshold_str = row.get(threshold_key, "")
                higher = _parse_cents(row.get(higher_key, ""))
                if lower is not None and higher is not None:
                    eff_date = _parse_effective_date(eff)
                    summer_t, winter_t = _parse_thresholds(threshold_str)
                    rates.tiered = TieredRates(
                        effective_date=eff_date,
                        lower_tier_price=lower,
                        higher_tier_price=higher,
                        summer_threshold_kwh=summer_t,
                        winter_threshold_kwh=winter_t,
                    )
                    log.info(f"Tiered rates found: {eff_date} — Lower={lower}, Higher={higher}, Thresholds={summer_t}/{winter_t}")
                    break

    # If table parsing failed (JS-rendered cells), try chart data fallback
    if not rates.tou or not rates.tiered:
        log.info("Table parsing incomplete — trying chart data fallback")
        rates = _parse_chart_fallback(soup, rates)

    return rates


def _parse_effective_date(text: str) -> str:
    """Parse 'Nov 1, 2025' -> '2025-11-01'."""
    m = re.search(r"(\w+)\s+(\d+),?\s+(\d{4})", text)
    if m:
        month_map = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
        }
        month = month_map.get(m.group(1).lower()[:3], 11)
        return f"{m.group(3)}-{month:02d}-{int(m.group(2)):02d}"
    return ""


def _parse_thresholds(text: str) -> tuple[int, int]:
    """Parse '600 (Summer) 1,000 (Winter)' -> (600, 1000)."""
    numbers = re.findall(r"([\d,]+)", text)
    if len(numbers) >= 2:
        return int(numbers[0].replace(",", "")), int(numbers[1].replace(",", ""))
    elif len(numbers) == 1:
        val = int(numbers[0].replace(",", ""))
        return val, val
    return 600, 1000


def _parse_chart_fallback(soup: BeautifulSoup, rates: OEBRateSet) -> OEBRateSet:
    """Extract rate values from chart/graph labels when table cells are JS-rendered.

    The OEB page embeds the latest values in the page text above/near the tables,
    used for the chart visualization. We look for floating-point numbers in the
    right context.
    """
    text = soup.get_text()

    if not rates.tou:
        # Look for three consecutive decimal numbers before the TOU table
        # Pattern: 9.8\n15.7\n20.3 (off-peak, mid-peak, on-peak)
        tou_match = re.search(
            r"Time-of-Use.*?(\d+\.\d)\s+(\d+\.\d)\s+(\d+\.\d)",
            text, re.DOTALL | re.IGNORECASE,
        )
        if tou_match:
            off = _parse_cents(tou_match.group(1))
            mid = _parse_cents(tou_match.group(2))
            on = _parse_cents(tou_match.group(3))
            if off is not None and mid is not None and on is not None:
                today = date.today()
                year = today.year if today.month >= 11 else today.year - 1
                eff_date = f"{year}-11-01"
                rates.tou = TOURates(
                    effective_date=eff_date,
                    off_peak=off, mid_peak=mid, on_peak=on,
                )
                log.info(f"TOU rates from chart: Off={off}, Mid={mid}, On={on}")

    if not rates.tiered:
        tiered_match = re.search(
            r"Tiered rates.*?(\d+\.\d)\s+.*?(\d+\.\d)",
            text, re.DOTALL | re.IGNORECASE,
        )
        if tiered_match:
            lower = _parse_cents(tiered_match.group(1))
            higher = _parse_cents(tiered_match.group(2))
            if lower is not None and higher is not None:
                today = date.today()
                year = today.year if today.month >= 11 else today.year - 1
                eff_date = f"{year}-11-01"
                rates.tiered = TieredRates(
                    effective_date=eff_date,
                    lower_tier_price=lower,
                    higher_tier_price=higher,
                    summer_threshold_kwh=600,
                    winter_threshold_kwh=1000,
                )
                log.info(f"Tiered rates from chart: Lower={lower}, Higher={higher}")

    if not rates.ulo:
        ulo_match = re.search(
            r"Ultra-Low.*?(\d+\.\d)\s+(\d+\.\d)\s+(\d+\.\d)\s+(\d+\.\d)",
            text, re.DOTALL | re.IGNORECASE,
        )
        if ulo_match:
            ulo_val = _parse_cents(ulo_match.group(1))
            wop = _parse_cents(ulo_match.group(2))
            mid = _parse_cents(ulo_match.group(3))
            on = _parse_cents(ulo_match.group(4))
            if all(v is not None for v in (ulo_val, wop, mid, on)):
                today = date.today()
                year = today.year if today.month >= 11 else today.year - 1
                eff_date = f"{year}-11-01"
                rates.ulo = ULORates(
                    effective_date=eff_date,
                    ultra_low_overnight=ulo_val,
                    weekend_off_peak=wop,
                    mid_peak=mid,
                    on_peak=on,
                )
                log.info(f"ULO rates from chart: ULO={ulo_val}, WOP={wop}, Mid={mid}, On={on}")

    return rates


def get_ontario_utilities() -> list[dict]:
    """Get all active Ontario utilities from the database."""
    from sqlalchemy import select
    from sqlalchemy.orm import Session
    from app.db.session import get_sync_engine
    from app.models import Utility

    engine = get_sync_engine()
    with Session(engine) as session:
        results = session.execute(
            select(Utility).where(
                Utility.country == "CA",
                Utility.state_province == "ON",
                Utility.is_active == True,
            )
        ).scalars().all()

        return [
            {"id": u.id, "name": u.name}
            for u in results
        ]


def build_tariff_entries(rates: OEBRateSet, customer_class: str) -> list[dict]:
    """Build tariff dicts from parsed OEB rates for a given customer class.

    Returns entries compatible with the store_tariffs format.
    """
    tariffs = []
    class_label = "Residential" if customer_class == "residential" else "Small Business"

    if rates.tou:
        tariffs.append({
            "name": f"Time-of-Use (TOU) — {class_label}",
            "code": "OEB-RPP-TOU",
            "customer_class": customer_class,
            "rate_type": "tou",
            "description": (
                f"Ontario Regulated Price Plan — Time-of-Use pricing for {class_label.lower()} customers. "
                f"Rates set by the Ontario Energy Board, effective {rates.tou.effective_date}. "
                f"Prices vary by time of day: on-peak, mid-peak, and off-peak. "
                f"Weekends and holidays are off-peak all day."
            ),
            "source_url": SOURCE_URL,
            "effective_date": rates.tou.effective_date,
            "components": [
                {
                    "component_type": "energy",
                    "unit": "$/kWh",
                    "rate_value": rates.tou.on_peak,
                    "period_label": "On-Peak",
                },
                {
                    "component_type": "energy",
                    "unit": "$/kWh",
                    "rate_value": rates.tou.mid_peak,
                    "period_label": "Mid-Peak",
                },
                {
                    "component_type": "energy",
                    "unit": "$/kWh",
                    "rate_value": rates.tou.off_peak,
                    "period_label": "Off-Peak",
                },
            ],
        })

    if rates.tiered:
        tariffs.append({
            "name": f"Tiered Pricing — {class_label}",
            "code": "OEB-RPP-TIERED",
            "customer_class": customer_class,
            "rate_type": "tiered",
            "description": (
                f"Ontario Regulated Price Plan — Tiered pricing for {class_label.lower()} customers. "
                f"Rates set by the Ontario Energy Board, effective {rates.tiered.effective_date}. "
                f"Lower tier applies to first {rates.tiered.summer_threshold_kwh} kWh/month (summer) "
                f"or {rates.tiered.winter_threshold_kwh} kWh/month (winter)."
            ),
            "source_url": SOURCE_URL,
            "effective_date": rates.tiered.effective_date,
            "components": [
                {
                    "component_type": "energy",
                    "unit": "$/kWh",
                    "rate_value": rates.tiered.lower_tier_price,
                    "tier_label": f"Lower Tier (up to {rates.tiered.summer_threshold_kwh}/{rates.tiered.winter_threshold_kwh} kWh)",
                    "tier_min_kwh": 0,
                    "tier_max_kwh": rates.tiered.summer_threshold_kwh,
                    "season": "summer",
                },
                {
                    "component_type": "energy",
                    "unit": "$/kWh",
                    "rate_value": rates.tiered.lower_tier_price,
                    "tier_label": f"Lower Tier (up to {rates.tiered.winter_threshold_kwh} kWh)",
                    "tier_min_kwh": 0,
                    "tier_max_kwh": rates.tiered.winter_threshold_kwh,
                    "season": "winter",
                },
                {
                    "component_type": "energy",
                    "unit": "$/kWh",
                    "rate_value": rates.tiered.higher_tier_price,
                    "tier_label": "Higher Tier (above threshold)",
                    "tier_min_kwh": rates.tiered.summer_threshold_kwh,
                    "season": "summer",
                },
                {
                    "component_type": "energy",
                    "unit": "$/kWh",
                    "rate_value": rates.tiered.higher_tier_price,
                    "tier_label": "Higher Tier (above threshold)",
                    "tier_min_kwh": rates.tiered.winter_threshold_kwh,
                    "season": "winter",
                },
            ],
        })

    if rates.ulo:
        tariffs.append({
            "name": f"Ultra-Low Overnight (ULO) — {class_label}",
            "code": "OEB-RPP-ULO",
            "customer_class": customer_class,
            "rate_type": "tou",
            "description": (
                f"Ontario Regulated Price Plan — Ultra-Low Overnight pricing for {class_label.lower()} customers. "
                f"Rates set by the Ontario Energy Board, effective {rates.ulo.effective_date}. "
                f"Designed for customers with significant overnight usage (e.g. EV charging). "
                f"Very low overnight rate, higher on-peak rate."
            ),
            "source_url": SOURCE_URL,
            "effective_date": rates.ulo.effective_date,
            "components": [
                {
                    "component_type": "energy",
                    "unit": "$/kWh",
                    "rate_value": rates.ulo.on_peak,
                    "period_label": "On-Peak",
                },
                {
                    "component_type": "energy",
                    "unit": "$/kWh",
                    "rate_value": rates.ulo.mid_peak,
                    "period_label": "Mid-Peak",
                },
                {
                    "component_type": "energy",
                    "unit": "$/kWh",
                    "rate_value": rates.ulo.weekend_off_peak,
                    "period_label": "Weekend Off-Peak",
                },
                {
                    "component_type": "energy",
                    "unit": "$/kWh",
                    "rate_value": rates.ulo.ultra_low_overnight,
                    "period_label": "Ultra-Low Overnight",
                },
            ],
        })

    return tariffs


def store_oeb_tariffs(utility_id: int, tariff_entries: list[dict], dry_run: bool) -> int:
    """Store OEB tariffs for a single Ontario utility."""
    if dry_run:
        return len(tariff_entries)

    from sqlalchemy import select
    from sqlalchemy.orm import Session
    from app.db.session import get_sync_engine
    from app.models import Tariff, RateComponent, CustomerClass, RateType, ComponentType

    CLASS_MAP = {
        "residential": CustomerClass.RESIDENTIAL,
        "commercial": CustomerClass.COMMERCIAL,
    }
    TYPE_MAP = {
        "tou": RateType.TOU,
        "tiered": RateType.TIERED,
    }
    COMP_MAP = {
        "energy": ComponentType.ENERGY,
        "fixed": ComponentType.FIXED,
    }

    engine = get_sync_engine()
    stored = 0

    with Session(engine) as session:
        for entry in tariff_entries:
            cc = CLASS_MAP.get(entry["customer_class"])
            rt = TYPE_MAP.get(entry["rate_type"])
            if not cc or not rt:
                continue

            eff_date = None
            if entry.get("effective_date"):
                try:
                    eff_date = date.fromisoformat(entry["effective_date"])
                except ValueError:
                    pass

            existing = session.execute(
                select(Tariff).where(
                    Tariff.utility_id == utility_id,
                    Tariff.name == entry["name"],
                    Tariff.customer_class == cc,
                )
            ).scalar_one_or_none()

            if existing:
                existing.rate_type = rt
                existing.description = entry.get("description", "")
                existing.source_url = entry.get("source_url", "")
                existing.effective_date = eff_date
                existing.code = entry.get("code", "")
                existing.last_verified_at = datetime.now(timezone.utc)
                tariff_obj = existing
            else:
                tariff_obj = Tariff(
                    utility_id=utility_id,
                    name=entry["name"],
                    code=entry.get("code", ""),
                    customer_class=cc,
                    rate_type=rt,
                    description=entry.get("description", ""),
                    source_url=entry.get("source_url", ""),
                    effective_date=eff_date,
                    last_verified_at=datetime.now(timezone.utc),
                    approved=True,
                )
                session.add(tariff_obj)

            new_components = []
            for comp in entry.get("components", []):
                ct = COMP_MAP.get(comp.get("component_type"))
                if not ct:
                    continue
                new_components.append(RateComponent(
                    component_type=ct,
                    unit=comp.get("unit", "$/kWh"),
                    rate_value=comp["rate_value"],
                    tier_min_kwh=comp.get("tier_min_kwh"),
                    tier_max_kwh=comp.get("tier_max_kwh"),
                    tier_label=comp.get("tier_label"),
                    period_label=comp.get("period_label"),
                    season=comp.get("season"),
                ))

            if not new_components:
                log.warning(f"  Skipping tariff '{entry['name']}' — 0 valid components")
                continue

            if existing:
                tariff_obj.rate_components.clear()
            for rc in new_components:
                tariff_obj.rate_components.append(rc)

            stored += 1

        session.commit()

    return stored


def main():
    parser = argparse.ArgumentParser(description="Scrape OEB rates and apply to all Ontario utilities")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to database")
    parser.add_argument("--output", type=str, help="Write parsed rates JSON to file")
    args = parser.parse_args()

    # Fetch and parse OEB page
    html = fetch_oeb_page()
    rates = parse_oeb_rates(html)

    if not rates.tou and not rates.tiered and not rates.ulo:
        log.error("Failed to parse any rates from OEB page")
        sys.exit(1)

    found = []
    if rates.tou:
        found.append(f"TOU ({rates.tou.effective_date})")
    if rates.tiered:
        found.append(f"Tiered ({rates.tiered.effective_date})")
    if rates.ulo:
        found.append(f"ULO ({rates.ulo.effective_date})")
    log.info(f"Parsed OEB rates: {', '.join(found)}")

    # Build tariff entries for both customer classes
    residential_tariffs = build_tariff_entries(rates, "residential")
    commercial_tariffs = build_tariff_entries(rates, "commercial")
    all_tariff_templates = residential_tariffs + commercial_tariffs

    log.info(f"Built {len(all_tariff_templates)} tariff templates (res + commercial)")

    if args.output:
        output_data = {
            "source": SOURCE_URL,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "tou": {
                "effective_date": rates.tou.effective_date if rates.tou else None,
                "off_peak_dollar_kwh": rates.tou.off_peak if rates.tou else None,
                "mid_peak_dollar_kwh": rates.tou.mid_peak if rates.tou else None,
                "on_peak_dollar_kwh": rates.tou.on_peak if rates.tou else None,
            },
            "tiered": {
                "effective_date": rates.tiered.effective_date if rates.tiered else None,
                "lower_tier_dollar_kwh": rates.tiered.lower_tier_price if rates.tiered else None,
                "higher_tier_dollar_kwh": rates.tiered.higher_tier_price if rates.tiered else None,
                "summer_threshold_kwh": rates.tiered.summer_threshold_kwh if rates.tiered else None,
                "winter_threshold_kwh": rates.tiered.winter_threshold_kwh if rates.tiered else None,
            },
            "ulo": {
                "effective_date": rates.ulo.effective_date if rates.ulo else None,
                "overnight_dollar_kwh": rates.ulo.ultra_low_overnight if rates.ulo else None,
                "weekend_off_peak_dollar_kwh": rates.ulo.weekend_off_peak if rates.ulo else None,
                "mid_peak_dollar_kwh": rates.ulo.mid_peak if rates.ulo else None,
                "on_peak_dollar_kwh": rates.ulo.on_peak if rates.ulo else None,
            },
            "tariff_templates": all_tariff_templates,
        }
        with open(args.output, "w") as f:
            json.dump(output_data, f, indent=2)
        log.info(f"Rates written to {args.output}")

    # Get Ontario utilities and apply rates
    utilities = get_ontario_utilities()
    if not utilities:
        log.warning("No Ontario utilities found in database")
        return

    log.info(f"Applying OEB rates to {len(utilities)} Ontario utilities...")

    total_stored = 0
    for util in utilities:
        count = store_oeb_tariffs(util["id"], all_tariff_templates, args.dry_run)
        total_stored += count
        action = "Would store" if args.dry_run else "Stored"
        log.info(f"  {action} {count} tariffs for {util['name']} (id={util['id']})")

    print("\n" + "=" * 60)
    print("OEB RATE SCRAPER SUMMARY")
    print("=" * 60)
    if rates.tou:
        print(f"  TOU  (eff. {rates.tou.effective_date}): Off={rates.tou.off_peak:.5f} Mid={rates.tou.mid_peak:.5f} On={rates.tou.on_peak:.5f} $/kWh")
    if rates.tiered:
        print(f"  Tier (eff. {rates.tiered.effective_date}): Lower={rates.tiered.lower_tier_price:.5f} Higher={rates.tiered.higher_tier_price:.5f} $/kWh")
    if rates.ulo:
        print(f"  ULO  (eff. {rates.ulo.effective_date}): Night={rates.ulo.ultra_low_overnight:.5f} WOP={rates.ulo.weekend_off_peak:.5f} Mid={rates.ulo.mid_peak:.5f} On={rates.ulo.on_peak:.5f} $/kWh")
    print(f"\n  Utilities updated: {len(utilities)}")
    print(f"  Total tariffs {'created/updated' if not args.dry_run else 'would create/update'}: {total_stored}")
    if args.dry_run:
        print("  (DRY RUN — no database changes made)")


if __name__ == "__main__":
    main()
