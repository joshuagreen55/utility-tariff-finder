"""
Seed US service territory polygons from HIFLD Electric Retail Service Territories.

Downloads GIS data from the HIFLD ArcGIS FeatureServer and loads polygons
into PostGIS. Also seeds Canadian province-based territory mappings.

Usage:
    python -m scripts.seed_territories [--us] [--canada]
"""

import argparse
import json

import httpx
from shapely.geometry import shape, MultiPolygon, Polygon
from geoalchemy2.shape import from_shape
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.session import get_sync_engine
from app.db.base import Base
from app.models import Utility, ServiceTerritory, Country

HIFLD_URL = (
    "https://services3.arcgis.com/OYP7N6mAJJCyH6hd/ArcGIS/rest/services/"
    "Electric_Retail_Service_Territories_HIFLD/FeatureServer/0/query"
)

CANADIAN_PROVINCE_POSTAL_PREFIXES = {
    "BC": ["V"],
    "AB": ["T"],
    "SK": ["S"],
    "MB": ["R"],
    "ON": ["K", "L", "M", "N", "P"],
    "QC": ["G", "H", "J"],
    "NB": ["E"],
    "NS": ["B"],
    "PE": ["C"],
    "NL": ["A"],
    "YT": ["Y"],
    "NT": ["X"],
    "NU": ["X"],
}


def fetch_hifld_territories(batch_size: int = 250) -> list[dict]:
    """Fetch territory features from HIFLD ArcGIS FeatureServer with pagination."""
    all_features = []
    offset = 0

    with httpx.Client(timeout=120) as client:
        while True:
            params = {
                "where": "1=1",
                "outFields": "NAME,ID,STATE,NAICS_DESC,ZIP,TELEPHONE",
                "outSR": "4326",
                "f": "geojson",
                "resultOffset": offset,
                "resultRecordCount": batch_size,
            }
            print(f"  Fetching batch at offset {offset}...")
            for attempt in range(3):
                try:
                    resp = client.get(HIFLD_URL, params=params)
                    resp.raise_for_status()
                    break
                except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
                    if attempt == 2:
                        raise
                    print(f"  Timeout, retrying ({attempt + 1}/3)...")
                    import time
                    time.sleep(5)
            data = resp.json()
            features = data.get("features", [])

            if not features:
                break

            all_features.extend(features)
            offset += len(features)

            if len(features) < batch_size:
                break

    return all_features


def _build_name_index(utilities: list) -> dict:
    """Build a case-insensitive name lookup index."""
    index = {}
    for u in utilities:
        index[u.name.lower()] = u
        for word in u.name.lower().split():
            if len(word) > 3:
                index.setdefault(f"_partial_{word}", [])
                index[f"_partial_{word}"].append(u)
    return index


def _find_utility(name: str, name_index: dict):
    """Find utility by name using the prebuilt index."""
    lower = name.lower()
    if lower in name_index:
        return name_index[lower]
    for key, val in name_index.items():
        if not key.startswith("_partial_") and lower in key:
            return val
    return None


def seed_us_territories(session: Session):
    """Download and insert HIFLD territory polygons, linking to existing utilities."""
    print("Fetching HIFLD territory data...")
    features = fetch_hifld_territories()
    print(f"Downloaded {len(features)} territory features")

    us_utilities = session.execute(
        select(Utility).where(Utility.country == Country.US)
    ).scalars().all()
    name_index = _build_name_index(us_utilities)
    existing_territory_ids = set(
        session.execute(select(ServiceTerritory.utility_id)).scalars().all()
    )
    print(f"  {len(us_utilities)} US utilities, {len(existing_territory_ids)} existing territories")

    created = 0
    skipped = 0

    for feat in features:
        props = feat.get("properties", {})
        geom_data = feat.get("geometry")

        name = props.get("NAME", "").strip()
        if not name or not geom_data:
            skipped += 1
            continue

        utility = _find_utility(name, name_index)

        if not utility:
            skipped += 1
            continue

        try:
            geom = shape(geom_data)
            if isinstance(geom, Polygon):
                geom = MultiPolygon([geom])
            elif not isinstance(geom, MultiPolygon):
                skipped += 1
                continue
        except Exception:
            skipped += 1
            continue

        zip_codes = None
        if props.get("ZIP"):
            zip_codes = [z.strip() for z in str(props["ZIP"]).split(",") if z.strip()]

        if utility.id in existing_territory_ids:
            skipped += 1
            continue

        territory = ServiceTerritory(
            utility_id=utility.id,
            geometry=from_shape(geom, srid=4326),
            zip_codes=zip_codes,
            source="HIFLD",
        )
        session.add(territory)
        existing_territory_ids.add(utility.id)
        created += 1

        if created % 100 == 0:
            session.commit()
            print(f"  Committed {created} territories...")

    session.commit()
    print(f"US territories: {created} created, {skipped} skipped")


def seed_canadian_territories(session: Session):
    """Insert postal-code-prefix-based territory mappings for Canadian utilities."""
    created = 0

    for province, prefixes in CANADIAN_PROVINCE_POSTAL_PREFIXES.items():
        utilities = session.execute(
            select(Utility).where(
                Utility.country == Country.CA,
                Utility.state_province == province,
            )
        ).scalars().all()

        for utility in utilities:
            existing = session.execute(
                select(ServiceTerritory).where(ServiceTerritory.utility_id == utility.id)
            ).scalar_one_or_none()
            if existing:
                continue

            territory = ServiceTerritory(
                utility_id=utility.id,
                postal_code_prefixes=prefixes,
                source="province_mapping",
            )
            session.add(territory)
            created += 1

    session.commit()
    print(f"Canadian territories: {created} created")


def main():
    parser = argparse.ArgumentParser(description="Seed service territory data")
    parser.add_argument("--us", action="store_true", help="Seed US territories from HIFLD")
    parser.add_argument("--canada", action="store_true", help="Seed Canadian territories")
    args = parser.parse_args()

    if not args.us and not args.canada:
        args.us = True
        args.canada = True

    engine = get_sync_engine()
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        if args.us:
            seed_us_territories(session)
        if args.canada:
            seed_canadian_territories(session)

    print("Territory seeding complete!")


if __name__ == "__main__":
    main()
