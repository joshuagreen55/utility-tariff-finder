import re

from geoalchemy2.functions import ST_Contains, ST_SetSRID, ST_MakePoint
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Utility, ServiceTerritory, Tariff, CustomerClass, Country
from app.schemas.lookup import AddressLookupResponse, GeocodedLocation, UtilityMatch
from app.services.geocoder import geocode_address

US_STATE_ABBREVS = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
}

CA_PROVINCE_ABBREVS = {
    "alberta": "AB", "british columbia": "BC", "manitoba": "MB",
    "new brunswick": "NB", "newfoundland and labrador": "NL", "newfoundland": "NL",
    "nova scotia": "NS", "northwest territories": "NT", "nunavut": "NU",
    "ontario": "ON", "prince edward island": "PE", "quebec": "QC",
    "saskatchewan": "SK", "yukon": "YT",
}

ALL_US_ABBREVS = set(US_STATE_ABBREVS.values())
ALL_CA_ABBREVS = set(CA_PROVINCE_ABBREVS.values())


def _extract_state_and_country(address: str) -> tuple[str | None, Country | None]:
    """Parse state/province abbreviation and country from a formatted address."""
    addr_lower = address.lower()

    is_canada = "canada" in addr_lower
    is_us = "united states" in addr_lower or "usa" in addr_lower

    for full_name, abbrev in CA_PROVINCE_ABBREVS.items():
        if full_name in addr_lower:
            return abbrev, Country.CA

    for full_name, abbrev in US_STATE_ABBREVS.items():
        if full_name in addr_lower:
            return abbrev, Country.US

    tokens = re.findall(r'\b([A-Z]{2})\b', address)
    for token in tokens:
        if is_canada and token in ALL_CA_ABBREVS:
            return token, Country.CA
        if is_us and token in ALL_US_ABBREVS:
            return token, Country.US
        if token in ALL_CA_ABBREVS and not is_us:
            return token, Country.CA
        if token in ALL_US_ABBREVS and not is_canada:
            return token, Country.US

    return None, Country.CA if is_canada else (Country.US if is_us else None)


async def lookup_utilities_by_address(address: str, db: AsyncSession) -> AddressLookupResponse:
    geo = await geocode_address(address)
    if not geo:
        return AddressLookupResponse(geocoded=None, utilities=[])

    geocoded = GeocodedLocation(
        latitude=geo["latitude"],
        longitude=geo["longitude"],
        formatted_address=geo.get("formatted_address"),
    )

    formatted = geo.get("formatted_address") or address
    state, country = _extract_state_and_country(formatted)
    matched_ids: set[int] = set()

    # 1) Point-in-polygon (most precise)
    utilities = await _point_in_polygon_lookup(geo["latitude"], geo["longitude"], db)

    # Filter out cross-border polygon matches (e.g. Canadian address matching US polygon)
    if country:
        utilities = [u for u in utilities if u.country == country.value]
    matched_ids.update(u.id for u in utilities)

    # 2) ZIP / postal code fallback (includes partial FSA matching for Canada)
    if not utilities:
        utilities = await _zip_code_fallback(formatted, db)
        matched_ids.update(u.id for u in utilities)

    # 3) State/province fallback — find all utilities in the same state,
    #    including those with zero tariffs (so users at least see the right utility)
    if state and country:
        state_extras = await _state_fallback(state, country, matched_ids, db)
        utilities.extend(state_extras)

    return AddressLookupResponse(geocoded=geocoded, utilities=utilities)


async def _point_in_polygon_lookup(lat: float, lon: float, db: AsyncSession) -> list[UtilityMatch]:
    point = ST_SetSRID(ST_MakePoint(lon, lat), 4326)

    res_count = (
        select(func.count(Tariff.id))
        .where(Tariff.utility_id == Utility.id, Tariff.customer_class == CustomerClass.RESIDENTIAL)
        .correlate(Utility)
        .scalar_subquery()
    )
    com_count = (
        select(func.count(Tariff.id))
        .where(Tariff.utility_id == Utility.id, Tariff.customer_class == CustomerClass.COMMERCIAL)
        .correlate(Utility)
        .scalar_subquery()
    )

    stmt = (
        select(
            Utility.id,
            Utility.name,
            Utility.country,
            Utility.state_province,
            Utility.utility_type,
            res_count.label("residential_tariff_count"),
            com_count.label("commercial_tariff_count"),
        )
        .join(ServiceTerritory, ServiceTerritory.utility_id == Utility.id)
        .where(ST_Contains(ServiceTerritory.geometry, point))
        .where(Utility.is_active.is_(True))
    )

    result = await db.execute(stmt)
    rows = result.all()

    return [
        UtilityMatch(
            id=r.id,
            name=r.name,
            country=r.country,
            state_province=r.state_province,
            utility_type=r.utility_type,
            match_method="polygon",
            residential_tariff_count=r.residential_tariff_count or 0,
            commercial_tariff_count=r.commercial_tariff_count or 0,
        )
        for r in rows
    ]


async def _state_fallback(
    state: str, country: Country, exclude_ids: set[int], db: AsyncSession
) -> list[UtilityMatch]:
    """Find utilities in the same state/province, supplementing polygon/ZIP
    matches. If other matches already exist, only adds utilities that have
    tariffs. If no other matches exist at all, includes all utilities in the
    state so the user at least sees the correct utility name."""
    res_count = (
        select(func.count(Tariff.id))
        .where(Tariff.utility_id == Utility.id, Tariff.customer_class == CustomerClass.RESIDENTIAL)
        .correlate(Utility)
        .scalar_subquery()
    )
    com_count = (
        select(func.count(Tariff.id))
        .where(Tariff.utility_id == Utility.id, Tariff.customer_class == CustomerClass.COMMERCIAL)
        .correlate(Utility)
        .scalar_subquery()
    )

    stmt = (
        select(
            Utility.id, Utility.name, Utility.country, Utility.state_province,
            Utility.utility_type,
            res_count.label("residential_tariff_count"),
            com_count.label("commercial_tariff_count"),
        )
        .where(
            Utility.state_province == state,
            Utility.country == country,
            Utility.is_active.is_(True),
        )
    )

    # Only require tariffs when other lookup methods already found matches
    if exclude_ids:
        stmt = stmt.where(Utility.id.notin_(exclude_ids))
        tariff_exists = (
            select(Tariff.id).where(Tariff.utility_id == Utility.id).correlate(Utility).exists()
        )
        stmt = stmt.where(tariff_exists)

    result = await db.execute(stmt)
    rows = result.all()

    return [
        UtilityMatch(
            id=r.id,
            name=r.name,
            country=r.country,
            state_province=r.state_province,
            utility_type=r.utility_type,
            match_method="state",
            residential_tariff_count=r.residential_tariff_count or 0,
            commercial_tariff_count=r.commercial_tariff_count or 0,
        )
        for r in rows
    ]


async def _zip_code_fallback(address: str, db: AsyncSession) -> list[UtilityMatch]:
    """Extract ZIP/postal code from address string and match against
    service_territories.zip_codes or postal_code_prefixes arrays.
    Also handles partial Canadian FSA codes (first 3 chars only)."""
    us_zip = re.search(r'\b(\d{5})(?:-\d{4})?\b', address)
    ca_postal_full = re.search(r'\b([A-Za-z]\d[A-Za-z])\s*\d[A-Za-z]\d\b', address)
    ca_postal_fsa = re.search(r'\b([A-Za-z]\d[A-Za-z])\b', address) if not ca_postal_full else None
    ca_postal = ca_postal_full or ca_postal_fsa

    if not us_zip and not ca_postal:
        return []

    res_count = (
        select(func.count(Tariff.id))
        .where(Tariff.utility_id == Utility.id, Tariff.customer_class == CustomerClass.RESIDENTIAL)
        .correlate(Utility)
        .scalar_subquery()
    )
    com_count = (
        select(func.count(Tariff.id))
        .where(Tariff.utility_id == Utility.id, Tariff.customer_class == CustomerClass.COMMERCIAL)
        .correlate(Utility)
        .scalar_subquery()
    )

    if us_zip:
        zip_code = us_zip.group(1)
        stmt = (
            select(
                Utility.id, Utility.name, Utility.country, Utility.state_province,
                Utility.utility_type,
                res_count.label("residential_tariff_count"),
                com_count.label("commercial_tariff_count"),
            )
            .join(ServiceTerritory, ServiceTerritory.utility_id == Utility.id)
            .where(ServiceTerritory.zip_codes.any(zip_code))
            .where(Utility.is_active.is_(True))
        )
    else:
        fsa = ca_postal.group(1).upper()
        first_letter = fsa[0]
        stmt = (
            select(
                Utility.id, Utility.name, Utility.country, Utility.state_province,
                Utility.utility_type,
                res_count.label("residential_tariff_count"),
                com_count.label("commercial_tariff_count"),
            )
            .join(ServiceTerritory, ServiceTerritory.utility_id == Utility.id)
            .where(
                ServiceTerritory.postal_code_prefixes.any(fsa)
                | ServiceTerritory.postal_code_prefixes.any(first_letter)
            )
            .where(Utility.is_active.is_(True))
        )

    result = await db.execute(stmt)
    rows = result.all()

    match_method = "zip_code" if us_zip else "postal_code_prefix"
    return [
        UtilityMatch(
            id=r.id,
            name=r.name,
            country=r.country,
            state_province=r.state_province,
            utility_type=r.utility_type,
            match_method=match_method,
            residential_tariff_count=r.residential_tariff_count or 0,
            commercial_tariff_count=r.commercial_tariff_count or 0,
        )
        for r in rows
    ]
