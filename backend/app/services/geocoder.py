import re

import httpx

from app.config import settings

_CA_POSTAL_PATTERN = re.compile(r'[A-Za-z]\d[A-Za-z]\s*\d[A-Za-z]\d')
_CA_KEYWORDS = re.compile(
    r'\b(canada|alberta|british columbia|manitoba|new brunswick|'
    r'newfoundland|nova scotia|northwest territories|nunavut|'
    r'ontario|prince edward island|quebec|québec|saskatchewan|yukon|'
    r'AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)\b',
    re.IGNORECASE,
)


def _looks_canadian(address: str) -> bool:
    """Heuristic: does this address appear to be Canadian?"""
    if _CA_POSTAL_PATTERN.search(address):
        return True
    if "canada" in address.lower():
        return True
    matches = _CA_KEYWORDS.findall(address)
    return len(matches) >= 2


async def geocode_address(address: str) -> dict | None:
    """Geocode using Census (US), then Nominatim, then Google as final fallback.
    Skips the US Census geocoder for Canadian-looking addresses to avoid
    cross-border mismatches."""

    is_ca = _looks_canadian(address)

    if not is_ca:
        result = await _census_geocode(address)
        if result:
            return result

    if settings.google_maps_api_key:
        result = await _google_geocode(address, country_hint="CA" if is_ca else None)
        if result:
            return result

    result = await _nominatim_geocode(address)
    if result:
        return result

    if not is_ca:
        if settings.google_maps_api_key:
            result = await _google_geocode(address)
            if result:
                return result

    return None


async def _census_geocode(address: str) -> dict | None:
    """US Census Bureau geocoder - free, no API key, US addresses only."""
    url = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
    params = {
        "address": address,
        "benchmark": "Public_AR_Current",
        "format": "json",
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

        matches = data.get("result", {}).get("addressMatches", [])
        if not matches:
            return None

        match = matches[0]
        coords = match["coordinates"]
        return {
            "latitude": coords["y"],
            "longitude": coords["x"],
            "formatted_address": match.get("matchedAddress"),
        }
    except Exception:
        return None


async def _google_geocode(address: str, country_hint: str | None = None) -> dict | None:
    """Google Maps Geocoding API — best coverage, requires API key.
    Retries once on OVER_QUERY_LIMIT responses."""
    import asyncio

    url = "https://maps.googleapis.com/maps/api/geocode/json"
    components = f"country:{country_hint}" if country_hint else "country:US|country:CA"
    params = {
        "address": address,
        "key": settings.google_maps_api_key,
        "components": components,
    }
    for attempt in range(2):
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                data = resp.json()

            status = data.get("status", "")
            if status == "OVER_QUERY_LIMIT" and attempt == 0:
                await asyncio.sleep(2)
                continue

            results = data.get("results", [])
            if not results:
                return None

            loc = results[0]["geometry"]["location"]
            return {
                "latitude": loc["lat"],
                "longitude": loc["lng"],
                "formatted_address": results[0].get("formatted_address"),
            }
        except Exception:
            return None
    return None


async def _nominatim_geocode(address: str) -> dict | None:
    """OpenStreetMap Nominatim - free, works for US and Canada."""
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": address,
        "format": "json",
        "limit": 1,
        "countrycodes": "us,ca",
    }
    headers = {"User-Agent": "UtilityTariffFinder/0.1"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, params=params, headers=headers)
            resp.raise_for_status()
            data = resp.json()

        if not data:
            return None

        hit = data[0]
        return {
            "latitude": float(hit["lat"]),
            "longitude": float(hit["lon"]),
            "formatted_address": hit.get("display_name"),
        }
    except Exception:
        return None
