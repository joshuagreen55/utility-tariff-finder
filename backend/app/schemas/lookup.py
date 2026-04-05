from pydantic import BaseModel

from app.models.utility import Country, UtilityType


class AddressLookupRequest(BaseModel):
    address: str


class GeocodedLocation(BaseModel):
    latitude: float
    longitude: float
    formatted_address: str | None = None


class UtilityMatch(BaseModel):
    id: int
    name: str
    country: Country
    state_province: str
    utility_type: UtilityType
    match_method: str
    residential_tariff_count: int = 0
    commercial_tariff_count: int = 0

    model_config = {"from_attributes": True}


class AddressLookupResponse(BaseModel):
    geocoded: GeocodedLocation | None = None
    utilities: list[UtilityMatch] = []
