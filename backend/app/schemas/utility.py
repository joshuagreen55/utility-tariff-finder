from datetime import datetime

from pydantic import BaseModel

from app.models.utility import Country, UtilityType


class UtilityBase(BaseModel):
    name: str
    eia_id: int | None = None
    country: Country
    state_province: str
    utility_type: UtilityType
    website_url: str | None = None


class UtilityCreate(UtilityBase):
    pass


class UtilityRead(UtilityBase):
    id: int
    is_active: bool
    tariff_count: int = 0
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class UtilityListRead(BaseModel):
    id: int
    name: str
    country: Country
    state_province: str
    utility_type: UtilityType
    tariff_count: int = 0

    model_config = {"from_attributes": True}
