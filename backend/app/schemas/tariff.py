from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, model_validator

from app.models.tariff import ComponentType, CustomerClass, RateType

_CURRENT_CUTOFF_YEARS = 2
_AGING_CUTOFF_YEARS = 5


def _compute_freshness(effective_date: date | None, last_verified_at: datetime | None) -> str:
    """Compute data freshness: 'current', 'aging', or 'stale'."""
    ref = None
    if last_verified_at:
        ref = last_verified_at.date()
    elif effective_date:
        ref = effective_date

    if ref is None:
        return "stale"

    age_days = (date.today() - ref).days
    if age_days <= _CURRENT_CUTOFF_YEARS * 365:
        return "current"
    if age_days <= _AGING_CUTOFF_YEARS * 365:
        return "aging"
    return "stale"


class RateComponentRead(BaseModel):
    id: int
    component_type: ComponentType
    unit: str
    rate_value: float
    tier_min_kwh: float | None = None
    tier_max_kwh: float | None = None
    tier_label: str | None = None
    period_index: int | None = None
    period_label: str | None = None
    season: str | None = None
    adjustment: float | None = None

    model_config = {"from_attributes": True}


class TariffListRead(BaseModel):
    id: int
    utility_id: int
    name: str
    code: str | None = None
    customer_class: CustomerClass
    rate_type: RateType
    is_default: bool
    effective_date: date | None = None
    end_date: date | None = None
    approved: bool
    last_verified_at: datetime | None = None
    data_freshness: str = "stale"

    model_config = {"from_attributes": True}

    @model_validator(mode="after")
    def set_freshness(self) -> "TariffListRead":
        self.data_freshness = _compute_freshness(self.effective_date, self.last_verified_at)
        return self


class TariffDetailRead(TariffListRead):
    description: str | None = None
    source_url: str | None = None
    rate_components: list[RateComponentRead] = []
    energy_schedule_weekday: Any | None = None
    energy_schedule_weekend: Any | None = None
    demand_schedule_weekday: Any | None = None
    demand_schedule_weekend: Any | None = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class TariffBrowseRead(BaseModel):
    """Tariff with utility context for the browse/filter view."""
    id: int
    utility_id: int
    utility_name: str
    country: str
    state_province: str
    name: str
    code: str | None = None
    customer_class: CustomerClass
    rate_type: RateType
    is_default: bool
    effective_date: date | None = None
    last_verified_at: datetime | None = None
    component_count: int = 0
    data_freshness: str = "stale"

    model_config = {"from_attributes": True}

    @model_validator(mode="after")
    def set_freshness(self) -> "TariffBrowseRead":
        self.data_freshness = _compute_freshness(self.effective_date, self.last_verified_at)
        return self


class TariffBrowseResponse(BaseModel):
    items: list[TariffBrowseRead]
    total: int
    limit: int
    offset: int


class TariffSourceRead(BaseModel):
    tariff_id: int
    source_url: str | None = None
    source_document_hash: str | None = None
    last_verified_at: datetime | None = None
    approved: bool

    model_config = {"from_attributes": True}
