from datetime import date, datetime, time
from typing import Any

from pydantic import BaseModel, ValidationInfo, model_validator

from app.models.tariff import ComponentType, CustomerClass, RateType
from app.services.computable import tariff_contract

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
    # Structured TOU clock window — Flux should prefer these over parsing
    # period_label. Times are wall-clock local to the utility; overnight
    # wraps have end < start. end==00:00 with start!=00:00 means through
    # end of calendar day.
    period_start_time: time | None = None
    period_end_time: time | None = None
    day_type: str | None = None  # weekday | weekend | holiday | all
    season: str | None = None
    # Inclusive season calendar (month 1–12, day 1–31). Nov→Mar wrap OK.
    season_start_month: int | None = None
    season_start_day: int | None = None
    season_end_month: int | None = None
    season_end_day: int | None = None
    adjustment: float | None = None
    # ADJUSTMENT already folded into the all-in ENERGY rates: do not add it.
    included_in_energy: bool = False

    model_config = {"from_attributes": True}


class _ComputableContract(BaseModel):
    """Machine contract for cost / TOU consumers (docs/MYSA_CONSUMER_CONTRACT.md).

    ``computable`` is False whenever the structured rows are not enough to
    price every interval of the year; ``computable_reasons`` says why
    (``code`` or ``code:detail``). ``computable_warnings`` are assumptions
    the consumer must honour even when computable.
    """

    computable: bool = False
    computable_reasons: list[str] = []
    computable_warnings: list[str] = []
    needs_review: bool = False

    @model_validator(mode="before")
    @classmethod
    def _attach_contract(cls, data: Any, info: ValidationInfo) -> Any:
        if isinstance(data, dict) or not hasattr(data, "rate_components"):
            return data
        out = {name: getattr(data, name) for name in cls.model_fields if hasattr(data, name)}
        ctx = info.context or {}
        out.update(tariff_contract(data, holiday_calendar=ctx.get("holiday_calendar")))
        return out


class TariffListRead(_ComputableContract):
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
    # official | third_party | unknown (app/services/source_type.py)
    source_type: str = "unknown"

    model_config = {"from_attributes": True}

    @model_validator(mode="after")
    def set_freshness(self) -> "TariffListRead":
        self.data_freshness = _compute_freshness(self.effective_date, self.last_verified_at)
        return self


class TariffDetailRead(TariffListRead):
    description: str | None = None
    source_url: str | None = None
    source_type_reason: str | None = None
    # TOU clock windows are local wall-clock time at the service address.
    # ``timezone`` is the utility fallback (None in multi-zone jurisdictions).
    clock_basis: str = "local_wall_clock"
    timezone: str | None = None
    timezone_source: str | None = None
    currency: str | None = None
    holiday_calendar: str | None = None
    rate_components: list[RateComponentRead] = []
    energy_schedule_weekday: Any | None = None
    energy_schedule_weekend: Any | None = None
    demand_schedule_weekday: Any | None = None
    demand_schedule_weekend: Any | None = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class TariffBrowseRead(_ComputableContract):
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
    source_type: str = "unknown"
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
    source_type: str = "unknown"
    source_type_reason: str | None = None
    source_document_hash: str | None = None
    last_verified_at: datetime | None = None
    approved: bool

    model_config = {"from_attributes": True}
