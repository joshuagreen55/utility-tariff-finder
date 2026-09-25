"""Payloads for POST /api/tariff-corrections (already-approved corrections)."""
from __future__ import annotations

import math
import re
from datetime import date, datetime, time
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from app.models.tariff import ComponentType, CustomerClass, RateType

_CENTS_RE = re.compile(r"¢|\bcents?\b|(?<![a-z])c/", re.IGNORECASE)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class CorrectionComponent(BaseModel):
    component_type: ComponentType
    unit: str = Field(min_length=1, max_length=50)
    # Decimal string preferred ("0.203000") to avoid float drift into Numeric(16,6).
    rate_value: Decimal
    tier_min_kwh: float | None = None
    tier_max_kwh: float | None = None
    tier_label: str | None = Field(default=None, max_length=100)
    period_label: str | None = Field(default=None, max_length=100)
    period_start_time: time | None = None
    period_end_time: time | None = None
    day_type: Literal["weekday", "weekend", "holiday", "all"] | None = None
    season: str | None = Field(default=None, max_length=50)
    season_start_month: int | None = Field(default=None, ge=1, le=12)
    season_start_day: int | None = Field(default=None, ge=1, le=31)
    season_end_month: int | None = Field(default=None, ge=1, le=12)
    season_end_day: int | None = Field(default=None, ge=1, le=31)
    included_in_energy: bool = False

    @field_validator("period_start_time", "period_end_time", mode="before")
    @classmethod
    def _end_of_day(cls, v):
        return "00:00" if isinstance(v, str) and v.strip() in ("24:00", "24:00:00") else v

    @field_validator("unit")
    @classmethod
    def _dollar_units(cls, v: str) -> str:
        if _CENTS_RE.search(v):
            raise ValueError("send dollar units (e.g. $/kWh, $/day), not cents")
        return v.strip()

    @model_validator(mode="after")
    def _sane_value(self):
        if not math.isfinite(float(self.rate_value)):
            raise ValueError("rate_value must be finite")
        if self.component_type == ComponentType.ENERGY and self.rate_value < 0:
            raise ValueError("energy rate_value must not be negative")
        return self


class CorrectionTariff(BaseModel):
    name: str = Field(min_length=1, max_length=500)
    code: str | None = Field(default=None, max_length=100)
    customer_class: CustomerClass
    rate_type: RateType
    effective_date: date | None = None
    description: str | None = None
    is_default: bool = False


class CorrectionTarget(BaseModel):
    utility_id: int
    # replace: supersede expected_live_tariff_id with a new row
    # create:  add a new live product (no predecessor)
    # retire:  soft-retire expected_live_tariff_id (no successor)
    mode: Literal["replace", "create", "retire"]
    expected_live_tariff_id: int | None = None


class CorrectionEvidence(BaseModel):
    source_url: str = Field(pattern=r"^https?://", max_length=2000)
    source_document_sha256: str | None = None
    retrieved_at: datetime | None = None
    page_ref: str | None = Field(default=None, max_length=200)
    quote: str | None = Field(default=None, max_length=4000)

    @field_validator("source_document_sha256")
    @classmethod
    def _sha(cls, v):
        if v is not None and not _SHA256_RE.match(v.lower()):
            raise ValueError("source_document_sha256 must be 64 hex chars")
        return v.lower() if v else v


class CorrectionPin(BaseModel):
    scope: Literal["document"] = "document"
    cause: Literal["extraction_error", "source_error"] | None = None


class TariffCorrectionRequest(BaseModel):
    """An already-approved manual correction. Approval / second check happens
    in the Mysa admin portal; UTF records who approved it but does not
    adjudicate it."""

    idempotency_key: str = Field(min_length=8, max_length=200)
    ticket_id: str = Field(min_length=1, max_length=100)
    approved_by: str = Field(min_length=3, max_length=200)
    approved_at: datetime
    requested_by: str | None = Field(default=None, max_length=200)
    target: CorrectionTarget
    tariff: CorrectionTariff | None = None
    components: list[CorrectionComponent] = []
    evidence: CorrectionEvidence
    pin: CorrectionPin | None = None

    @model_validator(mode="after")
    def _mode_rules(self):
        mode = self.target.mode
        if mode in ("replace", "retire") and self.target.expected_live_tariff_id is None:
            raise ValueError(f"mode={mode} requires target.expected_live_tariff_id")
        if mode == "create" and self.target.expected_live_tariff_id is not None:
            raise ValueError("mode=create must not set target.expected_live_tariff_id")
        if mode in ("replace", "create"):
            if self.tariff is None or not self.components:
                raise ValueError(f"mode={mode} requires tariff and at least one component")
        elif self.tariff is not None or self.components:
            raise ValueError("mode=retire takes no tariff or components")
        return self


class TariffCorrectionResponse(BaseModel):
    new_tariff_id: int | None = None
    superseded_tariff_id: int | None = None
    change_event_id: int
    pin_id: int | None = None
    computable: bool | None = None
    computable_reasons: list[str] = []
    computable_warnings: list[str] = []
    warnings: list[str] = []
    replayed: bool = False


class TariffCorrectionConflict(BaseModel):
    reason: Literal["not_live", "wrong_utility", "live_tariff_exists", "idempotency_key_reused", "refresh_in_progress"]
    current_live_tariff_id: int | None = None
    last_change_event_id: int | None = None
    detail: str
