from datetime import datetime

from pydantic import BaseModel

from app.models.monitoring import MonitoringStatus, ReviewStatus


class MonitoringSourceRead(BaseModel):
    id: int
    utility_id: int
    utility_name: str = ""
    url: str
    check_frequency_days: int
    last_checked_at: datetime | None = None
    last_changed_at: datetime | None = None
    status: MonitoringStatus

    model_config = {"from_attributes": True}


class MonitoringLogRead(BaseModel):
    id: int
    source_id: int
    checked_at: datetime
    content_hash: str
    changed: bool
    diff_summary: str | None = None
    review_status: ReviewStatus

    model_config = {"from_attributes": True}


class MonitoringLogUpdate(BaseModel):
    review_status: ReviewStatus


class MonitoringSourceUrlUpdate(BaseModel):
    url: str


class MonitoringCheckIdsRequest(BaseModel):
    source_ids: list[int]


# --- Analytics response models ---


class ErrorCategoryDetail(BaseModel):
    category: str
    description: str
    source_count: int
    utility_count: int
    top_domains: list[str] = []
    by_country: dict[str, int] = {}


class ErrorCategoryStateBreakdown(BaseModel):
    country: str
    state: str
    error_sources: int
    affected_utilities: int


class ErrorCategoriesResponse(BaseModel):
    total_error_sources: int
    total_affected_utilities: int
    categories: list[ErrorCategoryDetail]
    by_state: list[ErrorCategoryStateBreakdown]


class CountryCoverageSummary(BaseModel):
    total_utilities: int
    with_working_url: int
    with_tariff_data: int
    url_coverage_pct: float
    tariff_coverage_pct: float


class StateCoverage(BaseModel):
    country: str
    state: str
    total_utilities: int
    with_working_url: int
    with_tariff_data: int
    url_coverage_pct: float
    tariff_coverage_pct: float


class CoverageResponse(BaseModel):
    summary: dict[str, CountryCoverageSummary]
    by_state: list[StateCoverage]
