from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, delete as sa_delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.session import get_db
from app.api.deps import verify_admin_or_session
from app.models import Tariff, RateComponent, CustomerClass, RateType, Utility
from app.schemas.tariff import (
    TariffListRead,
    TariffDetailRead,
    TariffSourceRead,
    TariffBrowseRead,
    TariffBrowseResponse,
)

router = APIRouter()


@router.get("/tariffs/filters")
async def tariff_filter_options(db: AsyncSession = Depends(get_db)):
    """Return distinct values for tariff browser filter dropdowns."""
    rows = (await db.execute(
        select(
            Utility.country,
            Utility.state_province,
            Utility.id,
            Utility.name,
        )
        .join(Tariff, Tariff.utility_id == Utility.id)
        .where(Utility.is_active.is_(True))
        .distinct()
        .order_by(Utility.country, Utility.state_province, Utility.name)
    )).all()

    countries: dict[str, dict] = {}
    for country, state, uid, uname in rows:
        if country not in countries:
            countries[country] = {"states": {}}
        st_map = countries[country]["states"]
        if state not in st_map:
            st_map[state] = []
        st_map[state].append({"id": uid, "name": uname})

    return countries


@router.get("/tariffs/browse", response_model=TariffBrowseResponse)
async def browse_tariffs(
    country: str | None = None,
    state_province: str | None = None,
    utility_search: str | None = None,
    utility_id: int | None = None,
    customer_class: CustomerClass | None = None,
    rate_type: RateType | None = None,
    limit: int = Query(default=50, le=500),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    component_count_sq = (
        select(
            RateComponent.tariff_id,
            func.count(RateComponent.id).label("cnt"),
        )
        .group_by(RateComponent.tariff_id)
        .subquery()
    )

    base = (
        select(
            Tariff.id,
            Tariff.utility_id,
            Utility.name.label("utility_name"),
            Utility.country,
            Utility.state_province,
            Tariff.name.label("name"),
            Tariff.code,
            Tariff.customer_class,
            Tariff.rate_type,
            Tariff.is_default,
            Tariff.effective_date,
            Tariff.last_verified_at,
            func.coalesce(component_count_sq.c.cnt, 0).label("component_count"),
        )
        .join(Utility, Tariff.utility_id == Utility.id)
        .outerjoin(component_count_sq, Tariff.id == component_count_sq.c.tariff_id)
        .where(Utility.is_active.is_(True))
    )

    if country:
        base = base.where(Utility.country == country)
    if state_province:
        base = base.where(Utility.state_province == state_province)
    if utility_id:
        base = base.where(Tariff.utility_id == utility_id)
    elif utility_search:
        base = base.where(Utility.name.ilike(f"%{utility_search}%"))
    if customer_class:
        base = base.where(Tariff.customer_class == customer_class)
    if rate_type:
        base = base.where(Tariff.rate_type == rate_type)

    count_stmt = select(func.count()).select_from(base.subquery())
    total = (await db.execute(count_stmt)).scalar() or 0

    rows_stmt = base.order_by(
        Utility.country, Utility.state_province, Utility.name, Tariff.customer_class, Tariff.name
    ).offset(offset).limit(limit)

    rows = (await db.execute(rows_stmt)).mappings().all()

    items = [TariffBrowseRead(**dict(r)) for r in rows]
    return TariffBrowseResponse(items=items, total=total, limit=limit, offset=offset)


@router.get("/utilities/{utility_id}/tariffs", response_model=list[TariffListRead])
async def list_tariffs_for_utility(
    utility_id: int,
    customer_class: CustomerClass | None = None,
    rate_type: RateType | None = None,
    is_default: bool | None = None,
    limit: int = Query(default=100, le=500),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Tariff).where(Tariff.utility_id == utility_id)

    if customer_class:
        stmt = stmt.where(Tariff.customer_class == customer_class)
    if rate_type:
        stmt = stmt.where(Tariff.rate_type == rate_type)
    if is_default is not None:
        stmt = stmt.where(Tariff.is_default == is_default)

    stmt = stmt.order_by(Tariff.customer_class, Tariff.name).offset(offset).limit(limit)
    result = await db.execute(stmt)
    return result.scalars().all()


@router.get("/tariffs/{tariff_id}", response_model=TariffDetailRead)
async def get_tariff(tariff_id: int, db: AsyncSession = Depends(get_db)):
    stmt = (
        select(Tariff)
        .options(selectinload(Tariff.rate_components))
        .where(Tariff.id == tariff_id)
    )
    result = await db.execute(stmt)
    tariff = result.scalar_one_or_none()

    if not tariff:
        raise HTTPException(status_code=404, detail="Tariff not found")

    return tariff


@router.get("/tariffs/{tariff_id}/source", response_model=TariffSourceRead)
async def get_tariff_source(tariff_id: int, db: AsyncSession = Depends(get_db)):
    stmt = select(Tariff).where(Tariff.id == tariff_id)
    result = await db.execute(stmt)
    tariff = result.scalar_one_or_none()

    if not tariff:
        raise HTTPException(status_code=404, detail="Tariff not found")

    return TariffSourceRead(
        tariff_id=tariff.id,
        source_url=tariff.source_url,
        source_document_hash=tariff.source_document_hash,
        last_verified_at=tariff.last_verified_at,
        approved=tariff.approved,
    )


@router.delete(
    "/tariffs/{tariff_id}",
    dependencies=[Depends(verify_admin_or_session)],
)
async def delete_tariff(tariff_id: int, db: AsyncSession = Depends(get_db)):
    stmt = select(Tariff).where(Tariff.id == tariff_id)
    result = await db.execute(stmt)
    tariff = result.scalar_one_or_none()

    if not tariff:
        raise HTTPException(status_code=404, detail="Tariff not found")

    await db.delete(tariff)
    await db.commit()

    return {"ok": True, "deleted_tariff_id": tariff_id}
