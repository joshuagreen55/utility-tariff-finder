from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.models import Utility, Tariff, Country
from app.schemas.utility import UtilityRead, UtilityListRead

router = APIRouter()


@router.get("/utilities", response_model=list[UtilityListRead])
async def list_utilities(
    country: Country | None = None,
    state_province: str | None = None,
    search: str | None = None,
    limit: int = Query(default=50, le=500),
    offset: int = Query(default=0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    tariff_count_sub = (
        select(Tariff.utility_id, func.count(Tariff.id).label("tariff_count"))
        .group_by(Tariff.utility_id)
        .subquery()
    )

    stmt = (
        select(
            Utility.id,
            Utility.name,
            Utility.country,
            Utility.state_province,
            Utility.utility_type,
            func.coalesce(tariff_count_sub.c.tariff_count, 0).label("tariff_count"),
        )
        .outerjoin(tariff_count_sub, Utility.id == tariff_count_sub.c.utility_id)
        .where(Utility.is_active.is_(True))
    )

    if country:
        stmt = stmt.where(Utility.country == country)
    if state_province:
        stmt = stmt.where(Utility.state_province.ilike(f"%{state_province}%"))
    if search:
        stmt = stmt.where(Utility.name.ilike(f"%{search}%"))

    stmt = stmt.order_by(Utility.name).offset(offset).limit(limit)
    result = await db.execute(stmt)
    rows = result.all()

    return [
        UtilityListRead(
            id=r.id,
            name=r.name,
            country=r.country,
            state_province=r.state_province,
            utility_type=r.utility_type,
            tariff_count=r.tariff_count,
        )
        for r in rows
    ]


@router.get("/utilities/{utility_id}", response_model=UtilityRead)
async def get_utility(utility_id: int, db: AsyncSession = Depends(get_db)):
    tariff_count_sub = (
        select(func.count(Tariff.id))
        .where(Tariff.utility_id == utility_id)
        .scalar_subquery()
    )

    stmt = select(Utility, tariff_count_sub.label("tariff_count")).where(Utility.id == utility_id)
    result = await db.execute(stmt)
    row = result.first()

    if not row:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Utility not found")

    utility = row[0]
    return UtilityRead(
        id=utility.id,
        name=utility.name,
        eia_id=utility.eia_id,
        country=utility.country,
        state_province=utility.state_province,
        utility_type=utility.utility_type,
        website_url=utility.website_url,
        is_active=utility.is_active,
        tariff_count=row.tariff_count or 0,
        created_at=utility.created_at,
        updated_at=utility.updated_at,
    )
