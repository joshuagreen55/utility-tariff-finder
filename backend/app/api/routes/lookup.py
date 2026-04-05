from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db
from app.schemas.lookup import AddressLookupResponse
from app.services.territory_lookup import lookup_utilities_by_address

router = APIRouter()


@router.get("/lookup", response_model=AddressLookupResponse)
async def lookup_address(address: str, db: AsyncSession = Depends(get_db)):
    return await lookup_utilities_by_address(address, db)
