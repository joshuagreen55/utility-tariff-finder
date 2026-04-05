from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.config import settings

_async_engine = None
_sync_engine = None


def get_async_engine():
    global _async_engine
    if _async_engine is None:
        _async_engine = create_async_engine(settings.database_url, echo=False)
    return _async_engine


def get_sync_engine():
    global _sync_engine
    if _sync_engine is None:
        _sync_engine = create_engine(settings.sync_database_url, echo=False)
    return _sync_engine


async def get_db() -> AsyncSession:
    engine = get_async_engine()
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as session:
        yield session
