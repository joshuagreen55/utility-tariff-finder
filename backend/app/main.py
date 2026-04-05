from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.deps import verify_admin_or_session
from app.api.routes import auth, lookup, monitoring, tariffs, utilities
from app.config import settings
from app.middleware.auth_gate import SessionAuthGateMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    if settings.auth_enabled:
        if not (settings.auth_session_secret or "").strip():
            raise RuntimeError("AUTH_SESSION_SECRET is required when AUTH_ENABLED=true")
        if not settings.google_oauth_client_id or not settings.google_oauth_client_secret:
            raise RuntimeError(
                "GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET are required when AUTH_ENABLED=true"
            )
        if not (settings.auth_google_redirect_uri or "").strip():
            raise RuntimeError("AUTH_GOOGLE_REDIRECT_URI is required when AUTH_ENABLED=true")
    yield


app = FastAPI(
    title="Utility Tariff Finder",
    description="API for looking up electricity utility providers and rate tariffs by address (US & Canada)",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(SessionAuthGateMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router, prefix="/api", tags=["Auth"])
app.include_router(lookup.router, prefix="/api", tags=["Address Lookup"])
app.include_router(utilities.router, prefix="/api", tags=["Utilities"])
app.include_router(tariffs.router, prefix="/api", tags=["Tariffs"])
app.include_router(
    monitoring.router,
    prefix="/api/admin",
    tags=["Monitoring"],
    dependencies=[Depends(verify_admin_or_session)],
)


@app.get("/api/health")
async def health_check():
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    from app.db.session import get_async_engine
    try:
        engine = get_async_engine()
        factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with factory() as session:
            await session.execute(text("SELECT 1"))
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "db": str(e)},
        )
