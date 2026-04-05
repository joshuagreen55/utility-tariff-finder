"""Require a valid session cookie for most API routes when AUTH_ENABLED is on."""

from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

from app.api.deps import request_has_valid_admin_key
from app.auth.session_tokens import decode_session_token
from app.config import settings


def _session_payload(request: Request) -> dict | None:
    raw = request.cookies.get(settings.auth_cookie_name)
    if not raw:
        return None
    return decode_session_token(raw)


class SessionAuthGateMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if not settings.auth_enabled:
            return await call_next(request)

        if request.method == "OPTIONS":
            return await call_next(request)

        path = request.url.path

        if path == "/api/health" or path.startswith("/api/auth/"):
            return await call_next(request)

        if path.startswith("/api/"):
            if path.startswith("/api/admin/"):
                if request_has_valid_admin_key(request):
                    return await call_next(request)
                if _session_payload(request):
                    return await call_next(request)
                return JSONResponse(
                    {"detail": "Not authenticated"},
                    status_code=401,
                    headers={"WWW-Authenticate": "Bearer"},
                )

            if _session_payload(request):
                return await call_next(request)

            return JSONResponse(
                {"detail": "Not authenticated"},
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )

        return await call_next(request)
