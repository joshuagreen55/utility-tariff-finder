"""Shared FastAPI dependencies."""

from fastapi import Header, HTTPException
from starlette.requests import Request

from app.auth.session_tokens import decode_session_token
from app.config import settings


def _token_from_headers(x_admin_key: str | None, authorization: str | None) -> str:
    token = (x_admin_key or "").strip()
    if authorization and authorization.lower().startswith("bearer "):
        token = authorization[7:].strip()
    return token


def request_has_valid_admin_key(request: Request) -> bool:
    expected = (settings.admin_api_key or "").strip()
    if not expected:
        return False
    x = request.headers.get("x-admin-key")
    auth = request.headers.get("authorization")
    token = _token_from_headers(x, auth)
    return bool(token) and token == expected


def verify_admin_key(
    x_admin_key: str | None = Header(None, alias="X-Admin-Key"),
    authorization: str | None = Header(None),
) -> None:
    """
    If ADMIN_API_KEY is set in the environment, require it via:
    - Header X-Admin-Key: <key>, or
    - Header Authorization: Bearer <key>

    If ADMIN_API_KEY is empty, admin routes stay open (local dev only).
    """
    expected = (settings.admin_api_key or "").strip()
    if not expected:
        return

    token = _token_from_headers(x_admin_key, authorization)

    if not token or token != expected:
        raise HTTPException(status_code=401, detail="Admin authentication required")


def verify_admin_or_session(request: Request) -> None:
    """
    When AUTH_ENABLED: allow Google session cookie OR admin API key.
    When AUTH_DISABLED: same rules as verify_admin_key (key only if configured).
    """
    if settings.auth_enabled:
        if request_has_valid_admin_key(request):
            return
        raw = request.cookies.get(settings.auth_cookie_name)
        if raw and decode_session_token(raw):
            return
        raise HTTPException(status_code=401, detail="Admin authentication required")

    expected = (settings.admin_api_key or "").strip()
    if not expected:
        return
    if request_has_valid_admin_key(request):
        return
    raise HTTPException(status_code=401, detail="Admin authentication required")
