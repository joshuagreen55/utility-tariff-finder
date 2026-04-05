"""Google OAuth (Workspace / domain-restricted) + session cookie."""

from __future__ import annotations

from urllib.parse import urlencode

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from app.auth.session_tokens import (
    create_oauth_state_token,
    create_session_token,
    decode_session_token,
    verify_oauth_state_token,
)
from app.config import settings
from app.services.google_oauth import email_allowed, exchange_authorization_code, verify_google_id_token

router = APIRouter()

GOOGLE_AUTH_ENDPOINT = "https://accounts.google.com/o/oauth2/v2/auth"


def _cookie_params(max_age: int | None = None) -> dict:
    return {
        "httponly": True,
        "samesite": "lax",
        "secure": settings.auth_cookie_secure,
        "path": "/",
        **({"max_age": max_age} if max_age is not None else {}),
    }


def _public_redirect_base() -> str:
    base = (settings.public_app_url or "").strip().rstrip("/")
    if base:
        return base
    uri = (settings.auth_google_redirect_uri or "").strip()
    if "/api/auth/google/callback" in uri:
        return uri.split("/api/auth/google/callback")[0].rstrip("/") or "/"
    if settings.cors_origins_list:
        return settings.cors_origins_list[0].rstrip("/")
    return ""


@router.get("/auth/me")
async def auth_me(request: Request):
    if not settings.auth_enabled:
        return {"auth_enabled": False, "authenticated": False}

    raw = request.cookies.get(settings.auth_cookie_name)
    payload = decode_session_token(raw) if raw else None
    if not payload:
        return {"auth_enabled": True, "authenticated": False}

    return {
        "auth_enabled": True,
        "authenticated": True,
        "email": payload.get("email"),
        "name": payload.get("name"),
    }


@router.get("/auth/google/login")
async def google_login_start():
    if not settings.auth_enabled:
        raise HTTPException(status_code=404, detail="Auth not enabled")

    if not (
        settings.google_oauth_client_id
        and settings.google_oauth_client_secret
        and settings.auth_google_redirect_uri
    ):
        raise HTTPException(status_code=500, detail="Google OAuth is not configured")

    state = create_oauth_state_token()
    qs = urlencode(
        {
            "client_id": settings.google_oauth_client_id,
            "redirect_uri": settings.auth_google_redirect_uri,
            "response_type": "code",
            "scope": "openid email profile",
            "state": state,
            "prompt": "select_account",
            "hd": settings.auth_allowed_email_domain,
        }
    )
    r = RedirectResponse(f"{GOOGLE_AUTH_ENDPOINT}?{qs}", status_code=302)
    r.set_cookie(settings.oauth_state_cookie_name, state, **_cookie_params(max_age=600))
    return r


@router.get("/auth/google/callback")
async def google_login_callback(request: Request, code: str | None = None, state: str | None = None):
    if not settings.auth_enabled:
        raise HTTPException(status_code=404, detail="Auth not enabled")

    if not code or not state:
        return HTMLResponse(
            "<html><body><p>Missing code or state. Close this tab and try signing in again.</p></body></html>",
            status_code=400,
        )

    cookie_state = request.cookies.get(settings.oauth_state_cookie_name)
    if not cookie_state or cookie_state != state or not verify_oauth_state_token(state):
        return HTMLResponse(
            "<html><body><p>Invalid session state. Try signing in again.</p></body></html>",
            status_code=400,
        )

    try:
        token_payload = await exchange_authorization_code(code)
    except Exception as exc:  # noqa: BLE001
        return HTMLResponse(
            f"<html><body><p>Could not complete sign-in ({exc}). Try again.</p></body></html>",
            status_code=502,
        )

    id_tok = token_payload.get("id_token")
    if not id_tok:
        return HTMLResponse(
            "<html><body><p>No ID token from Google. Try again.</p></body></html>",
            status_code=502,
        )

    try:
        claims = verify_google_id_token(id_tok)
    except ValueError as exc:
        return HTMLResponse(
            f"<html><body><p>Invalid token from Google: {exc}</p></body></html>",
            status_code=502,
        )

    if not email_allowed(claims):
        return HTMLResponse(
            "<html><body><p>Access is limited to verified @"
            + settings.auth_allowed_email_domain
            + " accounts.</p></body></html>",
            status_code=403,
        )

    email = str(claims.get("email") or "")
    sub = str(claims.get("sub") or "")
    name = claims.get("name")
    session_jwt = create_session_token(email=email, sub=sub, name=name if isinstance(name, str) else None)

    dest = _public_redirect_base() or str(request.base_url).rstrip("/")
    dest = dest.rstrip("/") + "/"

    out = RedirectResponse(dest, status_code=302)
    out.delete_cookie(settings.oauth_state_cookie_name, path="/")
    out.set_cookie(
        settings.auth_cookie_name,
        session_jwt,
        **_cookie_params(max_age=settings.auth_session_max_days * 86400),
    )
    return out


@router.post("/auth/logout")
async def logout():
    if not settings.auth_enabled:
        return {"ok": True}
    response = JSONResponse({"ok": True})
    response.delete_cookie(settings.auth_cookie_name, path="/")
    return response
