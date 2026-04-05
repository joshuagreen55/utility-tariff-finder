"""Google OAuth: code exchange and ID token verification."""

from __future__ import annotations

import httpx
from google.oauth2 import id_token
from google.auth.transport import requests as google_requests

from app.config import settings


GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"


async def exchange_authorization_code(code: str) -> dict:
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            GOOGLE_TOKEN_URL,
            data={
                "code": code,
                "client_id": settings.google_oauth_client_id,
                "client_secret": settings.google_oauth_client_secret,
                "redirect_uri": settings.auth_google_redirect_uri,
                "grant_type": "authorization_code",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30.0,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"Google token exchange failed ({resp.status_code}): {resp.text}")
        return resp.json()


def verify_google_id_token(id_token_str: str) -> dict:
    """Validate signature, audience, expiry; returns token claims."""
    return id_token.verify_oauth2_token(
        id_token_str,
        google_requests.Request(),
        settings.google_oauth_client_id,
    )


def email_allowed(claims: dict) -> bool:
    domain = (settings.auth_allowed_email_domain or "").strip().lower()
    email = (claims.get("email") or "").strip().lower()
    if not email or not email.endswith(f"@{domain}"):
        return False
    # Google may omit email_verified for some account types — require when present
    ev = claims.get("email_verified")
    if ev is False:
        return False
    return True
