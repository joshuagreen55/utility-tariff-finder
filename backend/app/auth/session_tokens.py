"""Signed session and OAuth-state JWTs (HS256)."""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from typing import Any

import jwt

from app.config import settings


def _secret() -> str:
    s = (settings.auth_session_secret or "").strip()
    if not s:
        raise RuntimeError("AUTH_SESSION_SECRET is required when auth is enabled")
    return s


def create_oauth_state_token() -> str:
    payload = {
        "typ": "oauth_state",
        "nonce": secrets.token_urlsafe(32),
        "exp": datetime.now(timezone.utc) + timedelta(minutes=10),
    }
    return jwt.encode(payload, _secret(), algorithm="HS256")


def verify_oauth_state_token(token: str) -> bool:
    try:
        decoded = jwt.decode(token, _secret(), algorithms=["HS256"])
        return decoded.get("typ") == "oauth_state"
    except jwt.PyJWTError:
        return False


def create_session_token(*, email: str, sub: str, name: str | None = None) -> str:
    payload: dict[str, Any] = {
        "typ": "session",
        "email": email,
        "sub": sub,
        "exp": datetime.now(timezone.utc) + timedelta(days=settings.auth_session_max_days),
    }
    if name:
        payload["name"] = name
    return jwt.encode(payload, _secret(), algorithm="HS256")


def decode_session_token(token: str) -> dict[str, Any] | None:
    try:
        decoded = jwt.decode(token, _secret(), algorithms=["HS256"])
        if decoded.get("typ") != "session":
            return None
        return decoded
    except jwt.PyJWTError:
        return None
