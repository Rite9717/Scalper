"""Daily Kite session.

Personal apps get an access token and an empty refresh token. The browser
login in kite.callback obtains the request_token. This module exchanges it.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from urllib.parse import urlencode

import httpx

from scalper.clock import access_token_expires_at
from scalper.compliance import PersonalAccountError
from scalper.kite.api_facts import KITE_LOGIN_URL
from scalper.kite.http import authorization_header, parse_object_data
from scalper.kite.session import KiteSession


def session_checksum(api_key: str, request_token: str, api_secret: str) -> str:
    """SHA-256 hex of api_key + request_token + api_secret, with no separator."""
    payload = f"{api_key}{request_token}{api_secret}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def login_url(api_key: str, redirect_params: str | None = None) -> str:
    query = {"v": "3", "api_key": api_key}
    if redirect_params:
        query["redirect_params"] = redirect_params
    return f"{KITE_LOGIN_URL}?{urlencode(query)}"


def _parse_login_time(value: object) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Kite response did not include login_time")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    if parsed.tzinfo is None:
        from scalper.clock import IST

        parsed = parsed.replace(tzinfo=IST)
    return parsed


async def exchange_request_token(
    *,
    api_key: str,
    api_secret: str,
    request_token: str,
    expected_user_id: str,
    client: httpx.AsyncClient,
) -> KiteSession:
    """Exchange a one-time request_token for an access token.

    The API secret is used only to build the checksum. It is not sent.
    """
    if not api_key or not api_secret or not request_token:
        raise PersonalAccountError("api key, api secret, and request token are required")
    if not expected_user_id:
        raise PersonalAccountError("expected user id is required before login")

    checksum = session_checksum(api_key, request_token, api_secret)
    response = await client.post(
        "/session/token",
        data={
            "api_key": api_key,
            "request_token": request_token,
            "checksum": checksum,
        },
    )
    data = parse_object_data(response)
    refresh = str(data.get("refresh_token") or "")
    if refresh:
        raise PersonalAccountError(
            "this Kite app returned a refresh token; personal mode refuses platform credentials"
        )
    user_type = str(data.get("user_type") or "")
    if user_type != "individual":
        raise PersonalAccountError("Kite user_type is not individual")
    user_id = str(data.get("user_id") or "")
    if user_id != expected_user_id:
        raise PersonalAccountError("Kite user id does not match the configured account")
    exchanges = data.get("exchanges") or []
    if "NFO" not in exchanges:
        raise PersonalAccountError("NFO is not enabled on this account")
    access_token = str(data.get("access_token") or "")
    if not access_token:
        raise PersonalAccountError("Kite response did not include an access token")
    try:
        login_time = _parse_login_time(data.get("login_time"))
    except ValueError as exc:
        raise PersonalAccountError(str(exc)) from exc
    return KiteSession(
        user_id=user_id,
        user_type=user_type,
        access_token=access_token,
        login_time=login_time,
        expires_at=access_token_expires_at(login_time),
        api_key=api_key,
    )


def authenticated_headers(api_key: str, access_token: str) -> dict[str, str]:
    return {"Authorization": authorization_header(api_key, access_token)}
