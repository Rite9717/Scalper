"""HTTP helpers for the few Kite calls Phase 1 makes."""

from __future__ import annotations

import json
from typing import Any

import httpx

from scalper import __version__
from scalper.kite.api_facts import KITE_API_ROOT, KITE_VERSION


class KiteError(Exception):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        error_type: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_type = error_type


def kite_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=KITE_API_ROOT,
        timeout=httpx.Timeout(60.0, connect=10.0),
        follow_redirects=False,
        headers={
            "X-Kite-Version": KITE_VERSION,
            "User-Agent": f"scalper/{__version__} personal",
        },
    )


def authorization_header(api_key: str, access_token: str) -> str:
    return f"token {api_key}:{access_token}"


def parse_object_data(response: httpx.Response) -> dict[str, Any]:
    try:
        body = response.json()
    except json.JSONDecodeError as exc:
        raise KiteError(
            f"Kite returned a non-JSON body (HTTP {response.status_code})",
            status_code=response.status_code,
        ) from exc
    if not isinstance(body, dict):
        raise KiteError("Kite JSON body was not an object", status_code=response.status_code)
    if response.status_code >= 400 or body.get("status") != "success":
        error_type = body.get("error_type")
        raise KiteError(
            str(body.get("message") or f"HTTP {response.status_code}"),
            status_code=response.status_code,
            error_type=str(error_type) if error_type else None,
        )
    data = body.get("data")
    if not isinstance(data, dict):
        raise KiteError("Kite success payload did not contain an object", status_code=response.status_code)
    return data
