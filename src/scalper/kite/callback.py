"""Loopback callback that receives a manual Kite login.

The browser signs in on Kite. Kite redirects to this process with a one-time
request_token. The process exchanges it, stores the session, and exits.
The request token is not written to the log, the HTML, or the session file.
"""

from __future__ import annotations

import asyncio
import contextlib
import html
import json
import logging
import re
import secrets
import sys
import webbrowser
from dataclasses import dataclass
from datetime import timedelta

import httpx
import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from starlette.background import BackgroundTask

from scalper.clock import now_ist
from scalper.compliance import ComplianceError, PersonalAccountError
from scalper.config import Settings
from scalper.kite.auth import exchange_request_token, login_url
from scalper.kite.http import KiteError
from scalper.kite.session import save_session

CALLBACK_PATH = "/callback"
_TOKEN = re.compile(r"^[A-Za-z0-9]{8,128}$")
_REQUEST_TOKEN_IN_TEXT = re.compile(r"(request_token=)[^&\s\"']+", re.IGNORECASE)
_REDACT_LOGGERS = ("", "uvicorn", "uvicorn.access", "uvicorn.error", "fastapi")


class _RedactRequestToken(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            rendered = record.getMessage()
        except (TypeError, ValueError):
            rendered = str(record.msg)
        record.msg = _REQUEST_TOKEN_IN_TEXT.sub(r"\1***", rendered)
        record.args = ()
        return True


def classify_callback(*, status: str, request_token: str, state: str, expected_state: str) -> str:
    """Decide what a browser hit on /callback means. Does not echo the token."""
    if not status and not request_token and not state:
        return "waiting"
    if not _same_secret(state, expected_state):
        return "bad_state"
    if status != "success":
        return "kite_status"
    if not _TOKEN.fullmatch(request_token):
        return "bad_token"
    return "exchange"


def callback_url(host: str, port: int) -> str:
    return f"http://{host}:{port}{CALLBACK_PATH}"


def require_login_credentials(settings: Settings) -> None:
    missing = []
    if not settings.expected_user_id:
        missing.append("SCALPER_EXPECTED_USER_ID")
    if not settings.kite_api_key.get_secret_value():
        missing.append("SCALPER_KITE_API_KEY")
    if not settings.kite_api_secret.get_secret_value():
        missing.append("SCALPER_KITE_API_SECRET")
    if missing:
        raise ComplianceError("login requires " + ", ".join(missing))


@dataclass
class CallbackLogin:
    settings: Settings
    state: str
    client: httpx.AsyncClient
    server: uvicorn.Server | None = None
    succeeded: bool = False
    timed_out: bool = False
    user_id: str = ""
    expires_at: str = ""


def create_app(login: CallbackLogin) -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    log = structlog.get_logger()

    @app.exception_handler(Exception)
    async def unhandled(_request: Request, exc: Exception) -> HTMLResponse:
        if isinstance(exc, HTTPException):
            title = "Not found" if exc.status_code == 404 else "Login failed"
            return _html(exc.status_code, title, "That page is not part of the login callback.")
        log.error("login_callback_failed", error_type=type(exc).__name__)
        return _html(
            500,
            "Login failed",
            "The callback failed before the session was stored. Start scalper login again.",
        )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": "scalper-login"}

    @app.get("/done")
    async def done() -> HTMLResponse:
        if not login.succeeded:
            return _html(200, "Waiting for login", "The session is not stored yet.")
        return _html(
            200,
            "Login complete",
            f"Session stored for {login.user_id}. It expires at {login.expires_at}. You can close this tab.",
        )

    @app.get(CALLBACK_PATH)
    async def callback(request: Request) -> Response:
        status = request.query_params.get("status", "")
        request_token = request.query_params.get("request_token", "")
        state = request.query_params.get("state", "")
        kind = classify_callback(
            status=status,
            request_token=request_token,
            state=state,
            expected_state=login.state,
        )
        if kind == "waiting":
            return _html(
                200,
                "Waiting for Kite",
                "Log in through the URL printed by scalper login. Kite redirects to this page.",
            )
        if login.succeeded:
            return _html(409, "Login already finished", "This callback already stored a session. You can close this tab.")
        if kind == "bad_state":
            log.warning("kite_login_state_rejected")
            return _html(
                400,
                "Login did not match this process",
                "Start scalper login again and open the URL that process prints.",
            )
        if kind == "kite_status":
            log.warning("kite_login_not_successful", kite_status=status[:32])
            return _html(
                400,
                "Kite did not complete login",
                "The broker did not return a successful login. Open the printed URL and try again.",
            )
        if kind == "bad_token":
            log.warning("kite_login_token_unusable")
            return _html(
                400,
                "Login token was unusable",
                "Kite's redirect did not include a usable request token. Open the printed URL and try again.",
            )
        try:
            session = await exchange_request_token(
                api_key=login.settings.kite_api_key.get_secret_value(),
                api_secret=login.settings.kite_api_secret.get_secret_value(),
                request_token=request_token,
                expected_user_id=login.settings.expected_user_id,
                client=login.client,
            )
            save_session(login.settings.session_path, session)
        except PersonalAccountError as exc:
            log.error("session_refused", reason=str(exc))
            return _html(403, "Account refused", str(exc))
        except KiteError as exc:
            log.error("kite_request_failed", status_code=exc.status_code, error_type=exc.error_type)
            detail = exc.error_type or "the broker rejected the login"
            return _html(502, "Kite rejected the login", f"Kite returned {detail}. Open the printed URL and try again.")
        except httpx.HTTPError:
            log.error("kite_transport_failed")
            return _html(502, "Kite was unreachable", "The token exchange did not connect. Open the printed URL and try again.")
        login.succeeded = True
        login.user_id = session.user_id
        login.expires_at = session.expires_at.isoformat()
        log.info("kite_session_stored", user_id=session.user_id, expires_at=login.expires_at)
        response = RedirectResponse(url="/done", status_code=303)
        _secure_headers(response)
        if login.server is not None:
            response.background = BackgroundTask(_exit_after_response, login.server)
        return response

    return app


async def _exit_after_response(server: uvicorn.Server) -> None:
    # The browser follows the redirect before the process exits.
    await asyncio.sleep(1.0)
    server.should_exit = True


def _same_secret(left: str, right: str) -> bool:
    if len(left) != len(right) or not left:
        return False
    return secrets.compare_digest(left, right)


def _secure_headers(response: Response) -> None:
    response.headers["Cache-Control"] = "no-store"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-Robots-Tag"] = "noindex"
    response.headers["Content-Security-Policy"] = "default-src 'none'"


def _page(title: str, body: str) -> str:
    safe_title = html.escape(title)
    safe_body = html.escape(body)
    return (
        "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">"
        f"<title>{safe_title}</title></head><body><h1>{safe_title}</h1><p>{safe_body}</p></body></html>"
    )


def _html(status_code: int, title: str, body: str) -> HTMLResponse:
    response = HTMLResponse(_page(title, body), status_code=status_code)
    _secure_headers(response)
    return response


def _install_redact_filters() -> _RedactRequestToken:
    redact = _RedactRequestToken()
    for name in _REDACT_LOGGERS:
        logging.getLogger(name).addFilter(redact)
    return redact


def _remove_redact_filters(redact: _RedactRequestToken) -> None:
    for name in _REDACT_LOGGERS:
        logging.getLogger(name).removeFilter(redact)


def _instructions(settings: Settings, state: str, redirect: str) -> str:
    url = login_url(settings.kite_api_key.get_secret_value(), redirect_params=f"state={state}")
    deadline = now_ist().replace(microsecond=0) + timedelta(seconds=settings.login_timeout_seconds)
    return (
        "Register this redirect URL on the Kite developer console. It has to match exactly:\n"
        f"  {redirect}\n\n"
        "Then log in yourself at:\n"
        f"  {url}\n\n"
        f"This process waits until {deadline.isoformat()} and then exits.\n"
        "The request token is not written to the log.\n"
    )


async def _watchdog(server: uvicorn.Server, login: CallbackLogin, seconds: int) -> None:
    await asyncio.sleep(seconds)
    login.timed_out = True
    server.should_exit = True


async def run_callback_login(
    settings: Settings,
    *,
    open_browser: bool,
    client: httpx.AsyncClient | None = None,
    state: str | None = None,
) -> int:
    require_login_credentials(settings)
    owns_client = client is None
    if client is None:
        from scalper.kite.http import kite_client

        client = kite_client()
    assert client is not None
    login_state = state or secrets.token_urlsafe(24)
    login = CallbackLogin(settings=settings, state=login_state, client=client)
    app = create_app(login)
    config = uvicorn.Config(
        app,
        host=settings.callback_host,
        port=settings.callback_port,
        access_log=False,
        log_level="error",
        lifespan="off",
    )
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None  # type: ignore[method-assign]
    login.server = server
    redirect = callback_url(settings.callback_host, settings.callback_port)
    redact = _install_redact_filters()
    watchdog = asyncio.create_task(_watchdog(server, login, settings.login_timeout_seconds))
    try:
        sys.stdout.write(_instructions(settings, login_state, redirect))
        sys.stdout.flush()
        if open_browser:
            webbrowser.open(login_url(settings.kite_api_key.get_secret_value(), redirect_params=f"state={login_state}"))
        await server.serve()
    finally:
        watchdog.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watchdog
        _remove_redact_filters(redact)
        if owns_client:
            await client.aclose()
    log = structlog.get_logger()
    if not server.started and not login.succeeded:
        log.error("callback_bind_failed", host=settings.callback_host, port=settings.callback_port)
        return 2
    if login.succeeded:
        sys.stdout.write(json.dumps({"user_id": login.user_id, "expires_at": login.expires_at}) + "\n")
        sys.stdout.flush()
        return 0
    if login.timed_out:
        log.error("login_timed_out", seconds=settings.login_timeout_seconds)
        return 1
    log.error("login_stopped")
    return 1
