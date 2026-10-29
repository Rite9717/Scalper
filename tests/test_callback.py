from __future__ import annotations

import asyncio
import json
import logging
import socket

import httpx
import pytest

from scalper.config import Settings
from scalper.kite.auth import session_checksum
from scalper.kite.callback import (
    CallbackLogin,
    _RedactRequestToken,
    callback_url,
    classify_callback,
    create_app,
    run_callback_login,
)
from scalper.logging import configure_logging

REQUEST_TOKEN = "REQUESTTOKEN12345678"
API_SECRET = "super-secret-value"


def _success_body() -> dict[str, object]:
    return {
        "status": "success",
        "data": {
            "user_type": "individual",
            "user_id": "AB1234",
            "access_token": "access-token-value",
            "refresh_token": "",
            "login_time": "2026-10-24 08:15:00",
            "exchanges": ["NSE", "NFO"],
        },
    }


def _settings(tmp_path, port: int = 8765) -> Settings:
    return Settings(
        expected_user_id="AB1234",
        kite_api_key="key",
        kite_api_secret=API_SECRET,
        session_path=tmp_path / "session.json",
        callback_port=port,
        login_timeout_seconds=5,
    )


def _kite_client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://api.kite.trade",
        headers={"X-Kite-Version": "3"},
    )


def test_classify_callback_never_treats_a_bare_hit_as_a_login() -> None:
    assert classify_callback(status="", request_token="", state="", expected_state="abc") == "waiting"
    assert (
        classify_callback(status="success", request_token=REQUEST_TOKEN, state="nope", expected_state="abc")
        == "bad_state"
    )
    assert (
        classify_callback(status="error", request_token="", state="abc", expected_state="abc")
        == "kite_status"
    )
    assert classify_callback(status="success", request_token="short", state="abc", expected_state="abc") == "bad_token"
    assert (
        classify_callback(status="success", request_token=REQUEST_TOKEN, state="abc", expected_state="abc")
        == "exchange"
    )


def test_request_token_filter_redacts_log_urls() -> None:
    record = logging.LogRecord(
        name="uvicorn.access",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="GET /callback?request_token=%s&state=abc",
        args=(REQUEST_TOKEN,),
        exc_info=None,
    )
    assert _RedactRequestToken().filter(record) is True
    rendered = record.getMessage()
    assert REQUEST_TOKEN not in rendered
    assert "request_token=***" in rendered


def test_callback_pages_do_not_echo_the_request_token(tmp_path) -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.content.decode()
        seen["body"] = body
        assert API_SECRET not in body
        from urllib.parse import parse_qs

        fields = parse_qs(body)
        assert fields["checksum"] == [session_checksum("key", REQUEST_TOKEN, API_SECRET)]
        assert fields["request_token"] == [REQUEST_TOKEN]
        return httpx.Response(200, json=_success_body())

    async def scenario() -> None:
        settings = _settings(tmp_path)
        login = CallbackLogin(settings=settings, state="state-value", client=_kite_client(handler))
        app = create_app(login)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://login") as http:
            waiting = await http.get("/callback")
            assert waiting.status_code == 200
            assert "Waiting for Kite" in waiting.text
            assert waiting.headers["referrer-policy"] == "no-referrer"
            assert waiting.headers["cache-control"] == "no-store"

            rejected = await http.get(
                "/callback",
                params={"status": "success", "request_token": REQUEST_TOKEN, "state": "wrong"},
            )
            assert rejected.status_code == 400
            assert REQUEST_TOKEN not in rejected.text
            assert API_SECRET not in rejected.text

            exchanged = await http.get(
                "/callback",
                params={"status": "success", "request_token": REQUEST_TOKEN, "state": "state-value"},
                follow_redirects=False,
            )
            assert exchanged.status_code == 303
            assert exchanged.headers["location"] == "/done"
            assert REQUEST_TOKEN not in exchanged.headers["location"]
            done = await http.get("/done")
            assert done.status_code == 200
            assert "AB1234" in done.text
            assert "Login complete" in done.text
            assert REQUEST_TOKEN not in done.text
            assert "access-token-value" not in done.text
        await login.client.aclose()

    asyncio.run(scenario())
    assert seen["body"]
    stored = (tmp_path / "session.json").read_text(encoding="utf-8")
    assert REQUEST_TOKEN not in stored
    assert API_SECRET not in stored
    assert "access-token-value" in stored
    assert (tmp_path / "session.json").stat().st_mode & 0o777 == 0o600


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def test_live_callback_exchanges_once_and_shuts_down(tmp_path, capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", json_output=True)

    def handler(request: httpx.Request) -> httpx.Response:
        assert API_SECRET.encode() not in request.content
        return httpx.Response(200, json=_success_body())

    port = _free_port()
    settings = _settings(tmp_path, port=port)

    async def scenario() -> int:
        client = _kite_client(handler)
        task = asyncio.create_task(
            run_callback_login(settings, open_browser=False, client=client, state="state-value")
        )
        deadline = asyncio.get_running_loop().time() + 5
        while True:
            try:
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                writer.close()
                await writer.wait_closed()
                break
            except OSError:
                if asyncio.get_running_loop().time() > deadline:
                    raise
                await asyncio.sleep(0.02)
        async with httpx.AsyncClient() as http:
            health = await http.get(f"http://127.0.0.1:{port}/health")
            assert health.json() == {"status": "ok", "service": "scalper-login"}
            response = await http.get(
                callback_url(settings.callback_host, port),
                params={"status": "success", "request_token": REQUEST_TOKEN, "state": "state-value"},
                follow_redirects=True,
            )
        assert response.status_code == 200
        assert "Login complete" in response.text
        assert REQUEST_TOKEN not in response.text
        code = await task
        await client.aclose()
        return code

    code = asyncio.run(scenario())
    captured = capsys.readouterr()
    assert code == 0
    assert REQUEST_TOKEN not in captured.out
    assert REQUEST_TOKEN not in captured.err
    assert API_SECRET not in captured.out
    assert API_SECRET not in captured.err
    assert "http://127.0.0.1:" in captured.out
    payload = json.loads(captured.out.strip().splitlines()[-1])
    assert payload["user_id"] == "AB1234"
    assert "access-token-value" not in captured.out
