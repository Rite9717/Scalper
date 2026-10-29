from __future__ import annotations

import asyncio
import gzip
import hashlib
import os
from datetime import date, datetime
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest

from scalper.cli import establish_session, main, refresh_contract
from scalper.clock import IST, access_token_expires_at, in_eod_maintenance_window, now_ist
from scalper.compliance import PersonalAccountError
from scalper.config import Settings
from scalper.kite.auth import exchange_request_token, login_url, session_checksum
from scalper.kite.http import KiteError, kite_client
from scalper.kite.session import assess_session, load_session, save_session

FIXTURE_CSV = Path(__file__).resolve().parent / "fixtures" / "nfo_sample.csv"


def test_checksum_is_sha256_of_the_documented_concatenation() -> None:
    digest = session_checksum("api", "request", "secret")
    assert digest == hashlib.sha256(b"apirequestsecret").hexdigest()


def test_login_url_uses_the_documented_endpoint() -> None:
    url = login_url("mykey", redirect_params="some=X&more=Y")
    assert url.startswith("https://kite.zerodha.com/connect/login?")
    assert "v=3" in url
    assert "api_key=mykey" in url
    assert "redirect_params=some%3DX%26more%3DY" in url


def test_token_expiry_and_maintenance_window() -> None:
    morning = datetime(2026, 10, 24, 8, 15, tzinfo=IST)
    assert access_token_expires_at(morning) == datetime(2026, 10, 25, 6, 0, tzinfo=IST)
    before_open = datetime(2026, 10, 24, 5, 30, tzinfo=IST)
    assert access_token_expires_at(before_open) == datetime(2026, 10, 24, 6, 0, tzinfo=IST)
    at_six = datetime(2026, 10, 24, 6, 0, tzinfo=IST)
    assert access_token_expires_at(at_six) == datetime(2026, 10, 25, 6, 0, tzinfo=IST)
    assert in_eod_maintenance_window(datetime(2026, 10, 25, 3, 0, tzinfo=IST))
    assert not in_eod_maintenance_window(datetime(2026, 10, 25, 6, 0, tzinfo=IST))
    assert not in_eod_maintenance_window(datetime(2026, 10, 24, 18, 0, tzinfo=IST))
    with pytest.raises(ValueError):
        access_token_expires_at(datetime(2026, 10, 24, 8, 15))


def _success_body() -> dict[str, object]:
    return {
        "status": "success",
        "data": {
            "user_type": "individual",
            "user_id": "AB1234",
            "user_name": "Example",
            "access_token": "access-token-value",
            "refresh_token": "",
            "login_time": "2026-10-24 08:15:00",
            "exchanges": ["NSE", "NFO"],
            "api_key": "key",
        },
    }


def test_exchange_posts_checksum_and_not_the_secret(tmp_path) -> None:
    secret = "super-secret-value"
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.content.decode()
        seen["body"] = body
        assert "super-secret-value" not in body
        assert request.headers["X-Kite-Version"] == "3"
        assert "Authorization" not in request.headers
        parsed = parse_qs(body)
        assert parsed["checksum"] == [session_checksum("key", "req-token", secret)]
        assert parsed["api_key"] == ["key"]
        assert parsed["request_token"] == ["req-token"]
        return httpx.Response(200, json=_success_body())

    async def scenario() -> None:
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="https://api.kite.trade",
            headers={"X-Kite-Version": "3"},
        ) as client:
            session = await exchange_request_token(
                api_key="key",
                api_secret=secret,
                request_token="req-token",
                expected_user_id="AB1234",
                client=client,
            )
        assert session.access_token.get_secret_value() == "access-token-value"
        assert session.expires_at == datetime(2026, 10, 25, 6, 0, tzinfo=IST)
        path = tmp_path / "session.json"
        save_session(path, session)
        assert (path.stat().st_mode & 0o777) == 0o600
        raw = path.read_text(encoding="utf-8")
        assert secret not in raw
        assert "access-token-value" in raw
        loaded = load_session(path)
        assert loaded.user_id == "AB1234"
        assert loaded.access_token.get_secret_value() == "access-token-value"
        path.chmod(0o644)
        with pytest.raises(PersonalAccountError, match="group or others"):
            load_session(path)

    asyncio.run(scenario())
    assert seen["body"]


def test_exchange_refuses_the_wrong_account_and_platform_tokens() -> None:
    def handler(status_body: dict[str, object]):
        def _inner(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=status_body)

        return _inner

    async def attempt(body: dict[str, object]) -> None:
        transport = httpx.MockTransport(handler(body))
        async with httpx.AsyncClient(transport=transport, base_url="https://api.kite.trade") as client:
            await exchange_request_token(
                api_key="key",
                api_secret="secret",
                request_token="req-token",
                expected_user_id="AB1234",
                client=client,
            )

    wrong_user = _success_body()
    wrong_user["data"]["user_id"] = "ZZ9999"  # type: ignore[index]
    platform = _success_body()
    platform["data"]["refresh_token"] = "platform-refresh"  # type: ignore[index]
    no_nfo = _success_body()
    no_nfo["data"]["exchanges"] = ["NSE"]  # type: ignore[index]
    for body in (wrong_user, platform, no_nfo):
        with pytest.raises(PersonalAccountError):
            asyncio.run(attempt(body))


def test_corrupt_session_does_not_echo_the_token(tmp_path) -> None:
    path = tmp_path / "session.json"
    path.write_text('{"access_token": "LEAKED-TOKEN"}', encoding="utf-8")
    path.chmod(0o600)
    with pytest.raises(PersonalAccountError, match="unreadable") as caught:
        load_session(path)
    assert "LEAKED-TOKEN" not in str(caught.value)
    assert caught.value.__cause__ is None


def test_symlink_session_is_refused(tmp_path) -> None:
    target = tmp_path / "real.json"
    target.write_text("{}", encoding="utf-8")
    target.chmod(0o600)
    link = tmp_path / "link.json"
    link.symlink_to(target)
    with pytest.raises(PersonalAccountError, match="symlink"):
        load_session(link)


def test_session_health_blocks_new_trades_during_maintenance() -> None:
    from scalper.kite.session import KiteSession

    session = KiteSession(
        user_id="AB1234",
        user_type="individual",
        access_token="access-token-value",
        login_time=datetime(2026, 10, 24, 8, 0, tzinfo=IST),
        expires_at=datetime(2026, 10, 25, 6, 0, tzinfo=IST),
        api_key="key",
    )
    during = assess_session(
        session,
        now=datetime(2026, 10, 25, 3, 0, tzinfo=IST),
        expected_user_id="AB1234",
        api_key="key",
    )
    assert during.usable
    assert during.in_maintenance_window
    assert not during.new_trading_allowed
    after = assess_session(
        session,
        now=datetime(2026, 10, 25, 6, 0, tzinfo=IST),
        expected_user_id="AB1234",
        api_key="key",
    )
    assert after.expired
    assert not after.usable
    mismatch = assess_session(
        session,
        now=datetime(2026, 10, 24, 10, 0, tzinfo=IST),
        expected_user_id="ZZ9999",
        api_key="key",
    )
    assert not mismatch.user_matches
    assert not mismatch.usable


def test_login_command_requires_credentials_and_does_not_open_kite(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["login", "--no-browser"])
    captured = capsys.readouterr()
    assert code == 2
    assert "SCALPER_KITE_API_KEY" in captured.err
    assert "SCALPER_EXPECTED_USER_ID" in captured.err
    assert "kite.zerodha.com" not in captured.out
    assert "kite.zerodha.com" not in captured.err


def test_exchange_command_requires_a_request_token(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["session", "exchange"])
    captured = capsys.readouterr()
    assert code == 2
    assert "request_token_missing" in captured.err


def test_fetch_archives_and_resolves_with_a_mocked_kite(tmp_path) -> None:
    login = now_ist()
    from scalper.kite.session import KiteSession

    session = KiteSession(
        user_id="AB1234",
        user_type="individual",
        access_token="access-token-value",
        login_time=login,
        expires_at=access_token_expires_at(login),
        api_key="key",
    )
    settings = Settings(
        expected_user_id="AB1234",
        kite_api_key="key",
        kite_api_secret="secret",
        session_path=tmp_path / "session.json",
        instrument_archive_dir=tmp_path / "instruments",
    )
    save_session(settings.session_path, session)
    payload = gzip.compress(FIXTURE_CSV.read_bytes())

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/instruments/NFO"
        assert request.headers["Authorization"] == "token key:access-token-value"
        assert b"secret" not in request.content
        return httpx.Response(200, content=payload)

    async def scenario():
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport, base_url="https://api.kite.trade") as client:
            contract, archived, health = await refresh_contract(
                settings,
                as_of=date(2026, 10, 24),
                client=client,
            )
        return contract, archived, health

    contract, archived, health = asyncio.run(scenario())
    assert contract.tradingsymbol == "NIFTY26OCTFUT"
    assert contract.lot_size == 65
    assert contract.tick_size.as_tuple().exponent == -2
    assert archived.is_file()
    assert health.user_matches
    client = kite_client()
    try:
        assert client.headers["X-Kite-Version"] == "3"
        assert client.follow_redirects is False
    finally:
        asyncio.run(client.aclose())


def test_instrument_error_is_a_kite_error() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            json={"status": "error", "message": "Incorrect `api_key` or `access_token`.", "error_type": "TokenException"},
        )

    async def scenario() -> None:
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport, base_url="https://api.kite.trade") as client:
            from scalper.kite.instruments import fetch_instrument_csv

            await fetch_instrument_csv(api_key="key", access_token="token", client=client)

    with pytest.raises(KiteError) as caught:
        asyncio.run(scenario())
    assert caught.value.status_code == 403
    assert caught.value.error_type == "TokenException"


def test_establish_session_writes_a_private_file(tmp_path) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_success_body())

    settings = Settings(
        expected_user_id="AB1234",
        kite_api_key="key",
        kite_api_secret="super-secret-value",
        session_path=tmp_path / "session.json",
    )

    async def scenario() -> None:
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport, base_url="https://api.kite.trade") as client:
            await establish_session(settings, "req-token", client)

    asyncio.run(scenario())
    raw = settings.session_path.read_text(encoding="utf-8")
    assert "super-secret-value" not in raw
    assert os.stat(settings.session_path).st_mode & 0o777 == 0o600
