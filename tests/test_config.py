from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from scalper.cli import main
from scalper.compliance import LIVE_CONFIRMATION_PHRASE, LIVE_INITIAL_MAX_LOTS
from scalper.config import Settings, load_settings


def test_defaults_are_paper_and_inside_the_client_caps() -> None:
    settings = Settings()
    assert settings.trading_mode == "paper"
    assert settings.max_orders_per_second == 3
    assert settings.max_orders_per_minute == 150
    assert settings.max_orders_per_day == 5000
    assert settings.public_view()["position_lot_cap"] == 1


def test_rate_caps_cannot_be_raised_to_kite_limits() -> None:
    with pytest.raises(ValidationError):
        Settings(max_orders_per_second=10)
    with pytest.raises(ValidationError):
        Settings(max_orders_per_minute=400)
    with pytest.raises(ValidationError):
        Settings(max_orders_per_day=5001)
    with pytest.raises(ValidationError):
        Settings(max_orders_per_second=3, max_orders_per_minute=2)


def test_live_mode_requires_phrase_identity_and_public_ip() -> None:
    with pytest.raises(ValidationError):
        Settings(trading_mode="live")
    with pytest.raises(ValidationError):
        Settings(
            trading_mode="live",
            live_confirmation=LIVE_CONFIRMATION_PHRASE,
            expected_user_id="AB1234",
            kite_api_key="key",
            kite_api_secret="secret",
            static_ip="10.1.2.3",
        )
    settings = Settings(
        trading_mode="live",
        live_confirmation=LIVE_CONFIRMATION_PHRASE,
        expected_user_id="AB1234",
        kite_api_key="key",
        kite_api_secret="kite-secret-value",
        static_ip="8.8.8.8",
        paper_max_lots=5,
    )
    view = settings.public_view()
    assert settings.trading_mode == "live"
    assert view["position_lot_cap"] == LIVE_INITIAL_MAX_LOTS
    assert view["static_ip"] == "8.8.8.8"
    rendered = json.dumps(view)
    assert "kite-secret-value" not in rendered
    assert view["kite_api_secret_set"] is True


@pytest.mark.parametrize("ip", ["127.0.0.1", "10.0.0.8", "192.168.1.9", "172.16.0.4", "100.64.1.1"])
def test_private_and_shared_ips_are_rejected(ip: str) -> None:
    with pytest.raises(ValidationError):
        Settings(static_ip=ip)


def test_unbuilt_login_secrets_are_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCALPER_KITE_PASSWORD", "nope")
    with pytest.raises(Exception, match="no password or TOTP"):
        load_settings()


def test_totp_login_method_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCALPER_LOGIN_METHOD", "totp")
    with pytest.raises(Exception, match="browser callback"):
        load_settings()


def test_callback_host_must_be_loopback() -> None:
    with pytest.raises(ValidationError):
        Settings(callback_host="0.0.0.0")
    with pytest.raises(ValidationError):
        Settings(callback_port=80)
    settings = Settings(callback_host="localhost")
    assert settings.callback_host == "127.0.0.1"
    assert settings.public_view()["callback_url"] == "http://127.0.0.1:8765/callback"
    assert settings.public_view()["login_method"] == "callback"


def test_invalid_live_config_does_not_print_secrets(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("SCALPER_TRADING_MODE", "live")
    monkeypatch.setenv("SCALPER_KITE_API_KEY", "key-value")
    monkeypatch.setenv("SCALPER_KITE_API_SECRET", "super-secret-value")
    monkeypatch.setenv("SCALPER_EXPECTED_USER_ID", "AB1234")
    monkeypatch.setenv("SCALPER_STATIC_IP", "8.8.8.8")
    code = main(["health"])
    captured = capsys.readouterr()
    assert code == 2
    assert "super-secret-value" not in captured.out
    assert "super-secret-value" not in captured.err
    assert "key-value" not in captured.out
    assert "key-value" not in captured.err
    assert "SCALPER_LIVE_CONFIRMATION" in captured.err


def test_health_command_prints_a_redacted_view(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["health"])
    captured = capsys.readouterr()
    assert code == 0
    payload = json.loads(captured.out)
    assert payload["status"] == "ok"
    assert payload["phase"] == 8
    assert payload["trading_mode"] == "paper"
    assert payload["live_confirmation_phrase"] == LIVE_CONFIRMATION_PHRASE
    assert "access_token" not in captured.out
