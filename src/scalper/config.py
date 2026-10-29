"""Process settings.

Environment variables use the SCALPER_ prefix. A .env file is optional and
is loaded only by the CLI. Tests construct Settings directly.
"""

from __future__ import annotations

import os
from ipaddress import IPv4Address, ip_address
from pathlib import Path

from decimal import Decimal
from datetime import datetime

from pydantic import Field, SecretStr, field_validator, model_validator

from scalper.backtest.costs import (
    DEFAULT_BROKERAGE_CAP,
    DEFAULT_BROKERAGE_RATE,
    DEFAULT_EXCHANGE_RATE,
    DEFAULT_GST_RATE,
    DEFAULT_SEBI_RATE,
    DEFAULT_STAMP_BUY_RATE,
    DEFAULT_STT_SELL_RATE,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from scalper.compliance import (
    CLIENT_MAX_ORDERS_PER_DAY,
    CLIENT_MAX_ORDERS_PER_MINUTE,
    CLIENT_MAX_ORDERS_PER_SECOND,
    LIVE_CONFIRMATION_PHRASE,
    ComplianceError,
    assert_public_ipv4,
    assert_user_id,
)

# Password and TOTP login are not implemented. Fail closed if those
# variables are set, instead of ignoring a credential the process will not use.
_UNBUILT_LOGIN_ENV = (
    "SCALPER_KITE_PASSWORD",
    "SCALPER_KITE_TOTP_SECRET",
    "SCALPER_PASSWORD",
    "SCALPER_TOTP_SECRET",
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SCALPER_",
        env_file=None,
        extra="ignore",
        case_sensitive=False,
    )

    trading_mode: str = "paper"
    live_confirmation: str = ""
    expected_user_id: str = ""
    kite_api_key: SecretStr = SecretStr("")
    kite_api_secret: SecretStr = SecretStr("")
    static_ip: str = ""

    log_level: str = "INFO"
    log_json: bool = True

    data_dir: Path = Path("data")
    session_path: Path = Path("data/session.json")
    instrument_archive_dir: Path = Path("data/instruments")

    database_url: SecretStr = SecretStr("postgresql://scalper:scalper@localhost:5432/scalper")
    redis_url: SecretStr = SecretStr("redis://localhost:6379/0")

    max_orders_per_second: int = Field(default=CLIENT_MAX_ORDERS_PER_SECOND, ge=1, le=CLIENT_MAX_ORDERS_PER_SECOND)
    max_orders_per_minute: int = Field(default=CLIENT_MAX_ORDERS_PER_MINUTE, ge=1, le=CLIENT_MAX_ORDERS_PER_MINUTE)
    max_orders_per_day: int = Field(default=CLIENT_MAX_ORDERS_PER_DAY, ge=1, le=CLIENT_MAX_ORDERS_PER_DAY)
    paper_max_lots: int = Field(default=1, ge=1, le=50)
    callback_host: str = "127.0.0.1"
    callback_port: int = Field(default=8765, ge=1024, le=65535)
    login_timeout_seconds: int = Field(default=300, ge=1, le=900)
    cost_multiple: Decimal = Field(default=Decimal("2"), ge=0)
    slippage_ticks: int = Field(default=1, ge=0, le=20)
    latency_bars: int = Field(default=1, ge=1, le=10)
    spread_ticks: int = Field(default=1, ge=0, le=20)
    flatten_time: str = "15:20"
    brokerage_rate: Decimal = Field(default=DEFAULT_BROKERAGE_RATE, ge=0)
    brokerage_cap: Decimal = Field(default=DEFAULT_BROKERAGE_CAP, ge=0)
    stt_sell_rate: Decimal = Field(default=DEFAULT_STT_SELL_RATE, ge=0)
    exchange_rate: Decimal = Field(default=DEFAULT_EXCHANGE_RATE, ge=0)
    sebi_rate: Decimal = Field(default=DEFAULT_SEBI_RATE, ge=0)
    stamp_buy_rate: Decimal = Field(default=DEFAULT_STAMP_BUY_RATE, ge=0)
    gst_rate: Decimal = Field(default=DEFAULT_GST_RATE, ge=0)
    max_loss_per_trade: Decimal = Field(default=Decimal("3000"), ge=0)
    daily_loss_limit: Decimal = Field(default=Decimal("5000"), ge=0)
    alerts_enabled: bool = False
    telegram_bot_token: SecretStr = SecretStr("")
    telegram_chat_id: str = ""

    @field_validator("trading_mode")
    @classmethod
    def _mode(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"paper", "live"}:
            raise ValueError("trading_mode must be paper or live")
        return normalized

    @field_validator("log_level")
    @classmethod
    def _level(cls, value: str) -> str:
        normalized = value.strip().upper()
        if normalized not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
            raise ValueError("log_level must be DEBUG, INFO, WARNING, or ERROR")
        return normalized

    @field_validator("expected_user_id")
    @classmethod
    def _user(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            return cleaned
        try:
            assert_user_id(cleaned)
        except ComplianceError as exc:
            raise ValueError(str(exc)) from exc
        return cleaned

    @field_validator("static_ip")
    @classmethod
    def _ip(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            return cleaned
        try:
            parsed = assert_public_ipv4(cleaned)
        except ComplianceError as exc:
            raise ValueError(str(exc)) from exc
        return str(parsed)

    @field_validator("callback_host")
    @classmethod
    def _callback_host(cls, value: str) -> str:
        cleaned = value.strip().lower()
        if cleaned == "localhost":
            return "127.0.0.1"
        try:
            parsed = ip_address(cleaned)
        except ValueError as exc:
            raise ValueError("callback host must be 127.0.0.1") from exc
        if not isinstance(parsed, IPv4Address) or not parsed.is_loopback:
            raise ValueError("callback host must be 127.0.0.1")
        return str(parsed)

    @field_validator("flatten_time")
    @classmethod
    def _flatten_time(cls, value: str) -> str:
        try:
            datetime.strptime(value.strip(), "%H:%M")
        except ValueError as exc:
            raise ValueError("flatten_time must be HH:MM") from exc
        return value.strip()

    @model_validator(mode="after")
    def _live_gate(self) -> Settings:
        if self.max_orders_per_minute < self.max_orders_per_second:
            raise ValueError("max_orders_per_minute cannot be below max_orders_per_second")
        if self.alerts_enabled and (
            not self.telegram_bot_token.get_secret_value() or not self.telegram_chat_id.strip()
        ):
            raise ValueError("alerts require SCALPER_TELEGRAM_BOT_TOKEN and SCALPER_TELEGRAM_CHAT_ID")
        if self.trading_mode != "live":
            return self
        if self.live_confirmation != LIVE_CONFIRMATION_PHRASE:
            raise ValueError(
                "live mode requires SCALPER_LIVE_CONFIRMATION="
                f"{LIVE_CONFIRMATION_PHRASE}"
            )
        if not self.static_ip:
            raise ValueError("live mode requires SCALPER_STATIC_IP")
        if not self.expected_user_id:
            raise ValueError("live mode requires SCALPER_EXPECTED_USER_ID")
        if not self.kite_api_key.get_secret_value():
            raise ValueError("live mode requires SCALPER_KITE_API_KEY")
        if not self.kite_api_secret.get_secret_value():
            raise ValueError("live mode requires SCALPER_KITE_API_SECRET")
        return self

    def public_view(self) -> dict[str, object]:
        """Values that are safe to print. Secrets are presence flags only."""
        from scalper.compliance import position_lot_cap

        return {
            "trading_mode": self.trading_mode,
            "expected_user_id": self.expected_user_id,
            "static_ip": self.static_ip,
            "kite_api_key_set": bool(self.kite_api_key.get_secret_value()),
            "kite_api_secret_set": bool(self.kite_api_secret.get_secret_value()),
            "max_orders_per_second": self.max_orders_per_second,
            "max_orders_per_minute": self.max_orders_per_minute,
            "max_orders_per_day": self.max_orders_per_day,
            "position_lot_cap": position_lot_cap(
                trading_mode=self.trading_mode,
                paper_max_lots=self.paper_max_lots,
            ),
            "log_level": self.log_level,
            "data_dir": str(self.data_dir),
            "session_path": str(self.session_path),
            "instrument_archive_dir": str(self.instrument_archive_dir),
            "login_method": "callback",
            "callback_url": f"http://{self.callback_host}:{self.callback_port}/callback",
            "login_timeout_seconds": self.login_timeout_seconds,
            "cost_multiple": str(self.cost_multiple),
            "flatten_time": self.flatten_time,
            "max_loss_per_trade": str(self.max_loss_per_trade),
            "daily_loss_limit": str(self.daily_loss_limit),
            "alerts_enabled": self.alerts_enabled,
            "telegram_configured": bool(self.telegram_bot_token.get_secret_value()),
        }


def reject_unbuilt_login_secrets() -> None:
    method = os.environ.get("SCALPER_LOGIN_METHOD", "").strip().lower()
    if method and method != "callback":
        raise ComplianceError("login method is the local browser callback; TOTP automation is not available")
    present = [name for name in _UNBUILT_LOGIN_ENV if os.environ.get(name)]
    if present:
        names = ", ".join(present)
        raise ComplianceError(
            "Phase 1 has no password or TOTP login. Unset these variables: " + names
        )


def load_settings(env_file: Path | None = None) -> Settings:
    reject_unbuilt_login_secrets()
    path = env_file if env_file is not None else Path(".env")
    if path.is_file():
        return Settings(_env_file=path)
    return Settings()
