"""Structured logging. Secret values are redacted before they are rendered."""

from __future__ import annotations

import logging
import sys
from collections.abc import Mapping
from datetime import datetime
from typing import Any

import structlog

from scalper.clock import IST

_REDACT_KEYS = frozenset(
    {
        "api_secret",
        "kite_api_secret",
        "access_token",
        "kite_access_token",
        "request_token",
        "public_token",
        "refresh_token",
        "enctoken",
        "checksum",
        "authorization",
        "password",
        "token",
        "database_url",
        "redis_url",
        "kite_api_key",
        "telegram_bot_token",
        "bot_token",
    }
)


def _sensitive(key: str) -> bool:
    return key.lower() in _REDACT_KEYS


def _redact(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: "***" if _sensitive(str(key)) else _redact(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact(item) for item in value)
    return value


def redact_secrets(_logger: Any, _method: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    redacted = _redact(event_dict)
    if not isinstance(redacted, dict):
        return {"event": redacted}
    return redacted


def _add_timestamp(_logger: Any, _method: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    event_dict["timestamp"] = datetime.now(IST).isoformat()
    return event_dict


class _StderrLogger:
    """Writes to the current stderr so a closed capture stream is not reused."""

    def msg(self, message: str) -> None:
        print(message, file=sys.stderr, flush=True)

    log = debug = info = warn = warning = error = fatal = critical = exception = msg


class _StderrLoggerFactory:
    def __call__(self, *args: Any) -> _StderrLogger:
        return _StderrLogger()


def configure_logging(level: str = "INFO", json_output: bool = True) -> None:
    from scalper import PHASE

    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(service="scalper", phase=PHASE)
    renderer: structlog.types.Processor
    if json_output:
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=False)
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            _add_timestamp,
            redact_secrets,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(getattr(logging, level)),
        logger_factory=_StderrLoggerFactory(),
        cache_logger_on_first_use=False,
    )


def decision_logger() -> structlog.stdlib.BoundLogger:
    """Logger name reserved for signals, risk decisions, orders, and fills."""
    return structlog.get_logger("decision")
