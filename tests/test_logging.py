from __future__ import annotations

import pytest

from scalper.logging import configure_logging
import structlog


def test_secrets_are_redacted_and_instrument_tokens_are_not(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", json_output=True)
    structlog.get_logger().info(
        "session",
        access_token="super-secret-token",
        instrument_token=12345001,
        database_url="postgresql://scalper:db-password-xyz@db/scalper",
        nested={"request_token": "one-time-token", "lot_size": 65},
    )
    err = capsys.readouterr().err
    assert "super-secret-token" not in err
    assert "db-password-xyz" not in err
    assert "one-time-token" not in err
    assert "12345001" in err
    assert "65" in err
    assert "***" in err


def test_debug_is_filtered(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", json_output=True)
    structlog.get_logger().debug("hidden_detail", access_token="should-not-appear")
    structlog.get_logger().info("visible")
    err = capsys.readouterr().err
    assert "should-not-appear" not in err
    assert "hidden_detail" not in err
    assert "visible" in err
