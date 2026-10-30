from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from scalper.config import Settings
from scalper.paper.limits import PaperLimits, account_limits, resolved_limits


def test_paper_limits_use_the_configured_settings() -> None:
    settings = Settings(
        paper_max_lots=4,
        max_loss_per_trade=Decimal("1234"),
        daily_loss_limit=Decimal("4321"),
        max_orders_per_second=2,
        max_orders_per_minute=40,
        flatten_time="15:10",
        latency_bars=2,
        slippage_ticks=3,
    )
    paper = PaperLimits(stale_after=timedelta(seconds=9), max_rejects=5, stops_blind_grace=timedelta(seconds=30))
    gate = account_limits(settings, paper)
    record = resolved_limits(settings, paper)

    assert gate.trading_mode == "paper"
    assert gate.max_lots == 4
    assert gate.max_loss_per_trade == Decimal("1234")
    assert gate.daily_loss_limit == Decimal("4321")
    assert gate.max_orders_per_second == 2
    assert gate.max_orders_per_minute == 40
    assert gate.flatten_time.hour == 15 and gate.flatten_time.minute == 10
    assert gate.stale_after == timedelta(seconds=9)
    assert gate.max_rejects == 5
    assert record["max_loss_per_trade"] == "1234"
    assert record["daily_loss_limit"] == "4321"
    assert record["flatten_time"] == "15:10"
    assert record["stale_after_seconds"] == "9.0"
    assert record["stops_blind_grace_seconds"] == "30.0"
    assert record["latency_bars"] == "2"
    assert record["slippage_ticks"] == "3"
    assert "flatten_max_adverse_ticks" in record
    assert "reconnect_initial_seconds" not in record
    assert "reconnect_max_seconds" not in record
