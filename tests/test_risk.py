from __future__ import annotations

import inspect
from datetime import datetime, timedelta
from decimal import Decimal

from scalper.cli import main
from scalper.clock import IST
from scalper.compliance import LIVE_INITIAL_MAX_LOTS
from scalper.risk.gate import Intent, RiskSnapshot, assess
from scalper.risk.killfile import RESUME_CONFIRMATION, engage, is_killed, release

NOW = datetime(2026, 10, 27, 10, 0, tzinfo=IST)


def _snapshot(**overrides: object) -> RiskSnapshot:
    values: dict[str, object] = {
        "now": NOW,
        "trading_mode": "paper",
        "lots_requested": 1,
        "open_lots": 0,
        "max_lots": 1,
        "planned_loss": Decimal("1000"),
        "max_loss_per_trade": Decimal("3000"),
        "daily_pnl": Decimal("0"),
        "daily_loss_limit": Decimal("5000"),
        "orders_last_second": 0,
        "orders_last_minute": 0,
        "max_orders_per_second": 3,
        "max_orders_per_minute": 150,
        "last_quote_at": NOW,
        "stale_after": timedelta(seconds=5),
        "available_margin": Decimal("100000"),
        "required_margin": Decimal("20000"),
        "flatten_time": datetime.strptime("15:20", "%H:%M").time(),
        "broker_pnl": Decimal("0"),
        "internal_pnl": Decimal("0"),
        "pnl_tolerance": Decimal("250"),
        "exchange_time": NOW,
        "clock_drift_limit": timedelta(seconds=2),
        "recent_rejects": 0,
        "max_rejects": 3,
        "killed": False,
    }
    values.update(overrides)
    return RiskSnapshot(**values)  # type: ignore[arg-type]


def test_assess_has_no_bypass() -> None:
    assert "bypass" not in inspect.signature(assess).parameters


def test_a_clean_entry_is_allowed_and_a_kill_still_allows_the_exit() -> None:
    assert assess(Intent.ENTRY, _snapshot()).allowed is True
    killed = assess(Intent.ENTRY, _snapshot(killed=True))
    assert killed.allowed is False
    assert killed.action == "halt"
    assert killed.reason == "manual kill"
    flatten = assess(Intent.FLATTEN, _snapshot(killed=True))
    assert flatten.allowed is True
    assert flatten.action == "flatten"


def test_entry_caps_and_unknown_margin() -> None:
    assert assess(Intent.ENTRY, _snapshot(planned_loss=Decimal("3001"))).reason == "loss per trade cap"
    assert assess(Intent.ENTRY, _snapshot(open_lots=1)).reason == "position lot cap"
    assert assess(Intent.ENTRY, _snapshot(orders_last_second=3)).reason == "orders per second cap"
    assert assess(Intent.ENTRY, _snapshot(orders_last_minute=150)).reason == "orders per minute cap"
    assert assess(Intent.ENTRY, _snapshot(available_margin=None)).reason == "margin unknown"
    assert assess(Intent.ENTRY, _snapshot(available_margin=Decimal("10"))).reason == "insufficient margin"
    assert assess(Intent.ENTRY, _snapshot(trading_mode="live", max_lots=2)).reason == "live trading is capped at 1 lot"
    assert LIVE_INITIAL_MAX_LOTS == 1


def test_anomalies_halt_entries_and_still_allow_an_exit() -> None:
    cases = [
        _snapshot(last_quote_at=None),
        _snapshot(last_quote_at=NOW - timedelta(seconds=6)),
        _snapshot(exchange_time=NOW - timedelta(seconds=3)),
        _snapshot(broker_pnl=Decimal("0"), internal_pnl=Decimal("1000")),
        _snapshot(recent_rejects=3),
    ]
    for snapshot in cases:
        entry = assess(Intent.ENTRY, snapshot)
        exit_ = assess(Intent.EXIT, snapshot)
        assert entry.allowed is False
        assert entry.action == "halt"
        assert exit_.allowed is True


def test_daily_loss_and_flatten_time_block_entries() -> None:
    loss = assess(Intent.ENTRY, _snapshot(daily_pnl=Decimal("-5000")))
    assert loss.allowed is False
    assert loss.action == "flatten"
    late_now = NOW.replace(hour=15, minute=20)
    late = assess(Intent.ENTRY, _snapshot(now=late_now, exchange_time=late_now, last_quote_at=late_now))
    assert late.reason == "flatten time"
    assert assess(Intent.EXIT, _snapshot(now=late_now, exchange_time=late_now, last_quote_at=late_now)).allowed is True


def test_kill_file_survives_until_the_exact_phrase(tmp_path) -> None:
    path = tmp_path / "KILL"
    engage(path, "operator stop")
    assert is_killed(path)
    assert (path.stat().st_mode & 0o777) == 0o600
    try:
        release(path, "please")
    except Exception as exc:
        assert "exact confirmation" in str(exc)
    else:
        raise AssertionError("resume accepted a wrong phrase")
    assert path.exists()
    release(path, RESUME_CONFIRMATION)
    assert not path.exists()


def test_kill_command_refuses_resume_without_the_phrase() -> None:
    assert main(["kill", "--reason", "test"]) == 0
    assert main(["resume", "--confirm", "nope"]) == 2
    assert main(["resume", "--confirm", RESUME_CONFIRMATION]) == 0
