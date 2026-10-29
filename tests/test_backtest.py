from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from scalper.backtest.costs import ChargeSchedule, cost_gate_allows, round_trip_charges
from scalper.backtest.engine import BacktestSpec, Trade, run_backtest
from scalper.backtest.fills import RestingLimit, quote_limit_filled
from scalper.backtest.report import judge, max_drawdown, summarize
from scalper.backtest.strategy import EntrySignal, MarketState
from scalper.cli import main
from scalper.clock import IST
from scalper.data.models import Candle, DepthLevel, NormalizedQuote

TICK = Decimal("0.05")
LOT = 15


def _bar(hour: int, minute: int, low: str, high: str, open_: str = "100", close: str = "100") -> Candle:
    return Candle(
        instrument_token=1,
        bucket_start=datetime(2026, 10, 27, hour, minute, tzinfo=IST),
        interval="minute",
        open=Decimal(open_),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=100,
        oi=None,
        source="test",
    )


def _spec(**overrides: object) -> BacktestSpec:
    values: dict[str, object] = {
        "tick_size": TICK,
        "lot_size": LOT,
        "lots": 1,
        "charges": ChargeSchedule(spread_ticks=0),
        "cost_multiple": Decimal("0"),
        "slippage_ticks": 0,
        "latency_bars": 1,
        "flatten_time": datetime.strptime("15:20", "%H:%M").time(),
    }
    values.update(overrides)
    return BacktestSpec(**values)  # type: ignore[arg-type]


class _Scripted:
    name = "scripted"

    def __init__(self, signals: dict[int, EntrySignal]) -> None:
        self.signals = signals
        self.lengths: list[int] = []

    def on_bar(self, state: MarketState) -> EntrySignal | None:
        self.lengths.append(len(state.bars))
        return self.signals.get(len(state.bars) - 1)


def _buy(limit: str = "100", stop: str = "99", target: str = "101", bars: int = 5) -> EntrySignal:
    return EntrySignal("buy", Decimal(limit), Decimal(stop), Decimal(target), bars, "test")


def test_round_trip_matches_the_published_futures_schedule() -> None:
    schedule = ChargeSchedule(spread_ticks=0)
    breakdown = round_trip_charges(
        schedule,
        entry_price=Decimal("100"),
        exit_price=Decimal("110"),
        quantity=10,
        entry_side="buy",
        tick_size=TICK,
    )
    assert breakdown.brokerage == Decimal("0.63")
    assert breakdown.exchange == Decimal("0.04")
    assert breakdown.sebi == Decimal("0.00")
    assert breakdown.stt == Decimal("0.55")
    assert breakdown.stamp == Decimal("0.02")
    assert breakdown.gst == Decimal("0.12")
    assert breakdown.total == Decimal("1.36")
    assert breakdown.stt != Decimal("0.00")


def test_cost_gate_blocks_a_target_that_does_not_pay_for_the_round_trip() -> None:
    schedule = ChargeSchedule(spread_ticks=1)
    allowed = cost_gate_allows(
        schedule,
        entry_price=Decimal("100"),
        target_price=Decimal("100.05"),
        quantity=LOT,
        entry_side="buy",
        tick_size=TICK,
        cost_multiple=Decimal("2"),
    )
    wide = cost_gate_allows(
        schedule,
        entry_price=Decimal("100"),
        target_price=Decimal("110"),
        quantity=LOT,
        entry_side="buy",
        tick_size=TICK,
        cost_multiple=Decimal("2"),
    )
    assert allowed is False
    assert wide is True


def test_a_touch_does_not_fill_and_the_signal_bar_cannot_fill() -> None:
    strategy = _Scripted({0: _buy()})
    bars = [
        _bar(9, 15, "99.90", "100.20"),
        _bar(9, 16, "100.00", "100.20"),
        _bar(9, 17, "99.90", "100.20"),
    ]
    result = run_backtest(bars, strategy, _spec())
    assert strategy.lengths == [1]
    assert result.trades[0].entry_time == bars[2].bucket_start
    assert result.trades[0].entry_price == Decimal("100.00")


def test_stop_wins_when_the_fill_bar_could_also_reach_the_target() -> None:
    strategy = _Scripted({0: _buy(stop="99", target="101")})
    bars = [
        _bar(9, 15, "100", "100.20", close="100.10"),
        _bar(9, 16, "98.90", "101.20", open_="100.10", close="100"),
    ]
    result = run_backtest(bars, strategy, _spec())
    assert len(result.trades) == 1
    assert result.trades[0].exit_reason == "stop"
    assert result.trades[0].exit_price == Decimal("99.00")


def test_gap_through_a_stop_fills_at_the_open() -> None:
    strategy = _Scripted({0: _buy()})
    bars = [
        _bar(9, 15, "100", "100.20"),
        _bar(9, 16, "99.90", "100.20"),
        _bar(9, 17, "97", "98", open_="98", close="97.50"),
    ]
    result = run_backtest(bars, strategy, _spec())
    assert result.trades[0].exit_reason == "stop"
    assert result.trades[0].exit_price == Decimal("98.00")


def test_flatten_exits_at_the_open_before_a_target() -> None:
    strategy = _Scripted({0: _buy(target="120", bars=10)})
    bars = [
        _bar(15, 18, "100", "100.20"),
        _bar(15, 19, "99.90", "100.20"),
        _bar(15, 20, "99", "130", open_="101", close="129"),
    ]
    result = run_backtest(bars, strategy, _spec())
    assert result.trades[0].exit_reason == "flatten"
    assert result.trades[0].exit_price == Decimal("101.00")


def test_small_target_is_gated_and_quantity_follows_the_lot_size() -> None:
    strategy = _Scripted({0: _buy(target="100.05")})
    bars = [_bar(9, 15, "99", "101"), _bar(9, 16, "99", "101")]
    gated = run_backtest(bars, strategy, _spec(cost_multiple=Decimal("5")))
    assert gated.gated == 1
    assert gated.trades == []
    cheap = _spec(cost_multiple=Decimal("0"), lot_size=10, lots=1)
    expensive = _spec(cost_multiple=Decimal("0"), lot_size=20, lots=1)
    one = run_backtest(bars, _Scripted({0: _buy()}), cheap)
    two = run_backtest(bars, _Scripted({0: _buy()}), expensive)
    assert one.trades[0].quantity == 10
    assert two.trades[0].quantity == 20
    assert two.trades[0].charges > one.trades[0].charges


def test_unfilled_limit_expires() -> None:
    strategy = _Scripted({0: _buy(bars=1)})
    bars = [
        _bar(9, 15, "100", "100.20"),
        _bar(9, 16, "100.00", "100.20"),
        _bar(9, 17, "99.50", "100.20"),
    ]
    result = run_backtest(bars, strategy, _spec())
    assert result.trades == []
    assert result.expired_orders == 1


def test_quote_fill_needs_a_through_trade_or_enough_size_at_the_limit() -> None:
    when = datetime(2026, 10, 27, 9, 16, tzinfo=IST)
    order = RestingLimit("buy", Decimal("100"), 15, when)
    assert quote_limit_filled(_quote(when - timedelta(seconds=1), "99.95", 15), order) is False
    assert quote_limit_filled(_quote(when, "100.05", 15), order) is False
    assert quote_limit_filled(_quote(when, "100", 14), order) is False
    assert quote_limit_filled(_quote(when, "100", 15, depth=14), order) is False
    assert quote_limit_filled(_quote(when, "100", 15, depth=15), order) is True
    assert quote_limit_filled(_quote(when, "99.95", 1), order) is True


def test_acceptance_requires_three_hundred_trades_and_seventy_percent_of_windows() -> None:
    start = datetime(2026, 10, 1, 9, 15, tzinfo=IST)
    wins = [_trade(start, Decimal("1"))]
    verdict = judge(wins, [(start, start + timedelta(days=30))])
    assert verdict["passed"] is False
    assert any("300" in reason for reason in verdict["reasons"])
    good = [_trade(start, Decimal("1")), _trade(start + timedelta(days=1), Decimal("2"))]
    passed = judge(
        good,
        [(start, start + timedelta(days=10))],
        min_trades=2,
        min_positive_fraction=Decimal("0.70"),
    )
    assert passed["passed"] is True
    assert passed["parameters_fitted"] is False
    mixed = [
        _trade(start, Decimal("5")),
        _trade(start + timedelta(days=10), Decimal("-1")),
    ]
    failed = judge(
        mixed,
        [(start, start + timedelta(days=10)), (start + timedelta(days=10), start + timedelta(days=20))],
        min_trades=2,
        min_positive_fraction=Decimal("0.70"),
    )
    assert failed["passed"] is False
    assert failed["positive_windows"] == 1


def test_drawdown_and_profit_factor() -> None:
    start = datetime(2026, 10, 27, 9, 15, tzinfo=IST)
    trades = [_trade(start, Decimal("10")), _trade(start, Decimal("-30")), _trade(start, Decimal("5"))]
    assert max_drawdown([trade.net_pnl for trade in trades]) == Decimal("30")
    stats = summarize(trades)
    assert Decimal(stats["profit_factor"]) == Decimal("0.5")
    assert Decimal(stats["average_loss"]) == Decimal("30")
    assert stats["by_hour"]["9"] == 3


def test_backtest_command_prints_an_honest_plumbing_report(tmp_path) -> None:
    path = tmp_path / "bars.csv"
    path.write_text(
        "bucket_start,open,high,low,close,volume\n"
        "2026-10-27T09:15:00+05:30,100,100.20,99.90,100.10,10\n"
        "2026-10-27T09:16:00+05:30,100.10,100.20,99.90,100,10\n",
        encoding="utf-8",
    )
    code = main(["backtest", "--candles", str(path), "--tick", "0.05", "--lot", "15"])
    assert code == 0


def _trade(moment: datetime, pnl: Decimal) -> Trade:
    return Trade("buy", moment, moment, Decimal("100"), Decimal("100"), 1, pnl, Decimal("0"), pnl, "target", "test")


def _quote(moment: datetime, price: str, quantity: int, depth: int | None = None) -> NormalizedQuote:
    level_qty = quantity if depth is None else depth
    return NormalizedQuote(
        instrument_token=1,
        received_at=moment,
        exchange_ts=moment,
        last_price=Decimal(price),
        last_quantity=quantity,
        volume=100,
        average_price=Decimal(price),
        oi=None,
        open=Decimal(price),
        high=Decimal(price),
        low=Decimal(price),
        close=Decimal(price),
        depth=(
            DepthLevel("sell", 1, Decimal(price), level_qty, 1),
            DepthLevel("sell", 2, Decimal(price) + TICK, 1, 1),
            DepthLevel("sell", 3, Decimal(price) + TICK * 2, 1, 1),
            DepthLevel("sell", 4, Decimal(price) + TICK * 3, 1, 1),
            DepthLevel("sell", 5, Decimal(price) + TICK * 4, 1, 1),
            DepthLevel("buy", 1, Decimal(price) - TICK, 1, 1),
            DepthLevel("buy", 2, Decimal(price) - TICK * 2, 1, 1),
            DepthLevel("buy", 3, Decimal(price) - TICK * 3, 1, 1),
            DepthLevel("buy", 4, Decimal(price) - TICK * 4, 1, 1),
            DepthLevel("buy", 5, Decimal(price) - TICK * 5, 1, 1),
        ),
    )
