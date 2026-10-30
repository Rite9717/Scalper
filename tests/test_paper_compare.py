from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from scalper.backtest.costs import round_trip_charges
from scalper.backtest.engine import run_backtest
from scalper.backtest.strategy import EntrySignal, MarketState
from scalper.clock import IST
from scalper.config import Settings
from scalper.data.models import NormalizedQuote
from scalper.kite.instruments import NiftyFutureContract
from scalper.paper.compare import (
    ObservedTrade,
    candles_from_quotes,
    compare_trades,
    render_comparison,
    spec_from_settings,
    trades_from_journal,
)
from scalper.paper.journal import MemoryJournal
from scalper.paper.replay import run_recorded_quotes
from scalper.paper.session import charges_from_settings

DAY = datetime(2026, 10, 27, 10, 0, tzinfo=IST)


class _Once:
    name = "trend_pullback"

    def __init__(self) -> None:
        self.used = False

    def on_bar(self, state: MarketState) -> EntrySignal | None:
        if self.used or not state.bars:
            return None
        self.used = True
        return EntrySignal("buy", Decimal("100"), Decimal("99"), Decimal("103"), 20, "test")


def _contract() -> NiftyFutureContract:
    return NiftyFutureContract(
        instrument_token=1,
        exchange_token=1,
        tradingsymbol="NIFTY26OCTFUT",
        name="NIFTY",
        expiry=date(2026, 10, 29),
        tick_size=Decimal("0.05"),
        lot_size=15,
        exchange="NFO",
        segment="NFO-FUT",
        instrument_type="FUT",
    )


def _quotes() -> list[NormalizedQuote]:
    bars = (
        (DAY, (Decimal("100"), Decimal("100.2"), Decimal("99.9"), Decimal("100"))),
        (DAY + timedelta(minutes=1), (Decimal("100"), Decimal("100.3"), Decimal("99.7"), Decimal("100"))),
        (DAY + timedelta(minutes=2), (Decimal("99.5"), Decimal("99.6"), Decimal("98.8"), Decimal("99.2"))),
    )
    quotes: list[NormalizedQuote] = []
    volume = 0
    for moment, prices in bars:
        for price in prices:
            volume += 1
            quotes.append(
                NormalizedQuote(
                    instrument_token=1,
                    received_at=moment,
                    exchange_ts=moment,
                    last_price=price,
                    last_quantity=1,
                    volume=volume,
                    average_price=price,
                    oi=None,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    depth=(),
                )
            )
    return quotes


def _settings() -> Settings:
    return Settings(
        paper_max_lots=1,
        max_loss_per_trade=Decimal("3000"),
        daily_loss_limit=Decimal("5000"),
        latency_bars=1,
        slippage_ticks=1,
        flatten_time="15:20",
    )


def _paired() -> tuple[list[ObservedTrade], list]:
    settings = _settings()
    contract = _contract()
    quotes = _quotes()

    async def scenario():
        journal = MemoryJournal()
        run_id = await run_recorded_quotes(
            settings=settings,
            strategy=_Once(),
            contract=contract,
            quotes=quotes,
            journal=journal,
            day=DAY.date(),
            kill_file=Path("compare-kill"),
        )
        return await journal.read(run_id)

    view = asyncio.run(scenario())
    candles = candles_from_quotes(quotes, contract.instrument_token)
    backtest = run_backtest(
        candles,
        _Once(),
        spec_from_settings(settings, contract.tick_size, contract.lot_size, contract.expiry),
    )
    paper = trades_from_journal(
        view,
        charges=charges_from_settings(settings),
        tick_size=contract.tick_size,
    )
    return paper, backtest.trades


def test_compare_is_empty_when_paper_and_backtest_agree() -> None:
    paper, trades = _paired()
    report = compare_trades(
        paper,
        trades,
        tick_size=Decimal("0.05"),
        slippage_ticks=1,
    )
    text = render_comparison(report)
    assert report.differences == ()
    assert report.paper_net == report.backtest_net
    assert report.cause_counts == {}
    assert "no differences" in text
    assert f"net difference {format(Decimal('0'), 'f')}" in text


def test_compare_labels_a_tick_fill_the_candle_backtest_does_not_have() -> None:
    paper, trades = _paired()
    settings = _settings()
    contract = _contract()
    shifted_exit = paper[0].exit_price - Decimal("12.35")
    gross = (shifted_exit - paper[0].entry_price) * paper[0].quantity
    charges = round_trip_charges(
        charges_from_settings(settings),
        entry_price=paper[0].entry_price,
        exit_price=shifted_exit,
        quantity=paper[0].quantity,
        entry_side=paper[0].side,
        tick_size=contract.tick_size,
    ).total
    shifted = replace(paper[0], exit_price=shifted_exit, net_pnl=gross - charges)
    extra = replace(
        paper[0],
        entry_time=paper[0].entry_time + timedelta(hours=1),
        exit_time=paper[0].exit_time + timedelta(hours=1),
        net_pnl=Decimal("-9999.5"),
    )
    report = compare_trades(
        [shifted, extra],
        trades,
        tick_size=contract.tick_size,
        slippage_ticks=settings.slippage_ticks,
    )
    text = render_comparison(report)
    net_gap = report.paper_net - report.backtest_net
    assert abs(net_gap) > 100
    assert f"net difference {format(net_gap, 'f')}" in text
    assert format(shifted_exit, "f") in text
    assert format(trades[0].exit_price, "f") in text
    assert "tick-level versus candle-level fills" in text
    assert "present in only one" in text
    assert report.cause_counts["tick-level versus candle-level fills"] == 1
    assert report.cause_counts["present in only one"] == 1
    assert "slippage" not in report.cause_counts
    assert "-9999.5" in text
