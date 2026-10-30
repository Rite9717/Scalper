from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

import scalper.paper.replay as replay_module
from scalper.backtest.strategy import EntrySignal, MarketState
from scalper.clock import IST
from scalper.config import Settings
from scalper.data.models import NormalizedQuote
from scalper.execution.kite_broker import KiteBroker
from scalper.kite.instruments import NiftyFutureContract
from scalper.paper.journal import MemoryJournal
from scalper.paper.replay import NoRecordedTicks, no_ticks_message, run_recorded_quotes

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


def test_replay_writes_journal_rows_without_calling_kite(monkeypatch: pytest.MonkeyPatch) -> None:
    def boom(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("replay called Kite")

    monkeypatch.setattr(KiteBroker, "__init__", boom)
    monkeypatch.setattr("scalper.data.ticker.connect_kite_ticker", boom)
    monkeypatch.setattr("scalper.kite.session.load_session", boom)
    text = Path(replay_module.__file__).read_text(encoding="utf-8")
    assert "KiteBroker(" not in text
    assert "connect_kite_ticker" not in text
    assert "load_session" not in text

    async def scenario() -> None:
        journal = MemoryJournal()
        run_id = await run_recorded_quotes(
            settings=_settings(),
            strategy=_Once(),
            contract=_contract(),
            quotes=_quotes(),
            journal=journal,
            day=DAY.date(),
            kill_file=Path("replay-kill"),
        )
        view = await journal.read(run_id)
        assert view["runs"][0]["strategy"] == "trend_pullback"
        assert view["runs"][0]["tradingsymbol"] == "NIFTY26OCTFUT"
        assert view["runs"][0]["limits"]["slippage_ticks"] == "1"
        assert view["signals"]
        assert view["orders"]
        assert view["fills"]
        assert view["exits"]
        assert view["exits"][0]["fill_price"] is not None

    asyncio.run(scenario())


def test_replay_rejects_a_day_with_no_ticks() -> None:
    async def scenario() -> None:
        with pytest.raises(NoRecordedTicks, match=no_ticks_message(DAY.date())):
            await run_recorded_quotes(
                settings=_settings(),
                strategy=_Once(),
                contract=_contract(),
                quotes=[],
                journal=MemoryJournal(),
                day=DAY.date(),
                kill_file=Path("replay-kill"),
            )

    asyncio.run(scenario())
