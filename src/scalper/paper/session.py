"""Build the paper runner the live command and the replay command both use."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID

from scalper.backtest.costs import ChargeSchedule
from scalper.backtest.strategy import Strategy
from scalper.config import Settings
from scalper.data.cache import MemoryQuoteCache
from scalper.data.candles import MinuteBook
from scalper.execution.engine import ExecutionEngine
from scalper.execution.paper import PaperBroker
from scalper.execution.snapshot import LiveSnapshotProvider
from scalper.kite.instruments import NiftyFutureContract
from scalper.monitor.alerts import Monitor
from scalper.paper.bars import SessionMinuteBook
from scalper.paper.journal import PaperJournal
from scalper.paper.limits import PaperLimits, account_limits, resolved_limits
from scalper.paper.runner import PaperRunner


def charges_from_settings(settings: Settings) -> ChargeSchedule:
    return ChargeSchedule(
        brokerage_rate=settings.brokerage_rate,
        brokerage_cap=settings.brokerage_cap,
        stt_sell_rate=settings.stt_sell_rate,
        exchange_rate=settings.exchange_rate,
        sebi_rate=settings.sebi_rate,
        stamp_buy_rate=settings.stamp_buy_rate,
        gst_rate=settings.gst_rate,
        spread_ticks=settings.spread_ticks,
    )


class _FastClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += float(seconds)


def build_paper_runner(
    *,
    settings: Settings,
    strategy: Strategy,
    contract: NiftyFutureContract,
    journal: PaperJournal,
    run_id: UUID,
    monitor: Monitor,
    kill_file: Path,
    now: Callable[[], datetime],
    sleep: Callable[[float], Awaitable[None]],
    pace: bool,
    expiry_dates: set[date],
    paper: PaperLimits | None = None,
) -> PaperRunner:
    """Same limits, broker, risk provider, and journal for live ticks and replay."""
    limits = paper or PaperLimits()
    record = resolved_limits(settings, limits)
    gate = account_limits(settings, limits)
    charges = charges_from_settings(settings)
    cache = MemoryQuoteCache()
    broker = PaperBroker(
        contract.tick_size,
        contract.lot_size,
        tradingsymbol=contract.tradingsymbol,
        equity_cash=Decimal("1000000"),
    )
    provider = LiveSnapshotProvider(
        broker=broker,
        cache=cache,
        instrument_token=contract.instrument_token,
        tradingsymbol=contract.tradingsymbol,
        exchange=contract.exchange,
        kill_file=kill_file,
        live=False,
        fail_closed=True,
        limits=gate,
        tick_size=contract.tick_size,
        now=now,
    )
    engine_clock = _FastClock()
    engine = ExecutionEngine(
        broker,
        provider,
        tradingsymbol=contract.tradingsymbol,
        exchange=contract.exchange,
        lot_size=contract.lot_size,
        tick_size=contract.tick_size,
        kill_file=kill_file,
        monitor=monitor,
        flatten_timeout=limits.flatten_timeout,
        flatten_poll_interval=limits.flatten_poll_interval,
        flatten_max_adverse_ticks=limits.flatten_max_adverse_ticks,
        clock=engine_clock.monotonic,
        sleep=engine_clock.sleep,
        now_ms=lambda: max(1, int(now().timestamp() * 1000)),
    )
    return PaperRunner(
        engine=engine,
        broker=broker,
        cache=cache,
        book=SessionMinuteBook(MinuteBook(contract.instrument_token)),
        strategy=strategy,
        journal=journal,
        run_id=run_id,
        monitor=monitor,
        kill_file=kill_file,
        account=gate,
        paper=limits,
        now=now,
        sleep=sleep,
        instrument_token=contract.instrument_token,
        quantity=contract.lot_size * settings.paper_max_lots,
        latency_bars=settings.latency_bars,
        tick_size=contract.tick_size,
        slippage_ticks=settings.slippage_ticks,
        limits_record=record,
        pace=pace,
        charges=charges,
        expiry_dates=expiry_dates,
    )
