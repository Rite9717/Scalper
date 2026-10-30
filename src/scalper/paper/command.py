"""Wire `scalper paper` to the live ticker and the paper broker.

This module does not construct KiteBroker and does not call an order endpoint.
"""

from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import asyncpg

from scalper.clock import now_ist, today_ist
from scalper.config import Settings
from scalper.data.bridge import AsyncTickerBridge
from scalper.data.cache import MemoryQuoteCache
from scalper.data.candles import MinuteBook
from scalper.data.migrate import apply_migrations
from scalper.data.models import NormalizedQuote
from scalper.data.normalize import QuoteRejected, normalize_full_tick
from scalper.data.ticker import connect_kite_ticker
from scalper.execution.engine import ExecutionEngine
from scalper.execution.paper import PaperBroker
from scalper.execution.snapshot import LiveSnapshotProvider
from scalper.health import HealthServer
from scalper.kite.instruments import parse_instruments, select_current_nifty_future
from scalper.kite.session import load_session
from scalper.monitor.alerts import Monitor, RecordingSink, TelegramSink
from scalper.monitor.metrics import build_metrics
from scalper.paper.bars import SessionMinuteBook
from scalper.paper.limits import PaperLimits, account_limits, resolved_limits
from scalper.paper.journal import PostgresJournal
from scalper.paper.runner import PaperRunner, install_stop_signals
from scalper.paper.startup import refuse_to_start
from scalper.risk.killfile import kill_path
from scalper.strategy.registry import build_strategy
import httpx


def instrument_master_path(settings: Settings, day) -> Path:
    return settings.instrument_archive_dir / f"nfo-{day.isoformat()}.csv"


async def run_paper(settings: Settings, strategy_name: str) -> int:
    paper = PaperLimits()
    record = resolved_limits(settings, paper)
    day = today_ist()
    master = instrument_master_path(settings, day)
    killed = kill_path(settings.data_dir)

    async def ping() -> None:
        connection = await asyncpg.connect(settings.database_url.get_secret_value(), timeout=5)
        try:
            await connection.execute("SELECT 1")
        finally:
            await connection.close()

    await refuse_to_start(
        kill_file=killed,
        session_path=settings.session_path,
        expected_user_id=settings.expected_user_id,
        api_key=settings.kite_api_key.get_secret_value(),
        now=now_ist(),
        instrument_master=master,
        database_ping=ping,
    )
    print(json.dumps(record, sort_keys=True))
    contract = select_current_nifty_future(parse_instruments(master.read_text(encoding="utf-8")), day)
    await apply_migrations(settings.database_url.get_secret_value())
    connection = await asyncpg.connect(settings.database_url.get_secret_value(), timeout=5)
    session = load_session(settings.session_path)
    metrics = build_metrics()
    sink = _sink(settings)
    monitor = Monitor(metrics, sink)
    broker = PaperBroker(
        contract.tick_size,
        contract.lot_size,
        tradingsymbol=contract.tradingsymbol,
        equity_cash=Decimal("1000000"),
    )
    cache = MemoryQuoteCache()
    provider = LiveSnapshotProvider(
        broker=broker,
        cache=cache,
        instrument_token=contract.instrument_token,
        tradingsymbol=contract.tradingsymbol,
        exchange=contract.exchange,
        kill_file=killed,
        live=False,
        fail_closed=True,
        limits=account_limits(settings, paper),
        schedule=None,
        tick_size=contract.tick_size,
    )
    engine = ExecutionEngine(
        broker,
        provider,
        tradingsymbol=contract.tradingsymbol,
        exchange=contract.exchange,
        lot_size=contract.lot_size,
        tick_size=contract.tick_size,
        kill_file=killed,
        monitor=monitor,
        flatten_timeout=paper.flatten_timeout,
        flatten_poll_interval=paper.flatten_poll_interval,
        flatten_max_adverse_ticks=paper.flatten_max_adverse_ticks,
    )
    runner = PaperRunner(
        engine=engine,
        broker=broker,
        cache=cache,
        book=SessionMinuteBook(MinuteBook(contract.instrument_token)),
        strategy=build_strategy(strategy_name),
        journal=PostgresJournal(connection),
        run_id=uuid4(),
        monitor=monitor,
        kill_file=killed,
        account=account_limits(settings, paper),
        paper=paper,
        now=now_ist,
        sleep=asyncio.sleep,
        instrument_token=contract.instrument_token,
        quantity=contract.lot_size * settings.paper_max_lots,
        latency_bars=settings.latency_bars,
        tick_size=contract.tick_size,
        limits_record=record,
    )
    health = HealthServer(settings, metrics=metrics)
    await health.start()
    loop = asyncio.get_running_loop()
    bridge = AsyncTickerBridge(loop, asyncio.Queue())
    install_stop_signals(loop, runner)
    connect_kite_ticker(
        api_key=session.api_key,
        access_token=session.access_token.get_secret_value(),
        bridge=bridge,
        instrument_token=contract.instrument_token,
    )
    feed = _KiteQuotes(bridge, contract.tick_size, contract.instrument_token)
    try:
        await runner.run(feed)
    finally:
        await health.stop()
        await connection.close()
    return 0


def _sink(settings: Settings):
    token = settings.telegram_bot_token.get_secret_value()
    if settings.alerts_enabled and token and settings.telegram_chat_id.strip():
        return TelegramSink(token, settings.telegram_chat_id.strip(), httpx.AsyncClient())
    return RecordingSink()


class _KiteQuotes:
    def __init__(self, bridge: AsyncTickerBridge, tick_size: Decimal, instrument_token: int) -> None:
        self._bridge = bridge
        self._tick_size = tick_size
        self._token = instrument_token

    def __aiter__(self) -> "_KiteQuotes":
        return self

    async def __anext__(self) -> NormalizedQuote | object:
        if self._bridge.closed.is_set() and self._bridge.queue.empty():
            self._bridge.closed.clear()
            from scalper.paper.runner import FeedDrop

            return FeedDrop()
        raw = await self._bridge.queue.get()
        if self._bridge.closed.is_set():
            self._bridge.closed.clear()
            from scalper.paper.runner import FeedDrop

            return FeedDrop()
        if not isinstance(raw, dict):
            return await self.__anext__()
        try:
            return normalize_full_tick(
                raw,
                tick_size=self._tick_size,
                instrument_token=self._token,
                received_at=now_ist(),
            )
        except QuoteRejected:
            return await self.__anext__()
