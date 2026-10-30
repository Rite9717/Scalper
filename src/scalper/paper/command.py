"""Wire `scalper paper` to the live ticker and the paper broker.

This module does not construct KiteBroker and does not call an order endpoint.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from uuid import uuid4

import asyncpg

import httpx

from scalper.clock import now_ist, today_ist
from scalper.config import Settings
from scalper.data.bridge import AsyncTickerBridge, BridgeSignal
from scalper.data.migrate import apply_migrations
from scalper.data.models import NormalizedQuote
from scalper.data.normalize import QuoteRejected, normalize_full_tick
from scalper.data.ticker import close_ticker, connect_kite_ticker
from scalper.health import HealthServer
from scalper.kite.instruments import is_nifty_future, parse_instruments, select_current_nifty_future
from scalper.kite.session import load_session
from scalper.monitor.alerts import Monitor, RecordingSink, TelegramSink
from scalper.monitor.metrics import build_metrics
from scalper.paper.journal import PostgresJournal
from scalper.paper.limits import PaperLimits, resolved_limits
from scalper.paper.replay import replay_recorded_day
from scalper.paper.runner import PaperRunner, install_stop_signals
from scalper.paper.session import build_paper_runner
from scalper.paper.startup import refuse_to_start
from scalper.risk.killfile import kill_path
from scalper.strategy.registry import build_strategy


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
    instruments = parse_instruments(master.read_text(encoding="utf-8"))
    contract = select_current_nifty_future(instruments, day)
    expiries = {row.expiry for row in instruments if is_nifty_future(row) and row.expiry is not None}
    await apply_migrations(settings.database_url.get_secret_value())
    connection = await asyncpg.connect(settings.database_url.get_secret_value(), timeout=5)
    session = load_session(settings.session_path)
    metrics = build_metrics()
    monitor = Monitor(metrics, _sink(settings))
    runner = build_paper_runner(
        settings=settings,
        strategy=build_strategy(strategy_name),
        contract=contract,
        journal=PostgresJournal(connection),
        run_id=uuid4(),
        monitor=monitor,
        kill_file=killed,
        now=now_ist,
        sleep=asyncio.sleep,
        pace=True,
        expiry_dates=expiries,
        paper=paper,
    )
    health = HealthServer(settings, metrics=metrics)
    await health.start()
    loop = asyncio.get_running_loop()
    bridge = AsyncTickerBridge(loop, asyncio.Queue())
    install_stop_signals(loop, runner)
    ticker = connect_kite_ticker(
        api_key=session.api_key,
        access_token=session.access_token.get_secret_value(),
        bridge=bridge,
        instrument_token=contract.instrument_token,
    )
    feed = _KiteQuotes(bridge, contract.tick_size, contract.instrument_token)
    await serve_paper(runner, feed, ticker, health, connection)
    return 0


async def serve_paper(runner: PaperRunner, feed, ticker: object, health: HealthServer, connection) -> None:
    """Run the loop, then close the ticker before the database connection."""
    try:
        await runner.run(feed)
    except BaseException as exc:
        await runner.fail_closed(exc)
        raise
    finally:
        await finish_paper_session(ticker, health, connection)


async def finish_paper_session(ticker: object, health: HealthServer, connection) -> None:
    try:
        close_ticker(ticker)
    finally:
        try:
            await health.stop()
        finally:
            await connection.close()


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
        from scalper.paper.runner import FeedDrop

        raw = await self._bridge.queue.get()
        if isinstance(raw, BridgeSignal):
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
