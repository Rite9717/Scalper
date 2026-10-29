"""Live recording and historical download. Both require a stored Kite session."""

from __future__ import annotations

import asyncio

import asyncpg
import redis.asyncio as redis_async

from scalper.clock import now_ist
from scalper.config import Settings
from scalper.data.bridge import AsyncTickerBridge
from scalper.data.cache import RedisQuoteCache
from scalper.data.historical import fetch_historical_candles
from scalper.data.migrate import apply_migrations
from scalper.data.recorder import consume_ticks
from scalper.data.ticker import connect_kite_ticker
from scalper.data.writer import PostgresWriter
from scalper.kite.http import kite_client
from scalper.kite.instruments import NiftyFutureContract
from scalper.kite.session import assess_session, load_session, require_usable_session


class DataStoreError(Exception):
    def __init__(self, store: str, error_type: str) -> None:
        self.store = store
        self.error_type = error_type
        super().__init__(store)


async def record_until_stopped(settings: Settings, contract: NiftyFutureContract, stop: asyncio.Event) -> dict[str, int]:
    dsn = settings.database_url.get_secret_value()
    await apply_migrations(dsn)
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2, timeout=10)
    redis = redis_async.from_url(settings.redis_url.get_secret_value())
    try:
        try:
            await redis.ping()
        except Exception as exc:
            raise DataStoreError("redis", type(exc).__name__) from None
        session = _usable_session(settings)
        writer = PostgresWriter(pool)
        await writer.ensure_contract(contract)
        queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
        bridge = AsyncTickerBridge(asyncio.get_running_loop(), queue)
        ticker = connect_kite_ticker(
            api_key=session.api_key,
            access_token=session.access_token.get_secret_value(),
            bridge=bridge,
            instrument_token=contract.instrument_token,
        )
        try:
            stats = await consume_ticks(
                queue,
                instrument_token=contract.instrument_token,
                tick_size=contract.tick_size,
                writer=writer,
                cache=RedisQuoteCache(redis),
                stop=stop,
            )
        finally:
            close = getattr(ticker, "close", None)
            if close is not None:
                close()
        return {"stored": stats.stored, "rejected": stats.rejected, "candles": stats.candles, "dropped": bridge.dropped}
    finally:
        await redis.aclose()
        await pool.close()


async def download_history(
    settings: Settings,
    contract: NiftyFutureContract,
    *,
    start,
    end,
    interval: str,
) -> int:
    dsn = settings.database_url.get_secret_value()
    await apply_migrations(dsn)
    session = _usable_session(settings)
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2, timeout=10)
    try:
        writer = PostgresWriter(pool)
        await writer.ensure_contract(contract)
        async with kite_client() as client:
            candles = await fetch_historical_candles(
                client,
                api_key=session.api_key,
                access_token=session.access_token.get_secret_value(),
                instrument_token=contract.instrument_token,
                interval=interval,
                start=start,
                end=end,
                tick_size=contract.tick_size,
            )
        for candle in candles:
            await writer.write_candle(candle)
        await writer.flush()
        return len(candles)
    finally:
        await pool.close()


def _usable_session(settings: Settings):
    session = load_session(settings.session_path)
    health = assess_session(
        session,
        now=now_ist(),
        expected_user_id=settings.expected_user_id,
        api_key=settings.kite_api_key.get_secret_value(),
    )
    require_usable_session(health)
    return session
