"""Persist quotes, depth, and candles.

The Postgres writer batches rows. A memory writer is the stand-in for tests.
"""

from __future__ import annotations

from typing import Protocol

from scalper.data.models import Candle, NormalizedQuote
from scalper.kite.instruments import NiftyFutureContract

_INSERT_CONTRACT = """
INSERT INTO recorded_contracts (
    instrument_token, tradingsymbol, expiry, tick_size, lot_size, exchange
) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (instrument_token, tradingsymbol) DO NOTHING
"""

_INSERT_TICK = """
INSERT INTO ticks (
    received_at, instrument_token, last_price, last_quantity, volume,
    average_price, oi, open, high, low, close, exchange_ts
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (instrument_token, received_at) DO NOTHING
RETURNING received_at
"""

_INSERT_DEPTH = """
INSERT INTO depth_levels (
    received_at, instrument_token, side, level, price, quantity, orders
) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (instrument_token, received_at, side, level) DO NOTHING
"""

_INSERT_CANDLE = """
INSERT INTO candles (
    instrument_token, bucket_start, interval, open, high, low, close, volume, oi, source
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (instrument_token, interval, bucket_start, source) DO UPDATE
SET open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    oi = EXCLUDED.oi
"""


class MarketWriter(Protocol):
    async def ensure_contract(self, contract: NiftyFutureContract) -> None: ...

    async def write_quote(self, quote: NormalizedQuote) -> None: ...

    async def write_candle(self, candle: Candle) -> None: ...

    async def flush(self) -> None: ...


class MemoryWriter:
    def __init__(self) -> None:
        self.contracts: list[NiftyFutureContract] = []
        self.quotes: list[NormalizedQuote] = []
        self.candles: list[Candle] = []

    async def ensure_contract(self, contract: NiftyFutureContract) -> None:
        self.contracts.append(contract)

    async def write_quote(self, quote: NormalizedQuote) -> None:
        self.quotes.append(quote)

    async def write_candle(self, candle: Candle) -> None:
        self.candles.append(candle)

    async def flush(self) -> None:
        return None


class PostgresWriter:
    def __init__(self, pool: object, *, batch_size: int = 50) -> None:
        self._pool = pool
        self._batch_size = batch_size
        self._quotes: list[NormalizedQuote] = []
        self._candles: list[Candle] = []

    async def ensure_contract(self, contract: NiftyFutureContract) -> None:
        async with self._pool.acquire() as conn:  # type: ignore[attr-defined]
            await conn.execute(
                _INSERT_CONTRACT,
                contract.instrument_token,
                contract.tradingsymbol,
                contract.expiry,
                contract.tick_size,
                contract.lot_size,
                contract.exchange,
            )

    async def write_quote(self, quote: NormalizedQuote) -> None:
        self._quotes.append(quote)
        if len(self._quotes) >= self._batch_size:
            await self.flush()

    async def write_candle(self, candle: Candle) -> None:
        self._candles.append(candle)

    async def flush(self) -> None:
        quotes = self._quotes
        candles = self._candles
        self._quotes = []
        self._candles = []
        if not quotes and not candles:
            return
        async with self._pool.acquire() as conn:  # type: ignore[attr-defined]
            async with conn.transaction():
                for quote in quotes:
                    inserted = await conn.fetchrow(
                        _INSERT_TICK,
                        quote.received_at,
                        quote.instrument_token,
                        quote.last_price,
                        quote.last_quantity,
                        quote.volume,
                        quote.average_price,
                        quote.oi,
                        quote.open,
                        quote.high,
                        quote.low,
                        quote.close,
                        quote.exchange_ts,
                    )
                    if inserted is None:
                        continue
                    await conn.executemany(
                        _INSERT_DEPTH,
                        [
                            (
                                quote.received_at,
                                quote.instrument_token,
                                level.side,
                                level.level,
                                level.price,
                                level.quantity,
                                level.orders,
                            )
                            for level in quote.depth
                        ],
                    )
                for candle in candles:
                    await conn.execute(
                        _INSERT_CANDLE,
                        candle.instrument_token,
                        candle.bucket_start,
                        candle.interval,
                        candle.open,
                        candle.high,
                        candle.low,
                        candle.close,
                        candle.volume,
                        candle.oi,
                        candle.source,
                    )
