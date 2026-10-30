"""Read candles that `history` or the recorder already stored."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import asyncpg

from scalper.clock import ensure_ist
from scalper.data.models import Candle


def candle_from_row(row: object) -> Candle:
    mapping = row if isinstance(row, dict) else dict(row)
    oi = mapping["oi"]
    return Candle(
        instrument_token=int(mapping["instrument_token"]),
        bucket_start=ensure_ist(mapping["bucket_start"]),
        interval=str(mapping["interval"]),
        open=Decimal(mapping["open"]),
        high=Decimal(mapping["high"]),
        low=Decimal(mapping["low"]),
        close=Decimal(mapping["close"]),
        volume=int(mapping["volume"]),
        oi=None if oi is None else int(oi),
        source=str(mapping["source"]),
    )


def choose_candles(candles: list[Candle]) -> list[Candle]:
    """One bar per minute. A bar built from our own ticks replaces a downloaded bar."""
    chosen: dict[datetime, Candle] = {}
    for candle in candles:
        current = chosen.get(candle.bucket_start)
        if current is None or (candle.source == "ticks" and current.source != "ticks"):
            chosen[candle.bucket_start] = candle
    return [chosen[moment] for moment in sorted(chosen)]


async def fetch_stored_candles(
    dsn: str,
    *,
    instrument_token: int,
    interval: str,
    start: datetime,
    end: datetime,
) -> list[Candle]:
    """Return one candle per bucket. A bar built from our ticks wins over a Kite bar."""
    connection = await asyncpg.connect(dsn, timeout=10)
    try:
        rows = await connection.fetch(
            """
            SELECT instrument_token, bucket_start, interval, open, high, low, close, volume, oi, source
            FROM candles
            WHERE instrument_token = $1
              AND interval = $2
              AND bucket_start >= $3
              AND bucket_start < $4
            ORDER BY bucket_start, source
            """,
            instrument_token,
            interval,
            ensure_ist(start),
            ensure_ist(end),
        )
    finally:
        await connection.close()
    return choose_candles([candle_from_row(row) for row in rows])
