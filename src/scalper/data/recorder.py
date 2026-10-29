"""Consume bridged ticks and store quotes, depth, and minute candles."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from scalper.clock import ensure_ist
from scalper.data.cache import QuoteCache
from scalper.data.candles import MinuteBook
from scalper.data.models import NormalizedQuote
from scalper.data.normalize import QuoteRejected, normalize_full_tick
from scalper.data.writer import MarketWriter


@dataclass
class RecorderStats:
    stored: int = 0
    rejected: int = 0
    candles: int = 0


def monotonic_received_at(previous: datetime | None, current: datetime) -> datetime:
    """Keep the primary key unique when two ticks share a timestamp."""
    moment = ensure_ist(current)
    if previous is not None and moment <= previous:
        return previous + timedelta(microseconds=1)
    return moment


async def consume_ticks(
    queue: asyncio.Queue,
    *,
    instrument_token: int,
    tick_size: Decimal,
    writer: MarketWriter,
    cache: QuoteCache,
    stop: asyncio.Event,
    book: MinuteBook | None = None,
) -> RecorderStats:
    stats = RecorderStats()
    candles = book or MinuteBook(instrument_token)
    previous: datetime | None = None
    while True:
        if stop.is_set() and queue.empty():
            break
        try:
            raw = await asyncio.wait_for(queue.get(), timeout=0.05)
        except TimeoutError:
            continue
        if not isinstance(raw, dict):
            stats.rejected += 1
            continue
        received_at = monotonic_received_at(previous, _received_at(raw))
        try:
            quote = normalize_full_tick(
                raw,
                tick_size=tick_size,
                instrument_token=instrument_token,
                received_at=received_at,
            )
        except QuoteRejected:
            stats.rejected += 1
            continue
        quote = _with_received_at(quote, received_at)
        previous = quote.received_at
        await writer.write_quote(quote)
        await cache.put(quote)
        finished = candles.add(quote)
        if finished is not None:
            await writer.write_candle(finished)
            stats.candles += 1
        stats.stored += 1
    tail = candles.flush()
    if tail is not None:
        await writer.write_candle(tail)
        stats.candles += 1
    await writer.flush()
    return stats


def _received_at(raw: dict) -> datetime:
    from scalper.clock import now_ist

    value = raw.get("received_at")
    if isinstance(value, datetime):
        return value
    return now_ist()


def _with_received_at(quote: NormalizedQuote, received_at: datetime) -> NormalizedQuote:
    if quote.received_at == received_at:
        return quote
    return NormalizedQuote(
        instrument_token=quote.instrument_token,
        received_at=received_at,
        exchange_ts=quote.exchange_ts,
        last_price=quote.last_price,
        last_quantity=quote.last_quantity,
        volume=quote.volume,
        average_price=quote.average_price,
        oi=quote.oi,
        open=quote.open,
        high=quote.high,
        low=quote.low,
        close=quote.close,
        depth=quote.depth,
    )
