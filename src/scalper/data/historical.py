"""Kite historical candles.

The documented intervals stop at one minute. Requests are capped at 3 per
second. Each window stays inside the per-interval day limit so one call does
not ask for more than Kite will return.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from decimal import Decimal

import httpx

from scalper.clock import ensure_ist
from scalper.data.models import Candle
from scalper.data.prices import PriceError, rupees_to_tick
from scalper.kite.api_facts import KITE_HISTORICAL_INTERVALS, KITE_HISTORICAL_REQUESTS_PER_SECOND
from scalper.kite.auth import authenticated_headers
from scalper.kite.http import KiteError, parse_object_data

# Day caps used by Kite for one historical request. The chunk size is one day
# shorter so an inclusive from/to pair stays inside the cap.
KITE_HISTORICAL_MAX_DAYS = {
    "minute": 60,
    "3minute": 100,
    "5minute": 100,
    "10minute": 100,
    "15minute": 200,
    "30minute": 200,
    "60minute": 400,
    "day": 2000,
}


class IntervalLimiter:
    def __init__(
        self,
        requests_per_second: float,
        *,
        clock: Callable[[], float],
        sleep: Callable[[float], Awaitable[None]],
    ) -> None:
        if requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        self._interval = 1.0 / requests_per_second
        self._clock = clock
        self._sleep = sleep
        self._next = 0.0

    async def acquire(self) -> None:
        now = self._clock()
        delay = self._next - now
        if delay > 0:
            await self._sleep(delay)
            now = self._clock()
        self._next = now + self._interval


def historical_limiter() -> IntervalLimiter:
    return IntervalLimiter(
        KITE_HISTORICAL_REQUESTS_PER_SECOND,
        clock=asyncio.get_running_loop().time,
        sleep=asyncio.sleep,
    )


def iter_windows(start: datetime, end: datetime, interval: str):
    if interval not in KITE_HISTORICAL_INTERVALS:
        raise ValueError(f"interval must be one of: {', '.join(KITE_HISTORICAL_INTERVALS)}")
    if end <= start:
        raise ValueError("historical end must be after start")
    max_days = KITE_HISTORICAL_MAX_DAYS[interval]
    width = timedelta(days=max_days - 1)
    cursor = ensure_ist(start)
    stop = ensure_ist(end)
    while cursor < stop:
        nxt = min(stop, cursor + width)
        yield cursor, nxt
        if nxt == cursor:
            break
        cursor = nxt


def _kite_clock(moment: datetime) -> str:
    return ensure_ist(moment).strftime("%Y-%m-%d %H:%M:%S")


def parse_historical_candles(
    payload: dict,
    *,
    instrument_token: int,
    interval: str,
    tick_size: Decimal,
) -> list[Candle]:
    rows = payload.get("candles")
    if not isinstance(rows, list):
        raise KiteError("historical response did not contain candles")
    candles: list[Candle] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 6:
            raise KiteError("historical candle row is too short")
        bucket = _parse_bucket(row[0])
        try:
            open_, high, low, close = (
                rupees_to_tick(float(row[1]), tick_size),
                rupees_to_tick(float(row[2]), tick_size),
                rupees_to_tick(float(row[3]), tick_size),
                rupees_to_tick(float(row[4]), tick_size),
            )
        except (TypeError, ValueError, PriceError) as exc:
            raise KiteError("historical candle price was not a number") from exc
        volume = row[5]
        if type(volume) is not int or volume < 0:
            raise KiteError("historical candle volume was not an int")
        oi = row[6] if len(row) > 6 else None
        if oi is not None and (type(oi) is not int or oi < 0):
            raise KiteError("historical candle oi was not an int")
        candles.append(
            Candle(
                instrument_token=instrument_token,
                bucket_start=bucket,
                interval=interval,
                open=open_,
                high=high,
                low=low,
                close=close,
                volume=volume,
                oi=oi,
                source="kite",
            )
        )
    return candles


def _parse_bucket(value: object) -> datetime:
    if not isinstance(value, str):
        raise KiteError("historical candle timestamp was not a string")
    text = value
    if len(text) >= 5 and (text[-5] in "+-") and text[-3] != ":":
        text = text[:-2] + ":" + text[-2:]
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise KiteError("historical candle timestamp was not ISO") from exc
    return ensure_ist(parsed)


async def fetch_historical_candles(
    client: httpx.AsyncClient,
    *,
    api_key: str,
    access_token: str,
    instrument_token: int,
    interval: str,
    start: datetime,
    end: datetime,
    tick_size: Decimal,
    limiter: IntervalLimiter | None = None,
) -> list[Candle]:
    gate = limiter or historical_limiter()
    candles: list[Candle] = []
    headers = authenticated_headers(api_key, access_token)
    for window_start, window_end in iter_windows(start, end, interval):
        await gate.acquire()
        response = await client.get(
            f"/instruments/historical/{instrument_token}/{interval}",
            params={
                "from": _kite_clock(window_start),
                "to": _kite_clock(window_end),
                "oi": 1,
            },
            headers=headers,
        )
        data = parse_object_data(response)
        candles.extend(
            parse_historical_candles(
                data,
                instrument_token=instrument_token,
                interval=interval,
                tick_size=tick_size,
            )
        )
    return candles
