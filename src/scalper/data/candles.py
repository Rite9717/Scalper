"""Build 1-minute candles from our own ticks.

Kite's volume field is the cumulative volume for the day. The bar volume is
the increase since the previous stored tick. The first tick of a recording
adds no volume, because the earlier part of the day was not observed.
"""

from __future__ import annotations

from datetime import datetime

from scalper.clock import ensure_ist
from scalper.data.models import Candle, NormalizedQuote


def floor_minute(moment: datetime) -> datetime:
    local = ensure_ist(moment)
    return local.replace(second=0, microsecond=0)


class MinuteBook:
    def __init__(self, instrument_token: int) -> None:
        self._token = instrument_token
        self._start: datetime | None = None
        self._open = None
        self._high = None
        self._low = None
        self._close = None
        self._volume = 0
        self._oi: int | None = None
        self._previous_cumulative: int | None = None

    def add(self, quote: NormalizedQuote) -> Candle | None:
        if quote.instrument_token != self._token:
            raise ValueError("candle book received a different instrument")
        minute = floor_minute(quote.received_at)
        delta = self._volume_delta(quote.volume)
        if self._start is None:
            self._open_bucket(minute, quote, delta)
            return None
        if minute < self._start:
            raise ValueError("tick is older than the open candle")
        if minute == self._start:
            self._fold(quote, delta)
            return None
        finished = self._finish()
        self._open_bucket(minute, quote, delta)
        return finished

    def flush(self) -> Candle | None:
        if self._start is None:
            return None
        finished = self._finish()
        self._start = None
        return finished

    def _volume_delta(self, cumulative: int) -> int:
        previous = self._previous_cumulative
        self._previous_cumulative = cumulative
        if previous is None:
            return 0
        delta = cumulative - previous
        if delta < 0:
            return cumulative
        return delta

    def _open_bucket(self, minute: datetime, quote: NormalizedQuote, delta: int) -> None:
        self._start = minute
        self._open = quote.last_price
        self._high = quote.last_price
        self._low = quote.last_price
        self._close = quote.last_price
        self._volume = delta
        self._oi = quote.oi

    def _fold(self, quote: NormalizedQuote, delta: int) -> None:
        assert self._high is not None and self._low is not None
        self._high = max(self._high, quote.last_price)
        self._low = min(self._low, quote.last_price)
        self._close = quote.last_price
        self._volume += delta
        self._oi = quote.oi

    def _finish(self) -> Candle:
        assert self._start is not None
        assert self._open is not None and self._high is not None
        assert self._low is not None and self._close is not None
        return Candle(
            instrument_token=self._token,
            bucket_start=self._start,
            interval="minute",
            open=self._open,
            high=self._high,
            low=self._low,
            close=self._close,
            volume=self._volume,
            oi=self._oi,
            source="ticks",
        )
