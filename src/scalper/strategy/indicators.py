"""Decimal indicators used by the entry rules."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal

from scalper.clock import ensure_ist
from scalper.data.models import Candle


def ema(values: Sequence[Decimal], period: int) -> list[Decimal | None]:
    if period < 1:
        raise ValueError("ema period must be at least 1")
    line: list[Decimal | None] = [None] * len(values)
    if len(values) < period:
        return line
    seed = sum(values[:period], Decimal("0")) / Decimal(period)
    line[period - 1] = seed
    alpha = Decimal(2) / Decimal(period + 1)
    previous = seed
    for index in range(period, len(values)):
        previous = (values[index] - previous) * alpha + previous
        line[index] = previous
    return line


def wilder_atr(bars: Sequence[Candle], period: int) -> list[Decimal | None]:
    if period < 1:
        raise ValueError("atr period must be at least 1")
    line: list[Decimal | None] = [None] * len(bars)
    if len(bars) <= period:
        return line
    true_ranges: list[Decimal] = []
    for index in range(1, len(bars)):
        previous_close = bars[index - 1].close
        true_range = max(
            bars[index].high - bars[index].low,
            abs(bars[index].high - previous_close),
            abs(bars[index].low - previous_close),
        )
        true_ranges.append(true_range)
    if len(true_ranges) < period:
        return line
    seed = sum(true_ranges[:period], Decimal("0")) / Decimal(period)
    line[period] = seed
    previous = seed
    for offset in range(period, len(true_ranges)):
        previous = (previous * Decimal(period - 1) + true_ranges[offset]) / Decimal(period)
        line[offset + 1] = previous
    return line


def session_vwap(bars: Sequence[Candle]) -> list[Decimal | None]:
    line: list[Decimal | None] = [None] * len(bars)
    day = None
    cumulative_value = Decimal("0")
    cumulative_volume = Decimal("0")
    last: Decimal | None = None
    for index, bar in enumerate(bars):
        current = ensure_ist(bar.bucket_start).date()
        if current != day:
            day = current
            cumulative_value = Decimal("0")
            cumulative_volume = Decimal("0")
            last = None
        typical = (bar.high + bar.low + bar.close) / Decimal(3)
        cumulative_value += typical * Decimal(bar.volume)
        cumulative_volume += Decimal(bar.volume)
        if cumulative_volume > 0:
            last = cumulative_value / cumulative_volume
        line[index] = last
    return line


def floor_five_minutes(moment: datetime) -> datetime:
    local = ensure_ist(moment)
    minute = (local.minute // 5) * 5
    return local.replace(minute=minute, second=0, microsecond=0)


def completed_five_minute_bars(bars: Sequence[Candle]) -> list[Candle]:
    """Five-minute bars whose bucket has ended before the latest one-minute bar."""
    if not bars:
        return []
    current = floor_five_minutes(bars[-1].bucket_start)
    grouped: list[list[Candle]] = []
    for bar in bars:
        bucket = floor_five_minutes(bar.bucket_start)
        if bucket == current:
            break
        if not grouped or floor_five_minutes(grouped[-1][-1].bucket_start) != bucket:
            grouped.append([bar])
        else:
            grouped[-1].append(bar)
    completed: list[Candle] = []
    for group in grouped:
        completed.append(
            Candle(
                instrument_token=group[0].instrument_token,
                bucket_start=floor_five_minutes(group[0].bucket_start),
                interval="5minute",
                open=group[0].open,
                high=max(item.high for item in group),
                low=min(item.low for item in group),
                close=group[-1].close,
                volume=sum(item.volume for item in group),
                oi=group[-1].oi,
                source="aggregated",
            )
        )
    return completed
