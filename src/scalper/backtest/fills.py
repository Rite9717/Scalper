"""Limit-order fills.

A candle has no queue, so a buy limit fills only when the bar trades strictly
below the limit. A touch, where the low equals the limit, is not a fill.
Tick data can fill at the limit when the print and the displayed size both
cover the order. Slippage is an adverse number of ticks added after that.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

from scalper.data.models import Candle, NormalizedQuote


def on_tick(price: Decimal, tick_size: Decimal) -> Decimal:
    if tick_size <= 0:
        raise ValueError("tick size must be positive")
    return price.quantize(tick_size, rounding=ROUND_HALF_UP)


def _slip(tick_size: Decimal, slippage_ticks: int) -> Decimal:
    if slippage_ticks < 0:
        raise ValueError("slippage_ticks cannot be negative")
    return tick_size * slippage_ticks


@dataclass(frozen=True)
class RestingLimit:
    side: str
    limit_price: Decimal
    quantity: int
    active_after: datetime


def candle_limit_price(
    bar: Candle,
    *,
    side: str,
    limit_price: Decimal,
    tick_size: Decimal,
    slippage_ticks: int,
) -> Decimal | None:
    slip = _slip(tick_size, slippage_ticks)
    if side == "buy":
        if bar.low >= limit_price:
            return None
        slipped = on_tick(limit_price + slip, tick_size)
        if bar.high >= slipped:
            return slipped
        return on_tick(limit_price, tick_size)
    if side == "sell":
        if bar.high <= limit_price:
            return None
        slipped = on_tick(limit_price - slip, tick_size)
        if bar.low <= slipped:
            return slipped
        return on_tick(limit_price, tick_size)
    raise ValueError("side must be buy or sell")


def quote_limit_filled(
    quote: NormalizedQuote,
    order: RestingLimit,
) -> bool:
    """True when a later print can fill a resting limit."""
    if quote.received_at < order.active_after:
        return False
    if order.side == "buy":
        if quote.last_price < order.limit_price:
            return True
        if quote.last_price != order.limit_price or quote.last_quantity < order.quantity:
            return False
        available = sum(
            level.quantity
            for level in quote.depth
            if level.side == "sell" and level.price == order.limit_price
        )
        return available >= order.quantity
    if order.side == "sell":
        if quote.last_price > order.limit_price:
            return True
        if quote.last_price != order.limit_price or quote.last_quantity < order.quantity:
            return False
        available = sum(
            level.quantity
            for level in quote.depth
            if level.side == "buy" and level.price == order.limit_price
        )
        return available >= order.quantity
    raise ValueError("side must be buy or sell")


def adverse_exit_price(
    *,
    side: str,
    trigger: Decimal,
    bar_open: Decimal,
    tick_size: Decimal,
    slippage_ticks: int,
    gap_through: bool,
) -> Decimal:
    """Stop and flatten prices. A gap through the stop fills at the open."""
    slip = _slip(tick_size, slippage_ticks)
    base = bar_open if gap_through else trigger
    if side == "sell":
        return on_tick(base - slip, tick_size)
    if side == "buy":
        return on_tick(base + slip, tick_size)
    raise ValueError("side must be buy or sell")
