"""Tick-level stop, target, and time stop. Fills stay strictly through the limit."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from scalper.backtest.fills import on_tick
from scalper.execution.states import ManagedOrder


def strict_through(*, side: str, price: Decimal, limit_price: Decimal) -> bool:
    """A buy fills only below the limit. A sell fills only above it."""
    if side == "buy":
        return price < limit_price
    if side == "sell":
        return price > limit_price
    raise ValueError("side must be buy or sell")


def entry_expired(*, forming_index: int, last_live: int) -> bool:
    """Same expiry as the backtest: the bar after last_live does not fill."""
    return forming_index > last_live


def worse_limit(side: str, price: Decimal, tick_size: Decimal) -> Decimal:
    step = on_tick(tick_size, tick_size)
    if side == "sell":
        return on_tick(price - step, tick_size)
    if side == "buy":
        return on_tick(price + step, tick_size)
    raise ValueError("side must be buy or sell")


def inside_adverse_bound(
    *,
    side: str,
    price: Decimal,
    anchor: Decimal,
    tick_size: Decimal,
    max_ticks: int,
) -> bool:
    span = tick_size * max_ticks
    if side == "sell":
        return price >= anchor - span
    if side == "buy":
        return price <= anchor + span
    raise ValueError("side must be buy or sell")


@dataclass
class OpenPosition:
    side: str
    quantity: int
    fill_index: int
    fill_price: Decimal
    stop_price: Decimal
    target_price: Decimal
    time_stop_bars: int
    tag: str


@dataclass(frozen=True)
class ExitTrigger:
    reason: str
    price: Decimal


def position_trigger(position: OpenPosition, price: Decimal, forming_index: int) -> ExitTrigger | None:
    """Stop is taken before target. Time stop is the bar count from the fill."""
    exit_side_is_sell = position.side == "buy"
    if exit_side_is_sell:
        if price <= position.stop_price:
            return ExitTrigger("stop", position.stop_price)
        if price > position.target_price:
            return ExitTrigger("target", position.target_price)
    else:
        if price >= position.stop_price:
            return ExitTrigger("stop", position.stop_price)
        if price < position.target_price:
            return ExitTrigger("target", position.target_price)
    if forming_index >= position.fill_index + position.time_stop_bars:
        return ExitTrigger("time", price)
    return None


@dataclass
class WorkingExit:
    order: ManagedOrder
    reason: str
    trigger_at: datetime
    trigger_price: Decimal
    anchor: Decimal
    started_at: datetime
    timestamp_ms: int
