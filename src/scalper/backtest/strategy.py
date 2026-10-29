"""Strategy boundary used by the backtest, and later by paper and live trading.

A strategy sees only the bars closed so far. It returns a limit entry with the
stop, target, and time stop already chosen. It does not know whether the next
bar will fill it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from scalper.backtest.fills import on_tick
from scalper.data.models import Candle


@dataclass(frozen=True)
class EntrySignal:
    side: str
    limit_price: Decimal
    stop_price: Decimal
    target_price: Decimal
    time_stop_bars: int
    regime: str = "unspecified"


@dataclass(frozen=True)
class MarketState:
    bars: tuple[Candle, ...]
    tick_size: Decimal
    now: datetime
    daily_pnl: Decimal = Decimal("0")
    loss_streak: int = 0
    bars_since_last_loss: int | None = None
    spread_ticks: int | None = None
    best_depth: int | None = None
    expiry_day: bool = False


class Strategy(Protocol):
    name: str

    def on_bar(self, state: MarketState) -> EntrySignal | None: ...


class PlumbingStrategy:
    """Buys one tick under an up bar. This is a wiring check, not an edge."""

    name = "plumbing"

    def on_bar(self, state: MarketState) -> EntrySignal | None:
        bar = state.bars[-1]
        if bar.close <= bar.open:
            return None
        limit = on_tick(bar.close - state.tick_size, state.tick_size)
        distance = state.tick_size * 20
        return EntrySignal(
            side="buy",
            limit_price=limit,
            stop_price=on_tick(limit - distance, state.tick_size),
            target_price=on_tick(limit + distance, state.tick_size),
            time_stop_bars=5,
            regime="plumbing",
        )
