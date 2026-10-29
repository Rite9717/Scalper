"""Entry filters shared by every strategy.

Spread and depth are enforced when the market state has them. A candle file
has neither, so those two checks stay idle unless require_book is set.
The acceptance report says when they were not applied.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from decimal import Decimal

from scalper.backtest.strategy import MarketState, Strategy, EntrySignal
from scalper.clock import ensure_ist
from scalper.strategy.indicators import wilder_atr


@dataclass(frozen=True)
class FilterConfig:
    max_spread_ticks: int = 1
    min_depth: int = 1
    require_book: bool = False
    atr_period: int = 14
    atr_min: Decimal | None = None
    atr_max: Decimal | None = None
    skip_open_minutes: int = 5
    skip_close_minutes: int = 15
    session_open: time = time(9, 15)
    session_close: time = time(15, 30)
    expiry_flat_after: time = time(12, 30)
    daily_loss_limit: Decimal = Decimal("5000")
    loss_streak_limit: int = 3
    cooldown_bars: int = 5
    event_windows: tuple[tuple[datetime, datetime], ...] = ()


def rejection_reason(state: MarketState, config: FilterConfig) -> str | None:
    moment = ensure_ist(state.now)
    if _minutes_from(config.session_open, moment) < config.skip_open_minutes:
        return "opening window"
    if _minutes_until(config.session_close, moment) <= config.skip_close_minutes:
        return "closing window"
    if state.expiry_day and moment.time() >= config.expiry_flat_after:
        return "expiry afternoon"
    for start, end in config.event_windows:
        if ensure_ist(start) <= moment < ensure_ist(end):
            return "scheduled event"
    if state.daily_pnl <= -config.daily_loss_limit:
        return "daily loss limit"
    if (
        state.loss_streak >= config.loss_streak_limit
        and state.bars_since_last_loss is not None
        and state.bars_since_last_loss < config.cooldown_bars
    ):
        return "loss-streak cooldown"
    if config.atr_min is not None or config.atr_max is not None:
        atrs = wilder_atr(state.bars, config.atr_period)
        latest = atrs[-1] if atrs else None
        if latest is None:
            return "atr not ready"
        if config.atr_min is not None and latest < config.atr_min:
            return "atr below minimum"
        if config.atr_max is not None and latest > config.atr_max:
            return "atr above maximum"
    if state.spread_ticks is None or state.best_depth is None:
        if config.require_book:
            return "quote book unavailable"
        return None
    if state.spread_ticks > config.max_spread_ticks:
        return "spread too wide"
    if state.best_depth < config.min_depth:
        return "depth too thin"
    return None


class FilteredStrategy:
    def __init__(self, inner: Strategy, config: FilterConfig | None = None) -> None:
        self.inner = inner
        self.config = config or FilterConfig()
        self.name = inner.name
        self.blocked = 0

    def on_bar(self, state: MarketState) -> EntrySignal | None:
        if rejection_reason(state, self.config) is not None:
            self.blocked += 1
            return None
        signal = self.inner.on_bar(state)
        if signal is None:
            return None
        if rejection_reason(state, self.config) is not None:
            self.blocked += 1
            return None
        return signal


def _minutes_from(origin: time, moment: datetime) -> int:
    return (moment.hour * 60 + moment.minute) - (origin.hour * 60 + origin.minute)


def _minutes_until(deadline: time, moment: datetime) -> int:
    return (deadline.hour * 60 + deadline.minute) - (moment.hour * 60 + moment.minute)
