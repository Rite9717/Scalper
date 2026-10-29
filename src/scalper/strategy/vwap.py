"""VWAP-band mean reversion. It stays idle unless the regime is ranging.

Ranging means the finished five-minute EMA has moved less than a multiple of
ATR. A long needs a bar that trades below the lower band and closes back above
it. The target is VWAP. This rule is configurable and is not a proven edge.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from scalper.backtest.fills import on_tick
from scalper.backtest.strategy import EntrySignal, MarketState
from scalper.strategy.indicators import completed_five_minute_bars, ema, session_vwap, wilder_atr


@dataclass(frozen=True)
class VwapReversionConfig:
    ema_period: int = 20
    slope_lookback: int = 3
    trend_slope_atr: Decimal = Decimal("0.5")
    band_atr: Decimal = Decimal("1.5")
    atr_period: int = 14
    stop_atr: Decimal = Decimal("0.5")
    time_stop_bars: int = 10


class VwapReversion:
    name = "vwap_reversion"

    def __init__(self, config: VwapReversionConfig | None = None) -> None:
        self.config = config or VwapReversionConfig()

    def on_bar(self, state: MarketState) -> EntrySignal | None:
        bars = state.bars
        config = self.config
        if not _is_ranging(bars, config):
            return None
        atrs = wilder_atr(bars, config.atr_period)
        vwaps = session_vwap(bars)
        atr = atrs[-1]
        vwap = vwaps[-1]
        if atr is None or atr <= 0 or vwap is None:
            return None
        band = config.band_atr * atr
        lower = vwap - band
        upper = vwap + band
        bar = bars[-1]
        risk = on_tick(config.stop_atr * atr, state.tick_size)
        if risk <= 0:
            return None
        if bar.low < lower and bar.close > lower and bar.close > bar.open and vwap > lower:
            limit = on_tick(lower, state.tick_size)
            target = on_tick(vwap, state.tick_size)
            stop = on_tick(limit - risk, state.tick_size)
            if not (stop < limit < target):
                return None
            return EntrySignal("buy", limit, stop, target, config.time_stop_bars, "range")
        if bar.high > upper and bar.close < upper and bar.close < bar.open and vwap < upper:
            limit = on_tick(upper, state.tick_size)
            target = on_tick(vwap, state.tick_size)
            stop = on_tick(limit + risk, state.tick_size)
            if not (target < limit < stop):
                return None
            return EntrySignal("sell", limit, stop, target, config.time_stop_bars, "range")
        return None


def _is_ranging(bars, config: VwapReversionConfig) -> bool:
    higher = completed_five_minute_bars(bars)
    if len(higher) <= config.ema_period + config.slope_lookback:
        return False
    line = ema([bar.close for bar in higher], config.ema_period)
    latest = line[-1]
    earlier = line[-1 - config.slope_lookback]
    atrs = wilder_atr(higher, min(config.atr_period, max(len(higher) - 1, 1)))
    atr = atrs[-1]
    if latest is None or earlier is None or atr is None or atr <= 0:
        return False
    return abs(latest - earlier) <= config.trend_slope_atr * atr
