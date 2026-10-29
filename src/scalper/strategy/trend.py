"""Five-minute trend, VWAP side, then a one-minute pullback to EMA(20).

The confirmation bar must close back through the EMA in the trend direction
with volume at least the recent average. The limit sits on that EMA. This
rule is configurable and is not presented as a proven edge.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from scalper.backtest.fills import on_tick
from scalper.backtest.strategy import EntrySignal, MarketState
from scalper.strategy.indicators import completed_five_minute_bars, ema, session_vwap, wilder_atr


@dataclass(frozen=True)
class TrendPullbackConfig:
    ema_period: int = 20
    five_minute_ema_period: int = 20
    slope_lookback: int = 3
    volume_period: int = 20
    volume_multiple: Decimal = Decimal("1")
    atr_period: int = 14
    stop_atr: Decimal = Decimal("1")
    target_r: Decimal = Decimal("1.5")
    time_stop_bars: int = 10


class TrendPullback:
    name = "trend_pullback"

    def __init__(self, config: TrendPullbackConfig | None = None) -> None:
        self.config = config or TrendPullbackConfig()

    def on_bar(self, state: MarketState) -> EntrySignal | None:
        bars = state.bars
        config = self.config
        if len(bars) <= max(config.ema_period, config.volume_period, config.atr_period):
            return None
        closes = [bar.close for bar in bars]
        ema_line = ema(closes, config.ema_period)
        latest_ema = ema_line[-1]
        atrs = wilder_atr(bars, config.atr_period)
        atr = atrs[-1]
        vwaps = session_vwap(bars)
        vwap = vwaps[-1]
        side = _five_minute_trend(bars, config.five_minute_ema_period, config.slope_lookback)
        if latest_ema is None or atr is None or atr <= 0 or vwap is None or side is None:
            return None
        bar = bars[-1]
        prior_volume = [item.volume for item in bars[-config.volume_period - 1 : -1]]
        if len(prior_volume) < config.volume_period:
            return None
        average_volume = Decimal(sum(prior_volume)) / Decimal(config.volume_period)
        if Decimal(bar.volume) < config.volume_multiple * average_volume:
            return None
        limit = on_tick(latest_ema, state.tick_size)
        risk = on_tick(config.stop_atr * atr, state.tick_size)
        if risk <= 0:
            return None
        reward = on_tick(risk * config.target_r, state.tick_size)
        if side == "buy" and bar.close > vwap and bar.low <= latest_ema and bar.close > latest_ema and bar.close > bar.open:
            return EntrySignal(
                "buy",
                limit,
                on_tick(limit - risk, state.tick_size),
                on_tick(limit + reward, state.tick_size),
                config.time_stop_bars,
                "trend",
            )
        if side == "sell" and bar.close < vwap and bar.high >= latest_ema and bar.close < latest_ema and bar.close < bar.open:
            return EntrySignal(
                "sell",
                limit,
                on_tick(limit + risk, state.tick_size),
                on_tick(limit - reward, state.tick_size),
                config.time_stop_bars,
                "trend",
            )
        return None


def _five_minute_trend(bars, period: int, lookback: int) -> str | None:
    higher = completed_five_minute_bars(bars)
    if len(higher) <= period + lookback:
        return None
    line = ema([bar.close for bar in higher], period)
    latest = line[-1]
    earlier = line[-1 - lookback]
    if latest is None or earlier is None:
        return None
    if latest > earlier and higher[-1].close > latest:
        return "buy"
    if latest < earlier and higher[-1].close < latest:
        return "sell"
    return None
