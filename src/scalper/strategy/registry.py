"""Named strategies the backtest command can run."""

from scalper.backtest.strategy import PlumbingStrategy, Strategy
from scalper.strategy.filters import FilterConfig, FilteredStrategy
from scalper.strategy.trend import TrendPullback
from scalper.strategy.vwap import VwapReversion

STRATEGY_NAMES = ("plumbing", "trend_pullback", "vwap_reversion")


def build_strategy(name: str, filters: FilterConfig | None = None) -> Strategy:
    if name == "plumbing":
        return PlumbingStrategy()
    if name == "trend_pullback":
        return FilteredStrategy(TrendPullback(), filters)
    if name == "vwap_reversion":
        return FilteredStrategy(VwapReversion(), filters)
    known = ", ".join(STRATEGY_NAMES)
    raise ValueError(f"unknown strategy {name}; choose one of: {known}")
