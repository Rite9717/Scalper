"""Summaries and the walk-forward acceptance rule.

Windows are scored after the run. This module does not search parameters, and
it does not reuse a window both to change a rule and to grade that same rule.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from scalper.backtest.engine import BacktestResult, Trade
from scalper.clock import ensure_ist

MIN_TRADES = 300
MIN_POSITIVE_FRACTION = Decimal("0.70")


def max_drawdown(pnls: list[Decimal]) -> Decimal:
    equity = Decimal("0")
    peak = Decimal("0")
    worst = Decimal("0")
    for pnl in pnls:
        equity += pnl
        if equity > peak:
            peak = equity
        drop = peak - equity
        if drop > worst:
            worst = drop
    return worst


def summarize(trades: list[Trade]) -> dict[str, object]:
    count = len(trades)
    if count == 0:
        return {
            "trade_count": 0,
            "win_rate": "0",
            "average_win": None,
            "average_loss": None,
            "expectancy": "0",
            "profit_factor": None,
            "max_drawdown": "0",
            "by_hour": {},
            "by_regime": {},
        }
    wins = [trade.net_pnl for trade in trades if trade.net_pnl > 0]
    losses = [trade.net_pnl for trade in trades if trade.net_pnl < 0]
    gross_profit = sum(wins, Decimal("0"))
    gross_loss = sum(losses, Decimal("0"))
    by_hour: dict[str, int] = {}
    by_regime: dict[str, int] = {}
    for trade in trades:
        hour = str(ensure_ist(trade.entry_time).hour)
        by_hour[hour] = by_hour.get(hour, 0) + 1
        by_regime[trade.regime] = by_regime.get(trade.regime, 0) + 1
    profit_factor = None if not losses else str(gross_profit / abs(gross_loss))
    return {
        "trade_count": count,
        "win_rate": str(Decimal(len(wins)) / Decimal(count)),
        "average_win": None if not wins else str(gross_profit / len(wins)),
        "average_loss": None if not losses else str(abs(gross_loss) / len(losses)),
        "expectancy": str(sum((trade.net_pnl for trade in trades), Decimal("0")) / Decimal(count)),
        "profit_factor": profit_factor,
        "max_drawdown": str(max_drawdown([trade.net_pnl for trade in trades])),
        "by_hour": by_hour,
        "by_regime": by_regime,
    }


def windows_covering(first: datetime, last: datetime, window: timedelta) -> list[tuple[datetime, datetime]]:
    if window <= timedelta(0):
        raise ValueError("window must be positive")
    start = ensure_ist(first)
    final = ensure_ist(last) + timedelta(microseconds=1)
    windows: list[tuple[datetime, datetime]] = []
    while start < final:
        windows.append((start, start + window))
        start = start + window
        if len(windows) > 100_000:
            raise ValueError("window is too small for this history")
    return windows


def judge(
    trades: list[Trade],
    windows: list[tuple[datetime, datetime]],
    *,
    min_trades: int = MIN_TRADES,
    min_positive_fraction: Decimal = MIN_POSITIVE_FRACTION,
) -> dict[str, object]:
    grouped: list[list[Trade]] = []
    for start, end in windows:
        bucket = [trade for trade in trades if start <= ensure_ist(trade.entry_time) < end]
        if bucket:
            grouped.append(bucket)
    positive = 0
    for bucket in grouped:
        expectancy = sum((trade.net_pnl for trade in bucket), Decimal("0")) / Decimal(len(bucket))
        if expectancy > 0:
            positive += 1
    fraction = Decimal("0") if not grouped else Decimal(positive) / Decimal(len(grouped))
    reasons: list[str] = []
    if len(trades) < min_trades:
        reasons.append(f"fewer than {min_trades} trades")
    if not grouped:
        reasons.append("no window contains a trade")
    elif fraction < min_positive_fraction:
        reasons.append(
            f"positive expectancy in {fraction} of windows with trades; need at least {min_positive_fraction}"
        )
    return {
        "trade_count": len(trades),
        "windows_with_trades": len(grouped),
        "positive_windows": positive,
        "positive_fraction": str(fraction),
        "passed": not reasons,
        "reasons": reasons,
        "parameters_fitted": False,
        "metrics": summarize(trades),
    }


def result_report(
    result: BacktestResult,
    windows: list[tuple[datetime, datetime]],
    *,
    min_trades: int = MIN_TRADES,
    min_positive_fraction: Decimal = MIN_POSITIVE_FRACTION,
) -> dict[str, object]:
    verdict = judge(result.trades, windows, min_trades=min_trades, min_positive_fraction=min_positive_fraction)
    return {
        "strategy": result.strategy,
        "edge_claimed": False,
        "bars": result.bars,
        "gated": result.gated,
        "risk_vetoes": result.risk_vetoes,
        "margin_assumed": result.margin_assumed,
        "expired_orders": result.expired_orders,
        "acceptance": verdict,
    }
