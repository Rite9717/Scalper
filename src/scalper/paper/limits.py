"""Risk numbers for the paper loop.

Order size, loss limits, rate caps, and the flatten clock come from Settings.
The remaining knobs live here so they are visible at startup and stored with
the run. They are not silent substitutes for a value the user configured.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from scalper.config import Settings
from scalper.execution.snapshot import AccountLimits


@dataclass(frozen=True)
class PaperLimits:
    stale_after: timedelta = timedelta(seconds=5)
    pnl_tolerance: Decimal = Decimal("250")
    clock_drift_limit: timedelta = timedelta(seconds=2)
    max_rejects: int = 3
    flatten_timeout: float = 30.0
    flatten_poll_interval: float = 0.5
    flatten_max_adverse_ticks: int = 20
    reconnect_initial_seconds: float = 1.0
    reconnect_max_seconds: float = 30.0
    stops_blind_grace: timedelta = timedelta(seconds=30)


def account_limits(settings: Settings, paper: PaperLimits) -> AccountLimits:
    """Build the gate limits from the configured settings and the paper knobs."""
    return AccountLimits(
        trading_mode="paper",
        max_lots=settings.paper_max_lots,
        max_loss_per_trade=settings.max_loss_per_trade,
        daily_loss_limit=settings.daily_loss_limit,
        max_orders_per_second=settings.max_orders_per_second,
        max_orders_per_minute=settings.max_orders_per_minute,
        stale_after=paper.stale_after,
        pnl_tolerance=paper.pnl_tolerance,
        clock_drift_limit=paper.clock_drift_limit,
        max_rejects=paper.max_rejects,
        flatten_time=datetime.strptime(settings.flatten_time, "%H:%M").time(),
        reject_window=None,
    )


def resolved_limits(settings: Settings, paper: PaperLimits) -> dict[str, str]:
    """Every limit the run will use, as text for the log and the journal."""
    gate = account_limits(settings, paper)
    return {
        "trading_mode": gate.trading_mode,
        "max_lots": str(gate.max_lots),
        "max_loss_per_trade": format(gate.max_loss_per_trade, "f"),
        "daily_loss_limit": format(gate.daily_loss_limit, "f"),
        "max_orders_per_second": str(gate.max_orders_per_second),
        "max_orders_per_minute": str(gate.max_orders_per_minute),
        "flatten_time": settings.flatten_time,
        "stale_after_seconds": str(paper.stale_after.total_seconds()),
        "pnl_tolerance": format(paper.pnl_tolerance, "f"),
        "clock_drift_limit_seconds": str(paper.clock_drift_limit.total_seconds()),
        "max_rejects": str(paper.max_rejects),
        "flatten_timeout": str(paper.flatten_timeout),
        "flatten_poll_interval": str(paper.flatten_poll_interval),
        "flatten_max_adverse_ticks": str(paper.flatten_max_adverse_ticks),
        "reconnect_initial_seconds": str(paper.reconnect_initial_seconds),
        "reconnect_max_seconds": str(paper.reconnect_max_seconds),
        "stops_blind_grace_seconds": str(paper.stops_blind_grace.total_seconds()),
        "latency_bars": str(settings.latency_bars),
        "slippage_ticks": str(settings.slippage_ticks),
    }
