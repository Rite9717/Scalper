"""Fresh risk snapshots for each order decision.

The engine does not keep a snapshot of its own. A provider builds one at the
moment of the decision. Backtests use a fixed provider. Live trading uses
LiveSnapshotProvider, which fails closed when an input is missing.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Protocol

from scalper.backtest.costs import ChargeSchedule, side_charges
from scalper.clock import IST, now_ist
from scalper.data.cache import QuoteCache
from scalper.execution.broker import Broker
from scalper.execution.states import ManagedOrder
from scalper.risk.gate import RiskSnapshot
from scalper.risk.killfile import is_killed

PAISE = Decimal("0.01")


class SnapshotProvider(Protocol):
    async def build(
        self,
        *,
        lots_requested: int,
        open_lots: int,
        planned_loss: Decimal,
        orders: Sequence[ManagedOrder],
    ) -> RiskSnapshot:
        """Return the snapshot assess() should see for this decision."""

    def note_order(self) -> None:
        """Record one order that is about to be sent."""

    def note_reject(self) -> None:
        """Record one broker reject."""


class FixedSnapshotProvider:
    """Repeats one snapshot, updating only the fields of this decision."""

    def __init__(self, snapshot: RiskSnapshot) -> None:
        self.snapshot = snapshot
        self._rejects = snapshot.recent_rejects

    def note_order(self) -> None:
        return None

    def note_reject(self) -> None:
        self._rejects += 1

    async def build(
        self,
        *,
        lots_requested: int,
        open_lots: int,
        planned_loss: Decimal,
        orders: Sequence[ManagedOrder],
    ) -> RiskSnapshot:
        del orders
        return replace(
            self.snapshot,
            lots_requested=lots_requested,
            open_lots=open_lots,
            planned_loss=planned_loss,
            recent_rejects=self._rejects,
        )


@dataclass(frozen=True)
class AccountLimits:
    trading_mode: str = "paper"
    max_lots: int = 1
    max_loss_per_trade: Decimal = Decimal("3000")
    daily_loss_limit: Decimal = Decimal("5000")
    max_orders_per_second: int = 3
    max_orders_per_minute: int = 150
    stale_after: timedelta = timedelta(seconds=5)
    pnl_tolerance: Decimal = Decimal("250")
    clock_drift_limit: timedelta = timedelta(seconds=2)
    max_rejects: int = 3
    flatten_time: time = time(15, 20)
    reject_window: timedelta = timedelta(minutes=1)


class LiveSnapshotProvider:
    """Builds a snapshot from the quote cache, the broker, and the kill file.

    live=True fails closed when broker PnL or the exchange timestamp is missing.
    The gate treats those Nones as "skip this check", so the provider instead
    clears the quote and the margin fields, which the gate already uses to halt
    entries. live=False leaves the Nones in place for backtests.
    """

    def __init__(
        self,
        *,
        broker: Broker,
        cache: QuoteCache,
        instrument_token: int,
        tradingsymbol: str,
        exchange: str,
        kill_file: Path,
        live: bool,
        limits: AccountLimits | None = None,
        now: Callable[[], datetime] | None = None,
        schedule: ChargeSchedule | None = None,
        tick_size: Decimal = Decimal("0.05"),
    ) -> None:
        self.broker = broker
        self.cache = cache
        self.instrument_token = instrument_token
        self.tradingsymbol = tradingsymbol
        self.exchange = exchange
        self.kill_file = kill_file
        self.live = live
        self.limits = limits or AccountLimits()
        self._now = now or now_ist
        self.schedule = schedule or ChargeSchedule()
        self.tick_size = tick_size
        self._orders: list[datetime] = []
        self._rejects: list[datetime] = []

    def note_order(self) -> None:
        self._orders.append(self._now())

    def note_reject(self) -> None:
        self._rejects.append(self._now())

    async def build(
        self,
        *,
        lots_requested: int,
        open_lots: int,
        planned_loss: Decimal,
        orders: Sequence[ManagedOrder],
    ) -> RiskSnapshot:
        now = self._now()
        limits = self.limits
        killed = self._kill_flag()
        last_quote_at, exchange_time, last_price, quote_missing = await self._quote()
        broker_pnl, pnl_missing = await self._broker_pnl()
        available, required, margin_missing = await self._margins()
        internal = _pnl_from_fills(orders, last_price, self.schedule)
        if self.live and (pnl_missing or exchange_time is None or quote_missing):
            last_quote_at = None
            available = None
            required = None
        elif margin_missing or quote_missing:
            if quote_missing:
                last_quote_at = None
            if margin_missing:
                available = None
                required = None
        return RiskSnapshot(
            now=now,
            trading_mode=limits.trading_mode,
            lots_requested=lots_requested,
            open_lots=open_lots,
            max_lots=limits.max_lots,
            planned_loss=planned_loss,
            max_loss_per_trade=limits.max_loss_per_trade,
            daily_pnl=internal,
            daily_loss_limit=limits.daily_loss_limit,
            orders_last_second=self._since(self._orders, now, timedelta(seconds=1)),
            orders_last_minute=self._since(self._orders, now, timedelta(minutes=1)),
            max_orders_per_second=limits.max_orders_per_second,
            max_orders_per_minute=limits.max_orders_per_minute,
            last_quote_at=last_quote_at,
            stale_after=limits.stale_after,
            available_margin=available,
            required_margin=required,
            flatten_time=limits.flatten_time,
            broker_pnl=broker_pnl,
            internal_pnl=internal,
            pnl_tolerance=limits.pnl_tolerance,
            exchange_time=exchange_time,
            clock_drift_limit=limits.clock_drift_limit,
            recent_rejects=self._since(self._rejects, now, limits.reject_window),
            max_rejects=limits.max_rejects,
            killed=killed,
        )

    def _kill_flag(self) -> bool:
        try:
            return is_killed(self.kill_file)
        except OSError:
            return True

    async def _quote(self) -> tuple[datetime | None, datetime | None, Decimal | None, bool]:
        try:
            cached = await self.cache.get(self.instrument_token)
        except Exception:
            return None, None, None, True
        if not isinstance(cached, dict):
            return None, None, None, True
        received = _parse_time(cached.get("received_at"))
        exchange_time = _parse_time(cached.get("exchange_ts"))
        last_price = _parse_decimal(cached.get("last_price"))
        if received is None:
            return None, exchange_time, last_price, True
        return received, exchange_time, last_price, False

    async def _broker_pnl(self) -> tuple[Decimal | None, bool]:
        try:
            book = await self.broker.get_positions()
        except Exception:
            return None, True
        total = Decimal("0")
        saw = False
        for position in book.net:
            if position.tradingsymbol != self.tradingsymbol or position.exchange != self.exchange:
                continue
            saw = True
            if position.pnl is None:
                return None, True
            total += position.pnl
        if not saw:
            return Decimal("0"), False
        return total, False

    async def _margins(self) -> tuple[Decimal | None, Decimal | None, bool]:
        try:
            margins = await self.broker.get_margins()
        except Exception:
            return None, None, True
        equity = margins.equity
        if not equity.enabled:
            return None, None, True
        return equity.live_balance, equity.net, False

    def _since(self, events: list[datetime], now: datetime, window: timedelta) -> int:
        cutoff = now - window
        return sum(1 for moment in events if moment >= cutoff)


def _parse_time(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=IST)
    return parsed


def _parse_decimal(value: object) -> Decimal | None:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, str)):
        try:
            return Decimal(value)
        except Exception:
            return None
    if isinstance(value, float):
        try:
            return Decimal(str(value))
        except Exception:
            return None
    return None


def _pnl_from_fills(
    orders: Sequence[ManagedOrder],
    last_price: Decimal | None,
    schedule: ChargeSchedule,
) -> Decimal:
    cash = Decimal("0")
    net = 0
    for order in orders:
        if order.filled_quantity < 1:
            continue
        turnover = order.average_price * order.filled_quantity
        brokerage, exchange, sebi, stt, stamp = side_charges(schedule, turnover=turnover, side=order.side)
        gst = (schedule.gst_rate * (brokerage + exchange + sebi)).quantize(PAISE)
        charges = brokerage + exchange + sebi + stt + stamp + gst
        if order.side == "buy":
            cash -= turnover + charges
            net += order.filled_quantity
        else:
            cash += turnover - charges
            net -= order.filled_quantity
    if net != 0 and last_price is not None:
        cash += last_price * net
    return cash
