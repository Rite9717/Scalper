"""Fresh risk snapshots for each order decision.

The engine does not keep a snapshot of its own. A provider builds one at the
moment of the decision. Backtests use a fixed provider. Live trading uses
LiveSnapshotProvider, which fails closed when an input is missing.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Sequence
from time import monotonic
from dataclasses import dataclass, replace
from datetime import datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Protocol

from scalper.backtest.costs import ChargeSchedule, side_charges
from scalper.clock import IST, now_ist
from scalper.compliance import LIVE_INITIAL_MAX_LOTS
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
        read_timeout: float = 2.0,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.broker = broker
        self.cache = cache
        self.instrument_token = instrument_token
        self.tradingsymbol = tradingsymbol
        self.exchange = exchange
        self.kill_file = kill_file
        self.live = live
        self.limits = limits or AccountLimits(trading_mode="live" if live else "paper")
        _require_live_mode(self.live, self.limits)
        self._now = now or now_ist
        self.schedule = schedule or ChargeSchedule()
        self.tick_size = tick_size
        if isinstance(read_timeout, bool) or not isinstance(read_timeout, (int, float)) or read_timeout <= 0:
            raise ValueError("read timeout must be positive")
        self.read_timeout = float(read_timeout)
        self._sleep = sleep
        self._clock = clock or monotonic
        self._replay: tuple[object, object, object] | None = None
        self._miss_until = 0.0
        self._replayed = False
        self._quote_task: asyncio.Task[object] | None = None
        self._pos_task: asyncio.Task[object] | None = None
        self._mar_task: asyncio.Task[object] | None = None
        self._pending: list[asyncio.Task[object]] = []
        self._orders: list[datetime] = []
        self._rejects: list[datetime] = []

    def note_order(self) -> None:
        self._orders.append(self._now())

    def note_reject(self) -> None:
        self._rejects.append(self._now())

    def begin(self) -> None:
        """Start the cache, position, and margin reads together."""
        self._release_current()
        if self._replay is not None and self._clock() < self._miss_until:
            quote, positions, margins = self._replay
            self._replayed = True
            self._quote_task = self._start(self._ready(quote))
            self._pos_task = self._start(self._ready(positions))
            self._mar_task = self._start(self._ready(margins))
            return
        self._replayed = False
        self._quote_task = self._start(self._bounded(self._load_quote))
        self._pos_task = self._start(self._bounded(self.broker.get_positions))
        self._mar_task = self._start(self._bounded(self.broker.get_margins))

    def _start(self, awaitable: Awaitable[object]) -> asyncio.Task[object]:
        task = asyncio.create_task(awaitable)
        self._pending.append(task)
        return task

    def _release_current(self) -> None:
        for task in (self._quote_task, self._pos_task, self._mar_task):
            if task is not None and not task.done():
                task.cancel()
        self._quote_task = None
        self._pos_task = None
        self._mar_task = None

    async def close_reads(self) -> None:
        """Await or cancel every read begin() started."""
        tasks = list(self._pending)
        self._pending.clear()
        self._quote_task = None
        self._pos_task = None
        self._mar_task = None
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def positions(self) -> object | None:
        if self._pos_task is None or self._pos_task.done():
            self.begin()
        assert self._pos_task is not None
        return await self._pos_task

    async def build(
        self,
        *,
        lots_requested: int,
        open_lots: int,
        planned_loss: Decimal,
        orders: Sequence[ManagedOrder],
    ) -> RiskSnapshot:
        try:
            return await self._build(
                lots_requested=lots_requested,
                open_lots=open_lots,
                planned_loss=planned_loss,
                orders=orders,
            )
        finally:
            await self.close_reads()

    async def _build(
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
        if self._quote_task is None or self._pos_task is None or self._mar_task is None:
            self.begin()
        quote_task = self._quote_task
        position_task = self._pos_task
        margin_task = self._mar_task
        assert quote_task is not None and position_task is not None and margin_task is not None
        quote = await quote_task
        position_book = await position_task
        margin_book = await margin_task
        if isinstance(quote, tuple):
            last_quote_at, exchange_time, cached_last, quote_missing = quote
        else:
            last_quote_at, exchange_time, cached_last, quote_missing = None, None, None, True
        broker_pnl, pnl_missing, mark = self._broker_gross(position_book, cached_last)
        available, required, margin_missing = self._margins(margin_book)
        if not self._replayed:
            if quote is None or position_book is None or margin_book is None:
                self._replay = (quote, position_book, margin_book)
                self._miss_until = self._clock() + self.read_timeout
            else:
                self._replay = None
                self._miss_until = 0.0
        gross = _gross_from_fills(orders, mark)
        daily = gross - _charges(orders, self.schedule)
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
            daily_pnl=daily,
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
            internal_pnl=gross,
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

    async def _ready(self, value: object) -> object:
        return value

    async def _bounded(self, start: Callable[[], Awaitable[object]]) -> object | None:
        """One read. A timeout or error is a missing input."""
        awaitable = start()
        try:
            if self._sleep is None:
                try:
                    return await asyncio.wait_for(awaitable, self.read_timeout)
                except (TimeoutError, asyncio.TimeoutError, Exception):
                    return None
            task: asyncio.Task[object] | None = None
            timer: asyncio.Task[object] | None = None
            try:
                task = asyncio.ensure_future(awaitable)
                timer = asyncio.ensure_future(self._sleep(self.read_timeout))
                done, _pending = await asyncio.wait({task, timer}, return_when=asyncio.FIRST_COMPLETED)
                if task not in done or task.cancelled():
                    return None
                error = task.exception()
                if error is not None:
                    return None
                return task.result()
            finally:
                children = [item for item in (task, timer) if item is not None]
                for item in children:
                    if not item.done():
                        item.cancel()
                if children:
                    await asyncio.gather(*children, return_exceptions=True)
        finally:
            if inspect.iscoroutine(awaitable) and inspect.getcoroutinestate(awaitable) is inspect.CORO_CREATED:
                awaitable.close()

    async def _load_quote(self) -> tuple[datetime | None, datetime | None, Decimal | None, bool]:
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

    def _broker_gross(
        self,
        book: object,
        cached_last: Decimal | None,
    ) -> tuple[Decimal | None, bool, Decimal | None]:
        """Gross price PnL of the broker's position, marked at the same last price as our fills."""
        if book is None or not hasattr(book, "net"):
            return None, True, cached_last
        matching = [
            position
            for position in book.net
            if position.tradingsymbol == self.tradingsymbol and position.exchange == self.exchange
        ]
        mark = cached_last
        for position in matching:
            if position.last_price is not None:
                mark = position.last_price
                break
        if not matching:
            return Decimal("0"), False, mark
        total = Decimal("0")
        for position in matching:
            if position.quantity == 0:
                if position.pnl is None:
                    return None, True, mark
                total += position.pnl
                continue
            if mark is None:
                return None, True, mark
            total += (mark - position.average_price) * position.quantity
        return total, False, mark

    def _margins(self, margins: object) -> tuple[Decimal | None, Decimal | None, bool]:
        if margins is None or not hasattr(margins, "equity"):
            return None, None, True
        equity = margins.equity
        if not equity.enabled:
            return None, None, True
        return equity.live_balance, equity.net, False

    def _since(self, events: list[datetime], now: datetime, window: timedelta) -> int:
        cutoff = now - window
        return sum(1 for moment in events if moment >= cutoff)


def _require_live_mode(live: bool, limits: AccountLimits) -> None:
    if live and limits.trading_mode != "live":
        raise ValueError("live snapshots require trading_mode live")
    if live and limits.max_lots > LIVE_INITIAL_MAX_LOTS:
        raise ValueError("live snapshots are capped at most one lot")
    if not live and limits.trading_mode == "live":
        raise ValueError("a backtest snapshot cannot use trading_mode live")


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


def _gross_from_fills(orders: Sequence[ManagedOrder], mark: Decimal | None) -> Decimal:
    """Price PnL only. Charges are not included, so this can match the broker's gross PnL."""
    cash = Decimal("0")
    net = 0
    for order in orders:
        if order.filled_quantity < 1:
            continue
        turnover = order.average_price * order.filled_quantity
        if order.side == "buy":
            cash -= turnover
            net += order.filled_quantity
        else:
            cash += turnover
            net -= order.filled_quantity
    if net != 0 and mark is not None:
        cash += mark * net
    return cash


def _charges(orders: Sequence[ManagedOrder], schedule: ChargeSchedule) -> Decimal:
    total = Decimal("0")
    for order in orders:
        if order.filled_quantity < 1:
            continue
        turnover = order.average_price * order.filled_quantity
        brokerage, exchange, sebi, stt, stamp = side_charges(schedule, turnover=turnover, side=order.side)
        gst = (schedule.gst_rate * (brokerage + exchange + sebi)).quantize(PAISE)
        total += brokerage + exchange + sebi + stt + stamp + gst
    return total
