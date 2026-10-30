"""Turns a decision into one tagged limit order and keeps the position flat on shutdown.

Every placement goes through place_live_limit. A PlaceOutcome only means the
order was submitted. Status and position come from the order book afterward.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from datetime import time as clock_time
from decimal import Decimal
from pathlib import Path

from scalper.execution.broker import Broker
from scalper.execution.errors import AmbiguousOrderOutcome
from scalper.execution.halt import entries_halted, entry_halt_reason, halt_new_entries
from scalper.execution.snapshot import SnapshotProvider
from scalper.execution.states import TERMINAL, IllegalTransition, ManagedOrder, OrderStatus
from scalper.execution.submit import (
    _BOOK_CONFIRM_READS,
    _BOOK_CONFIRM_WINDOW_SECONDS,
    close_on_ambiguous,
    place_live_limit,
)
from scalper.kite.api_facts import KITE_MAX_MODIFICATIONS_PER_ORDER
from scalper.kite.tags import make_order_tag
from scalper.monitor.alerts import Monitor
from scalper.clock import IST
from scalper.risk.gate import Intent, RiskDecision, RiskSnapshot, assess


class UnmanagedPosition(RuntimeError):
    """Shutdown stopped because a position or an unknown order is still open."""


class PositionMismatch(RuntimeError):
    """The broker's net quantity for this instrument does not match filled orders."""


_PRODUCT = "MIS"


@dataclass(frozen=True)
class SubmitResult:
    order: ManagedOrder | None
    decision: RiskDecision


@dataclass(frozen=True)
class ShutdownReport:
    clean: bool
    open_quantity: int


class ExecutionEngine:
    def __init__(
        self,
        broker: Broker,
        provider: SnapshotProvider,
        *,
        tradingsymbol: str,
        exchange: str,
        lot_size: int,
        tick_size: Decimal,
        kill_file: Path,
        monitor: Monitor,
        flatten_timeout: float,
        flatten_poll_interval: float,
        flatten_max_adverse_ticks: int,
        clock: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        now_ms: Callable[[], int] | None = None,
    ) -> None:
        if not tradingsymbol or not exchange:
            raise ValueError("tradingsymbol and exchange are required")
        if lot_size < 1:
            raise ValueError("lot size must be positive")
        if tick_size <= 0:
            raise ValueError("tick size must be positive")
        if isinstance(flatten_timeout, bool) or not isinstance(flatten_timeout, (int, float)) or flatten_timeout <= 0:
            raise ValueError("flatten timeout must be positive")
        if (
            isinstance(flatten_poll_interval, bool)
            or not isinstance(flatten_poll_interval, (int, float))
            or flatten_poll_interval <= 0
        ):
            raise ValueError("flatten poll interval must be positive")
        if type(flatten_max_adverse_ticks) is not int or flatten_max_adverse_ticks < 1:
            raise ValueError("flatten max adverse ticks must be a positive integer")
        self.broker = broker
        self.provider = provider
        self.tradingsymbol = tradingsymbol
        self.exchange = exchange
        self.lot_size = lot_size
        self.tick_size = tick_size
        self.kill_file = kill_file
        self.monitor = monitor
        self.flatten_timeout = float(flatten_timeout)
        self.flatten_poll_interval = float(flatten_poll_interval)
        self.flatten_max_adverse_ticks = flatten_max_adverse_ticks
        self._monotonic = clock or time.monotonic
        self._sleep = sleep or asyncio.sleep
        self._now_ms = now_ms or (lambda: time.time_ns() // 1_000_000)
        self.orders: dict[str, ManagedOrder] = {}
        self.foreign_orders: list[ManagedOrder] = []
        self.position_quantity = 0
        self.unmanaged: list[str] = []
        self._our_tags: set[str] = set()
        self.recent_rejects = 0
        self._reject_ids: set[str] = set()
        self._replacement_at: dict[tuple[int, str], int] = {}
        self._modifications: dict[str, int] = {}

    async def submit_entry(
        self,
        *,
        side: str,
        limit_price: Decimal,
        quantity: int,
        stop_price: Decimal,
        strategy_id: str,
        timestamp_ms: int,
    ) -> SubmitResult:
        return await self._submit(
            intent=Intent.ENTRY,
            side=side,
            limit_price=limit_price,
            quantity=quantity,
            strategy_id=strategy_id,
            timestamp_ms=timestamp_ms,
            planned_loss=abs(limit_price - stop_price) * quantity,
        )

    async def submit_exit(
        self,
        *,
        side: str,
        limit_price: Decimal,
        quantity: int,
        strategy_id: str,
        timestamp_ms: int,
    ) -> SubmitResult:
        return await self._submit(
            intent=Intent.EXIT,
            side=side,
            limit_price=limit_price,
            quantity=quantity,
            strategy_id=strategy_id,
            timestamp_ms=timestamp_ms,
            planned_loss=Decimal("0"),
        )

    async def modify_price(
        self,
        tag: str,
        limit_price: Decimal,
        *,
        strategy_id: str,
        timestamp_ms: int,
    ) -> SubmitResult:
        current = self.orders[tag]
        order_id = current.broker_order_id
        if order_id is None:
            raise UnmanagedPosition(f"remaining {self.position_quantity}; orders {tag}")
        if not current.working:
            raise IllegalTransition(f"cannot modify an order in status {current.status.value}")
        if self._modifications.get(order_id, 0) >= KITE_MAX_MODIFICATIONS_PER_ORDER:
            booked = await self.cancel(order_id)
            if booked.status is not OrderStatus.CANCELLED or booked.remaining < 1:
                return SubmitResult(booked, RiskDecision(True, "allow", "cancel confirmed on the book"))
        else:
            await self._count_modify(order_id, limit_price)
            book = await self.refresh()
            return SubmitResult(self._require_book_order(order_id, book), RiskDecision(True, "allow", "price modified"))
        booked = await self._book_order(order_id)
        sequence = self._replacement_sequence(timestamp_ms, strategy_id)
        return await self._submit(
            intent=Intent(current.intent),
            side=current.side,
            limit_price=limit_price,
            quantity=booked.remaining,
            strategy_id=strategy_id,
            timestamp_ms=timestamp_ms,
            planned_loss=Decimal("0"),
            sequence=sequence,
        )

    async def cancel(self, order_id: str) -> ManagedOrder:
        """Cancel by broker order id, then keep only the status on the book."""
        await self.broker.cancel(order_id)
        book = await self.refresh()
        return self._require_book_order(order_id, book)

    async def on_trade(self, price: Decimal, quantity: int) -> None:
        """Apply a paper print when the broker simulates one, then read fills from the book."""
        apply = getattr(self.broker, "on_trade", None)
        if apply is not None:
            apply(price, quantity)
        await self.refresh()

    async def refresh(self) -> list[ManagedOrder]:
        book = await self.broker.get_orders()
        ours: list[ManagedOrder] = []
        foreign: list[ManagedOrder] = []
        for order in book:
            if not self._same_instrument(order):
                continue
            if order.tag and order.tag in self._our_tags:
                ours.append(order)
                self.orders[order.tag] = order
                self._note_reject(order)
            else:
                foreign.append(order)
        self.foreign_orders = foreign
        self.position_quantity = _net_filled(ours)
        await self._check_broker_position()
        return book

    async def recover(self) -> None:
        book = await self.broker.get_orders()
        self.orders.clear()
        self.foreign_orders.clear()
        self.unmanaged.clear()
        self._our_tags.clear()
        for order in book:
            if not self._same_instrument(order):
                continue
            if order.tag and order.broker_order_id:
                self._our_tags.add(order.tag)
                self.orders[order.tag] = order
                self._note_reject(order)
            elif order.broker_order_id:
                self.foreign_orders.append(order)
        self.position_quantity = _net_filled(list(self.orders.values()))
        await self._check_broker_position()

    async def shutdown(self, last_price: Decimal | None) -> ShutdownReport:
        book = await self.refresh()
        self._raise_for_foreign()
        for order in list(self.orders.values()):
            if order.working and order.broker_order_id:
                await self.broker.cancel(order.broker_order_id)
                book = await self.refresh()
        self._raise_for_foreign()
        if self.position_quantity == 0:
            return ShutdownReport(True, 0)
        if last_price is None:
            raise UnmanagedPosition(f"remaining {self.position_quantity}; orders none")
        side = _flatten_side(self.position_quantity)
        stamp = self._now_ms()
        result = await self._submit(
            intent=Intent.FLATTEN,
            side=side,
            limit_price=last_price,
            quantity=abs(self.position_quantity),
            strategy_id="flatten",
            timestamp_ms=stamp,
            planned_loss=Decimal("0"),
        )
        if result.order is None or result.order.broker_order_id is None:
            raise UnmanagedPosition(f"remaining {self.position_quantity}; orders none")
        if result.order.status in TERMINAL and result.order.status is not OrderStatus.FILLED:
            raise UnmanagedPosition(
                f"remaining {self.position_quantity}; orders {result.order.broker_order_id}"
            )
        if result.order.status is OrderStatus.FILLED and self.position_quantity == 0:
            return ShutdownReport(True, 0)
        worst = _adverse_limit(
            result.order.side,
            result.order.limit_price,
            self.flatten_max_adverse_ticks,
            self.tick_size,
        )
        await self._drive_flatten(result.order.broker_order_id, worst, self._monotonic(), stamp)
        return ShutdownReport(True, self.position_quantity)

    async def _drive_flatten(self, order_id: str, worst: Decimal, placed_at: float, timestamp_ms: int) -> None:
        deadline = placed_at + self.flatten_timeout
        # The first reprice waits one full poll after the order is placed.
        last_modify_at = placed_at
        while True:
            book = await self.refresh()
            order = self._require_book_order(order_id, book)
            if order.status is OrderStatus.FILLED and self.position_quantity == 0:
                return
            if order.status in TERMINAL:
                raise UnmanagedPosition(f"remaining {self.position_quantity}; orders {order_id}")
            now = self._monotonic()
            if now >= deadline:
                raise UnmanagedPosition(f"remaining {self.position_quantity}; orders {order_id}")
            spaced = now - last_modify_at >= self.flatten_poll_interval
            nxt = _worse_price(order.side, order.limit_price, self.tick_size)
            can_reprice = _price_allowed(order.side, nxt, worst) and nxt != order.limit_price
            if spaced and can_reprice and self._modifications.get(order_id, 0) >= KITE_MAX_MODIFICATIONS_PER_ORDER:
                booked = await self.cancel(order_id)
                if booked.status is not OrderStatus.CANCELLED or booked.remaining < 1:
                    if self.position_quantity == 0:
                        return
                    raise UnmanagedPosition(f"remaining {self.position_quantity}; orders {order_id}")
                sequence = self._replacement_sequence(timestamp_ms, "flatten")
                replaced = await self._submit(
                    intent=Intent.FLATTEN,
                    side=order.side,
                    limit_price=nxt,
                    quantity=booked.remaining,
                    strategy_id="flatten",
                    timestamp_ms=timestamp_ms,
                    planned_loss=Decimal("0"),
                    sequence=sequence,
                )
                if replaced.order is None or replaced.order.broker_order_id is None:
                    raise UnmanagedPosition(f"remaining {self.position_quantity}; orders {order_id}")
                order_id = replaced.order.broker_order_id
                last_modify_at = self._monotonic()
                await self._sleep(self.flatten_poll_interval)
                continue
            if spaced and can_reprice:
                await self._count_modify(order_id, nxt)
                last_modify_at = self._monotonic()
                await self._sleep(self.flatten_poll_interval)
                continue
            await self._sleep(self.flatten_poll_interval)

    async def _submit(
        self,
        *,
        intent: Intent,
        side: str,
        limit_price: Decimal,
        quantity: int,
        strategy_id: str,
        timestamp_ms: int,
        planned_loss: Decimal,
        sequence: int | None = None,
    ) -> SubmitResult:
        if type(timestamp_ms) is not int or timestamp_ms < 1:
            raise ValueError("timestamp_ms must be a positive millisecond timestamp")
        if sequence is None:
            sequence = 0
        tag = make_order_tag(timestamp_ms=timestamp_ms, strategy_id=strategy_id, sequence=sequence)
        if intent is Intent.ENTRY and entries_halted():
            return SubmitResult(None, RiskDecision(False, "halt", entry_halt_reason()))
        start_reads = getattr(self.provider, "begin", None)
        if start_reads is not None:
            start_reads()
        try:
            await self.refresh()
            if intent is Intent.ENTRY and self.foreign_orders:
                foreign_id = self.foreign_orders[0].broker_order_id or "unknown"
                return SubmitResult(None, RiskDecision(False, "halt", f"foreign order {foreign_id}"))
            existing = self.orders.get(tag)
            if existing is not None:
                return SubmitResult(existing, RiskDecision(True, "allow", "duplicate tag"))
            lots = max(1, quantity // self.lot_size)
            open_lots = abs(self.position_quantity) // self.lot_size
            try:
                snapshot = await self.provider.build(
                    lots_requested=lots,
                    open_lots=open_lots,
                    planned_loss=planned_loss,
                    orders=list(self.orders.values()),
                )
            except Exception:
                if intent is Intent.ENTRY:
                    return SubmitResult(None, RiskDecision(False, "halt", "risk snapshot unavailable"))
                snapshot = _protective_snapshot(lots_requested=lots, open_lots=open_lots, planned_loss=planned_loss)
            decision = assess(intent, snapshot)
            if not decision.allowed:
                return SubmitResult(None, decision)
            self._our_tags.add(tag)
            self.provider.note_order()
            try:
                await place_live_limit(
                    self.broker,
                    ManagedOrder(
                        tag=tag,
                        side=side,
                        limit_price=limit_price,
                        quantity=quantity,
                        intent=intent.value,
                        tradingsymbol=self.tradingsymbol,
                        exchange=self.exchange,
                        product=_PRODUCT,
                    ),
                    kill_file=self.kill_file,
                    monitor=self.monitor,
                )
            except AmbiguousOrderOutcome:
                raise
            booked = await self._confirm_tag(tag)
            self.orders[tag] = booked
            self._note_reject(booked)
            return SubmitResult(booked, decision)
        finally:
            close = getattr(self.provider, "close_reads", None)
            if close is not None:
                await close()

    async def _confirm_tag(self, tag: str) -> ManagedOrder:
        interval = _BOOK_CONFIRM_WINDOW_SECONDS / (_BOOK_CONFIRM_READS - 1)
        for attempt in range(_BOOK_CONFIRM_READS):
            if attempt:
                await self._sleep(interval)
            book = await self.refresh()
            matches = [order for order in book if order.tag == tag and self._same_instrument(order)]
            if len(matches) > 1:
                await close_on_ambiguous(
                    f"more than one order uses tag {tag}",
                    kill_file=self.kill_file,
                    monitor=self.monitor,
                )
            if len(matches) == 1:
                return matches[0]
        await close_on_ambiguous(
            f"placed tag {tag} is not on the order book",
            kill_file=self.kill_file,
            monitor=self.monitor,
        )
        raise AssertionError("close_on_ambiguous returns")

    async def _count_modify(self, order_id: str, limit_price: Decimal) -> None:
        await self.broker.modify_price(order_id, limit_price)
        self._modifications[order_id] = self._modifications.get(order_id, 0) + 1

    async def _book_order(self, order_id: str) -> ManagedOrder:
        book = await self.refresh()
        return self._require_book_order(order_id, book)

    def _require_book_order(self, order_id: str, book: list[ManagedOrder]) -> ManagedOrder:
        for order in book:
            if order.broker_order_id == order_id:
                return order
        raise UnmanagedPosition(f"remaining {self.position_quantity}; orders {order_id}")

    def _replacement_sequence(self, timestamp_ms: int, strategy_id: str) -> int:
        """Next replacement tag for one decision. A new timestamp starts again at 1."""
        if type(timestamp_ms) is not int or timestamp_ms < 1:
            raise ValueError("timestamp_ms must be a positive millisecond timestamp")
        key = (timestamp_ms, strategy_id)
        sequence = self._replacement_at.get(key, 1)
        if sequence > 61:
            raise ValueError("sequence must be an int from 0 to 61")
        self._replacement_at[key] = sequence + 1
        return sequence

    def _same_instrument(self, order: ManagedOrder) -> bool:
        return order.tradingsymbol == self.tradingsymbol and order.exchange == self.exchange

    def _note_reject(self, order: ManagedOrder) -> None:
        if not self._same_instrument(order) or not order.tag or order.tag not in self._our_tags:
            return
        if order.status is not OrderStatus.REJECTED or not order.broker_order_id:
            return
        if order.broker_order_id in self._reject_ids:
            return
        self._reject_ids.add(order.broker_order_id)
        self.recent_rejects += 1
        self.provider.note_reject()

    def _raise_for_foreign(self) -> None:
        if not self.foreign_orders:
            return
        ids = [order.broker_order_id or "unknown" for order in self.foreign_orders]
        raise UnmanagedPosition(f"remaining {self.position_quantity}; orders {', '.join(ids)}")

    async def _check_broker_position(self) -> None:
        fetch = getattr(self.provider, "positions", None)
        try:
            positions = await fetch() if fetch is not None else await self.broker.get_positions()
        except Exception:
            return
        if positions is None:
            return
        broker_qty = sum(
            position.quantity
            for position in positions.net
            if position.tradingsymbol == self.tradingsymbol and position.exchange == self.exchange
        )
        if broker_qty == self.position_quantity:
            return
        reason = (
            f"position mismatch {self.tradingsymbol}: broker {broker_qty} fills {self.position_quantity}"
        )
        halt_new_entries(reason)
        raise PositionMismatch(reason)


def _protective_snapshot(*, lots_requested: int, open_lots: int, planned_loss: Decimal) -> RiskSnapshot:
    """Used when the provider fails during an exit or flatten, so the close can still be sent."""
    now = datetime.now(IST)
    return RiskSnapshot(
        now=now,
        trading_mode="paper",
        lots_requested=lots_requested,
        open_lots=open_lots,
        max_lots=1,
        planned_loss=planned_loss,
        max_loss_per_trade=Decimal("1000000"),
        daily_pnl=Decimal("0"),
        daily_loss_limit=Decimal("1000000"),
        orders_last_second=0,
        orders_last_minute=0,
        max_orders_per_second=100,
        max_orders_per_minute=10000,
        last_quote_at=now,
        stale_after=timedelta(hours=1),
        available_margin=Decimal("1"),
        required_margin=Decimal("0"),
        flatten_time=clock_time(23, 59),
        broker_pnl=None,
        internal_pnl=None,
        pnl_tolerance=Decimal("1"),
        exchange_time=None,
        clock_drift_limit=timedelta(hours=1),
        recent_rejects=0,
        max_rejects=100,
        killed=False,
    )


def _net_filled(book: list[ManagedOrder]) -> int:
    net = 0
    for order in book:
        if order.side == "buy":
            net += order.filled_quantity
        elif order.side == "sell":
            net -= order.filled_quantity
    return net


def _flatten_side(position_quantity: int) -> str:
    return "sell" if position_quantity > 0 else "buy"


def _worse_price(side: str, price: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return price
    if side == "sell":
        return price - step
    return price + step


def _adverse_limit(side: str, start: Decimal, ticks: int, tick_size: Decimal) -> Decimal:
    move = tick_size * ticks
    if side == "sell":
        return start - move
    if side == "buy":
        return start + move
    raise ValueError("side must be buy or sell")


def _price_allowed(side: str, price: Decimal, worst: Decimal) -> bool:
    """True when price is not past the adverse limit for this flatten side."""
    if side == "sell":
        return price >= worst
    if side == "buy":
        return price <= worst
    return False
