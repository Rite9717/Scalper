"""Turns a decision into one tagged limit order and keeps the position flat on shutdown.

Every placement goes through place_live_limit. A PlaceOutcome only means the
order was submitted. Status and position come from the order book afterward.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path

from scalper.execution.broker import Broker
from scalper.execution.errors import AmbiguousOrderOutcome
from scalper.execution.halt import entries_halted, entry_halt_reason, halt_new_entries
from scalper.execution.states import TERMINAL, ManagedOrder, OrderStatus, note_modification
from scalper.execution.submit import place_live_limit
from scalper.kite.api_facts import KITE_MAX_MODIFICATIONS_PER_ORDER
from scalper.kite.tags import make_order_tag
from scalper.monitor.alerts import Monitor
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
        snapshot: RiskSnapshot,
        *,
        tradingsymbol: str,
        exchange: str,
        lot_size: int,
        kill_file: Path,
        monitor: Monitor,
        flatten_timeout: float = 0.0,
        flatten_poll_interval: float = 0.0,
        flatten_reprice_step: Decimal = Decimal("0.05"),
        flatten_worst_price: Decimal | None = None,
    ) -> None:
        if not tradingsymbol or not exchange:
            raise ValueError("tradingsymbol and exchange are required")
        if lot_size < 1:
            raise ValueError("lot size must be positive")
        if flatten_timeout < 0 or flatten_poll_interval < 0:
            raise ValueError("flatten timing must not be negative")
        if flatten_reprice_step < 0:
            raise ValueError("flatten reprice step must not be negative")
        self.broker = broker
        self.snapshot = snapshot
        self.tradingsymbol = tradingsymbol
        self.exchange = exchange
        self.lot_size = lot_size
        self.kill_file = kill_file
        self.monitor = monitor
        self.flatten_timeout = flatten_timeout
        self.flatten_poll_interval = flatten_poll_interval
        self.flatten_reprice_step = flatten_reprice_step
        self.flatten_worst_price = flatten_worst_price
        self.orders: dict[str, ManagedOrder] = {}
        self.foreign_orders: list[ManagedOrder] = []
        self.position_quantity = 0
        self.unmanaged: list[str] = []
        self._our_tags: set[str] = set()
        self.recent_rejects = snapshot.recent_rejects
        self._reject_ids: set[str] = set()
        self._replace_sequence = 1
        self._flatten_modifications = 0

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
        _updated, action = note_modification(current, limit_price)
        order_id = current.broker_order_id
        if order_id is None:
            raise UnmanagedPosition(f"remaining {self.position_quantity}; orders {tag}")
        if action == "modify":
            await self.broker.modify_price(order_id, limit_price)
            book = await self.refresh()
            return SubmitResult(self._require_book_order(order_id, book), RiskDecision(True, "allow", "price modified"))
        booked = await self.cancel(order_id)
        if booked.status is not OrderStatus.CANCELLED or booked.remaining < 1:
            return SubmitResult(booked, RiskDecision(True, "allow", "cancel confirmed on the book"))
        sequence = self._replace_sequence
        self._replace_sequence += 1
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
        result = await self._submit(
            intent=Intent.FLATTEN,
            side=side,
            limit_price=last_price,
            quantity=abs(self.position_quantity),
            strategy_id="flatten",
            timestamp_ms=0,
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
        await self._drive_flatten(result.order.broker_order_id)
        return ShutdownReport(True, self.position_quantity)

    async def _drive_flatten(self, order_id: str) -> None:
        deadline = time.monotonic() + self.flatten_timeout
        sent = 0
        while True:
            book = await self.refresh()
            order = self._require_book_order(order_id, book)
            if order.status is OrderStatus.FILLED and self.position_quantity == 0:
                return
            if order.status in TERMINAL:
                raise UnmanagedPosition(f"remaining {self.position_quantity}; orders {order_id}")
            if sent < KITE_MAX_MODIFICATIONS_PER_ORDER and order.modifications < KITE_MAX_MODIFICATIONS_PER_ORDER:
                nxt = _worse_price(order.side, order.limit_price, self.flatten_reprice_step)
                if _price_allowed(order.side, nxt, self.flatten_worst_price) and nxt != order.limit_price:
                    await self.broker.modify_price(order_id, nxt)
                    sent += 1
                    continue
            if time.monotonic() >= deadline:
                raise UnmanagedPosition(f"remaining {self.position_quantity}; orders {order_id}")
            await asyncio.sleep(self.flatten_poll_interval)

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
        if sequence is None:
            sequence = 0
        tag = make_order_tag(timestamp_ms=timestamp_ms, strategy_id=strategy_id, sequence=sequence)
        if intent is Intent.ENTRY and entries_halted():
            return SubmitResult(None, RiskDecision(False, "halt", entry_halt_reason()))
        await self.refresh()
        if intent is Intent.ENTRY and self.foreign_orders:
            foreign_id = self.foreign_orders[0].broker_order_id or "unknown"
            return SubmitResult(None, RiskDecision(False, "halt", f"foreign order {foreign_id}"))
        existing = self.orders.get(tag)
        if existing is not None:
            return SubmitResult(existing, RiskDecision(True, "allow", "duplicate tag"))
        lots = max(1, quantity // self.lot_size)
        open_lots = abs(self.position_quantity) // self.lot_size
        decision = assess(
            intent,
            replace(
                self.snapshot,
                lots_requested=lots,
                open_lots=open_lots,
                planned_loss=planned_loss,
                recent_rejects=self.recent_rejects,
            ),
        )
        if not decision.allowed:
            return SubmitResult(None, decision)
        self._our_tags.add(tag)
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
        book = await self.refresh()
        matches = [order for order in book if order.tag == tag]
        if len(matches) != 1:
            raise UnmanagedPosition(f"remaining {self.position_quantity}; orders {tag}")
        booked = matches[0]
        self.orders[tag] = booked
        self._note_reject(booked)
        return SubmitResult(booked, decision)

    def _require_book_order(self, order_id: str, book: list[ManagedOrder]) -> ManagedOrder:
        for order in book:
            if order.broker_order_id == order_id:
                return order
        raise UnmanagedPosition(f"remaining {self.position_quantity}; orders {order_id}")

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
        self.snapshot = replace(self.snapshot, recent_rejects=self.recent_rejects)

    def _raise_for_foreign(self) -> None:
        if not self.foreign_orders:
            return
        ids = [order.broker_order_id or "unknown" for order in self.foreign_orders]
        raise UnmanagedPosition(f"remaining {self.position_quantity}; orders {', '.join(ids)}")

    async def _check_broker_position(self) -> None:
        positions = await self.broker.get_positions()
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


def _price_allowed(side: str, price: Decimal, worst: Decimal | None) -> bool:
    if worst is None:
        return False
    if side == "sell":
        return price >= worst
    if side == "buy":
        return price <= worst
    return False
