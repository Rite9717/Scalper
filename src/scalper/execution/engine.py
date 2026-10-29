"""Turns a decision into one tagged limit order and keeps the position flat on shutdown.

The paper broker is the default. A second submit with the same tag does not
create a second order. After 25 price changes the working order is cancelled
and a new tag is used. Shutdown is clean only when the position is flat and
every broker order is one this engine knows.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal

from scalper.execution.paper import PaperBroker
from scalper.execution.states import ManagedOrder, OrderStatus, note_modification
from scalper.kite.tags import make_order_tag
from scalper.risk.gate import Intent, RiskDecision, RiskSnapshot, assess


class UnmanagedPosition(RuntimeError):
    """Shutdown stopped because a position or an unknown order is still open."""


@dataclass(frozen=True)
class BrokerView:
    tag: str
    broker_order_id: str
    status: OrderStatus
    side: str
    limit_price: Decimal
    quantity: int
    filled_quantity: int


@dataclass(frozen=True)
class SubmitResult:
    order: ManagedOrder | None
    decision: RiskDecision


@dataclass(frozen=True)
class ShutdownReport:
    clean: bool
    open_quantity: int


class ExecutionEngine:
    def __init__(self, broker: PaperBroker, snapshot: RiskSnapshot, *, lot_size: int) -> None:
        self.broker = broker
        self.snapshot = snapshot
        self.lot_size = lot_size
        self.orders: dict[str, ManagedOrder] = {}
        self.position_quantity = 0
        self.unmanaged: list[str] = []
        self._replace_sequence = 1

    def submit_entry(
        self,
        *,
        side: str,
        limit_price: Decimal,
        quantity: int,
        stop_price: Decimal,
        strategy_id: str,
        timestamp_ms: int,
    ) -> SubmitResult:
        return self._submit(
            intent=Intent.ENTRY,
            side=side,
            limit_price=limit_price,
            quantity=quantity,
            strategy_id=strategy_id,
            timestamp_ms=timestamp_ms,
            planned_loss=abs(limit_price - stop_price) * quantity,
        )

    def submit_exit(
        self,
        *,
        side: str,
        limit_price: Decimal,
        quantity: int,
        strategy_id: str,
        timestamp_ms: int,
    ) -> SubmitResult:
        return self._submit(
            intent=Intent.EXIT,
            side=side,
            limit_price=limit_price,
            quantity=quantity,
            strategy_id=strategy_id,
            timestamp_ms=timestamp_ms,
            planned_loss=Decimal("0"),
        )

    def modify_price(self, tag: str, limit_price: Decimal, *, strategy_id: str, timestamp_ms: int) -> SubmitResult:
        current = self.orders[tag]
        updated, action = note_modification(current, limit_price)
        if action == "modify":
            stored = self.broker.replace_price(tag, limit_price, updated.modifications)
            self.orders[tag] = stored
            return SubmitResult(stored, RiskDecision(True, "allow", "price modified"))
        cancelled = self.broker.cancel_tag(tag)
        self.orders[tag] = cancelled
        sequence = self._replace_sequence
        self._replace_sequence += 1
        return self._submit(
            intent=Intent(current.intent),
            side=current.side,
            limit_price=limit_price,
            quantity=current.remaining,
            strategy_id=strategy_id,
            timestamp_ms=timestamp_ms,
            planned_loss=Decimal("0"),
            sequence=sequence,
        )

    def on_trade(self, price: Decimal, quantity: int) -> None:
        for order in self.broker.on_trade(price, quantity):
            previous = self.orders.get(order.tag)
            already = 0 if previous is None else previous.filled_quantity
            delta = order.filled_quantity - already
            self.orders[order.tag] = order
            if delta > 0:
                signed = delta if order.side == "buy" else -delta
                self.position_quantity += signed

    def reconcile(self, broker_orders: list[BrokerView]) -> list[str]:
        problems: list[str] = []
        for view in broker_orders:
            local = self.orders.get(view.tag)
            if local is None or local.broker_order_id != view.broker_order_id:
                if view.broker_order_id not in self.unmanaged:
                    self.unmanaged.append(view.broker_order_id)
                problems.append(f"unknown order {view.broker_order_id}")
                continue
            if local.filled_quantity != view.filled_quantity:
                problems.append(f"fill mismatch {view.tag}")
                self.orders[view.tag] = replace(
                    local,
                    filled_quantity=view.filled_quantity,
                    status=view.status,
                )
        return problems

    def recover(self, broker_orders: list[BrokerView]) -> None:
        self.orders.clear()
        self.unmanaged.clear()
        self.position_quantity = 0
        for view in broker_orders:
            if not view.tag:
                self.unmanaged.append(view.broker_order_id)
                continue
            self.orders[view.tag] = ManagedOrder(
                tag=view.tag,
                side=view.side,
                limit_price=view.limit_price,
                quantity=view.quantity,
                filled_quantity=view.filled_quantity,
                status=view.status,
                broker_order_id=view.broker_order_id,
            )
            signed = view.filled_quantity if view.side == "buy" else -view.filled_quantity
            self.position_quantity += signed

    def shutdown(self, last_price: Decimal | None) -> ShutdownReport:
        for tag, order in list(self.orders.items()):
            if not order.working:
                continue
            if tag not in self.broker.orders:
                if order.broker_order_id not in self.unmanaged:
                    self.unmanaged.append(order.broker_order_id or tag)
                continue
            self.orders[tag] = self.broker.cancel_tag(tag)
        if self.position_quantity != 0:
            if last_price is None:
                raise UnmanagedPosition("open position and no price to flatten")
            side = "sell" if self.position_quantity > 0 else "buy"
            result = self.submit_exit(
                side=side,
                limit_price=last_price,
                quantity=abs(self.position_quantity),
                strategy_id="flatten",
                timestamp_ms=0,
            )
            if result.order is None or not result.decision.allowed:
                raise UnmanagedPosition("flatten was refused")
            self.on_trade(last_price, abs(self.position_quantity))
        if self.position_quantity != 0 or self.unmanaged:
            raise UnmanagedPosition("shutdown left an unmanaged position")
        return ShutdownReport(True, self.position_quantity)

    def _submit(
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
            ),
        )
        if not decision.allowed:
            return SubmitResult(None, decision)
        placed = self.broker.place(
            ManagedOrder(
                tag=tag,
                side=side,
                limit_price=limit_price,
                quantity=quantity,
                intent=intent.value,
            )
        )
        self.orders[tag] = placed
        return SubmitResult(placed, decision)


