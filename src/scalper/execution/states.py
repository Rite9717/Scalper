"""Order lifecycle. Illegal transitions fail instead of being guessed."""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from enum import Enum

from scalper.kite.api_facts import KITE_MAX_MODIFICATIONS_PER_ORDER


class OrderStatus(str, Enum):
    NEW = "new"
    OPEN = "open"
    PARTIAL = "partial"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


TERMINAL = {
    OrderStatus.FILLED,
    OrderStatus.CANCELLED,
    OrderStatus.REJECTED,
    OrderStatus.EXPIRED,
}
WORKING = {OrderStatus.OPEN, OrderStatus.PARTIAL}


class IllegalTransition(RuntimeError):
    """The order cannot move from its current status to the requested one."""


@dataclass(frozen=True)
class ManagedOrder:
    tag: str
    side: str
    limit_price: Decimal
    quantity: int
    filled_quantity: int = 0
    average_price: Decimal = Decimal("0")
    status: OrderStatus = OrderStatus.NEW
    modifications: int = 0
    broker_order_id: str | None = None
    intent: str = "entry"
    tradingsymbol: str = ""
    exchange: str = ""
    product: str = "MIS"

    @property
    def remaining(self) -> int:
        return self.quantity - self.filled_quantity

    @property
    def working(self) -> bool:
        return self.status in WORKING


def accept(order: ManagedOrder, broker_order_id: str) -> ManagedOrder:
    _require(order, {OrderStatus.NEW}, "accept")
    return replace(order, status=OrderStatus.OPEN, broker_order_id=broker_order_id)


def reject(order: ManagedOrder) -> ManagedOrder:
    _require(order, {OrderStatus.NEW}, "reject")
    return replace(order, status=OrderStatus.REJECTED)


def cancel(order: ManagedOrder) -> ManagedOrder:
    _require(order, WORKING, "cancel")
    return replace(order, status=OrderStatus.CANCELLED)


def expire(order: ManagedOrder) -> ManagedOrder:
    _require(order, WORKING, "expire")
    return replace(order, status=OrderStatus.EXPIRED)


def fill(order: ManagedOrder, quantity: int, price: Decimal) -> ManagedOrder:
    _require(order, WORKING, "fill")
    if quantity < 1 or quantity > order.remaining:
        raise IllegalTransition("fill quantity is outside the remaining size")
    filled = order.filled_quantity + quantity
    average = ((order.average_price * order.filled_quantity) + (price * quantity)) / filled
    status = OrderStatus.FILLED if filled == order.quantity else OrderStatus.PARTIAL
    return replace(order, filled_quantity=filled, average_price=average, status=status)


def note_modification(order: ManagedOrder, limit_price: Decimal) -> tuple[ManagedOrder, str]:
    """Return the modified order, or the unchanged order and the action 'replace'."""
    _require(order, WORKING, "modify")
    if order.modifications >= KITE_MAX_MODIFICATIONS_PER_ORDER:
        return order, "replace"
    return replace(order, limit_price=limit_price, modifications=order.modifications + 1), "modify"


def _require(order: ManagedOrder, allowed: set[OrderStatus], action: str) -> None:
    if order.status not in allowed:
        raise IllegalTransition(f"cannot {action} an order in status {order.status.value}")
