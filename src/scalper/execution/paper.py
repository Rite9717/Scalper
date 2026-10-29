"""In-memory broker. A repeated tag returns the original order and does not open another."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from scalper.backtest.fills import on_tick
from scalper.execution.states import ManagedOrder, accept, fill, reject


class PaperBroker:
    def __init__(self, tick_size: Decimal, lot_size: int) -> None:
        if tick_size <= 0 or lot_size < 1:
            raise ValueError("tick size and lot size must be positive")
        self.tick_size = tick_size
        self.lot_size = lot_size
        self.orders: dict[str, ManagedOrder] = {}
        self.last_price: Decimal | None = None
        self._sequence = 0

    def place(self, order: ManagedOrder) -> ManagedOrder:
        existing = self.orders.get(order.tag)
        if existing is not None:
            return existing
        if order.quantity < 1 or order.quantity % self.lot_size != 0:
            stored = reject(order)
        elif on_tick(order.limit_price, self.tick_size) != order.limit_price:
            stored = reject(order)
        else:
            self._sequence += 1
            stored = accept(order, f"paper-{self._sequence}")
        self.orders[order.tag] = stored
        return stored

    def cancel(self, tag: str) -> ManagedOrder:
        from scalper.execution.states import cancel

        stored = cancel(self.orders[tag])
        self.orders[tag] = stored
        return stored

    def replace_price(self, tag: str, limit_price: Decimal, modifications: int) -> ManagedOrder:
        updated = replace(self.orders[tag], limit_price=limit_price, modifications=modifications)
        self.orders[tag] = updated
        return updated

    def on_trade(self, price: Decimal, quantity: int) -> list[ManagedOrder]:
        self.last_price = price
        changed: list[ManagedOrder] = []
        for tag, order in list(self.orders.items()):
            if not order.working:
                continue
            traded = _fill_quantity(order, price, quantity)
            if traded < 1:
                continue
            updated = fill(order, traded, order.limit_price)
            self.orders[tag] = updated
            changed.append(updated)
        return changed


def _fill_quantity(order: ManagedOrder, price: Decimal, quantity: int) -> int:
    if quantity < 1:
        return 0
    if order.side == "buy":
        if price < order.limit_price:
            return min(order.remaining, quantity)
        if price == order.limit_price and quantity >= order.remaining:
            return order.remaining
        return 0
    if order.side == "sell":
        if price > order.limit_price:
            return min(order.remaining, quantity)
        if price == order.limit_price and quantity >= order.remaining:
            return order.remaining
        return 0
    return 0
