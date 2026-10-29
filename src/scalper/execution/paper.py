"""In-memory broker. A repeated tag returns the original order and does not open another.

The execution engine calls the synchronous methods. The async methods are the
Broker protocol. None of them open a socket.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from scalper.backtest.fills import on_tick
from scalper.execution.broker import (
    BrokerMargins,
    BrokerPosition,
    MarginSegment,
    PlaceOutcome,
    PositionBook,
)
from scalper.execution.errors import BrokerError, InputException
from scalper.execution.states import ManagedOrder, accept, fill, reject
from scalper.kite.api_facts import KITE_MAX_MODIFICATIONS_PER_ORDER


class PaperBroker:
    def __init__(
        self,
        tick_size: Decimal,
        lot_size: int,
        *,
        tradingsymbol: str = "",
        equity_cash: Decimal = Decimal("0"),
    ) -> None:
        if tick_size <= 0 or lot_size < 1:
            raise ValueError("tick size and lot size must be positive")
        self.tick_size = tick_size
        self.lot_size = lot_size
        self.tradingsymbol = tradingsymbol
        self.equity_cash = equity_cash
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

    def cancel_tag(self, tag: str) -> ManagedOrder:
        from scalper.execution.states import cancel

        stored = cancel(self.orders[tag])
        self.orders[tag] = stored
        return stored

    async def place_limit(self, order: ManagedOrder) -> PlaceOutcome:
        return PlaceOutcome(self.place(order))

    async def cancel(self, order_id: str) -> ManagedOrder:
        return self.cancel_tag(self._tag_for_order_id(order_id))

    async def modify_price(self, order_id: str, limit_price: Decimal) -> ManagedOrder:
        tag = self._tag_for_order_id(order_id)
        order = self.orders[tag]
        if not order.working:
            raise InputException(f"cannot modify an order in status {order.status.value}")
        if order.modifications >= KITE_MAX_MODIFICATIONS_PER_ORDER:
            raise InputException("order has reached the modification cap")
        if on_tick(limit_price, self.tick_size) != limit_price:
            raise InputException("limit price is not on the tick")
        return self.replace_price(tag, limit_price, order.modifications + 1)

    async def get_orders(self) -> list[ManagedOrder]:
        return list(self.orders.values())

    async def get_positions(self) -> PositionBook:
        quantity, average = _net_position(list(self.orders.values()))
        if quantity == 0:
            return PositionBook((), ())
        position = BrokerPosition(
            tradingsymbol=self.tradingsymbol,
            exchange="NFO",
            product="MIS",
            quantity=quantity,
            average_price=average,
            overnight_quantity=0,
        )
        return PositionBook((position,), (position,))

    async def get_margins(self) -> BrokerMargins:
        equity = MarginSegment(
            enabled=True,
            net=self.equity_cash,
            cash=self.equity_cash,
            live_balance=self.equity_cash,
        )
        commodity = MarginSegment(
            enabled=False,
            net=Decimal("0"),
            cash=Decimal("0"),
            live_balance=Decimal("0"),
        )
        return BrokerMargins(equity=equity, commodity=commodity)

    def _tag_for_order_id(self, order_id: str) -> str:
        matches = [order.tag for order in self.orders.values() if order.broker_order_id == order_id]
        if len(matches) != 1:
            raise BrokerError(f"unknown broker order {order_id}")
        return matches[0]

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


def _net_position(orders: list[ManagedOrder]) -> tuple[int, Decimal]:
    quantity = 0
    average = Decimal("0")
    for order in orders:
        if order.filled_quantity < 1:
            continue
        delta = order.filled_quantity if order.side == "buy" else -order.filled_quantity
        price = order.average_price
        if quantity == 0 or quantity * delta > 0:
            new_abs = abs(quantity + delta)
            average = ((average * Decimal(abs(quantity))) + (price * Decimal(abs(delta)))) / Decimal(new_abs)
            quantity += delta
            continue
        quantity += delta
        if quantity == 0:
            average = Decimal("0")
        elif quantity * (quantity - delta) < 0:
            average = price
    return quantity, average
