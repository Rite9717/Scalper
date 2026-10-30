"""Async broker contract shared by the paper book and Kite."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol, runtime_checkable

from scalper.execution.states import ManagedOrder


@dataclass(frozen=True)
class PlaceOutcome:
    """Result of place_limit after a broker acknowledgement or an order-book poll.

    recovered is true when the tag was found on GET /orders after the POST
    did not finish. An ambiguous POST is never sent again.
    """

    order: ManagedOrder
    recovered: bool = False


@dataclass(frozen=True)
class BrokerPosition:
    tradingsymbol: str
    exchange: str
    product: str
    quantity: int
    average_price: Decimal
    overnight_quantity: int
    pnl: Decimal | None = None
    last_price: Decimal | None = None


@dataclass(frozen=True)
class PositionBook:
    net: tuple[BrokerPosition, ...]
    day: tuple[BrokerPosition, ...]


@dataclass(frozen=True)
class MarginSegment:
    enabled: bool
    net: Decimal
    cash: Decimal
    live_balance: Decimal


@dataclass(frozen=True)
class BrokerMargins:
    equity: MarginSegment
    commodity: MarginSegment


@runtime_checkable
class Broker(Protocol):
    async def place_limit(self, order: ManagedOrder) -> PlaceOutcome:
        """Place one LIMIT order. An ambiguous place is resolved by tag, then left unresolved."""

    async def cancel(self, order_id: str) -> ManagedOrder:
        """Cancel by broker order id. The id is not the tag."""

    async def modify_price(self, order_id: str, limit_price: Decimal) -> ManagedOrder:
        """Change the limit price. The order type stays LIMIT."""

    async def get_orders(self) -> list[ManagedOrder]:
        """Return the current order book."""

    async def get_positions(self) -> PositionBook:
        """Return net and day positions."""

    async def get_margins(self) -> BrokerMargins:
        """Return equity and commodity margins."""
