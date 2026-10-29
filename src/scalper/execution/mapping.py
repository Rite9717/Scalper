"""Map Kite order, position, and margin payloads into local types.

Working statuses stay open. A fill that is neither zero nor complete becomes
PARTIAL. COMPLETE is FILLED only when filled_quantity equals quantity.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from scalper.execution.broker import BrokerMargins, BrokerPosition, MarginSegment, PositionBook
from scalper.execution.errors import DataException, OrderMappingError
from scalper.execution.states import ManagedOrder, OrderStatus

# Kite's order book has no PARTIAL status. These remain working until a
# terminal status arrives. CANCEL PENDING is not treated as cancelled.
_WORKING = {
    "OPEN",
    "TRIGGER PENDING",
    "PUT ORDER REQ RECEIVED",
    "VALIDATION PENDING",
    "OPEN PENDING",
    "MODIFY VALIDATION PENDING",
    "MODIFY PENDING",
    "CANCEL PENDING",
    "AMO REQ RECEIVED",
}


def map_kite_order(row: dict[str, Any]) -> ManagedOrder:
    status_text = _text(row, "status").upper()
    quantity = _int(row, "quantity")
    filled = _int(row, "filled_quantity")
    if quantity < 1:
        raise OrderMappingError("order quantity must be positive")
    if filled < 0 or filled > quantity:
        raise OrderMappingError("filled quantity is outside the order quantity")
    side = _text(row, "transaction_type").upper()
    if side not in {"BUY", "SELL"}:
        raise OrderMappingError("transaction_type must be BUY or SELL")
    order_id = row.get("order_id")
    if isinstance(order_id, bool) or order_id is None or str(order_id) == "":
        raise OrderMappingError("order is missing an order id")
    tag_value = row.get("tag")
    tag = "" if tag_value is None else str(tag_value)
    return ManagedOrder(
        tag=tag,
        side=side.lower(),
        limit_price=_decimal(row, "price"),
        quantity=quantity,
        filled_quantity=filled,
        average_price=_decimal(row, "average_price"),
        status=_status(status_text, quantity, filled),
        broker_order_id=str(order_id),
        tradingsymbol=_text(row, "tradingsymbol"),
        exchange=_text(row, "exchange"),
        product=_text(row, "product"),
    )


def map_kite_orders(rows: Any) -> list[ManagedOrder]:
    if not isinstance(rows, list):
        raise DataException("order book was not a list", error_type="DataException")
    orders: list[ManagedOrder] = []
    for row in rows:
        if not isinstance(row, dict):
            raise OrderMappingError("order row was not an object")
        orders.append(map_kite_order(row))
    return orders


def map_positions(data: Any) -> PositionBook:
    if not isinstance(data, dict) or "net" not in data or "day" not in data:
        raise DataException("positions payload was missing net or day", error_type="DataException")
    return PositionBook(
        net=_position_list(data["net"], "net"),
        day=_position_list(data["day"], "day"),
    )


def map_margins(data: Any) -> BrokerMargins:
    if not isinstance(data, dict):
        raise DataException("margins payload was not an object", error_type="DataException")
    try:
        equity = data["equity"]
        commodity = data["commodity"]
    except KeyError as exc:
        raise DataException("margins payload was missing a segment", error_type="DataException") from exc
    return BrokerMargins(equity=_segment(equity, "equity"), commodity=_segment(commodity, "commodity"))


def _status(status_text: str, quantity: int, filled: int) -> OrderStatus:
    if status_text == "COMPLETE":
        if filled != quantity:
            raise OrderMappingError("COMPLETE order is not fully filled")
        return OrderStatus.FILLED
    if status_text == "REJECTED":
        if filled != 0:
            raise OrderMappingError("REJECTED order reports a fill")
        return OrderStatus.REJECTED
    if status_text == "CANCELLED":
        if filled == quantity:
            raise OrderMappingError("CANCELLED order is fully filled")
        return OrderStatus.CANCELLED
    if status_text in _WORKING:
        if filled == quantity:
            return OrderStatus.FILLED
        if filled > 0:
            return OrderStatus.PARTIAL
        return OrderStatus.OPEN
    raise OrderMappingError(f"unmapped Kite status {status_text}")


def _position_list(rows: Any, book: str) -> tuple[BrokerPosition, ...]:
    if not isinstance(rows, list):
        raise DataException(f"positions {book} book was not a list", error_type="DataException")
    mapped: list[BrokerPosition] = []
    for row in rows:
        if not isinstance(row, dict):
            raise OrderMappingError(f"positions {book} row was not an object")
        mapped.append(
            BrokerPosition(
                tradingsymbol=_text(row, "tradingsymbol"),
                exchange=_text(row, "exchange"),
                product=_text(row, "product"),
                quantity=_int(row, "quantity"),
                average_price=_decimal(row, "average_price"),
                overnight_quantity=_int(row, "overnight_quantity"),
            )
        )
    return tuple(mapped)


def _segment(segment: Any, name: str) -> MarginSegment:
    if not isinstance(segment, dict):
        raise DataException(f"{name} margins were not an object", error_type="DataException")
    available = segment.get("available")
    if not isinstance(available, dict):
        raise DataException(f"{name} margins were missing available cash", error_type="DataException")
    enabled = segment.get("enabled")
    if type(enabled) is not bool:
        raise DataException(f"{name} margins enabled flag was not a boolean", error_type="DataException")
    return MarginSegment(
        enabled=enabled,
        net=_decimal(segment, "net"),
        cash=_decimal(available, "cash"),
        live_balance=_decimal(available, "live_balance"),
    )


def _text(row: dict[str, Any], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        raise OrderMappingError(f"{field} is missing")
    return value.strip()


def _int(row: dict[str, Any], field: str) -> int:
    value = row.get(field)
    if type(value) is bool or not isinstance(value, int):
        raise OrderMappingError(f"{field} is not an integer")
    return value


def _decimal(row: dict[str, Any], field: str) -> Decimal:
    value = row.get(field)
    if type(value) is bool or value is None:
        raise OrderMappingError(f"{field} is missing")
    try:
        if isinstance(value, Decimal):
            parsed = value
        elif type(value) is int:
            parsed = Decimal(value)
        elif isinstance(value, str):
            parsed = Decimal(value)
        else:
            raise OrderMappingError(f"{field} is not a decimal")
    except InvalidOperation as exc:
        raise OrderMappingError(f"{field} is not a decimal") from exc
    if parsed < 0:
        raise OrderMappingError(f"{field} must not be negative")
    return parsed
