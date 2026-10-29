"""Kite Connect v3 orders. Paper mode never touches the HTTP client.

A connect failure is sent once more, because the request never reached Kite.
A timeout, dropped connection, 5xx, or unreadable body is polled on the order
book and is never posted again.
"""

from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any
from urllib.parse import quote

import httpx

from scalper.compliance import ComplianceError, assert_broker_call_allowed, assert_order_tag, assert_order_type_allowed
from scalper.execution.broker import BrokerMargins, PlaceOutcome, PositionBook
from scalper.execution.errors import (
    KITE_ERROR_TYPES,
    AmbiguousOrderOutcome,
    BrokerError,
    DataException,
    InputException,
    NetworkException,
)
from scalper.execution.mapping import map_kite_orders, map_margins, map_positions
from scalper.execution.states import ManagedOrder, OrderStatus, accept
from scalper.kite.auth import authenticated_headers

# Five reads spread across this window. The first poll is immediate.
AMBIGUOUS_PLACE_POLLS = 5
AMBIGUOUS_PLACE_WINDOW_SECONDS = 5.0
AMBIGUOUS_PLACE_INTERVAL_SECONDS = AMBIGUOUS_PLACE_WINDOW_SECONDS / (AMBIGUOUS_PLACE_POLLS - 1)

_SAFE_TO_RESEND = (httpx.ConnectError, httpx.ConnectTimeout)


class _NotSent(Exception):
    """The TCP connection failed before the request could reach Kite."""


class _Ambiguous(Exception):
    """The request may have been processed. It must not be posted again."""


class KiteBroker:
    def __init__(
        self,
        *,
        trading_mode: str,
        api_key: str,
        access_token: str,
        static_ip: str,
        observed_ip: str | None,
        client: httpx.AsyncClient,
        tradingsymbol: str,
        exchange: str = "NFO",
        poll_attempts: int = AMBIGUOUS_PLACE_POLLS,
        poll_interval: float = AMBIGUOUS_PLACE_INTERVAL_SECONDS,
    ) -> None:
        if poll_attempts < 1:
            raise ValueError("poll attempts must be at least 1")
        if poll_interval < 0:
            raise ValueError("poll interval must not be negative")
        self.trading_mode = trading_mode
        self.api_key = api_key
        self.access_token = access_token
        self.static_ip = static_ip
        self.observed_ip = observed_ip
        self.client = client
        self.tradingsymbol = tradingsymbol
        self.exchange = exchange
        self.poll_attempts = poll_attempts
        self.poll_interval = poll_interval

    async def place_limit(self, order: ManagedOrder) -> PlaceOutcome:
        assert_order_type_allowed("LIMIT")
        assert_order_tag(order.tag)
        self._require_placeable(order)
        try:
            order_id = await self._post_limit(order)
        except _NotSent:
            try:
                order_id = await self._post_limit(order)
            except _NotSent as exc:
                cause = exc.__cause__
                error_type = type(cause).__name__ if cause is not None else "ConnectError"
                raise NetworkException("place_limit did not reach Kite", error_type=error_type) from exc
            except _Ambiguous as exc:
                return await self._poll_for_tag(order, exc)
        except _Ambiguous as exc:
            return await self._poll_for_tag(order, exc)
        return PlaceOutcome(accept(order, order_id))

    async def cancel(self, order_id: str) -> ManagedOrder:
        path = _order_path(order_id)
        data = await self._request("DELETE", path, mode="strict")
        _require_order_id(data)
        return await self._book_order(order_id, "cancel")

    async def modify_price(self, order_id: str, limit_price: Decimal) -> ManagedOrder:
        assert_order_type_allowed("LIMIT")
        if limit_price <= 0:
            raise InputException("limit price must be positive")
        path = _order_path(order_id)
        data = await self._request(
            "PUT",
            path,
            form={
                "order_type": "LIMIT",
                "price": format(limit_price, "f"),
            },
            mode="strict",
        )
        _require_order_id(data)
        return await self._book_order(order_id, "modify")

    async def get_orders(self) -> list[ManagedOrder]:
        data = await self._request("GET", "/orders", mode="strict")
        return map_kite_orders(data)

    async def get_positions(self) -> PositionBook:
        data = await self._request("GET", "/portfolio/positions", mode="strict")
        return map_positions(data)

    async def get_margins(self) -> BrokerMargins:
        data = await self._request("GET", "/user/margins", mode="strict")
        return map_margins(data)

    async def _post_limit(self, order: ManagedOrder) -> str:
        side = order.side.upper()
        data = await self._request(
            "POST",
            "/orders/regular",
            form={
                "tradingsymbol": self.tradingsymbol,
                "exchange": self.exchange,
                "transaction_type": side,
                "order_type": "LIMIT",
                "quantity": order.quantity,
                "product": "MIS",
                "validity": "DAY",
                "price": format(order.limit_price, "f"),
                "tag": order.tag,
            },
            mode="place",
        )
        return _require_order_id(data)

    async def _poll_for_tag(self, order: ManagedOrder, cause: BaseException) -> PlaceOutcome:
        for attempt in range(self.poll_attempts):
            if attempt > 0 and self.poll_interval > 0:
                await asyncio.sleep(self.poll_interval)
            try:
                found = await self._poll_once(order.tag)
            except AmbiguousOrderOutcome:
                raise
            except BrokerError as exc:
                raise AmbiguousOrderOutcome(
                    "place_limit may have reached Kite and the order book could not be read"
                ) from exc
            if found is not None:
                return PlaceOutcome(found, recovered=True)
        raise AmbiguousOrderOutcome(
            "place_limit may have reached Kite and the tag did not appear on the order book"
        ) from cause

    async def _poll_once(self, tag: str) -> ManagedOrder | None:
        try:
            matches = await self._orders_with_tag(tag)
        except _Ambiguous:
            return None
        if len(matches) > 1:
            raise AmbiguousOrderOutcome("more than one order uses this tag")
        if len(matches) == 1:
            return matches[0]
        return None

    async def _orders_with_tag(self, tag: str) -> list[ManagedOrder]:
        data = await self._request("GET", "/orders", mode="read")
        return [item for item in map_kite_orders(data) if item.tag == tag]

    async def _book_order(self, order_id: str, action: str) -> ManagedOrder:
        try:
            return await self._require_order(order_id)
        except NetworkException as exc:
            raise AmbiguousOrderOutcome(
                f"the {action} was acknowledged but the order book could not be read"
            ) from exc

    async def _require_order(self, order_id: str) -> ManagedOrder:
        for item in await self.get_orders():
            if item.broker_order_id == order_id:
                return item
        raise DataException(
            "the order id from Kite is not in the order book",
            error_type="DataException",
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        form: dict[str, object] | None = None,
        mode: str,
    ) -> Any:
        assert_broker_call_allowed(
            method=method,
            path=path,
            trading_mode=self.trading_mode,
            configured_static_ip=self.static_ip,
            observed_egress_ip=self.observed_ip,
        )
        if self.trading_mode != "live":
            raise ComplianceError("paper mode does not call Kite")
        try:
            response = await self.client.request(
                method,
                path,
                headers=authenticated_headers(self.api_key, self.access_token),
                **({} if form is None else {"data": form}),
            )
        except httpx.TransportError as exc:
            if mode == "place" and isinstance(exc, _SAFE_TO_RESEND):
                raise _NotSent(str(exc)) from exc
            if mode == "strict":
                raise NetworkException(str(exc), error_type=type(exc).__name__) from exc
            raise _Ambiguous(str(exc)) from exc
        return _parse_data(response, mode=mode)

    def _require_placeable(self, order: ManagedOrder) -> None:
        if order.status is not OrderStatus.NEW:
            raise InputException("only a new order can be placed")
        side = order.side.upper()
        if side not in {"BUY", "SELL"}:
            raise InputException("transaction side must be buy or sell")
        if type(order.quantity) is not int or order.quantity < 1:
            raise InputException("quantity must be a positive integer")
        if order.limit_price <= 0:
            raise InputException("limit price must be positive")


def _order_path(order_id: str) -> str:
    if not order_id.isalnum():
        raise InputException("order id must be alphanumeric")
    return f"/orders/regular/{quote(order_id, safe='')}"


def _require_order_id(data: Any) -> str:
    if not isinstance(data, dict):
        raise DataException("Kite success payload did not contain an object", error_type="DataException")
    order_id = data.get("order_id")
    if isinstance(order_id, bool) or order_id is None or str(order_id) == "":
        raise DataException("Kite success payload did not contain an order id", error_type="DataException")
    return str(order_id)


def _parse_data(response: httpx.Response, *, mode: str) -> Any:
    if response.status_code >= 500:
        if mode == "strict":
            raise NetworkException(
                f"HTTP {response.status_code}",
                error_type="NetworkException",
                status_code=response.status_code,
            )
        raise _Ambiguous(f"HTTP {response.status_code}")
    try:
        body = json.loads(response.content, parse_float=Decimal)
    except json.JSONDecodeError as exc:
        if mode == "strict":
            raise DataException(
                "Kite returned a non-JSON body",
                error_type="DataException",
                status_code=response.status_code,
            ) from exc
        raise _Ambiguous("Kite returned a non-JSON body") from exc
    if not isinstance(body, dict):
        if mode == "strict":
            raise DataException(
                "Kite JSON body was not an object",
                error_type="DataException",
                status_code=response.status_code,
            )
        raise _Ambiguous("Kite JSON body was not an object")
    error_type = body.get("error_type")
    error_name = str(error_type) if error_type else None
    message = str(body.get("message") or f"HTTP {response.status_code}")
    if response.status_code < 400 and body.get("status") == "success":
        if "data" not in body:
            raise DataException("Kite success payload did not contain data", error_type="DataException")
        return body["data"]
    exception_type = KITE_ERROR_TYPES.get(error_name or "", BrokerError)
    raise exception_type(message, error_type=error_name, status_code=response.status_code)
