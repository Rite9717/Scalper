"""Live LIMIT orders. Paper mode never reaches the HTTP call."""

from __future__ import annotations

import httpx

from scalper.compliance import assert_broker_call_allowed, assert_order_type_allowed
from scalper.execution.states import ManagedOrder
from scalper.kite.auth import authenticated_headers


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
    ) -> None:
        self.trading_mode = trading_mode
        self.api_key = api_key
        self.access_token = access_token
        self.static_ip = static_ip
        self.observed_ip = observed_ip
        self.client = client
        self.tradingsymbol = tradingsymbol
        self.exchange = exchange

    async def place_limit(self, order: ManagedOrder) -> str:
        assert_order_type_allowed("LIMIT")
        assert_broker_call_allowed(
            method="POST",
            path="/orders/regular",
            trading_mode=self.trading_mode,
            configured_static_ip=self.static_ip,
            observed_egress_ip=self.observed_ip,
        )
        response = await self.client.post(
            "/orders/regular",
            headers=authenticated_headers(self.api_key, self.access_token),
            data={
                "tradingsymbol": self.tradingsymbol,
                "exchange": self.exchange,
                "transaction_type": order.side.upper(),
                "order_type": "LIMIT",
                "quantity": order.quantity,
                "product": "MIS",
                "validity": "DAY",
                "price": format(order.limit_price, "f"),
                "tag": order.tag,
            },
        )
        body = response.json()
        if response.status_code >= 400 or body.get("status") != "success":
            raise RuntimeError("kite order was refused")
        return str(body["data"]["order_id"])
