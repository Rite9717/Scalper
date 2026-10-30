"""Operator alerts. A feed stall or a kill switch is sent once until it clears."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

import httpx
import structlog

from scalper.logging import decision_logger
from scalper.monitor.metrics import Metrics


class AlertSink(Protocol):
    async def send(self, text: str) -> None: ...


class RecordingSink:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def send(self, text: str) -> None:
        self.messages.append(text)


class TelegramSink:
    def __init__(self, token: str, chat_id: str, client: httpx.AsyncClient) -> None:
        self._token = token
        self._chat_id = chat_id
        self._client = client

    async def send(self, text: str) -> None:
        try:
            response = await self._client.post(
                f"https://api.telegram.org/bot{self._token}/sendMessage",
                json={"chat_id": self._chat_id, "text": text},
            )
        except httpx.HTTPError:
            structlog.get_logger().error("telegram_delivery_failed")
            return
        if response.status_code >= 400:
            structlog.get_logger().error("telegram_delivery_failed", status_code=response.status_code)


@dataclass
class Monitor:
    metrics: Metrics
    sink: AlertSink
    _feed_open: bool = False
    _kill_open: bool = False
    _blind_open: bool = False

    async def risk_veto(self, reason: str) -> None:
        self.metrics.risk_vetoes.labels(reason=reason).inc()
        decision_logger().info("risk_veto", reason=reason)

    async def order_rejected(self, reason: str) -> None:
        self.metrics.orders.labels(event="rejected").inc()
        await self._emit("reject", f"Order rejected: {reason}")

    async def order_submitted(self) -> None:
        self.metrics.orders.labels(event="submitted").inc()

    async def feed_stall(self, age_seconds: float) -> None:
        self.metrics.quote_age_seconds.set(age_seconds)
        self.metrics.feed_stalled.set(1)
        if self._feed_open:
            return
        self._feed_open = True
        await self._emit("feed_stall", f"Feed stall. Last quote is {age_seconds:.0f} seconds old.")

    async def stops_blind(self, seconds: float) -> None:
        self.metrics.stops_blind_seconds.set(seconds)
        if self._blind_open:
            return
        self._blind_open = True
        await self._emit("stops_blind", f"Open position and the feed is blind. {seconds:.0f} seconds without a tick.")

    async def stops_covered(self) -> None:
        self._blind_open = False
        self.metrics.stops_blind_seconds.set(0)

    def beat(self, unix_time: float) -> None:
        self.metrics.heartbeat.set(unix_time)

    async def feed_resumed(self) -> None:
        self._feed_open = False
        self.metrics.feed_stalled.set(0)
        self.metrics.quote_age_seconds.set(0)

    async def kill_switch(self, engaged: bool, reason: str) -> None:
        self.metrics.kill_switch.set(1 if engaged else 0)
        if engaged:
            if self._kill_open:
                return
            self._kill_open = True
            await self._emit("kill_switch", f"Kill switch on: {reason}")
            return
        self._kill_open = False
        await self._emit("kill_switch", "Kill switch cleared.")

    async def daily_summary(self, *, trades: int, net_pnl: Decimal, vetoes: int) -> None:
        self.metrics.daily_pnl.set(float(net_pnl))
        await self._emit(
            "daily_summary",
            f"Daily summary. Trades {trades}. Net pnl {net_pnl}. Risk vetoes {vetoes}.",
        )

    async def _emit(self, kind: str, text: str) -> None:
        self.metrics.alerts.labels(kind=kind).inc()
        decision_logger().info("alert", kind=kind)
        await self.sink.send(text)
