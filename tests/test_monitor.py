from __future__ import annotations

import asyncio
from decimal import Decimal

import httpx
import pytest
from prometheus_client import CollectorRegistry

from scalper.config import Settings
from scalper.health import HealthServer
from scalper.logging import configure_logging
from scalper.monitor.alerts import Monitor, RecordingSink, TelegramSink
from scalper.monitor.metrics import build_metrics, render_metrics


def _monitor() -> tuple[Monitor, RecordingSink]:
    sink = RecordingSink()
    return Monitor(build_metrics(CollectorRegistry()), sink), sink


def test_feed_stall_and_kill_switch_alert_once_until_they_clear() -> None:
    async def scenario() -> RecordingSink:
        monitor, sink = _monitor()
        await monitor.feed_stall(12)
        await monitor.feed_stall(15)
        await monitor.feed_resumed()
        await monitor.feed_stall(9)
        await monitor.kill_switch(True, "operator stop")
        await monitor.kill_switch(True, "operator stop")
        await monitor.kill_switch(False, "")
        await monitor.order_rejected("margin")
        await monitor.daily_summary(trades=4, net_pnl=Decimal("-250.50"), vetoes=2)
        return sink

    sink = asyncio.run(scenario())
    assert sink.messages == [
        "Feed stall. Last quote is 12 seconds old.",
        "Feed stall. Last quote is 9 seconds old.",
        "Kill switch on: operator stop",
        "Kill switch cleared.",
        "Order rejected: margin",
        "Daily summary. Trades 4. Net pnl -250.50. Risk vetoes 2.",
    ]


def test_metrics_render_includes_the_kill_switch_and_the_feed() -> None:
    async def scenario() -> bytes:
        monitor, _sink = _monitor()
        await monitor.kill_switch(True, "operator stop")
        await monitor.feed_stall(8)
        await monitor.risk_veto("manual kill")
        return render_metrics(monitor.metrics)

    body = asyncio.run(scenario()).decode()
    assert "scalper_kill_switch 1.0" in body
    assert "scalper_feed_stalled 1.0" in body
    assert 'scalper_risk_vetoes_total{reason="manual kill"} 1.0' in body


def test_telegram_send_does_not_log_the_bot_token(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", json_output=True)
    token = "123456:secret-bot-token"

    def handler(request: httpx.Request) -> httpx.Response:
        assert token in str(request.url)
        assert "hello" in request.content.decode()
        return httpx.Response(500, json={"ok": False})

    async def scenario() -> None:
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            await TelegramSink(token, "99", client).send("hello")

    asyncio.run(scenario())
    logged = capsys.readouterr().err
    assert token not in logged
    assert "telegram_delivery_failed" in logged


def test_alerts_require_a_token_and_a_chat() -> None:
    with pytest.raises(Exception, match="TELEGRAM"):
        Settings(alerts_enabled=True)


def test_metrics_endpoint_is_served() -> None:
    async def scenario() -> bytes:
        server = HealthServer(Settings(), host="127.0.0.1", port=0)
        await server.start()
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
            writer.write(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            await writer.drain()
            payload = await reader.read()
            writer.close()
            await writer.wait_closed()
            return payload
        finally:
            await server.stop()

    payload = asyncio.run(scenario())
    assert b"200 OK" in payload
    assert b"scalper_kill_switch" in payload
