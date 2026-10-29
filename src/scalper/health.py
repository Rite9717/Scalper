"""Process health endpoint. It does not call Kite, Redis, or Postgres."""

from __future__ import annotations

import asyncio
import json

from scalper import PHASE
from scalper.config import Settings
from scalper.monitor.metrics import Metrics, build_metrics, render_metrics

_MAX_REQUEST_BYTES = 8192


def http_health_payload(settings: Settings) -> dict[str, object]:
    return {
        "status": "ok",
        "service": "scalper",
        "phase": PHASE,
        "trading_mode": settings.trading_mode,
    }


class HealthServer:
    def __init__(
        self,
        settings: Settings,
        host: str = "127.0.0.1",
        port: int = 8080,
        metrics: Metrics | None = None,
    ) -> None:
        self._settings = settings
        self._host = host
        self._port = port
        self._metrics = metrics or build_metrics()
        self._server: asyncio.AbstractServer | None = None

    @property
    def port(self) -> int:
        if self._server is None or not self._server.sockets:
            raise RuntimeError("health server is not running")
        return int(self._server.sockets[0].getsockname()[1])

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self._host, self._port)

    async def stop(self) -> None:
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            blob = await asyncio.wait_for(reader.read(_MAX_REQUEST_BYTES), timeout=5)
            path = _request_path(blob)
            if path == "/health":
                body = json.dumps(http_health_payload(self._settings)).encode("utf-8")
                content_type = "application/json"
                status = "200 OK"
            elif path == "/metrics":
                body = render_metrics(self._metrics)
                content_type = "text/plain; version=0.0.4"
                status = "200 OK"
            else:
                body = b'{"status":"not_found"}'
                content_type = "application/json"
                status = "404 Not Found"
            header = (
                f"HTTP/1.1 {status}\r\n"
                f"Content-Type: {content_type}\r\n"
                f"Content-Length: {len(body)}\r\n"
                "Connection: close\r\n"
                "\r\n"
            ).encode("ascii")
            writer.write(header + body)
            await writer.drain()
        except (asyncio.TimeoutError, ConnectionError, UnicodeError):
            return
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                return


def _request_path(blob: bytes) -> str:
    line = blob.split(b"\r\n", 1)[0].decode("ascii", errors="replace")
    parts = line.split(" ")
    if len(parts) < 2 or parts[0] != "GET":
        return ""
    return parts[1].split("?", 1)[0]
