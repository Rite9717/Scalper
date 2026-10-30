"""Move KiteTicker callbacks onto the asyncio loop.

KiteTicker runs on a Twisted thread. on_ticks only schedules work with
call_soon_threadsafe. It does not parse prices and it does not divide them.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass, field


class BridgeSignal:
    """A websocket lifecycle event, queued beside ticks."""

    def __init__(self, kind: str) -> None:
        self.kind = kind


@dataclass
class AsyncTickerBridge:
    loop: asyncio.AbstractEventLoop
    queue: asyncio.Queue
    dropped: int = 0
    received: int = 0
    connected_once: bool = False
    closed: asyncio.Event = field(init=False)
    last_error: str | None = None

    def __post_init__(self) -> None:
        self.closed = asyncio.Event()

    def on_ticks(self, _ws: object, ticks: Sequence[object]) -> None:
        payload = list(ticks)
        self.loop.call_soon_threadsafe(self._accept, payload)

    def on_close(self, _ws: object, code: object, reason: object) -> None:
        text = f"{code}: {reason}" if reason else str(code)
        self.loop.call_soon_threadsafe(self._mark_closed, text)

    def on_error(self, _ws: object, code: object, reason: object) -> None:
        text = f"{code}: {reason}" if reason else str(code)
        self.loop.call_soon_threadsafe(self._mark_error, text)

    def _accept(self, ticks: list[object]) -> None:
        for tick in ticks:
            self.received += 1
            try:
                self.queue.put_nowait(tick)
            except asyncio.QueueFull:
                self.dropped += 1

    def note_reconnect(self) -> None:
        self.loop.call_soon_threadsafe(self._enqueue, BridgeSignal("reconnect"))

    def _mark_closed(self, text: str) -> None:
        self.last_error = text
        self.closed.set()
        self._enqueue(BridgeSignal("disconnect"))

    def _mark_error(self, text: str) -> None:
        self.last_error = text

    def _enqueue(self, item: object) -> None:
        try:
            self.queue.put_nowait(item)
        except asyncio.QueueFull:
            self.dropped += 1
