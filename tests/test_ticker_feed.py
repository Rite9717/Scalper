from __future__ import annotations

import asyncio
from datetime import timedelta
from decimal import Decimal

from scalper.data.bridge import AsyncTickerBridge
from scalper.data.models import Candle
from scalper.data.ticker import handle_ticker_connect
from scalper.paper.command import _KiteQuotes
from scalper.paper.runner import FeedDrop


class _Socket:
    MODE_FULL = "full"

    def subscribe(self, _tokens: list[int]) -> None:
        return None

    def set_mode(self, _mode: str, _tokens: list[int]) -> None:
        return None


def test_disconnect_and_reconnect_each_emit_a_feed_drop() -> None:
    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        bridge = AsyncTickerBridge(loop, asyncio.Queue())
        bridge.on_close(None, 1006, "bye")
        await asyncio.sleep(0)
        feed = _KiteQuotes(bridge, Decimal("0.05"), 1)
        first = await feed.__anext__()
        assert isinstance(first, FeedDrop)

        handle_ticker_connect(_Socket(), None, bridge=bridge, instrument_token=1)
        handle_ticker_connect(_Socket(), None, bridge=bridge, instrument_token=1)
        await asyncio.sleep(0)
        second = await feed.__anext__()
        assert isinstance(second, FeedDrop)
        assert bridge.queue.empty()

    asyncio.run(scenario())
