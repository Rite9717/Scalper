"""Subscribe the traded contract in full mode.

The real KiteTicker object is constructed only when a recording session
starts. Tests use a stand-in with the same subscribe and set_mode methods.
"""

from __future__ import annotations

from typing import Protocol

from scalper.data.bridge import AsyncTickerBridge
from scalper.kite.api_facts import TRADED_CONTRACT_MODE


class _TickerSocket(Protocol):
    MODE_FULL: str

    def subscribe(self, instrument_tokens: list[int]) -> None: ...

    def set_mode(self, mode: str, instrument_tokens: list[int]) -> None: ...


def arm_full_mode(ws: _TickerSocket, instrument_token: int) -> None:
    """Subscribe one token and request the 5-level depth packet."""
    token = int(instrument_token)
    mode = getattr(ws, "MODE_FULL", TRADED_CONTRACT_MODE)
    if mode != TRADED_CONTRACT_MODE:
        raise RuntimeError("ticker full mode is not the Kite full mode")
    ws.subscribe([token])
    ws.set_mode(mode, [token])


def connect_kite_ticker(
    *,
    api_key: str,
    access_token: str,
    bridge: AsyncTickerBridge,
    instrument_token: int,
) -> object:
    """Start KiteTicker on its own thread and arm full mode on connect."""
    from kiteconnect import KiteTicker

    ticker = KiteTicker(api_key, access_token)

    def on_connect(ws: _TickerSocket, _response: object) -> None:
        arm_full_mode(ws, instrument_token)

    ticker.on_ticks = bridge.on_ticks
    ticker.on_connect = on_connect
    ticker.on_close = bridge.on_close
    ticker.on_error = bridge.on_error
    ticker.connect(threaded=True)
    return ticker


def close_ticker(ticker: object) -> None:
    """Close the websocket and stop the ticker thread. Each call is attempted."""
    for name in ("close", "stop"):
        method = getattr(ticker, name, None)
        if method is None:
            continue
        try:
            method()
        except Exception:
            continue
