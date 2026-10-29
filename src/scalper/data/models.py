"""Normalized market records. Prices are Decimal rupees on the contract tick."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class DepthLevel:
    side: str
    level: int
    price: Decimal
    quantity: int
    orders: int


@dataclass(frozen=True)
class NormalizedQuote:
    instrument_token: int
    received_at: datetime
    exchange_ts: datetime | None
    last_price: Decimal
    last_quantity: int
    volume: int
    average_price: Decimal
    oi: int | None
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    depth: tuple[DepthLevel, ...]

    def to_cache(self) -> dict[str, object]:
        return {
            "instrument_token": self.instrument_token,
            "received_at": self.received_at.isoformat(),
            "exchange_ts": None if self.exchange_ts is None else self.exchange_ts.isoformat(),
            "last_price": format(self.last_price, "f"),
            "last_quantity": self.last_quantity,
            "volume": self.volume,
            "average_price": format(self.average_price, "f"),
            "oi": self.oi,
            "open": format(self.open, "f"),
            "high": format(self.high, "f"),
            "low": format(self.low, "f"),
            "close": format(self.close, "f"),
            "depth": [
                {
                    "side": level.side,
                    "level": level.level,
                    "price": format(level.price, "f"),
                    "quantity": level.quantity,
                    "orders": level.orders,
                }
                for level in self.depth
            ],
        }


@dataclass(frozen=True)
class Candle:
    instrument_token: int
    bucket_start: datetime
    interval: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    oi: int | None
    source: str
