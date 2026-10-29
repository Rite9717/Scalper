"""Turn one KiteTicker full-mode callback into a Decimal quote.

The callback prices are rupees. This module does not divide them.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import Decimal

from scalper.clock import ensure_ist
from scalper.data.models import DepthLevel, NormalizedQuote
from scalper.data.prices import PriceError, rupees_to_tick


class QuoteRejected(ValueError):
    """The tick is not a full-mode quote for the traded contract."""


def normalize_full_tick(
    raw: Mapping[str, object],
    *,
    tick_size: Decimal,
    instrument_token: int,
    received_at: datetime,
) -> NormalizedQuote:
    if raw.get("mode") != "full":
        raise QuoteRejected("traded contract requires a full-mode tick")
    token = raw.get("instrument_token")
    if type(token) is not int or token != instrument_token:
        raise QuoteRejected("tick is for a different instrument")
    try:
        last_price = rupees_to_tick(_as_float(raw.get("last_price")), tick_size)
        average_price = rupees_to_tick(_as_float(raw.get("average_traded_price")), tick_size)
        ohlc = raw.get("ohlc")
        if not isinstance(ohlc, Mapping):
            raise QuoteRejected("full-mode tick has no ohlc")
        open_ = rupees_to_tick(_as_float(ohlc.get("open")), tick_size)
        high = rupees_to_tick(_as_float(ohlc.get("high")), tick_size)
        low = rupees_to_tick(_as_float(ohlc.get("low")), tick_size)
        close = rupees_to_tick(_as_float(ohlc.get("close")), tick_size)
        depth = _depth(raw.get("depth"), tick_size)
    except PriceError as exc:
        raise QuoteRejected(str(exc)) from exc
    return NormalizedQuote(
        instrument_token=token,
        received_at=ensure_ist(received_at),
        exchange_ts=_optional_time(raw.get("exchange_timestamp")),
        last_price=last_price,
        last_quantity=_as_int(raw.get("last_traded_quantity"), "last_traded_quantity"),
        volume=_as_int(raw.get("volume_traded"), "volume_traded"),
        average_price=average_price,
        oi=_optional_int(raw.get("oi")),
        open=open_,
        high=high,
        low=low,
        close=close,
        depth=depth,
    )


def _depth(raw: object, tick_size: Decimal) -> tuple[DepthLevel, ...]:
    if not isinstance(raw, Mapping):
        raise QuoteRejected("full-mode tick has no depth")
    levels: list[DepthLevel] = []
    for side in ("buy", "sell"):
        book = raw.get(side)
        if not isinstance(book, list) or len(book) != 5:
            raise QuoteRejected("full mode depth must have 5 levels on each side")
        for index, entry in enumerate(book, start=1):
            if not isinstance(entry, Mapping):
                raise QuoteRejected("depth level is not an object")
            levels.append(
                DepthLevel(
                    side=side,
                    level=index,
                    price=rupees_to_tick(_as_float(entry.get("price")), tick_size),
                    quantity=_as_int(entry.get("quantity"), "quantity"),
                    orders=_as_int(entry.get("orders"), "orders"),
                )
            )
    return tuple(levels)


def _as_float(value: object) -> float:
    if type(value) is not float:
        raise PriceError("ticker price must be a float in rupees")
    return value


def _as_int(value: object, field: str) -> int:
    if type(value) is not int or value < 0:
        raise QuoteRejected(f"{field} must be a non-negative int")
    return value


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value < 0:
        raise QuoteRejected("oi must be a non-negative int")
    return value


def _optional_time(value: object) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, datetime):
        raise QuoteRejected("exchange_timestamp must be a datetime")
    if value.tzinfo is None:
        from scalper.clock import IST

        value = value.replace(tzinfo=IST)
    return ensure_ist(value)
