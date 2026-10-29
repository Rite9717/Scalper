"""Convert KiteTicker rupee floats onto the contract tick.

KiteTicker has already divided binary paise by the segment divisor. These
helpers must not divide again. An int is rejected so a raw paise value cannot
be stored as if it were rupees.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP


class PriceError(ValueError):
    """A ticker price could not be placed on the contract tick."""


def rupees_to_tick(value: float, tick_size: Decimal) -> Decimal:
    if type(value) is not float:
        raise PriceError("ticker price must be a float in rupees")
    if tick_size <= 0:
        raise PriceError("tick size must be positive")
    quantized = Decimal(str(value)).quantize(tick_size, rounding=ROUND_HALF_UP)
    if quantized < 0:
        raise PriceError("ticker price must not be negative")
    return quantized
