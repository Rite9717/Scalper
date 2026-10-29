"""Futures transaction costs.

Defaults follow Zerodha's published F&O futures table and the NSE STT rate for
a sale of futures, both as published for dates on and after 1 April 2026:
0.05% STT on the sell side. Exchange, stamp, and SEBI rates change. Override
them in settings when the tariff changes. Stamp and STT are outside the GST base.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP

PAISE = Decimal("0.01")

# Checked against https://zerodha.com/charges/ and the NSE STT table on 27 Oct 2026.
DEFAULT_BROKERAGE_RATE = Decimal("0.0003")
DEFAULT_BROKERAGE_CAP = Decimal("20")
DEFAULT_STT_SELL_RATE = Decimal("0.0005")
DEFAULT_EXCHANGE_RATE = Decimal("0.0000183")
DEFAULT_SEBI_RATE = Decimal("0.000001")
DEFAULT_STAMP_BUY_RATE = Decimal("0.00002")
DEFAULT_GST_RATE = Decimal("0.18")


@dataclass(frozen=True)
class ChargeSchedule:
    brokerage_rate: Decimal = DEFAULT_BROKERAGE_RATE
    brokerage_cap: Decimal = DEFAULT_BROKERAGE_CAP
    stt_sell_rate: Decimal = DEFAULT_STT_SELL_RATE
    exchange_rate: Decimal = DEFAULT_EXCHANGE_RATE
    sebi_rate: Decimal = DEFAULT_SEBI_RATE
    stamp_buy_rate: Decimal = DEFAULT_STAMP_BUY_RATE
    gst_rate: Decimal = DEFAULT_GST_RATE
    spread_ticks: int = 1

    def __post_init__(self) -> None:
        for name in (
            "brokerage_rate",
            "brokerage_cap",
            "stt_sell_rate",
            "exchange_rate",
            "sebi_rate",
            "stamp_buy_rate",
            "gst_rate",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} cannot be negative")
        if self.spread_ticks < 0:
            raise ValueError("spread_ticks cannot be negative")


@dataclass(frozen=True)
class ChargeBreakdown:
    brokerage: Decimal
    exchange: Decimal
    sebi: Decimal
    stt: Decimal
    stamp: Decimal
    gst: Decimal
    spread: Decimal

    @property
    def total(self) -> Decimal:
        return self.brokerage + self.exchange + self.sebi + self.stt + self.stamp + self.gst + self.spread


def _money(value: Decimal) -> Decimal:
    return value.quantize(PAISE, rounding=ROUND_HALF_UP)


def side_charges(schedule: ChargeSchedule, *, turnover: Decimal, side: str) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal]:
    """Return brokerage, exchange, sebi, stt, stamp for one fill."""
    if side not in {"buy", "sell"}:
        raise ValueError("side must be buy or sell")
    if turnover < 0:
        raise ValueError("turnover cannot be negative")
    brokerage = _money(min(schedule.brokerage_cap, schedule.brokerage_rate * turnover))
    exchange = _money(schedule.exchange_rate * turnover)
    sebi = _money(schedule.sebi_rate * turnover)
    stt = _money(schedule.stt_sell_rate * turnover) if side == "sell" else Decimal("0.00")
    stamp = _money(schedule.stamp_buy_rate * turnover) if side == "buy" else Decimal("0.00")
    return brokerage, exchange, sebi, stt, stamp


def round_trip_charges(
    schedule: ChargeSchedule,
    *,
    entry_price: Decimal,
    exit_price: Decimal,
    quantity: int,
    entry_side: str,
    tick_size: Decimal,
) -> ChargeBreakdown:
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    if entry_side not in {"buy", "sell"}:
        raise ValueError("entry_side must be buy or sell")
    exit_side = "sell" if entry_side == "buy" else "buy"
    entry_turnover = entry_price * quantity
    exit_turnover = exit_price * quantity
    entry = side_charges(schedule, turnover=entry_turnover, side=entry_side)
    exit_ = side_charges(schedule, turnover=exit_turnover, side=exit_side)
    brokerage = entry[0] + exit_[0]
    exchange = entry[1] + exit_[1]
    sebi = entry[2] + exit_[2]
    stt = entry[3] + exit_[3]
    stamp = entry[4] + exit_[4]
    gst = _money(schedule.gst_rate * (brokerage + exchange + sebi))
    spread = _money(Decimal(schedule.spread_ticks) * tick_size * quantity)
    return ChargeBreakdown(
        brokerage=brokerage,
        exchange=exchange,
        sebi=sebi,
        stt=stt,
        stamp=stamp,
        gst=gst,
        spread=spread,
    )


def cost_gate_allows(
    schedule: ChargeSchedule,
    *,
    entry_price: Decimal,
    target_price: Decimal,
    quantity: int,
    entry_side: str,
    tick_size: Decimal,
    cost_multiple: Decimal,
) -> bool:
    """True when the planned target covers cost_multiple times the estimated round trip."""
    if cost_multiple < 0:
        raise ValueError("cost_multiple cannot be negative")
    reward = abs(target_price - entry_price) * quantity
    costs = round_trip_charges(
        schedule,
        entry_price=entry_price,
        exit_price=target_price,
        quantity=quantity,
        entry_side=entry_side,
        tick_size=tick_size,
    ).total
    return reward >= cost_multiple * costs
