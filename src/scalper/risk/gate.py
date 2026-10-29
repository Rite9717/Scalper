"""Pure entry veto. There is no bypass argument.

Exits and flattens stay allowed when the account is in trouble, so a kill
switch can still get the position flat. A new entry does not.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from decimal import Decimal
from enum import Enum

from scalper.clock import ensure_ist
from scalper.compliance import LIVE_INITIAL_MAX_LOTS


class Intent(str, Enum):
    ENTRY = "entry"
    EXIT = "exit"
    FLATTEN = "flatten"


@dataclass(frozen=True)
class RiskSnapshot:
    now: datetime
    trading_mode: str
    lots_requested: int
    open_lots: int
    max_lots: int
    planned_loss: Decimal
    max_loss_per_trade: Decimal
    daily_pnl: Decimal
    daily_loss_limit: Decimal
    orders_last_second: int
    orders_last_minute: int
    max_orders_per_second: int
    max_orders_per_minute: int
    last_quote_at: datetime | None
    stale_after: timedelta
    available_margin: Decimal | None
    required_margin: Decimal | None
    flatten_time: time
    broker_pnl: Decimal | None
    internal_pnl: Decimal | None
    pnl_tolerance: Decimal
    exchange_time: datetime | None
    clock_drift_limit: timedelta
    recent_rejects: int
    max_rejects: int
    killed: bool


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    action: str
    reason: str


def assess(intent: Intent, snapshot: RiskSnapshot) -> RiskDecision:
    if intent not in {Intent.ENTRY, Intent.EXIT, Intent.FLATTEN}:
        return RiskDecision(False, "halt", "unknown intent")
    halt = _halt_reason(snapshot)
    if halt is not None:
        if intent is Intent.ENTRY:
            return RiskDecision(False, "halt", halt)
        return RiskDecision(True, "flatten", halt)
    flatten = _flatten_reason(snapshot)
    if flatten is not None:
        if intent is Intent.ENTRY:
            return RiskDecision(False, "flatten", flatten)
        return RiskDecision(True, "flatten", flatten)
    if intent is not Intent.ENTRY:
        return RiskDecision(True, "allow", "protective order")
    entry = _entry_reason(snapshot)
    if entry is not None:
        return RiskDecision(False, "veto", entry)
    return RiskDecision(True, "allow", "entry allowed")


def _halt_reason(snapshot: RiskSnapshot) -> str | None:
    if snapshot.killed:
        return "manual kill"
    if snapshot.recent_rejects >= snapshot.max_rejects:
        return "repeated order rejects"
    if snapshot.broker_pnl is not None and snapshot.internal_pnl is not None:
        if abs(snapshot.broker_pnl - snapshot.internal_pnl) > snapshot.pnl_tolerance:
            return "pnl mismatch versus broker"
    if snapshot.exchange_time is not None:
        drift = abs(ensure_ist(snapshot.now) - ensure_ist(snapshot.exchange_time))
        if drift > snapshot.clock_drift_limit:
            return "clock drift"
    if snapshot.last_quote_at is None:
        return "feed stall"
    age = ensure_ist(snapshot.now) - ensure_ist(snapshot.last_quote_at)
    if age > snapshot.stale_after:
        return "feed stall"
    return None


def _flatten_reason(snapshot: RiskSnapshot) -> str | None:
    if ensure_ist(snapshot.now).time() >= snapshot.flatten_time:
        return "flatten time"
    if snapshot.daily_pnl <= -snapshot.daily_loss_limit:
        return "daily loss limit"
    return None


def _entry_reason(snapshot: RiskSnapshot) -> str | None:
    if snapshot.trading_mode == "live" and snapshot.max_lots > LIVE_INITIAL_MAX_LOTS:
        return "live trading is capped at 1 lot"
    if snapshot.lots_requested < 1:
        return "order size must be at least 1 lot"
    if snapshot.open_lots + snapshot.lots_requested > snapshot.max_lots:
        return "position lot cap"
    if snapshot.planned_loss > snapshot.max_loss_per_trade:
        return "loss per trade cap"
    if snapshot.orders_last_second >= snapshot.max_orders_per_second:
        return "orders per second cap"
    if snapshot.orders_last_minute >= snapshot.max_orders_per_minute:
        return "orders per minute cap"
    if snapshot.available_margin is None or snapshot.required_margin is None:
        return "margin unknown"
    if snapshot.available_margin < snapshot.required_margin:
        return "insufficient margin"
    return None
