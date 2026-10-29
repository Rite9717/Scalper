"""Replay candles through a strategy, limit fills, and the charge model.

The signal bar never fills the order. The order is first eligible on the bar
`latency_bars` after the signal. A stop is taken before a target when one
bar could hit both. A new position is flattened at the configured clock time.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

from scalper.backtest.costs import ChargeSchedule, cost_gate_allows, round_trip_charges
from scalper.backtest.fills import adverse_exit_price, candle_limit_price, on_tick
from scalper.backtest.strategy import EntrySignal, MarketState, Strategy
from scalper.clock import IST, ensure_ist
from scalper.data.models import Candle
from scalper.risk.gate import Intent, RiskSnapshot, assess


@dataclass(frozen=True)
class BacktestSpec:
    tick_size: Decimal
    lot_size: int
    lots: int
    charges: ChargeSchedule
    cost_multiple: Decimal
    slippage_ticks: int
    latency_bars: int
    flatten_time: time
    expiry_dates: frozenset = frozenset()
    available_margin: Decimal | None = None
    max_loss_per_trade: Decimal = Decimal("3000")
    daily_loss_limit: Decimal = Decimal("5000")

    @property
    def quantity(self) -> int:
        return self.lot_size * self.lots


@dataclass(frozen=True)
class Trade:
    side: str
    entry_time: datetime
    exit_time: datetime
    entry_price: Decimal
    exit_price: Decimal
    quantity: int
    gross_pnl: Decimal
    charges: Decimal
    net_pnl: Decimal
    exit_reason: str
    regime: str


@dataclass
class BacktestResult:
    strategy: str
    trades: list[Trade]
    gated: int
    expired_orders: int
    bars: int
    risk_vetoes: int = 0
    margin_assumed: bool = False


@dataclass
class _Pending:
    signal: EntrySignal
    limit_price: Decimal
    stop_price: Decimal
    target_price: Decimal
    active_from: int
    last_live: int


@dataclass
class _Session:
    day: date | None = None
    daily_pnl: Decimal = Decimal("0")
    loss_streak: int = 0
    last_loss_index: int | None = None

    def note(self, trade: Trade, index: int) -> None:
        day = ensure_ist(trade.exit_time).date()
        if self.day != day:
            self.day = day
            self.daily_pnl = Decimal("0")
            self.loss_streak = 0
            self.last_loss_index = None
        self.daily_pnl += trade.net_pnl
        if trade.net_pnl < 0:
            self.loss_streak += 1
            self.last_loss_index = index
        else:
            self.loss_streak = 0
            self.last_loss_index = None

    def pnl_for(self, moment: datetime) -> Decimal:
        if self.day != ensure_ist(moment).date():
            return Decimal("0")
        return self.daily_pnl

    def bars_since(self, index: int) -> int | None:
        if self.last_loss_index is None:
            return None
        return index - self.last_loss_index


def _record(trades: list[Trade], session: _Session, trade: Trade, index: int) -> None:
    trades.append(trade)
    session.note(trade, index)


@dataclass
class _Position:
    side: str
    entry_price: Decimal
    entry_time: datetime
    fill_index: int
    stop_price: Decimal
    target_price: Decimal
    time_stop_bars: int
    regime: str


def run_backtest(bars: list[Candle], strategy: Strategy, spec: BacktestSpec) -> BacktestResult:
    if spec.lot_size <= 0 or spec.lots <= 0:
        raise ValueError("lot size and lots must be positive")
    if spec.latency_bars < 1:
        raise ValueError("latency_bars must be at least 1 so the signal bar cannot fill")
    trades: list[Trade] = []
    gated = 0
    expired = 0
    risk_vetoes = 0
    order_times: list[datetime] = []
    pending: _Pending | None = None
    position: _Position | None = None
    seen: list[Candle] = []
    session = _Session()
    for index, bar in enumerate(bars):
        seen.append(bar)
        if _flatten_due(bar, spec.flatten_time):
            if position is not None:
                _record(trades, session, _exit_trade(position, bar, _flatten_fill(position, bar, spec), "flatten", spec), index)
                position = None
            if pending is not None:
                expired += 1
                pending = None
            continue
        if position is not None:
            closed = _exit_on_bar(position, bar, index, spec)
            if closed is not None:
                _record(trades, session, closed, index)
                position = None
        if position is None and pending is not None and index > pending.last_live:
            expired += 1
            pending = None
        if position is None and pending is not None and index >= pending.active_from:
            filled = candle_limit_price(
                bar,
                side=pending.signal.side,
                limit_price=pending.limit_price,
                tick_size=spec.tick_size,
                slippage_ticks=spec.slippage_ticks,
            )
            if filled is not None:
                position = _Position(
                    side=pending.signal.side,
                    entry_price=filled,
                    entry_time=ensure_ist(bar.bucket_start),
                    fill_index=index,
                    stop_price=pending.stop_price,
                    target_price=pending.target_price,
                    time_stop_bars=pending.signal.time_stop_bars,
                    regime=pending.signal.regime,
                )
                pending = None
                closed = _exit_on_bar(position, bar, index, spec)
                if closed is not None:
                    _record(trades, session, closed, index)
                    position = None
        if position is None and pending is None:
            moment = ensure_ist(bar.bucket_start)
            signal = strategy.on_bar(
                MarketState(
                    bars=tuple(seen),
                    tick_size=spec.tick_size,
                    now=moment,
                    daily_pnl=session.pnl_for(moment),
                    loss_streak=session.loss_streak,
                    bars_since_last_loss=session.bars_since(index),
                    expiry_day=moment.date() in spec.expiry_dates,
                )
            )
            if signal is None:
                continue
            decision = _entry_risk(signal, spec, moment, session, order_times)
            if not decision.allowed:
                risk_vetoes += 1
                continue
            pending = _accept_signal(signal, index, spec)
            if pending is not None:
                order_times.append(moment)
            if pending is None:
                gated += 1
    if position is not None and bars:
        last = bars[-1]
        _record(trades, session, _exit_trade(position, last, _close_fill(position, last, spec), "end", spec), len(bars) - 1)
    if pending is not None:
        expired += 1
    return BacktestResult(
        strategy=strategy.name,
        trades=trades,
        gated=gated,
        expired_orders=expired,
        bars=len(bars),
        risk_vetoes=risk_vetoes,
        margin_assumed=spec.available_margin is None,
    )


def load_candles(path: Path, *, instrument_token: int = 0, interval: str = "minute") -> list[Candle]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("candle file has no header")
        missing = {"bucket_start", "open", "high", "low", "close", "volume"} - set(reader.fieldnames)
        if missing:
            raise ValueError("candle file is missing columns: " + ", ".join(sorted(missing)))
        candles: list[Candle] = []
        for row in reader:
            moment = datetime.fromisoformat(row["bucket_start"])
            if moment.tzinfo is None:
                moment = moment.replace(tzinfo=IST)
            candles.append(
                Candle(
                    instrument_token=int(row.get("instrument_token") or instrument_token),
                    bucket_start=ensure_ist(moment),
                    interval=interval,
                    open=Decimal(row["open"]),
                    high=Decimal(row["high"]),
                    low=Decimal(row["low"]),
                    close=Decimal(row["close"]),
                    volume=int(row["volume"]),
                    oi=int(row["oi"]) if row.get("oi") else None,
                    source="csv",
                )
            )
    return candles


def _entry_risk(signal: EntrySignal, spec: BacktestSpec, moment: datetime, session: _Session, order_times: list[datetime]):
    limit = on_tick(signal.limit_price, spec.tick_size)
    stop = on_tick(signal.stop_price, spec.tick_size)
    planned = abs(limit - stop) * spec.quantity
    notional = abs(limit * spec.quantity)
    required = (notional * Decimal("0.15")).quantize(Decimal("0.01"))
    available = spec.available_margin if spec.available_margin is not None else notional * 10
    second_ago = moment - timedelta(seconds=1)
    minute_ago = moment - timedelta(seconds=60)
    return assess(
        Intent.ENTRY,
        RiskSnapshot(
            now=moment,
            trading_mode="paper",
            lots_requested=spec.lots,
            open_lots=0,
            max_lots=max(spec.lots, 1),
            planned_loss=planned,
            max_loss_per_trade=spec.max_loss_per_trade,
            daily_pnl=session.pnl_for(moment),
            daily_loss_limit=spec.daily_loss_limit,
            orders_last_second=sum(1 for item in order_times if item >= second_ago),
            orders_last_minute=sum(1 for item in order_times if item >= minute_ago),
            max_orders_per_second=3,
            max_orders_per_minute=150,
            last_quote_at=moment,
            stale_after=timedelta(seconds=5),
            available_margin=available,
            required_margin=required,
            flatten_time=spec.flatten_time,
            broker_pnl=None,
            internal_pnl=None,
            pnl_tolerance=Decimal("250"),
            exchange_time=moment,
            clock_drift_limit=timedelta(seconds=2),
            recent_rejects=0,
            max_rejects=3,
            killed=False,
        ),
    )


def _accept_signal(signal: EntrySignal, index: int, spec: BacktestSpec) -> _Pending | None:
    if signal.side not in {"buy", "sell"} or signal.time_stop_bars < 1:
        raise ValueError("entry needs a buy or sell side and a time stop of at least 1 bar")
    limit = on_tick(signal.limit_price, spec.tick_size)
    stop = on_tick(signal.stop_price, spec.tick_size)
    target = on_tick(signal.target_price, spec.tick_size)
    if signal.side == "buy" and not (stop < limit < target):
        raise ValueError("buy stop must be below the limit and the target above it")
    if signal.side == "sell" and not (target < limit < stop):
        raise ValueError("sell target must be below the limit and the stop above it")
    if not cost_gate_allows(
        spec.charges,
        entry_price=limit,
        target_price=target,
        quantity=spec.quantity,
        entry_side=signal.side,
        tick_size=spec.tick_size,
        cost_multiple=spec.cost_multiple,
    ):
        return None
    active_from = index + spec.latency_bars
    return _Pending(
        signal=signal,
        limit_price=limit,
        stop_price=stop,
        target_price=target,
        active_from=active_from,
        last_live=active_from + signal.time_stop_bars - 1,
    )


def _exit_on_bar(position: _Position, bar: Candle, index: int, spec: BacktestSpec) -> Trade | None:
    exit_side = "sell" if position.side == "buy" else "buy"
    if position.side == "buy":
        stop_hit = bar.low <= position.stop_price
        target_hit = bar.high > position.target_price
        gap = bar.open <= position.stop_price
    else:
        stop_hit = bar.high >= position.stop_price
        target_hit = bar.low < position.target_price
        gap = bar.open >= position.stop_price
    if stop_hit:
        price = adverse_exit_price(
            side=exit_side,
            trigger=position.stop_price,
            bar_open=bar.open,
            tick_size=spec.tick_size,
            slippage_ticks=spec.slippage_ticks,
            gap_through=gap,
        )
        return _exit_trade(position, bar, price, "stop", spec)
    if target_hit:
        price = candle_limit_price(
            bar,
            side=exit_side,
            limit_price=position.target_price,
            tick_size=spec.tick_size,
            slippage_ticks=spec.slippage_ticks,
        )
        if price is None:
            price = on_tick(position.target_price, spec.tick_size)
        return _exit_trade(position, bar, price, "target", spec)
    if index >= position.fill_index + position.time_stop_bars:
        return _exit_trade(position, bar, _close_fill(position, bar, spec), "time", spec)
    return None


def _flatten_fill(position: _Position, bar: Candle, spec: BacktestSpec) -> Decimal:
    return adverse_exit_price(
        side="sell" if position.side == "buy" else "buy",
        trigger=bar.open,
        bar_open=bar.open,
        tick_size=spec.tick_size,
        slippage_ticks=spec.slippage_ticks,
        gap_through=True,
    )


def _close_fill(position: _Position, bar: Candle, spec: BacktestSpec) -> Decimal:
    return adverse_exit_price(
        side="sell" if position.side == "buy" else "buy",
        trigger=bar.close,
        bar_open=bar.open,
        tick_size=spec.tick_size,
        slippage_ticks=spec.slippage_ticks,
        gap_through=False,
    )


def _exit_trade(position: _Position, bar: Candle, price: Decimal, reason: str, spec: BacktestSpec) -> Trade:
    if position.side == "buy":
        gross = (price - position.entry_price) * spec.quantity
    else:
        gross = (position.entry_price - price) * spec.quantity
    charges = round_trip_charges(
        spec.charges,
        entry_price=position.entry_price,
        exit_price=price,
        quantity=spec.quantity,
        entry_side=position.side,
        tick_size=spec.tick_size,
    ).total
    return Trade(
        side=position.side,
        entry_time=position.entry_time,
        exit_time=ensure_ist(bar.bucket_start),
        entry_price=position.entry_price,
        exit_price=price,
        quantity=spec.quantity,
        gross_pnl=gross,
        charges=charges,
        net_pnl=gross - charges,
        exit_reason=reason,
        regime=position.regime,
    )


def _flatten_due(bar: Candle, flatten_time: time) -> bool:
    return ensure_ist(bar.bucket_start).timetz().replace(tzinfo=None) >= flatten_time
