"""Paper loop. Ticks come from the caller. Orders go only to PaperBroker."""

from __future__ import annotations

import asyncio
import signal
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Protocol
from uuid import UUID

import structlog

from scalper.backtest.costs import ChargeSchedule, round_trip_charges
from scalper.backtest.fills import adverse_exit_price, candle_limit_price
from scalper.data.candles import floor_minute
from scalper.backtest.strategy import EntrySignal, MarketState, Strategy
from scalper.clock import ensure_ist
from scalper.data.cache import QuoteCache
from scalper.data.models import NormalizedQuote
from scalper.execution.engine import ExecutionEngine, UnmanagedPosition
from scalper.execution.halt import halt_new_entries
from scalper.execution.paper import PaperBroker
from scalper.execution.snapshot import AccountLimits
from scalper.execution.states import ManagedOrder, OrderStatus
from scalper.monitor.alerts import Monitor
from scalper.paper.bars import SessionMinuteBook
from scalper.paper.exits import (
    ExitTrigger,
    OpenPosition,
    WorkingExit,
    entry_expired,
    inside_adverse_bound,
    position_trigger,
    strict_through,
    worse_limit,
)
from scalper.paper.journal import PaperJournal
from scalper.paper.limits import PaperLimits
from scalper.risk.gate import Intent
from scalper.risk.killfile import engage, is_killed


class FeedDrop:
    """The websocket closed. Entries stay blocked until a later tick."""


class Silence:
    """No tick arrived before stale_after."""


class Stop:
    """SIGINT or SIGTERM. Flatten, then leave the loop."""


@dataclass
class _PendingEntry:
    signal_index: int
    active_from: int
    last_live: int
    order: ManagedOrder
    signal: EntrySignal
    timestamp_ms: int


class _Clock(Protocol):
    def __call__(self) -> datetime: ...


class PaperRunner:
    def __init__(
        self,
        *,
        engine: ExecutionEngine,
        broker: PaperBroker,
        cache: QuoteCache,
        book: SessionMinuteBook,
        strategy: Strategy,
        journal: PaperJournal,
        run_id: UUID,
        monitor: Monitor,
        kill_file: Path,
        account: AccountLimits,
        paper: PaperLimits,
        now: Callable[[], datetime],
        sleep: Callable[[float], Awaitable[None]],
        instrument_token: int,
        quantity: int,
        latency_bars: int,
        tick_size: Decimal,
        slippage_ticks: int = 0,
        limits_record: dict[str, str],
        pace: bool = True,
        charges: ChargeSchedule | None = None,
        expiry_dates: set[date] | None = None,
    ) -> None:
        if latency_bars < 1:
            raise ValueError("latency_bars must be at least 1 so the signal bar cannot fill")
        self.engine = engine
        self.broker = broker
        self.cache = cache
        self.book = book
        self.strategy = strategy
        self.journal = journal
        self.run_id = run_id
        self.monitor = monitor
        self.kill_file = kill_file
        self.account = account
        self.paper = paper
        self._now = now
        self._sleep = sleep
        self.token = instrument_token
        self.quantity = quantity
        self.latency_bars = latency_bars
        self.tick_size = tick_size
        if type(slippage_ticks) is not int or slippage_ticks < 0:
            raise ValueError("slippage_ticks cannot be negative")
        self.slippage_ticks = slippage_ticks
        self.limits_record = limits_record
        self._bar_open: Decimal | None = None
        self._bar_minute: datetime | None = None
        self._clean_bar: datetime | None = None
        self._pace = pace
        self.bars: list = []
        self.pending: _PendingEntry | None = None
        self.position: OpenPosition | None = None
        self.exit: WorkingExit | None = None
        self.last_price: Decimal | None = None
        self.last_tick_at: datetime | None = None
        self.gap = False
        self._stop = asyncio.Event()
        self._stall_open = False
        self._reconnect_wait = paper.reconnect_initial_seconds
        self._fail_closed_done = False
        self.charges = charges or ChargeSchedule()
        self.expiry_dates = set(expiry_dates or ())
        self.daily_pnl = Decimal("0")
        self.loss_streak = 0
        self.last_loss_index: int | None = None
        self._pnl_day: date | None = None
        self._log = structlog.get_logger()

    def request_stop(self) -> None:
        self._stop.set()

    async def run(self, source: AsyncIterator[object]) -> None:
        self._log.info("paper_limits", **self.limits_record)
        await self.journal.start_run(
            run_id=self.run_id,
            started_at=self._now(),
            strategy=self.strategy.name,
            tradingsymbol=self.engine.tradingsymbol,
            limits=self.limits_record,
        )
        try:
            iterator = source.__aiter__()
            while not self._stop.is_set():
                item = await self._next(iterator)
                if item is None:
                    break
                if isinstance(item, Stop) or self._stop.is_set():
                    break
                if isinstance(item, Silence):
                    await self._on_silence()
                    continue
                if isinstance(item, FeedDrop):
                    await self._on_drop()
                    continue
                if not isinstance(item, NormalizedQuote):
                    raise TypeError("paper feed emitted an unknown event")
                await self._on_quote(item)
            await self._shutdown("runner stopped")
        except BaseException as exc:
            await self.fail_closed(exc)
            raise

    async def _next(self, iterator: AsyncIterator[object]) -> object | None:
        if not self._pace:
            try:
                return await iterator.__anext__()
            except StopAsyncIteration:
                return None
        nxt = asyncio.ensure_future(iterator.__anext__())
        pause = asyncio.ensure_future(self._sleep(self.paper.stale_after.total_seconds()))
        try:
            done, _pending = await asyncio.wait({nxt, pause}, return_when=asyncio.FIRST_COMPLETED)
        except Exception:
            nxt.cancel()
            pause.cancel()
            raise
        if nxt not in done:
            nxt.cancel()
            await self._finish(nxt, pause)
            return Silence()
        pause.cancel()
        await self._finish(pause)
        try:
            return nxt.result()
        except StopAsyncIteration:
            return None

    async def _finish(self, *tasks: asyncio.Future[object]) -> None:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _on_silence(self) -> None:
        self._beat()
        if self._now().timetz().replace(tzinfo=None) >= self.account.flatten_time:
            await self._shutdown("flatten time")
            self.request_stop()
            return
        if self.last_tick_at is None:
            return
        age = self._now() - self.last_tick_at
        if age <= self.paper.stale_after:
            return
        await self._note_stall(age)
        if self.position is not None and self.exit is None and age >= self.paper.stale_after + self.paper.stops_blind_grace:
            await self._blind_flatten()

    async def _on_drop(self) -> None:
        self.gap = True
        self._clean_bar = None
        drop = getattr(self.cache, "drop", None)
        if drop is not None:
            await drop(self.token)
        self._stall_open = False
        await self.monitor.feed_stall(self.paper.stale_after.total_seconds())
        self._stall_open = True
        if self.position is not None:
            age = 0.0 if self.last_tick_at is None else (self._now() - self.last_tick_at).total_seconds()
            await self.monitor.stops_blind(age)
        self._log.warning("websocket_dropped")
        await self._sleep_until_stop(self._reconnect_wait)
        self._reconnect_wait = min(self._reconnect_wait * 2, self.paper.reconnect_max_seconds)

    async def _sleep_until_stop(self, seconds: float) -> None:
        if self._stop.is_set():
            return
        pause = asyncio.ensure_future(self._sleep(seconds))
        stopped = asyncio.ensure_future(self._stop.wait())
        try:
            done, _pending = await asyncio.wait({pause, stopped}, return_when=asyncio.FIRST_COMPLETED)
        except BaseException:
            pause.cancel()
            stopped.cancel()
            await self._finish(pause, stopped)
            raise
        for task in (pause, stopped):
            if task not in done:
                task.cancel()
        await self._finish(pause, stopped)

    async def _on_quote(self, quote: NormalizedQuote) -> None:
        self._beat()
        moment = ensure_ist(quote.received_at)
        if self._stop.is_set() or moment.timetz().replace(tzinfo=None) >= self.account.flatten_time:
            self.last_price = quote.last_price
            self.request_stop()
            return
        self.last_price = quote.last_price
        self.last_tick_at = moment
        self.broker.last_price = quote.last_price
        minute = floor_minute(moment)
        if minute != self._bar_minute:
            if self.gap and self._clean_bar is None and self._bar_minute is not None:
                self._clean_bar = minute
            self._bar_minute = minute
            self._bar_open = quote.last_price
        await self.cache.put(quote)
        if not self.gap:
            await self.monitor.stops_covered()
        finished = self.book.add(quote)
        if (
            finished is not None
            and self.gap
            and self._clean_bar is not None
            and finished.bucket_start == self._clean_bar
        ):
            self.gap = False
            self._clean_bar = None
            self._reconnect_wait = self.paper.reconnect_initial_seconds
            await self.monitor.feed_resumed()
            self._stall_open = False
        if finished is not None:
            self.bars.append(finished)
            await self._on_bar()
            await self._fill_completed_bar(finished, len(self.bars) - 1)
        await self._manage_tick(quote)

    async def _on_bar(self) -> None:
        if self.position is not None or self.pending is not None or self.exit is not None:
            return
        if self.gap:
            await self._record_decision(Intent.ENTRY, False, "halt", "feed gap")
            return
        if is_killed(self.kill_file):
            await self._record_decision(Intent.ENTRY, False, "halt", "kill file is engaged")
            return
        index = len(self.bars) - 1
        bars_since = None if self.last_loss_index is None else index - self.last_loss_index
        signal = self.strategy.on_bar(
            MarketState(
                bars=tuple(self.bars),
                tick_size=self.tick_size,
                now=self._now(),
                daily_pnl=self.daily_pnl,
                loss_streak=self.loss_streak,
                bars_since_last_loss=bars_since,
                expiry_day=self._now().date() in self.expiry_dates,
            )
        )
        if signal is None:
            return
        await self.journal.record_signal(
            run_id=self.run_id,
            at=self._now(),
            side=signal.side,
            limit_price=signal.limit_price,
            stop_price=signal.stop_price,
            target_price=signal.target_price,
            time_stop_bars=signal.time_stop_bars,
            regime=signal.regime,
            accepted=True,
        )
        stamp = self._millis(self._now())
        result = await self.engine.submit_entry(
            side=signal.side,
            limit_price=signal.limit_price,
            quantity=self.quantity,
            stop_price=signal.stop_price,
            strategy_id=self._strategy_id("entry"),
            timestamp_ms=stamp,
        )
        await self._record_decision(Intent.ENTRY, result.decision.allowed, result.decision.action, result.decision.reason)
        if result.order is None:
            return
        await self._record_order(result.order)
        signal_index = len(self.bars) - 1
        active_from = signal_index + self.latency_bars
        self.pending = _PendingEntry(
            signal_index=signal_index,
            active_from=active_from,
            last_live=active_from + signal.time_stop_bars - 1,
            order=result.order,
            signal=signal,
            timestamp_ms=stamp,
        )

    async def _manage_tick(self, quote: NormalizedQuote) -> None:
        forming = len(self.bars)
        if self.pending is not None and entry_expired(forming_index=forming, last_live=self.pending.last_live):
            await self._cancel_entry()
        if self.position is not None and self.exit is None:
            trigger = position_trigger(self.position, quote.last_price, forming)
            if trigger is not None:
                await self._send_exit(trigger, quote.received_at)
        if self.exit is not None:
            await self._work_exit(quote)

    async def _fill_completed_bar(self, bar, index: int) -> None:
        if self.pending is not None and self.pending.active_from <= index <= self.pending.last_live:
            price = candle_limit_price(
                bar,
                side=self.pending.order.side,
                limit_price=self.pending.order.limit_price,
                tick_size=self.tick_size,
                slippage_ticks=self.slippage_ticks,
            )
            if price is not None:
                await self._fill_entry(price, bar.bucket_start)
        if self.exit is not None and self.exit.reason == "target":
            price = candle_limit_price(
                bar,
                side=self.exit.order.side,
                limit_price=self.exit.order.limit_price,
                tick_size=self.tick_size,
                slippage_ticks=self.slippage_ticks,
            )
            if price is not None:
                await self.engine.on_trade(price, self.exit.order.quantity, price)
                order = self.engine.orders.get(self.exit.order.tag)
                if order is not None and order.status is OrderStatus.FILLED:
                    await self._record_filled_exit(order, bar.bucket_start)

    async def _fill_entry(self, price: Decimal, at: datetime) -> None:
        assert self.pending is not None
        await self.engine.on_trade(price, self.pending.order.quantity, price)
        order = self.engine.orders.get(self.pending.order.tag)
        if order is None or order.filled_quantity < 1:
            return
        await self.journal.record_fill(
            run_id=self.run_id,
            at=at,
            tag=order.tag,
            price=order.average_price,
            quantity=order.filled_quantity,
        )
        # Paper fills are always complete. A partial fill is still an open position
        # and must be tracked here; revisit this path before any live runner.
        self.position = OpenPosition(
            side=order.side,
            quantity=order.filled_quantity,
            fill_index=len(self.bars),
            fill_price=order.average_price,
            stop_price=self.pending.signal.stop_price,
            target_price=self.pending.signal.target_price,
            time_stop_bars=self.pending.signal.time_stop_bars,
            tag=order.tag,
        )
        if order.status is OrderStatus.FILLED:
            self.pending = None

    async def _cancel_entry(self) -> None:
        assert self.pending is not None and self.pending.order.broker_order_id is not None
        booked = await self.engine.cancel(self.pending.order.broker_order_id)
        await self._record_order(booked)
        self.pending = None

    async def _send_exit(self, trigger: ExitTrigger, at: datetime) -> None:
        assert self.position is not None
        side = "sell" if self.position.side == "buy" else "buy"
        stamp = self._millis(at)
        result = await self.engine.submit_exit(
            side=side,
            limit_price=trigger.price,
            quantity=self.position.quantity,
            strategy_id=self._strategy_id("exit"),
            timestamp_ms=stamp,
        )
        await self._record_decision(Intent.EXIT, result.decision.allowed, result.decision.action, result.decision.reason)
        if result.order is None:
            await self._abandon(f"exit was not sent: {result.decision.reason}")
            return
        await self._record_order(result.order)
        if trigger.reason in {"stop", "time"}:
            await self._fill_protective_exit(result.order, trigger, at)
            return
        self.exit = WorkingExit(
            order=result.order,
            reason=trigger.reason,
            trigger_at=ensure_ist(at),
            trigger_price=trigger.price,
            anchor=trigger.price,
            started_at=self._now(),
            timestamp_ms=stamp,
        )

    async def _fill_protective_exit(self, placed: ManagedOrder, trigger: ExitTrigger, at: datetime) -> None:
        assert self.position is not None and self.exit is None
        bar_open = self._bar_open if self._bar_open is not None else trigger.price
        exit_side = "sell" if self.position.side == "buy" else "buy"
        if trigger.reason == "stop" and self.position.side == "buy":
            gap = bar_open <= self.position.stop_price
        elif trigger.reason == "stop":
            gap = bar_open >= self.position.stop_price
        else:
            gap = False
        price = adverse_exit_price(
            side=exit_side,
            trigger=trigger.price,
            bar_open=bar_open,
            tick_size=self.tick_size,
            slippage_ticks=self.slippage_ticks,
            gap_through=gap,
        )
        self.exit = WorkingExit(
            order=placed,
            reason=trigger.reason,
            trigger_at=ensure_ist(at),
            trigger_price=trigger.price,
            anchor=trigger.price,
            started_at=self._now(),
            timestamp_ms=self._millis(at),
        )
        await self.engine.on_trade(price, placed.quantity, price)
        order = self.engine.orders.get(placed.tag)
        if order is not None and order.status is OrderStatus.FILLED:
            await self._record_filled_exit(order, at)

    async def _work_exit(self, quote: NormalizedQuote) -> None:
        assert self.exit is not None
        if strict_through(side=self.exit.order.side, price=quote.last_price, limit_price=self.exit.order.limit_price):
            await self.engine.on_trade(quote.last_price, self.exit.order.quantity)
            order = self.engine.orders.get(self.exit.order.tag)
            if order is not None and order.status is OrderStatus.FILLED:
                await self._record_filled_exit(order, quote.received_at)
                return
        elapsed = (self._now() - self.exit.started_at).total_seconds()
        if elapsed >= self.paper.flatten_timeout:
            await self._abandon("exit did not fill before the flatten timeout")
            return
        if elapsed < self.paper.flatten_poll_interval or self.exit.order.broker_order_id is None:
            return
        nxt = worse_limit(self.exit.order.side, self.exit.order.limit_price, self.tick_size)
        if not inside_adverse_bound(
            side=self.exit.order.side,
            price=nxt,
            anchor=self.exit.anchor,
            tick_size=self.tick_size,
            max_ticks=self.paper.flatten_max_adverse_ticks,
        ):
            await self._abandon("exit did not fill inside the adverse tick limit")
            return
        result = await self.engine.modify_price(
            self.exit.order.tag,
            nxt,
            strategy_id=self._strategy_id("exit"),
            timestamp_ms=self.exit.timestamp_ms,
        )
        if result.order is not None:
            self.exit.order = result.order
            self.exit.started_at = self._now()
            await self._record_order(result.order)

    async def _record_filled_exit(self, order: ManagedOrder, at: datetime) -> None:
        assert self.exit is not None and self.position is not None
        await self.journal.record_fill(
            run_id=self.run_id,
            at=at,
            tag=order.tag,
            price=order.average_price,
            quantity=order.filled_quantity,
        )
        await self.journal.record_exit(
            run_id=self.run_id,
            at=at,
            reason=self.exit.reason,
            trigger_at=self.exit.trigger_at,
            trigger_price=self.exit.trigger_price,
            fill_at=at,
            fill_price=order.average_price,
            quantity=order.filled_quantity,
        )
        self._note_closed_trade(order.average_price, len(self.bars) - 1)
        self.position = None
        self.exit = None

    def _note_closed_trade(self, exit_price: Decimal, index: int) -> None:
        assert self.position is not None
        if self.position.side == "buy":
            gross = (exit_price - self.position.fill_price) * self.position.quantity
        else:
            gross = (self.position.fill_price - exit_price) * self.position.quantity
        charges = round_trip_charges(
            self.charges,
            entry_price=self.position.fill_price,
            exit_price=exit_price,
            quantity=self.position.quantity,
            entry_side=self.position.side,
            tick_size=self.tick_size,
        ).total
        net = gross - charges
        day = self._now().date()
        if self._pnl_day != day:
            self._pnl_day = day
            self.daily_pnl = Decimal("0")
            self.loss_streak = 0
            self.last_loss_index = None
        self.daily_pnl += net
        if net < 0:
            self.loss_streak += 1
            self.last_loss_index = index
            return
        self.loss_streak = 0
        self.last_loss_index = None

    async def _note_stall(self, age: timedelta) -> None:
        seconds = age.total_seconds()
        self._log.warning("feed_stall", age_seconds=seconds)
        await self.monitor.feed_stall(seconds)
        self._stall_open = True
        if self.position is not None:
            await self.monitor.stops_blind(seconds)

    async def _blind_flatten(self) -> None:
        self._log.error("stops_blind_flatten", last_price=None if self.last_price is None else format(self.last_price, "f"))
        await self._shutdown("feed blind with an open position")

    async def fail_closed(self, exc: BaseException) -> None:
        """Halt, kill, alert, flatten, and record the end. Each step is attempted."""
        if self._fail_closed_done:
            return
        self._fail_closed_done = True
        task = asyncio.current_task()
        if isinstance(exc, asyncio.CancelledError) and task is not None:
            while task.uncancel():
                pass
        reason = f"{type(exc).__name__}: {exc}"
        await self._attempt("halt", lambda: halt_new_entries(reason))
        await self._attempt("kill", lambda: engage(self.kill_file, reason))
        await self._attempt("alert", lambda: self.monitor.kill_switch(True, reason))
        await self._attempt("flatten", lambda: self.engine.shutdown(self.last_price))
        final_position = self.engine.position_quantity
        kill_engaged = is_killed(self.kill_file)
        await self._attempt(
            "run_end",
            lambda: self.journal.record_run_end(
                run_id=self.run_id,
                ended_at=self._now(),
                reason=reason,
                final_position=final_position,
                kill_engaged=kill_engaged,
            ),
        )

    async def _attempt(self, step: str, action: Callable[[], object]) -> None:
        try:
            result = action()
            if isinstance(result, Awaitable):
                await result
        except BaseException:
            self._log.exception("paper_fail_closed_step_failed", step=step)

    async def _shutdown(self, reason: str) -> None:
        try:
            await self.engine.shutdown(self.last_price, strategy_id=self._strategy_id("flatten"))
        except UnmanagedPosition as exc:
            engage(self.kill_file, f"{reason}: {exc}")
            await self.monitor.kill_switch(True, f"{reason}: {exc}")
            raise
        filled = [
            order
            for order in self.engine.orders.values()
            if order.intent == "flatten" and order.status is OrderStatus.FILLED and order.filled_quantity > 0
        ]
        if filled:
            order = filled[-1]
            await self.journal.record_exit(
                run_id=self.run_id,
                at=self._now(),
                reason="flatten",
                trigger_at=self._now(),
                trigger_price=order.limit_price,
                fill_at=self._now(),
                fill_price=order.average_price,
                quantity=order.filled_quantity,
            )
        self.pending = None
        self.position = None
        self.exit = None

    def _strategy_id(self, role: str) -> str:
        return f"{self.strategy.name}-{role}"

    async def _abandon(self, reason: str) -> None:
        order = self.exit.order if self.exit is not None else None
        trigger = self.exit.trigger_price if self.exit is not None else None
        remaining = order.remaining if order is not None else self.engine.position_quantity
        order_id = order.broker_order_id if order is not None else None
        await self.journal.record_exit(
            run_id=self.run_id,
            at=self._now(),
            reason=f"{reason}; order_id={order_id}; remaining={remaining}",
            trigger_at=self.exit.trigger_at if self.exit is not None else None,
            trigger_price=trigger,
            fill_at=None,
            fill_price=self.last_price,
            quantity=remaining,
        )
        engage(self.kill_file, reason)
        await self.monitor.kill_switch(True, reason)
        raise UnmanagedPosition(reason)

    async def _record_decision(self, intent: Intent, allowed: bool, action: str, reason: str) -> None:
        if not allowed:
            await self.monitor.risk_veto(reason)
        await self.journal.record_decision(
            run_id=self.run_id,
            at=self._now(),
            intent=intent.value,
            allowed=allowed,
            action=action,
            reason=reason,
        )

    async def _record_order(self, order: ManagedOrder) -> None:
        await self.journal.record_order(
            run_id=self.run_id,
            at=self._now(),
            tag=order.tag,
            broker_order_id=order.broker_order_id,
            side=order.side,
            limit_price=order.limit_price,
            quantity=order.quantity,
            intent=order.intent,
            status=order.status.value,
        )

    def _beat(self) -> None:
        self.monitor.beat(self._now().timestamp())

    @staticmethod
    def _millis(moment: datetime) -> int:
        stamp = int(ensure_ist(moment).timestamp() * 1000)
        if stamp < 1:
            raise ValueError("timestamp_ms must be a positive millisecond timestamp")
        return stamp


def install_stop_signals(loop: asyncio.AbstractEventLoop, runner: PaperRunner) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, runner.request_stop)
