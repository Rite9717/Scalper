from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from prometheus_client import CollectorRegistry

from scalper.clock import IST
from scalper.execution.broker import (
    BrokerMargins,
    BrokerPosition,
    MarginSegment,
    PlaceOutcome,
    PositionBook,
)
from scalper.execution.engine import ExecutionEngine, PositionMismatch, UnmanagedPosition
from scalper.execution.errors import AmbiguousOrderOutcome
from scalper.execution.halt import entries_halted
from scalper.risk.killfile import is_killed
from scalper.execution.snapshot import FixedSnapshotProvider
from scalper.execution.states import ManagedOrder, OrderStatus
from scalper.kite.api_facts import KITE_MAX_MODIFICATIONS_PER_ORDER
from scalper.monitor.alerts import Monitor, RecordingSink
from scalper.monitor.metrics import build_metrics
from scalper.risk.gate import RiskSnapshot

TICK = Decimal("0.05")
LOT = 15
SYMBOL = "NIFTY26OCTFUT"
EXCHANGE = "NFO"
NOW = datetime(2026, 10, 27, 10, 0, tzinfo=IST)


class FakeBroker:
    """In-memory book whose cancel and place return values can disagree with the book."""

    def __init__(self) -> None:
        self.orders: dict[str, ManagedOrder] = {}
        self.modify_prices: list[Decimal] = []
        self._sequence = 0
        self.reject_on_book = False
        self.cancel_fill: int | None = None
        self.track_modifications = True
        self.omit_from_book = False
        self.order_reads = 0

    async def place_limit(self, order: ManagedOrder) -> PlaceOutcome:
        self._sequence += 1
        order_id = f"id-{self._sequence}"
        opened = replace(order, status=OrderStatus.OPEN, broker_order_id=order_id)
        stored = replace(opened, status=OrderStatus.REJECTED) if self.reject_on_book else opened
        if not self.omit_from_book:
            self.orders[order_id] = stored
        return PlaceOutcome(opened)

    async def cancel(self, order_id: str) -> ManagedOrder:
        current = self.orders[order_id]
        if self.cancel_fill is not None:
            filled = self.cancel_fill
            status = OrderStatus.FILLED if filled >= current.quantity else OrderStatus.PARTIAL
            self.orders[order_id] = replace(
                current,
                status=status,
                filled_quantity=filled,
                average_price=current.limit_price,
            )
        else:
            self.orders[order_id] = replace(current, status=OrderStatus.CANCELLED)
        return replace(current, status=OrderStatus.CANCELLED, filled_quantity=0)

    async def modify_price(self, order_id: str, limit_price: Decimal) -> ManagedOrder:
        self.modify_prices.append(limit_price)
        current = self.orders[order_id]
        modifications = current.modifications + 1 if self.track_modifications else current.modifications
        self.orders[order_id] = replace(
            current,
            limit_price=limit_price,
            modifications=modifications,
        )
        return replace(current, limit_price=Decimal("1"))

    async def get_orders(self) -> list[ManagedOrder]:
        self.order_reads += 1
        return list(self.orders.values())

    async def get_positions(self) -> PositionBook:
        nets: dict[tuple[str, str], int] = {}
        for order in self.orders.values():
            signed = order.filled_quantity if order.side == "buy" else -order.filled_quantity
            key = (order.tradingsymbol, order.exchange)
            nets[key] = nets.get(key, 0) + signed
        positions = tuple(
            BrokerPosition(symbol, exchange, "MIS", quantity, Decimal("0"), 0)
            for (symbol, exchange), quantity in nets.items()
            if quantity != 0
        )
        return PositionBook(positions, ())

    async def get_margins(self) -> BrokerMargins:
        empty = MarginSegment(False, Decimal("0"), Decimal("0"), Decimal("0"))
        return BrokerMargins(empty, empty)

    def show(self, order_id: str, **changes: object) -> None:
        self.orders[order_id] = replace(self.orders[order_id], **changes)


def _snapshot(**overrides: object) -> RiskSnapshot:
    values: dict[str, object] = {
        "now": NOW,
        "trading_mode": "paper",
        "lots_requested": 1,
        "open_lots": 0,
        "max_lots": 1,
        "planned_loss": Decimal("0"),
        "max_loss_per_trade": Decimal("3000"),
        "daily_pnl": Decimal("0"),
        "daily_loss_limit": Decimal("5000"),
        "orders_last_second": 0,
        "orders_last_minute": 0,
        "max_orders_per_second": 3,
        "max_orders_per_minute": 150,
        "last_quote_at": NOW,
        "stale_after": timedelta(seconds=5),
        "available_margin": Decimal("100000"),
        "required_margin": Decimal("20000"),
        "flatten_time": datetime.strptime("15:20", "%H:%M").time(),
        "broker_pnl": None,
        "internal_pnl": None,
        "pnl_tolerance": Decimal("250"),
        "exchange_time": NOW,
        "clock_drift_limit": timedelta(seconds=2),
        "recent_rejects": 0,
        "max_rejects": 3,
        "killed": False,
    }
    values.update(overrides)
    return RiskSnapshot(**values)  # type: ignore[arg-type]


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _engine(broker: FakeBroker, **kwargs: object) -> tuple[ExecutionEngine, FakeClock]:
    clock = FakeClock()
    values: dict[str, object] = {
        "tradingsymbol": SYMBOL,
        "exchange": EXCHANGE,
        "lot_size": LOT,
        "tick_size": TICK,
        "kill_file": Path("KILL"),
        "monitor": Monitor(build_metrics(CollectorRegistry()), RecordingSink()),
        "flatten_timeout": 5.0,
        "flatten_poll_interval": 0.5,
        "flatten_max_adverse_ticks": 20,
        "clock": clock.monotonic,
        "sleep": clock.sleep,
    }
    values.update(kwargs)
    engine = ExecutionEngine(broker, FixedSnapshotProvider(_snapshot()), **values)  # type: ignore[arg-type]
    return engine, clock


async def _open_long(engine: ExecutionEngine, broker: FakeBroker) -> str:
    placed = await engine.submit_entry(
        side="buy",
        limit_price=Decimal("100"),
        quantity=LOT,
        stop_price=Decimal("99"),
        strategy_id="trend",
        timestamp_ms=1_000,
    )
    assert placed.order is not None and placed.order.broker_order_id is not None
    broker.show(placed.order.broker_order_id, status=OrderStatus.FILLED, filled_quantity=LOT, average_price=Decimal("100"))
    await engine.refresh()
    assert engine.position_quantity == LOT
    return placed.order.broker_order_id


def test_an_entry_can_be_rejected_after_the_broker_returns_an_id() -> None:
    async def scenario() -> None:
        broker = FakeBroker()
        broker.reject_on_book = True
        engine, _clock = _engine(broker)
        result = await engine.submit_entry(
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            stop_price=Decimal("99"),
            strategy_id="trend",
            timestamp_ms=2_000,
        )
        assert result.order is not None
        assert result.order.status is OrderStatus.REJECTED
        assert result.order.broker_order_id == "id-1"
        assert engine.position_quantity == 0
        assert engine.recent_rejects == 1

    asyncio.run(scenario())


def test_a_fill_that_arrives_during_cancel_stays_on_the_position() -> None:
    async def scenario() -> None:
        broker = FakeBroker()
        engine, _clock = _engine(broker)
        placed = await engine.submit_entry(
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            stop_price=Decimal("99"),
            strategy_id="trend",
            timestamp_ms=3_000,
        )
        assert placed.order is not None and placed.order.broker_order_id is not None
        broker.cancel_fill = 5
        cancelled = await engine.cancel(placed.order.broker_order_id)
        assert cancelled.status is OrderStatus.PARTIAL
        assert cancelled.filled_quantity == 5
        assert engine.position_quantity == 5

    asyncio.run(scenario())


def test_flatten_rejected_leaves_the_position_unmanaged() -> None:
    async def scenario() -> None:
        broker = FakeBroker()
        engine, _clock = _engine(broker)
        await _open_long(engine, broker)
        broker.reject_on_book = True
        try:
            await engine.shutdown(Decimal("101"))
        except UnmanagedPosition as exc:
            assert "remaining 15" in str(exc)
            assert "id-2" in str(exc)
        else:
            raise AssertionError("flatten reject was treated as flat")
        assert engine.position_quantity == LOT
        assert engine.recent_rejects == 1

    asyncio.run(scenario())


def test_flatten_partial_then_complete_ends_flat() -> None:
    async def scenario() -> None:
        broker = FakeBroker()
        engine, _clock = _engine(broker, flatten_timeout=1.0)
        await _open_long(engine, broker)
        seen: list[int] = []
        original = broker.get_orders

        async def scripted() -> list[ManagedOrder]:
            book = await original()
            for order in book:
                if order.side != "sell":
                    continue
                seen.append(order.filled_quantity)
                if len(seen) == 1:
                    broker.show(order.broker_order_id or "", status=OrderStatus.PARTIAL, filled_quantity=6, average_price=Decimal("101"))
                else:
                    broker.show(
                        order.broker_order_id or "",
                        status=OrderStatus.FILLED,
                        filled_quantity=LOT,
                        average_price=Decimal("101"),
                    )
            return await original()

        broker.get_orders = scripted  # type: ignore[method-assign]
        report = await engine.shutdown(Decimal("101"))
        assert seen[0] == 0
        assert report.clean is True
        assert engine.position_quantity == 0

    asyncio.run(scenario())


def test_flatten_that_never_fills_reports_the_order_and_remaining_size() -> None:
    async def scenario() -> None:
        broker = FakeBroker()
        engine, _clock = _engine(broker, flatten_timeout=0.4, flatten_poll_interval=0.5, flatten_max_adverse_ticks=2)
        await _open_long(engine, broker)
        try:
            await engine.shutdown(Decimal("101"))
        except UnmanagedPosition as exc:
            text = str(exc)
        else:
            raise AssertionError("an open flatten was marked flat")
        assert "remaining 15" in text
        assert "id-2" in text
        assert broker.modify_prices == []
        assert engine.position_quantity == LOT

    asyncio.run(scenario())


async def _open_short(engine: ExecutionEngine, broker: FakeBroker) -> str:
    placed = await engine.submit_entry(
        side="sell",
        limit_price=Decimal("99"),
        quantity=LOT,
        stop_price=Decimal("100"),
        strategy_id="trend",
        timestamp_ms=2_000,
    )
    assert placed.order is not None and placed.order.broker_order_id is not None
    broker.show(
        placed.order.broker_order_id,
        status=OrderStatus.FILLED,
        filled_quantity=LOT,
        average_price=Decimal("99"),
    )
    await engine.refresh()
    assert engine.position_quantity == -LOT
    return placed.order.broker_order_id


def test_flatten_reprice_stops_at_the_adverse_tick_limit_for_long_and_short() -> None:
    async def scenario(long_position: bool) -> None:
        broker = FakeBroker()
        interval = 0.5
        engine, clock = _engine(
            broker,
            flatten_timeout=3.0,
            flatten_poll_interval=interval,
            flatten_max_adverse_ticks=2,
        )
        modified_at: list[float] = []
        original = broker.modify_price

        async def timed(order_id: str, limit_price: Decimal) -> ManagedOrder:
            modified_at.append(clock.now)
            return await original(order_id, limit_price)

        broker.modify_price = timed  # type: ignore[method-assign]
        if long_position:
            await _open_long(engine, broker)
            start = Decimal("101")
            expected = [Decimal("100.95"), Decimal("100.90")]
        else:
            await _open_short(engine, broker)
            start = Decimal("99")
            expected = [Decimal("99.05"), Decimal("99.10")]
        try:
            await engine.shutdown(start)
        except UnmanagedPosition:
            pass
        else:
            raise AssertionError("repriced flatten was marked flat")
        assert broker.modify_prices == expected
        assert modified_at[0] >= interval
        assert all(modified_at[i] - modified_at[i - 1] >= interval for i in range(1, len(modified_at)))
        assert clock.sleeps
        assert all(pause == interval for pause in clock.sleeps)
        assert modified_at == [interval, interval * 2]

    asyncio.run(scenario(True))
    asyncio.run(scenario(False))


def test_flatten_reprice_stops_after_twenty_five_steps() -> None:
    async def scenario() -> None:
        broker = FakeBroker()
        engine, _clock = _engine(
            broker,
            flatten_timeout=30.0,
            flatten_max_adverse_ticks=25,
        )
        await _open_long(engine, broker)
        try:
            await engine.shutdown(Decimal("101"))
        except UnmanagedPosition as exc:
            assert "remaining 15" in str(exc)
        else:
            raise AssertionError("the 25th reprice was treated as a fill")
        assert len(broker.modify_prices) == 25
        assert broker.modify_prices[-1] == Decimal("101") - TICK * 25

    asyncio.run(scenario())


def test_flatten_replaces_the_order_after_twenty_five_mods_inside_the_tick_limit() -> None:
    async def scenario(long_position: bool) -> None:
        broker = FakeBroker()
        ticks = 40
        engine, _clock = _engine(
            broker,
            flatten_timeout=60.0,
            flatten_poll_interval=0.5,
            flatten_max_adverse_ticks=ticks,
        )
        placed: list[ManagedOrder] = []
        mods: list[tuple[str, Decimal]] = []
        original_place = broker.place_limit
        original_modify = broker.modify_price

        async def record_place(order: ManagedOrder) -> PlaceOutcome:
            outcome = await original_place(order)
            placed.append(outcome.order)
            return outcome

        async def record_modify(order_id: str, limit_price: Decimal) -> ManagedOrder:
            mods.append((order_id, limit_price))
            return await original_modify(order_id, limit_price)

        broker.place_limit = record_place  # type: ignore[method-assign]
        broker.modify_price = record_modify  # type: ignore[method-assign]
        if long_position:
            await _open_long(engine, broker)
            start = Decimal("101")
            side = "sell"
            remaining = LOT
        else:
            await _open_short(engine, broker)
            start = Decimal("99")
            side = "buy"
            remaining = -LOT
        bound = start - TICK * ticks if side == "sell" else start + TICK * ticks
        try:
            await engine.shutdown(start)
        except UnmanagedPosition as exc:
            text = str(exc)
        else:
            raise AssertionError("the replacement flatten was marked flat")
        flatten_orders = [order for order in placed if order.side == side]
        assert len(flatten_orders) == 2
        original, replacement = flatten_orders
        assert original.tag != replacement.tag
        assert replacement.broker_order_id is not None
        assert replacement.broker_order_id in text
        assert f"remaining {remaining}" in text
        assert engine.orders[original.tag].status is OrderStatus.CANCELLED
        by_order: dict[str, list[Decimal]] = {}
        for order_id, price in mods:
            by_order.setdefault(order_id, []).append(price)
        assert original.broker_order_id is not None
        assert len(by_order[original.broker_order_id]) == KITE_MAX_MODIFICATIONS_PER_ORDER
        assert len(by_order[replacement.broker_order_id]) <= KITE_MAX_MODIFICATIONS_PER_ORDER
        assert all(len(prices) <= KITE_MAX_MODIFICATIONS_PER_ORDER for prices in by_order.values())
        prices = [order.limit_price for order in flatten_orders]
        prices.extend(price for _order_id, price in mods)
        if side == "sell":
            assert all(bound <= price <= start for price in prices)
        else:
            assert all(start <= price <= bound for price in prices)

    asyncio.run(scenario(True))
    asyncio.run(scenario(False))


def test_twenty_five_modifications_are_replaced_with_a_new_tag() -> None:
    async def scenario() -> None:
        broker = FakeBroker()
        broker.track_modifications = False
        engine, _clock = _engine(broker)
        placed = await engine.submit_entry(
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            stop_price=Decimal("99"),
            strategy_id="trend",
            timestamp_ms=4_000,
        )
        assert placed.order is not None and placed.order.broker_order_id is not None
        tag = placed.order.tag
        order_id = placed.order.broker_order_id
        for step in range(25):
            result = await engine.modify_price(
                tag,
                Decimal("100") + TICK * (step + 1),
                strategy_id="trend",
                timestamp_ms=5_000,
            )
            assert result.order is not None
            assert result.order.tag == tag
        assert engine._modifications[order_id] == 25
        replaced = await engine.modify_price(tag, Decimal("102"), strategy_id="trend", timestamp_ms=5_000)
        assert replaced.order is not None
        assert replaced.order.tag != tag
        assert engine.orders[tag].status is OrderStatus.CANCELLED

    asyncio.run(scenario())


def test_unknown_broker_orders_at_shutdown_are_named() -> None:
    async def scenario() -> None:
        broker = FakeBroker()
        engine, _clock = _engine(broker)
        broker.orders["stranger"] = ManagedOrder(
            tag="",
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            status=OrderStatus.OPEN,
            broker_order_id="stranger",
            tradingsymbol=SYMBOL,
            exchange=EXCHANGE,
        )
        try:
            await engine.shutdown(Decimal("100"))
        except UnmanagedPosition as exc:
            assert "stranger" in str(exc)
        else:
            raise AssertionError("unknown order was ignored")

    asyncio.run(scenario())


def test_a_missing_tag_after_placement_engages_halt_and_kill() -> None:
    async def scenario() -> None:
        broker = FakeBroker()
        broker.omit_from_book = True
        engine, clock = _engine(broker)
        with pytest.raises(AmbiguousOrderOutcome, match="not on the order book"):
            await engine.submit_entry(
                side="buy",
                limit_price=Decimal("100"),
                quantity=LOT,
                stop_price=Decimal("99"),
                strategy_id="trend",
                timestamp_ms=11_000,
            )
        assert entries_halted()
        assert is_killed(Path("KILL"))
        assert engine.monitor.sink.messages
        assert "Kill switch" in engine.monitor.sink.messages[0]
        assert broker.order_reads == 6
        assert clock.sleeps == [1.25, 1.25, 1.25, 1.25]
        refused = await engine.submit_entry(
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            stop_price=Decimal("99"),
            strategy_id="trend",
            timestamp_ms=12_000,
        )
        assert refused.order is None

    asyncio.run(scenario())


def test_other_instruments_are_ignored_for_position_rejects_and_shutdown() -> None:
    async def scenario() -> None:
        broker = FakeBroker()
        engine, _clock = _engine(broker)
        broker.orders["other"] = ManagedOrder(
            tag="",
            side="buy",
            limit_price=Decimal("200"),
            quantity=LOT,
            filled_quantity=LOT,
            status=OrderStatus.FILLED,
            broker_order_id="other-1",
            tradingsymbol="BANKNIFTY26OCTFUT",
            exchange=EXCHANGE,
        )
        broker.orders["rejected-other"] = ManagedOrder(
            tag="manual",
            side="buy",
            limit_price=Decimal("200"),
            quantity=LOT,
            status=OrderStatus.REJECTED,
            broker_order_id="other-2",
            tradingsymbol="BANKNIFTY26OCTFUT",
            exchange=EXCHANGE,
        )
        await engine.refresh()
        assert engine.position_quantity == 0
        assert engine.recent_rejects == 0
        assert engine.foreign_orders == []
        report = await engine.shutdown(Decimal("100"))
        assert report.clean is True

    asyncio.run(scenario())


def test_a_foreign_tag_on_this_instrument_blocks_entries() -> None:
    async def scenario() -> None:
        broker = FakeBroker()
        engine, _clock = _engine(broker)
        broker.orders["manual"] = ManagedOrder(
            tag="manual",
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            status=OrderStatus.OPEN,
            broker_order_id="manual-1",
            tradingsymbol=SYMBOL,
            exchange=EXCHANGE,
        )
        refused = await engine.submit_entry(
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            stop_price=Decimal("99"),
            strategy_id="trend",
            timestamp_ms=8_000,
        )
        assert refused.order is None
        assert refused.decision.action == "halt"
        assert [order.broker_order_id for order in engine.foreign_orders] == ["manual-1"]
        try:
            await engine.shutdown(Decimal("100"))
        except UnmanagedPosition as exc:
            assert "manual-1" in str(exc)
        else:
            raise AssertionError("foreign order was left unmanaged")

    asyncio.run(scenario())


def test_a_position_mismatch_halts_new_entries() -> None:
    async def scenario() -> None:
        broker = FakeBroker()
        engine, _clock = _engine(broker)

        async def wrong_positions() -> PositionBook:
            return PositionBook(
                (BrokerPosition(SYMBOL, EXCHANGE, "MIS", 5, Decimal("100"), 0),),
                (),
            )

        broker.get_positions = wrong_positions  # type: ignore[method-assign]
        with pytest.raises(PositionMismatch):
            await engine.refresh()
        assert entries_halted()
        refused = await engine.submit_entry(
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            stop_price=Decimal("99"),
            strategy_id="trend",
            timestamp_ms=9_000,
        )
        assert refused.order is None
        assert refused.decision.allowed is False

    asyncio.run(scenario())
