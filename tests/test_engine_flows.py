from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

from prometheus_client import CollectorRegistry

from scalper.clock import IST
from scalper.execution.broker import BrokerMargins, MarginSegment, PlaceOutcome, PositionBook
from scalper.execution.engine import ExecutionEngine, UnmanagedPosition
from scalper.execution.states import ManagedOrder, OrderStatus
from scalper.monitor.alerts import Monitor, RecordingSink
from scalper.monitor.metrics import build_metrics
from scalper.risk.gate import RiskSnapshot

TICK = Decimal("0.05")
LOT = 15
NOW = datetime(2026, 10, 27, 10, 0, tzinfo=IST)


class FakeBroker:
    """In-memory book whose cancel and place return values can disagree with the book."""

    def __init__(self) -> None:
        self.orders: dict[str, ManagedOrder] = {}
        self.modify_prices: list[Decimal] = []
        self._sequence = 0
        self.reject_on_book = False
        self.cancel_fill: int | None = None

    async def place_limit(self, order: ManagedOrder) -> PlaceOutcome:
        self._sequence += 1
        order_id = f"id-{self._sequence}"
        opened = replace(order, status=OrderStatus.OPEN, broker_order_id=order_id)
        stored = replace(opened, status=OrderStatus.REJECTED) if self.reject_on_book else opened
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
        self.orders[order_id] = replace(
            current,
            limit_price=limit_price,
            modifications=current.modifications + 1,
        )
        return replace(current, limit_price=Decimal("1"))

    async def get_orders(self) -> list[ManagedOrder]:
        return list(self.orders.values())

    async def get_positions(self) -> PositionBook:
        return PositionBook((), ())

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


def _engine(broker: FakeBroker, **kwargs: object) -> ExecutionEngine:
    return ExecutionEngine(
        broker,
        _snapshot(),
        lot_size=LOT,
        kill_file=Path("KILL"),
        monitor=Monitor(build_metrics(CollectorRegistry()), RecordingSink()),
        **kwargs,  # type: ignore[arg-type]
    )


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
        engine = _engine(broker)
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
        engine = _engine(broker)
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
        engine = _engine(broker)
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
        engine = _engine(broker, flatten_timeout=1.0)
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
        engine = _engine(broker, flatten_timeout=0.0, flatten_worst_price=Decimal("101"))
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


def test_flatten_reprice_stops_at_the_configured_worst_price() -> None:
    async def scenario() -> None:
        broker = FakeBroker()
        engine = _engine(
            broker,
            flatten_timeout=0.0,
            flatten_reprice_step=TICK,
            flatten_worst_price=Decimal("100.90"),
        )
        await _open_long(engine, broker)
        try:
            await engine.shutdown(Decimal("101"))
        except UnmanagedPosition as exc:
            assert "id-2" in str(exc)
            assert "remaining 15" in str(exc)
        else:
            raise AssertionError("repriced flatten was marked flat")
        assert broker.modify_prices == [Decimal("100.95"), Decimal("100.90")]

    asyncio.run(scenario())


def test_flatten_reprice_stops_after_twenty_five_steps() -> None:
    async def scenario() -> None:
        broker = FakeBroker()
        engine = _engine(
            broker,
            flatten_timeout=0.0,
            flatten_reprice_step=TICK,
            flatten_worst_price=Decimal("0"),
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


def test_unknown_broker_orders_at_shutdown_are_named() -> None:
    async def scenario() -> None:
        broker = FakeBroker()
        engine = _engine(broker)
        broker.orders["stranger"] = ManagedOrder(
            tag="",
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            status=OrderStatus.OPEN,
            broker_order_id="stranger",
        )
        try:
            await engine.shutdown(Decimal("100"))
        except UnmanagedPosition as exc:
            assert "stranger" in str(exc)
        else:
            raise AssertionError("unknown order was ignored")

    asyncio.run(scenario())
