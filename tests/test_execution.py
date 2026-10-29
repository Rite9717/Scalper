from __future__ import annotations

import asyncio
import inspect
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from prometheus_client import CollectorRegistry

from scalper.clock import IST
from scalper.compliance import ComplianceError
from scalper.execution.engine import ExecutionEngine, UnmanagedPosition
from scalper.execution.halt import halt_new_entries
from scalper.execution.kite_broker import KiteBroker
from scalper.execution.paper import PaperBroker
from scalper.execution.states import IllegalTransition, ManagedOrder, OrderStatus, fill
from scalper.kite.api_facts import KITE_MAX_MODIFICATIONS_PER_ORDER
from scalper.monitor.alerts import Monitor, RecordingSink
from scalper.monitor.metrics import build_metrics
from scalper.risk.gate import RiskSnapshot

NOW = datetime(2026, 10, 27, 10, 0, tzinfo=IST)
TICK = Decimal("0.05")
LOT = 15


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


class _FillOnRead(PaperBroker):
    """Paper book that prints one price whenever the engine reads the book."""

    def __init__(self, price: Decimal) -> None:
        super().__init__(TICK, LOT)
        self.print_price = price

    async def get_orders(self) -> list[ManagedOrder]:
        self.on_trade(self.print_price, 1_000_000)
        return list(self.orders.values())


def _engine(
    snapshot: RiskSnapshot | None = None,
    broker: PaperBroker | None = None,
    **kwargs: object,
) -> ExecutionEngine:
    return ExecutionEngine(
        broker or PaperBroker(TICK, LOT),
        snapshot or _snapshot(),
        lot_size=LOT,
        kill_file=Path("KILL"),
        monitor=Monitor(build_metrics(CollectorRegistry()), RecordingSink()),
        **kwargs,  # type: ignore[arg-type]
    )


def test_partial_fills_in_both_directions_change_position_only_when_filled() -> None:
    async def scenario() -> None:
        engine = _engine()
        first = await engine.submit_entry(
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            stop_price=Decimal("99"),
            strategy_id="trend",
            timestamp_ms=1_000,
        )
        assert first.order is not None
        assert first.order.status is OrderStatus.OPEN
        assert engine.position_quantity == 0
        await engine.on_trade(Decimal("100"), 10)
        assert engine.orders[first.order.tag].status is OrderStatus.OPEN
        assert engine.position_quantity == 0
        await engine.on_trade(Decimal("99.95"), 5)
        assert engine.orders[first.order.tag].status is OrderStatus.PARTIAL
        assert engine.position_quantity == 5
        await engine.on_trade(Decimal("99.90"), 10)
        assert engine.orders[first.order.tag].status is OrderStatus.FILLED
        assert engine.position_quantity == LOT
        sale = await engine.submit_exit(
            side="sell",
            limit_price=Decimal("101"),
            quantity=LOT,
            strategy_id="trend",
            timestamp_ms=1_500,
        )
        assert sale.order is not None
        assert engine.position_quantity == LOT
        await engine.on_trade(Decimal("101.05"), 4)
        assert engine.position_quantity == LOT - 4
        await engine.on_trade(Decimal("101.10"), LOT)
        assert engine.orders[sale.order.tag].status is OrderStatus.FILLED
        assert engine.position_quantity == 0

    asyncio.run(scenario())


def test_the_same_tag_is_not_sent_twice() -> None:
    async def scenario() -> ExecutionEngine:
        engine = _engine()
        first = await engine.submit_entry(
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            stop_price=Decimal("99"),
            strategy_id="trend",
            timestamp_ms=1_000,
        )
        second = await engine.submit_entry(
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            stop_price=Decimal("99"),
            strategy_id="trend",
            timestamp_ms=1_000,
        )
        return engine, first, second

    engine, first, second = asyncio.run(scenario())
    assert first.order is not None and second.order is not None
    assert first.order.tag == second.order.tag
    assert first.order.broker_order_id == second.order.broker_order_id
    assert len(engine.broker.orders) == 1


def test_the_twenty_sixth_price_change_cancels_and_uses_a_new_tag() -> None:
    async def scenario() -> None:
        engine = _engine()
        placed = await engine.submit_entry(
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            stop_price=Decimal("99"),
            strategy_id="trend",
            timestamp_ms=2_000,
        )
        assert placed.order is not None
        current = placed.order.tag
        for step in range(KITE_MAX_MODIFICATIONS_PER_ORDER):
            result = await engine.modify_price(
                current,
                Decimal("100") + TICK * (step + 1),
                strategy_id="trend",
                timestamp_ms=3_000,
            )
            assert result.order is not None
            assert result.order.tag == current
        replaced = await engine.modify_price(current, Decimal("102"), strategy_id="trend", timestamp_ms=3_000)
        assert replaced.order is not None
        assert replaced.order.tag != current
        assert engine.orders[current].status is OrderStatus.CANCELLED

    asyncio.run(scenario())


def test_kill_switch_blocks_the_entry_and_shutdown_flattens_a_long() -> None:
    async def scenario() -> None:
        blocked = _engine(_snapshot(killed=True))
        refused = await blocked.submit_entry(
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            stop_price=Decimal("99"),
            strategy_id="trend",
            timestamp_ms=4_000,
        )
        assert refused.order is None
        assert refused.decision.allowed is False
        engine = _engine(broker=_FillOnRead(Decimal("101")))
        await engine.submit_entry(
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            stop_price=Decimal("99"),
            strategy_id="trend",
            timestamp_ms=5_000,
        )
        await engine.on_trade(Decimal("99.95"), LOT)
        assert engine.position_quantity == LOT
        report = await engine.shutdown(Decimal("101"))
        assert report.clean is True
        assert engine.position_quantity == 0

    asyncio.run(scenario())


def test_shutdown_refuses_an_unknown_broker_order() -> None:
    async def scenario() -> None:
        broker = PaperBroker(TICK, LOT)
        broker.orders["stranger"] = ManagedOrder(
            tag="",
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            status=OrderStatus.OPEN,
            broker_order_id="broker-9",
        )
        engine = _engine(broker=broker)
        with pytest.raises(UnmanagedPosition, match="broker-9"):
            await engine.shutdown(Decimal("100"))

    asyncio.run(scenario())


def test_recovery_adopts_a_filled_order_and_can_flatten_it() -> None:
    async def scenario() -> None:
        broker = _FillOnRead(Decimal("100"))
        broker.orders["adoptedtag01"] = ManagedOrder(
            tag="adoptedtag01",
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            filled_quantity=LOT,
            status=OrderStatus.FILLED,
            broker_order_id="broker-1",
        )
        engine = _engine(broker=broker)
        await engine.recover()
        assert engine.position_quantity == LOT
        report = await engine.shutdown(Decimal("100"))
        assert report.clean is True
        assert engine.position_quantity == 0

    asyncio.run(scenario())


def test_cancel_uses_the_broker_order_id_from_placement() -> None:
    async def scenario() -> None:
        engine = _engine()
        placed = await engine.submit_entry(
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            stop_price=Decimal("99"),
            strategy_id="trend",
            timestamp_ms=6_000,
        )
        assert placed.order is not None
        order_id = placed.order.broker_order_id
        assert order_id is not None
        cancelled = await engine.cancel(order_id)
        assert cancelled.broker_order_id == order_id
        assert cancelled.status is OrderStatus.CANCELLED
        assert engine.position_quantity == 0

    asyncio.run(scenario())


def test_the_engine_refuses_entries_while_the_memory_halt_is_set() -> None:
    async def scenario() -> None:
        engine = _engine(_snapshot(killed=False))
        halt_new_entries("ambiguous place abc")
        refused = await engine.submit_entry(
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            stop_price=Decimal("99"),
            strategy_id="trend",
            timestamp_ms=7_000,
        )
        assert refused.order is None
        assert refused.decision.allowed is False
        assert refused.decision.action == "halt"
        assert engine.snapshot.killed is False
        assert engine.broker.orders == {}

    asyncio.run(scenario())


def test_the_engine_places_only_through_place_live_limit() -> None:
    source = Path(inspect.getfile(ExecutionEngine)).read_text(encoding="utf-8")
    assert "place_live_limit(" in source
    assert ".place_limit(" not in source


def test_a_fill_cannot_exceed_the_remaining_quantity() -> None:
    order = ManagedOrder("tag", "buy", Decimal("100"), 15, status=OrderStatus.OPEN, broker_order_id="1")
    with pytest.raises(IllegalTransition):
        fill(order, 16, Decimal("100"))


def test_paper_mode_never_posts_a_live_order() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.content.decode())
        return httpx.Response(200, json={"status": "success", "data": {"order_id": "1"}})

    async def scenario() -> None:
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport, base_url="https://api.kite.trade") as client:
            broker = KiteBroker(
                trading_mode="paper",
                api_key="key",
                access_token="token",
                static_ip="8.8.8.8",
                observed_ip="8.8.8.8",
                client=client,
                tradingsymbol="NIFTY26OCTFUT",
            )
            order = ManagedOrder("abc123", "buy", Decimal("100"), 15)
            with pytest.raises(ComplianceError):
                await broker.place_limit(order)
            live = KiteBroker(
                trading_mode="live",
                api_key="key",
                access_token="token",
                static_ip="8.8.8.8",
                observed_ip="8.8.8.8",
                client=client,
                tradingsymbol="NIFTY26OCTFUT",
            )
            outcome = await live.place_limit(order)
            assert outcome.order.broker_order_id == "1"
            assert outcome.order.status is OrderStatus.OPEN
            assert outcome.recovered is False

    asyncio.run(scenario())
    assert len(seen) == 1
    assert "order_type=LIMIT" in seen[0]
    assert "tag=abc123" in seen[0]
