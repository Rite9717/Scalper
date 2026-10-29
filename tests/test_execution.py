from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from decimal import Decimal

import httpx
import pytest

from scalper.clock import IST
from scalper.compliance import ComplianceError
from scalper.execution.engine import BrokerView, ExecutionEngine, UnmanagedPosition
from scalper.execution.kite_broker import KiteBroker
from scalper.execution.paper import PaperBroker
from scalper.execution.states import IllegalTransition, ManagedOrder, OrderStatus, fill
from scalper.kite.api_facts import KITE_MAX_MODIFICATIONS_PER_ORDER
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


def _engine(snapshot: RiskSnapshot | None = None) -> ExecutionEngine:
    return ExecutionEngine(PaperBroker(TICK, LOT), snapshot or _snapshot(), lot_size=LOT)


def test_partial_fill_then_completion_and_a_touch_does_not_fill() -> None:
    engine = _engine()
    first = engine.submit_entry(
        side="buy",
        limit_price=Decimal("100"),
        quantity=LOT,
        stop_price=Decimal("99"),
        strategy_id="trend",
        timestamp_ms=1_000,
    )
    assert first.order is not None
    assert first.order.status is OrderStatus.OPEN
    engine.on_trade(Decimal("100"), 10)
    assert engine.orders[first.order.tag].status is OrderStatus.OPEN
    engine.on_trade(Decimal("99.95"), 5)
    assert engine.orders[first.order.tag].status is OrderStatus.PARTIAL
    assert engine.position_quantity == 5
    engine.on_trade(Decimal("99.90"), 10)
    assert engine.orders[first.order.tag].status is OrderStatus.FILLED
    assert engine.position_quantity == LOT


def test_the_same_tag_is_not_sent_twice() -> None:
    engine = _engine()
    first = engine.submit_entry(
        side="buy",
        limit_price=Decimal("100"),
        quantity=LOT,
        stop_price=Decimal("99"),
        strategy_id="trend",
        timestamp_ms=1_000,
    )
    second = engine.submit_entry(
        side="buy",
        limit_price=Decimal("100"),
        quantity=LOT,
        stop_price=Decimal("99"),
        strategy_id="trend",
        timestamp_ms=1_000,
    )
    assert first.order is not None and second.order is not None
    assert first.order.tag == second.order.tag
    assert first.order.broker_order_id == second.order.broker_order_id
    assert len(engine.broker.orders) == 1


def test_the_twenty_sixth_price_change_cancels_and_uses_a_new_tag() -> None:
    engine = _engine()
    placed = engine.submit_entry(
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
        result = engine.modify_price(
            current,
            Decimal("100") + TICK * (step + 1),
            strategy_id="trend",
            timestamp_ms=3_000,
        )
        assert result.order is not None
        assert result.order.tag == current
    replaced = engine.modify_price(current, Decimal("102"), strategy_id="trend", timestamp_ms=3_000)
    assert replaced.order is not None
    assert replaced.order.tag != current
    assert engine.orders[current].status is OrderStatus.CANCELLED


def test_kill_switch_blocks_the_entry_and_shutdown_flattens_a_long() -> None:
    blocked = _engine(_snapshot(killed=True))
    refused = blocked.submit_entry(
        side="buy",
        limit_price=Decimal("100"),
        quantity=LOT,
        stop_price=Decimal("99"),
        strategy_id="trend",
        timestamp_ms=4_000,
    )
    assert refused.order is None
    assert refused.decision.allowed is False
    engine = _engine()
    engine.submit_entry(
        side="buy",
        limit_price=Decimal("100"),
        quantity=LOT,
        stop_price=Decimal("99"),
        strategy_id="trend",
        timestamp_ms=5_000,
    )
    engine.on_trade(Decimal("99.95"), LOT)
    report = engine.shutdown(Decimal("101"))
    assert report.clean is True
    assert engine.position_quantity == 0


def test_shutdown_refuses_an_unknown_broker_order() -> None:
    engine = _engine()
    engine.recover(
        [
            BrokerView(
                tag="",
                broker_order_id="broker-9",
                status=OrderStatus.OPEN,
                side="buy",
                limit_price=Decimal("100"),
                quantity=LOT,
                filled_quantity=0,
            )
        ]
    )
    with pytest.raises(UnmanagedPosition):
        engine.shutdown(Decimal("100"))


def test_recovery_adopts_a_filled_order_and_can_flatten_it() -> None:
    engine = _engine()
    engine.recover(
        [
            BrokerView(
                tag="adoptedtag01",
                broker_order_id="broker-1",
                status=OrderStatus.FILLED,
                side="buy",
                limit_price=Decimal("100"),
                quantity=LOT,
                filled_quantity=LOT,
            )
        ]
    )
    assert engine.position_quantity == LOT
    report = engine.shutdown(Decimal("100"))
    assert report.clean is True
    assert engine.position_quantity == 0


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
