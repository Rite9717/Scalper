from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta
from decimal import Decimal

import httpx
import pytest
from prometheus_client import CollectorRegistry

from scalper.clock import IST
from scalper.compliance import ComplianceError
from scalper.execution.broker import Broker
from scalper.execution.engine import ExecutionEngine
from scalper.execution.halt import entries_halted
from scalper.execution.errors import (
    AmbiguousOrderOutcome,
    BrokerError,
    DataException,
    InputException,
    NetworkException,
    OrderException,
    OrderMappingError,
    TokenException,
)
from scalper.execution.kite_broker import (
    AMBIGUOUS_PLACE_INTERVAL_SECONDS,
    AMBIGUOUS_PLACE_POLLS,
    AMBIGUOUS_PLACE_WINDOW_SECONDS,
    KiteBroker,
)
from scalper.execution.paper import PaperBroker
from scalper.execution.states import IllegalTransition, ManagedOrder, OrderStatus
from scalper.execution.submit import engage
from scalper.execution.submit import place_live_limit
from scalper.kite.api_facts import KITE_MAX_MODIFICATIONS_PER_ORDER
from scalper.monitor.alerts import Monitor, RecordingSink
from scalper.monitor.metrics import build_metrics
from scalper.risk.gate import RiskSnapshot
from scalper.risk.killfile import is_killed, read_reason

TICK = Decimal("0.05")
LOT = 15
Action = Callable[[KiteBroker], Awaitable[object]]


def _order(tag: str = "abc123") -> ManagedOrder:
    return ManagedOrder(tag, "buy", Decimal("100"), LOT)


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "order_id": "99",
        "status": "OPEN",
        "transaction_type": "BUY",
        "quantity": LOT,
        "filled_quantity": 0,
        "price": 100,
        "average_price": 0,
        "tag": "abc123",
        "tradingsymbol": "NIFTY26OCTFUT",
        "exchange": "NFO",
        "product": "MIS",
    }
    row.update(overrides)
    return row


def _ok(data: object) -> httpx.Response:
    return httpx.Response(200, json={"status": "success", "data": data})


def _error(status: int, error_type: str, message: str = "refused") -> httpx.Response:
    return httpx.Response(status, json={"status": "error", "message": message, "error_type": error_type})


def _run(
    handler: Callable[[httpx.Request], httpx.Response],
    action: Action,
    *,
    mode: str = "live",
    observed_ip: str | None = "8.8.8.8",
    poll_interval: float = 0.0,
) -> object:
    async def scenario() -> object:
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport, base_url="https://api.kite.trade") as client:
            broker = KiteBroker(
                trading_mode=mode,
                api_key="key",
                access_token="token",
                static_ip="8.8.8.8",
                observed_ip=observed_ip,
                client=client,
                tradingsymbol="NIFTY26OCTFUT",
                poll_interval=poll_interval,
            )
            return await action(broker)

    return asyncio.run(scenario())


def test_paper_and_kite_implement_the_async_broker_protocol() -> None:
    paper = PaperBroker(TICK, LOT)
    names = ("place_limit", "cancel", "modify_price", "get_orders", "get_positions", "get_margins")
    for name in names:
        assert inspect.iscoroutinefunction(getattr(PaperBroker, name))
        assert inspect.iscoroutinefunction(getattr(KiteBroker, name))
    assert isinstance(paper, Broker)

    def handler(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("protocol check must not call Kite")

    kite = _run(handler, lambda broker: _touch_isinstance(broker), mode="paper")
    assert kite is True


async def _touch_isinstance(broker: KiteBroker) -> bool:
    return isinstance(broker, Broker)


def test_paper_protocol_rejects_bad_orders_and_reports_a_position() -> None:
    broker = PaperBroker(TICK, LOT, tradingsymbol="NIFTY26OCTFUT", equity_cash=Decimal("100000"))

    async def scenario() -> None:
        placed = await broker.place_limit(_order())
        again = await broker.place_limit(_order())
        assert placed.order.broker_order_id == again.order.broker_order_id
        assert len(await broker.get_orders()) == 1
        off_tick = await broker.place_limit(ManagedOrder("offtick1", "buy", Decimal("100.001"), LOT))
        assert off_tick.order.status is OrderStatus.REJECTED
        order_id = placed.order.broker_order_id
        assert order_id is not None
        with pytest.raises(InputException):
            await broker.modify_price(order_id, Decimal("100.001"))
        changed = await broker.modify_price(order_id, Decimal("100.05"))
        assert changed.limit_price == Decimal("100.05")
        assert changed.modifications == 1
        broker.replace_price(changed.tag, changed.limit_price, KITE_MAX_MODIFICATIONS_PER_ORDER)
        with pytest.raises(InputException):
            await broker.modify_price(order_id, Decimal("100.10"))
        with pytest.raises(BrokerError):
            await broker.cancel("missing")
        cancelled = await broker.cancel(order_id)
        assert cancelled.status is OrderStatus.CANCELLED
        fresh = await broker.place_limit(ManagedOrder("filled01", "buy", Decimal("100"), LOT))
        assert fresh.order.broker_order_id is not None
        broker.on_trade(Decimal("99.95"), LOT)
        book = await broker.get_positions()
        assert book.net[0].quantity == LOT
        assert book.net[0].average_price == Decimal("100")
        assert book.net[0].tradingsymbol == "NIFTY26OCTFUT"
        margins = await broker.get_margins()
        assert margins.equity.cash == Decimal("100000")
        assert margins.commodity.enabled is False
        with pytest.raises(IllegalTransition):
            await broker.cancel(fresh.order.broker_order_id)

    asyncio.run(scenario())


def test_paper_mode_never_reaches_the_http_layer() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return _ok([])

    async def action(broker: KiteBroker) -> None:
        order = _order()
        with pytest.raises(ComplianceError, match="unless mode is live"):
            await broker.place_limit(order)
        with pytest.raises(ComplianceError, match="unless mode is live"):
            await broker.cancel("99")
        with pytest.raises(ComplianceError, match="unless mode is live"):
            await broker.modify_price("99", Decimal("101"))
        for read in (broker.get_orders, broker.get_positions, broker.get_margins):
            with pytest.raises(ComplianceError, match="does not call Kite"):
                await read()

    _run(handler, action, mode="paper")
    assert seen == []


@pytest.mark.parametrize("method", ["POST", "PUT", "DELETE"])
def test_live_mutation_from_the_wrong_ip_does_not_reach_http(method: str) -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.method)
        return _ok({"order_id": "99"})

    async def action(broker: KiteBroker) -> None:
        with pytest.raises(ComplianceError, match="registered static IP"):
            if method == "POST":
                await broker.place_limit(_order())
            elif method == "PUT":
                await broker.modify_price("99", Decimal("101"))
            else:
                await broker.cancel("99")

    _run(handler, action, observed_ip="1.1.1.1")
    assert seen == []


def test_reads_do_not_require_the_static_ip() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/orders"
        assert request.headers["Authorization"] == "token key:token"
        return _ok([])

    orders = _run(handler, lambda broker: broker.get_orders(), observed_ip="1.1.1.1")
    assert orders == []


def test_ambiguous_place_polls_five_times_across_about_five_seconds() -> None:
    assert AMBIGUOUS_PLACE_POLLS == 5
    assert AMBIGUOUS_PLACE_WINDOW_SECONDS == 5.0
    assert AMBIGUOUS_PLACE_INTERVAL_SECONDS == 1.25
    interval = inspect.signature(KiteBroker.__init__).parameters["poll_interval"].default
    attempts = inspect.signature(KiteBroker.__init__).parameters["poll_attempts"].default
    assert interval == 1.25
    assert attempts == 5


@pytest.mark.parametrize("error", [httpx.ConnectError, httpx.ConnectTimeout])
def test_connect_failure_is_sent_once_more_and_places_one_order(error: type[Exception]) -> None:
    posts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/orders/regular"
        posts.append(request.content.decode())
        if len(posts) == 1:
            raise error("connection failed")
        return _ok({"order_id": "55"})

    outcome = _run(handler, lambda broker: broker.place_limit(_order()))
    assert len(posts) == 2
    assert posts[0] == posts[1]
    assert "order_type=LIMIT" in posts[0]
    assert "tag=abc123" in posts[0]
    assert outcome.recovered is False
    assert outcome.order.broker_order_id == "55"
    assert outcome.order.status is OrderStatus.OPEN


def test_a_second_connect_failure_does_not_post_a_third_time() -> None:
    posts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal posts
        assert request.method == "POST"
        posts += 1
        raise httpx.ConnectError("connection failed")

    with pytest.raises(NetworkException, match="did not reach Kite") as caught:
        _run(handler, lambda broker: broker.place_limit(_order()))
    assert caught.value.error_type == "ConnectError"
    assert posts == 2


def test_read_timeout_with_the_order_on_the_book_does_not_post_again() -> None:
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.method == "POST":
            raise httpx.ReadTimeout("timed out")
        assert request.url.path == "/orders"
        return _ok([_row(status="OPEN", filled_quantity=5, average_price=99.95)])

    outcome = _run(handler, lambda broker: broker.place_limit(_order()))
    assert calls == [
        ("POST", "/orders/regular"),
        ("GET", "/orders"),
    ]
    assert outcome.recovered is True
    assert outcome.order.broker_order_id == "99"
    assert outcome.order.status is OrderStatus.PARTIAL
    assert outcome.order.filled_quantity == 5
    assert outcome.order.average_price == Decimal("99.95")


def test_a_positions_error_does_not_change_an_order_found_on_the_book() -> None:
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.method == "POST":
            raise httpx.ReadTimeout("timed out")
        if request.url.path == "/portfolio/positions":
            return _error(403, "TokenException", "positions unavailable")
        return _ok([_row(status="OPEN", filled_quantity=5, average_price=99.95)])

    outcome = _run(handler, lambda broker: broker.place_limit(_order()))
    assert ("GET", "/portfolio/positions") not in calls
    assert calls == [
        ("POST", "/orders/regular"),
        ("GET", "/orders"),
    ]
    assert outcome.recovered is True
    assert outcome.order.broker_order_id == "99"
    assert outcome.order.status is OrderStatus.PARTIAL
    assert outcome.order.filled_quantity == 5


def test_an_order_book_read_failure_stays_ambiguous() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.method == "POST":
            raise httpx.ReadTimeout("timed out")
        raise httpx.ReadError("connection dropped")

    with pytest.raises(AmbiguousOrderOutcome, match="did not appear"):
        _run(handler, lambda broker: broker.place_limit(_order()))
    assert calls.count("/orders/regular") == 1
    assert calls.count("/orders") == 5
    assert "/portfolio/positions" not in calls


def test_read_timeout_finds_the_order_after_three_stale_polls() -> None:
    calls: list[tuple[str, str]] = []
    order_reads = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal order_reads
        calls.append((request.method, request.url.path))
        if request.method == "POST":
            raise httpx.ReadTimeout("timed out")
        assert request.url.path == "/orders"
        order_reads += 1
        if order_reads <= 3:
            return _ok([_row(order_id="old", tag="older1")])
        return _ok([_row(order_id="77", status="OPEN", filled_quantity=5)])

    outcome = _run(handler, lambda broker: broker.place_limit(_order()))
    posts = [call for call in calls if call[0] == "POST"]
    assert posts == [("POST", "/orders/regular")]
    assert order_reads == 4
    assert calls.count(("GET", "/orders")) == 4
    assert outcome.recovered is True
    assert outcome.order.broker_order_id == "77"
    assert outcome.order.status is OrderStatus.PARTIAL


def test_read_timeout_with_no_order_raises_and_posts_once() -> None:
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.method == "POST":
            raise httpx.ReadTimeout("timed out")
        assert request.url.path == "/orders"
        return _ok([_row(order_id="old", tag="older1")])

    with pytest.raises(AmbiguousOrderOutcome, match="did not appear") as caught:
        _run(handler, lambda broker: broker.place_limit(_order()))
    assert "reject" not in str(caught.value).lower()
    assert [call for call in calls if call[0] == "POST"] == [("POST", "/orders/regular")]
    assert calls.count(("GET", "/orders")) == 5


def test_http_500_with_the_order_on_the_book_does_not_post_again() -> None:
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.method == "POST":
            return httpx.Response(500, content=b"unavailable")
        assert request.url.path == "/orders"
        return _ok([_row(order_id="41")])

    outcome = _run(handler, lambda broker: broker.place_limit(_order()))
    assert [call for call in calls if call[0] == "POST"] == [("POST", "/orders/regular")]
    assert outcome.recovered is True
    assert outcome.order.broker_order_id == "41"
    assert outcome.order.status is OrderStatus.OPEN


@pytest.mark.parametrize(
    "error",
    [httpx.ReadError, httpx.RemoteProtocolError, httpx.WriteError, httpx.PoolTimeout],
)
def test_other_ambiguous_transport_errors_are_not_posted_again(error: type[Exception]) -> None:
    posts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal posts
        if request.method == "POST":
            posts += 1
            raise error("did not finish")
        assert request.url.path == "/orders"
        return _ok([_row()])

    outcome = _run(handler, lambda broker: broker.place_limit(_order()))
    assert posts == 1
    assert outcome.recovered is True
    assert outcome.order.broker_order_id == "99"


def test_an_unparseable_place_body_is_not_posted_again() -> None:
    posts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal posts
        if request.method == "POST":
            posts += 1
            return httpx.Response(200, content=b"<html>gateway</html>")
        assert request.url.path == "/orders"
        return _ok([_row(order_id="12")])

    outcome = _run(handler, lambda broker: broker.place_limit(_order()))
    assert posts == 1
    assert outcome.recovered is True
    assert outcome.order.broker_order_id == "12"


def test_duplicate_tags_are_not_treated_as_a_successful_place() -> None:
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path))
        if request.method == "POST":
            raise httpx.ReadTimeout("timed out")
        assert request.url.path == "/orders"
        return _ok([_row(order_id="1"), _row(order_id="2")])

    with pytest.raises(AmbiguousOrderOutcome, match="more than one"):
        _run(handler, lambda broker: broker.place_limit(_order()))
    assert [call for call in calls if call[0] == "POST"] == [("POST", "/orders/regular")]
    assert calls.count(("GET", "/orders")) == 1


@pytest.mark.parametrize(
    ("error_type", "exception"),
    [("OrderException", OrderException), ("InputException", InputException), ("TokenException", TokenException)],
)
def test_a_definite_kite_error_is_not_resent(error_type: str, exception: type[Exception]) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        return _error(400, error_type, "RMS:Margin Exceeds")

    with pytest.raises(exception) as caught:
        _run(handler, lambda broker: broker.place_limit(_order()))
    assert caught.value.error_type == error_type
    assert calls == ["POST"]


def test_a_parsed_network_exception_is_not_treated_as_an_ambiguous_place() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        return _error(200, "NetworkException", "gateway")

    with pytest.raises(NetworkException) as caught:
        _run(handler, lambda broker: broker.place_limit(_order()))
    assert caught.value.error_type == "NetworkException"
    assert calls == ["POST"]


def test_ambiguous_place_engages_the_kill_file_and_alerts(tmp_path) -> None:
    posts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal posts
        if request.method == "POST":
            posts += 1
            raise httpx.ReadTimeout("timed out")
        assert request.url.path == "/orders"
        return _ok([])

    async def scenario() -> tuple[RecordingSink, object]:
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport, base_url="https://api.kite.trade") as client:
            broker = KiteBroker(
                trading_mode="live",
                api_key="key",
                access_token="token",
                static_ip="8.8.8.8",
                observed_ip="8.8.8.8",
                client=client,
                tradingsymbol="NIFTY26OCTFUT",
                poll_interval=0,
            )
            sink = RecordingSink()
            monitor = Monitor(build_metrics(CollectorRegistry()), sink)
            kill_file = tmp_path / "KILL"
            with pytest.raises(AmbiguousOrderOutcome):
                await place_live_limit(broker, _order(), kill_file=kill_file, monitor=monitor)
            return sink, kill_file

    sink, kill_file = asyncio.run(scenario())
    assert posts == 1
    assert is_killed(kill_file)
    assert "abc123" in (read_reason(kill_file) or "")
    assert sink.messages == [f"Kill switch on: {read_reason(kill_file)}"]


class _CallOrder:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[str] = []
        self.reasons: list[str] = []

    async def kill_switch(self, engaged: bool, reason: str) -> None:
        self.calls.append("kill_switch")
        self.reasons.append(reason)
        if self.fail:
            raise RuntimeError("alert down")


def _entry_snapshot() -> RiskSnapshot:
    now = datetime(2026, 10, 27, 10, 0, tzinfo=IST)
    return RiskSnapshot(
        now=now,
        trading_mode="paper",
        lots_requested=1,
        open_lots=0,
        max_lots=1,
        planned_loss=Decimal("0"),
        max_loss_per_trade=Decimal("3000"),
        daily_pnl=Decimal("0"),
        daily_loss_limit=Decimal("5000"),
        orders_last_second=0,
        orders_last_minute=0,
        max_orders_per_second=3,
        max_orders_per_minute=150,
        last_quote_at=now,
        stale_after=timedelta(seconds=5),
        available_margin=Decimal("100000"),
        required_margin=Decimal("20000"),
        flatten_time=datetime.strptime("15:20", "%H:%M").time(),
        broker_pnl=None,
        internal_pnl=None,
        pnl_tolerance=Decimal("250"),
        exchange_time=now,
        clock_drift_limit=timedelta(seconds=2),
        recent_rejects=0,
        max_rejects=3,
        killed=False,
    )


def _readonly_dir(tmp_path) -> object:
    directory = tmp_path / "readonly"
    directory.mkdir()
    directory.chmod(0o555)
    return directory


async def _place_ambiguous(kill_file, monitor) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            raise httpx.ReadTimeout("timed out")
        assert request.url.path == "/orders"
        return _ok([])

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="https://api.kite.trade") as client:
        broker = KiteBroker(
            trading_mode="live",
            api_key="key",
            access_token="token",
            static_ip="8.8.8.8",
            observed_ip="8.8.8.8",
            client=client,
            tradingsymbol="NIFTY26OCTFUT",
            poll_interval=0,
        )
        await place_live_limit(broker, _order(), kill_file=kill_file, monitor=monitor)


def test_a_read_only_kill_directory_still_alerts(tmp_path) -> None:
    directory = _readonly_dir(tmp_path)
    sink = RecordingSink()
    monitor = Monitor(build_metrics(CollectorRegistry()), sink)
    kill_file = directory / "KILL"
    try:
        with pytest.raises(AmbiguousOrderOutcome) as caught:
            asyncio.run(_place_ambiguous(kill_file, monitor))
        assert "kill file was not engaged" in str(caught.value)
        assert sink.messages
        assert "kill file was not engaged" in sink.messages[0]
        assert not is_killed(kill_file)
    finally:
        directory.chmod(0o755)


def test_an_alert_error_after_a_successful_kill_keeps_the_original_outcome(tmp_path) -> None:
    switch = _CallOrder(fail=True)
    kill_file = tmp_path / "KILL"
    with pytest.raises(AmbiguousOrderOutcome) as caught:
        asyncio.run(_place_ambiguous(kill_file, switch))
    assert is_killed(kill_file)
    assert "did not appear" in str(caught.value)
    assert "alert down" not in str(caught.value)
    assert not isinstance(caught.value.__cause__, RuntimeError)
    assert switch.calls == ["kill_switch"]


def test_kill_and_alert_failures_both_raise_ambiguous(tmp_path) -> None:
    directory = _readonly_dir(tmp_path)
    switch = _CallOrder(fail=True)
    try:
        with pytest.raises(AmbiguousOrderOutcome) as caught:
            asyncio.run(_place_ambiguous(directory / "KILL", switch))
        assert "kill file was not engaged" in str(caught.value)
        assert not isinstance(caught.value, RuntimeError)
        assert switch.calls == ["kill_switch"]
    finally:
        directory.chmod(0o755)


def test_engage_runs_before_the_alert(tmp_path, monkeypatch) -> None:
    sequence: list[str] = []

    def record_engage(path, reason) -> None:
        sequence.append("engage")
        assert entries_halted()
        engage(path, reason)

    monkeypatch.setattr("scalper.execution.submit.engage", record_engage)
    switch = _CallOrder()

    async def scenario() -> None:
        real_switch = switch.kill_switch

        async def record_switch(engaged: bool, reason: str) -> None:
            sequence.append("kill_switch")
            await real_switch(engaged, reason)

        switch.kill_switch = record_switch  # type: ignore[method-assign]
        await _place_ambiguous(tmp_path / "KILL", switch)

    with pytest.raises(AmbiguousOrderOutcome):
        asyncio.run(scenario())
    assert sequence == ["engage", "kill_switch"]


def test_a_failed_kill_file_still_blocks_the_next_entry(tmp_path) -> None:
    directory = _readonly_dir(tmp_path)
    sink = RecordingSink()
    monitor = Monitor(build_metrics(CollectorRegistry()), sink)

    async def scenario() -> None:
        engine = ExecutionEngine(
            PaperBroker(TICK, LOT, tradingsymbol="NIFTY26OCTFUT"),
            _entry_snapshot(),
            tradingsymbol="NIFTY26OCTFUT",
            exchange="NFO",
            lot_size=LOT,
            kill_file=tmp_path / "engine-kill",
            monitor=monitor,
        )
        first = await engine.submit_entry(
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            stop_price=Decimal("99"),
            strategy_id="trend",
            timestamp_ms=1_000,
        )
        assert first.order is not None
        assert engine.snapshot.killed is False
        with pytest.raises(AmbiguousOrderOutcome):
            await _place_ambiguous(directory / "KILL", monitor)
        assert not is_killed(directory / "KILL")
        assert entries_halted()
        second = await engine.submit_entry(
            side="buy",
            limit_price=Decimal("100"),
            quantity=LOT,
            stop_price=Decimal("99"),
            strategy_id="trend",
            timestamp_ms=2_000,
        )
        assert second.order is None
        assert second.decision.allowed is False
        assert second.decision.action == "halt"
        assert len(engine.broker.orders) == 1

    try:
        asyncio.run(scenario())
    finally:
        directory.chmod(0o755)


def test_get_orders_maps_kite_statuses_including_partial_fills() -> None:
    rows = [
        _row(order_id="1", status="OPEN", filled_quantity=0, tag="open1"),
        _row(order_id="2", status="OPEN", filled_quantity=5, average_price=99.95, tag="part1"),
        _row(order_id="3", status="OPEN", filled_quantity=LOT, average_price=100, tag="full1"),
        _row(order_id="4", status="COMPLETE", filled_quantity=LOT, average_price=100, tag="done1"),
        _row(order_id="5", status="CANCELLED", filled_quantity=5, average_price=99.9, tag="gone1"),
        _row(order_id="6", status="REJECTED", tag="bad1"),
        _row(order_id="7", status="TRIGGER PENDING", tag="trig1"),
        _row(order_id="8", status="CANCEL PENDING", filled_quantity=0, tag="pend1"),
        _row(order_id="9", status="MODIFY PENDING", filled_quantity=5, tag="mod1"),
    ]

    orders = _run(lambda _request: _ok(rows), lambda broker: broker.get_orders())
    by_tag = {order.tag: order for order in orders}
    assert by_tag["open1"].status is OrderStatus.OPEN
    assert by_tag["open1"].tradingsymbol == "NIFTY26OCTFUT"
    assert by_tag["open1"].exchange == "NFO"
    assert by_tag["open1"].product == "MIS"
    assert by_tag["part1"].status is OrderStatus.PARTIAL
    assert by_tag["part1"].filled_quantity == 5
    assert by_tag["full1"].status is OrderStatus.FILLED
    assert by_tag["done1"].status is OrderStatus.FILLED
    assert by_tag["gone1"].status is OrderStatus.CANCELLED
    assert by_tag["gone1"].filled_quantity == 5
    assert by_tag["bad1"].status is OrderStatus.REJECTED
    assert by_tag["trig1"].status is OrderStatus.OPEN
    assert by_tag["pend1"].status is OrderStatus.OPEN
    assert by_tag["mod1"].status is OrderStatus.PARTIAL


def test_an_unknown_kite_status_is_not_invented() -> None:
    with pytest.raises(OrderMappingError, match="unmapped"):
        _run(lambda _request: _ok([_row(status="FOO")]), lambda broker: broker.get_orders())


def test_a_complete_order_that_is_not_filled_is_not_marked_filled() -> None:
    with pytest.raises(OrderMappingError, match="COMPLETE"):
        _run(
            lambda _request: _ok([_row(status="COMPLETE", filled_quantity=0)]),
            lambda broker: broker.get_orders(),
        )


def test_get_orders_read_error_is_a_network_exception() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ReadError("connection dropped")

    with pytest.raises(NetworkException) as caught:
        _run(handler, lambda broker: broker.get_orders())
    assert caught.value.error_type == "ReadError"


def test_cancel_timeout_is_not_reported_as_cancelled() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        raise httpx.ConnectError("connection dropped")

    with pytest.raises(NetworkException):
        _run(handler, lambda broker: broker.cancel("99"))
    assert calls == ["DELETE"]


def test_cancel_returns_the_book_order_including_a_partial_fill() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "DELETE":
            assert request.url.path == "/orders/regular/99"
            return _ok({"order_id": "99"})
        return _ok([_row(status="CANCELLED", filled_quantity=5, average_price=99.9)])

    order = _run(handler, lambda broker: broker.cancel("99"))
    assert order.status is OrderStatus.CANCELLED
    assert order.filled_quantity == 5
    assert order.broker_order_id == "99"


def test_a_dropped_book_read_after_cancel_does_not_send_another_cancel() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "DELETE":
            return _ok({"order_id": "99"})
        raise httpx.ReadError("connection dropped")

    with pytest.raises(AmbiguousOrderOutcome, match="acknowledged") as caught:
        _run(handler, lambda broker: broker.cancel("99"))
    assert "reject" not in str(caught.value).lower()
    assert calls == ["DELETE", "GET"]


def test_cancel_does_not_invent_an_order_missing_from_the_book() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "DELETE":
            return _ok({"order_id": "99"})
        return _ok([])

    with pytest.raises(DataException, match="not in the order book"):
        _run(handler, lambda broker: broker.cancel("99"))


def test_modify_price_stays_a_limit_and_returns_the_book() -> None:
    bodies: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "PUT":
            assert request.url.path == "/orders/regular/99"
            bodies.append(request.content.decode())
            return _ok({"order_id": "99"})
        return _ok([_row(price=100, status="OPEN", filled_quantity=5)])

    order = _run(handler, lambda broker: broker.modify_price("99", Decimal("101")))
    assert bodies == ["order_type=LIMIT&price=101"]
    assert order.limit_price == Decimal("100")
    assert order.status is OrderStatus.PARTIAL


def test_modify_timeout_is_not_reported_as_a_new_price() -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        raise httpx.ReadTimeout("timed out")

    with pytest.raises(NetworkException):
        _run(handler, lambda broker: broker.modify_price("99", Decimal("101")))
    assert calls == ["PUT"]


def test_positions_and_margins_are_mapped() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/portfolio/positions":
            return _ok(
                {
                    "net": [
                        {
                            "tradingsymbol": "NIFTY26OCTFUT",
                            "exchange": "NFO",
                            "product": "MIS",
                            "quantity": LOT,
                            "overnight_quantity": 0,
                            "average_price": 24500.05,
                        }
                    ],
                    "day": [],
                }
            )
        assert request.url.path == "/user/margins"
        return _ok(
            {
                "equity": {"enabled": True, "net": 1000, "available": {"cash": 800, "live_balance": 750.5}},
                "commodity": {"enabled": False, "net": 0, "available": {"cash": 0, "live_balance": 0}},
            }
        )

    book = _run(handler, lambda broker: broker.get_positions())
    margins = _run(handler, lambda broker: broker.get_margins())
    assert book.day == ()
    assert book.net[0].tradingsymbol == "NIFTY26OCTFUT"
    assert book.net[0].quantity == LOT
    assert book.net[0].average_price == Decimal("24500.05")
    assert margins.equity.enabled is True
    assert margins.equity.cash == Decimal("800")
    assert margins.equity.live_balance == Decimal("750.5")
    assert margins.commodity.enabled is False


def test_a_positions_payload_without_a_book_is_rejected() -> None:
    with pytest.raises(DataException, match="net or day"):
        _run(lambda _request: _ok({"net": []}), lambda broker: broker.get_positions())


def test_a_slashed_order_id_is_refused_before_http() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return _ok({"order_id": "1"})

    with pytest.raises(InputException):
        _run(handler, lambda broker: broker.cancel("99/100"))
    assert seen == []
