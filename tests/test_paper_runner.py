from __future__ import annotations

import asyncio
import signal
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

from scalper.backtest.strategy import EntrySignal, MarketState
from scalper.clock import IST
from scalper.config import Settings
from scalper.data.bridge import AsyncTickerBridge
from scalper.data.cache import MemoryQuoteCache
from scalper.data.candles import MinuteBook
from scalper.data.models import Candle, NormalizedQuote
from scalper.execution.engine import ExecutionEngine, UnmanagedPosition
from scalper.execution.halt import entries_halted
from scalper.execution.kite_broker import KiteBroker
from scalper.execution.paper import PaperBroker
from scalper.execution.snapshot import LiveSnapshotProvider
from scalper.execution.states import fill
from scalper.kite.session import KiteSession, save_session
from scalper.monitor.alerts import Monitor, RecordingSink
from scalper.monitor.metrics import build_metrics
from scalper.paper.bars import SessionMinuteBook
from scalper.paper.command import _KiteQuotes
from scalper.paper.journal import MemoryJournal
from scalper.paper.limits import PaperLimits, account_limits, resolved_limits
from scalper.paper.runner import FeedDrop, PaperRunner, Silence, Stop, install_stop_signals
from scalper.paper.startup import PaperStartupError, refuse_to_start
from scalper.risk.killfile import engage, is_killed

DAY = datetime(2026, 10, 27, 10, 0, tzinfo=IST)
TICK = Decimal("0.05")
LOT = 15


class _Script:
    name = "trend_pullback"

    def __init__(self, signals: list[EntrySignal | None]) -> None:
        self._signals = list(signals)
        self.seen = 0

    def on_bar(self, state: MarketState) -> EntrySignal | None:
        self.seen += 1
        if not self._signals:
            return None
        return self._signals.pop(0)


class _FillFlatten(PaperBroker):
    async def get_orders(self):
        for tag, order in list(self.orders.items()):
            if order.intent == "flatten" and order.working:
                self.orders[tag] = fill(order, order.quantity, order.limit_price)
        return await super().get_orders()


def _quote(moment: datetime, price: str, volume: int) -> NormalizedQuote:
    px = Decimal(price)
    return NormalizedQuote(
        instrument_token=1,
        received_at=moment,
        exchange_ts=moment,
        last_price=px,
        last_quantity=1,
        volume=volume,
        average_price=px,
        oi=None,
        open=px,
        high=px,
        low=px,
        close=px,
        depth=(),
    )


def _settings() -> Settings:
    return Settings(
        expected_user_id="AB1234",
        kite_api_key="key",
        paper_max_lots=1,
        max_loss_per_trade=Decimal("20"),
        daily_loss_limit=Decimal("5000"),
        max_orders_per_second=3,
        max_orders_per_minute=150,
        flatten_time="15:20",
        latency_bars=1,
        slippage_ticks=1,
    )


async def _asleep(_seconds: float) -> None:
    return None


class _FastClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += float(seconds)


def _runner(
    *,
    broker: PaperBroker | None = None,
    strategy: _Script | None = None,
    paper: PaperLimits | None = None,
    now=None,
    sleep=_asleep,
    flatten_timeout: float = 30.0,
    flatten_poll: float = 0.5,
) -> tuple[PaperRunner, MemoryJournal, dict[str, datetime]]:
    settings = _settings()
    limits = paper or PaperLimits(flatten_timeout=flatten_timeout, flatten_poll_interval=flatten_poll)
    gate = account_limits(settings, limits)
    clock = {"now": DAY}
    if now is None:
        now = lambda: clock["now"]
    cache = MemoryQuoteCache()
    engine_clock = _FastClock()
    broker = broker or PaperBroker(TICK, LOT, tradingsymbol="NIFTY26OCTFUT", equity_cash=Decimal("1000000"))
    monitor = Monitor(build_metrics(), RecordingSink())
    engine = ExecutionEngine(
        broker,
        LiveSnapshotProvider(
            broker=broker,
            cache=cache,
            instrument_token=1,
            tradingsymbol="NIFTY26OCTFUT",
            exchange="NFO",
            kill_file=Path("paper-kill"),
            live=False,
            fail_closed=True,
            limits=gate,
            now=now,
            tick_size=TICK,
        ),
        tradingsymbol="NIFTY26OCTFUT",
        exchange="NFO",
        lot_size=LOT,
        tick_size=TICK,
        kill_file=Path("paper-kill"),
        monitor=monitor,
        flatten_timeout=limits.flatten_timeout,
        flatten_poll_interval=limits.flatten_poll_interval,
        flatten_max_adverse_ticks=limits.flatten_max_adverse_ticks,
        clock=engine_clock.monotonic,
        sleep=engine_clock.sleep,
    )
    journal = MemoryJournal()
    runner = PaperRunner(
        engine=engine,
        broker=broker,
        cache=cache,
        book=SessionMinuteBook(MinuteBook(1)),
        strategy=strategy or _Script([]),
        journal=journal,
        run_id=uuid4(),
        monitor=monitor,
        kill_file=Path("paper-kill"),
        account=gate,
        paper=limits,
        now=now,
        sleep=sleep,
        instrument_token=1,
        quantity=LOT,
        latency_bars=settings.latency_bars,
        tick_size=TICK,
        limits_record=resolved_limits(settings, limits),
        pace=False,
    )
    return runner, journal, clock


def _good_signal() -> EntrySignal:
    return EntrySignal("buy", Decimal("100"), Decimal("99"), Decimal("102"), 5, "trend")


def _wide_signal() -> EntrySignal:
    return EntrySignal("buy", Decimal("100"), Decimal("90"), Decimal("110"), 5, "trend")


async def _stream(items: list[object]):
    for item in items:
        yield item


def test_replayed_ticks_persist_signal_decision_order_fill_and_exit() -> None:
    async def scenario() -> None:
        runner, journal, clock = _runner(strategy=_Script([_good_signal(), _wide_signal()]))
        ticks = [
            _quote(DAY, "100", 10),
            _quote(DAY + timedelta(minutes=1), "100.05", 20),
            _quote(DAY + timedelta(minutes=2), "100.05", 30),
            _quote(DAY + timedelta(minutes=3), "99.90", 40),
            _quote(DAY + timedelta(minutes=4), "99", 50),
            _quote(DAY + timedelta(minutes=5), "99.10", 60),
            _quote(DAY + timedelta(minutes=6), "100", 70),
            _quote(DAY + timedelta(minutes=7), "100", 80),
        ]

        async def source():
            for tick in ticks:
                clock["now"] = tick.received_at
                yield tick
            yield Stop()

        await runner.run(source())
        view = await journal.read(runner.run_id)
        assert view["runs"][0]["limits"]["max_loss_per_trade"] == "20"
        assert view["runs"][0]["strategy"] == "trend_pullback"
        assert any(row["side"] == "buy" and row["accepted"] is True for row in view["signals"])
        assert any(row["allowed"] is True and row["intent"] == "entry" for row in view["decisions"])
        assert any(row["allowed"] is False and row["reason"] == "loss per trade cap" for row in view["decisions"])
        assert any(row["intent"] == "entry" and row["status"] == "open" for row in view["orders"])
        assert any(row["quantity"] == LOT for row in view["fills"])
        assert any(row["reason"] == "stop" and row["trigger_price"] == "99" and row["fill_price"] is not None for row in view["exits"])

    asyncio.run(scenario())


def test_unfilled_entry_is_cancelled_after_the_backtest_time_stop() -> None:
    async def scenario() -> None:
        short = EntrySignal("buy", Decimal("100"), Decimal("99"), Decimal("102"), 1, "trend")
        runner, _journal, clock = _runner(strategy=_Script([short]))
        ticks = [
            _quote(DAY, "100", 10),
            _quote(DAY + timedelta(minutes=1), "100.05", 20),
            _quote(DAY + timedelta(minutes=2), "100.05", 30),
        ]

        async def source():
            for tick in ticks:
                clock["now"] = tick.received_at
                yield tick
            yield Stop()

        await runner.run(source())
        assert runner.pending is None
        assert runner.position is None
        assert any(order.status.value == "cancelled" and order.intent == "entry" for order in runner.broker.orders.values())

    asyncio.run(scenario())


def test_sigterm_flattens_an_open_position() -> None:
    async def scenario() -> None:
        broker = _FillFlatten(TICK, LOT, tradingsymbol="NIFTY26OCTFUT", equity_cash=Decimal("1000000"))
        runner, _journal, clock = _runner(broker=broker, strategy=_Script([None, _good_signal()]))
        loop = asyncio.get_running_loop()
        install_stop_signals(loop, runner)
        ticks = [
            _quote(DAY, "100", 10),
            _quote(DAY + timedelta(minutes=1), "100", 20),
            _quote(DAY + timedelta(minutes=2), "100.05", 30),
            _quote(DAY + timedelta(minutes=3), "99.90", 40),
        ]

        async def source():
            for tick in ticks:
                clock["now"] = tick.received_at
                yield tick
            loop.call_soon(lambda: os_kill())
            await asyncio.sleep(0)
            while not runner._stop.is_set():
                await asyncio.sleep(0)
            yield Stop()

        def os_kill() -> None:
            signal.raise_signal(signal.SIGTERM)

        await runner.run(source())
        assert runner.engine.position_quantity == 0
        assert any(order.intent == "flatten" for order in broker.orders.values())
        assert is_killed(Path("paper-kill")) is False

    asyncio.run(scenario())


def test_a_shutdown_that_cannot_flatten_engages_the_kill_file() -> None:
    async def scenario() -> None:
        runner, _journal, clock = _runner(
            strategy=_Script([None, _good_signal()]),
            paper=PaperLimits(flatten_timeout=0.4, flatten_poll_interval=0.5),
            flatten_timeout=0.4,
            flatten_poll=0.5,
        )
        ticks = [
            _quote(DAY, "100", 10),
            _quote(DAY + timedelta(minutes=1), "100", 20),
            _quote(DAY + timedelta(minutes=2), "100.05", 30),
            _quote(DAY + timedelta(minutes=3), "99.90", 40),
        ]

        async def source():
            for tick in ticks:
                clock["now"] = tick.received_at
                yield tick
            yield Stop()

        with pytest.raises(UnmanagedPosition):
            await runner.run(source())
        assert is_killed(Path("paper-kill")) is True
        assert any("Kill switch" in message for message in runner.monitor.sink.messages)  # type: ignore[attr-defined]

    asyncio.run(scenario())


def test_websocket_drop_mid_position_blocks_entries_until_a_fresh_tick() -> None:
    async def scenario() -> None:
        runner, journal, clock = _runner(strategy=_Script([_good_signal(), _good_signal()]))
        from scalper.data.models import Candle

        runner.gap = True
        runner.bars = [
            Candle(1, DAY, "minute", Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), 1, None, "ticks")
        ]
        await runner.journal.start_run(
            run_id=runner.run_id,
            started_at=DAY,
            strategy="trend_pullback",
            tradingsymbol="NIFTY26OCTFUT",
            limits=runner.limits_record,
        )
        await runner._on_bar()
        view = await journal.read(runner.run_id)
        assert view["decisions"][0]["reason"] == "feed gap"
        assert runner.broker.orders == {}
        clock["now"] = DAY + timedelta(minutes=1)
        await runner._on_quote(_quote(DAY + timedelta(minutes=1), "100", 20))
        assert runner.gap is False

    asyncio.run(scenario())


def test_an_unfilled_stop_is_repriced_then_the_kill_file_is_engaged() -> None:
    async def scenario() -> None:
        runner, _journal, clock = _runner(
            strategy=_Script([_good_signal()]),
            paper=PaperLimits(flatten_timeout=30.0, flatten_poll_interval=0.5, flatten_max_adverse_ticks=2),
        )
        ticks = [
            _quote(DAY, "100", 10),
            _quote(DAY + timedelta(minutes=1), "100.05", 20),
            _quote(DAY + timedelta(minutes=2), "99.90", 30),
            _quote(DAY + timedelta(minutes=2, seconds=1), "99", 40),
            _quote(DAY + timedelta(minutes=2, seconds=2), "99", 50),
        ]

        async def source():
            for tick in ticks:
                clock["now"] = tick.received_at
                yield tick
            assert runner.exit is not None
            assert runner.exit.order.limit_price == Decimal("98.95")
            clock["now"] = ticks[-1].received_at + timedelta(seconds=40)
            yield _quote(DAY + timedelta(minutes=2, seconds=3), "98.90", 60)

        with pytest.raises(UnmanagedPosition):
            await runner.run(source())
        assert is_killed(Path("paper-kill")) is True

    asyncio.run(scenario())


def test_a_ticker_disconnect_reaches_the_runner_and_blocks_entries() -> None:
    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        bridge = AsyncTickerBridge(loop, asyncio.Queue())
        bridge.on_close(None, 1000, "dropped")
        await asyncio.sleep(0)
        drop = await _KiteQuotes(bridge, TICK, 1).__anext__()
        assert isinstance(drop, FeedDrop)
        runner, journal, clock = _runner(strategy=_Script([_good_signal()]))
        runner.bars = [
            Candle(1, DAY, "minute", Decimal("100"), Decimal("100"), Decimal("100"), Decimal("100"), 1, None, "ticks")
        ]

        async def source():
            yield drop
            yield Stop()

        await runner.run(source())
        assert runner.gap is True
        clock["now"] = DAY + timedelta(minutes=1)
        await runner._on_bar()
        view = await journal.read(runner.run_id)
        assert any(row["reason"] == "feed gap" for row in view["decisions"])
        assert runner.broker.orders == {}

    asyncio.run(scenario())


def test_feed_stall_alerts_once_and_an_open_position_goes_blind() -> None:
    async def scenario() -> None:
        runner, _journal, clock = _runner(
            strategy=_Script([None, _good_signal()]),
            paper=PaperLimits(stale_after=timedelta(seconds=5), stops_blind_grace=timedelta(seconds=30), flatten_timeout=0.4, flatten_poll_interval=0.5),
            flatten_timeout=0.4,
            flatten_poll=0.5,
        )
        ticks = [
            _quote(DAY, "100", 10),
            _quote(DAY + timedelta(minutes=1), "100", 20),
            _quote(DAY + timedelta(minutes=2), "100.05", 30),
            _quote(DAY + timedelta(minutes=3), "99.90", 40),
        ]

        async def source():
            for tick in ticks:
                clock["now"] = tick.received_at
                yield tick
            clock["now"] = ticks[-1].received_at + timedelta(seconds=6)
            yield Silence()
            yield Silence()
            clock["now"] = ticks[-1].received_at + timedelta(seconds=40)
            yield Silence()

        with pytest.raises(UnmanagedPosition):
            await runner.run(source())
        messages = runner.monitor.sink.messages  # type: ignore[attr-defined]
        assert sum("Feed stall" in message for message in messages) == 1
        assert any("feed is blind" in message for message in messages)
        assert runner.monitor.metrics.stops_blind_seconds._value.get() > 0  # type: ignore[attr-defined]
        assert is_killed(Path("paper-kill")) is True

    asyncio.run(scenario())


def test_paper_runner_never_calls_an_order_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    def reject_live(self, *args: object, **kwargs: object) -> None:
        if kwargs.get("trading_mode") == "live":
            raise AssertionError("paper runner constructed KiteBroker with trading_mode=live")
        raise AssertionError("paper runner constructed KiteBroker")

    monkeypatch.setattr(KiteBroker, "__init__", reject_live)

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/orders"):
            raise AssertionError(request.url.path)
        return httpx.Response(204)

    transport = httpx.MockTransport(handler)

    class Guarded(httpx.AsyncClient):
        def __init__(self, *args: object, **kwargs: object) -> None:
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", Guarded)

    async def prove_transport() -> None:
        async with httpx.AsyncClient() as client:
            with pytest.raises(AssertionError, match="/orders"):
                await client.post("https://api.kite.trade/orders/regular")

    asyncio.run(prove_transport())
    for path in Path("src/scalper/paper").glob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert "KiteBroker(" not in text
        assert 'trading_mode="live"' not in text

    async def scenario() -> None:
        runner, _journal, clock = _runner(strategy=_Script([None]))

        async def source():
            tick = _quote(DAY, "100", 10)
            clock["now"] = tick.received_at
            yield tick
            yield Stop()

        await runner.run(source())

    asyncio.run(scenario())


def _session(path: Path, *, expires: datetime) -> None:
    save_session(
        path,
        KiteSession(
            user_id="AB1234",
            user_type="individual",
            access_token="token",
            login_time=datetime(2026, 10, 27, 9, 0, tzinfo=IST),
            expires_at=expires,
            api_key="key",
        ),
    )


def test_a_runner_error_closes_the_ticker_before_the_database() -> None:
    async def scenario() -> None:
        order: list[str] = []

        class Ticker:
            def close(self) -> None:
                order.append("close")

            def stop(self) -> None:
                order.append("stop")

        class Health:
            async def stop(self) -> None:
                order.append("health")

        class Connection:
            async def close(self) -> None:
                assert order[:2] == ["close", "stop"]
                order.append("database")

        runner, _journal, _clock = _runner(strategy=_Script([]))

        async def explode(_feed):
            raise RuntimeError("runner down")

        runner.run = explode  # type: ignore[method-assign]
        with pytest.raises(RuntimeError, match="runner down"):
            await serve(runner, Ticker(), Health(), Connection())
        assert order == ["close", "stop", "health", "database"]

    async def serve(runner, ticker, health, connection) -> None:
        from scalper.paper.command import serve_paper

        await serve_paper(runner, _stream([]), ticker, health, connection)

    asyncio.run(scenario())


def test_startup_refuses_when_the_kill_file_exists(tmp_path: Path) -> None:
    kill = tmp_path / "KILL"
    engage(kill, "manual")

    async def scenario() -> None:
        with pytest.raises(PaperStartupError, match="kill file"):
            await refuse_to_start(
                kill_file=kill,
                session_path=tmp_path / "session.json",
                expected_user_id="AB1234",
                api_key="key",
                now=DAY,
                instrument_master=tmp_path / "nfo.csv",
                database_ping=_ping_ok,
            )

    asyncio.run(scenario())
    assert is_killed(kill) is True


def test_startup_refuses_a_missing_session(tmp_path: Path) -> None:
    async def scenario() -> None:
        with pytest.raises(PaperStartupError, match="no Kite session"):
            await refuse_to_start(
                kill_file=tmp_path / "KILL",
                session_path=tmp_path / "missing.json",
                expected_user_id="AB1234",
                api_key="key",
                now=DAY,
                instrument_master=tmp_path / "nfo.csv",
                database_ping=_ping_ok,
            )

    asyncio.run(scenario())


def test_startup_refuses_an_expired_session(tmp_path: Path) -> None:
    _session(tmp_path / "session.json", expires=datetime(2026, 10, 27, 6, 0, tzinfo=IST))

    async def scenario() -> None:
        with pytest.raises(PaperStartupError, match="expired"):
            await refuse_to_start(
                kill_file=tmp_path / "KILL",
                session_path=tmp_path / "session.json",
                expected_user_id="AB1234",
                api_key="key",
                now=DAY,
                instrument_master=tmp_path / "nfo.csv",
                database_ping=_ping_ok,
            )

    asyncio.run(scenario())


def test_startup_refuses_a_different_user(tmp_path: Path) -> None:
    _session(tmp_path / "session.json", expires=datetime(2026, 10, 28, 6, 0, tzinfo=IST))

    async def scenario() -> None:
        with pytest.raises(PaperStartupError, match="different user"):
            await refuse_to_start(
                kill_file=tmp_path / "KILL",
                session_path=tmp_path / "session.json",
                expected_user_id="ZZ9999",
                api_key="key",
                now=DAY,
                instrument_master=tmp_path / "nfo.csv",
                database_ping=_ping_ok,
            )

    asyncio.run(scenario())


def test_startup_refuses_the_maintenance_window(tmp_path: Path) -> None:
    _session(tmp_path / "session.json", expires=datetime(2026, 10, 28, 6, 0, tzinfo=IST))

    async def scenario() -> None:
        with pytest.raises(PaperStartupError, match="00:00-06:00"):
            await refuse_to_start(
                kill_file=tmp_path / "KILL",
                session_path=tmp_path / "session.json",
                expected_user_id="AB1234",
                api_key="key",
                now=datetime(2026, 10, 27, 1, 0, tzinfo=IST),
                instrument_master=tmp_path / "nfo.csv",
                database_ping=_ping_ok,
            )

    asyncio.run(scenario())


def test_startup_refuses_a_missing_instrument_master(tmp_path: Path) -> None:
    _session(tmp_path / "session.json", expires=datetime(2026, 10, 28, 6, 0, tzinfo=IST))

    async def scenario() -> None:
        with pytest.raises(PaperStartupError, match="instrument master"):
            await refuse_to_start(
                kill_file=tmp_path / "KILL",
                session_path=tmp_path / "session.json",
                expected_user_id="AB1234",
                api_key="key",
                now=DAY,
                instrument_master=tmp_path / "missing.csv",
                database_ping=_ping_ok,
            )

    asyncio.run(scenario())


def test_startup_refuses_an_unreachable_database(tmp_path: Path) -> None:
    _session(tmp_path / "session.json", expires=datetime(2026, 10, 28, 6, 0, tzinfo=IST))
    master = tmp_path / "nfo.csv"
    master.write_text("x\n", encoding="utf-8")

    async def down() -> None:
        raise ConnectionError("connection refused")

    async def scenario() -> None:
        with pytest.raises(PaperStartupError, match="database is unreachable"):
            await refuse_to_start(
                kill_file=tmp_path / "KILL",
                session_path=tmp_path / "session.json",
                expected_user_id="AB1234",
                api_key="key",
                now=DAY,
                instrument_master=master,
                database_ping=down,
            )

    asyncio.run(scenario())


async def _ping_ok() -> None:
    return None


class _BoomJournal(MemoryJournal):
    def __init__(self, method: str) -> None:
        super().__init__()
        self.method = method

    async def record_order(self, **kwargs: object) -> None:
        if self.method == "record_order":
            raise RuntimeError("journal order")
        await super().record_order(**kwargs)  # type: ignore[arg-type]

    async def record_fill(self, **kwargs: object) -> None:
        if self.method == "record_fill":
            raise RuntimeError("journal fill")
        await super().record_fill(**kwargs)  # type: ignore[arg-type]

    async def record_exit(self, **kwargs: object) -> None:
        if self.method == "record_exit":
            raise RuntimeError("journal exit")
        await super().record_exit(**kwargs)  # type: ignore[arg-type]


def _assert_fail_closed(runner: PaperRunner, journal: MemoryJournal) -> None:
    assert entries_halted()
    assert runner.engine.position_quantity == 0 or is_killed(Path("paper-kill"))
    assert journal.run_ends
    assert journal.run_ends[-1]["kill_engaged"] is True


def test_journal_order_fill_and_exit_failures_still_flatten_or_kill() -> None:
    async def scenario(method: str, ticks: list[NormalizedQuote]) -> None:
        from scalper.execution import halt as halt_module

        halt_module._halted = False
        halt_module._reason = ""
        kill = Path("paper-kill")
        if kill.exists():
            kill.unlink()
        runner, _journal, clock = _runner(strategy=_Script([_good_signal()]))
        boom = _BoomJournal(method)
        runner.journal = boom

        async def source():
            for tick in ticks:
                clock["now"] = tick.received_at
                yield tick
            yield Stop()

        with pytest.raises(RuntimeError, match="journal"):
            await runner.run(source())
        _assert_fail_closed(runner, boom)

    ticks = [
        _quote(DAY, "100", 10),
        _quote(DAY + timedelta(minutes=1), "100.05", 20),
        _quote(DAY + timedelta(minutes=2), "99.90", 30),
        _quote(DAY + timedelta(minutes=3), "99", 40),
        _quote(DAY + timedelta(minutes=4), "99.10", 50),
    ]
    asyncio.run(scenario("record_order", ticks[:2]))
    asyncio.run(scenario("record_fill", ticks[:3]))
    asyncio.run(scenario("record_exit", ticks))


def test_an_engine_error_mid_loop_is_fail_closed() -> None:
    async def scenario() -> None:
        runner, journal, clock = _runner(strategy=_Script([_good_signal()]))

        async def explode(**_kwargs: object):
            raise RuntimeError("engine down")

        runner.engine.submit_entry = explode  # type: ignore[method-assign]

        async def source():
            for tick in (_quote(DAY, "100", 10), _quote(DAY + timedelta(minutes=1), "100", 20)):
                clock["now"] = tick.received_at
                yield tick

        with pytest.raises(RuntimeError, match="engine down"):
            await runner.run(source())
        _assert_fail_closed(runner, journal)

    asyncio.run(scenario())


def test_a_feed_error_is_fail_closed() -> None:
    async def scenario() -> None:
        runner, journal, clock = _runner(strategy=_Script([]))

        async def source():
            tick = _quote(DAY, "100", 10)
            clock["now"] = tick.received_at
            yield tick
            raise RuntimeError("feed down")

        with pytest.raises(RuntimeError, match="feed down"):
            await runner.run(source())
        _assert_fail_closed(runner, journal)

    asyncio.run(scenario())


def test_cancelling_the_runner_is_fail_closed() -> None:
    async def scenario() -> None:
        runner, journal, clock = _runner(strategy=_Script([]))
        started = asyncio.Event()

        async def source():
            tick = _quote(DAY, "100", 10)
            clock["now"] = tick.received_at
            yield tick
            started.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(runner.run(source()))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        _assert_fail_closed(runner, journal)

    asyncio.run(scenario())
