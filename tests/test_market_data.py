from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from scalper.cli import main
from scalper.clock import IST
from scalper.data.bridge import AsyncTickerBridge
from scalper.data.cache import MemoryQuoteCache, RedisQuoteCache
from scalper.data.candles import MinuteBook
from scalper.data.historical import (
    IntervalLimiter,
    fetch_historical_candles,
    iter_windows,
    parse_historical_candles,
)
from scalper.data.migrate import migration_scripts, split_sql
from scalper.data.normalize import QuoteRejected, normalize_full_tick
from scalper.data.prices import PriceError, rupees_to_tick
from scalper.data.recorder import consume_ticks, monotonic_received_at
from scalper.data.ticker import arm_full_mode
from scalper.data.writer import MemoryWriter

FIXTURE_CSV = Path(__file__).resolve().parent / "fixtures" / "nfo_sample.csv"

TOKEN = 12345001
TICK = Decimal("0.05")


def test_ticker_prices_are_quantized_and_not_divided() -> None:
    price = 2450005 / 100
    assert rupees_to_tick(price, TICK) == Decimal("24500.05")
    assert rupees_to_tick(price, TICK) != Decimal("245.0005")
    with pytest.raises(PriceError):
        rupees_to_tick(2450005, TICK)  # type: ignore[arg-type]


def _levels(base: float, side: str) -> list[dict[str, object]]:
    step = 0.05 if side == "sell" else -0.05
    return [
        {"price": base + step * (index + 1), "quantity": 65, "orders": index + 1}
        for index in range(5)
    ]


def _full_tick(price: float = 24500.05, volume: int = 1000, token: int = TOKEN) -> dict[str, object]:
    return {
        "mode": "full",
        "instrument_token": token,
        "last_price": price,
        "last_traded_quantity": 65,
        "average_traded_price": price,
        "volume_traded": volume,
        "ohlc": {"open": price, "high": price, "low": price, "close": price},
        "oi": 15,
        "exchange_timestamp": datetime(2026, 10, 25, 9, 15, tzinfo=IST),
        "depth": {"buy": _levels(price, "buy"), "sell": _levels(price, "sell")},
    }


def test_full_tick_keeps_rupees_and_five_levels() -> None:
    received = datetime(2026, 10, 25, 9, 15, 1, tzinfo=IST)
    quote = normalize_full_tick(_full_tick(), tick_size=TICK, instrument_token=TOKEN, received_at=received)
    assert quote.last_price == Decimal("24500.05")
    assert len(quote.depth) == 10
    assert {level.level for level in quote.depth if level.side == "buy"} == {1, 2, 3, 4, 5}
    assert quote.to_cache()["last_price"] == "24500.05"
    with pytest.raises(QuoteRejected):
        normalize_full_tick({**_full_tick(), "mode": "quote"}, tick_size=TICK, instrument_token=TOKEN, received_at=received)
    with pytest.raises(QuoteRejected):
        normalize_full_tick(_full_tick(token=99), tick_size=TICK, instrument_token=TOKEN, received_at=received)


def test_minute_book_uses_cumulative_volume_delta() -> None:
    book = MinuteBook(TOKEN)
    first = _quote_at(datetime(2026, 10, 25, 9, 15, 1, tzinfo=IST), 1000, 24500.05)
    second = _quote_at(datetime(2026, 10, 25, 9, 15, 20, tzinfo=IST), 1065, 24501.0)
    third = _quote_at(datetime(2026, 10, 25, 9, 16, 1, tzinfo=IST), 1100, 24499.5)
    assert book.add(first) is None
    assert book.add(second) is None
    rolled = book.add(third)
    assert rolled is not None
    assert rolled.volume == 65
    assert rolled.open == Decimal("24500.05")
    assert rolled.high == Decimal("24501.00") or rolled.high == Decimal("24501")
    assert rolled.close == Decimal("24501") or rolled.close == Decimal("24501.00")
    tail = book.flush()
    assert tail is not None
    assert tail.bucket_start == datetime(2026, 10, 25, 9, 16, tzinfo=IST)
    assert tail.volume == 35


def _quote_at(moment: datetime, volume: int, price: float):
    return normalize_full_tick(
        {**_full_tick(price=price, volume=volume), "received_at": moment},
        tick_size=TICK,
        instrument_token=TOKEN,
        received_at=moment,
    )


def test_recorder_stores_quotes_depth_and_a_candle() -> None:
    async def scenario():
        queue: asyncio.Queue = asyncio.Queue()
        moment = datetime(2026, 10, 25, 9, 15, 1, tzinfo=IST)
        first = _full_tick(volume=1000)
        second = _full_tick(volume=1065)
        first["received_at"] = moment
        second["received_at"] = moment + timedelta(seconds=10)
        other = _full_tick(token=99)
        other["received_at"] = moment + timedelta(seconds=11)
        queue.put_nowait(first)
        queue.put_nowait(second)
        queue.put_nowait(other)
        stop = asyncio.Event()
        stop.set()
        writer = MemoryWriter()
        cache = MemoryQuoteCache()
        stats = await consume_ticks(
            queue,
            instrument_token=TOKEN,
            tick_size=TICK,
            writer=writer,
            cache=cache,
            stop=stop,
        )
        return stats, writer, cache

    stats, writer, cache = asyncio.run(scenario())
    assert stats.stored == 2
    assert stats.rejected == 1
    assert stats.candles == 1
    assert writer.quotes[0].last_price == Decimal("24500.05")
    assert len(writer.quotes[0].depth) == 10
    assert writer.candles[0].volume == 65
    assert cache.values[TOKEN]["last_price"] == "24500.05"


def test_received_timestamps_stay_unique() -> None:
    moment = datetime(2026, 10, 25, 9, 15, tzinfo=IST)
    assert monotonic_received_at(None, moment) == moment
    assert monotonic_received_at(moment, moment) == moment + timedelta(microseconds=1)


def test_bridge_moves_ticks_from_another_thread_and_drops_overflow() -> None:
    async def scenario():
        loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue(maxsize=1)
        bridge = AsyncTickerBridge(loop, queue)
        thread = threading.Thread(target=lambda: bridge.on_ticks(None, [{"instrument_token": TOKEN}]))
        thread.start()
        thread.join()
        item = await asyncio.wait_for(queue.get(), timeout=1)
        assert item["instrument_token"] == TOKEN
        bridge.on_ticks(None, [{"n": 1}, {"n": 2}])
        await asyncio.sleep(0)
        assert queue.qsize() == 1
        assert bridge.dropped == 1

    asyncio.run(scenario())


def test_full_mode_subscription_is_one_token() -> None:
    class FakeSocket:
        MODE_FULL = "full"

        def __init__(self) -> None:
            self.subscribed = None
            self.mode = None

        def subscribe(self, instrument_tokens: list[int]) -> None:
            self.subscribed = instrument_tokens

        def set_mode(self, mode: str, instrument_tokens: list[int]) -> None:
            self.mode = (mode, instrument_tokens)

    socket = FakeSocket()
    arm_full_mode(socket, TOKEN)
    assert socket.subscribed == [TOKEN]
    assert socket.mode == ("full", [TOKEN])


def test_historical_windows_and_rupee_candles() -> None:
    start = datetime(2026, 1, 1, 9, 15, tzinfo=IST)
    end = datetime(2026, 4, 11, 15, 30, tzinfo=IST)
    windows = list(iter_windows(start, end, "minute"))
    assert len(windows) >= 2
    with pytest.raises(ValueError):
        list(iter_windows(start, end, "second"))
    payload = {
        "candles": [
            ["2026-10-25T09:15:00+0530", 24500.05, 24510.0, 24490.0, 24505.0, 1200, 15],
        ]
    }
    candles = parse_historical_candles(payload, instrument_token=TOKEN, interval="minute", tick_size=TICK)
    assert candles[0].close == Decimal("24505.00") or candles[0].close == Decimal("24505")
    assert candles[0].open == Decimal("24500.05")
    assert candles[0].source == "kite"
    assert candles[0].volume == 1200


def test_historical_fetch_is_rate_limited_and_uses_the_token_header() -> None:
    slept: list[float] = []
    clock = {"t": 0.0}

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)
        clock["t"] += seconds

    def now() -> float:
        return clock["t"]

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "token key:access"
        assert request.url.path == f"/instruments/historical/{TOKEN}/minute"
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {
                    "candles": [
                        ["2026-10-25T09:15:00+0530", 24500.05, 24510.0, 24490.0, 24500.05, 10, 1],
                    ]
                },
            },
        )

    async def scenario():
        limiter = IntervalLimiter(3, clock=now, sleep=fake_sleep)
        await limiter.acquire()
        await limiter.acquire()
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport, base_url="https://api.kite.trade") as client:
            candles = await fetch_historical_candles(
                client,
                api_key="key",
                access_token="access",
                instrument_token=TOKEN,
                interval="minute",
                start=datetime(2026, 10, 25, 9, 15, tzinfo=IST),
                end=datetime(2026, 10, 25, 9, 20, tzinfo=IST),
                tick_size=TICK,
                limiter=IntervalLimiter(1000, clock=now, sleep=fake_sleep),
            )
        return candles

    candles = asyncio.run(scenario())
    assert slept[0] == pytest.approx(1 / 3)
    assert candles[0].open == Decimal("24500.05")


def test_migration_script_creates_ticks_and_depth() -> None:
    scripts = dict(migration_scripts())
    sql = scripts["002_market_data.sql"]
    statements = split_sql(sql)
    assert any("CREATE TABLE IF NOT EXISTS ticks" in statement for statement in statements)
    assert any("depth_levels" in statement for statement in statements)
    assert any("create_hypertable" in statement for statement in statements)
    assert "65" not in sql
    assert "0.05" not in sql


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.ttl = None

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.values[key] = value
        self.ttl = ex

    async def get(self, key: str) -> str | None:
        return self.values.get(key)


def test_redis_cache_stores_decimal_strings() -> None:
    async def scenario():
        client = _FakeRedis()
        cache = RedisQuoteCache(client, ttl_seconds=60)
        quote = _quote_at(datetime(2026, 10, 25, 9, 15, tzinfo=IST), 1000, 24500.05)
        await cache.put(quote)
        loaded = await cache.get(TOKEN)
        return client, loaded

    client, loaded = asyncio.run(scenario())
    assert client.ttl == 60
    assert loaded is not None
    assert loaded["last_price"] == "24500.05"
    assert "245.0005" not in client.values[f"scalper:quote:{TOKEN}"]


def test_history_command_rejects_a_bad_timestamp() -> None:
    code = main(
        [
            "history",
            "--csv",
            str(FIXTURE_CSV),
            "--from",
            "not-a-date",
            "--to",
            "2026-10-25",
            "--as-of",
            "2026-10-25",
        ]
    )
    assert code == 2
