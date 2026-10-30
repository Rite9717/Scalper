from __future__ import annotations

import os
from datetime import datetime
from decimal import Decimal
from importlib.resources import files
from uuid import uuid4

import asyncpg
import pytest

from scalper.clock import IST
from scalper.data.migrate import split_sql
from scalper.paper.journal import MemoryJournal, PostgresJournal

WHEN = datetime(2026, 10, 27, 10, 15, tzinfo=IST)


async def exercise(journal: MemoryJournal | PostgresJournal, run_id) -> None:
    limits = {"max_loss_per_trade": "1234", "stale_after_seconds": "5.0"}
    await journal.start_run(
        run_id=run_id,
        started_at=WHEN,
        strategy="trend_pullback",
        tradingsymbol="NIFTY26OCTFUT",
        limits=limits,
    )
    await journal.record_signal(
        run_id=run_id,
        at=WHEN,
        side="buy",
        limit_price=Decimal("100"),
        stop_price=Decimal("99"),
        target_price=Decimal("102"),
        time_stop_bars=3,
        regime="trend",
        accepted=True,
    )
    await journal.record_decision(
        run_id=run_id,
        at=WHEN,
        intent="entry",
        allowed=False,
        action="veto",
        reason="feed stall",
    )
    await journal.record_order(
        run_id=run_id,
        at=WHEN,
        tag="tag0001",
        broker_order_id="paper-1",
        side="buy",
        limit_price=Decimal("100"),
        quantity=15,
        intent="entry",
        status="OPEN",
    )
    await journal.record_fill(
        run_id=run_id,
        at=WHEN,
        tag="tag0001",
        price=Decimal("99.95"),
        quantity=15,
    )
    await journal.record_exit(
        run_id=run_id,
        at=WHEN,
        reason="stop",
        trigger_at=WHEN,
        trigger_price=Decimal("99"),
        fill_at=WHEN,
        fill_price=Decimal("98.90"),
        quantity=15,
    )
    view = await journal.read(run_id)
    assert view["runs"][0]["limits"]["max_loss_per_trade"] == "1234"
    assert view["runs"][0]["strategy"] == "trend_pullback"
    assert view["signals"][0]["side"] == "buy"
    assert view["signals"][0]["limit_price"] == "100"
    assert view["signals"][0]["accepted"] is True
    assert view["decisions"][0]["reason"] == "feed stall"
    assert view["decisions"][0]["allowed"] is False
    assert view["orders"][0]["tag"] == "tag0001"
    assert view["orders"][0]["status"] == "OPEN"
    assert view["fills"][0]["price"] == "99.95"
    assert view["fills"][0]["quantity"] == 15
    assert view["exits"][0]["reason"] == "stop"
    assert view["exits"][0]["trigger_price"] == "99"
    assert view["exits"][0]["fill_price"] == "98.90"


def test_memory_journal_records_the_paper_loop() -> None:
    import asyncio

    asyncio.run(exercise(MemoryJournal(), uuid4()))


def test_paper_journal_migration_on_postgres() -> None:
    import asyncio

    dsn = os.environ.get(
        "SCALPER_TEST_DATABASE_URL",
        "postgresql://scalper:scalper@127.0.0.1:5432/scalper",
    )

    async def scenario() -> None:
        try:
            connection = await asyncpg.connect(dsn, timeout=2)
        except Exception as exc:
            pytest.skip(f"postgres is not available: {type(exc).__name__}")
        run_id = uuid4()
        try:
            script = files("scalper.data.sql").joinpath("003_paper_journal.sql").read_text(encoding="utf-8")
            for statement in split_sql(script):
                await connection.execute(statement)
            await exercise(PostgresJournal(connection), run_id)
        finally:
            await connection.execute("DELETE FROM paper_exits WHERE run_id = $1", run_id)
            await connection.execute("DELETE FROM paper_fills WHERE run_id = $1", run_id)
            await connection.execute("DELETE FROM paper_orders WHERE run_id = $1", run_id)
            await connection.execute("DELETE FROM paper_decisions WHERE run_id = $1", run_id)
            await connection.execute("DELETE FROM paper_signals WHERE run_id = $1", run_id)
            await connection.execute("DELETE FROM paper_runs WHERE run_id = $1", run_id)
            await connection.close()

    asyncio.run(scenario())
