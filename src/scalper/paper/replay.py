"""Replay recorded ticks through the same paper loop as a live session.

This module does not construct a Kite client, a KiteBroker, or a Kite session.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg

from scalper.clock import IST, ensure_ist
from scalper.config import Settings
from scalper.data.models import DepthLevel, NormalizedQuote
from scalper.kite.instruments import NiftyFutureContract
from scalper.monitor.alerts import Monitor, RecordingSink
from scalper.monitor.metrics import build_metrics
from scalper.paper.journal import PaperJournal, PostgresJournal
from scalper.paper.limits import PaperLimits
from scalper.paper.runner import Stop
from scalper.paper.session import build_paper_runner
from scalper.risk.killfile import kill_path
from scalper.strategy.registry import build_strategy
from scalper.backtest.strategy import Strategy


class NoRecordedTicks(Exception):
    """The requested day has no stored ticks."""


def no_ticks_message(day: date) -> str:
    return f"no recorded ticks for {day.isoformat()}"


def _day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=IST)
    return start, start + timedelta(days=1)


def _decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


async def fetch_recorded_day(
    connection: asyncpg.Connection,
    day: date,
) -> tuple[NiftyFutureContract, list[NormalizedQuote]] | None:
    """Load one day's ticks and the contract they were recorded against."""
    start, end = _day_bounds(day)
    token_row = await connection.fetchrow(
        """
        SELECT instrument_token
        FROM ticks
        WHERE received_at >= $1 AND received_at < $2
        GROUP BY instrument_token
        ORDER BY count(*) DESC
        LIMIT 1
        """,
        start,
        end,
    )
    if token_row is None:
        return None
    token = int(token_row["instrument_token"])
    contract_row = await connection.fetchrow(
        """
        SELECT instrument_token, tradingsymbol, expiry, tick_size, lot_size, exchange
        FROM recorded_contracts
        WHERE instrument_token = $1
        ORDER BY first_seen_at DESC
        LIMIT 1
        """,
        token,
    )
    if contract_row is None:
        raise NoRecordedTicks(f"recorded ticks for {day.isoformat()} have no contract row")
    tick_rows = await connection.fetch(
        """
        SELECT received_at, instrument_token, last_price, last_quantity, volume,
               average_price, oi, open, high, low, close, exchange_ts
        FROM ticks
        WHERE instrument_token = $1 AND received_at >= $2 AND received_at < $3
        ORDER BY received_at
        """,
        token,
        start,
        end,
    )
    depth_rows = await connection.fetch(
        """
        SELECT received_at, side, level, price, quantity, orders
        FROM depth_levels
        WHERE instrument_token = $1 AND received_at >= $2 AND received_at < $3
        ORDER BY received_at, side, level
        """,
        token,
        start,
        end,
    )
    depth_by_time: dict[datetime, list[DepthLevel]] = {}
    for row in depth_rows:
        moment = ensure_ist(row["received_at"])
        depth_by_time.setdefault(moment, []).append(
            DepthLevel(
                side=str(row["side"]),
                level=int(row["level"]),
                price=_decimal(row["price"]),
                quantity=int(row["quantity"]),
                orders=int(row["orders"]),
            )
        )
    quotes = [
        NormalizedQuote(
            instrument_token=token,
            received_at=ensure_ist(row["received_at"]),
            exchange_ts=None if row["exchange_ts"] is None else ensure_ist(row["exchange_ts"]),
            last_price=_decimal(row["last_price"]),
            last_quantity=int(row["last_quantity"]),
            volume=int(row["volume"]),
            average_price=_decimal(row["average_price"]),
            oi=None if row["oi"] is None else int(row["oi"]),
            open=_decimal(row["open"]),
            high=_decimal(row["high"]),
            low=_decimal(row["low"]),
            close=_decimal(row["close"]),
            depth=tuple(depth_by_time.get(ensure_ist(row["received_at"]), ())),
        )
        for row in tick_rows
    ]
    if not quotes:
        return None
    contract = NiftyFutureContract(
        instrument_token=token,
        exchange_token=0,
        tradingsymbol=str(contract_row["tradingsymbol"]),
        name="NIFTY",
        expiry=contract_row["expiry"],
        tick_size=_decimal(contract_row["tick_size"]),
        lot_size=int(contract_row["lot_size"]),
        exchange=str(contract_row["exchange"]),
        segment="NFO-FUT",
        instrument_type="FUT",
    )
    return contract, quotes


async def run_recorded_quotes(
    *,
    settings: Settings,
    strategy: Strategy,
    contract: NiftyFutureContract,
    quotes: Sequence[NormalizedQuote],
    journal: PaperJournal,
    day: date,
    run_id: UUID | None = None,
    kill_file: Path | None = None,
) -> UUID:
    """Drive the paper loop from ticks. The clock follows each tick's timestamp."""
    if not quotes:
        raise NoRecordedTicks(no_ticks_message(day))
    chosen = run_id or uuid4()
    clock = {"now": ensure_ist(quotes[0].received_at)}

    def now() -> datetime:
        return clock["now"]

    async def instant(_seconds: float) -> None:
        return None

    runner = build_paper_runner(
        settings=settings,
        strategy=strategy,
        contract=contract,
        journal=journal,
        run_id=chosen,
        monitor=Monitor(build_metrics(), RecordingSink()),
        kill_file=kill_file or kill_path(settings.data_dir),
        now=now,
        sleep=instant,
        pace=False,
        expiry_dates={contract.expiry},
        paper=PaperLimits(),
    )
    await runner.run(_paced(quotes, clock))
    return runner.run_id


async def _paced(quotes: Sequence[NormalizedQuote], clock: dict[str, datetime]) -> AsyncIterator[object]:
    for quote in quotes:
        clock["now"] = ensure_ist(quote.received_at)
        yield quote
    yield Stop()


async def replay_recorded_day(settings: Settings, strategy_name: str, day: date) -> UUID:
    """Load one day from Postgres and persist a new paper run. No Kite calls."""
    connection = await asyncpg.connect(settings.database_url.get_secret_value(), timeout=5)
    try:
        loaded = await fetch_recorded_day(connection, day)
        if loaded is None:
            raise NoRecordedTicks(no_ticks_message(day))
        contract, quotes = loaded
        return await run_recorded_quotes(
            settings=settings,
            strategy=build_strategy(strategy_name),
            contract=contract,
            quotes=quotes,
            journal=PostgresJournal(connection),
            day=day,
        )
    finally:
        await connection.close()
