"""Compare one paper run with a backtest of the same recorded ticks."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

import asyncpg

from scalper.backtest.costs import ChargeSchedule, round_trip_charges
from scalper.backtest.engine import BacktestSpec, Trade, run_backtest
from scalper.clock import ensure_ist
from scalper.config import Settings
from scalper.data.candles import MinuteBook
from scalper.data.models import Candle, NormalizedQuote
from scalper.paper.bars import SessionMinuteBook
from scalper.paper.journal import PostgresJournal
from scalper.paper.replay import NoRecordedTicks, fetch_recorded_day, no_ticks_message
from scalper.paper.session import charges_from_settings
from scalper.strategy.registry import build_strategy


class MissingPaperRun(Exception):
    """The journal has no run with this id."""


@dataclass(frozen=True)
class ObservedTrade:
    side: str
    entry_time: datetime
    exit_time: datetime
    entry_price: Decimal
    exit_price: Decimal
    quantity: int
    net_pnl: Decimal


@dataclass(frozen=True)
class TradeDifference:
    index: str
    field: str
    paper: str
    backtest: str
    causes: tuple[str, ...]


@dataclass(frozen=True)
class Comparison:
    differences: tuple[TradeDifference, ...]
    cause_counts: dict[str, int]
    paper_net: Decimal
    backtest_net: Decimal


def candles_from_quotes(quotes: list[NormalizedQuote], instrument_token: int) -> list[Candle]:
    book = SessionMinuteBook(MinuteBook(instrument_token))
    bars: list[Candle] = []
    for quote in quotes:
        finished = book.add(quote)
        if finished is not None:
            bars.append(finished)
    last = book.flush()
    if last is not None:
        bars.append(last)
    return bars


def _money(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _moment(value: object) -> datetime:
    if isinstance(value, datetime):
        return ensure_ist(value)
    return ensure_ist(datetime.fromisoformat(str(value)))


def trades_from_journal(
    view: dict[str, list[dict[str, object]]],
    *,
    charges: ChargeSchedule,
    tick_size: Decimal,
) -> list[ObservedTrade]:
    entry_side = {}
    for order in view["orders"]:
        if order.get("intent") == "entry" and order.get("tag") not in entry_side:
            entry_side[order["tag"]] = str(order["side"])
    entries = sorted(
        (row for row in view["fills"] if row.get("tag") in entry_side),
        key=lambda row: _moment(row["at"]),
    )
    exits = sorted(
        (row for row in view["exits"] if row.get("fill_price") not in {None, ""}),
        key=lambda row: _moment(row.get("fill_at") or row["at"]),
    )
    trades: list[ObservedTrade] = []
    for fill, closed in zip(entries, exits, strict=False):
        side = entry_side[str(fill["tag"])]
        entry_price = _money(fill["price"])
        exit_price = _money(closed["fill_price"])
        quantity = int(closed.get("quantity") or fill["quantity"])
        gross = (exit_price - entry_price) * quantity if side == "buy" else (entry_price - exit_price) * quantity
        cost = round_trip_charges(
            charges,
            entry_price=entry_price,
            exit_price=exit_price,
            quantity=quantity,
            entry_side=side,
            tick_size=tick_size,
        ).total
        trades.append(
            ObservedTrade(
                side=side,
                entry_time=_moment(fill["at"]),
                exit_time=_moment(closed.get("fill_at") or closed["at"]),
                entry_price=entry_price,
                exit_price=exit_price,
                quantity=quantity,
                net_pnl=gross - cost,
            )
        )
    return trades


def _from_backtest(trade: Trade) -> ObservedTrade:
    return ObservedTrade(
        side=trade.side,
        entry_time=ensure_ist(trade.entry_time),
        exit_time=ensure_ist(trade.exit_time),
        entry_price=trade.entry_price,
        exit_price=trade.exit_price,
        quantity=trade.quantity,
        net_pnl=trade.net_pnl,
    )


def _price_cause(diff: Decimal, slip: Decimal) -> str | None:
    if diff == 0:
        return None
    if slip > 0 and abs(diff) in {slip, slip * 2}:
        return "slippage"
    return "tick-level versus candle-level fills"


def compare_trades(
    paper: list[ObservedTrade],
    backtest: list[Trade],
    *,
    tick_size: Decimal,
    slippage_ticks: int,
) -> Comparison:
    other = [_from_backtest(trade) for trade in backtest]
    slip = tick_size * slippage_ticks
    diffs: list[TradeDifference] = []
    paired = min(len(paper), len(other))
    for index in range(paired):
        left = paper[index]
        right = other[index]
        causes: list[str] = []
        if left.entry_time != right.entry_time or left.exit_time != right.exit_time:
            causes.append("latency")
        for field, first, second in (
            ("entry_price", left.entry_price, right.entry_price),
            ("exit_price", left.exit_price, right.exit_price),
        ):
            cause = _price_cause(first - second, slip)
            if cause is not None and cause not in causes:
                causes.append(cause)
        if not causes and left.net_pnl != right.net_pnl:
            causes.append("tick-level versus candle-level fills")
        if not causes:
            continue
        labeled = tuple(causes)
        if left.entry_time != right.entry_time:
            diffs.append(_diff(index, "entry_time", str(left.entry_time), str(right.entry_time), labeled))
        if left.exit_time != right.exit_time:
            diffs.append(_diff(index, "exit_time", str(left.exit_time), str(right.exit_time), labeled))
        if left.entry_price != right.entry_price:
            diffs.append(
                _diff(index, "entry_price", format(left.entry_price, "f"), format(right.entry_price, "f"), labeled)
            )
        if left.exit_price != right.exit_price:
            diffs.append(
                _diff(index, "exit_price", format(left.exit_price, "f"), format(right.exit_price, "f"), labeled)
            )
        if left.net_pnl != right.net_pnl:
            diffs.append(_diff(index, "net_pnl", format(left.net_pnl, "f"), format(right.net_pnl, "f"), labeled))
    for index, trade in enumerate(paper[paired:], start=paired):
        diffs.append(
            _diff(index, "trade", _describe(trade), "", ("present in only one",))
        )
    for index, trade in enumerate(other[paired:], start=paired):
        diffs.append(
            _diff(index, "trade", "", _describe(trade), ("present in only one",))
        )
    counts: Counter[str] = Counter()
    seen: set[tuple[str, str]] = set()
    for diff in diffs:
        for cause in diff.causes:
            key = (diff.index, cause)
            if key in seen:
                continue
            seen.add(key)
            counts[cause] += 1
    return Comparison(
        differences=tuple(diffs),
        cause_counts=dict(counts),
        paper_net=sum((trade.net_pnl for trade in paper), Decimal("0")),
        backtest_net=sum((trade.net_pnl for trade in other), Decimal("0")),
    )


def _diff(index: int, field: str, paper: str, backtest: str, causes: tuple[str, ...]) -> TradeDifference:
    return TradeDifference(index=str(index), field=field, paper=paper, backtest=backtest, causes=causes)


def _describe(trade: ObservedTrade) -> str:
    return (
        f"{trade.side} {trade.quantity} entry {trade.entry_time.isoformat()} {format(trade.entry_price, 'f')} "
        f"exit {trade.exit_time.isoformat()} {format(trade.exit_price, 'f')} net {format(trade.net_pnl, 'f')}"
    )


def render_comparison(report: Comparison) -> str:
    lines = [
        f"paper net {format(report.paper_net, 'f')}",
        f"backtest net {format(report.backtest_net, 'f')}",
        f"net difference {format(report.paper_net - report.backtest_net, 'f')}",
    ]
    if not report.differences:
        lines.append("no differences")
    for item in report.differences:
        cause = ", ".join(item.causes)
        lines.append(
            f"trade {item.index} {item.field}: paper {item.paper} backtest {item.backtest} cause {cause}"
        )
    lines.append("counts")
    if not report.cause_counts:
        lines.append("none: 0")
    for cause in sorted(report.cause_counts):
        lines.append(f"{cause}: {report.cause_counts[cause]}")
    return "\n".join(lines)


def spec_from_settings(settings: Settings, contract_tick: Decimal, contract_lot: int, expiry: date) -> BacktestSpec:
    return BacktestSpec(
        tick_size=contract_tick,
        lot_size=contract_lot,
        lots=settings.paper_max_lots,
        charges=charges_from_settings(settings),
        cost_multiple=settings.cost_multiple,
        slippage_ticks=settings.slippage_ticks,
        latency_bars=settings.latency_bars,
        flatten_time=datetime.strptime(settings.flatten_time, "%H:%M").time(),
        expiry_dates=frozenset({expiry}),
        max_loss_per_trade=settings.max_loss_per_trade,
        daily_loss_limit=settings.daily_loss_limit,
    )


async def compare_recorded_run(settings: Settings, run_id: str, day: date, strategy_name: str) -> str:
    connection = await asyncpg.connect(settings.database_url.get_secret_value(), timeout=5)
    try:
        view = await PostgresJournal(connection).read(UUID(run_id))
        if not view["runs"]:
            raise MissingPaperRun(f"no paper run {run_id}")
        loaded = await fetch_recorded_day(connection, day)
        if loaded is None:
            raise NoRecordedTicks(no_ticks_message(day))
        contract, quotes = loaded
        candles = candles_from_quotes(quotes, contract.instrument_token)
        result = run_backtest(
            candles,
            build_strategy(strategy_name),
            spec_from_settings(settings, contract.tick_size, contract.lot_size, contract.expiry),
        )
        paper = trades_from_journal(view, charges=charges_from_settings(settings), tick_size=contract.tick_size)
        report = compare_trades(paper, result.trades, tick_size=contract.tick_size, slippage_ticks=settings.slippage_ticks)
        return render_comparison(report)
    finally:
        await connection.close()


# Re-export for the CLI error path.
__all__ = [
    "MissingPaperRun",
    "compare_recorded_run",
    "compare_trades",
    "render_comparison",
    "spec_from_settings",
]
