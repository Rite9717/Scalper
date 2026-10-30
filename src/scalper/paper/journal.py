"""Persist a paper run. MemoryJournal is the test double for PostgresJournal."""

from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from typing import Protocol
from uuid import UUID

import asyncpg


def _text_price(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, "f")


def _num(value: object) -> str | None:
    if value is None:
        return None
    return format(Decimal(str(value)), "f")


class PaperJournal(Protocol):
    async def start_run(
        self,
        *,
        run_id: UUID,
        started_at: datetime,
        strategy: str,
        tradingsymbol: str,
        limits: dict[str, str],
    ) -> None: ...

    async def record_signal(
        self,
        *,
        run_id: UUID,
        at: datetime,
        side: str | None,
        limit_price: Decimal | None,
        stop_price: Decimal | None,
        target_price: Decimal | None,
        time_stop_bars: int | None,
        regime: str | None,
        accepted: bool,
    ) -> None: ...

    async def record_decision(
        self,
        *,
        run_id: UUID,
        at: datetime,
        intent: str,
        allowed: bool,
        action: str,
        reason: str,
    ) -> None: ...

    async def record_order(
        self,
        *,
        run_id: UUID,
        at: datetime,
        tag: str,
        broker_order_id: str | None,
        side: str,
        limit_price: Decimal,
        quantity: int,
        intent: str,
        status: str,
    ) -> None: ...

    async def record_fill(
        self,
        *,
        run_id: UUID,
        at: datetime,
        tag: str,
        price: Decimal,
        quantity: int,
    ) -> None: ...

    async def record_exit(
        self,
        *,
        run_id: UUID,
        at: datetime,
        reason: str,
        trigger_at: datetime | None,
        trigger_price: Decimal | None,
        fill_at: datetime | None,
        fill_price: Decimal | None,
        quantity: int | None,
    ) -> None: ...

    async def record_run_end(
        self,
        *,
        run_id: UUID,
        ended_at: datetime,
        reason: str,
        final_position: int,
        kill_engaged: bool,
    ) -> None: ...

    async def read(self, run_id: UUID) -> dict[str, list[dict[str, object]]]: ...


class MemoryJournal:
    def __init__(self) -> None:
        self.runs: list[dict[str, object]] = []
        self.signals: list[dict[str, object]] = []
        self.decisions: list[dict[str, object]] = []
        self.orders: list[dict[str, object]] = []
        self.fills: list[dict[str, object]] = []
        self.exits: list[dict[str, object]] = []
        self.run_ends: list[dict[str, object]] = []

    async def start_run(
        self,
        *,
        run_id: UUID,
        started_at: datetime,
        strategy: str,
        tradingsymbol: str,
        limits: dict[str, str],
    ) -> None:
        self.runs.append(
            {
                "run_id": str(run_id),
                "started_at": started_at,
                "strategy": strategy,
                "tradingsymbol": tradingsymbol,
                "limits": dict(limits),
            }
        )

    async def record_signal(
        self,
        *,
        run_id: UUID,
        at: datetime,
        side: str | None,
        limit_price: Decimal | None,
        stop_price: Decimal | None,
        target_price: Decimal | None,
        time_stop_bars: int | None,
        regime: str | None,
        accepted: bool,
    ) -> None:
        self.signals.append(
            {
                "run_id": str(run_id),
                "at": at,
                "side": side,
                "limit_price": _text_price(limit_price),
                "stop_price": _text_price(stop_price),
                "target_price": _text_price(target_price),
                "time_stop_bars": time_stop_bars,
                "regime": regime,
                "accepted": accepted,
            }
        )

    async def record_decision(
        self,
        *,
        run_id: UUID,
        at: datetime,
        intent: str,
        allowed: bool,
        action: str,
        reason: str,
    ) -> None:
        self.decisions.append(
            {
                "run_id": str(run_id),
                "at": at,
                "intent": intent,
                "allowed": allowed,
                "action": action,
                "reason": reason,
            }
        )

    async def record_order(
        self,
        *,
        run_id: UUID,
        at: datetime,
        tag: str,
        broker_order_id: str | None,
        side: str,
        limit_price: Decimal,
        quantity: int,
        intent: str,
        status: str,
    ) -> None:
        self.orders.append(
            {
                "run_id": str(run_id),
                "at": at,
                "tag": tag,
                "broker_order_id": broker_order_id,
                "side": side,
                "limit_price": _text_price(limit_price),
                "quantity": quantity,
                "intent": intent,
                "status": status,
            }
        )

    async def record_fill(
        self,
        *,
        run_id: UUID,
        at: datetime,
        tag: str,
        price: Decimal,
        quantity: int,
    ) -> None:
        self.fills.append(
            {
                "run_id": str(run_id),
                "at": at,
                "tag": tag,
                "price": _text_price(price),
                "quantity": quantity,
            }
        )

    async def record_exit(
        self,
        *,
        run_id: UUID,
        at: datetime,
        reason: str,
        trigger_at: datetime | None,
        trigger_price: Decimal | None,
        fill_at: datetime | None,
        fill_price: Decimal | None,
        quantity: int | None,
    ) -> None:
        self.exits.append(
            {
                "run_id": str(run_id),
                "at": at,
                "reason": reason,
                "trigger_at": trigger_at,
                "trigger_price": _text_price(trigger_price),
                "fill_at": fill_at,
                "fill_price": _text_price(fill_price),
                "quantity": quantity,
            }
        )

    async def record_run_end(
        self,
        *,
        run_id: UUID,
        ended_at: datetime,
        reason: str,
        final_position: int,
        kill_engaged: bool,
    ) -> None:
        self.run_ends.append(
            {
                "run_id": str(run_id),
                "ended_at": ended_at,
                "reason": reason,
                "final_position": final_position,
                "kill_engaged": kill_engaged,
            }
        )

    async def read(self, run_id: UUID) -> dict[str, list[dict[str, object]]]:
        key = str(run_id)
        return {
            "runs": [row for row in self.runs if row["run_id"] == key],
            "signals": [row for row in self.signals if row["run_id"] == key],
            "decisions": [row for row in self.decisions if row["run_id"] == key],
            "orders": [row for row in self.orders if row["run_id"] == key],
            "fills": [row for row in self.fills if row["run_id"] == key],
            "exits": [row for row in self.exits if row["run_id"] == key],
            "run_ends": [row for row in self.run_ends if row["run_id"] == key],
        }


class PostgresJournal:
    def __init__(self, connection: asyncpg.Connection) -> None:
        self._connection = connection

    async def start_run(
        self,
        *,
        run_id: UUID,
        started_at: datetime,
        strategy: str,
        tradingsymbol: str,
        limits: dict[str, str],
    ) -> None:
        await self._connection.execute(
            """
            INSERT INTO paper_runs (run_id, started_at, strategy, tradingsymbol, limits)
            VALUES ($1, $2, $3, $4, $5::jsonb)
            """,
            run_id,
            started_at,
            strategy,
            tradingsymbol,
            json.dumps(limits),
        )

    async def record_signal(
        self,
        *,
        run_id: UUID,
        at: datetime,
        side: str | None,
        limit_price: Decimal | None,
        stop_price: Decimal | None,
        target_price: Decimal | None,
        time_stop_bars: int | None,
        regime: str | None,
        accepted: bool,
    ) -> None:
        await self._connection.execute(
            """
            INSERT INTO paper_signals (
                run_id, at, side, limit_price, stop_price, target_price,
                time_stop_bars, regime, accepted
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            run_id,
            at,
            side,
            _text_price(limit_price),
            _text_price(stop_price),
            _text_price(target_price),
            time_stop_bars,
            regime,
            accepted,
        )

    async def record_decision(
        self,
        *,
        run_id: UUID,
        at: datetime,
        intent: str,
        allowed: bool,
        action: str,
        reason: str,
    ) -> None:
        await self._connection.execute(
            """
            INSERT INTO paper_decisions (run_id, at, intent, allowed, action, reason)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            run_id,
            at,
            intent,
            allowed,
            action,
            reason,
        )

    async def record_order(
        self,
        *,
        run_id: UUID,
        at: datetime,
        tag: str,
        broker_order_id: str | None,
        side: str,
        limit_price: Decimal,
        quantity: int,
        intent: str,
        status: str,
    ) -> None:
        await self._connection.execute(
            """
            INSERT INTO paper_orders (
                run_id, at, tag, broker_order_id, side, limit_price, quantity, intent, status
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            run_id,
            at,
            tag,
            broker_order_id,
            side,
            _text_price(limit_price),
            quantity,
            intent,
            status,
        )

    async def record_fill(
        self,
        *,
        run_id: UUID,
        at: datetime,
        tag: str,
        price: Decimal,
        quantity: int,
    ) -> None:
        await self._connection.execute(
            """
            INSERT INTO paper_fills (run_id, at, tag, price, quantity)
            VALUES ($1, $2, $3, $4, $5)
            """,
            run_id,
            at,
            tag,
            _text_price(price),
            quantity,
        )

    async def record_exit(
        self,
        *,
        run_id: UUID,
        at: datetime,
        reason: str,
        trigger_at: datetime | None,
        trigger_price: Decimal | None,
        fill_at: datetime | None,
        fill_price: Decimal | None,
        quantity: int | None,
    ) -> None:
        await self._connection.execute(
            """
            INSERT INTO paper_exits (
                run_id, at, reason, trigger_at, trigger_price, fill_at, fill_price, quantity
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            run_id,
            at,
            reason,
            trigger_at,
            _text_price(trigger_price),
            fill_at,
            _text_price(fill_price),
            quantity,
        )

    async def record_run_end(
        self,
        *,
        run_id: UUID,
        ended_at: datetime,
        reason: str,
        final_position: int,
        kill_engaged: bool,
    ) -> None:
        await self._connection.execute(
            """
            INSERT INTO paper_run_ends (run_id, ended_at, reason, final_position, kill_engaged)
            VALUES ($1, $2, $3, $4, $5)
            """,
            run_id,
            ended_at,
            reason,
            final_position,
            kill_engaged,
        )

    async def read(self, run_id: UUID) -> dict[str, list[dict[str, object]]]:
        run_row = await self._connection.fetchrow("SELECT * FROM paper_runs WHERE run_id = $1", run_id)
        runs = []
        if run_row is not None:
            limits = run_row["limits"]
            if isinstance(limits, str):
                limits = json.loads(limits)
            runs.append(
                {
                    "run_id": str(run_row["run_id"]),
                    "started_at": run_row["started_at"],
                    "strategy": run_row["strategy"],
                    "tradingsymbol": run_row["tradingsymbol"],
                    "limits": limits,
                }
            )
        return {
            "runs": runs,
            "signals": await self._rows("paper_signals", run_id),
            "decisions": await self._rows("paper_decisions", run_id),
            "orders": await self._rows("paper_orders", run_id),
            "fills": await self._rows("paper_fills", run_id),
            "exits": await self._rows("paper_exits", run_id),
            "run_ends": await self._run_ends(run_id),
        }

    async def _run_ends(self, run_id: UUID) -> list[dict[str, object]]:
        fetched = await self._connection.fetch("SELECT * FROM paper_run_ends WHERE run_id = $1", run_id)
        return [
            {
                "run_id": str(row["run_id"]),
                "ended_at": row["ended_at"],
                "reason": row["reason"],
                "final_position": row["final_position"],
                "kill_engaged": row["kill_engaged"],
            }
            for row in fetched
        ]

    async def _rows(self, table: str, run_id: UUID) -> list[dict[str, object]]:
        fetched = await self._connection.fetch(f"SELECT * FROM {table} WHERE run_id = $1 ORDER BY id", run_id)
        rows: list[dict[str, object]] = []
        for record in fetched:
            row = {key: record[key] for key in record.keys() if key != "id"}
            row["run_id"] = str(row["run_id"])
            for price_key in ("limit_price", "stop_price", "target_price", "price", "trigger_price", "fill_price"):
                if price_key in row:
                    row[price_key] = _num(row[price_key])
            rows.append(row)
        return rows
