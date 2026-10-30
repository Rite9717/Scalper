"""Fail-closed checks before the paper loop starts. A refusal leaves the KILL file alone."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime
from pathlib import Path

from scalper.compliance import PersonalAccountError
from scalper.kite.session import assess_session, load_session, require_usable_session
from scalper.risk.killfile import is_killed


class PaperStartupError(RuntimeError):
    """The paper loop must not start."""


async def refuse_to_start(
    *,
    kill_file: Path,
    session_path: Path,
    expected_user_id: str,
    api_key: str,
    now: datetime,
    instrument_master: Path,
    database_ping: Callable[[], Awaitable[None]],
) -> None:
    if is_killed(kill_file):
        raise PaperStartupError("kill file is engaged")
    try:
        session = load_session(session_path)
    except PersonalAccountError as exc:
        raise PaperStartupError(str(exc)) from exc
    health = assess_session(
        session,
        now=now,
        expected_user_id=expected_user_id,
        api_key=api_key,
    )
    if health.in_maintenance_window:
        raise PaperStartupError("kite session is inside the 00:00-06:00 maintenance window")
    try:
        require_usable_session(health)
    except PersonalAccountError as exc:
        raise PaperStartupError(str(exc)) from exc
    if not instrument_master.is_file():
        raise PaperStartupError("instrument master for today is missing")
    try:
        await database_ping()
    except Exception as exc:
        raise PaperStartupError("database is unreachable") from exc
