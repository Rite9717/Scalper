"""Say which manual steps are still left before paper trading can start."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import asyncpg
import redis.asyncio as redis_async

from scalper.clock import now_ist
from scalper.compliance import PersonalAccountError
from scalper.config import Settings
from scalper.kite.session import assess_session, load_session, require_usable_session
from scalper.ops.go_live import ready_for_live
from scalper.paper.command import instrument_master_path
from scalper.risk.killfile import is_killed, kill_path

PAPER_GATES = ("credentials", "database", "session", "instrument_master", "kill_switch")


@dataclass(frozen=True)
class Status:
    name: str
    ready: bool
    action: str


def credentials_status(settings: Settings) -> Status:
    missing = []
    if not settings.expected_user_id:
        missing.append("SCALPER_EXPECTED_USER_ID")
    if not settings.kite_api_key.get_secret_value():
        missing.append("SCALPER_KITE_API_KEY")
    if not settings.kite_api_secret.get_secret_value():
        missing.append("SCALPER_KITE_API_SECRET")
    if missing:
        return Status("credentials", False, "Set " + ", ".join(missing) + " in .env")
    return Status("credentials", True, "")


def session_status(settings: Settings, now: datetime) -> Status:
    try:
        session = load_session(settings.session_path)
    except PersonalAccountError as exc:
        return Status("session", False, f"{exc}. Run: python -m scalper login")
    health = assess_session(
        session,
        now=now,
        expected_user_id=settings.expected_user_id,
        api_key=settings.kite_api_key.get_secret_value(),
    )
    if health.in_maintenance_window:
        return Status("session", False, "Wait until 06:00 IST, then run: python -m scalper login")
    try:
        require_usable_session(health)
    except PersonalAccountError as exc:
        return Status("session", False, f"{exc}. Run: python -m scalper login")
    return Status("session", True, "")


def instrument_status(settings: Settings, day: date) -> Status:
    path = instrument_master_path(settings, day)
    if not path.is_file():
        return Status(
            "instrument_master",
            False,
            "Run: python -m scalper instruments fetch",
        )
    return Status("instrument_master", True, "")


def kill_status(settings: Settings) -> Status:
    path = kill_path(settings.data_dir)
    if is_killed(path):
        return Status(
            "kill_switch",
            False,
            "The kill file is on. Resume only when you mean to: "
            "python -m scalper resume --confirm I_RESUME_TRADING",
        )
    return Status("kill_switch", True, "")


async def database_status(settings: Settings) -> Status:
    try:
        connection = await asyncpg.connect(settings.database_url.get_secret_value(), timeout=3)
        try:
            await connection.execute("SELECT 1")
        finally:
            await connection.close()
    except Exception as exc:
        return Status("database", False, f"Start Postgres with: docker compose up -d ({type(exc).__name__})")
    return Status("database", True, "")


async def redis_status(settings: Settings) -> Status:
    client = redis_async.from_url(
        settings.redis_url.get_secret_value(),
        socket_connect_timeout=3,
        socket_timeout=3,
    )
    try:
        await client.ping()
    except Exception as exc:
        return Status(
            "redis",
            False,
            f"Start Redis before scalper record: docker compose up -d ({type(exc).__name__})",
        )
    finally:
        await client.aclose()
    return Status("redis", True, "")


async def collect_status(settings: Settings, *, now: datetime | None = None, day: date | None = None) -> list[Status]:
    moment = now or now_ist()
    session_day = day or moment.date()
    return [
        credentials_status(settings),
        await database_status(settings),
        await redis_status(settings),
        session_status(settings, moment),
        instrument_status(settings, session_day),
        kill_status(settings),
    ]


def paper_ready(checks: list[Status]) -> bool:
    ready = {item.name: item.ready for item in checks}
    return all(ready.get(name) is True for name in PAPER_GATES)


def render_doctor(checks: list[Status]) -> str:
    lines = [f"paper ready: {'yes' if paper_ready(checks) else 'no'}", f"ready_for_live={str(ready_for_live()).lower()}"]
    for item in checks:
        if item.ready:
            lines.append(f"- {item.name}: ready")
        else:
            lines.append(f"- {item.name}: not ready. {item.action}")
    lines.extend(
        [
            "",
            "Still done by you, not by this program:",
            "- Fill .env from .env.example with your Zerodha user id, API key, and API secret.",
            "- Log in yourself in the browser. The access token expires at 06:00 IST.",
            "- Download history, then backtest it: python -m scalper backtest --stored --csv <nfo.csv> --from YYYY-MM-DD --to YYYY-MM-DD --strategy trend_pullback",
            "- On a trading day: python -m scalper paper --strategy trend_pullback",
            "- Live orders stay off until you have finished the scalper go-live checklist yourself.",
        ]
    )
    return "\n".join(lines) + "\n"
