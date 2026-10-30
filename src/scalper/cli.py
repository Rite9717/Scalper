"""Command line for the Phase 1 skeleton.

`login` listens on loopback for one manual Kite login, stores the session, and exits.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import asyncpg
import httpx
import structlog
from pydantic import ValidationError

from scalper import PHASE, __version__
from scalper.backtest.costs import ChargeSchedule
from scalper.backtest.engine import BacktestSpec, load_candles, run_backtest
from scalper.backtest.report import result_report, windows_covering
from scalper.ops.go_live import render_checklist
from scalper.risk.killfile import KillFileError, engage, kill_path, release
from scalper.execution.engine import UnmanagedPosition
from scalper.paper.command import run_paper
from scalper.paper.replay import NoRecordedTicks, replay_recorded_day
from scalper.paper.startup import PaperStartupError
from scalper.strategy.registry import STRATEGY_NAMES, build_strategy
from scalper.clock import IST, now_ist, today_ist
from scalper.compliance import ComplianceError, LIVE_CONFIRMATION_PHRASE, position_lot_cap
from scalper.data.migrate import apply_migrations
from scalper.data.service import DataStoreError, download_history, record_until_stopped
from scalper.config import Settings, load_settings
from scalper.health import HealthServer, http_health_payload
from scalper.kite.auth import exchange_request_token
from scalper.kite.callback import run_callback_login
from scalper.kite.http import KiteError, kite_client
from scalper.kite.instruments import (
    InstrumentMasterError,
    archive_instrument_master,
    fetch_instrument_csv,
    parse_instruments,
    select_current_nifty_future,
)
from scalper.kite.session import assess_session, load_session, require_usable_session, save_session
from scalper.logging import configure_logging, decision_logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="scalper", description="Personal Nifty futures scalper.")
    parser.add_argument("--version", action="version", version=f"scalper {__version__} phase {PHASE}")
    sub = parser.add_subparsers(dest="command", required=True)

    login = sub.add_parser("login", help="Catch one manual Kite login on a loopback callback, then exit.")
    login.add_argument("--no-browser", action="store_true", help="Print the login URL without opening a browser.")
    sub.add_parser("health", help="Print the redacted runtime configuration and exit.")

    serve = sub.add_parser("serve", help="Run the health server until SIGTERM.")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)

    instruments = sub.add_parser("instruments", help="Resolve or download the NFO instrument master.")
    instrument_commands = instruments.add_subparsers(dest="instruments_command", required=True)
    resolve = instrument_commands.add_parser("resolve", help="Resolve the current NIFTY future from a local CSV.")
    resolve.add_argument("--csv", required=True, type=Path)
    resolve.add_argument("--as-of", dest="as_of", default=None, help="YYYY-MM-DD in the exchange calendar.")
    fetch = instrument_commands.add_parser("fetch", help="Download today's NFO dump and resolve the contract.")
    fetch.add_argument("--as-of", dest="as_of", default=None)

    session = sub.add_parser("session", help="Exchange a request token. Does not obtain one.")
    session_commands = session.add_subparsers(dest="session_command", required=True)
    session_commands.add_parser(
        "exchange",
        help="Read SCALPER_REQUEST_TOKEN from the environment and store the access token.",
    )

    database = sub.add_parser("db", help="Apply the Timescale market-data migrations.")
    database_commands = database.add_subparsers(dest="db_command", required=True)
    database_commands.add_parser("migrate", help="Create tick, depth, and candle tables.")

    record = sub.add_parser("record", help="Record full-mode ticks and depth for one contract.")
    record.add_argument("--csv", required=True, type=Path)
    record.add_argument("--as-of", dest="as_of", default=None)

    history = sub.add_parser("history", help="Download Kite historical candles into Timescale.")
    history.add_argument("--csv", required=True, type=Path)
    history.add_argument("--from", dest="start", required=True)
    history.add_argument("--to", dest="end", required=True)
    history.add_argument("--interval", default="minute")
    history.add_argument("--as-of", dest="as_of", default=None)

    paper = sub.add_parser("paper", help="Run the paper loop on live Kite ticks. Orders are not sent.")
    paper.add_argument("--strategy", required=True, choices=("trend_pullback", "vwap_reversion"))
    paper.add_argument("--replay", action="store_true", help="Replay recorded ticks instead of the live websocket.")
    paper.add_argument("--date", default=None, help="YYYY-MM-DD session to replay. Required with --replay.")

    backtest = sub.add_parser("backtest", help="Replay a candle file. The plumbing strategy claims no edge.")
    backtest.add_argument("--candles", required=True, type=Path)
    backtest.add_argument("--tick", required=True, help="Contract tick size, for example 0.05.")
    backtest.add_argument("--lot", required=True, type=int, help="Contract lot size from the instrument master.")
    backtest.add_argument("--lots", type=int, default=1)
    backtest.add_argument("--window-days", type=int, default=20)
    backtest.add_argument("--strategy", default="plumbing", choices=STRATEGY_NAMES)

    kill = sub.add_parser("kill", help="Refuse every new entry until an explicit resume.")
    kill.add_argument("--reason", default="manual")
    resume = sub.add_parser("resume", help="Remove the kill file. Requires the exact confirmation phrase.")
    resume.add_argument("--confirm", default="")
    sub.add_parser("go-live", help="Print the blocking checklist. This command does not enable live trading.")
    return parser


def _parse_moment(raw: str) -> datetime:
    text = raw.strip()
    try:
        if len(text) == 10:
            parsed = datetime.strptime(text, "%Y-%m-%d")
        else:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    except ValueError as exc:
        raise ComplianceError("timestamps must be YYYY-MM-DD or YYYY-MM-DD HH:MM:SS") from exc
    return parsed.replace(tzinfo=IST)


def _safe_db_error(exc: BaseException) -> dict[str, str]:
    text = str(exc)
    if "://" in text or "password" in text.lower():
        text = type(exc).__name__
    return {"error_type": type(exc).__name__, "reason": text}


def _contract_from_csv(csv_path: Path, as_of: date):
    if not csv_path.is_file():
        raise InstrumentMasterError(f"instrument csv not found: {csv_path}")
    return select_current_nifty_future(parse_instruments(csv_path.read_text(encoding="utf-8")), as_of)


def _parse_as_of(raw: str | None) -> date:
    if raw is None:
        return today_ist()
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ComplianceError("as-of must be YYYY-MM-DD") from exc


def _format_validation_error(exc: ValidationError) -> str:
    lines = ["invalid configuration:"]
    for err in exc.errors(include_input=False, include_url=False):
        location = ".".join(str(part) for part in err.get("loc", ())) or "settings"
        lines.append(f"{location}: {err['msg']}")
    return "\n".join(lines)


def _print_json(payload: dict[str, object]) -> None:
    sys.stdout.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _resolve_local(csv_path: Path, as_of: date) -> int:
    if not csv_path.is_file():
        print(f"instrument csv not found: {csv_path}", file=sys.stderr)
        return 2
    contract = select_current_nifty_future(parse_instruments(csv_path.read_text(encoding="utf-8")), as_of)
    decision_logger().info("contract_resolved", source="csv", **contract.to_dict())
    _print_json(contract.to_dict())
    return 0


async def establish_session(settings: Settings, request_token: str, client: httpx.AsyncClient):
    session = await exchange_request_token(
        api_key=settings.kite_api_key.get_secret_value(),
        api_secret=settings.kite_api_secret.get_secret_value(),
        request_token=request_token,
        expected_user_id=settings.expected_user_id,
        client=client,
    )
    save_session(settings.session_path, session)
    return session


async def refresh_contract(settings: Settings, *, as_of: date, client: httpx.AsyncClient):
    session = load_session(settings.session_path)
    health = assess_session(
        session,
        now=now_ist(),
        expected_user_id=settings.expected_user_id,
        api_key=settings.kite_api_key.get_secret_value(),
    )
    require_usable_session(health)
    csv_text = await fetch_instrument_csv(
        api_key=session.api_key,
        access_token=session.access_token.get_secret_value(),
        client=client,
    )
    archived = archive_instrument_master(csv_text, settings.instrument_archive_dir, as_of)
    contract = select_current_nifty_future(parse_instruments(csv_text), as_of)
    return contract, archived, health


async def _serve(settings: Settings, host: str, port: int) -> None:
    server = HealthServer(settings, host=host, port=port)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _stop() -> None:
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            pass
    await server.start()
    structlog.get_logger().info(
        "health_server_started",
        host=host,
        port=server.port,
        trading_mode=settings.trading_mode,
    )
    await stop.wait()
    await server.stop()
    structlog.get_logger().info("health_server_stopped")


def _prepare(argv: list[str] | None) -> tuple[argparse.Namespace, Settings] | int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "instruments" and args.instruments_command == "resolve":
        configure_logging("INFO", json_output=False)
        try:
            return _resolve_local(args.csv, _parse_as_of(args.as_of))
        except (ComplianceError, InstrumentMasterError) as exc:
            print(str(exc), file=sys.stderr)
            return 2
    try:
        settings = load_settings()
    except ValidationError as exc:
        print(_format_validation_error(exc), file=sys.stderr)
        return 2
    except ComplianceError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    configure_logging(settings.log_level, settings.log_json)
    return args, settings


def main(argv: list[str] | None = None) -> int:
    prepared = _prepare(argv)
    if isinstance(prepared, int):
        return prepared
    args, settings = prepared
    try:
        if args.command == "login":
            return asyncio.run(run_callback_login(settings, open_browser=not args.no_browser))
        if args.command == "health":
            payload = settings.public_view()
            payload.update(http_health_payload(settings))
            payload["live_confirmation_phrase"] = LIVE_CONFIRMATION_PHRASE
            payload["position_lot_cap"] = position_lot_cap(
                trading_mode=settings.trading_mode,
                paper_max_lots=settings.paper_max_lots,
            )
            _print_json(payload)
            return 0
        if args.command == "serve":
            asyncio.run(_serve(settings, args.host, args.port))
            return 0
        if args.command == "instruments" and args.instruments_command == "fetch":
            return asyncio.run(_fetch(settings, _parse_as_of(args.as_of)))
        if args.command == "session" and args.session_command == "exchange":
            return asyncio.run(_exchange(settings))
        if args.command == "db" and args.db_command == "migrate":
            return asyncio.run(_migrate(settings))
        if args.command == "record":
            return asyncio.run(_record(settings, args.csv, _parse_as_of(args.as_of)))
        if args.command == "history":
            return asyncio.run(
                _history(
                    settings,
                    args.csv,
                    _parse_as_of(args.as_of),
                    _parse_moment(args.start),
                    _parse_moment(args.end),
                    args.interval,
                )
            )
        if args.command == "paper":
            if args.replay:
                return asyncio.run(_replay_paper(settings, args.strategy, args.date))
            return asyncio.run(run_paper(settings, args.strategy))
        if args.command == "backtest":
            return _run_backtest(settings, args)
        if args.command == "kill":
            engage(kill_path(settings.data_dir), args.reason)
            _print_json({"killed": True, "reason": args.reason})
            return 0
        if args.command == "resume":
            try:
                release(kill_path(settings.data_dir), args.confirm)
            except KillFileError as exc:
                raise ComplianceError(str(exc)) from exc
            _print_json({"killed": False})
            return 0
        if args.command == "go-live":
            sys.stdout.write(render_checklist())
            return 2
    except PaperStartupError as exc:
        structlog.get_logger().error("paper_startup_refused", reason=str(exc))
        return 2
    except UnmanagedPosition as exc:
        structlog.get_logger().error("paper_not_flat", reason=str(exc))
        return 1
    except ComplianceError as exc:
        structlog.get_logger().error("compliance_refused", reason=str(exc))
        return 2
    except InstrumentMasterError as exc:
        structlog.get_logger().error("instrument_master_rejected", reason=str(exc))
        return 2
    except KiteError as exc:
        structlog.get_logger().error(
            "kite_request_failed",
            status_code=exc.status_code,
            error_type=exc.error_type,
        )
        return 1
    except httpx.HTTPError:
        structlog.get_logger().error("kite_transport_failed")
        return 1
    except DataStoreError as exc:
        structlog.get_logger().error("store_unavailable", store=exc.store, error_type=exc.error_type)
        return 1
    except (OSError, asyncpg.PostgresError) as exc:
        structlog.get_logger().error("database_error", **_safe_db_error(exc))
        return 1
    print(f"unhandled command: {args.command}", file=sys.stderr)
    return 2


async def _replay_paper(settings: Settings, strategy_name: str, raw_date: str | None) -> int:
    if not raw_date:
        print("paper --replay requires --date YYYY-MM-DD", file=sys.stderr)
        return 2
    try:
        day = datetime.strptime(raw_date, "%Y-%m-%d").date()
    except ValueError:
        print("date must be YYYY-MM-DD", file=sys.stderr)
        return 2
    try:
        run_id = await replay_recorded_day(settings, strategy_name, day)
    except NoRecordedTicks as exc:
        print(str(exc), file=sys.stderr)
        return 2
    _print_json({"run_id": str(run_id)})
    return 0


async def _exchange(settings: Settings) -> int:
    request_token = os.environ.get("SCALPER_REQUEST_TOKEN", "").strip()
    if not request_token:
        structlog.get_logger().error("request_token_missing")
        return 2
    async with kite_client() as client:
        session = await establish_session(settings, request_token, client)
    structlog.get_logger().info(
        "kite_session_stored",
        user_id=session.user_id,
        expires_at=session.expires_at.isoformat(),
        session_path=str(settings.session_path),
    )
    _print_json({"user_id": session.user_id, "expires_at": session.expires_at.isoformat()})
    return 0


def _run_backtest(settings: Settings, args: argparse.Namespace) -> int:
    try:
        tick_size = Decimal(args.tick)
    except Exception as exc:
        raise ComplianceError("tick must be a decimal such as 0.05") from exc
    if args.lot <= 0 or args.lots <= 0 or args.window_days <= 0:
        raise ComplianceError("lot, lots, and window-days must be positive")
    bars = load_candles(args.candles)
    spec = BacktestSpec(
        tick_size=tick_size,
        lot_size=args.lot,
        lots=args.lots,
        charges=ChargeSchedule(
            brokerage_rate=settings.brokerage_rate,
            brokerage_cap=settings.brokerage_cap,
            stt_sell_rate=settings.stt_sell_rate,
            exchange_rate=settings.exchange_rate,
            sebi_rate=settings.sebi_rate,
            stamp_buy_rate=settings.stamp_buy_rate,
            gst_rate=settings.gst_rate,
            spread_ticks=settings.spread_ticks,
        ),
        cost_multiple=settings.cost_multiple,
        slippage_ticks=settings.slippage_ticks,
        latency_bars=settings.latency_bars,
        flatten_time=datetime.strptime(settings.flatten_time, "%H:%M").time(),
    )
    try:
        strategy = build_strategy(args.strategy)
    except ValueError as exc:
        raise ComplianceError(str(exc)) from exc
    result = run_backtest(bars, strategy, spec)
    if bars:
        windows = windows_covering(bars[0].bucket_start, bars[-1].bucket_start, timedelta(days=args.window_days))
    else:
        windows = []
    _print_json(result_report(result, windows))
    return 0


async def _migrate(settings: Settings) -> int:
    applied = await apply_migrations(settings.database_url.get_secret_value())
    _print_json({"applied": applied})
    return 0


async def _record(settings: Settings, csv_path: Path, as_of: date) -> int:
    contract = _contract_from_csv(csv_path, as_of)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    try:
        stats = await record_until_stopped(settings, contract, stop)
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.remove_signal_handler(sig)
            except (NotImplementedError, ValueError):
                pass
    decision_logger().info("recording_stopped", tradingsymbol=contract.tradingsymbol, **stats)
    _print_json({"tradingsymbol": contract.tradingsymbol, **stats})
    return 0


async def _history(
    settings: Settings,
    csv_path: Path,
    as_of: date,
    start: datetime,
    end: datetime,
    interval: str,
) -> int:
    contract = _contract_from_csv(csv_path, as_of)
    count = await download_history(settings, contract, start=start, end=end, interval=interval)
    decision_logger().info(
        "historical_candles_stored",
        tradingsymbol=contract.tradingsymbol,
        interval=interval,
        candles=count,
    )
    _print_json({"tradingsymbol": contract.tradingsymbol, "interval": interval, "candles": count})
    return 0


async def _fetch(settings: Settings, as_of: date) -> int:
    async with kite_client() as client:
        contract, archived, health = await refresh_contract(settings, as_of=as_of, client=client)
    log = structlog.get_logger()
    if health.in_maintenance_window:
        log.warning("kite_eod_maintenance_window")
    decision_logger().info("contract_resolved", source="kite", archive=str(archived), **contract.to_dict())
    _print_json(contract.to_dict())
    return 0
