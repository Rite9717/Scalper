from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from scalper.cli import main
from scalper.clock import IST
from scalper.config import Settings
from scalper.data.models import Candle
from scalper.data.stored import choose_candles
from scalper.ops.doctor import (
    Status,
    credentials_status,
    instrument_status,
    kill_status,
    paper_ready,
    render_doctor,
    session_status,
)
from scalper.risk.killfile import engage, kill_path


def test_doctor_names_the_missing_env_fields() -> None:
    status = credentials_status(Settings())
    assert status.ready is False
    assert "SCALPER_EXPECTED_USER_ID" in status.action
    assert "SCALPER_KITE_API_KEY" in status.action
    assert "SCALPER_KITE_API_SECRET" in status.action


def test_doctor_asks_for_login_and_todays_instrument_file() -> None:
    settings = Settings(expected_user_id="AB1234", kite_api_key="key", kite_api_secret="secret")
    moment = datetime(2026, 10, 27, 10, 0, tzinfo=IST)
    session = session_status(settings, moment)
    master = instrument_status(settings, moment.date())
    assert session.ready is False
    assert "scalper login" in session.action
    assert master.ready is False
    assert "instruments fetch" in master.action


def test_a_kill_file_is_not_paper_ready(tmp_path) -> None:
    settings = Settings(data_dir=tmp_path)
    engage(kill_path(tmp_path), "stop")
    status = kill_status(settings)
    assert status.ready is False
    checks = [
        Status("credentials", True, ""),
        Status("database", True, ""),
        Status("session", True, ""),
        Status("instrument_master", True, ""),
        status,
        Status("redis", False, "Start Redis before scalper record"),
    ]
    assert paper_ready(checks) is False
    text = render_doctor(checks)
    assert "paper ready: no" in text
    assert "ready_for_live=false" in text


def test_redis_can_be_down_and_paper_still_ready() -> None:
    checks = [
        Status(name, True, "")
        for name in ("credentials", "database", "session", "instrument_master", "kill_switch")
    ]
    checks.append(Status("redis", False, "Start Redis before scalper record"))
    assert paper_ready(checks) is True
    assert "paper ready: yes" in render_doctor(checks)


def test_doctor_command_reports_the_manual_steps(capsys) -> None:
    code = main(["doctor"])
    text = capsys.readouterr().out
    assert code == 2
    assert "paper ready: no" in text
    assert "scalper login" in text
    assert "instruments fetch" in text
    assert "ready_for_live=false" in text


def test_own_ticks_replace_a_downloaded_bar_for_the_same_minute() -> None:
    moment = datetime(2026, 10, 27, 10, 0, tzinfo=IST)
    downloaded = _bar(moment, "kite", Decimal("100"))
    recorded = _bar(moment, "ticks", Decimal("101"))
    chosen = choose_candles([downloaded, recorded])
    assert len(chosen) == 1
    assert chosen[0].source == "ticks"
    assert chosen[0].close == Decimal("101")


def test_backtest_stored_requires_a_date_range() -> None:
    assert main(["backtest", "--stored"]) == 2


def test_backtest_refuses_a_file_and_stored_candles_together(tmp_path) -> None:
    path = tmp_path / "bars.csv"
    path.write_text("bucket_start,open,high,low,close,volume\n", encoding="utf-8")
    code = main(
        [
            "backtest",
            "--stored",
            "--candles",
            str(path),
            "--from",
            "2026-10-01",
            "--to",
            "2026-10-02",
            "--csv",
            str(path),
            "--tick",
            "0.05",
            "--lot",
            "15",
        ]
    )
    assert code == 2


def _bar(moment: datetime, source: str, close: Decimal) -> Candle:
    return Candle(
        instrument_token=1,
        bucket_start=moment,
        interval="minute",
        open=close,
        high=close,
        low=close,
        close=close,
        volume=1,
        oi=None,
        source=source,
    )
