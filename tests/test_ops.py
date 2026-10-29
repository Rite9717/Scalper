from __future__ import annotations

from pathlib import Path

from scalper.cli import main
from scalper.ops.go_live import checklist, ready_for_live, render_checklist

ROOT = Path(__file__).resolve().parents[1]


def test_go_live_stays_blocked() -> None:
    assert ready_for_live() is False
    text = render_checklist()
    assert "not cleared for live orders" in text
    assert any(item.blocking and "300" in item.text for item in checklist())
    assert any(item.blocking and "4 weeks" in item.text for item in checklist())
    code = main(["go-live"])
    assert code == 2


def test_compose_stays_on_paper_and_localhost() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    script = (ROOT / "scripts" / "backup-postgres.sh").read_text(encoding="utf-8")
    assert "SCALPER_TRADING_MODE: paper" in compose
    assert "127.0.0.1:5432:5432" in compose
    assert "set -eu" in script
    assert "pg_dump" in script
