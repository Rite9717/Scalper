from __future__ import annotations

import os
from pathlib import Path

import pytest

import scalper.execution.halt as entry_halt

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_CSV = Path(__file__).resolve().parent / "fixtures" / "nfo_sample.csv"


@pytest.fixture(autouse=True)
def isolated_process(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keep developer env files and SCALPER_* variables out of unit tests."""
    monkeypatch.chdir(tmp_path)
    for name in list(os.environ):
        if name.startswith("SCALPER_"):
            monkeypatch.delenv(name, raising=False)
    entry_halt._halted = False
    entry_halt._reason = ""


@pytest.fixture
def nfo_csv_text() -> str:
    return FIXTURE_CSV.read_text(encoding="utf-8")
