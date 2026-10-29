"""A kill file outlives the process. While it exists, new entries are refused."""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from scalper.clock import now_ist

RESUME_CONFIRMATION = "I_RESUME_TRADING"


class KillFileError(RuntimeError):
    """The kill file could not be changed."""


def kill_path(data_dir: Path) -> Path:
    return data_dir / "KILL"


def is_killed(path: Path) -> bool:
    return path.exists()


def engage(path: Path, reason: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps({"reason": reason, "at": now_ist().isoformat()}) + "\n"
    temporary = path.with_name(path.name + ".tmp")
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            fd = -1
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        if fd >= 0:
            os.close(fd)
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)
    os.chmod(path, 0o600)


def release(path: Path, confirmation: str) -> None:
    if confirmation != RESUME_CONFIRMATION:
        raise KillFileError("resume requires the exact confirmation phrase")
    if not path.exists():
        return
    path.unlink()


def read_reason(path: Path) -> str | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "kill file unreadable"
    reason = payload.get("reason")
    return reason if isinstance(reason, str) and reason else "manual kill"


def described_at(path: Path) -> datetime | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        raw = payload.get("at")
        if not isinstance(raw, str):
            return None
        return datetime.fromisoformat(raw)
    except (OSError, json.JSONDecodeError, ValueError):
        return None
