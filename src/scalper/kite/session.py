"""On-disk Kite session. The file holds the access token and is mode 0600."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict, SecretStr

from scalper.clock import ensure_ist, in_eod_maintenance_window
from scalper.compliance import PersonalAccountError


class _StoredSession(BaseModel):
    model_config = ConfigDict(extra="forbid")

    user_id: str
    user_type: str
    access_token: str
    login_time: datetime
    expires_at: datetime
    api_key: str


@dataclass(frozen=True)
class KiteSession:
    user_id: str
    user_type: str
    access_token: SecretStr
    login_time: datetime
    expires_at: datetime
    api_key: str

    def __init__(
        self,
        *,
        user_id: str,
        user_type: str,
        access_token: str | SecretStr,
        login_time: datetime,
        expires_at: datetime,
        api_key: str,
    ) -> None:
        secret = access_token if isinstance(access_token, SecretStr) else SecretStr(access_token)
        object.__setattr__(self, "user_id", user_id)
        object.__setattr__(self, "user_type", user_type)
        object.__setattr__(self, "access_token", secret)
        object.__setattr__(self, "login_time", ensure_ist(login_time))
        object.__setattr__(self, "expires_at", ensure_ist(expires_at))
        object.__setattr__(self, "api_key", api_key)


@dataclass(frozen=True)
class SessionHealth:
    usable: bool
    expired: bool
    in_maintenance_window: bool
    user_matches: bool
    api_key_matches: bool

    @property
    def new_trading_allowed(self) -> bool:
        return self.usable and not self.in_maintenance_window


def assess_session(
    session: KiteSession,
    *,
    now: datetime,
    expected_user_id: str,
    api_key: str,
) -> SessionHealth:
    moment = ensure_ist(now)
    user_matches = session.user_id == expected_user_id
    api_key_matches = session.api_key == api_key
    expired = moment >= session.expires_at
    usable = user_matches and api_key_matches and not expired
    return SessionHealth(
        usable=usable,
        expired=expired,
        in_maintenance_window=in_eod_maintenance_window(moment),
        user_matches=user_matches,
        api_key_matches=api_key_matches,
    )


def require_usable_session(health: SessionHealth) -> None:
    if not health.user_matches:
        raise PersonalAccountError("stored session is for a different user id")
    if not health.api_key_matches:
        raise PersonalAccountError("stored session was minted for a different API key")
    if health.expired or not health.usable:
        raise PersonalAccountError("stored Kite session has expired; log in again")


def _atomic_private_write(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
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


def save_session(path: Path, session: KiteSession) -> None:
    payload = json.dumps(
        {
            "user_id": session.user_id,
            "user_type": session.user_type,
            "access_token": session.access_token.get_secret_value(),
            "login_time": session.login_time.isoformat(),
            "expires_at": session.expires_at.isoformat(),
            "api_key": session.api_key,
        },
        separators=(",", ":"),
    )
    _atomic_private_write(path, payload + "\n")


def load_session(path: Path) -> KiteSession:
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags)
    except FileNotFoundError as exc:
        raise PersonalAccountError("no Kite session file; log in first") from exc
    except OSError as exc:
        raise PersonalAccountError("Kite session file must be a regular file, not a symlink") from exc
    try:
        mode = os.fstat(fd).st_mode & 0o777
        if mode & 0o077:
            raise PersonalAccountError("Kite session file must not be readable by group or others")
        with os.fdopen(fd, "r", encoding="utf-8") as handle:
            fd = -1
            raw = handle.read()
    finally:
        if fd >= 0:
            os.close(fd)
    try:
        stored = _StoredSession.model_validate_json(raw)
    except ValueError:
        # Do not chain the parse error. Its message can contain the access token.
        raise PersonalAccountError("Kite session file is unreadable or has the wrong shape") from None
    if stored.user_type != "individual":
        raise PersonalAccountError("stored session is not an individual account")
    return KiteSession(
        user_id=stored.user_id,
        user_type=stored.user_type,
        access_token=stored.access_token,
        login_time=stored.login_time,
        expires_at=stored.expires_at,
        api_key=stored.api_key,
    )
