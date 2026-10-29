"""Exchange clock helpers. All trading decisions use Asia/Kolkata."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

# Documented Kite access-token expiry. See session.access_token_expires_at.
TOKEN_EXPIRY_TIME = time(6, 0)

# Kite's instrument dump is generated once a day. The docs recommend fetching
# it around 08:30 IST.
INSTRUMENT_REFRESH_LOCAL_TIME = time(8, 30)


def now_ist() -> datetime:
    return datetime.now(IST)


def today_ist() -> date:
    return now_ist().date()


def ensure_ist(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(IST)


def access_token_expires_at(login_time: datetime) -> datetime:
    """Return 06:00 IST after the session's trading day.

    Kite documents expiry at 06:00 on the next day. A login at or after
    06:00 IST expires at 06:00 IST on the next calendar date. A login before
    06:00 still belongs to the previous trading day and expires at 06:00
    that same calendar morning.
    """
    local = ensure_ist(login_time)
    six_am = local.replace(hour=6, minute=0, second=0, microsecond=0)
    if local < six_am:
        return six_am
    return six_am + timedelta(days=1)


def in_eod_maintenance_window(moment: datetime) -> bool:
    """True from 00:00 inclusive until 06:00 exclusive, IST.

    The published token expiry is 06:00. Zerodha has also described an
    end-of-day process after midnight that has invalidated tokens earlier.
    New trading is refused in this window. Reads may still proceed, with a
    warning.
    """
    local = ensure_ist(moment)
    return local.time() < TOKEN_EXPIRY_TIME
