"""Hard limits for this personal account.

The risk manager in a later phase is the runtime veto. These checks are the
ones the skeleton can already enforce: mode, identity, order shape, rate
ceilings, and which broker calls may leave the registered static IP.
"""

from __future__ import annotations

import ipaddress
import re

from scalper.kite.api_facts import (
    KITE_ORDERS_PER_DAY,
    KITE_ORDERS_PER_MINUTE,
    KITE_ORDERS_PER_SECOND,
    KITE_TTL_MINUTES_FORUM,
)

# Client ceilings. They are inside Kite's published order limits.
CLIENT_MAX_ORDERS_PER_SECOND = 3
CLIENT_MAX_ORDERS_PER_MINUTE = 150
CLIENT_MAX_ORDERS_PER_DAY = KITE_ORDERS_PER_DAY

# Live trading starts at one lot. This is not a setting.
LIVE_INITIAL_MAX_LOTS = 1

LIVE_CONFIRMATION_PHRASE = "I_CONFIRM_LIVE_TRADING_FOR_MY_ACCOUNT_ONLY"

_ORDER_TAG = re.compile(r"^[A-Za-z0-9]{1,20}$")
_USER_ID = re.compile(r"^[A-Za-z0-9]{2,32}$")


class ComplianceError(Exception):
    """A call was refused because it would break the personal-account rules."""


class PersonalAccountError(ComplianceError):
    """The Kite session does not belong to this one personal account."""


def assert_rate_ceilings_hold() -> None:
    if CLIENT_MAX_ORDERS_PER_SECOND > KITE_ORDERS_PER_SECOND:
        raise ComplianceError("client per-second cap exceeds the Kite order limit")
    if CLIENT_MAX_ORDERS_PER_MINUTE > KITE_ORDERS_PER_MINUTE:
        raise ComplianceError("client per-minute cap exceeds the Kite order limit")
    if CLIENT_MAX_ORDERS_PER_DAY > KITE_ORDERS_PER_DAY:
        raise ComplianceError("client daily cap exceeds the Kite order limit")


def assert_public_ipv4(value: str) -> ipaddress.IPv4Address:
    try:
        parsed = ipaddress.ip_address(value)
    except ValueError as exc:
        raise ComplianceError("static IP must be a public IPv4 address") from exc
    if not isinstance(parsed, ipaddress.IPv4Address) or not parsed.is_global:
        raise ComplianceError("static IP must be a public IPv4 address")
    return parsed


def assert_user_id(value: str) -> str:
    if not _USER_ID.fullmatch(value):
        raise ComplianceError("expected user id must be 2-32 letters or digits")
    return value


def assert_order_type_allowed(order_type: str) -> None:
    if order_type != "LIMIT":
        raise ComplianceError("only LIMIT orders are permitted")


def assert_order_tag(tag: str) -> str:
    """Tags are alphanumeric and at most 20 characters.

    The Kite docs describe the field that way. One official example also
    contains a space. This system rejects spaces so a tag can be used as an
    idempotency key in logs and file names.
    """
    if not _ORDER_TAG.fullmatch(tag):
        raise ComplianceError("order tag must be 1-20 alphanumeric characters")
    return tag


def assert_ttl_minutes(minutes: int) -> int:
    if type(minutes) is not int or minutes not in KITE_TTL_MINUTES_FORUM:
        allowed = ", ".join(str(item) for item in KITE_TTL_MINUTES_FORUM)
        raise ComplianceError(f"TTL minutes must be one of: {allowed}")
    return minutes


def position_lot_cap(*, trading_mode: str, paper_max_lots: int) -> int:
    if trading_mode == "live":
        return LIVE_INITIAL_MAX_LOTS
    if paper_max_lots < 1:
        raise ComplianceError("paper lot cap must be at least 1")
    return paper_max_lots


def broker_call_mutates_orders(method: str, path: str) -> bool:
    """True for POST, PUT, and DELETE on /orders.

    Zerodha's April 2026 note says the static-IP check applies to order
    endpoints. The same note says the order book, positions, and WebSocket
    stay reachable from other IPs. GET is treated as a read. Session login,
    instrument downloads, and historical candles are not order mutations.
    """
    if method.upper() not in {"POST", "PUT", "DELETE"}:
        return False
    cleaned = path.split("?", 1)[0]
    if not cleaned.startswith("/"):
        cleaned = "/" + cleaned
    return cleaned == "/orders" or cleaned.startswith("/orders/")


def assert_broker_call_allowed(
    *,
    method: str,
    path: str,
    trading_mode: str,
    configured_static_ip: str,
    observed_egress_ip: str | None,
) -> None:
    """Refuse a Kite call that would place, modify, or cancel an order.

    Paper mode never mutates orders at the broker. Live mutations must leave
    from the configured public static IP. Non-mutating calls return without
    checking the egress IP, so research can run off the VPS.
    """
    if not broker_call_mutates_orders(method, path):
        return
    if trading_mode != "live":
        raise ComplianceError("broker order changes are refused unless mode is live")
    if not configured_static_ip or not observed_egress_ip:
        raise ComplianceError("live order changes require the registered static IP")
    configured = assert_public_ipv4(configured_static_ip)
    observed = assert_public_ipv4(observed_egress_ip)
    if observed != configured:
        raise ComplianceError("egress IP does not match the registered static IP")
