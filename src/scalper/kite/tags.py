"""Order-tag helpers.

A tag is the idempotency key we send to Kite. The broker order id remains
the execution identity. The same inputs always produce the same tag, so a
retry of one decision does not look like a new decision.
"""

from __future__ import annotations

import hashlib
import re

from scalper.compliance import assert_order_tag

_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
_STRATEGY_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def _base62(value: int, width: int) -> str:
    if type(value) is not int or value < 0:
        raise ValueError("base62 value must be a non-negative int")
    if value == 0:
        encoded = "0"
    else:
        chars: list[str] = []
        remaining = value
        while remaining:
            remaining, remainder = divmod(remaining, 62)
            chars.append(_ALPHABET[remainder])
        encoded = "".join(reversed(chars))
    if len(encoded) > width:
        raise ValueError("value does not fit in the tag width")
    return encoded.rjust(width, "0")


def make_order_tag(*, timestamp_ms: int, strategy_id: str, sequence: int = 0) -> str:
    """Build a 13-character alphanumeric tag.

    Layout: 8-character base62 millisecond timestamp, 1-character base62
    sequence (0-61), 4-character sha256 prefix of the strategy id.
    """
    if not _STRATEGY_ID.fullmatch(strategy_id):
        raise ValueError("strategy_id must be 1-64 characters of letters, digits, '_' or '-'")
    if type(sequence) is not int or not 0 <= sequence <= 61:
        raise ValueError("sequence must be an int from 0 to 61")
    digest = hashlib.sha256(strategy_id.encode("utf-8")).hexdigest()[:4]
    tag = _base62(timestamp_ms, 8) + _base62(sequence, 1) + digest
    return assert_order_tag(tag)
