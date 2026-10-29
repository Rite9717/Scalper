"""Process-local entry halt.

Set when an order may have reached the broker and the outcome is unknown.
It is raised before the kill file is touched, so a failed disk write still
blocks a new entry.
"""

from __future__ import annotations

_halted = False
_reason = ""


def halt_new_entries(reason: str) -> None:
    global _halted, _reason
    if _halted:
        return
    _halted = True
    _reason = reason


def entries_halted() -> bool:
    return _halted


def entry_halt_reason() -> str:
    return _reason
