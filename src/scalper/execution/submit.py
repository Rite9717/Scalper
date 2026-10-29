"""Live submit path. An unresolved place stops new entries and alerts the operator."""

from __future__ import annotations

from pathlib import Path

import structlog

from scalper.execution.broker import Broker, PlaceOutcome
from scalper.execution.errors import AmbiguousOrderOutcome
from scalper.execution.halt import halt_new_entries
from scalper.execution.states import ManagedOrder
from scalper.monitor.alerts import Monitor
from scalper.risk.killfile import engage

log = structlog.get_logger()

_BOOK_CONFIRM_READS = 5
_BOOK_CONFIRM_WINDOW_SECONDS = 5.0


async def close_on_ambiguous(
    reason: str,
    *,
    kill_file: Path,
    monitor: Monitor,
    original: AmbiguousOrderOutcome | None = None,
) -> None:
    """Halt, engage the kill file, alert, then raise AmbiguousOrderOutcome."""
    halt_new_entries(reason)
    kill_error: Exception | None = None
    try:
        engage(kill_file, reason)
    except Exception as caught:
        kill_error = caught
    alert_reason = reason if kill_error is None else f"{reason}; kill file was not engaged: {kill_error}"
    try:
        await monitor.kill_switch(True, alert_reason)
    except Exception as alert_error:
        log.error(
            "kill_alert_failed",
            error_type=type(alert_error).__name__,
            error=str(alert_error),
        )
    if kill_error is not None:
        raise AmbiguousOrderOutcome(f"{reason}; kill file was not engaged: {kill_error}") from kill_error
    if original is not None:
        raise original
    raise AmbiguousOrderOutcome(reason)


async def place_live_limit(
    broker: Broker,
    order: ManagedOrder,
    *,
    kill_file: Path,
    monitor: Monitor,
) -> PlaceOutcome:
    """Place one limit order. AmbiguousOrderOutcome engages the kill file, then alerts.

    A kill-file failure is still followed by the alert. The caller then receives
    AmbiguousOrderOutcome noting that failure. An alert failure is logged and
    does not replace the original outcome.
    """
    try:
        return await broker.place_limit(order)
    except AmbiguousOrderOutcome as exc:
        await close_on_ambiguous(
            f"ambiguous place {order.tag}: {exc}",
            kill_file=kill_file,
            monitor=monitor,
            original=exc,
        )
