"""Live submit path. An unresolved place stops new entries and alerts the operator."""

from __future__ import annotations

from pathlib import Path

import structlog

from scalper.execution.broker import Broker, PlaceOutcome
from scalper.execution.errors import AmbiguousOrderOutcome
from scalper.execution.states import ManagedOrder
from scalper.monitor.alerts import Monitor
from scalper.risk.killfile import engage

log = structlog.get_logger()


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
        reason = f"ambiguous place {order.tag}: {exc}"
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
            raise AmbiguousOrderOutcome(f"{exc}; kill file was not engaged: {kill_error}") from kill_error
        raise
