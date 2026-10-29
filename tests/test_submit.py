from __future__ import annotations

import asyncio
from decimal import Decimal

import httpx
import pytest
from prometheus_client import CollectorRegistry

from scalper.execution.errors import AmbiguousOrderOutcome
from scalper.execution.kite_broker import KiteBroker
from scalper.execution.states import ManagedOrder
from scalper.execution.submit import engage
from scalper.execution.submit import place_live_limit
from scalper.logging import configure_logging
from scalper.monitor.alerts import Monitor, RecordingSink
from scalper.monitor.metrics import build_metrics
from scalper.risk.killfile import is_killed

LOT = 15


class _Switch:
    def __init__(self, *, fail: bool) -> None:
        self.fail = fail
        self.reasons: list[str] = []

    async def kill_switch(self, engaged: bool, reason: str) -> None:
        steps.append("alert")
        self.reasons.append(reason)
        if self.fail:
            raise RuntimeError("alert down")


steps: list[str] = []


def _broker(client: httpx.AsyncClient) -> KiteBroker:
    return KiteBroker(
        trading_mode="live",
        api_key="key",
        access_token="token",
        static_ip="8.8.8.8",
        observed_ip="8.8.8.8",
        client=client,
        tradingsymbol="NIFTY26OCTFUT",
        poll_interval=0,
    )


def _handler(request: httpx.Request) -> httpx.Response:
    if request.method == "POST":
        raise httpx.ReadTimeout("timed out")
    assert request.url.path == "/orders"
    return httpx.Response(200, json={"status": "success", "data": []})


def _order() -> ManagedOrder:
    return ManagedOrder("abc123", "buy", Decimal("100"), LOT)


def test_a_kill_file_failure_still_alerts_and_raises_ambiguous(tmp_path, monkeypatch) -> None:
    steps.clear()

    def fail_engage(path, reason) -> None:
        steps.append("kill")
        raise OSError("disk full")

    monkeypatch.setattr("scalper.execution.submit.engage", fail_engage)
    switch = _Switch(fail=False)

    async def scenario() -> None:
        transport = httpx.MockTransport(_handler)
        async with httpx.AsyncClient(transport=transport, base_url="https://api.kite.trade") as client:
            with pytest.raises(AmbiguousOrderOutcome) as caught:
                await place_live_limit(
                    _broker(client),
                    _order(),
                    kill_file=tmp_path / "KILL",
                    monitor=switch,  # type: ignore[arg-type]
                )
        assert "kill file was not engaged" in str(caught.value)
        assert "disk full" in str(caught.value)
        assert isinstance(caught.value.__cause__, OSError)

    asyncio.run(scenario())
    assert steps == ["kill", "alert"]
    assert switch.reasons
    assert "disk full" in switch.reasons[0]
    assert not is_killed(tmp_path / "KILL")


def test_an_alert_failure_keeps_the_original_ambiguous_outcome(
    tmp_path,
    monkeypatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    steps.clear()
    configure_logging("INFO", json_output=True)

    def record_engage(path, reason) -> None:
        steps.append("kill")
        engage(path, reason)

    monkeypatch.setattr("scalper.execution.submit.engage", record_engage)
    switch = _Switch(fail=True)
    kill_file = tmp_path / "KILL"

    async def scenario() -> None:
        transport = httpx.MockTransport(_handler)
        async with httpx.AsyncClient(transport=transport, base_url="https://api.kite.trade") as client:
            with pytest.raises(AmbiguousOrderOutcome) as caught:
                await place_live_limit(
                    _broker(client),
                    _order(),
                    kill_file=kill_file,
                    monitor=switch,  # type: ignore[arg-type]
                )
        assert "did not appear" in str(caught.value)
        assert "kill file was not engaged" not in str(caught.value)
        assert not isinstance(caught.value.__cause__, RuntimeError)

    asyncio.run(scenario())
    assert steps == ["kill", "alert"]
    assert is_killed(kill_file)
    logged = capsys.readouterr().err
    assert "kill_alert_failed" in logged
    assert "alert down" in logged


def test_kill_and_alert_failures_both_surface_as_ambiguous(
    tmp_path,
    monkeypatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    steps.clear()
    configure_logging("INFO", json_output=True)

    def fail_engage(path, reason) -> None:
        steps.append("kill")
        raise OSError("disk full")

    monkeypatch.setattr("scalper.execution.submit.engage", fail_engage)
    switch = _Switch(fail=True)

    async def scenario() -> None:
        transport = httpx.MockTransport(_handler)
        async with httpx.AsyncClient(transport=transport, base_url="https://api.kite.trade") as client:
            with pytest.raises(AmbiguousOrderOutcome) as caught:
                await place_live_limit(
                    _broker(client),
                    _order(),
                    kill_file=tmp_path / "KILL",
                    monitor=switch,  # type: ignore[arg-type]
                )
        assert "kill file was not engaged" in str(caught.value)
        assert "disk full" in str(caught.value)
        assert isinstance(caught.value.__cause__, OSError)
        assert not isinstance(caught.value.__cause__, RuntimeError)

    asyncio.run(scenario())
    assert steps == ["kill", "alert"]
    assert not is_killed(tmp_path / "KILL")
    logged = capsys.readouterr().err
    assert "kill_alert_failed" in logged
    assert "alert down" in logged
