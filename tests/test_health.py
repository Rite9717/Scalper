from __future__ import annotations

import asyncio
import json
from urllib.error import HTTPError
from urllib.request import urlopen

from scalper.config import Settings
from scalper.health import HealthServer


def _get(url: str) -> tuple[int, dict[str, object]]:
    try:
        with urlopen(url, timeout=3) as response:
            return response.status, json.loads(response.read().decode())
    except HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def test_health_endpoint_exposes_mode_and_phase_only() -> None:
    async def scenario() -> None:
        server = HealthServer(Settings(expected_user_id="AB1234"), host="127.0.0.1", port=0)
        await server.start()
        try:
            status, body = await asyncio.to_thread(_get, f"http://127.0.0.1:{server.port}/health")
            missing_status, missing_body = await asyncio.to_thread(
                _get,
                f"http://127.0.0.1:{server.port}/other",
            )
        finally:
            await server.stop()
        assert status == 200
        assert body == {
            "status": "ok",
            "service": "scalper",
            "phase": 8,
            "trading_mode": "paper",
        }
        assert missing_status == 404
        assert missing_body == {"status": "not_found"}
        assert "AB1234" not in json.dumps(body)

    asyncio.run(scenario())
