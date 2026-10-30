"""Latest-quote cache. Redis holds the hot value; tests use memory."""

from __future__ import annotations

import json
from typing import Protocol

from scalper.data.models import NormalizedQuote


class QuoteCache(Protocol):
    async def put(self, quote: NormalizedQuote) -> None: ...

    async def get(self, instrument_token: int) -> dict[str, object] | None: ...


class MemoryQuoteCache:
    def __init__(self) -> None:
        self.values: dict[int, dict[str, object]] = {}

    async def put(self, quote: NormalizedQuote) -> None:
        self.values[quote.instrument_token] = quote.to_cache()

    async def get(self, instrument_token: int) -> dict[str, object] | None:
        return self.values.get(instrument_token)

    async def drop(self, instrument_token: int) -> None:
        self.values.pop(instrument_token, None)


class RedisQuoteCache:
    def __init__(self, client: object, *, ttl_seconds: int = 86_400) -> None:
        self._client = client
        self._ttl = ttl_seconds

    def _key(self, instrument_token: int) -> str:
        return f"scalper:quote:{instrument_token}"

    async def put(self, quote: NormalizedQuote) -> None:
        payload = json.dumps(quote.to_cache())
        await self._client.set(self._key(quote.instrument_token), payload, ex=self._ttl)  # type: ignore[attr-defined]

    async def drop(self, instrument_token: int) -> None:
        await self._client.delete(self._key(instrument_token))  # type: ignore[attr-defined]

    async def get(self, instrument_token: int) -> dict[str, object] | None:
        raw = await self._client.get(self._key(instrument_token))  # type: ignore[attr-defined]
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            return None
        return parsed
