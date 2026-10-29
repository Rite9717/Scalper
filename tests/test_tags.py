from __future__ import annotations

import pytest

from scalper.kite.tags import make_order_tag


def test_order_tag_is_deterministic_and_compact() -> None:
    first = make_order_tag(timestamp_ms=1_761_300_000_000, strategy_id="trend_pullback", sequence=0)
    again = make_order_tag(timestamp_ms=1_761_300_000_000, strategy_id="trend_pullback", sequence=0)
    other = make_order_tag(timestamp_ms=1_761_300_000_000, strategy_id="vwap_band", sequence=0)
    sequenced = make_order_tag(timestamp_ms=1_761_300_000_000, strategy_id="trend_pullback", sequence=1)
    assert first == again
    assert first != other
    assert first != sequenced
    assert len(first) == 13
    assert first.isalnum()


def test_order_tag_rejects_bad_inputs() -> None:
    with pytest.raises(ValueError):
        make_order_tag(timestamp_ms=True, strategy_id="trend", sequence=0)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        make_order_tag(timestamp_ms=1, strategy_id="has space", sequence=0)
    with pytest.raises(ValueError):
        make_order_tag(timestamp_ms=1, strategy_id="trend", sequence=62)
