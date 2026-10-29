from __future__ import annotations

import pytest

from scalper.compliance import (
    CLIENT_MAX_ORDERS_PER_MINUTE,
    CLIENT_MAX_ORDERS_PER_SECOND,
    ComplianceError,
    assert_broker_call_allowed,
    assert_order_tag,
    assert_order_type_allowed,
    assert_rate_ceilings_hold,
    assert_ttl_minutes,
    broker_call_mutates_orders,
    position_lot_cap,
)
from scalper.kite.api_facts import (
    KITE_HISTORICAL_INTERVALS,
    KITE_ORDERS_PER_MINUTE,
    KITE_ORDERS_PER_SECOND,
    KITE_TTL_MINUTES_FORUM,
    TRADED_CONTRACT_MODE,
)


def test_documented_limits_and_our_tighter_caps() -> None:
    assert_rate_ceilings_hold()
    assert CLIENT_MAX_ORDERS_PER_SECOND == 3
    assert CLIENT_MAX_ORDERS_PER_MINUTE == 150
    assert CLIENT_MAX_ORDERS_PER_SECOND < KITE_ORDERS_PER_SECOND
    assert CLIENT_MAX_ORDERS_PER_MINUTE < KITE_ORDERS_PER_MINUTE
    assert TRADED_CONTRACT_MODE == "full"
    assert "minute" in KITE_HISTORICAL_INTERVALS
    assert "5minute" in KITE_HISTORICAL_INTERVALS
    assert "day" in KITE_HISTORICAL_INTERVALS
    assert "second" not in KITE_HISTORICAL_INTERVALS
    assert KITE_TTL_MINUTES_FORUM == (15, 30, 60, 90, 120)


def test_only_limit_orders_and_known_ttl_values() -> None:
    assert_order_type_allowed("LIMIT")
    for rejected in ("MARKET", "SL", "SL-M", "limit"):
        with pytest.raises(ComplianceError):
            assert_order_type_allowed(rejected)
    assert assert_ttl_minutes(15) == 15
    with pytest.raises(ComplianceError):
        assert_ttl_minutes(1)
    with pytest.raises(ComplianceError):
        assert_ttl_minutes(True)  # type: ignore[arg-type]


def test_order_tag_charset() -> None:
    assert assert_order_tag("Ab12") == "Ab12"
    for bad in ("", "has space", "has_underscore", "a" * 21, "tag-1"):
        with pytest.raises(ComplianceError):
            assert_order_tag(bad)


def test_live_lot_cap_ignores_the_paper_setting() -> None:
    assert position_lot_cap(trading_mode="live", paper_max_lots=5) == 1
    assert position_lot_cap(trading_mode="paper", paper_max_lots=5) == 5


@pytest.mark.parametrize(
    ("method", "path", "mutates"),
    [
        ("GET", "/orders", False),
        ("GET", "/orders/123", False),
        ("GET", "/instruments/NFO", False),
        ("POST", "/session/token", False),
        ("POST", "/orders/regular", True),
        ("PUT", "/orders/regular/123", True),
        ("DELETE", "/orders/regular/123", True),
        ("POST", "/ordersomething", False),
    ],
)
def test_order_mutation_detection(method: str, path: str, mutates: bool) -> None:
    assert broker_call_mutates_orders(method, path) is mutates


def test_order_mutations_stay_on_the_static_ip() -> None:
    assert_broker_call_allowed(
        method="GET",
        path="/orders",
        trading_mode="paper",
        configured_static_ip="",
        observed_egress_ip=None,
    )
    assert_broker_call_allowed(
        method="POST",
        path="/session/token",
        trading_mode="paper",
        configured_static_ip="",
        observed_egress_ip=None,
    )
    with pytest.raises(ComplianceError, match="paper|live"):
        assert_broker_call_allowed(
            method="POST",
            path="/orders/regular",
            trading_mode="paper",
            configured_static_ip="8.8.8.8",
            observed_egress_ip="8.8.8.8",
        )
    assert_broker_call_allowed(
        method="DELETE",
        path="/orders/regular/99",
        trading_mode="live",
        configured_static_ip="8.8.8.8",
        observed_egress_ip="8.8.8.8",
    )
    with pytest.raises(ComplianceError, match="egress"):
        assert_broker_call_allowed(
            method="PUT",
            path="/orders/regular/99",
            trading_mode="live",
            configured_static_ip="8.8.8.8",
            observed_egress_ip="1.1.1.1",
        )
    with pytest.raises(ComplianceError):
        assert_broker_call_allowed(
            method="POST",
            path="/orders/regular",
            trading_mode="live",
            configured_static_ip="8.8.8.8",
            observed_egress_ip="10.0.0.5",
        )
