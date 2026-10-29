"""Prometheus metrics for the personal trading process."""

from __future__ import annotations

from dataclasses import dataclass

from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest


@dataclass
class Metrics:
    registry: CollectorRegistry
    risk_vetoes: Counter
    orders: Counter
    alerts: Counter
    kill_switch: Gauge
    feed_stalled: Gauge
    quote_age_seconds: Gauge
    daily_pnl: Gauge


def build_metrics(registry: CollectorRegistry | None = None) -> Metrics:
    target = registry or CollectorRegistry()
    return Metrics(
        registry=target,
        risk_vetoes=Counter(
            "scalper_risk_vetoes_total",
            "Entries refused by the risk manager",
            ["reason"],
            registry=target,
        ),
        orders=Counter(
            "scalper_orders_total",
            "Order lifecycle events",
            ["event"],
            registry=target,
        ),
        alerts=Counter(
            "scalper_alerts_total",
            "Operator alerts",
            ["kind"],
            registry=target,
        ),
        kill_switch=Gauge(
            "scalper_kill_switch",
            "1 when the manual kill switch is on",
            registry=target,
        ),
        feed_stalled=Gauge(
            "scalper_feed_stalled",
            "1 when the market-data feed is stalled",
            registry=target,
        ),
        quote_age_seconds=Gauge(
            "scalper_quote_age_seconds",
            "Age of the newest quote in seconds",
            registry=target,
        ),
        daily_pnl=Gauge(
            "scalper_daily_pnl",
            "Net pnl for the current session day",
            registry=target,
        ),
    )


def render_metrics(metrics: Metrics) -> bytes:
    return generate_latest(metrics.registry)
