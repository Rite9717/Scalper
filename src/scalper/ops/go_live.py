"""Go-live checklist.

`ready_for_live` stays false. A passing backtest and four weeks of paper
trading are evidence this repository cannot invent for itself.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Check:
    text: str
    blocking: bool


def checklist() -> list[Check]:
    return [
        Check("Backtest acceptance passed: at least 300 trades, and positive expectancy after costs in at least 70% of windows", True),
        Check("At least 4 weeks of paper trading finished within tolerance of that backtest", True),
        Check("The report was judged on expectancy and drawdown, and no parameters were fitted on the scored windows", True),
        Check("The Mumbai VPS egress IP is a public static address registered on the Kite developer account", True),
        Check("The Kite redirect URL is exactly http://127.0.0.1:8765/callback", True),
        Check("Postgres password is not the workstation default, and a backup restore has been rehearsed", True),
        Check("Kill switch and resume were tested, and Telegram alerts were received", True),
        Check("Charge rates were checked against the current Zerodha futures tariff", True),
        Check("Live mode is still one lot, with the confirmation phrase set only on the VPS", True),
    ]


def ready_for_live() -> bool:
    return False


def render_checklist() -> str:
    lines = [
        "This installation is not cleared for live orders.",
        "ready_for_live=false",
        "",
    ]
    for item in checklist():
        mark = "blocking" if item.blocking else "check"
        lines.append(f"- [{mark}] {item.text}")
    return "\n".join(lines) + "\n"
