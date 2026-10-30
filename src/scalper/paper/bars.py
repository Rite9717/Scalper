"""One-minute candles built only from the cash session.

Ticks before 09:15 or after 15:30 IST are ignored. They do not open a bar
and they do not update the bar that is already open.
"""

from __future__ import annotations

from datetime import datetime, time

from scalper.clock import ensure_ist
from scalper.data.candles import MinuteBook
from scalper.data.models import Candle, NormalizedQuote

SESSION_OPEN = time(9, 15)
SESSION_CLOSE = time(15, 30)


def in_cash_session(moment: datetime) -> bool:
    clock = ensure_ist(moment).timetz().replace(tzinfo=None)
    return SESSION_OPEN <= clock <= SESSION_CLOSE


class SessionMinuteBook:
    """Forwards a tick to the candle book only inside the cash session."""

    def __init__(self, book: MinuteBook) -> None:
        self.book = book
        self.ignored = 0

    def add(self, quote: NormalizedQuote) -> Candle | None:
        if not in_cash_session(quote.received_at):
            self.ignored += 1
            return None
        return self.book.add(quote)

    def flush(self) -> Candle | None:
        return self.book.flush()
