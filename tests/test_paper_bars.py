from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from scalper.clock import IST
from scalper.data.candles import MinuteBook
from scalper.data.models import NormalizedQuote
from scalper.paper.bars import SessionMinuteBook


def _quote(moment: datetime) -> NormalizedQuote:
    return NormalizedQuote(
        instrument_token=1,
        received_at=moment,
        exchange_ts=moment,
        last_price=Decimal("100"),
        last_quantity=1,
        volume=1,
        average_price=Decimal("100"),
        oi=None,
        open=Decimal("100"),
        high=Decimal("100"),
        low=Decimal("100"),
        close=Decimal("100"),
        depth=(),
    )


def test_pre_open_ticks_never_reach_the_candle_book() -> None:
    book = MinuteBook(1)
    session = SessionMinuteBook(book)
    pre_open = datetime(2026, 10, 27, 9, 14, 59, tzinfo=IST)
    at_open = datetime(2026, 10, 27, 9, 15, tzinfo=IST)
    after_close = datetime(2026, 10, 27, 15, 30, 1, tzinfo=IST)
    at_close = datetime(2026, 10, 27, 15, 30, tzinfo=IST)

    assert session.add(_quote(pre_open)) is None
    assert book._start is None
    assert session.ignored == 1

    assert session.add(_quote(at_open)) is None
    assert book._start == at_open.replace(second=0, microsecond=0)

    finished = session.add(_quote(at_close))
    assert finished is not None
    assert finished.bucket_start == at_open.replace(second=0, microsecond=0)
    assert session.add(_quote(after_close)) is None
    assert session.ignored == 2
    assert book._start == at_close.replace(second=0, microsecond=0)
