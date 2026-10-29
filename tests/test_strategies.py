from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from scalper.backtest.engine import Trade
from scalper.backtest.report import judge
from scalper.backtest.strategy import MarketState
from scalper.backtest.costs import ChargeSchedule
from scalper.clock import IST
from scalper.cli import main
from scalper.data.models import Candle
from scalper.strategy.filters import FilterConfig, rejection_reason
from scalper.strategy.indicators import completed_five_minute_bars, ema, session_vwap
from scalper.strategy.registry import build_strategy
from scalper.strategy.trend import TrendPullback, TrendPullbackConfig
from scalper.strategy.vwap import VwapReversion, VwapReversionConfig

TICK = Decimal("0.05")


def _candle(moment: datetime, close: str, *, low: str | None = None, high: str | None = None, volume: int = 100) -> Candle:
    close_price = Decimal(close)
    return Candle(
        instrument_token=1,
        bucket_start=moment,
        interval="minute",
        open=close_price - Decimal("0.05"),
        high=close_price if high is None else Decimal(high),
        low=close_price if low is None else Decimal(low),
        close=close_price,
        volume=volume,
        oi=None,
        source="test",
    )


def _state(bars: list[Candle], **overrides: object) -> MarketState:
    values: dict[str, object] = {
        "bars": tuple(bars),
        "tick_size": TICK,
        "now": bars[-1].bucket_start,
    }
    values.update(overrides)
    return MarketState(**values)  # type: ignore[arg-type]


def test_ema_and_session_vwap() -> None:
    assert ema([Decimal(str(value)) for value in (1, 2, 3, 4)], 3) == [
        None,
        None,
        Decimal("2"),
        Decimal("3"),
    ]
    start = datetime(2026, 10, 27, 9, 15, tzinfo=IST)
    bars = [
        _candle(start, "10", volume=1),
        _candle(start + timedelta(minutes=1), "12", volume=1),
    ]
    vwap = session_vwap(bars)
    assert vwap[0] == Decimal("10")
    assert vwap[1] == Decimal("11")


def test_five_minute_bars_drop_the_open_bucket() -> None:
    start = datetime(2026, 10, 27, 9, 15, tzinfo=IST)
    bars = [_candle(start + timedelta(minutes=offset), "100") for offset in range(6)]
    completed = completed_five_minute_bars(bars)
    assert len(completed) == 1
    assert completed[0].bucket_start == start
    assert completed[0].volume == 500


def test_filters_block_the_open_a_loss_streak_and_a_wide_spread() -> None:
    start = datetime(2026, 10, 27, 9, 16, tzinfo=IST)
    bars = [_candle(start, "100")]
    assert rejection_reason(_state(bars), FilterConfig()) == "opening window"
    later = [_candle(start.replace(hour=10), "100")]
    streak = _state(later, loss_streak=3, bars_since_last_loss=1)
    assert rejection_reason(streak, FilterConfig()) == "loss-streak cooldown"
    wide = _state(later, spread_ticks=2, best_depth=10)
    assert rejection_reason(wide, FilterConfig()) == "spread too wide"
    expiry = _state([_candle(start.replace(hour=13), "100")], expiry_day=True)
    assert rejection_reason(expiry, FilterConfig()) == "expiry afternoon"
    assert rejection_reason(_state(later), FilterConfig()) is None


def test_trend_pullback_requires_the_vwap_side_and_a_close_back_through_the_ema() -> None:
    bars = _rising_bars()
    config = TrendPullbackConfig(
        ema_period=3,
        five_minute_ema_period=3,
        slope_lookback=1,
        volume_period=3,
        atr_period=3,
        volume_multiple=Decimal("1"),
        stop_atr=Decimal("1"),
        target_r=Decimal("2"),
        time_stop_bars=4,
    )
    strategy = TrendPullback(config)
    signal = strategy.on_bar(_state(bars))
    assert signal is not None
    assert signal.side == "buy"
    assert signal.regime == "trend"
    assert signal.stop_price < signal.limit_price < signal.target_price
    blocked = bars[:-1] + [_with_close_below_vwap(bars)]
    assert strategy.on_bar(_state(blocked)) is None


def test_vwap_reversion_stays_idle_while_the_five_minute_trend_is_steep() -> None:
    flat = VwapReversion(
        VwapReversionConfig(
            ema_period=3,
            slope_lookback=1,
            trend_slope_atr=Decimal("2"),
            band_atr=Decimal("0.25"),
            atr_period=3,
            stop_atr=Decimal("1"),
            time_stop_bars=4,
        )
    )
    ranging_bars = _flat_then_spike()
    signal = flat.on_bar(_state(ranging_bars))
    assert signal is not None
    assert signal.side == "buy"
    assert signal.regime == "range"
    steep = VwapReversion(
        VwapReversionConfig(
            ema_period=3,
            slope_lookback=1,
            trend_slope_atr=Decimal("0.01"),
            band_atr=Decimal("0.25"),
            atr_period=3,
        )
    )
    assert steep.on_bar(_state(_rising_bars())) is None


def test_acceptance_on_a_short_sample_does_not_pass() -> None:
    start = datetime(2026, 10, 27, 10, 0, tzinfo=IST)
    trades = [
        Trade("buy", start, start, Decimal("100"), Decimal("101"), 15, Decimal("15"), Decimal("1"), Decimal("14"), "target", "trend")
    ]
    verdict = judge(trades, [(start, start + timedelta(days=20))])
    assert verdict["passed"] is False
    assert verdict["parameters_fitted"] is False
    assert any("300" in reason for reason in verdict["reasons"])


def test_trend_command_on_a_short_file_fails_the_gate(tmp_path) -> None:
    path = tmp_path / "bars.csv"
    lines = ["bucket_start,open,high,low,close,volume"]
    start = datetime(2026, 10, 27, 10, 0, tzinfo=IST)
    for offset in range(30):
        moment = start + timedelta(minutes=offset)
        lines.append(f"{moment.isoformat()},100,100.2,99.8,100.1,100")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    code = main(
        ["backtest", "--candles", str(path), "--tick", "0.05", "--lot", "15", "--strategy", "trend_pullback"]
    )
    assert code == 0


def test_registry_rejects_an_unknown_name() -> None:
    try:
        build_strategy("magic")
    except ValueError as exc:
        assert "trend_pullback" in str(exc)
    else:
        raise AssertionError("unknown strategy was accepted")


def _rising_bars() -> list[Candle]:
    start = datetime(2026, 10, 27, 10, 0, tzinfo=IST)
    bars: list[Candle] = []
    price = Decimal("100")
    for offset in range(25):
        close = price + Decimal("0.20")
        bars.append(
            Candle(
                instrument_token=1,
                bucket_start=start + timedelta(minutes=offset),
                interval="minute",
                open=price,
                high=close + Decimal("0.10"),
                low=price - Decimal("0.05"),
                close=close,
                volume=100,
                oi=None,
                source="test",
            )
        )
        price = close
    line = ema([bar.close for bar in bars], 3)
    latest = line[-1]
    assert latest is not None
    pullback_open = latest + Decimal("0.10")
    pullback_close = latest + Decimal("0.30")
    bars.append(
        Candle(
            instrument_token=1,
            bucket_start=start + timedelta(minutes=25),
            interval="minute",
            open=pullback_open,
            high=pullback_close + Decimal("0.10"),
            low=latest - Decimal("0.05"),
            close=pullback_close,
            volume=400,
            oi=None,
            source="test",
        )
    )
    return bars


def _with_close_below_vwap(bars: list[Candle]) -> Candle:
    vwap = session_vwap(bars[:-1])[-1]
    assert vwap is not None
    last = bars[-1]
    close = vwap - Decimal("1")
    return Candle(
        instrument_token=last.instrument_token,
        bucket_start=last.bucket_start,
        interval=last.interval,
        open=close + Decimal("0.20"),
        high=close + Decimal("0.20"),
        low=close - Decimal("0.20"),
        close=close,
        volume=last.volume,
        oi=None,
        source=last.source,
    )


def _flat_then_spike() -> list[Candle]:
    start = datetime(2026, 10, 27, 10, 0, tzinfo=IST)
    bars: list[Candle] = []
    for offset in range(25):
        bars.append(
            Candle(
                instrument_token=1,
                bucket_start=start + timedelta(minutes=offset),
                interval="minute",
                open=Decimal("100"),
                high=Decimal("100.15"),
                low=Decimal("99.85"),
                close=Decimal("100"),
                volume=100,
                oi=None,
                source="test",
            )
        )
    vwap = session_vwap(bars)[-1]
    from scalper.strategy.indicators import wilder_atr

    atr = wilder_atr(bars, 3)[-1]
    assert vwap is not None and atr is not None
    lower = vwap - Decimal("0.25") * atr
    bars.append(
        Candle(
            instrument_token=1,
            bucket_start=start + timedelta(minutes=25),
            interval="minute",
            open=lower + Decimal("0.05"),
            high=lower + Decimal("0.20"),
            low=lower - Decimal("0.20"),
            close=lower + Decimal("0.10"),
            volume=100,
            oi=None,
            source="test",
        )
    )
    return bars
