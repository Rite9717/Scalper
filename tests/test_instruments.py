from __future__ import annotations

import json
from pathlib import Path
from datetime import date
from decimal import Decimal

import pytest

from scalper.cli import main
from scalper.kite.instruments import (
    AmbiguousContract,
    ContractNotFound,
    InstrumentMasterError,
    archive_instrument_master,
    is_nifty_future,
    parse_instruments,
    select_current_nifty_future,
)

FIXTURE_CSV = Path(__file__).resolve().parent / "fixtures" / "nfo_sample.csv"
REPO_ROOT = Path(__file__).resolve().parents[1]

HEADER = (
    "instrument_token,exchange_token,tradingsymbol,name,last_price,expiry,"
    "strike,tick_size,lot_size,instrument_type,segment,exchange\n"
)


def test_resolver_source_does_not_hardcode_contract_specs() -> None:
    source = (REPO_ROOT / "src" / "scalper" / "kite" / "instruments.py").read_text(encoding="utf-8")
    assert "lot_size = 65" not in source
    assert "lot_size = 75" not in source
    assert 'tick_size = Decimal("0.05")' not in source
    assert 'tick_size = Decimal("0.15")' not in source


def test_selects_current_month_from_the_row(nfo_csv_text: str) -> None:
    instruments = parse_instruments(nfo_csv_text)
    option = next(item for item in instruments if item.tradingsymbol == "NIFTY26OCT25000CE")
    future = next(item for item in instruments if item.tradingsymbol == "NIFTY26OCTFUT")
    assert option.strike == Decimal("25000")
    assert future.strike is None
    assert future.name == "NIFTY"
    assert next(item for item in instruments if item.tradingsymbol == "NIFTY26NOVFUT").name == "NIFTY"

    current = select_current_nifty_future(instruments, date(2026, 10, 24))
    assert current.to_dict() == {
        "instrument_token": 12345001,
        "exchange_token": 1001,
        "tradingsymbol": "NIFTY26OCTFUT",
        "name": "NIFTY",
        "expiry": "2026-10-27",
        "tick_size": "0.05",
        "lot_size": 65,
        "exchange": "NFO",
        "segment": "NFO-FUT",
        "instrument_type": "FUT",
    }
    assert current.tick_size == Decimal("0.05")


def test_expiry_day_stays_on_the_expiring_contract_then_rolls(nfo_csv_text: str) -> None:
    instruments = parse_instruments(nfo_csv_text)
    assert select_current_nifty_future(instruments, date(2026, 10, 27)).tradingsymbol == "NIFTY26OCTFUT"
    november = select_current_nifty_future(instruments, date(2026, 10, 28))
    assert november.tradingsymbol == "NIFTY26NOVFUT"
    assert november.lot_size == 65
    december = select_current_nifty_future(instruments, date(2026, 11, 25))
    assert december.tradingsymbol == "NIFTY26DECFUT"
    assert december.lot_size == 75
    assert december.tick_size == Decimal("0.15")
    with pytest.raises(ContractNotFound):
        select_current_nifty_future(instruments, date(2026, 12, 30))


def test_near_dated_other_underlyings_are_not_nifty(nfo_csv_text: str) -> None:
    instruments = parse_instruments(nfo_csv_text)
    symbols = {item.tradingsymbol for item in instruments if is_nifty_future(item)}
    assert symbols == {"NIFTY26OCTFUT", "NIFTY26NOVFUT", "NIFTY26DECFUT", "NIFTY26SEPFUT"}
    assert "BANKNIFTY26OCTFUT" not in symbols
    assert "NIFTYNXT5026OCTFUT" not in symbols
    assert "MIDCPNIFTY26OCTFUT" not in symbols
    assert "NIFTY26OCT25000CE" not in symbols
    assert "BANKEX26OCTFUT" not in symbols


def test_blank_name_legacy_row_is_accepted() -> None:
    text = HEADER + "5720322,22345,NIFTY26OCTFUT,,78,2026-10-27,,0.05,65,FUT,NFO-FUT,NFO\n"
    contract = select_current_nifty_future(parse_instruments(text), date(2026, 10, 24))
    assert contract.tradingsymbol == "NIFTY26OCTFUT"
    assert contract.name == "NIFTY"
    assert contract.instrument_token == 5720322


def test_spaced_header_matches_the_documented_csv_shape() -> None:
    text = (
        "instrument_token, exchange_token, tradingsymbol, name, last_price, expiry, "
        "strike, tick_size, lot_size, instrument_type, segment, exchange\n"
        "5720322,22345,NIFTY26OCTFUT,,78.0,2026-10-27,,0.05,65,FUT,NFO-FUT,NFO\n"
    )
    contract = select_current_nifty_future(parse_instruments(text), date(2026, 10, 24))
    assert contract.instrument_token == 5720322
    assert contract.tick_size == Decimal("0.05")


def test_duplicate_expiry_is_rejected() -> None:
    text = (
        HEADER
        + "1,1,NIFTY26OCTFUT,NIFTY,1,2026-10-27,0,0.05,65,FUT,NFO-FUT,NFO\n"
        + "2,2,NIFTY26OCTFUT,NIFTY,1,2026-10-27,0,0.05,65,FUT,NFO-FUT,NFO\n"
    )
    with pytest.raises(AmbiguousContract):
        select_current_nifty_future(parse_instruments(text), date(2026, 10, 24))


def test_bad_tick_and_missing_column_and_extra_column() -> None:
    bad_tick = HEADER + "1,1,NIFTY26OCTFUT,NIFTY,1,2026-10-27,0,abc,65,FUT,NFO-FUT,NFO\n"
    with pytest.raises(InstrumentMasterError, match="tick_size"):
        parse_instruments(bad_tick)
    with pytest.raises(InstrumentMasterError, match="missing columns"):
        parse_instruments("tradingsymbol,name\nNIFTY26OCTFUT,NIFTY\n")
    extra = HEADER + "1,1,NIFTY26OCTFUT,NIFTY,1,2026-10-27,0,0.05,65,FUT,NFO-FUT,NFO,UNEXPECTED\n"
    with pytest.raises(InstrumentMasterError, match="extra columns"):
        parse_instruments(extra)


def test_bom_and_crlf_normalize(nfo_csv_text: str) -> None:
    mangled = "\ufeff" + nfo_csv_text.replace("\n", "\r\n")
    contract = select_current_nifty_future(parse_instruments(mangled), date(2026, 10, 24))
    assert contract.tradingsymbol == "NIFTY26OCTFUT"


def test_archive_keeps_the_first_dump_and_a_changed_revision(tmp_path, nfo_csv_text: str) -> None:
    first = archive_instrument_master(nfo_csv_text, tmp_path, date(2026, 10, 24))
    second = archive_instrument_master(nfo_csv_text, tmp_path, date(2026, 10, 24))
    assert first == second
    assert first.name == "nfo-2026-10-24.csv"
    changed = nfo_csv_text.replace("25000.5", "25001.5")
    revision = archive_instrument_master(changed, tmp_path, date(2026, 10, 24))
    assert revision != first
    assert revision.name.startswith("nfo-2026-10-24-")
    assert "25000.5" in first.read_text(encoding="utf-8")
    assert "25001.5" in revision.read_text(encoding="utf-8")
    assert (tmp_path / "nfo-2026-10-24.sha256").is_file()


def test_resolve_cli_prints_the_contract(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["instruments", "resolve", "--csv", str(FIXTURE_CSV), "--as-of", "2026-10-24"])
    captured = capsys.readouterr()
    assert code == 0
    payload = json.loads(captured.out)
    assert payload["tradingsymbol"] == "NIFTY26OCTFUT"
    assert payload["lot_size"] == 65
    assert payload["tick_size"] == "0.05"
    assert payload["expiry"] == "2026-10-27"
    assert "contract_resolved" in captured.err
