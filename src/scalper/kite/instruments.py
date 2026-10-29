"""NFO instrument master and the current-month Nifty futures contract.

Lot size and tick size always come from the downloaded row. They are not
constants. The exchange reuses instrument tokens after expiry, so the archive
keeps each day's dump and the resolver keys off exchange plus tradingsymbol.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import re
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

import httpx

from scalper.kite.auth import authenticated_headers
from scalper.kite.http import KiteError, parse_object_data

_NIFTY_FUTURE = re.compile(r"^NIFTY\d{2}[A-Z]{3}FUT$")
_INTEGER = re.compile(r"^[1-9]\d*$")
_DECIMAL = re.compile(r"^(?:0|[1-9]\d*)(?:\.\d+)?$")
_REQUIRED_COLUMNS = (
    "instrument_token",
    "exchange_token",
    "tradingsymbol",
    "name",
    "last_price",
    "expiry",
    "strike",
    "tick_size",
    "lot_size",
    "instrument_type",
    "segment",
    "exchange",
)


class InstrumentMasterError(Exception):
    """The instrument dump could not be parsed or did not contain one contract."""


class ContractNotFound(InstrumentMasterError):
    pass


class AmbiguousContract(InstrumentMasterError):
    pass


@dataclass(frozen=True)
class Instrument:
    instrument_token: int
    exchange_token: int
    tradingsymbol: str
    name: str
    expiry: date | None
    strike: Decimal | None
    tick_size: Decimal
    lot_size: int
    instrument_type: str
    segment: str
    exchange: str


@dataclass(frozen=True)
class NiftyFutureContract:
    instrument_token: int
    exchange_token: int
    tradingsymbol: str
    name: str
    expiry: date
    tick_size: Decimal
    lot_size: int
    exchange: str
    segment: str
    instrument_type: str

    def to_dict(self) -> dict[str, object]:
        return {
            "instrument_token": self.instrument_token,
            "exchange_token": self.exchange_token,
            "tradingsymbol": self.tradingsymbol,
            "name": self.name,
            "expiry": self.expiry.isoformat(),
            "tick_size": format(self.tick_size, "f"),
            "lot_size": self.lot_size,
            "exchange": self.exchange,
            "segment": self.segment,
            "instrument_type": self.instrument_type,
        }


def normalize_instrument_csv(text: str) -> str:
    cleaned = text.lstrip("\ufeff").replace("\r\n", "\n").replace("\r", "\n")
    if cleaned and not cleaned.endswith("\n"):
        cleaned += "\n"
    return cleaned


def decode_instrument_body(payload: bytes) -> str:
    if payload.startswith(b"\x1f\x8b"):
        try:
            payload = gzip.decompress(payload)
        except gzip.BadGzipFile as exc:
            raise InstrumentMasterError("instrument dump was not valid gzip") from exc
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise InstrumentMasterError("instrument dump was not UTF-8") from exc
    return normalize_instrument_csv(text)


def _cell(row: dict[str, str | None], key: str) -> str:
    value = row.get(key)
    if value is None:
        return ""
    return value.strip()


def _require_int(raw: str, *, row_number: int, field: str) -> int:
    if not _INTEGER.fullmatch(raw):
        raise InstrumentMasterError(f"row {row_number}: {field} is not a positive integer")
    return int(raw)


def _require_decimal(raw: str, *, row_number: int, field: str) -> Decimal:
    if not _DECIMAL.fullmatch(raw):
        raise InstrumentMasterError(f"row {row_number}: {field} is not a decimal")
    value = Decimal(raw)
    if value <= 0:
        raise InstrumentMasterError(f"row {row_number}: {field} must be positive")
    return value


def parse_instruments(text: str) -> list[Instrument]:
    normalized = normalize_instrument_csv(text)
    if not normalized.strip():
        raise InstrumentMasterError("instrument dump is empty")
    reader = csv.DictReader(io.StringIO(normalized))
    if reader.fieldnames is None:
        raise InstrumentMasterError("instrument dump has no header")
    fieldnames = [name.strip() for name in reader.fieldnames]
    missing = [name for name in _REQUIRED_COLUMNS if name not in fieldnames]
    if missing:
        raise InstrumentMasterError("instrument dump is missing columns: " + ", ".join(missing))
    reader.fieldnames = fieldnames
    instruments: list[Instrument] = []
    for row_number, raw_row in enumerate(reader, start=2):
        if raw_row.get(None):
            raise InstrumentMasterError(f"row {row_number}: unexpected extra columns")
        row = {key: value for key, value in raw_row.items() if key is not None}
        if not any(_cell(row, key) for key in fieldnames):
            continue
        tradingsymbol = _cell(row, "tradingsymbol")
        if not tradingsymbol:
            raise InstrumentMasterError(f"row {row_number}: tradingsymbol is empty")
        instrument_type = _cell(row, "instrument_type")
        segment = _cell(row, "segment")
        exchange = _cell(row, "exchange")
        if not instrument_type or not segment or not exchange:
            raise InstrumentMasterError(
                f"row {row_number}: instrument_type, segment, and exchange are required"
            )
        expiry_raw = _cell(row, "expiry")
        try:
            expiry = date.fromisoformat(expiry_raw) if expiry_raw else None
        except ValueError as exc:
            raise InstrumentMasterError(f"row {row_number}: expiry is not YYYY-MM-DD") from exc
        strike_raw = _cell(row, "strike")
        if strike_raw == "":
            strike = None
        elif not _DECIMAL.fullmatch(strike_raw):
            raise InstrumentMasterError(f"row {row_number}: strike is not a decimal")
        else:
            strike = Decimal(strike_raw)
            if instrument_type == "FUT" and strike == 0:
                strike = None
        instruments.append(
            Instrument(
                instrument_token=_require_int(_cell(row, "instrument_token"), row_number=row_number, field="instrument_token"),
                exchange_token=_require_int(_cell(row, "exchange_token"), row_number=row_number, field="exchange_token"),
                tradingsymbol=tradingsymbol,
                name=_cell(row, "name"),
                expiry=expiry,
                strike=strike,
                tick_size=_require_decimal(_cell(row, "tick_size"), row_number=row_number, field="tick_size"),
                lot_size=_require_int(_cell(row, "lot_size"), row_number=row_number, field="lot_size"),
                instrument_type=instrument_type,
                segment=segment,
                exchange=exchange,
            )
        )
    if not instruments:
        raise InstrumentMasterError("instrument dump has no rows")
    return instruments


def is_nifty_future(instrument: Instrument) -> bool:
    if instrument.exchange != "NFO" or instrument.segment != "NFO-FUT":
        return False
    if instrument.instrument_type != "FUT":
        return False
    if not _NIFTY_FUTURE.fullmatch(instrument.tradingsymbol):
        return False
    return instrument.name in {"", "NIFTY"}


def select_current_nifty_future(instruments: list[Instrument], as_of: date) -> NiftyFutureContract:
    """Pick the nearest NIFTY future whose expiry is on or after as_of.

    On the expiry date the expiring contract is still the current-month
    contract. The next morning's dump is what rolls the selection forward.
    """
    candidates = [
        instrument
        for instrument in instruments
        if is_nifty_future(instrument) and instrument.expiry is not None and instrument.expiry >= as_of
    ]
    if not candidates:
        raise ContractNotFound(f"no current-month NIFTY future on or after {as_of.isoformat()}")
    candidates.sort(key=lambda instrument: (instrument.expiry, instrument.tradingsymbol))
    nearest = candidates[0].expiry
    same_expiry = [instrument for instrument in candidates if instrument.expiry == nearest]
    if len(same_expiry) != 1:
        symbols = ", ".join(instrument.tradingsymbol for instrument in same_expiry)
        raise AmbiguousContract(f"more than one NIFTY future expires on {nearest.isoformat()}: {symbols}")
    chosen = same_expiry[0]
    assert chosen.expiry is not None
    return NiftyFutureContract(
        instrument_token=chosen.instrument_token,
        exchange_token=chosen.exchange_token,
        tradingsymbol=chosen.tradingsymbol,
        name=chosen.name or "NIFTY",
        expiry=chosen.expiry,
        tick_size=chosen.tick_size,
        lot_size=chosen.lot_size,
        exchange=chosen.exchange,
        segment=chosen.segment,
        instrument_type=chosen.instrument_type,
    )


def archive_instrument_master(csv_text: str, directory: Path, as_of: date, exchange: str = "NFO") -> Path:
    """Write the day's dump. A later download with different bytes is kept beside it."""
    normalized = normalize_instrument_csv(csv_text)
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
    directory.mkdir(parents=True, exist_ok=True)
    canonical = directory / f"{exchange.lower()}-{as_of.isoformat()}.csv"
    sidecar = directory / f"{exchange.lower()}-{as_of.isoformat()}.sha256"
    if canonical.exists():
        existing = hashlib.sha256(canonical.read_bytes()).hexdigest()
        if existing == digest:
            return canonical
        revision = directory / f"{exchange.lower()}-{as_of.isoformat()}-{digest[:12]}.csv"
        revision.write_text(normalized, encoding="utf-8")
        revision.with_suffix(".sha256").write_text(digest + "\n", encoding="utf-8")
        return revision
    canonical.write_text(normalized, encoding="utf-8")
    sidecar.write_text(digest + "\n", encoding="utf-8")
    return canonical


async def fetch_instrument_csv(
    *,
    api_key: str,
    access_token: str,
    client: httpx.AsyncClient,
    exchange: str = "NFO",
) -> str:
    response = await client.get(
        f"/instruments/{exchange}",
        headers=authenticated_headers(api_key, access_token),
    )
    if response.status_code != 200:
        content_type = response.headers.get("content-type", "")
        if "json" in content_type:
            parse_object_data(response)
        raise KiteError(
            f"instrument download failed with HTTP {response.status_code}",
            status_code=response.status_code,
        )
    return decode_instrument_body(response.content)
