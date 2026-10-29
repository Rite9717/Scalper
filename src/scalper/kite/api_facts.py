"""Kite Connect v3 facts this system is built against.

Sources checked for Phase 1:
- https://kite.trade/docs/connect/v3/
- https://kite.trade/docs/connect/v3/user/
- https://kite.trade/docs/connect/v3/orders/
- https://kite.trade/docs/connect/v3/market-quotes/
- https://kite.trade/docs/connect/v3/historical/
- https://kite.trade/docs/connect/v3/websocket/
- https://kite.trade/docs/connect/v3/exceptions/
- Zerodha staff posts on the Kite Connect forum, March 2026, for the static
  IP and market-protection rollout, and January 2026 for TTL values.

Where a number comes from a forum reply rather than the API reference, the
name says so. Do not treat those as a stable enum.
"""

from __future__ import annotations

KITE_API_ROOT = "https://api.kite.trade"
KITE_LOGIN_URL = "https://kite.zerodha.com/connect/login"
KITE_VERSION = "3"
KITE_WS_URL = "wss://ws.kite.trade"

# Authorization scheme for authenticated calls: "token {api_key}:{access_token}".
# POST /session/token is the exception: it is unauthenticated and form-encoded.

KITE_ORDERS_PER_SECOND = 10
KITE_ORDERS_PER_MINUTE = 400
KITE_ORDERS_PER_DAY = 5000
KITE_MAX_MODIFICATIONS_PER_ORDER = 25
KITE_HISTORICAL_REQUESTS_PER_SECOND = 3
KITE_QUOTE_REQUESTS_PER_SECOND = 1
KITE_OTHER_REQUESTS_PER_SECOND = 10

KITE_QUOTE_INSTRUMENT_LIMIT = 500
KITE_OHLC_INSTRUMENT_LIMIT = 1000
KITE_LTP_INSTRUMENT_LIMIT = 1000

KITE_WS_MAX_INSTRUMENTS_PER_CONNECTION = 3000
KITE_WS_MAX_CONNECTIONS = 3
KITE_WS_MODES = ("ltp", "quote", "full")
# Full mode is the 184-byte packet with five levels of depth.
TRADED_CONTRACT_MODE = "full"

# pykiteconnect KiteTicker divides binary integer prices by the segment
# divisor (100 for NFO, prices in paise) before the tick callback. Callbacks
# already yield rupees as float. Later phases must not divide again. Floats
# still get rounded to the contract tick with Decimal at the strategy boundary.
KITE_TICKER_PRICES_ARE_RUPEES = True

KITE_ORDER_TAG_MAX_LENGTH = 20
KITE_MODIFIABLE_ORDER_FIELDS = frozenset(
    {
        "order_type",
        "quantity",
        "price",
        "trigger_price",
        "disclosed_quantity",
        "validity",
    }
)

KITE_HISTORICAL_INTERVALS = (
    "minute",
    "3minute",
    "5minute",
    "10minute",
    "15minute",
    "30minute",
    "60minute",
    "day",
)

# Official docs describe validity_ttl as minutes. Zerodha staff (January 2026)
# said arbitrary values are not auto-cancelled; the Kite web UI values are
# the ones the OMS honours. Confirm again before the execution phase.
KITE_TTL_MINUTES_FORUM = (15, 30, 60, 90, 120)

KITE_ORDER_TYPES = ("MARKET", "LIMIT", "SL", "SL-M")
KITE_VALIDITIES = ("DAY", "IOC", "TTL")
KITE_PRODUCTS = ("CNC", "NRML", "MIS", "MTF", "BO", "CO")
KITE_VARIETIES = ("regular", "amo", "co", "iceberg", "auction")

# market_protection is required for MARKET and SL-M algo orders.
# -1 means automatic. 0 is rejected. LIMIT orders do not use it.
KITE_MARKET_PROTECTION_AUTO = -1
KITE_MARKET_PROTECTION_MIN = 1
KITE_MARKET_PROTECTION_MAX = 100
