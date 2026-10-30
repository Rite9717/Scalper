# Scalper

Personal intraday scalping system for the current-month Nifty 50 futures contract (NFO). Phase 8 adds the [deployment guide](docs/deployment.md), the [runbook](docs/runbook.md), and a go-live checklist that still refuses live trading. A live order is refused unless the mode is live and the egress IP matches.

Default mode is paper. Live mode requires `SCALPER_TRADING_MODE=live`, your Zerodha user id, one API key, a public static IP, and this exact confirmation:

```text
SCALPER_LIVE_CONFIRMATION=I_CONFIRM_LIVE_TRADING_FOR_MY_ACCOUNT_ONLY
```

Live size is one lot. That cap is code, not a setting.

## What you do by hand

The program is finished. These steps stay yours because they need your Zerodha account, your browser, and real market days:

1. Copy `.env.example` to `.env`. Set `SCALPER_EXPECTED_USER_ID`, `SCALPER_KITE_API_KEY`, and `SCALPER_KITE_API_SECRET`. Leave `SCALPER_TRADING_MODE=paper`.
2. `docker compose up -d`, then `python -m scalper db migrate`.
3. `python -m scalper login` and finish Zerodha's own login, including TOTP, in the browser.
4. `python -m scalper instruments fetch`.
5. `python -m scalper doctor`. It prints `paper ready: yes` or the exact command still left.
6. Download research candles, then backtest the stored bars (no separate CSV of candles):

```bash
python -m scalper history --csv data/instruments/nfo-YYYY-MM-DD.csv --from YYYY-MM-DD --to YYYY-MM-DD
python -m scalper backtest --stored --csv data/instruments/nfo-YYYY-MM-DD.csv --from YYYY-MM-DD --to YYYY-MM-DD --strategy trend_pullback
```

7. On a trading day, `python -m scalper paper --strategy trend_pullback`. Orders are not sent.
8. `python -m scalper go-live` lists what you still have to observe yourself (a long backtest, four weeks of paper, the VPS, alerts, a backup restore). It does not turn live trading on.

## Tests

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
pytest
```

Resolve the sample contract without credentials:

```bash
python -m scalper instruments resolve \
  --csv tests/fixtures/nfo_sample.csv \
  --as-of 2026-10-24
```

Show the redacted config:

```bash
python -m scalper health
```

## Docker

Postgres (TimescaleDB), Redis, and a health server. Nothing in this compose file logs into Kite or sends an order.

```bash
cp .env.example .env
mkdir -p data
docker compose up --build
curl --fail http://127.0.0.1:8080/health
```

The app user inside the image is uid 10001. `./data` has to be writable by that user before `scalper instruments fetch` can archive a dump. Host ports are bound to `127.0.0.1` only.

`POSTGRES_PASSWORD` defaults to `scalper` for a workstation. Change it before this stack holds a session or market data. Phase 8 is the VPS deployment guide; this compose file is the local skeleton.

## Daily login

Personal Kite apps do not get a refresh token. The access token expires at 06:00 IST. Login is manual, in your browser. This process only catches the redirect.

Register this URL on the Kite developer console before the first login. It has to match exactly, including the port:

```text
http://127.0.0.1:8765/callback
```

Put your user id and API credentials in `.env`, then:

```bash
python -m scalper login
```

The command prints a Kite URL, opens it, and waits. You log in yourself, including TOTP on Zerodha's page. Kite redirects to the loopback callback. The process exchanges the request token, writes `data/session.json` mode `0600`, and exits. The request token is not written to the log.

Run this where the browser can reach `127.0.0.1`. On the VPS, use an SSH tunnel (`ssh -L 8765:127.0.0.1:8765 user@vps`) and run `scalper login` there, so the session file is created on the execution host. Login is not an order call, so it does not have to use the static IP. Later order calls still do.

There is no password field and no TOTP field. If `SCALPER_KITE_PASSWORD`, `SCALPER_KITE_TOTP_SECRET`, or `SCALPER_LOGIN_METHOD=totp` is set, startup fails.

`scalper session exchange` remains a manual fallback when you already have a request token. Pass it only through `SCALPER_REQUEST_TOKEN` for that one command. Do not put it in `.env`.

The callback listens on `127.0.0.1` only. A non-loopback host is rejected so the request token cannot arrive on a LAN address.

## Phase 2 market data

Apply the Timescale tables after Postgres is up:

```bash
python -m scalper db migrate
```

Record the traded contract from a downloaded instrument file. This uses the stored Kite session, subscribes in `full` mode, and writes ticks, depth, and 1-minute candles. Prices from `KiteTicker` are already rupees; they are quantized to the contract tick and are not divided again.

```bash
python -m scalper record --csv data/instruments/nfo-2026-10-25.csv
```

Ctrl+C flushes the last candle and exits. Download broker minute candles for research with:

```bash
python -m scalper history \
  --csv data/instruments/nfo-2026-10-25.csv \
  --from 2026-10-20 \
  --to 2026-10-25 \
  --interval minute
```

Historical calls stay at or below 3 requests per second.

## Phase 3 backtest

Replay a candle file. `--tick` and `--lot` come from the instrument master for that contract. The built-in `plumbing` strategy only checks the wiring. The report says `edge_claimed: false`, and it fails the acceptance rule until a later strategy has at least 300 trades and positive expectancy in 70% of the windows.

```bash
python -m scalper backtest \
  --candles data/candles.csv \
  --tick 0.05 \
  --lot 65 \
  --strategy trend_pullback
```

`--strategy` is `plumbing`, `trend_pullback`, or `vwap_reversion`. Trend pullback follows a five-minute EMA and VWAP, then buys or sells a one-minute bar that tags EMA(20) and closes back through it on above-average volume. VWAP reversion trades only while that five-minute EMA is flat relative to ATR, from the outer band back toward VWAP. Shared filters cover the open and close windows, expiry afternoon, scheduled events, an ATR band, a daily loss limit, and a loss-streak cooldown. Spread and depth are checked when a quote book is present.

A buy limit fills only when a later bar trades strictly below the limit. The signal bar itself cannot fill. If one bar could hit both the stop and the target, the stop is used. Charges default to Zerodha's futures table as published on 27 Oct 2026, including 0.05% STT on the sell side. Change `SCALPER_STT_SELL_RATE` and the other charge settings when the tariff changes.

## Execution

The execution engine talks to the paper broker unless the mode is live. A repeated decision uses the same order tag and does not open a second order. A limit can be repriced 25 times; the next change cancels it and places a new tagged order. Shutdown cancels working orders and flattens any position. It raises if an unknown broker order is still open.

Paper mode never posts to Kite. A live post is a LIMIT order and is refused when the egress IP does not match the registered static IP.

## Monitoring

`http://127.0.0.1:8080/metrics` is the Prometheus scrape endpoint. Alerts stay off until both the bot token and chat id are set and `SCALPER_ALERTS_ENABLED=true`. A feed stall and a kill switch each send one message until the condition clears. Order rejects and the daily summary are sent as they happen. The bot token is not written to the log.

## Risk manager

Every new entry goes through one gate. There is no bypass. A manual kill, a feed stall, clock drift, a PnL mismatch with the broker, or repeated rejects halt new entries. The daily loss limit and the 15:20 flatten time do the same. Exits stay allowed so the position can still be closed.

```bash
python -m scalper kill --reason "operator stop"
python -m scalper resume --confirm I_RESUME_TRADING
```

Live size stays one lot. That cap is not a setting.

## Instrument master

`scalper instruments fetch` downloads `GET /instruments/NFO`, archives it under `data/instruments/nfo-YYYY-MM-DD.csv`, and prints the current-month contract. Lot size and tick size come from that row. A second download the same day with different bytes is stored beside the first file; the morning file is left in place.

The current-month contract is the NFO future whose tradingsymbol matches `NIFTY` + `YY` + `MMM` + `FUT`, whose name is `NIFTY` or blank, and whose expiry is the nearest date on or after the as-of date. On the expiry date that contract is still current.

## Where the Kite v3 docs differ from the brief

Checked against https://kite.trade/docs/connect/v3/ and the pages it links to (user, orders, market quotes, historical, websocket, exceptions), plus Zerodha staff notes on the developer forum for the April 2026 static-IP rollout and for TTL values.

These match the brief:

- Login is `https://kite.zerodha.com/connect/login?v=3&api_key=...`, then `POST /session/token` with a SHA-256 hex checksum of `api_key + request_token + api_secret` and no separator. The secret is not sent. `X-Kite-Version: 3` is required. Later calls use `Authorization: token api_key:access_token`.
- The access token expires at 06:00 the next day. `refresh_token` is only for approved platforms. The sample personal response has it empty.
- WebSocket URL is `wss://ws.kite.trade`. Modes are `ltp`, `quote`, and `full`. Limits are 3000 instruments per connection and 3 connections. `full` is the packet with five levels of depth.
- pykiteconnect `KiteTicker` converts binary prices to rupees before the callback. This code will not divide those prices again.
- Order tag is documented as alphanumeric, maximum 20 characters. Order placement limits are 10/s, 400/min, 5000/day, and 25 modifications per order.
- Historical candle limit is 3 requests/second. Instrument tokens are flushed at expiry, so the master has to be archived. A placed order returns an `order_id` and is not a fill.
- From 1 April 2026, order calls from an IP that is not on the Kite developer whitelist are rejected. The whitelist is on the developer account, not per app.

These do not match the brief, and the code follows the docs:

- Historical intervals are `minute`, `3minute`, `5minute`, `10minute`, `15minute`, `30minute`, `60minute`, and `day`. There is no tick or second history. `continuous=1` is documented for expired futures and the text says it returns day candles.
- Quote calls have their own limit: 1 request/second. `/quote` accepts 500 instruments, `/quote/ohlc` and `/quote/ltp` accept 1000.
- `validity_ttl` is in minutes. Zerodha staff said arbitrary values are not auto-cancelled. The durations that match the Kite web UI are 15, 30, 60, 90, and 120 minutes. A TTL of a few seconds is not available. IOC is the immediate-cancel validity. NFO documents IOC. The same April 2026 note says MCX algo orders do not support IOC.
- `MARKET` and `SL-M` algo orders require `market_protection` of `-1` or `1` through `100`. Zero is rejected. This system allows `LIMIT` only, so it does not send `market_protection`.
- The static-IP check, per Zerodha, applies to order endpoints. WebSocket, order book, positions, and other reads stay usable from any IP. Whitelist edits are limited to one change per calendar week. The regulation allows an IP to be shared with immediate family; this process still requires one configured user id and refuses any other id.
- A forum report from August 2024 says an end-of-day process after midnight has invalidated tokens before the documented 06:00 expiry. New trading is refused from 00:00 until 06:00 IST. The hard expiry stays 06:00.
- Logging in again the same day may return the same access token. A logout on the Kite website invalidates the API session with HTTP 403 `TokenException`.
- The docs' order example for `tag` contains a space (`connect test order1`). Tags produced here are `[A-Za-z0-9]`, at most 20 characters. Some order payloads also include a `tags` array beside `tag`.
- The modifiable order fields listed are `order_type`, `quantity`, `price`, `trigger_price`, `disclosed_quantity`, and `validity`. Tag is not in that list. This system will not modify an order onto `MARKET`.
- `autoslice` can split one order into many and burn the rate budget. It stays off.
- The instrument CSV in the docs is a gzipped dump, best fetched once a day around 08:30 IST. The documented unique key is exchange plus tradingsymbol, because tokens are reused. The 2015 sample row has an empty `name` for `NIFTY15DECFUT`; current dumps quote the name. Both are accepted. The sample lot size of 75 is an old row, not a constant.
- The client order caps are 3/second and 150/minute, which are inside Kite's 10/second and 400/minute. The daily ceiling cannot be configured above 5000. At 150/minute the 5000/day placement cap is reachable in about 34 minutes. The default daily ceiling is still 5000 until you set a lower `SCALPER_MAX_ORDERS_PER_DAY`.
- WebSocket: market data is binary, postbacks are text, and an idle socket gets a 1-byte heartbeat. The 3000-instrument cap is enforced on the subscribe call; extra tokens error and the connection stays up. The 3-connection figure is described by Zerodha as a soft limit that can still get an app suspended.

## Layout

```text
src/scalper/config.py            settings and the live-mode gate
src/scalper/logging.py           structlog, secrets redacted
src/scalper/compliance.py        LIMIT-only, throttles, static-IP gate
src/scalper/kite/auth.py         checksum and token exchange
src/scalper/kite/callback.py     loopback browser login
src/scalper/kite/session.py      0600 session file and 06:00 expiry
src/scalper/kite/instruments.py  NFO dump and current NIFTY future
src/scalper/kite/api_facts.py    numbers taken from the Kite docs
src/scalper/ops/doctor.py        which manual steps are still left
```

`python -m scalper doctor` is the list of what you still do by hand. Live trading stays off.
