-- Phase 2 market data. Prices are numeric rupees, already quantized to tick size.

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS scalper_meta (
    key text PRIMARY KEY,
    value text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);
-- instrument_token is not a permanent identity: the contract row records the
-- symbol and expiry that this token meant on the day it was recorded.

CREATE TABLE IF NOT EXISTS recorded_contracts (
    instrument_token bigint NOT NULL,
    tradingsymbol text NOT NULL,
    expiry date NOT NULL,
    tick_size numeric NOT NULL,
    lot_size integer NOT NULL,
    exchange text NOT NULL,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_token, tradingsymbol)
);

CREATE TABLE IF NOT EXISTS ticks (
    received_at timestamptz NOT NULL,
    instrument_token bigint NOT NULL,
    last_price numeric NOT NULL,
    last_quantity bigint NOT NULL,
    volume bigint NOT NULL,
    average_price numeric NOT NULL,
    oi bigint,
    open numeric NOT NULL,
    high numeric NOT NULL,
    low numeric NOT NULL,
    close numeric NOT NULL,
    exchange_ts timestamptz,
    PRIMARY KEY (instrument_token, received_at)
);

SELECT create_hypertable('ticks', 'received_at', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS depth_levels (
    received_at timestamptz NOT NULL,
    instrument_token bigint NOT NULL,
    side text NOT NULL,
    level smallint NOT NULL,
    price numeric NOT NULL,
    quantity bigint NOT NULL,
    orders integer NOT NULL,
    PRIMARY KEY (instrument_token, received_at, side, level),
    CONSTRAINT depth_levels_side CHECK (side IN ('buy', 'sell')),
    CONSTRAINT depth_levels_level CHECK (level BETWEEN 1 AND 5)
);

SELECT create_hypertable('depth_levels', 'received_at', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS candles (
    instrument_token bigint NOT NULL,
    bucket_start timestamptz NOT NULL,
    interval text NOT NULL,
    open numeric NOT NULL,
    high numeric NOT NULL,
    low numeric NOT NULL,
    close numeric NOT NULL,
    volume bigint NOT NULL,
    oi bigint,
    source text NOT NULL,
    PRIMARY KEY (instrument_token, interval, bucket_start, source)
);

SELECT create_hypertable('candles', 'bucket_start', if_not_exists => TRUE);

INSERT INTO scalper_meta (key, value)
VALUES ('schema_phase', '2')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
