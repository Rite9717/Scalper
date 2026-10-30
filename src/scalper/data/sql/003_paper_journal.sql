-- Paper-loop journal. One run id ties signals, decisions, orders, fills, and exits
-- together so a paper session can be compared with a backtest trade by trade.

CREATE TABLE IF NOT EXISTS paper_runs (
    run_id uuid PRIMARY KEY,
    started_at timestamptz NOT NULL,
    strategy text NOT NULL,
    tradingsymbol text NOT NULL,
    limits jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_signals (
    id bigserial PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES paper_runs (run_id),
    at timestamptz NOT NULL,
    side text,
    limit_price numeric,
    stop_price numeric,
    target_price numeric,
    time_stop_bars integer,
    regime text,
    accepted boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_decisions (
    id bigserial PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES paper_runs (run_id),
    at timestamptz NOT NULL,
    intent text NOT NULL,
    allowed boolean NOT NULL,
    action text NOT NULL,
    reason text NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_orders (
    id bigserial PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES paper_runs (run_id),
    at timestamptz NOT NULL,
    tag text NOT NULL,
    broker_order_id text,
    side text NOT NULL,
    limit_price numeric NOT NULL,
    quantity integer NOT NULL,
    intent text NOT NULL,
    status text NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_fills (
    id bigserial PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES paper_runs (run_id),
    at timestamptz NOT NULL,
    tag text NOT NULL,
    price numeric NOT NULL,
    quantity integer NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_exits (
    id bigserial PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES paper_runs (run_id),
    at timestamptz NOT NULL,
    reason text NOT NULL,
    trigger_at timestamptz,
    trigger_price numeric,
    fill_at timestamptz,
    fill_price numeric,
    quantity integer
);
