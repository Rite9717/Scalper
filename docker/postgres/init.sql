-- Enables Timescale and a marker row. Tick and depth tables are applied by
-- `scalper db migrate` so an existing volume picks them up too.
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS scalper_meta (
    key text PRIMARY KEY,
    value text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO scalper_meta (key, value)
VALUES ('schema_phase', '1')
ON CONFLICT (key) DO NOTHING;
