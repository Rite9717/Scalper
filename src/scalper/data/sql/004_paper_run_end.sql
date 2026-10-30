-- How a paper run ended. Added after 003 was already applied, so this is a new migration.

CREATE TABLE IF NOT EXISTS paper_run_ends (
    run_id uuid PRIMARY KEY REFERENCES paper_runs (run_id),
    ended_at timestamptz NOT NULL,
    reason text NOT NULL,
    final_position integer NOT NULL,
    kill_engaged boolean NOT NULL
);
