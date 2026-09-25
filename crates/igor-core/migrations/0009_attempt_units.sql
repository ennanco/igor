CREATE TABLE attempt_units (
    attempt_id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    unit_name TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL DEFAULT 'reserved'
        CHECK (state IN ('reserved', 'started', 'finished', 'removed')),
    stdout_path TEXT NOT NULL,
    stderr_path TEXT NOT NULL,
    invocation_id TEXT,
    result TEXT CHECK (result IN ('succeeded', 'failed', 'cancelled', 'lost')),
    exit_code INTEGER,
    term_signal INTEGER,
    error TEXT,
    finished_at TEXT,
    removed_at TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    FOREIGN KEY (attempt_id, job_id, project_id)
        REFERENCES attempts(id, job_id, project_id) ON DELETE CASCADE
);

CREATE INDEX attempt_units_state_idx ON attempt_units (state, updated_at);
