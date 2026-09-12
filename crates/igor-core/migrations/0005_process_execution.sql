CREATE TABLE attempt_processes (
    attempt_id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    pid INTEGER NOT NULL CHECK (pid > 0),
    process_group_id INTEGER NOT NULL CHECK (process_group_id > 0),
    process_start_ticks INTEGER NOT NULL CHECK (process_start_ticks >= 0),
    stdout_path TEXT NOT NULL,
    stderr_path TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    exit_code INTEGER,
    term_signal INTEGER,
    error TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (exit_code IS NULL OR term_signal IS NULL),
    FOREIGN KEY (attempt_id, job_id, project_id)
        REFERENCES attempts(id, job_id, project_id) ON DELETE CASCADE
);

CREATE INDEX attempt_processes_heartbeat_idx ON attempt_processes (heartbeat_at);
