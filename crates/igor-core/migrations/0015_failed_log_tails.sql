CREATE TABLE failed_log_tails (
    attempt_id TEXT PRIMARY KEY REFERENCES attempts(id) ON DELETE CASCADE,
    stdout BLOB NOT NULL,
    stderr BLOB NOT NULL,
    limit_kb INTEGER NOT NULL CHECK (limit_kb BETWEEN 0 AND 16384),
    complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
    captured_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
