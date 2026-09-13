ALTER TABLE jobs ADD COLUMN cancel_requested_at TEXT;
ALTER TABLE jobs ADD COLUMN cancel_grace_seconds INTEGER
    CHECK (cancel_grace_seconds IS NULL OR cancel_grace_seconds >= 0);

CREATE INDEX jobs_cancellation_idx ON jobs (cancel_requested_at)
WHERE cancel_requested_at IS NOT NULL;
