CREATE TABLE events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
    job_id TEXT,
    attempt_id TEXT,
    kind TEXT NOT NULL CHECK (kind IN ('job_submitted', 'job_state_changed', 'attempt_created', 'attempt_state_changed', 'action_state_changed', 'delivery_state_changed', 'recovery_state_changed', 'report_state_changed', 'cleanup_state_changed', 'resource_reserved', 'resource_released', 'artifact_registered')),
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    occurred_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (attempt_id IS NULL OR job_id IS NOT NULL),
    FOREIGN KEY (job_id, project_id) REFERENCES jobs(id, project_id) ON DELETE RESTRICT,
    FOREIGN KEY (attempt_id, job_id, project_id) REFERENCES attempts(id, job_id, project_id) ON DELETE RESTRICT
);

CREATE TRIGGER events_no_update BEFORE UPDATE ON events
BEGIN SELECT RAISE(ABORT, 'events are append-only'); END;
CREATE TRIGGER events_no_delete BEFORE DELETE ON events
BEGIN SELECT RAISE(ABORT, 'events are append-only'); END;

CREATE TABLE resources (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE CHECK (length(trim(name)) > 0),
    kind TEXT NOT NULL CHECK (kind IN ('host', 'cpu', 'memory', 'gpu', 'named')),
    capacity INTEGER NOT NULL DEFAULT 1 CHECK (capacity > 0),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(metadata_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE resource_leases (
    id TEXT PRIMARY KEY NOT NULL,
    resource_id TEXT NOT NULL REFERENCES resources(id) ON DELETE CASCADE,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    owner TEXT NOT NULL CHECK (length(trim(owner)) > 0),
    quantity INTEGER NOT NULL DEFAULT 1 CHECK (quantity > 0),
    expires_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (resource_id, job_id)
);

CREATE INDEX events_project_stream_idx ON events (project_id, sequence);
CREATE INDEX events_job_stream_idx ON events (job_id, sequence) WHERE job_id IS NOT NULL;
CREATE INDEX events_attempt_stream_idx ON events (attempt_id, sequence) WHERE attempt_id IS NOT NULL;
CREATE INDEX resource_leases_expiry_idx ON resource_leases (expires_at);
CREATE INDEX resource_leases_resource_idx ON resource_leases (resource_id, expires_at);
CREATE INDEX resource_leases_job_idx ON resource_leases (job_id);
