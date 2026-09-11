CREATE TABLE actions (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    job_id TEXT REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_id TEXT REFERENCES attempts(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('extract_metrics', 'publish_artifacts', 'generate_report', 'send_notification', 'classify_failure', 'diagnose_or_repair', 'cleanup_failed_artifacts')),
    state TEXT NOT NULL CHECK (state IN ('pending', 'running', 'succeeded', 'failed', 'cancelled')),
    spec_json TEXT NOT NULL CHECK (json_valid(spec_json)),
    idempotency_key TEXT NOT NULL UNIQUE,
    available_at TEXT NOT NULL,
    claim_id TEXT,
    claim_owner TEXT,
    claim_expires_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK ((claim_id IS NULL AND claim_owner IS NULL AND claim_expires_at IS NULL) OR
           (claim_id IS NOT NULL AND claim_owner IS NOT NULL AND claim_expires_at IS NOT NULL)),
    CHECK ((state = 'running') = (claim_id IS NOT NULL))
);

CREATE TABLE metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    value REAL,
    document_json TEXT NOT NULL CHECK (json_valid(document_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (attempt_id, name)
);

CREATE TABLE artifacts (
    id TEXT PRIMARY KEY NOT NULL,
    attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('metrics', 'model', 'checkpoint', 'figure', 'log', 'crash_dump', 'report', 'other')),
    role_detail TEXT,
    sha256 TEXT,
    state TEXT NOT NULL DEFAULT 'published' CHECK (state IN ('published', 'removed')),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(metadata_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (attempt_id, path),
    CHECK ((role = 'other' AND role_detail IS NOT NULL) OR
           (role != 'other' AND role_detail IS NULL))
);

CREATE TABLE deliveries (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    event_id TEXT REFERENCES events(id) ON DELETE CASCADE,
    channel TEXT NOT NULL CHECK (channel IN ('telegram', 'local')),
    state TEXT NOT NULL CHECK (state IN ('pending', 'delivering', 'delivered', 'failed', 'cancelled')),
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    idempotency_key TEXT NOT NULL UNIQUE,
    available_at TEXT NOT NULL,
    claim_id TEXT,
    claim_owner TEXT,
    claim_expires_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK ((claim_id IS NULL AND claim_owner IS NULL AND claim_expires_at IS NULL) OR
           (claim_id IS NOT NULL AND claim_owner IS NOT NULL AND claim_expires_at IS NOT NULL)),
    CHECK ((state = 'delivering') = (claim_id IS NOT NULL))
);

CREATE TABLE recoveries (
    id TEXT PRIMARY KEY NOT NULL,
    attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK (state IN ('pending', 'diagnosing', 'proposed', 'approved', 'rejected', 'applying', 'succeeded', 'failed', 'cancelled')),
    diagnosis_json TEXT CHECK (diagnosis_json IS NULL OR json_valid(diagnosis_json)),
    patch_json TEXT CHECK (patch_json IS NULL OR json_valid(patch_json)),
    validation_json TEXT CHECK (validation_json IS NULL OR json_valid(validation_json)),
    decision_json TEXT CHECK (decision_json IS NULL OR json_valid(decision_json)),
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE report_runs (
    id TEXT PRIMARY KEY NOT NULL,
    generation_id TEXT NOT NULL REFERENCES generations(id) ON DELETE CASCADE,
    state TEXT NOT NULL CHECK (state IN ('pending', 'generating', 'published', 'failed', 'cancelled', 'superseded')),
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,
    context_hash TEXT NOT NULL,
    output_hash TEXT,
    error TEXT,
    idempotency_key TEXT NOT NULL UNIQUE,
    lease_id TEXT,
    lease_owner TEXT,
    lease_expires_at TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE agent_sessions (
    id TEXT PRIMARY KEY NOT NULL,
    report_run_id TEXT REFERENCES report_runs(id) ON DELETE CASCADE,
    recovery_id TEXT REFERENCES recoveries(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    external_session_id TEXT NOT NULL,
    cleanup_state TEXT NOT NULL CHECK (cleanup_state IN ('pending', 'running', 'succeeded', 'failed', 'cancelled')),
    lease_id TEXT,
    lease_owner TEXT,
    lease_expires_at TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (report_run_id IS NOT NULL OR recovery_id IS NOT NULL),
    UNIQUE (provider, external_session_id)
);

CREATE INDEX actions_pending_idx ON actions (state, available_at, created_at);
CREATE INDEX actions_lease_idx ON actions (claim_expires_at) WHERE claim_id IS NOT NULL;
CREATE INDEX deliveries_pending_idx ON deliveries (state, available_at, created_at);
CREATE INDEX deliveries_lease_idx ON deliveries (claim_expires_at) WHERE claim_id IS NOT NULL;
CREATE INDEX metrics_attempt_idx ON metrics (attempt_id, name);
CREATE INDEX artifacts_attempt_idx ON artifacts (attempt_id, state);
CREATE INDEX recoveries_attempt_idx ON recoveries (attempt_id, created_at);
CREATE INDEX report_runs_generation_idx ON report_runs (generation_id, created_at);
CREATE INDEX agent_sessions_cleanup_idx ON agent_sessions (cleanup_state, lease_expires_at);
