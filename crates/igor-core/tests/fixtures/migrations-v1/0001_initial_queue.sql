CREATE TABLE projects (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL CHECK (length(trim(name)) > 0),
    root_path TEXT NOT NULL,
    config_path TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE families (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name TEXT NOT NULL CHECK (length(trim(name)) > 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (project_id, name),
    UNIQUE (id, project_id)
);

CREATE TABLE generations (
    id TEXT PRIMARY KEY NOT NULL,
    family_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    generation_number INTEGER NOT NULL CHECK (generation_number > 0),
    source_revision TEXT NOT NULL CHECK (length(trim(source_revision)) > 0),
    protocol_digest TEXT NOT NULL CHECK (length(trim(protocol_digest)) > 0),
    spec_json TEXT NOT NULL CHECK (json_valid(spec_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (family_id, generation_number),
    UNIQUE (family_id, source_revision, protocol_digest),
    UNIQUE (id, family_id, project_id),
    FOREIGN KEY (family_id, project_id) REFERENCES families(id, project_id) ON DELETE CASCADE
);

CREATE TABLE jobs (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    family_id TEXT,
    generation_id TEXT,
    name TEXT NOT NULL CHECK (length(trim(name)) > 0),
    state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled', 'lost', 'superseded')),
    priority INTEGER NOT NULL DEFAULT 0,
    submission_order INTEGER NOT NULL,
    spec_json TEXT NOT NULL CHECK (json_valid(spec_json)),
    claim_id TEXT,
    claim_owner TEXT,
    claim_expires_at TEXT,
    submitted_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK ((family_id IS NULL) = (generation_id IS NULL)),
    CHECK ((claim_id IS NULL AND claim_owner IS NULL AND claim_expires_at IS NULL) OR
           (claim_id IS NOT NULL AND claim_owner IS NOT NULL AND claim_expires_at IS NOT NULL)),
    CHECK ((state = 'running') = (claim_id IS NOT NULL)),
    UNIQUE (project_id, submission_order),
    UNIQUE (id, project_id),
    FOREIGN KEY (family_id, project_id) REFERENCES families(id, project_id) ON DELETE RESTRICT,
    FOREIGN KEY (generation_id, family_id, project_id) REFERENCES generations(id, family_id, project_id) ON DELETE RESTRICT
);

CREATE TABLE attempts (
    id TEXT PRIMARY KEY NOT NULL,
    job_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    state TEXT NOT NULL CHECK (state IN ('pending', 'starting', 'running', 'succeeded', 'failed', 'cancelled', 'lost')),
    spec_json TEXT NOT NULL CHECK (json_valid(spec_json)),
    started_at TEXT,
    finished_at TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (job_id, sequence),
    UNIQUE (id, job_id, project_id),
    FOREIGN KEY (job_id, project_id) REFERENCES jobs(id, project_id) ON DELETE CASCADE
);

CREATE INDEX jobs_queue_order_idx ON jobs (state, priority DESC, submission_order ASC);
CREATE INDEX jobs_generation_idx ON jobs (generation_id, state);
CREATE INDEX attempts_job_lookup_idx ON attempts (job_id, sequence DESC);
