ALTER TABLE artifacts RENAME TO artifacts_previous;

CREATE TABLE artifacts (
    id TEXT PRIMARY KEY NOT NULL,
    attempt_id TEXT NOT NULL REFERENCES attempts(id) ON DELETE CASCADE,
    path TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('metrics', 'model', 'checkpoint', 'figure', 'log', 'crash_dump', 'report', 'other')),
    role_detail TEXT,
    sha256 TEXT,
    size_bytes INTEGER CHECK (size_bytes IS NULL OR size_bytes >= 0),
    state TEXT NOT NULL DEFAULT 'registered' CHECK (state IN ('registered', 'staged', 'published', 'removed')),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(metadata_json)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (attempt_id, path),
    CHECK ((role = 'other' AND role_detail IS NOT NULL) OR
           (role != 'other' AND role_detail IS NULL)),
    CHECK (state NOT IN ('registered', 'staged') OR (size_bytes IS NOT NULL AND sha256 IS NOT NULL))
);

INSERT INTO artifacts
    (id, attempt_id, path, role, role_detail, sha256, size_bytes, state, metadata_json, created_at, updated_at)
SELECT id, attempt_id, path, role, role_detail, sha256, NULL, state, metadata_json, created_at, created_at
FROM artifacts_previous;

DROP TABLE artifacts_previous;
CREATE INDEX artifacts_attempt_idx ON artifacts (attempt_id, state);
