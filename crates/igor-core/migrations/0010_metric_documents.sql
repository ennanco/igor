CREATE TABLE metric_documents (
    attempt_id TEXT PRIMARY KEY NOT NULL REFERENCES attempts(id),
    version INTEGER NOT NULL CHECK (version = 1),
    document_json TEXT NOT NULL CHECK (json_valid(document_json) AND json_extract(document_json, '$.schema_version') = version),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
