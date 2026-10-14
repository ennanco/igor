CREATE TABLE owned_output_namespaces (
    attempt_id TEXT PRIMARY KEY REFERENCES attempts(id),
    identity_json TEXT NOT NULL CHECK(json_valid(identity_json)
        AND COALESCE(json_extract(identity_json, '$.attempt_id') = attempt_id, 0)
        AND COALESCE(json_extract(identity_json, '$.created_empty') = 1, 0)
        AND COALESCE(json_type(identity_json, '$.root') = 'text', 0)
        AND COALESCE(json_type(identity_json, '$.project_root') = 'text', 0)
        AND COALESCE(json_type(identity_json, '$.stdout') = 'text', 0)
        AND COALESCE(json_type(identity_json, '$.stderr') = 'text', 0)
        AND COALESCE(json_type(identity_json, '$.dev') = 'integer', 0)
        AND COALESCE(json_type(identity_json, '$.ino') = 'integer', 0)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE TRIGGER owned_namespace_immutable BEFORE UPDATE ON owned_output_namespaces
BEGIN SELECT RAISE(ABORT, 'immutable owned namespace'); END;
CREATE TRIGGER owned_namespace_preserved BEFORE DELETE ON owned_output_namespaces
BEGIN SELECT RAISE(ABORT, 'preserved owned namespace'); END;
CREATE TABLE owned_output_manifest (
    id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL REFERENCES owned_output_namespaces(attempt_id),
    identity_json TEXT NOT NULL CHECK(json_valid(identity_json)
        AND COALESCE(json_extract(identity_json, '$.namespace.attempt_id') = attempt_id, 0)
        AND COALESCE(json_type(identity_json, '$.name') = 'text', 0)
        AND COALESCE(json_type(identity_json, '$.size') = 'integer'
            AND json_extract(identity_json, '$.size') >= 0, 0)
        AND COALESCE(length(json_extract(identity_json, '$.sha256')) = 64, 0)
        AND COALESCE(json_type(identity_json, '$.dev') = 'integer', 0)
        AND COALESCE(json_type(identity_json, '$.ino') = 'integer', 0)),
    state TEXT NOT NULL CHECK(state IN ('prepared', 'completed')),
    decided_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE INDEX owned_output_attempt ON owned_output_manifest(attempt_id, state);
CREATE TABLE owned_output_discovery (
    attempt_id TEXT PRIMARY KEY REFERENCES owned_output_namespaces(attempt_id),
    complete INTEGER NOT NULL CHECK(complete = 1),
    identity_json TEXT NOT NULL CHECK(json_valid(identity_json))
);
CREATE TRIGGER owned_discovery_immutable BEFORE UPDATE ON owned_output_discovery
BEGIN SELECT RAISE(ABORT, 'immutable completed discovery'); END;
CREATE TRIGGER owned_discovery_preserved BEFORE DELETE ON owned_output_discovery
BEGIN SELECT RAISE(ABORT, 'preserved completed discovery'); END;
CREATE TRIGGER owned_output_identity_immutable
BEFORE UPDATE OF id, attempt_id, identity_json, decided_at ON owned_output_manifest
BEGIN SELECT RAISE(ABORT, 'immutable owned output'); END;
CREATE TRIGGER owned_output_completed BEFORE UPDATE OF state ON owned_output_manifest
WHEN OLD.state = 'completed' AND NEW.state != 'completed'
BEGIN SELECT RAISE(ABORT, 'completed owned output'); END;
CREATE TRIGGER owned_output_preserved BEFORE DELETE ON owned_output_manifest
BEGIN SELECT RAISE(ABORT, 'preserved owned output'); END;
