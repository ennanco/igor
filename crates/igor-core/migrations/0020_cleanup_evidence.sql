CREATE TABLE failed_cleanup_intents (
    artifact_id TEXT PRIMARY KEY NOT NULL REFERENCES artifacts(id),
    schema_version INTEGER NOT NULL DEFAULT 1 CHECK(schema_version = 1),
    identity_json TEXT NOT NULL CHECK(json_valid(identity_json)),
    state TEXT NOT NULL DEFAULT 'prepared' CHECK(state IN ('prepared', 'completed')),
    decided_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    prepared_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE TRIGGER failed_cleanup_identity_insert BEFORE INSERT ON failed_cleanup_intents
WHEN NEW.state != 'prepared' OR EXISTS (SELECT 1 FROM failed_cleanup_intents WHERE artifact_id = NEW.artifact_id)
BEGIN SELECT RAISE(ABORT, 'failed cleanup intent must be new and prepared'); END;
CREATE TRIGGER failed_cleanup_identity_update BEFORE UPDATE ON failed_cleanup_intents
WHEN NEW.artifact_id IS NOT OLD.artifact_id OR NEW.schema_version IS NOT OLD.schema_version
 OR NEW.identity_json IS NOT OLD.identity_json OR NEW.decided_at IS NOT OLD.decided_at
 OR NEW.prepared_at IS NOT OLD.prepared_at OR (OLD.state = 'completed' AND NEW.state != OLD.state)
BEGIN SELECT RAISE(ABORT, 'immutable failed cleanup evidence'); END;
CREATE TRIGGER failed_cleanup_identity_delete BEFORE DELETE ON failed_cleanup_intents
BEGIN SELECT RAISE(ABORT, 'immutable failed cleanup evidence'); END;

CREATE TABLE cleanup_tombstones (
    operation TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK(schema_version = 1),
    reason TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK(outcome IN ('removed', 'retained', 'raw_retired')),
    identity_json TEXT NOT NULL CHECK(json_valid(identity_json)),
    decided_at TEXT NOT NULL,
    completed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    action_id TEXT REFERENCES actions(id),
    PRIMARY KEY(operation, subject_id)
);
CREATE TRIGGER cleanup_tombstone_update BEFORE UPDATE ON cleanup_tombstones
BEGIN SELECT RAISE(ABORT, 'immutable cleanup tombstone'); END;
CREATE TRIGGER cleanup_tombstone_insert BEFORE INSERT ON cleanup_tombstones
WHEN EXISTS (SELECT 1 FROM cleanup_tombstones WHERE operation = NEW.operation AND subject_id = NEW.subject_id)
BEGIN SELECT RAISE(ABORT, 'immutable cleanup tombstone'); END;
CREATE TRIGGER cleanup_tombstone_delete BEFORE DELETE ON cleanup_tombstones
BEGIN SELECT RAISE(ABORT, 'immutable cleanup tombstone'); END;
