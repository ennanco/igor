CREATE TABLE repair_worktrees (
    recovery_id TEXT PRIMARY KEY NOT NULL REFERENCES recoveries(id),
    project_id TEXT NOT NULL REFERENCES projects(id),
    path TEXT NOT NULL UNIQUE,
    root_path TEXT NOT NULL,
    repository_path TEXT NOT NULL,
    common_dir TEXT NOT NULL,
    admin_dir TEXT NOT NULL UNIQUE,
    identity_json TEXT NOT NULL CHECK (json_valid(identity_json)),
    delete_on_release INTEGER NOT NULL CHECK (delete_on_release IN (0, 1)),
    state TEXT NOT NULL DEFAULT 'registered' CHECK (state IN ('registered', 'released', 'cleaning', 'removed', 'retained')),
    patch BLOB,
    patch_sha256 TEXT,
    records_json TEXT,
    released_at TEXT,
    CHECK ((state = 'registered' AND patch IS NULL AND patch_sha256 IS NULL AND records_json IS NULL AND released_at IS NULL) OR
           (state != 'registered' AND typeof(patch) = 'blob' AND patch_sha256 IS NOT NULL AND length(patch_sha256) = 64 AND
            patch_sha256 NOT GLOB '*[^0-9a-f]*' AND
            released_at IS NOT NULL AND records_json IS NOT NULL AND json_valid(records_json) AND
            COALESCE(json_type(records_json, '$.schema_version') = 'integer' AND json_extract(records_json, '$.schema_version') = 1, 0) AND
            COALESCE(json_type(records_json, '$.diagnosis.schema_version') = 'integer' AND json_extract(records_json, '$.diagnosis.schema_version') = 1, 0) AND
            COALESCE(json_type(records_json, '$.diagnosis.content') = 'object', 0) AND
            COALESCE(json_type(records_json, '$.validation_results.schema_version') = 'integer' AND json_extract(records_json, '$.validation_results.schema_version') = 1, 0) AND
            COALESCE(json_type(records_json, '$.validation_results.results') = 'array', 0) AND
            COALESCE(json_type(records_json, '$.changed_paths') = 'array', 0) AND
            COALESCE(json_type(records_json, '$.candidate') = 'null' OR
             (json_type(records_json, '$.candidate') = 'object' AND json_type(records_json, '$.candidate.schema_version') = 'integer' AND json_extract(records_json, '$.candidate.schema_version') = 1), 0)))
);
CREATE INDEX repair_worktrees_cleanup_idx ON repair_worktrees(state, recovery_id);

CREATE TRIGGER repair_worktrees_insert_guard BEFORE INSERT ON repair_worktrees
WHEN NEW.state != 'registered' OR EXISTS (
    SELECT 1 FROM repair_worktrees WHERE recovery_id = NEW.recovery_id OR path = NEW.path OR admin_dir = NEW.admin_dir) OR NOT EXISTS (
    SELECT 1 FROM recoveries r JOIN attempts a ON a.id = r.attempt_id
    WHERE r.id = NEW.recovery_id AND a.project_id = NEW.project_id)
BEGIN SELECT RAISE(ABORT, 'invalid repair worktree registration'); END;

CREATE TRIGGER repair_worktrees_update_guard BEFORE UPDATE ON repair_worktrees
WHEN NEW.recovery_id != OLD.recovery_id OR NEW.project_id != OLD.project_id OR
     NEW.path != OLD.path OR NEW.root_path != OLD.root_path OR
     NEW.repository_path != OLD.repository_path OR NEW.common_dir != OLD.common_dir OR
     NEW.admin_dir != OLD.admin_dir OR NEW.identity_json != OLD.identity_json OR
     NEW.delete_on_release != OLD.delete_on_release OR
     (NEW.state IN ('cleaning', 'removed') AND NEW.delete_on_release != 1) OR
     (NEW.state = 'retained' AND NEW.delete_on_release != 0) OR
     (OLD.state != 'registered' AND (NEW.patch IS NOT OLD.patch OR NEW.patch_sha256 IS NOT OLD.patch_sha256 OR
      NEW.records_json IS NOT OLD.records_json OR NEW.released_at IS NOT OLD.released_at)) OR
     NOT (NEW.state = OLD.state OR (OLD.state = 'registered' AND NEW.state = 'released') OR
          (OLD.state = 'released' AND NEW.state IN ('cleaning', 'retained')) OR
          (OLD.state = 'cleaning' AND NEW.state = 'removed'))
BEGIN SELECT RAISE(ABORT, 'immutable repair worktree registration or release'); END;

CREATE TRIGGER repair_worktrees_release_guard BEFORE UPDATE ON repair_worktrees
WHEN NEW.state != 'registered' AND (
    NOT EXISTS (SELECT 1 FROM recoveries WHERE id = NEW.recovery_id AND
                state IN ('succeeded', 'failed', 'rejected', 'cancelled')) OR
    EXISTS (SELECT 1 FROM actions a JOIN recoveries r ON r.id = NEW.recovery_id
            JOIN attempts t ON t.id = r.attempt_id
            WHERE a.kind = 'diagnose_or_repair' AND a.state IN ('pending', 'running') AND
                  (a.attempt_id = t.id OR (a.attempt_id IS NULL AND
                   (a.job_id = t.job_id OR (a.job_id IS NULL AND a.project_id = t.project_id))))) OR
    EXISTS (SELECT 1 FROM agent_sessions WHERE recovery_id = NEW.recovery_id AND
            (cleanup_state IN ('pending', 'running') OR lease_id IS NOT NULL OR lease_owner IS NOT NULL OR lease_expires_at IS NOT NULL)))
BEGIN SELECT RAISE(ABORT, 'repair worktree still needed'); END;

CREATE TRIGGER repair_worktrees_no_delete BEFORE DELETE ON repair_worktrees
BEGIN SELECT RAISE(ABORT, 'repair worktree evidence is durable'); END;

CREATE TRIGGER repair_worktrees_recovery_guard BEFORE UPDATE ON recoveries
WHEN EXISTS (SELECT 1 FROM repair_worktrees WHERE recovery_id = OLD.id AND state != 'registered') AND
     (NEW.id != OLD.id OR NEW.attempt_id != OLD.attempt_id OR NEW.state NOT IN ('succeeded', 'failed', 'rejected', 'cancelled'))
BEGIN SELECT RAISE(ABORT, 'released recovery cannot be reactivated'); END;

CREATE TRIGGER repair_worktrees_action_insert_guard BEFORE INSERT ON actions
WHEN NEW.kind = 'diagnose_or_repair' AND NEW.state IN ('pending', 'running') AND EXISTS (
    SELECT 1 FROM repair_worktrees w JOIN recoveries r ON r.id = w.recovery_id JOIN attempts t ON t.id = r.attempt_id
    WHERE w.state != 'registered' AND (NEW.attempt_id = t.id OR (NEW.attempt_id IS NULL AND
          (NEW.job_id = t.job_id OR (NEW.job_id IS NULL AND NEW.project_id = t.project_id)))))
BEGIN SELECT RAISE(ABORT, 'released worktree cannot be claimed'); END;
CREATE TRIGGER repair_worktrees_action_update_guard BEFORE UPDATE ON actions
WHEN NEW.kind = 'diagnose_or_repair' AND NEW.state IN ('pending', 'running') AND EXISTS (
    SELECT 1 FROM repair_worktrees w JOIN recoveries r ON r.id = w.recovery_id JOIN attempts t ON t.id = r.attempt_id
    WHERE w.state != 'registered' AND (NEW.attempt_id = t.id OR (NEW.attempt_id IS NULL AND
          (NEW.job_id = t.job_id OR (NEW.job_id IS NULL AND NEW.project_id = t.project_id)))))
BEGIN SELECT RAISE(ABORT, 'released worktree cannot be claimed'); END;
CREATE TRIGGER repair_worktrees_session_insert_guard BEFORE INSERT ON agent_sessions
WHEN (NEW.cleanup_state IN ('pending', 'running') OR NEW.lease_id IS NOT NULL OR NEW.lease_owner IS NOT NULL OR NEW.lease_expires_at IS NOT NULL) AND EXISTS (
    SELECT 1 FROM repair_worktrees WHERE recovery_id = NEW.recovery_id AND state != 'registered')
BEGIN SELECT RAISE(ABORT, 'released worktree cannot be claimed'); END;
CREATE TRIGGER repair_worktrees_session_update_guard BEFORE UPDATE ON agent_sessions
WHEN (NEW.cleanup_state IN ('pending', 'running') OR NEW.lease_id IS NOT NULL OR NEW.lease_owner IS NOT NULL OR NEW.lease_expires_at IS NOT NULL) AND EXISTS (
    SELECT 1 FROM repair_worktrees WHERE recovery_id = NEW.recovery_id AND state != 'registered')
BEGIN SELECT RAISE(ABORT, 'released worktree cannot be claimed'); END;
