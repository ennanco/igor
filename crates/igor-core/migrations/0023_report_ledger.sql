ALTER TABLE report_runs ADD COLUMN action_id TEXT REFERENCES actions(id) ON DELETE RESTRICT;
ALTER TABLE report_runs ADD COLUMN project_id TEXT REFERENCES projects(id) ON DELETE RESTRICT;
ALTER TABLE report_runs ADD COLUMN family_id TEXT REFERENCES families(id) ON DELETE RESTRICT;
ALTER TABLE report_runs ADD COLUMN context_json TEXT CHECK (context_json IS NULL OR json_valid(context_json));
ALTER TABLE report_runs ADD COLUMN spec_json TEXT CHECK (spec_json IS NULL OR json_valid(spec_json));
ALTER TABLE report_runs ADD COLUMN output_path TEXT;
ALTER TABLE report_runs ADD COLUMN output_size_bytes INTEGER CHECK (output_size_bytes IS NULL OR output_size_bytes >= 0);
ALTER TABLE report_runs ADD COLUMN previous_output_hash TEXT;
ALTER TABLE report_runs ADD COLUMN renderer_version INTEGER;
ALTER TABLE report_runs ADD COLUMN initial_lease_id TEXT;
ALTER TABLE report_runs ADD COLUMN publication_phase TEXT CHECK (publication_phase IS NULL OR publication_phase IN ('context', 'prepared', 'published'));
ALTER TABLE report_runs ADD COLUMN prepared_at TEXT;
ALTER TABLE report_runs ADD COLUMN published_at TEXT;

-- Legacy rows retain unknown context/output provenance; no hashes are fabricated.
CREATE INDEX report_runs_action_idx ON report_runs(action_id, state);
CREATE INDEX report_runs_output_idx ON report_runs(project_id, output_path, state);
CREATE UNIQUE INDEX report_runs_prepared_idx ON report_runs(action_id)
    WHERE action_id IS NOT NULL AND state = 'generating' AND output_hash IS NOT NULL;

CREATE TRIGGER report_runs_insert_guard BEFORE INSERT ON report_runs
WHEN NEW.action_id IS NOT NULL AND (
    NEW.state IS NOT 'generating' OR NEW.publication_phase IS NOT 'context' OR NEW.output_hash IS NOT NULL OR
    NEW.output_size_bytes IS NOT NULL OR NEW.previous_output_hash IS NOT NULL OR
    NEW.prepared_at IS NOT NULL OR NEW.published_at IS NOT NULL OR
    NEW.context_json IS NULL OR NEW.spec_json IS NULL OR NEW.output_path IS NULL OR
    NEW.renderer_version IS NOT 1 OR NEW.initial_lease_id IS NOT NEW.lease_id OR
    NEW.project_id IS NULL OR NEW.family_id IS NULL OR NEW.lease_id IS NULL OR NEW.lease_owner IS NULL OR
    NEW.lease_expires_at IS NULL OR length(NEW.context_hash) != 71 OR substr(NEW.context_hash,1,7) != 'sha256:' OR
    substr(NEW.context_hash,8) GLOB '*[^0-9a-f]*' OR
    json_extract(NEW.context_json, '$.schema_version') IS NOT 1 OR
    json_extract(NEW.context_json, '$.family.id') IS NOT NEW.family_id OR
    json_extract(NEW.context_json, '$.family.project_id') IS NOT NEW.project_id OR
    json_extract(NEW.context_json, '$.generation.identity.id') IS NOT NEW.generation_id OR
    json_extract(NEW.spec_json, '$.family_id') IS NOT NEW.family_id OR
    json_extract(NEW.spec_json, '$.project.id') IS NOT NEW.project_id OR
    json_extract(NEW.spec_json, '$.generation.id') IS NOT NEW.generation_id OR
    NOT EXISTS (SELECT 1 FROM actions a WHERE a.id = NEW.action_id AND a.kind = 'generate_report'
        AND a.project_id = NEW.project_id AND a.state = 'running' AND a.claim_id = NEW.lease_id
        AND a.claim_owner = NEW.lease_owner AND a.claim_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now')))
BEGIN SELECT RAISE(ABORT, 'invalid report run preparation'); END;

CREATE TRIGGER report_runs_identity_guard BEFORE UPDATE ON report_runs
WHEN (OLD.action_id IS NULL AND NEW.action_id IS NOT NULL) OR (OLD.action_id IS NOT NULL AND (
    NEW.id IS NOT OLD.id OR NEW.action_id IS NOT OLD.action_id OR NEW.project_id IS NOT OLD.project_id OR
    NEW.family_id IS NOT OLD.family_id OR NEW.generation_id IS NOT OLD.generation_id OR
    NEW.provider IS NOT OLD.provider OR NEW.model IS NOT OLD.model OR NEW.prompt_hash IS NOT OLD.prompt_hash OR
    NEW.context_hash IS NOT OLD.context_hash OR NEW.context_json IS NOT OLD.context_json OR
    NEW.spec_json IS NOT OLD.spec_json OR NEW.output_path IS NOT OLD.output_path OR
    NEW.renderer_version IS NOT OLD.renderer_version OR NEW.initial_lease_id IS NOT OLD.initial_lease_id OR
    NEW.idempotency_key IS NOT OLD.idempotency_key OR NEW.created_at IS NOT OLD.created_at OR
    (OLD.output_hash IS NOT NULL AND (NEW.output_hash IS NOT OLD.output_hash OR
        NEW.output_size_bytes IS NOT OLD.output_size_bytes OR NEW.previous_output_hash IS NOT OLD.previous_output_hash OR
        NEW.prepared_at IS NOT OLD.prepared_at)) OR
    (OLD.published_at IS NOT NULL AND NEW.published_at IS NOT OLD.published_at)))
BEGIN SELECT RAISE(ABORT, 'immutable report run evidence'); END;

CREATE TRIGGER report_runs_update_guard BEFORE UPDATE ON report_runs
WHEN OLD.action_id IS NOT NULL AND (
    NOT (NEW.state = OLD.state OR (OLD.state = 'generating' AND NEW.state IN ('published','failed','cancelled')) OR
        (OLD.state = 'published' AND NEW.state = 'superseded')) OR
    (OLD.state IN ('failed','cancelled','superseded') AND (NEW.lease_id IS NOT OLD.lease_id OR
        NEW.lease_owner IS NOT OLD.lease_owner OR NEW.lease_expires_at IS NOT OLD.lease_expires_at OR
        NEW.error IS NOT OLD.error OR NEW.updated_at IS NOT OLD.updated_at OR
        NEW.publication_phase IS NOT OLD.publication_phase OR NEW.output_hash IS NOT OLD.output_hash OR
        NEW.output_size_bytes IS NOT OLD.output_size_bytes OR NEW.previous_output_hash IS NOT OLD.previous_output_hash OR
        NEW.prepared_at IS NOT OLD.prepared_at OR NEW.published_at IS NOT OLD.published_at)) OR
    (OLD.state = 'published' AND (NEW.lease_id IS NOT OLD.lease_id OR NEW.lease_owner IS NOT OLD.lease_owner OR
        NEW.lease_expires_at IS NOT OLD.lease_expires_at OR NEW.error IS NOT OLD.error)) OR
    NEW.publication_phase IS NULL OR
    (NEW.publication_phase = 'context' AND (NEW.output_hash IS NOT NULL OR NEW.output_size_bytes IS NOT NULL OR
        NEW.previous_output_hash IS NOT NULL OR NEW.prepared_at IS NOT NULL)) OR
    (NEW.publication_phase != 'published' AND NEW.published_at IS NOT NULL) OR
    NOT (NEW.publication_phase = OLD.publication_phase OR
        (OLD.publication_phase = 'context' AND NEW.publication_phase = 'prepared') OR
        (OLD.publication_phase = 'prepared' AND NEW.publication_phase = 'published')) OR
    (NEW.publication_phase != 'context' AND (NEW.output_hash IS NULL OR NEW.output_size_bytes IS NULL OR NEW.prepared_at IS NULL)) OR
    (NEW.output_hash IS NOT NULL AND (length(NEW.output_hash) != 71 OR substr(NEW.output_hash,1,7) != 'sha256:' OR
        substr(NEW.output_hash,8) GLOB '*[^0-9a-f]*')) OR
    (NEW.previous_output_hash IS NOT NULL AND (length(NEW.previous_output_hash) != 71 OR substr(NEW.previous_output_hash,1,7) != 'sha256:' OR
        substr(NEW.previous_output_hash,8) GLOB '*[^0-9a-f]*')) OR
    (NEW.publication_phase = 'published' AND (NEW.state NOT IN ('published','superseded') OR NEW.published_at IS NULL)) OR
    (NEW.state IN ('published','superseded') AND NEW.publication_phase != 'published') OR
    (NEW.lease_id IS NOT OLD.lease_id AND NOT EXISTS (SELECT 1 FROM actions a WHERE a.id = OLD.action_id
        AND a.state = 'running' AND a.claim_id = NEW.lease_id AND a.claim_owner = NEW.lease_owner
        AND a.claim_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))))
BEGIN SELECT RAISE(ABORT, 'invalid report run transition'); END;

CREATE TRIGGER report_runs_no_delete BEFORE DELETE ON report_runs WHEN OLD.action_id IS NOT NULL
BEGIN SELECT RAISE(ABORT, 'report run evidence is durable'); END;

CREATE TRIGGER report_action_success_guard BEFORE UPDATE OF state ON actions
WHEN NEW.kind = 'generate_report' AND OLD.state != 'succeeded' AND NEW.state = 'succeeded' AND
    NOT EXISTS (SELECT 1 FROM report_runs r WHERE r.action_id = NEW.id AND r.project_id = NEW.project_id
        AND r.state = 'published' AND r.publication_phase = 'published' AND r.lease_id = OLD.claim_id)
BEGIN SELECT RAISE(ABORT, 'report success requires a published run'); END;
