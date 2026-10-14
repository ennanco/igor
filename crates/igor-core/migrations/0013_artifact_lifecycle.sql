ALTER TABLE artifacts ADD COLUMN lifecycle_state TEXT NOT NULL DEFAULT 'running'
    CHECK (lifecycle_state IN ('running', 'failed', 'published', 'retained', 'superseded', 'removed'));

UPDATE artifacts SET lifecycle_state = CASE
    WHEN state = 'removed' THEN 'removed'
    WHEN state = 'published' THEN 'published'
    WHEN EXISTS (
        SELECT 1 FROM attempts WHERE attempts.id = artifacts.attempt_id
            AND attempts.state IN ('failed', 'cancelled', 'lost')
    ) OR EXISTS (
        SELECT 1 FROM metric_documents WHERE metric_documents.attempt_id = artifacts.attempt_id
            AND json_extract(metric_documents.document_json, '$.status') IN ('partial', 'failed')
    ) OR EXISTS (
        SELECT 1 FROM actions WHERE actions.attempt_id = artifacts.attempt_id
            AND actions.kind = 'extract_metrics' AND actions.state = 'failed'
    ) THEN 'failed'
    ELSE 'running'
END;

CREATE INDEX artifacts_lifecycle_idx ON artifacts (lifecycle_state, attempt_id);

CREATE TRIGGER artifacts_lifecycle_insert BEFORE INSERT ON artifacts
WHEN (NEW.state = 'removed') != (NEW.lifecycle_state = 'removed')
    OR (NEW.state = 'published' AND NEW.lifecycle_state NOT IN ('published', 'retained', 'superseded'))
    OR (NEW.state IN ('registered', 'staged') AND NEW.lifecycle_state = 'published')
BEGIN
    SELECT RAISE(ABORT, 'inconsistent artifact publication and lifecycle states');
END;

CREATE TRIGGER artifacts_lifecycle_update BEFORE UPDATE OF state, lifecycle_state ON artifacts
WHEN (NEW.state = 'removed') != (NEW.lifecycle_state = 'removed')
    OR (NEW.state = 'published' AND NEW.lifecycle_state NOT IN ('published', 'retained', 'superseded'))
    OR (NEW.state IN ('registered', 'staged') AND NEW.lifecycle_state = 'published')
BEGIN
    SELECT RAISE(ABORT, 'inconsistent artifact publication and lifecycle states');
END;
